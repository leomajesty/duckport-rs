"""Shrink the database file after a rebuild.

Dropping the `<table>__pk_old` copies a rebuild leaves behind never returns space to the
filesystem: DuckDB marks their blocks free and reuses them for later writes, so the file
stays the size it grew to. Only a full rewrite produces a smaller file, so the live
catalog is copied into a fresh database next to the old one.

The caller swaps the two files while the server is down and keeps the original until the
new one has been verified. That makes the original the backup for this step — a 273 GB
database needs no second copy of itself, which is what makes the whole thing affordable
on a disk that could never hold a conventional backup.

Before any of this the rebuild itself is verified: a live table must be a superset of its
`__pk_old` copy, down to per-month row counts, because dropping the copy is irreversible.
"""

from typing import Dict, List, Optional, Sequence, Set, Tuple

from duckport_flight import lit, q
from reconcile.rebuild import OLD_SUFFIX, data_tables

CP = "__compact"  # ATTACH alias for the database being written

Row = Tuple[int, Optional[str], Optional[str]]  # rows, min(time), max(time)


def pairs(dp) -> List[Tuple[str, str]]:
    """(live, old) for every data.<table>__pk_old whose rebuilt table still exists."""
    tables = data_tables(dp)
    out = []
    for old in [t for t in tables if t.endswith(OLD_SUFFIX)]:
        live = old[: -len(OLD_SUFFIX)]
        if live in tables:
            out.append((live, old))
    return out


def orphans(dp) -> List[str]:
    """<table>__pk_old copies whose rebuilt table is gone — never drop these blindly."""
    tables = data_tables(dp)
    return [t for t in tables if t.endswith(OLD_SUFFIX) and t[: -len(OLD_SUFFIX)] not in tables]


def _keyed(dp) -> Dict[str, int]:
    return {r["table_name"]: r["n"] for r in dp.query(
        "SELECT table_name, count(*) AS n FROM duckdb_constraints() "
        "WHERE schema_name = 'data' AND constraint_type = 'PRIMARY KEY' GROUP BY 1")}


def verify(dp, time_columns: Dict[str, str]) -> List[Tuple[str, List[str], str]]:
    """Check that each rebuilt table is a superset of the copy it replaced.

    Returns (table, problems, summary); an empty `problems` means the copy is safe to drop.
    Equal row counts and matching first/last timestamps still allow a hole in the middle,
    so tables with a time column are also compared month by month.
    """
    keyed = _keyed(dp)
    out = []
    for live, old in pairs(dp):
        col = time_columns.get(live)
        problems: List[str] = []
        if col:
            c = q(col)
            r = dp.query(
                f"SELECT (SELECT count(*) FROM data.{q(live)}) AS nl, "
                f"       (SELECT count(*) FROM data.{q(old)}) AS no, "
                f"       (SELECT CAST(min({c}) AS VARCHAR) FROM data.{q(live)}) AS ll, "
                f"       (SELECT CAST(max({c}) AS VARCHAR) FROM data.{q(live)}) AS lh, "
                f"       (SELECT CAST(min({c}) AS VARCHAR) FROM data.{q(old)}) AS ol, "
                f"       (SELECT CAST(max({c}) AS VARCHAR) FROM data.{q(old)}) AS oh")[0]
        else:
            r = dp.query(
                f"SELECT (SELECT count(*) FROM data.{q(live)}) AS nl, "
                f"       (SELECT count(*) FROM data.{q(old)}) AS no, "
                f"       NULL AS ll, NULL AS lh, NULL AS ol, NULL AS oh")[0]

        if r["nl"] < r["no"]:
            problems.append(f"行数倒退 {r['nl']:,} < {r['no']:,}")
        if col and r["ol"] is not None:
            if r["ll"] > r["ol"]:
                problems.append(f"起点晚于旧表 {r['ll']} > {r['ol']}")
            if r["lh"] < r["oh"]:
                problems.append(f"终点早于旧表 {r['lh']} < {r['oh']}")
        if keyed.get(live):
            problems.append("重建后的表仍带 PRIMARY KEY，重建没有生效")
        if not keyed.get(old):
            problems.append("旧表没有 PRIMARY KEY，可能不是重建产物")

        months = 0
        if col and not problems:
            c = q(col)
            g = dp.query(
                f"WITH l AS (SELECT date_trunc('month', {c}) m, count(*) n "
                f"           FROM data.{q(live)} GROUP BY 1), "
                f"     o AS (SELECT date_trunc('month', {c}) m, count(*) n "
                f"           FROM data.{q(old)} GROUP BY 1), "
                f"     d AS (SELECT o.m, o.n AS nold, coalesce(l.n, 0) AS nlive "
                f"           FROM o LEFT JOIN l USING (m) WHERE coalesce(l.n, 0) < o.n) "
                f"SELECT (SELECT count(*) FROM o) AS total, (SELECT count(*) FROM d) AS bad, "
                f"       coalesce((SELECT CAST(min(m) AS VARCHAR) FROM d), '') AS first_bad, "
                f"       coalesce((SELECT sum(nold - nlive) FROM d), 0) AS missing")[0]
            months = g["total"]
            if g["bad"]:
                problems.append(f"{g['bad']}/{months} 个月份行数少于旧表"
                                f"（最早 {g['first_bad']}，共少 {g['missing']:,} 行）")

        span = f"[{r['ll']} .. {r['lh']}]" if col else "snapshot"
        seg = f"，逐月 {months}/{months} 段不缺行" if months else ""
        out.append((live, problems, f"{r['nl']:,} 行 ⊇ 旧表 {r['no']:,} 行  {span}{seg}"))
    return out


#: Written by the compaction itself (every step is logged), so it cannot be part of the
#: snapshot the compaction is checked against. The rewrite still copies it like any table.
VOLATILE = {("data", "upgrade_log")}


def tables(dp) -> List[Tuple[str, str]]:
    """(schema, table) for every base table that a rewrite has to reproduce exactly."""
    rows = dp.query(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE database_name = current_database() ORDER BY schema_name, table_name")
    return [(r["schema_name"], r["table_name"]) for r in rows
            if not r["table_name"].endswith(OLD_SUFFIX)
            and (r["schema_name"], r["table_name"]) not in VOLATILE]


def snapshot(dp, names: Sequence[Tuple[str, str]], time_columns: Dict[str, str],
             catalog: Optional[str] = None) -> Dict[str, Row]:
    """Row count and time span per table, as one UNION ALL so it reads one consistent state."""
    if not names:
        return {}
    parts = []
    for schema, table in names:
        ref = f"{q(schema)}.{q(table)}"
        if catalog:
            ref = f"{q(catalog)}.{ref}"
        col = time_columns.get(table)
        span = (f"CAST(min({q(col)}) AS VARCHAR) AS lo, CAST(max({q(col)}) AS VARCHAR) AS hi"
                if col else "NULL::VARCHAR AS lo, NULL::VARCHAR AS hi")
        parts.append(f"SELECT {lit(f'{schema}.{table}')} AS t, count(*) AS n, {span} FROM {ref}")
    rows = dp.query("\nUNION ALL\n".join(parts))
    return {r["t"]: (r["n"], r["lo"], r["hi"]) for r in rows}


def differences(baseline: Dict[str, Row], actual: Dict[str, Row], where: str) -> List[str]:
    out = []
    for key in sorted(baseline):
        if key not in actual:
            out.append(f"{key}：{where}中缺失")
            continue
        want, got = tuple(baseline[key]), tuple(actual[key])
        if want != got:
            out.append(f"{key}：期望 {want[0]:,} 行 [{want[1]} .. {want[2]}]，"
                       f"实得 {got[0]:,} 行 [{got[1]} .. {got[2]}]")
    for key in sorted(set(actual) - set(baseline)):
        out.append(f"{key}：{where}中多出未预期的表")
    return out


def payload_bytes(dp) -> int:
    """Bytes the live data occupies inside the file — what a rewrite would come out at."""
    r = dp.query("SELECT used_blocks, block_size FROM pragma_database_size()")[0]
    return int(r["used_blocks"]) * int(r["block_size"])


def rewrite(dp, path: str, names: Sequence[Tuple[str, str]], time_columns: Dict[str, str],
            baseline: Dict[str, Row]) -> Dict[str, Row]:
    """Copy the whole catalog into a new database at `path` and verify it against `baseline`.

    Raises RuntimeError when the copy does not match; the caller removes the file. The
    source is left untouched, so a failure here costs nothing but the time and the space.
    """
    source = dp.scalar("SELECT current_database()")
    dp.execute(f"ATTACH {lit(path)} AS {q(CP)}")
    try:
        dp.execute(f"COPY FROM DATABASE {q(source)} TO {q(CP)}")
        actual = snapshot(dp, names, time_columns, catalog=CP)
        problems = differences(baseline, actual, "新库")
        if problems:
            raise RuntimeError("新库与基线不一致：\n    " + "\n    ".join(problems))
        return actual
    finally:
        try:
            dp.execute(f"DETACH {q(CP)}")
        except Exception:
            pass
