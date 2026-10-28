"""Rebuild a business table without PRIMARY KEY / UNIQUE constraints, and undo it.

DuckDB 1.5 cannot vacuum deleted rows of tables that carry an index (a PRIMARY KEY is
one). A table is copied into a constraint-free twin `<table>__nopk`, in time order, the
row count is verified, and the names are swapped in one transaction; the original stays
as `<table>__pk_old` until `finalize` drops it or `rollback` puts it back.
"""

import time
from typing import Dict, List, Optional, Tuple

from duckport_flight import lit, q

OLD_SUFFIX = "__pk_old"
NEW_SUFFIX = "__nopk"


def data_tables(dp) -> List[str]:
    """Every base table in schema data. Suffixes are matched in Python: `_` is a LIKE wildcard."""
    return [r["table_name"] for r in dp.query(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'data' ORDER BY table_name")]


def has_key(dp, table) -> bool:
    return bool(dp.scalar(
        "SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' "
        f"AND table_name = {lit(table)} AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')"))


def create_twin_sql(dp, table) -> str:
    """CREATE TABLE for <table>__nopk: same columns, types, defaults and NOT NULL, no keys."""
    cols = dp.query(
        "SELECT column_name, data_type, column_default, is_nullable FROM duckdb_columns() "
        f"WHERE schema_name = 'data' AND table_name = {lit(table)} ORDER BY column_index"
    )
    if not cols:
        raise RuntimeError(f"no columns found for data.{table}")
    parts = []
    for c in cols:
        part = f"{q(c['column_name'])} {c['data_type']}"
        if c["column_default"] is not None:
            part += f" DEFAULT {c['column_default']}"
        if not c["is_nullable"]:
            part += " NOT NULL"
        parts.append(part)
    return f"CREATE TABLE data.{q(table + NEW_SUFFIX)} ({', '.join(parts)})"


def copy_table(dp, table, time_col: Optional[str]) -> int:
    """Copy into the twin (month by month in time order when there is a time column),
    verify the row count, swap names. Returns the number of rows."""
    names = set(data_tables(dp))
    if table + OLD_SUFFIX in names:
        raise RuntimeError(f"{table}{OLD_SUFFIX} 已存在，先 finalize 或 rollback")
    if table + NEW_SUFFIX in names:  # an interrupted copy; the original is intact
        dp.execute(f"DROP TABLE data.{q(table + NEW_SUFFIX)}")
    src, dst = f"data.{q(table)}", f"data.{q(table + NEW_SUFFIX)}"
    dp.execute(create_twin_sql(dp, table))
    try:
        if time_col:
            tc = q(time_col)
            months = dp.query(
                f"SELECT DISTINCT CAST(date_trunc('month', {tc}) AS VARCHAR) AS m FROM {src} "
                f"WHERE {tc} IS NOT NULL ORDER BY m")
            for r in months:
                t0 = time.time()
                n = dp.execute(
                    f"INSERT INTO {dst} SELECT * FROM {src} WHERE {tc} >= TIMESTAMP {lit(r['m'])} "
                    f"AND {tc} < TIMESTAMP {lit(r['m'])} + INTERVAL 1 MONTH ORDER BY {tc}")
                print(f"    {r['m'][:7]}  {n:>12,} rows  {time.time() - t0:5.1f}s", flush=True)
            dp.execute(f"INSERT INTO {dst} SELECT * FROM {src} WHERE {tc} IS NULL")
        else:
            dp.execute(f"INSERT INTO {dst} SELECT * FROM {src}")

        src_n = dp.scalar(f"SELECT count(*) FROM {src}")
        dst_n = dp.scalar(f"SELECT count(*) FROM {dst}")
        if src_n != dst_n:
            raise RuntimeError(f"data.{table} 行数不一致：{src_n} vs {dst_n}")
    except Exception as e:
        # A copy that dies half-way (disk full on rmt.ctb2, the server killed) must not leave the
        # half-filled twin behind: dropped and checkpointed, its blocks are free again for the
        # retry instead of waiting for the next run or a --rollback. The original is untouched.
        raise RuntimeError(f"{str(e).splitlines()[0]}；{_discard(dp, dst)}") from e
    dp.transaction([
        f"ALTER TABLE {src} RENAME TO {q(table + OLD_SUFFIX)}",
        f"ALTER TABLE {dst} RENAME TO {q(table)}",
    ])
    return dst_n


def _discard(dp, dst: str) -> str:
    """Best effort: drop the half-copied twin and checkpoint so its blocks are reusable now."""
    try:
        dp.execute(f"DROP TABLE IF EXISTS {dst}")
    except Exception as e:  # e.g. the server is gone
        return f"半成品 {dst} 未能删除（{str(e).splitlines()[0][:80]}），下次重建或 --rollback 会清理"
    try:
        dp.execute("CHECKPOINT")
    except Exception as e:
        return f"半成品 {dst} 已删除，但 CHECKPOINT 失败（{str(e).splitlines()[0][:80]}）：空间在下次 checkpoint 后复用"
    return f"半成品 {dst} 已删除，空间已回到空闲块"


def finalize(dp) -> List[str]:
    """Drop every <table>__pk_old; returns their names."""
    olds = [t for t in data_tables(dp) if t.endswith(OLD_SUFFIX)]
    for old in olds:
        dp.execute(f"DROP TABLE data.{q(old)}")
    if olds:
        dp.execute("CHECKPOINT")
    return olds


def _restore_statements(dp, table, old, time_col, snapshot) -> Tuple[List[str], str]:
    """Statements that move rows written since the rebuild from data.<table> into
    data.<old> (which still has its key)."""
    new, prev = f"data.{q(table)}", f"data.{q(old)}"
    if snapshot:
        # Snapshot table: the new copy is the current state.
        return [f"DELETE FROM {prev}", f"INSERT INTO {prev} SELECT * FROM {new}"], "用当前快照替换"
    if not has_key(dp, old):
        # Only an index, no constraint to resolve conflicts against: copy rows not present.
        return [f"INSERT INTO {prev} SELECT * FROM {new} EXCEPT SELECT * FROM {prev}"], "补回了新表中多出的行"
    if not time_col:
        return [f"INSERT INTO {prev} SELECT * FROM {new} ON CONFLICT DO NOTHING"], "按主键补回了新写入的数据"

    col = q(time_col)
    old_max = dp.scalar(f"SELECT CAST(max({col}) AS VARCHAR) FROM {prev}")
    if old_max is None:
        # The old table was empty: everything in the new one was written after the rebuild.
        return [f"INSERT INTO {prev} SELECT * FROM {new} WHERE {col} IS NOT NULL ON CONFLICT DO NOTHING"], \
            "原表为空，新写入的数据已全部补回"
    stmts = [f"INSERT INTO {prev} SELECT * FROM {new} WHERE {col} > TIMESTAMP {lit(old_max)} ON CONFLICT DO NOTHING"]
    note = "重建后新写入的数据已补回"
    older_new = dp.scalar(f"SELECT count(*) FROM {new} WHERE {col} <= TIMESTAMP {lit(old_max)}")
    older_old = dp.scalar(f"SELECT count(*) FROM {prev} WHERE {col} <= TIMESTAMP {lit(old_max)}")
    if older_new != older_old:
        # Backfilled (or deleted) rows inside the old range: copy whatever the old table lacks.
        stmts.append(f"INSERT INTO {prev} SELECT * FROM {new} WHERE {col} <= TIMESTAMP {lit(old_max)} "
                     "ON CONFLICT DO NOTHING")
        note += f"；{old_max} 之前的行数不一致（{older_new} vs {older_old}），已按主键补齐"
    return stmts, note


def rollback(dp, time_columns: Dict[str, str], snapshots) -> List[Tuple[str, str]]:
    """Put every <table>__pk_old back under its name, keeping rows written since.
    Returns (table, note) per restored table."""
    tables = data_tables(dp)
    done = []
    for old in [t for t in tables if t.endswith(OLD_SUFFIX)]:
        table = old[: -len(OLD_SUFFIX)]
        stmts, note = [], "恢复"
        if table in tables:
            stmts, note = _restore_statements(dp, table, old, time_columns.get(table), table in snapshots)
            stmts.append(f"DROP TABLE data.{q(table)}")
        stmts.append(f"ALTER TABLE data.{q(old)} RENAME TO {q(table)}")
        dp.transaction(stmts)
        done.append((table, note))
    for t in data_tables(dp):
        if t.endswith(NEW_SUFFIX):
            dp.execute(f"DROP TABLE data.{q(t)}")
    dp.execute("CHECKPOINT")
    return done
