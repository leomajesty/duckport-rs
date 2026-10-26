"""Fix actions for doctor findings (duckport upgrade).

Each fix takes one finding at level `online` or `stop` and brings its target to the
desired state. Fixes re-check what they touch, so running them again is harmless.
Rows are never deleted without first being exported to a parquet file under the run's
quarantine directory; the export and the delete run in one transaction.
"""

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Tuple

from duckport_flight import lit, text
from reconcile import rebuild
from reconcile.rules import Finding, Options, bad_string_condition, dup_scope, is_bloated, storage_rows
from reconcile.specs import (
    RETIRED_DUCK_TIME_KEY, SYSTEM_COLUMN_DEFAULTS, SYSTEM_DDL, SYSTEM_TABLES,
)
from reconcile.state import State, q_ident

# Fixes run in this order: system tables and watermark rows first (L2 moves values into
# watermark rows), deletes before rebuilds, columns last so they land on rebuilt tables.
ORDER = ["S1", "S2", "L2", "L1", "D1", "D2", "D3", "B1", "D5", "D4", "B2"]
# Deleting corrupted or duplicate business rows needs an operator's confirmation.
NEEDS_CONFIRMATION = {"D1", "D2"}


@dataclass
class Context:
    dp: object
    run_id: str
    phase: str
    quarantine_dir: str
    opts: Options


@dataclass
class Result:
    finding: Finding
    status: str          # ok | error
    rows: int
    detail: str
    seconds: float


FIXES: Dict[str, Callable[[Context, State, Finding], Tuple[int, str]]] = {}


def fix(rule_id):
    def wrap(fn):
        FIXES[rule_id] = fn
        return fn
    return wrap


def actionable(findings: List[Finding], levels) -> List[Finding]:
    todo = [f for f in findings if f.level in levels and f.rule in FIXES]
    return sorted(todo, key=lambda f: (ORDER.index(f.rule), f.target))


def apply(ctx: Context, state: State, findings: List[Finding], levels, confirmed=False) -> List[Result]:
    """Fix every actionable finding; D1/D2 deletions only when `confirmed`."""
    results = []
    for f in actionable(findings, levels):
        started = datetime.now()
        t0 = time.time()
        print(f"  [{f.rule}] {f.target} …", flush=True)
        if f.rule in NEEDS_CONFIRMATION and not confirmed:
            res = Result(f, "skipped", 0, "未经人工确认，未删除", 0)
            print(f"    - {res.detail}", flush=True)
            log_result(ctx, res, started)
            results.append(res)
            continue
        try:
            rows, detail = FIXES[f.rule](ctx, state, f)
            res = Result(f, "ok", rows, detail, time.time() - t0)
        except Exception as e:  # keep going: other targets are independent
            res = Result(f, "error", 0, str(e).splitlines()[0][:500], time.time() - t0)
        mark = "✓" if res.status == "ok" else "✗"
        print(f"    {mark} {res.detail}（{res.seconds:.1f}s）", flush=True)
        log_result(ctx, res, started)
        results.append(res)
    return results


def log_result(ctx, res: Result, started):
    f = res.finding
    try:
        ctx.dp.execute(
            "INSERT INTO data.upgrade_log VALUES ("
            f"{lit(ctx.run_id)}, {lit(ctx.phase)}, {lit(f.rule)}, {text(f.target)}, {text(f.fix or f.message)}, "
            f"{lit(res.status)}, {int(res.rows)}, {text(res.detail)}, "
            f"TIMESTAMP {lit(_ts(started))}, TIMESTAMP {lit(_ts(datetime.now()))})")
    except Exception as e:
        print(f"    (upgrade_log 写入失败：{str(e).splitlines()[0]})")


def _ts(dt):
    return dt.isoformat(sep=" ", timespec="seconds")


# ── helpers ───────────────────────────────────────────────────────────────────

def quarantine_delete(ctx: Context, relation: str, where: str, name: str) -> Tuple[int, str]:
    """Export the rows of `relation` matching `where` to parquet, then delete them."""
    os.makedirs(ctx.quarantine_dir, exist_ok=True)
    base = os.path.join(ctx.quarantine_dir, re.sub(r"[^A-Za-z0-9_.-]", "_", name))
    path, n = base + ".parquet", 1
    while os.path.exists(path):
        n += 1
        path = f"{base}.{n}.parquet"
    res = ctx.dp.transaction([
        f"COPY (SELECT * FROM {relation} WHERE {where}) TO {lit(path)} (FORMAT parquet)",
        f"DELETE FROM {relation} WHERE {where}",
    ])
    copied, deleted = res["rows_affected"]
    if copied != deleted:  # one transaction, single writer: should never happen
        raise RuntimeError(f"导出 {copied} 行但删除了 {deleted} 行，见 {path}")
    if not deleted:
        if os.path.exists(path):
            os.remove(path)
        return 0, "没有需要处理的行"
    return deleted, f"{deleted:,} 行已导出到 {path} 后删除"


def _table(state, name):
    t = state.tables.get(name)
    if not t:
        raise RuntimeError(f"data.{name} 不存在")
    return t


# ── fixes ─────────────────────────────────────────────────────────────────────

@fix("S1")
def fix_system_table(ctx, state, f):
    name = f.target
    t = state.tables.get(name)
    if not t:
        ctx.dp.execute(SYSTEM_DDL[name])
        return 0, f"已建表 data.{name}"
    added = []
    for col, ty in SYSTEM_TABLES[name].items():
        if col in t.columns:
            continue
        default = SYSTEM_COLUMN_DEFAULTS.get((name, col))
        ctx.dp.execute(f"ALTER TABLE data.{q_ident(name)} ADD COLUMN IF NOT EXISTS {q_ident(col)} {ty}"
                       + (f" DEFAULT {default}" if default else ""))
        added.append(col)
    return 0, "已补列：" + ", ".join(added)


@fix("S2")
def fix_watermark_row(ctx, state, f):
    spec = state.spec(f.target)
    tc = q_ident(spec.time_column)
    if f.target in state.watermark:
        ctx.dp.execute(f"UPDATE data.watermark SET time_column = {lit(spec.time_column)} "
                       f"WHERE table_name = {lit(f.target)}")
        return 1, f"time_column 改为 {spec.time_column}"
    # The writer reloads duck_time from this row; max(time) makes it continue after the data.
    ctx.dp.execute(
        "INSERT INTO data.watermark (table_name, time_column, start_time, duck_time, updated_at) "
        f"SELECT {lit(f.target)}, {lit(spec.time_column)}, min({tc}), max({tc}), CURRENT_TIMESTAMP "
        f"FROM data.{q_ident(f.target)} ON CONFLICT (table_name) DO NOTHING")
    return 1, "已补 watermark 行（duck_time = 表中最新时间）"


@fix("L1")
def fix_retired_table(ctx, state, f):
    rows, detail = quarantine_delete(ctx, f"data.{q_ident(f.target)}", "true", f"L1.{f.target}")
    ctx.dp.execute(f"DROP TABLE data.{q_ident(f.target)}")
    return rows, (detail + "；" if rows else "") + f"已删除 data.{f.target}"


def _config_key(f):
    return f.target.split(":", 1)[1]


@fix("L2")
def fix_retired_duck_time(ctx, state, f):
    key = _config_key(f)
    value = state.config.get(key)
    market = RETIRED_DUCK_TIME_KEY.match(key).group("market")
    res = ctx.dp.transaction([
        f"UPDATE data.watermark SET duck_time = TRY_CAST({lit(value)} AS TIMESTAMP), "
        "updated_at = CURRENT_TIMESTAMP "
        f"WHERE starts_with(table_name, {lit(market + '_')}) AND duck_time IS NULL",
        f"DELETE FROM data.config_dict WHERE key = {lit(key)}",
    ])
    moved = res["rows_affected"][0]
    return moved, (f"{moved} 行 watermark 的 duck_time 取自它；" if moved else "") + f"已删除（原值 {value}）"


@fix("D1")
def fix_bad_strings(ctx, state, f):
    t = _table(state, f.target)
    cond = bad_string_condition(state.spec(t.name), t)
    return quarantine_delete(ctx, f"data.{q_ident(t.name)}", f"({cond})", f"D1.{t.name}")


@fix("D2")
def fix_duplicates(ctx, state, f):
    t = _table(state, f.target)
    spec = state.spec(t.name)
    where, scope = dup_scope(spec, t, ctx.opts)
    key = ", ".join(q_ident(c) for c in spec.dedup_key)
    # Keep the most recently written row of each key (highest rowid).
    extra = (f"rowid IN (SELECT rowid FROM (SELECT rowid, row_number() OVER "
             f"(PARTITION BY {key} ORDER BY rowid DESC) AS rn FROM data.{q_ident(t.name)}{where}) WHERE rn > 1)")
    rows, detail = quarantine_delete(ctx, f"data.{q_ident(t.name)}", extra, f"D2.{t.name}")
    return rows, f"{scope}：{detail}"


def _shown(col, width=40):
    """A column for display: long strings cut, with their length."""
    c = q_ident(col)
    return (f"CASE WHEN length(CAST({c} AS VARCHAR)) > {width} THEN left(CAST({c} AS VARCHAR), {width}) "
            f"|| '…(' || length(CAST({c} AS VARCHAR)) || ')' ELSE CAST({c} AS VARCHAR) END AS {c}")


def preview(dp, state: State, f: Finding, opts: Options, limit=5) -> Tuple[int, List[dict]]:
    """For a D1/D2 finding: (rows the fix would delete, a few sample rows to show the operator)."""
    t = _table(state, f.target)
    spec = state.spec(t.name)
    rel = f"data.{q_ident(t.name)}"
    if f.rule == "D1":
        cond = bad_string_condition(spec, t)
        cols = [c for c in (*spec.dedup_key, *spec.string_checks) if c in t.columns]
        cols = list(dict.fromkeys(cols))
        n = dp.scalar(f"SELECT count(*) FROM {rel} WHERE {cond}")
        rows = dp.query(f"SELECT {', '.join(_shown(c) for c in cols)} FROM {rel} WHERE {cond} LIMIT {limit}")
        return n, rows
    where, _ = dup_scope(spec, t, opts)
    key = ", ".join(q_ident(c) for c in spec.dedup_key)
    n = dp.scalar(f"SELECT count(*) - count(DISTINCT ({key})) FROM {rel}{where}")
    rows = dp.query(f"SELECT {', '.join(_shown(c) for c in spec.dedup_key)}, count(*) AS copies FROM {rel}{where} "
                    f"GROUP BY {key} HAVING count(*) > 1 ORDER BY copies DESC LIMIT {limit}")
    return n, rows


@fix("D3")
def fix_staging(ctx, state, f):
    name = f.target.split(".", 1)[1]
    return quarantine_delete(ctx, f"main.{q_ident(name)}", "true", f"D3.{name}")


def _rebuild(ctx, state, table) -> int:
    """Copy `table` into a constraint-free twin and swap; the original stays as <table>__pk_old."""
    spec = state.spec(table)
    if spec:
        time_col = spec.time_column
    else:
        time_col = (state.watermark.get(table) or {}).get("time_column")
        if time_col not in state.tables[table].columns:
            time_col = None
    return rebuild.copy_table(ctx.dp, table, time_col)


@fix("B1")
def fix_primary_key(ctx, state, f):
    n = _rebuild(ctx, state, f.target)
    return n, f"已重建为无主键表（{n:,} 行），原表保留为 {f.target}{rebuild.OLD_SUFFIX}"


@fix("D5")
def fix_bloat(ctx, state, f):
    t = _table(state, f.target)
    if rebuild.has_key(ctx.dp, t.name):
        return 0, "跳过：带主键，由 B1 重建"
    physical, live = storage_rows(ctx.dp, t)
    if not is_bloated(physical, live, ctx.opts):
        return 0, "已不再膨胀（同一窗口里已重建）"
    n = _rebuild(ctx, state, t.name)
    # Same rows, no constraints changed: nothing to roll back to, so free the space now.
    ctx.dp.execute(f"DROP TABLE data.{q_ident(t.name + rebuild.OLD_SUFFIX)}")
    return n, f"已重建（物理 {physical:,} 行 → {n:,} 行）"


@fix("D4")
def fix_watermark_time(ctx, state, f):
    spec = state.spec(f.target)
    tc = q_ident(spec.time_column)
    latest = ctx.dp.scalar(f"SELECT CAST(max({tc}) AS VARCHAR) FROM data.{q_ident(f.target)}")
    if latest is None:
        return 0, "表已为空，未修改"
    ctx.dp.execute(f"UPDATE data.watermark SET duck_time = TIMESTAMP {lit(latest)}, "
                   f"updated_at = CURRENT_TIMESTAMP WHERE table_name = {lit(f.target)}")
    old = (state.watermark.get(f.target) or {}).get("duck_time")
    return 1, f"duck_time {old} → {latest}"


@fix("B2")
def fix_columns(ctx, state, f):
    t = _table(state, f.target)
    spec = state.spec(t.name)
    added = []
    for col, ty in spec.columns.items():
        if col in t.columns:
            continue
        default = spec.column_defaults.get(col)
        ctx.dp.execute(f"ALTER TABLE data.{q_ident(t.name)} ADD COLUMN IF NOT EXISTS {q_ident(col)} {ty}"
                       + (f" DEFAULT {default}" if default else ""))
        added.append(col)
    return 0, "已补列：" + ", ".join(added)

