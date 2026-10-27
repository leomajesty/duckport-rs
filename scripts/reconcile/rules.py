"""Rules comparing a database against the desired state. Detection only (doctor).

Each rule yields Findings. A finding's level says how it would be fixed:
  block  - a precondition is missing; nothing is changed until it is met
  stop   - fixed inside one write-stop window (ingestors stopped)
  online - fixed while everything runs
  hint   - reported only
The fixes for these findings live in reconcile.fixes and reuse the conditions below.
"""

from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Callable, Iterable, List

from duckport_flight import lit
from reconcile.specs import (
    LEFTOVER_SUFFIXES, PLUGIN_MIN_VERSION, RETIRED_DUCK_TIME_KEY, RETIRED_TABLES,
    SERVER_MIN_VERSION, SYSTEM_TABLES, version_at_least,
)
from reconcile import memory, startgap
from reconcile.state import State, q_ident

LEVELS = ("block", "stop", "online", "hint")


@dataclass
class Finding:
    rule: str
    level: str
    target: str
    message: str
    fix: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class Options:
    deep: bool = False             # full-table duplicate scan instead of the recent window
    dup_window_days: int = 7
    bloat_ratio: float = 2.0
    bloat_min_dead_rows: int = 1_000_000
    free_ratio: float = 0.4


RULES: List[tuple] = []  # (id, func)


def rule(rule_id: str):
    def wrap(fn: Callable[[State, object, Options], Iterable[Finding]]):
        RULES.append((rule_id, fn))
        return fn
    return wrap


def run_rules(state: State, dp, opts: Options) -> List[Finding]:
    findings: List[Finding] = []
    for rule_id, fn in RULES:
        try:
            findings.extend(fn(state, dp, opts))
        except Exception as e:  # one broken check must not hide the others
            findings.append(Finding(rule_id, "hint", "-", f"检查失败：{str(e).splitlines()[0][:200]}"))
    order = {lvl: i for i, lvl in enumerate(LEVELS)}
    return sorted(findings, key=lambda f: (order[f.level], f.rule, f.target))


# ── versions ──────────────────────────────────────────────────────────────────

@rule("V1")
def server_version(state, dp, opts):
    ok = version_at_least(state.server_version, SERVER_MIN_VERSION)
    if ok is False:
        yield Finding("V1", "block", "duckport-server",
                      f"服务端 v{state.server_version} 低于 {SERVER_MIN_VERSION}",
                      "升级服务端（deploy.sh 或 duckport upgrade）")


@rule("V2")
def plugin_versions(state, dp, opts):
    for inst in state.instances:
        minimum = PLUGIN_MIN_VERSION.get(inst.exec)
        if not minimum:
            continue
        ok = version_at_least(inst.version, minimum)
        if ok is False:
            yield Finding("V2", "block", inst.name,
                          f"{inst.exec} v{inst.version} 低于 {minimum}",
                          f"升级插件实例 {inst.name} 并重启")
        elif ok is None:
            yield Finding("V2", "hint", inst.name, f"读不到 {inst.exec} 的安装版本")


def _writer_ready(state: State, table: str):
    """(ready, reason): can the table's writers handle a table without a key?

    For plugins duckport knows (with a minimum version) the installed version decides, so a
    rolled-back instance is caught even though the newer version registered anti_join.
    Other writers count as ready only through their registration (table_specs or the
    write_mode key); without one the answer is unknown (None).
    """
    writers = state.writers(table)
    if writers and all(PLUGIN_MIN_VERSION.get(w.exec) for w in writers):
        old = [f"{w.name}（{w.exec} v{w.version or '?'}）" for w in writers
               if version_at_least(w.version, PLUGIN_MIN_VERSION[w.exec]) is not True]
        if old:
            return False, "写入方仍是旧写法：" + "、".join(old)
        return True, "写入方插件版本满足要求"
    spec = state.spec(table)
    if spec and spec.source == "registered" and spec.write_mode == "anti_join":
        return True, f"表规格登记 write_mode=anti_join（{spec.plugin} v{spec.writer_version or '?'}）"
    if state.write_modes.get(table) == "anti_join":
        return True, "已登记 write_mode=anti_join"
    if not writers:
        return None, "找不到写入这张表的插件实例，也没有 write_mode 登记"
    return None, "写入方插件不在已知列表里，也没有登记 write_mode"


@rule("V3")
def writer_compat(state, dp, opts):
    for t in state.business_tables():
        if t.keyed:
            continue
        ready, reason = _writer_ready(state, t.name)
        if ready is False:
            yield Finding("V3", "block", t.name,
                          f"表已没有主键，但{reason}；这些实例的写入会全部失败（ON CONFLICT 报 Binder Error）",
                          "立即升级这些插件实例；<表>__pk_old 还在时也可以 duckport upgrade --rollback")


# ── system tables and retired objects ─────────────────────────────────────────

@rule("S1")
def system_tables(state, dp, opts):
    for name, expected in SYSTEM_TABLES.items():
        t = state.tables.get(name)
        if not t:
            yield Finding("S1", "online", name, "系统表不存在", "建表（服务端启动时也会创建）")
            continue
        missing = [c for c in expected if c not in t.columns]
        wrong = [f"{c}: {t.columns[c]} ≠ {ty}" for c, ty in expected.items()
                 if c in t.columns and t.columns[c] != ty]
        if missing:
            yield Finding("S1", "online", name, f"缺少列：{', '.join(missing)}", "补列")
        if wrong:
            yield Finding("S1", "hint", name, f"列类型不一致：{'; '.join(wrong)}")


@rule("S2")
def watermark_rows(state, dp, opts):
    for t in state.business_tables():
        spec = state.spec(t.name)
        if not spec or spec.kind != "append":
            continue
        row = state.watermark.get(t.name)
        if not row:
            yield Finding("S2", "online", t.name, "data.watermark 中没有这张表的行",
                          f"补一行，time_column={spec.time_column}")
        elif row.get("time_column") != spec.time_column:
            yield Finding("S2", "online", t.name,
                          f"watermark.time_column={row.get('time_column')!r}，应为 {spec.time_column!r}", "修正")
    for name in sorted(state.watermark):
        if name not in state.tables:
            yield Finding("S2", "hint", name, "watermark 中有这张表的行，但表不存在")
        else:
            col = state.watermark[name].get("time_column")
            if col and col not in state.tables[name].columns:
                yield Finding("S2", "hint", name, f"watermark.time_column={col!r} 不是这张表的列")


@rule("S3")
def registered_specs(state, dp, opts):
    for name in state.bad_specs:
        yield Finding("S3", "hint", name, "data.table_specs 中这张表的规格无法解析，按内置规格或未知表处理",
                      f"重启写入它的插件重新登记（{name}）")
    for name, spec in sorted(state.registered.items()):
        if name not in state.tables:
            yield Finding("S3", "hint", name, f"{spec.plugin} 登记了规格，但表不存在")


@rule("L1")
def retired_tables(state, dp, opts):
    for name, why in RETIRED_TABLES.items():
        if name in state.tables:
            yield Finding("L1", "online", name, f"废弃的表仍存在（{why}）", "删除")


@rule("L2")
def retired_duck_time_keys(state, dp, opts):
    for key in sorted(state.config):
        m = RETIRED_DUCK_TIME_KEY.match(key)
        if not m:
            continue
        market = m.group("market")
        targets = [n for n in state.watermark if n.startswith(market + "_")]
        has_time = any(state.watermark[n].get("duck_time") for n in targets)
        fix = "删除" if has_time else "先把值迁入 data.watermark（对应表还没有 duck_time），再删除"
        yield Finding("L2", "online", f"config_dict:{key}", "废弃的 key（duck_time 已移到 data.watermark）", fix)


@rule("C1")
def leftovers(state, dp, opts):
    for name in sorted(state.tables):
        if name.endswith(LEFTOVER_SUFFIXES):
            kind = "去主键迁移保留的旧表" if name.endswith("__pk_old") else "迁移中断留下的半成品"
            yield Finding("C1", "hint", name, kind,
                          "sudo duckport upgrade --compact（校验后删除旧表并压缩库文件）；只删旧表不压缩 --finalize；"
                          "有问题 --rollback")


# ── business table structure ──────────────────────────────────────────────────

@rule("B1")
def keyed_business_tables(state, dp, opts):
    for t in state.business_tables():
        if not t.keyed:
            continue
        ready, reason = _writer_ready(state, t.name)
        if ready and t.name + "__pk_old" in state.tables:
            yield Finding("B1", "block", t.name, f"上次去主键留下的 {t.name}__pk_old 还在",
                          "先处理它：sudo duckport upgrade --compact（校验后删除并压缩）、--finalize（只删除）或 --rollback（恢复）")
        elif ready:
            yield Finding("B1", "stop", t.name,
                          f"带主键/唯一约束/索引（约 {t.estimated_size:,} 行）；{reason}",
                          "重建为无主键表，原表保留为 <表>__pk_old")
        elif ready is None:
            # Unknown writer (e.g. a third-party plugin): never rebuilt automatically.
            yield Finding("B1", "hint", t.name, f"带主键/唯一约束/索引；{reason}，不会自动去主键",
                          f"写入方改用 anti-join 后，在 data.config_dict 登记 write_mode:{t.name}=anti_join，"
                          "duckport upgrade 会自动去主键")
        else:
            yield Finding("B1", "block", t.name, f"带主键/唯一约束/索引，但{reason}",
                          "先升级写入它的插件，再去主键")


@rule("B2")
def business_columns(state, dp, opts):
    for t in state.business_tables():
        spec = state.spec(t.name)
        if not spec:
            continue
        missing = [c for c in spec.columns if c not in t.columns]
        extra = [c for c in t.columns if c not in spec.columns]
        if missing:
            yield Finding("B2", "online", t.name, f"缺少列：{', '.join(missing)}", "按规格补列")
        legacy = [c for c in extra if c in spec.legacy_columns]
        other = [c for c in extra if c not in spec.legacy_columns]
        if legacy:
            yield Finding("B2", "hint", t.name, f"旧版本插件留下的列：{', '.join(legacy)}",
                          "新版插件启动时会自行删除")
        if other:
            yield Finding("B2", "hint", t.name, f"规格之外的列：{', '.join(other)}")


# ── data health ───────────────────────────────────────────────────────────────

def bad_string_condition(spec, table) -> str:
    """WHERE condition matching rows that break the spec's string checks ('' if none apply)."""
    conds = []
    for col, chk in (spec.string_checks if spec else {}).items():
        if col not in table.columns:
            continue
        c = q_ident(col)
        parts = []
        if chk.max_len:
            parts.append(f"length({c}) > {int(chk.max_len)}")
        if chk.pattern:
            parts.append(f"NOT regexp_full_match({c}, {lit(chk.pattern)})")
        if parts:
            conds.append(f"({c} IS NOT NULL AND ({' OR '.join(parts)}))")
    return " OR ".join(conds)


def dup_scope(spec, table, opts):
    """(WHERE clause or '', description) limiting the duplicate scan."""
    if spec.time_column and not opts.deep:
        tc = q_ident(spec.time_column)
        return (f" WHERE {tc} >= (SELECT max({tc}) FROM data.{q_ident(table.name)}) "
                f"- INTERVAL {int(opts.dup_window_days)} DAY"), f"最近 {opts.dup_window_days} 天"
    return "", "全表"


def storage_rows(dp, table):
    """(physical rows incl. deleted ones, live rows)."""
    first = next(iter(table.columns), None)
    if not first:
        return 0, 0
    physical = dp.scalar(
        f"SELECT sum(count) FROM pragma_storage_info({lit('data.' + table.name)}) "
        f"WHERE column_name = {lit(first)} AND segment_type <> 'VALIDITY'") or 0
    if not physical:
        return 0, 0
    return physical, dp.scalar(f"SELECT count(*) FROM data.{q_ident(table.name)}") or 0


def is_bloated(physical, live, opts):
    return physical - live >= opts.bloat_min_dead_rows and physical > opts.bloat_ratio * max(live, 1)


@rule("D1")
def bad_strings(state, dp, opts):
    for t in state.business_tables():
        spec = state.spec(t.name)
        cond = bad_string_condition(spec, t)
        if not cond:
            continue
        n = dp.scalar(f"SELECT count(*) FROM data.{q_ident(t.name)} WHERE {cond}")
        if n:
            yield Finding("D1", "online", t.name,
                          f"{n:,} 行字符串不合规（{', '.join(spec.string_checks)}；常见于 DuckDB ≤ 1.5.1 的 duckdb#22279）",
                          "人工确认后导出到隔离文件并删除")


@rule("D2")
def duplicate_keys(state, dp, opts):
    for t in state.business_tables():
        spec = state.spec(t.name)
        if not spec or t.keyed or any(c not in t.columns for c in spec.dedup_key):
            continue
        key = ", ".join(q_ident(c) for c in spec.dedup_key)
        where, scope = dup_scope(spec, t, opts)
        n = dp.scalar(f"SELECT count(*) - count(DISTINCT ({key})) FROM data.{q_ident(t.name)}{where}")
        if n:
            yield Finding("D2", "online", t.name, f"{scope}有 {n:,} 行重复（按 {', '.join(spec.dedup_key)}）",
                          "人工确认后每组保留最后写入的一行，其余导出到隔离文件并删除")


@rule("D3")
def staging_rows(state, dp, opts):
    running = [i.name for i in state.instances if i.active]
    for name, est in sorted(state.staging.items()):
        n = dp.scalar(f"SELECT count(*) FROM main.{q_ident(name)}")
        if not n:
            continue
        if running:
            yield Finding("D3", "hint", f"main.{name}", f"{n:,} 行（ingestor 运行中，可能只是正在写入）")
        else:
            yield Finding("D3", "stop", f"main.{name}", f"{n:,} 行残留（ingestor 都已停止）", "清空")


@rule("D4")
def watermark_vs_data(state, dp, opts):
    for t in state.business_tables():
        spec = state.spec(t.name)
        row = state.watermark.get(t.name)
        if not spec or spec.kind != "append" or not row or spec.time_column not in t.columns:
            continue
        latest = dp.scalar(f"SELECT max({q_ident(spec.time_column)}) FROM data.{q_ident(t.name)}")
        duck = row.get("duck_time")
        if latest is None:
            continue
        # The plugin keeps duck_time in memory and writes it back, so the fix needs the
        # writer stopped (and it reloads the corrected value when it starts again).
        if duck is None:
            yield Finding("D4", "stop", t.name, f"表里有数据（最新 {latest}），但 watermark.duck_time 为空",
                          "设为最新数据时间")
            continue
        slack = 2 * (spec.period or timedelta(0))
        if duck > latest + slack:
            yield Finding("D4", "stop", t.name,
                          f"watermark.duck_time={duck} 超前于最新数据 {latest}",
                          "校正为最新数据时间（否则 ingestor 会跳过这段缺口）")
        elif latest > duck + slack:
            yield Finding("D4", "hint", t.name,
                          f"最新数据 {latest} 晚于 watermark.duck_time={duck}（loadhist 未同步水位？）")


@rule("D5")
def bloat(state, dp, opts):
    for t in state.business_tables():
        physical, live = storage_rows(dp, t)
        if is_bloated(physical, live, opts):
            yield Finding("D5", "stop", t.name,
                          f"物理存储 {physical:,} 行，有效 {live:,} 行（已删除的行没有回收）",
                          "重建这张表" + ("（它带主键，去主键时一并完成）" if t.keyed else ""))


# ── retention and storage ─────────────────────────────────────────────────────

@rule("R1")
def retention_targets(state, dp, opts):
    for p in state.retention_policies:
        if not p.get("enabled"):
            continue
        t = state.tables.get(p["table_name"])
        if not t:
            yield Finding("R1", "hint", p["table_name"], "retention 策略引用的表不存在")
        elif t.keyed:
            yield Finding("R1", "hint", p["table_name"], "retention 会跳过这张表（带主键），去主键后才生效")


@rule("R2")
def retention_start_gap(state, dp, opts):
    starts = {i.name: i.start_date for i in state.instances if i.start_date}
    candidates = [i.name for i in state.instances]
    for g in startgap.gaps(state.retention_policies, state.watermark, starts, candidates=candidates):
        gap, cost, fix = g.describe()
        yield Finding("R2", "hint", g.table, f"{gap}；{cost}", fix)


@rule("M1")
def memory_limit_vs_host(state, dp, opts):
    limit = memory.parse_size(dp.scalar("SELECT current_setting('memory_limit')"))
    total, avail = memory.host_memory()
    if limit and total and limit > total:
        s = memory.suggest(total, avail, memory.parse_size(dp.scalar("SELECT memory_usage FROM pragma_database_size()")))
        yield Finding("M1", "hint", "duckport-server",
                      f"memory_limit {memory.fmt(limit)} 超过本机内存 {memory.fmt(total)}：DuckDB 不会及时落盘，"
                      "重建这类大操作会被内核 OOM 杀掉（upgrade 的停写窗口会临时降低）",
                      f"把 server.env 的 DUCKPORT_DUCKDB_MEMORY_LIMIT 改为 {memory.mib(s) if s else '本机能给出的一半'}"
                      "（本机现在能给出的一半），重启服务端；`sudo duckport upgrade` 会询问并代改")


@rule("P1")
def free_space(state, dp, opts):
    total, free = state.db_size.get("total_blocks") or 0, state.db_size.get("free_blocks") or 0
    if total and free / total >= opts.free_ratio:
        yield Finding("P1", "hint", "duckport.db",
                      f"空闲块占 {free / total:.0%}（{free:,} / {total:,}），库文件可以压缩",
                      "停服后 COPY FROM DATABASE（见 upgrade-1.1.md）")
