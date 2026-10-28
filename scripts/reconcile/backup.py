"""Backup taken at the start of the stop window, and the disk/memory it needs.

The backup is `COPY FROM DATABASE` into a new database file next to the live one: a
consistent copy taken by the server itself (ingestors are stopped, the server keeps
running). Restoring it is a file swap with the server stopped.

Costs measured on pro.studio (DuckDB 1.5.6): tables without indexes copy at ~2 GB/s with
little memory; tables with a PRIMARY KEY rebuild their index in the copy, ~1.5M rows/s
and ~50–100 bytes of memory per row.
"""

import os
import re
import shutil
from dataclasses import dataclass, field
from typing import List, Optional

from duckport_flight import lit, q
from reconcile.rules import Finding
from reconcile.state import State

COPY_BYTES_PER_S = 2e9
KEYED_ROWS_PER_S = 1.5e6
INDEX_BYTES_PER_ROW = 100
BASE_MEMORY = 1 << 30
# Rebuilding 1.96 billion keyed rows on pro.studio (2026-10-26): 15M rows/s, and the
# constraint-free copy took 0.57 of the space the keyed tables used (no index, no deleted rows).
REBUILD_ROWS_PER_S = 15e6
KEYED_DATA_SHARE = 0.6
_UNITS = {"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30,
          "TIB": 1 << 40, "BYTES": 1}


@dataclass
class Resources:
    free: Optional[int] = None          # bytes free on the database's filesystem
    reusable: int = 0                   # free blocks inside the file: DuckDB fills these before growing it
    rebuild_bytes: int = 0              # extra space the rebuilds need while old and new coexist
    compact_bytes: int = 0              # what a rewrite of the live data would need on disk afterwards
    rebuild_rows: int = 0
    backup_bytes: int = 0
    backup_memory: int = 0
    backup_seconds: float = 0.0
    memory_available: Optional[int] = None
    warnings: List[str] = field(default_factory=list)   # these make the backup impossible
    notes: List[str] = field(default_factory=list)      # worth knowing, change nothing
    fatal: Optional[str] = None

    @property
    def backup_possible(self):
        return not self.warnings

    def window_seconds(self, with_backup):
        secs = 10 + self.rebuild_rows / REBUILD_ROWS_PER_S
        return secs + (self.backup_seconds if with_backup else 0)


def _parse_size(text) -> Optional[int]:
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text or "")
    if not m or m.group(2).upper() not in _UNITS:
        return None
    return int(float(m.group(1)) * _UNITS[m.group(2).upper()])


def gb(n) -> str:
    return f"{n / 1e9:.1f} GB"


def _host_memory_available() -> Optional[int]:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def disk_plan(free: int, reusable: int, rebuild: int, backup: int):
    """(fatal, backup_warning) for the stop window. The rebuilds grow the file only by what
    the free blocks inside it cannot absorb; the backup is a separate file and needs disk."""
    need = max(0, rebuild - reusable)
    inside = f"，库文件内可复用 {gb(reusable)}" if reusable else ""
    if free < need:
        return (f"磁盘不足：重建需要约 {gb(rebuild)}（新旧两份数据同时存在）{inside}，"
                f"磁盘空闲只有 {gb(free)}"), None
    if free < need + backup:
        return None, (f"磁盘不足以备份：备份约 {gb(backup)} + 重建约 {gb(need)}"
                      f"{'（已扣除库文件内可复用的 ' + gb(reusable) + '）' if reusable else ''}，空闲只有 {gb(free)}")
    return None, None


def estimate(dp, state: State, findings: List[Finding], db_path: str) -> Resources:
    res = Resources()
    size = state.db_size
    block_size = dp.scalar(
        "SELECT block_size FROM pragma_database_size() WHERE database_name = current_database()") or 0
    used = (size.get("used_blocks") or 0) * block_size
    # Blocks freed by earlier DROPs (a rolled-back rebuild, retention) stay in the file and are
    # handed out before it grows: rmt.ctb2 had 20 GB of them and 7 GB of disk, which is enough.
    res.reusable = (size.get("free_blocks") or 0) * block_size
    # estimated_size also counts deleted rows; copies carry only the live ones
    physical_total = sum(t.estimated_size for t in state.tables.values()) or 1
    live = {}

    def live_rows(name):
        if name not in live:
            live[name] = dp.scalar(f"SELECT count(*) FROM data.{q(name)}") or 0
        return live[name]

    rebuilt = sorted({f.target for f in findings if f.rule in ("B1", "D5") and f.level == "stop"} & set(state.tables))
    res.rebuild_rows = sum(live_rows(t) for t in rebuilt)
    res.rebuild_bytes = int(1.1 * sum(
        used * live_rows(t) / physical_total * (KEYED_DATA_SHARE if state.tables[t].keyed else 1.0)
        for t in rebuilt))

    # A later `upgrade --compact` rewrites the live data into a new file next to the old one,
    # so it needs that much free disk -- free blocks inside the file do not help there.
    res.compact_bytes = int(1.1 * sum(
        used * live_rows(t.name) / physical_total * (KEYED_DATA_SHARE if t.keyed else 1.0)
        for t in state.business_tables()))

    keyed_rows = sum(live_rows(t.name) for t in state.business_tables() if t.keyed)
    res.backup_bytes = used
    res.backup_memory = BASE_MEMORY + keyed_rows * INDEX_BYTES_PER_ROW
    res.backup_seconds = (used / COPY_BYTES_PER_S) + keyed_rows / KEYED_ROWS_PER_S

    if db_path and os.path.exists(os.path.dirname(os.path.abspath(db_path))):
        res.free = shutil.disk_usage(os.path.dirname(os.path.abspath(db_path))).free
    limit = _parse_size(dp.scalar("SELECT current_setting('memory_limit')"))
    host = _host_memory_available()
    known = [m for m in (limit, host) if m]
    res.memory_available = min(known) if known else None

    if res.free is not None:
        fatal, warning = disk_plan(res.free, res.reusable, res.rebuild_bytes, res.backup_bytes)
        res.fatal = fatal
        if warning:
            res.warnings.append(warning)
        if res.compact_bytes and res.free < res.compact_bytes:
            res.notes.append(f"重建后的压缩要在旁边重写一份存活数据，约需 {gb(res.compact_bytes)} 空闲磁盘，"
                             f"现在只有 {gb(res.free)}：到时会跳过压缩（库照常可用，空闲块留给写入复用），"
                             "腾出空间后 sudo duckport upgrade --compact")
    if res.memory_available is not None and res.memory_available < res.backup_memory:
        res.warnings.append(f"内存不足以备份：带主键的表 {keyed_rows:,} 行，复制时重建索引约需 "
                            f"{gb(res.backup_memory)}，可用 {gb(res.memory_available)}"
                            "（服务端 memory_limit 与本机可用内存取小）")
    return res


def take(dp, path: str) -> str:
    """COPY FROM DATABASE into a new file at `path`, then compare every table's row count."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        raise RuntimeError(f"{path} 已存在")
    db = dp.scalar("SELECT current_database()")
    dp.execute(f"ATTACH {lit(path)} AS upgrade_backup")
    try:
        dp.execute(f"COPY FROM DATABASE {q(db)} TO upgrade_backup")
        tables = dp.query(f"SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = {lit(db)}")
        copied = {(r["schema_name"], r["table_name"]) for r in dp.query(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'upgrade_backup'")}
        missing = [f"{r['schema_name']}.{r['table_name']}" for r in tables
                   if (r["schema_name"], r["table_name"]) not in copied]
        if missing:
            raise RuntimeError("备份里缺少表：" + ", ".join(missing))
        diff = dp.query(" UNION ALL ".join(
            f"SELECT {lit(r['schema_name'] + '.' + r['table_name'])} AS t, "
            f"(SELECT count(*) FROM {q(db)}.{q(r['schema_name'])}.{q(r['table_name'])}) AS a, "
            f"(SELECT count(*) FROM upgrade_backup.{q(r['schema_name'])}.{q(r['table_name'])}) AS b"
            for r in tables))
    finally:
        dp.execute("DETACH upgrade_backup")
    bad = [f"{r['t']} {r['a']} vs {r['b']}" for r in diff if r["a"] != r["b"]]
    if bad:
        raise RuntimeError("备份行数不一致：" + "; ".join(bad))
    return f"{len(tables)} 张表、{sum(r['a'] for r in diff):,} 行，逐表核对一致"
