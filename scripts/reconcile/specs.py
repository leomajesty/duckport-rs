"""Desired state of a duckport database for the current release.

Only the latest state is described here; there is no per-version upgrade logic. Older
databases are recognised by what they contain, not by a recorded version.
"""

import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, Optional, Tuple

SERVER_MIN_VERSION = "1.1.0"

# Lowest plugin version (by console-script name, as in registry.json `exec`) that writes
# with the anti-join dedup and therefore works on tables without a primary key.
PLUGIN_MIN_VERSION = {
    "binance-ingestor": "0.2.0",
    "binance-funding": "0.2.0",
}

# System tables owned by duckport-server: name -> {column: type}.
SYSTEM_TABLES: Dict[str, Dict[str, str]] = {
    "config_dict": {"key": "VARCHAR", "value": "VARCHAR"},
    "watermark": {
        "table_name": "VARCHAR", "ingestor": "VARCHAR", "max_lag_seconds": "INTEGER",
        "time_column": "VARCHAR", "start_time": "TIMESTAMP", "duck_time": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "retention_policy": {
        "table_name": "VARCHAR", "keep_days": "INTEGER", "batch_days": "INTEGER",
        "archive_dir": "VARCHAR", "enabled": "BOOLEAN", "updated_at": "TIMESTAMP",
    },
    "retention_log": {
        "run_id": "VARCHAR", "table_name": "VARCHAR", "cutoff": "DATE", "first_day": "DATE",
        "last_day": "DATE", "rows_archived": "BIGINT", "rows_deleted": "BIGINT",
        "dry_run": "BOOLEAN", "free_blocks_before": "BIGINT", "free_blocks_after": "BIGINT",
        "status": "VARCHAR", "error": "VARCHAR", "started_at": "TIMESTAMP", "finished_at": "TIMESTAMP",
    },
}

# DDL the server runs at startup (crates/duckport-server/src/main.rs init_system_schema and
# retention.rs SYSTEM_SCHEMA_SQL); used to recreate a missing system table without a restart.
SYSTEM_DDL: Dict[str, str] = {
    "config_dict": "CREATE TABLE IF NOT EXISTS data.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)",
    "watermark": (
        "CREATE TABLE IF NOT EXISTS data.watermark (table_name VARCHAR PRIMARY KEY, ingestor VARCHAR, "
        "max_lag_seconds INTEGER, time_column VARCHAR NOT NULL, start_time TIMESTAMP, duck_time TIMESTAMP, "
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    ),
    "retention_policy": (
        "CREATE TABLE IF NOT EXISTS data.retention_policy (table_name VARCHAR PRIMARY KEY, "
        "keep_days INTEGER NOT NULL, batch_days INTEGER NOT NULL DEFAULT 1, archive_dir VARCHAR, "
        "enabled BOOLEAN NOT NULL DEFAULT true, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    ),
    "retention_log": (
        "CREATE TABLE IF NOT EXISTS data.retention_log (run_id VARCHAR, table_name VARCHAR, cutoff DATE, "
        "first_day DATE, last_day DATE, rows_archived BIGINT, rows_deleted BIGINT, dry_run BOOLEAN, "
        "free_blocks_before BIGINT, free_blocks_after BIGINT, status VARCHAR, error VARCHAR, "
        "started_at TIMESTAMP, finished_at TIMESTAMP)"
    ),
}
# Defaults for system columns added to an existing table (others are added as plain NULL columns).
SYSTEM_COLUMN_DEFAULTS = {("watermark", "updated_at"): "CURRENT_TIMESTAMP"}

# Written by `duckport upgrade` itself; not a business table, not checked by S1.
UPGRADE_LOG_DDL = (
    "CREATE TABLE IF NOT EXISTS data.upgrade_log (run_id VARCHAR, phase VARCHAR, rule VARCHAR, "
    "target VARCHAR, action VARCHAR, status VARCHAR, rows BIGINT, detail VARCHAR, "
    "started_at TIMESTAMP, finished_at TIMESTAMP)"
)
TOOL_TABLES = {"upgrade_log"}

# Tables that must not exist any more.
RETIRED_TABLES = {"retention_tasks": "1.1.0 起由 data.retention_policy 取代"}

# config_dict keys that must not exist any more: pattern -> reason.
RETIRED_CONFIG_KEYS = {
    r"^(?P<market>[a-z0-9_]+)_duck_time$": "duck_time 已移到 data.watermark",
    r"^.+_pqt_time$": "beta_v2.2 的 Parquet 切分水位，duckport-rs 不再使用",
}

LEFTOVER_SUFFIXES = ("__pk_old", "__nopk")


@dataclass(frozen=True)
class StringCheck:
    max_len: int
    pattern: Optional[str] = None  # full-match regex (DuckDB RE2 syntax)


@dataclass(frozen=True)
class TableSpec:
    table: str
    plugin: str                         # console-script name of the writer plugin
    kind: str                           # "append" | "snapshot"
    columns: Dict[str, str]             # expected columns -> type
    dedup_key: Tuple[str, ...]
    time_column: Optional[str] = None   # append tables
    period: Optional[timedelta] = None  # expected spacing of time_column values
    string_checks: Dict[str, StringCheck] = field(default_factory=dict)
    legacy_columns: Tuple[str, ...] = ()  # columns older plugin versions had
    column_defaults: Dict[str, str] = field(default_factory=dict)  # used when a missing column is added
    source: str = "builtin"


KLINE_COLUMNS = {
    "open_time": "TIMESTAMP", "symbol": "VARCHAR", "open": "DOUBLE", "high": "DOUBLE",
    "low": "DOUBLE", "close": "DOUBLE", "volume": "DOUBLE", "quote_volume": "DOUBLE",
    "trade_num": "INTEGER", "taker_buy_base_asset_volume": "DOUBLE",
    "taker_buy_quote_asset_volume": "DOUBLE", "avg_price": "DOUBLE",
}
EXGINFO_COLUMNS = {
    "market": "VARCHAR", "symbol": "VARCHAR", "status": "VARCHAR", "base_asset": "VARCHAR",
    "quote_asset": "VARCHAR", "price_tick": "VARCHAR", "lot_size": "VARCHAR",
    "min_notional_value": "VARCHAR", "contract_type": "VARCHAR", "margin_asset": "VARCHAR",
    "pre_market": "BOOLEAN", "created_time": "TIMESTAMP", "underlying_type": "VARCHAR",
}
FUNDING_COLUMNS = {
    "funding_time": "TIMESTAMP", "symbol": "VARCHAR", "funding_rate": "DOUBLE", "mark_price": "DOUBLE",
}

# Symbols can contain CJK characters (e.g. 币安人生USDT), so only the length is checked;
# corruption from duckdb#22279 glues many values together and blows past it.
SYMBOL_CHECK = StringCheck(max_len=40)
MARKET_CHECK = StringCheck(max_len=32, pattern=r"[a-z0-9_]{1,32}")

KLINE_TABLE = re.compile(r"^(usdt_perp|usdt_spot)_(\d+)([mhd])$")
_UNITS = {"m": "minutes", "h": "hours", "d": "days"}


def builtin_spec(table: str) -> Optional[TableSpec]:
    """Spec for a table written by a known plugin, or None."""
    m = KLINE_TABLE.match(table)
    if m:
        period = timedelta(**{_UNITS[m.group(3)]: int(m.group(2))})
        return TableSpec(
            table=table, plugin="binance-ingestor", kind="append", columns=KLINE_COLUMNS,
            dedup_key=("open_time", "symbol"), time_column="open_time", period=period,
            string_checks={"symbol": SYMBOL_CHECK},
        )
    if table == "exginfo":
        return TableSpec(
            table=table, plugin="binance-ingestor", kind="snapshot", columns=EXGINFO_COLUMNS,
            dedup_key=("market", "symbol"),
            string_checks={"market": MARKET_CHECK, "symbol": StringCheck(max_len=64),
                           "status": StringCheck(max_len=32)},
            column_defaults={"created_time": "CURRENT_TIMESTAMP"},
        )
    if table == "usdt_perp_funding":
        return TableSpec(
            table=table, plugin="binance-funding", kind="append", columns=FUNDING_COLUMNS,
            dedup_key=("funding_time", "symbol"), time_column="funding_time",
            period=timedelta(hours=8), string_checks={"symbol": SYMBOL_CHECK},
            legacy_columns=("funding_interval_hours", "source"),
        )
    return None


def parse_version(v: Optional[str]) -> Tuple[int, ...]:
    """'0.2.0' -> (0, 2, 0); unknown or malformed -> ()."""
    if not v:
        return ()
    parts = []
    for p in re.split(r"[.+-]", v):
        if not p.isdigit():
            break
        parts.append(int(p))
    return tuple(parts)


def version_at_least(v: Optional[str], minimum: str) -> Optional[bool]:
    """None when v is unknown."""
    have = parse_version(v)
    if not have:
        return None
    return have >= parse_version(minimum)
