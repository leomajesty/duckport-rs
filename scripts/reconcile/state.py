"""Snapshot of a duckport database (and the plugin instances writing to it).

Metadata is read once; rules that need to scan table data query the server themselves.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from reconcile.specs import LEFTOVER_SUFFIXES, RETIRED_TABLES, SYSTEM_TABLES, TableSpec, builtin_spec


@dataclass
class Instance:
    name: str
    exec: str
    version: Optional[str] = None
    active: bool = False


@dataclass
class Table:
    name: str
    columns: Dict[str, str]          # column -> type, in table order
    keyed: bool                      # has PRIMARY KEY / UNIQUE / an index
    estimated_size: int


@dataclass
class State:
    server_version: str
    tables: Dict[str, Table]                       # schema data
    staging: Dict[str, int]                        # schema main, _staging_* -> estimated rows
    config: Dict[str, str]
    watermark: Dict[str, dict]
    retention_policies: List[dict]
    db_size: dict
    instances: List[Instance] = field(default_factory=list)
    write_modes: Dict[str, str] = field(default_factory=dict)

    def business_tables(self) -> List[Table]:
        skip = set(SYSTEM_TABLES) | set(RETIRED_TABLES)
        return [t for name, t in sorted(self.tables.items())
                if name not in skip and not name.endswith(LEFTOVER_SUFFIXES)]

    def spec(self, table: str) -> Optional[TableSpec]:
        return builtin_spec(table)

    def writers(self, table: str) -> List[Instance]:
        """Plugin instances that write `table`: the watermark owner, or for tables
        without a watermark row (snapshot tables) every instance of the spec's plugin."""
        owner = (self.watermark.get(table) or {}).get("ingestor")
        if owner:
            hits = [i for i in self.instances if i.name == owner]
            if hits:
                return hits
        spec = self.spec(table)
        if spec:
            return [i for i in self.instances if i.exec == spec.plugin]
        return []


def collect(dp, instances: List[Instance]) -> State:
    ping = dp.action("duckport.ping")
    cols: Dict[str, Dict[str, str]] = {}
    for r in dp.query(
        "SELECT table_name, column_name, data_type FROM duckdb_columns() "
        "WHERE schema_name = 'data' ORDER BY table_name, column_index"
    ):
        cols.setdefault(r["table_name"], {})[r["column_name"]] = r["data_type"]
    keyed = {r["table_name"] for r in dp.query(
        "SELECT table_name FROM duckdb_constraints() WHERE schema_name = 'data' "
        "AND constraint_type IN ('PRIMARY KEY', 'UNIQUE') "
        "UNION SELECT table_name FROM duckdb_indexes() WHERE schema_name = 'data'")}
    tables = {}
    for r in dp.query("SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'data'"):
        name = r["table_name"]
        tables[name] = Table(name, cols.get(name, {}), name in keyed, r["estimated_size"] or 0)
    staging = {r["table_name"]: r["estimated_size"] or 0 for r in dp.query(
        "SELECT table_name, estimated_size FROM duckdb_tables() "
        "WHERE schema_name = 'main' AND starts_with(table_name, '_staging_')")}

    config = {}
    if "config_dict" in tables:
        config = {r["key"]: r["value"] for r in dp.query("SELECT key, value FROM data.config_dict")}
    watermark = {}
    if "watermark" in tables:
        wcols = tables["watermark"].columns
        select = ", ".join(c if c in wcols else f"NULL AS {c}" for c in SYSTEM_TABLES["watermark"])
        watermark = {r["table_name"]: r for r in dp.query(f"SELECT {select} FROM data.watermark")}
    policies = []
    if "retention_policy" in tables:
        policies = dp.query("SELECT table_name, keep_days, enabled FROM data.retention_policy")
    size = (dp.query("SELECT total_blocks, used_blocks, free_blocks FROM pragma_database_size() "
                     "WHERE database_name = current_database()") or [{}])[0]
    write_modes = {k.split(":", 1)[1]: v for k, v in config.items() if k.startswith("write_mode:")}
    return State(
        server_version=ping.get("server_version", ""), tables=tables, staging=staging,
        config=config, watermark=watermark, retention_policies=policies, db_size=size,
        instances=instances, write_modes=write_modes,
    )


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
