"""
DuckportClient — Python client for duckport-rs gRPC database service.

Wraps Arrow Flight RPCs into a typed API:
  DoAction : duckport.ping / execute / execute_transaction
  DoPut    : duckport.append  (bulk Arrow ingestion)
  DoGet    : Airport-protocol table scans

Write methods use a *staging-table* pattern to work around DuckDB Appender's lack
of ON CONFLICT support:
  1. DoPut -> staging table (no PK)
  2. execute_transaction -> INSERT INTO target ON CONFLICT DO NOTHING + watermark
  3. Truncate staging
"""

import json
from datetime import timezone
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from binance_ingestor.utils.log_kit import logger


class DuckportClient:

    KLINE_COL_DEFS = [
        "open_time TIMESTAMP",
        "symbol VARCHAR",
        "open DOUBLE",
        "high DOUBLE",
        "low DOUBLE",
        "close DOUBLE",
        "volume DOUBLE",
        "quote_volume DOUBLE",
        "trade_num INT",
        "taker_buy_base_asset_volume DOUBLE",
        "taker_buy_quote_asset_volume DOUBLE",
        "avg_price DOUBLE",
    ]

    EXGINFO_COL_NAMES = [
        "market", "symbol", "status", "base_asset", "quote_asset",
        "price_tick", "lot_size", "min_notional_value",
        "contract_type", "margin_asset", "pre_market",
    ]

    EXGINFO_COL_DEFS = [
        "market VARCHAR",
        "symbol VARCHAR",
        "status VARCHAR",
        "base_asset VARCHAR",
        "quote_asset VARCHAR",
        "price_tick VARCHAR",
        "lot_size VARCHAR",
        "min_notional_value VARCHAR",
        "contract_type VARCHAR",
        "margin_asset VARCHAR",
        "pre_market BOOLEAN",
    ]

    EXGINFO_ARROW_SCHEMA = pa.schema([
        ("market", pa.string()),
        ("symbol", pa.string()),
        ("status", pa.string()),
        ("base_asset", pa.string()),
        ("quote_asset", pa.string()),
        ("price_tick", pa.string()),
        ("lot_size", pa.string()),
        ("min_notional_value", pa.string()),
        ("contract_type", pa.string()),
        ("margin_asset", pa.string()),
        ("pre_market", pa.bool_()),
    ])

    def __init__(self, addr: str = "localhost:50051", schema: str = "data"):
        self.location = f"grpc://{addr}"
        self.schema = schema
        self.client = flight.FlightClient(self.location)
        logger.info(f"DuckportClient connected: {self.location}, schema={schema}")

    # ── Low-level RPCs ──────────────────────────────────────────────

    def ping(self) -> dict:
        action = flight.Action("duckport.ping", b"")
        results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute(self, sql: str) -> dict:
        body = json.dumps({"sql": sql}).encode("utf-8")
        action = flight.Action("duckport.execute", body)
        results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute_transaction(self, statements: List[str]) -> dict:
        body = json.dumps({"statements": statements}).encode("utf-8")
        action = flight.Action("duckport.execute_transaction", body)
        results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        descriptor = flight.FlightDescriptor.for_path(
            "duckport.append", schema, table,
        )
        writer, reader = self.client.do_put(descriptor, data.schema)
        writer.write_table(data)
        writer.done_writing()
        buf = reader.read()
        resp = json.loads(bytes(buf))
        writer.close()
        return resp

    def read_table(self, schema: str, table: str) -> pa.Table:
        ticket_json = json.dumps({"schema": schema, "table": table}).encode("utf-8")
        reader = self.client.do_get(flight.Ticket(ticket_json))
        return reader.read_all()

    # ── Schema initialisation ───────────────────────────────────────

    def init_schema(
        self,
        markets: List[str],
        interval: str,
        data_sources: set,
    ):
        s = self.schema
        stmts: List[str] = [f"CREATE SCHEMA IF NOT EXISTS {s}"]

        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {s}.config_dict "
            f"(key VARCHAR PRIMARY KEY, value VARCHAR)"
        )

        kline_cols = ", ".join(self.KLINE_COL_DEFS)
        for market in markets:
            if market not in data_sources:
                continue
            target = f"{market}_{interval}"
            stmts.append(
                f"CREATE TABLE IF NOT EXISTS {s}.{target} "
                f"({kline_cols}, PRIMARY KEY (open_time, symbol))"
            )
            stmts.append(
                f"CREATE TABLE IF NOT EXISTS _staging_{target} ({kline_cols})"
            )

        exginfo_cols = ", ".join(self.EXGINFO_COL_DEFS)
        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {s}.exginfo "
            f"({exginfo_cols}, "
            f"created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            f"PRIMARY KEY (market, symbol))"
        )
        stmts.append(
            f"CREATE TABLE IF NOT EXISTS _staging_exginfo ({exginfo_cols})"
        )

        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {s}.retention_tasks ("
            f"market VARCHAR NOT NULL, "
            f"interval VARCHAR NOT NULL, "
            f"schema_name VARCHAR NOT NULL, "
            f"retention_days INTEGER NOT NULL DEFAULT 7, "
            f"parquet_dir VARCHAR NOT NULL, "
            f"start_date VARCHAR NOT NULL DEFAULT '2009-01-03', "
            f"file_period INTEGER NOT NULL DEFAULT 1, "
            f"enabled BOOLEAN NOT NULL DEFAULT true, "
            f"updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            f"PRIMARY KEY (market, interval))"
        )

        self.execute_transaction(stmts)
        logger.info(f"Schema '{s}' initialised on duckport-rs")

    def verify_kline_interval(self, interval: str):
        s = self.schema
        self.execute(
            f"INSERT INTO {s}.config_dict (key, value) "
            f"VALUES ('kline_interval', '{interval}') "
            f"ON CONFLICT(key) DO NOTHING"
        )
        table = self.read_table(s, "config_dict")
        df = table.to_pandas()
        row = df[df["key"] == "kline_interval"]
        if row.empty:
            raise ValueError("config_dict: kline_interval not found after insert")
        stored = row["value"].iloc[0]
        if stored != interval:
            raise ValueError(
                f"kline interval mismatch! config={interval}, db={stored}"
            )
        logger.info("kline interval consistency check passed")

    # ── High-level write operations ─────────────────────────────────

    def write_kline(
        self,
        df: pd.DataFrame,
        market: str,
        interval: str,
        current_time,
    ) -> dict:
        s = self.schema
        target = f"{s}.{market}_{interval}"
        staging = f"_staging_{market}_{interval}"

        df = df.copy()
        if hasattr(df["open_time"].dt, "tz") and df["open_time"].dt.tz is not None:
            df["open_time"] = df["open_time"].dt.tz_localize(None)

        arrow_table = pa.Table.from_pandas(df, preserve_index=False)

        self.execute(f"TRUNCATE {staging}")
        append_resp = self.append("main", staging, arrow_table)

        ts = pd.to_datetime(current_time).strftime("%Y-%m-%d %H:%M:%S")
        tx_resp = self.execute_transaction([
            f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING",
            f"INSERT OR REPLACE INTO {s}.config_dict (key, value) "
            f"VALUES ('{market}_duck_time', '{ts}')",
            f"TRUNCATE {staging}",
        ])

        rows_appended = append_resp.get("rows_appended", "?")
        logger.info(
            f"write_kline: {market} staged={rows_appended}, "
            f"tx={tx_resp.get('rows_affected')}"
        )
        return tx_resp

    def replace_duck_time(self, market: str, duck_time_str: str) -> None:
        """Set ``{market}_duck_time`` watermark in ``config_dict`` (after bulk load)."""
        s = self.schema
        self.execute_transaction([
            f"INSERT OR REPLACE INTO {s}.config_dict (key, value) "
            f"VALUES ('{market}_duck_time', '{duck_time_str}')",
        ])

    def bulk_write_kline(
        self,
        arrow_table: pa.Table,
        market: str,
        interval: str,
        duck_time_str: str,
        chunk_size: int = 200_000,
        *,
        sync_duck_time: bool = True,
    ) -> dict:
        """Bulk-load historical kline data using the staging-table pattern with chunking.

        When ``sync_duck_time`` is False (e.g. multi-file Plan A loadhist), only rows are
        inserted; call :meth:`replace_duck_time` once after all chunks/files.
        """
        s = self.schema
        target = f"{s}.{market}_{interval}"
        staging = f"_staging_{market}_{interval}"
        total = len(arrow_table)

        for offset in range(0, total, chunk_size):
            length = min(chunk_size, total - offset)
            chunk = arrow_table.slice(offset, length)
            self.execute(f"TRUNCATE {staging}")
            self.append("main", staging, chunk)
            self.execute_transaction([
                f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING",
                f"TRUNCATE {staging}",
            ])
            logger.info(
                f"bulk_write_kline: {market} chunk {offset}–{offset + length} / {total}"
            )

        if sync_duck_time:
            self.replace_duck_time(market, duck_time_str)
        logger.info(
            f"bulk_write_kline: {market} done, total={total}, "
            f"duck_time={'synced' if sync_duck_time else 'deferred'} ({duck_time_str})"
        )
        return {"total_rows": total}

    def save_exginfo(self, df: pd.DataFrame, market: str):
        s = self.schema
        cols = self.EXGINFO_COL_NAMES
        sub = df[cols].copy()
        for c in ("price_tick", "lot_size", "min_notional_value"):
            sub[c] = sub[c].astype(str)
        arrow_table = pa.Table.from_pandas(
            sub, schema=self.EXGINFO_ARROW_SCHEMA, preserve_index=False,
        )

        self.execute("TRUNCATE _staging_exginfo")
        self.append("main", "_staging_exginfo", arrow_table)

        col_list = ", ".join(cols)
        self.execute_transaction([
            f"DELETE FROM {s}.exginfo WHERE market = '{market}'",
            f"INSERT INTO {s}.exginfo ({col_list}) "
            f"SELECT {col_list} FROM _staging_exginfo",
            "TRUNCATE _staging_exginfo",
        ])
        logger.info(f"save_exginfo: {market}, {len(df)} records")

    def execute_retention(
        self,
        market: str,
        interval: str,
        pqt_file_path: str,
        period_start: str,
        period_end: str,
    ):
        from binance_ingestor.config import RETENTION_ENABLED

        if not RETENTION_ENABLED:
            logger.info(
                "execute_retention: skipped (RETENTION_ENABLED=false, Plan A — "
                "hot data stays in DuckDB)"
            )
            return

        s = self.schema
        target = f"{s}.{market}_{interval}"
        self.execute(
            f"COPY (SELECT * FROM {target} "
            f"WHERE open_time >= '{period_start}' AND open_time < '{period_end}' "
            f"ORDER BY open_time) "
            f"TO '{pqt_file_path}' (FORMAT parquet)"
        )
        self.execute(
            f"DELETE FROM {target} "
            f"WHERE open_time < ('{period_end}'::timestamp - INTERVAL '1 day')"
        )
        logger.info(f"retention: {market} exported {pqt_file_path}")

    def register_retention_task(
        self,
        market: str,
        interval: str,
        parquet_dir: str,
        retention_days: int = 7,
        start_date: str = '2009-01-03',
        file_period: int = 1,
        enabled: bool = True,
    ):
        """Upsert a row in ``data.retention_tasks`` (server-side scheduler must also be on)."""
        s = self.schema
        en = "true" if enabled else "false"
        self.execute(
            f"INSERT OR REPLACE INTO {s}.retention_tasks "
            f"(market, interval, schema_name, retention_days, parquet_dir, "
            f"start_date, file_period, enabled, updated_at) "
            f"VALUES ('{market}', '{interval}', '{s}', {retention_days}, "
            f"'{parquet_dir}', '{start_date}', {file_period}, {en}, CURRENT_TIMESTAMP)"
        )
        logger.info(
            f"retention_task registered: {market}/{interval} → {parquet_dir} (enabled={enabled})"
        )

    # ── Read helpers ────────────────────────────────────────────────

    def read_duck_time(self, market: str) -> Optional[pd.Timestamp]:
        try:
            table = self.read_table(self.schema, "config_dict")
            df = table.to_pandas()
            row = df[df["key"] == f"{market}_duck_time"]
            if row.empty:
                return None
            return pd.to_datetime(row["value"].iloc[0]).tz_localize(timezone.utc)
        except Exception as e:
            logger.warning(f"read_duck_time({market}): {e}")
            return None

    def read_duck_times(self, markets: List[str]) -> Dict[str, Optional[pd.Timestamp]]:
        result: Dict[str, Optional[pd.Timestamp]] = {}
        try:
            table = self.read_table(self.schema, "config_dict")
            df = table.to_pandas()
            for market in markets:
                row = df[df["key"] == f"{market}_duck_time"]
                if row.empty:
                    result[market] = None
                else:
                    result[market] = pd.to_datetime(
                        row["value"].iloc[0]
                    ).tz_localize(timezone.utc)
        except Exception as e:
            logger.warning(f"read_duck_times: {e}")
            for market in markets:
                result.setdefault(market, None)
        return result

    def close(self):
        self.client.close()
        logger.info("DuckportClient closed")
