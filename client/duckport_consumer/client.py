"""
DuckportConsumer — Python consumer client for duckport-rs.

Connects to duckport-rs via Arrow Flight and provides high-level query methods
that mirror the old FlightClient interface (get_market / get_symbol / get_exginfo).

**Plan A:** Resample SQL runs only against DuckDB 主表（`data.kline_...`）。`pqt_path`、
`redundancy_hours` 与 Parquet / hybrid 路径已弃用，仅为兼容旧调用保留；传入非默认值时会打日志提示。
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from duckport_consumer.resample import build_duckdb_resample_sql

logger = logging.getLogger("duckport_consumer")


class DuckportConsumer:
    """Consumer client for duckport-rs gRPC database service.

    Args:
        addr: duckport-rs gRPC address (host:port).
        schema: DuckDB schema name where tables live (e.g. "data").
        kline_interval_minutes: Base K-line interval in minutes (default 1).
        suffix: Table name suffix derived from kline_interval (e.g. "_1m").
        pqt_path: *Deprecated (Plan A).* Ignored. Previously used for hybrid
            `read_parquet` + hot table queries; all resample now uses DuckDB only.
        redundancy_hours: *Deprecated (Plan A).* Ignored.
    """

    def __init__(
        self,
        addr: str = "localhost:50051",
        schema: str = "data",
        kline_interval_minutes: int = 1,
        suffix: str = "_1m",
        pqt_path: Optional[str] = None,
        redundancy_hours: int = 1,
    ):
        self._location = f"grpc://{addr}"
        self._client = flight.FlightClient(self._location)
        self._schema = schema
        self._kline_interval_minutes = kline_interval_minutes
        self._suffix = suffix
        self._pqt_path = pqt_path
        self._redundancy_hours = redundancy_hours
        self._plan_a_hybrid_warned = False
        logger.info("DuckportConsumer connected to %s", self._location)
        if pqt_path is not None or redundancy_hours != 1:
            self._log_plan_a_deprecation()

    def _log_plan_a_deprecation(self) -> None:
        if self._plan_a_hybrid_warned:
            return
        self._plan_a_hybrid_warned = True
        logger.warning(
            "Plan A: pqt_path / redundancy_hours 已弃用（混合 Parquet+热表查询已关闭）。"
            "resample 仅查询 DuckDB 主表；可移除这些参数。 "
            f"pqt_path={self._pqt_path!r} redundancy_hours={self._redundancy_hours}"
        )

    # ------------------------------------------------------------------
    # Low-level RPC methods
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pa.Table:
        """Execute arbitrary read-only SQL via the custom duckport.query DoGet.

        Returns the full result as a PyArrow Table.
        """
        ticket_data = json.dumps({"type": "duckport.query", "sql": sql})
        ticket = flight.Ticket(ticket_data.encode("utf-8"))
        reader = self._client.do_get(ticket)
        return reader.read_all()

    def ping(self) -> dict:
        """Health check via duckport.ping DoAction."""
        action = flight.Action("duckport.ping", b"")
        results = list(self._client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def read_duck_time(self, market: str) -> Optional[datetime]:
        """Read the latest duck_time for a market from config_dict."""
        try:
            table = self.query(
                f"SELECT value FROM {self._schema}.config_dict "
                f"WHERE key = '{market}_duck_time'"
            )
            if table.num_rows == 0:
                return None
            ts_str = table.column("value")[0].as_py()
            return pd.to_datetime(ts_str).tz_localize(tz=timezone.utc)
        except Exception as e:
            logger.warning("Failed to read duck_time for %s: %s", market, e)
            return None

    # ------------------------------------------------------------------
    # High-level query methods (compatible with old FlightClient)
    # ------------------------------------------------------------------

    def get_market(
        self,
        market: str,
        interval: int = 60,
        offset: int = 0,
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pa.Table:
        """Resample K-line data for all symbols in a market.

        Mirrors old FlightClient.get_market().
        """
        return self._resample_query(market, interval, offset, begin, end)

    def get_symbol(
        self,
        market: str,
        symbol: str,
        interval: int = 60,
        offset: int = 0,
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pa.Table:
        """Resample K-line data for a single symbol.

        Mirrors old FlightClient.get_symbol().
        """
        return self._resample_query(market, interval, offset, begin, end, symbol)

    def get_exginfo(self, market: str) -> pa.Table:
        """Fetch exchange info for a market.

        Mirrors old FlightClient.get_exginfo().
        """
        if market not in ("usdt_perp", "usdt_spot"):
            raise ValueError(f"Unsupported market: {market}")
        return self.query(
            f"SELECT * FROM {self._schema}.exginfo WHERE market = '{market}'"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resample_query(
        self,
        market: str,
        interval: int,
        offset: int,
        begin: Optional[datetime],
        end: Optional[datetime],
        symbol: Optional[str] = None,
    ) -> pa.Table:
        if self._pqt_path is not None or self._redundancy_hours != 1:
            self._log_plan_a_deprecation()
        if market not in ("usdt_perp", "usdt_spot"):
            raise ValueError(f"Unsupported market: {market}")
        if interval <= 0:
            raise ValueError(f"interval must be positive, got {interval}")
        if 60 % interval != 0:
            raise ValueError(f"interval {interval} must divide 60 evenly")
        if interval % self._kline_interval_minutes != 0:
            raise ValueError(
                f"interval {interval} must be a multiple of "
                f"kline_interval_minutes ({self._kline_interval_minutes})"
            )
        if offset >= interval or offset % self._kline_interval_minutes != 0:
            raise ValueError(
                f"Offset must be a multiple of {self._kline_interval_minutes} "
                f"and less than interval ({interval})"
            )

        if begin is None:
            begin = datetime.now(tz=timezone.utc) - timedelta(days=90)
        elif isinstance(begin, str):
            begin = pd.to_datetime(begin)
            begin = begin.tz_convert(timezone.utc) if begin.tzinfo is not None else begin.tz_localize(timezone.utc)
        if end is None:
            end = datetime.now(tz=timezone.utc)
        elif isinstance(end, str):
            end = pd.to_datetime(end)
            end = end.tz_convert(timezone.utc) if end.tzinfo is not None else end.tz_localize(timezone.utc)

        table_name = f"{self._schema}.{market}{self._suffix}"

        logger.info(
            "resample (Plan A: DuckDB only) market=%s interval=%d begin=%s end=%s symbol=%s",
            market, interval, begin, end, symbol,
        )

        sql = build_duckdb_resample_sql(
            table_name,
            interval,
            offset,
            begin,
            end,
            self._kline_interval_minutes,
            symbol,
        )
        try:
            return self.query(sql)
        except Exception as e:
            logger.error("resample query failed: %s", e)
            raise

    def close(self):
        """Close the Flight client connection."""
        try:
            self._client.close()
            logger.info("DuckportConsumer connection closed")
        except Exception as e:
            logger.warning("Error closing connection: %s", e)
