"""
SQL builders for K-line resample queries.

Migrated from duckport/core/flight_func/flight_api.py (FlightGets).
All SQL is constructed here and sent to duckport-rs via the `duckport.query`
DoGet ticket for server-side execution.

**Plan A:** 仅使用 :func:`build_duckdb_resample_sql` 查询热库主表（无 Parquet / hybrid / pqt_time）。
"""

from datetime import datetime
from typing import Optional

RESAMPLE_COLUMNS = """
    DATE_TRUNC('minute',
        open_time - (EXTRACT(MINUTE FROM (open_time - INTERVAL '{offset} minute'))
                     % {interval}) * INTERVAL '1 minute'
    ) AS resample_time,
    first(open ORDER BY open_time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close ORDER BY open_time) AS close,
    sum(volume) AS volume,
    sum(quote_volume) AS quote_volume,
    sum(trade_num) AS trade_num,
    sum(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume,
    sum(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume,
    first(avg_price ORDER BY open_time) AS avg_price,
    symbol"""


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _build_resample_fragment(
    source: str,
    interval: int,
    offset: int,
    begin: datetime,
    end: datetime,
    kline_interval_minutes: int,
    symbol: Optional[str] = None,
    *,
    end_inclusive: bool = True,
) -> str:
    """Build a single resample sub-query (GROUP BY + HAVING) for one data source."""
    end_op = "<=" if end_inclusive else "<"
    cols = RESAMPLE_COLUMNS.format(interval=interval, offset=offset)
    sql = f"""
    SELECT {cols}
    FROM {source}
    WHERE open_time >= '{_fmt_ts(begin)}' AND open_time {end_op} '{_fmt_ts(end)}'"""
    if symbol:
        sql += f"\n      AND symbol = '{symbol}'"
    sql += f"""
    GROUP BY symbol, resample_time
    HAVING count(open_time) = {interval // kline_interval_minutes}
    ORDER BY resample_time"""
    return sql


def build_duckdb_resample_sql(
    table_name: str,
    interval: int,
    offset: int,
    begin: datetime,
    end: datetime,
    kline_interval_minutes: int,
    symbol: Optional[str] = None,
) -> str:
    """Build a resample query against a DuckDB table (recent data)."""
    return _build_resample_fragment(
        source=table_name,
        interval=interval,
        offset=offset,
        begin=begin,
        end=end,
        kline_interval_minutes=kline_interval_minutes,
        symbol=symbol,
    )
