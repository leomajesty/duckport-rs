"""
SQL builders for K-line resample queries.

Migrated from duckport/core/flight_func/flight_api.py (FlightGets).
All SQL is constructed here and sent to duckport-rs via the `duckport.query`
DoGet ticket for server-side execution.

**Plan A:** 仅使用 :func:`build_duckdb_resample_sql` 查询热库主表（无 Parquet / hybrid / pqt_time）。
"""

import math
from datetime import datetime, timedelta
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
    extra_predicate: Optional[str] = None,
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
    if extra_predicate:
        sql += f"\n      AND {extra_predicate}"
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
    *,
    extra_predicate: Optional[str] = None,
    end_inclusive: bool = True,
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
        end_inclusive=end_inclusive,
        extra_predicate=extra_predicate,
    )


# One resample group was measured around 1–2 KiB of DuckDB hash state.
# Stay near 2 GiB per query, and never hand a single query more than 45 days.
SPLIT_BYTES_PER_GROUP = 2048
SPLIT_BUDGET_BYTES = 2 * 1024**3
SPLIT_MAX_CHUNK_MINUTES = 45 * 24 * 60


def floor_bucket(ts: datetime, interval: int, offset: int) -> datetime:
    """Earliest bar time of the resample bucket that contains ``ts``.

    This follows the SQL expression
    ``open_time - (EXTRACT(MINUTE FROM (open_time - offset)) % interval)``.
    Seconds are discarded. The timezone, when present, is left unchanged.
    """
    if interval <= 0:
        raise ValueError(f"interval must be positive, got {interval}")
    base = ts.replace(second=0, microsecond=0)
    shifted = base - timedelta(minutes=offset)
    rem = shifted.minute % interval
    return base - timedelta(minutes=rem)


def plan_chunk_minutes(
    symbol_count: int,
    interval: int,
    window_minutes: float,
) -> Optional[int]:
    """Minutes per chunk, or None when one query stays inside the budget."""
    if symbol_count < 1 or interval <= 0 or window_minutes <= interval:
        return None
    budget_groups = SPLIT_BUDGET_BYTES // SPLIT_BYTES_PER_GROUP
    estimated_groups = symbol_count * math.ceil(window_minutes / interval)
    if estimated_groups <= budget_groups:
        return None
    buckets = max(1, budget_groups // symbol_count)
    chunk_minutes = buckets * interval
    cap = max(interval, (SPLIT_MAX_CHUNK_MINUTES // interval) * interval)
    chunk_minutes = min(max(chunk_minutes, interval), cap)
    if chunk_minutes >= window_minutes:
        return None
    return chunk_minutes


def chunk_ranges(
    begin: datetime,
    end: datetime,
    interval: int,
    offset: int,
    chunk_minutes: int,
) -> list[tuple[datetime, datetime, bool]]:
    """Split ``[begin, end]`` on resample-bucket boundaries.

    Every cut is a bucket start, so a bucket is never divided across chunks.
    Ranges before the last use ``open_time < cut``. The last range keeps the
    caller's inclusive end. A window shorter than one chunk stays one range.
    """
    if chunk_minutes <= 0:
        raise ValueError(f"chunk_minutes must be positive, got {chunk_minutes}")
    if chunk_minutes % interval != 0:
        raise ValueError(
            f"chunk_minutes {chunk_minutes} must be a multiple of interval {interval}"
        )
    step = timedelta(minutes=chunk_minutes)
    cursor = floor_bucket(begin, interval, offset)
    while cursor <= begin:
        cursor += step
    cuts: list[datetime] = []
    while cursor < end:
        cuts.append(cursor)
        cursor += step
    points = [begin, *cuts, end]
    last = len(points) - 2
    return [(points[i], points[i + 1], i == last) for i in range(len(points) - 1)]


def plan_resample_ranges(
    begin: datetime,
    end: datetime,
    interval: int,
    offset: int,
    symbol_count: int,
) -> list[tuple[datetime, datetime, bool]]:
    """Ranges for one ``get_market`` / ``get_symbol`` call. One range means no split."""
    window_minutes = (end - begin).total_seconds() / 60.0
    chunk_minutes = plan_chunk_minutes(symbol_count, interval, window_minutes)
    if chunk_minutes is None:
        return [(begin, end, True)]
    return chunk_ranges(begin, end, interval, offset, chunk_minutes)


def halve_range(
    begin: datetime,
    end: datetime,
    end_inclusive: bool,
    interval: int,
    offset: int,
) -> Optional[tuple[tuple[datetime, datetime, bool], tuple[datetime, datetime, bool]]]:
    """Split one range at the bucket boundary nearest its midpoint.

    Returns None when the range is already a single bucket. The left half
    excludes the cut. The right half keeps ``end_inclusive``.
    """
    if end <= begin or interval <= 0:
        return None
    if end - begin <= timedelta(minutes=interval):
        return None
    target = begin + (end - begin) / 2
    step = timedelta(minutes=interval)
    cursor = floor_bucket(begin, interval, offset)
    if cursor <= begin:
        cursor += step
    cut: Optional[datetime] = None
    while cursor < end:
        cut = cursor
        if cursor >= target:
            break
        cursor += step
    if cut is None or cut <= begin or cut >= end:
        return None
    return (begin, cut, False), (cut, end, end_inclusive)


def build_symbol_count_sql(
    table_name: str,
    begin: datetime,
    end: datetime,
    symbol: Optional[str] = None,
    extra_predicate: Optional[str] = None,
) -> str:
    """Count symbols in the same inclusive window the resample will scan."""
    sql = (
        f"SELECT count(DISTINCT symbol) AS n FROM {table_name} "
        f"WHERE open_time >= '{_fmt_ts(begin)}' AND open_time <= '{_fmt_ts(end)}'"
    )
    if symbol:
        sql += f" AND symbol = '{symbol}'"
    if extra_predicate:
        sql += f" AND {extra_predicate}"
    return sql
