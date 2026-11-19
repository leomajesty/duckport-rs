"""Markets the consumer client accepts. Builds SQL only; needs no server.

    python tests/python/test_consumer_markets.py
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "client"))

from duckport_consumer.client import (  # noqa: E402
    build_exginfo_sql, build_market_kline_sql, perp_asset_where, usdt_perp_asset_where,
)

BEGIN, END = datetime(2026, 11, 1), datetime(2026, 11, 2)


def kline_sql(market, **kw):
    return build_market_kline_sql("data", "_1m", market, 5, 0, BEGIN, END, 1, **kw)


def raises(fn, text):
    try:
        fn()
    except ValueError as e:
        assert text in str(e), str(e)
        return
    raise AssertionError(f"expected ValueError containing {text!r}")


def test_kline_markets():
    for market in ("usdt_perp", "usdt_spot", "others_perp"):
        assert f"data.{market}_1m" in kline_sql(market)
    for market in ("usdc_perp", "coin_perp", "others_spot"):
        raises(lambda: kline_sql(market), "Unsupported market")
        raises(lambda: build_exginfo_sql("data", market), "Unsupported market")


def test_exginfo_without_asset_class_is_the_plain_statement():
    for market in ("usdt_perp", "usdt_spot", "others_perp"):
        assert build_exginfo_sql("data", market) == f"SELECT * FROM data.exginfo WHERE market = '{market}'"


def test_asset_class_on_both_perp_markets():
    assert usdt_perp_asset_where("crypto") == "market = 'usdt_perp' AND contract_type = 'PERPETUAL'"
    assert perp_asset_where("usdt_perp", "crypto") == usdt_perp_asset_where("crypto")
    assert perp_asset_where("others_perp", "tradifi") == \
        "market = 'others_perp' AND contract_type = 'TRADIFI_PERPETUAL'"
    assert "market = 'others_perp' AND contract_type = 'PERPETUAL'" in kline_sql("others_perp", asset_class="crypto")
    assert "market = 'others_perp'" in build_exginfo_sql("data", "others_perp", "tradifi")
    # usdt_perp statements are what they were
    assert build_exginfo_sql("data", "usdt_perp", "crypto") == \
        "SELECT * FROM data.exginfo WHERE market = 'usdt_perp' AND contract_type = 'PERPETUAL'"


def test_asset_class_on_the_rows_own_columns():
    """binance-ingestor >= 0.7.0 writes contract_type / underlying_type on every perp row."""
    for market in ("usdt_perp", "others_perp"):
        sql = kline_sql(market, asset_class="crypto", row_columns=True)
        assert "AND (contract_type = 'PERPETUAL')" in sql
        assert "exginfo" not in sql and "symbol IN" not in sql
    assert "AND (contract_type = 'TRADIFI_PERPETUAL')" in kline_sql("usdt_perp", asset_class="tradifi", row_columns=True)
    equity = kline_sql("usdt_perp", asset_class="EQUITY", row_columns=True)
    assert "AND (contract_type = 'TRADIFI_PERPETUAL' AND underlying_type IN ('EQUITY', 'HK_EQUITY', " in equity
    assert "AND (underlying_type = 'PREMARKET')" in kline_sql("usdt_perp", asset_class="premarket", row_columns=True)
    # no asset_class: the statement is the same with or without the columns
    assert kline_sql("usdt_perp", row_columns=True) == kline_sql("usdt_perp")
    # without the columns the symbols still come from exginfo
    assert "symbol IN (SELECT symbol FROM data.exginfo" in kline_sql("usdt_perp", asset_class="crypto")
    raises(lambda: kline_sql("usdt_spot", asset_class="crypto", row_columns=True), "only supported for perp markets")
    raises(lambda: kline_sql("usdt_perp", asset_class="a'b", row_columns=True), "Invalid asset_class")


def chunked_consumer(answers):
    """A consumer whose chunk queries get ``answers`` in order (a table, or an error to raise)."""
    import pyarrow as pa
    from duckport_consumer.client import DuckportConsumer
    consumer = DuckportConsumer.__new__(DuckportConsumer)
    consumer._schema, consumer._suffix, consumer._kline_interval_minutes = "data", "_1m", 1
    consumer._pqt_path, consumer._redundancy_hours, consumer._asset_columns = None, 1, {}
    pending = list(answers)

    def query(sql):
        if "count(DISTINCT symbol)" in sql:
            return pa.table({"n": [900]})
        answer = pending.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer
    consumer.query = query
    return consumer, pending


def test_a_chunk_without_rows_does_not_fail_the_others():
    """A long range is read in chunks; the server answers a zero-row chunk with an error."""
    import pyarrow as pa
    from pyarrow import flight
    from duckport_consumer.client import plan_resample_ranges
    begin, end = datetime(2021, 1, 1), datetime(2026, 11, 1)
    chunks = len(plan_resample_ranges(begin, end, 60, 0, 900))
    assert chunks >= 3, chunks
    empty = flight.FlightInternalError("Server never sent a data message")
    row = pa.table({"symbol": ["BTCUSDT"]})

    consumer, pending = chunked_consumer([empty, empty] + [row] * (chunks - 2))
    assert consumer.get_market("usdt_perp", 60, 0, begin, end).num_rows == chunks - 2
    assert not pending

    # an out-of-memory chunk is read in two halves; one of them may be empty too
    oom = flight.FlightInternalError("Out of Memory Error: failed to allocate")
    consumer, pending = chunked_consumer([oom, empty, row] + [row] * (chunks - 1))
    assert consumer.get_market("usdt_perp", 60, 0, begin, end).num_rows == chunks
    assert not pending

    # no rows at all: the error the server gives for a single empty query
    consumer, _ = chunked_consumer([empty] * chunks)
    try:
        consumer.get_market("usdt_perp", 60, 0, begin, end)
    except flight.FlightInternalError as e:
        assert "never sent a data message" in str(e)
    else:
        raise AssertionError("expected the empty-result error")

    # any other error still fails the call
    consumer, _ = chunked_consumer([row, flight.FlightInternalError("Binder Error: boom")])
    try:
        consumer.get_market("usdt_perp", 60, 0, begin, end)
    except flight.FlightInternalError as e:
        assert "Binder Error" in str(e)
    else:
        raise AssertionError("expected the Binder Error")


def test_asset_class_is_refused_on_spot():
    raises(lambda: kline_sql("usdt_spot", asset_class="crypto"), "only supported for perp markets")
    raises(lambda: build_exginfo_sql("data", "usdt_spot", "crypto"), "only supported for perp markets")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
