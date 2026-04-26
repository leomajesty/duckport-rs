#!/usr/bin/env python3
"""Phase 4 — DuckportClient ingestor migration end-to-end test.

Covers: ping, schema init, interval verify, write_kline, Airport read-back,
        duck_time watermark, ON CONFLICT DO NOTHING, save_exginfo, replace
        exginfo, multi-market writes, batch duck_time read.

Prerequisites:
  duckport-rs server running (empty DB, no seed):
    DUCKPORT_DB_PATH=/tmp/duckport_phase4.db cargo run --bin duckport-server

  Run from duckport-rs root:
    python tests/python/test_phase4_ingestor.py
"""

import os
import sys
import traceback
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

INGESTOR_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "ingestor")
sys.path.insert(0, os.path.abspath(INGESTOR_ROOT))
from binance_ingestor.duckport_client import DuckportClient

ADDR = os.getenv("DUCKPORT_ADDR", "localhost:50051")
INTERVAL = "5m"
MARKETS = ["usdt_perp", "usdt_spot"]
DATA_SOURCES = {"usdt_perp", "usdt_spot"}

passed = 0
failed = 0


def run_test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  \u2705 {name}")
        passed += 1
    except Exception as e:
        print(f"  \u274c {name}: {e}")
        traceback.print_exc()
        failed += 1


def make_kline_df(n_symbols=3, n_bars=5, base_time=None):
    if base_time is None:
        base_time = datetime(2026, 4, 22, 0, 0, 0)
    rows = []
    for i in range(n_bars):
        t = base_time + timedelta(minutes=5 * i)
        for j in range(n_symbols):
            sym = f"SYM{j+1}USDT"
            rows.append({
                "open_time": t,
                "symbol": sym,
                "open": 100.0 + i,
                "high": 105.0 + i,
                "low": 95.0 + i,
                "close": 102.0 + i,
                "volume": 1000.0 * (j + 1),
                "quote_volume": 100000.0 * (j + 1),
                "trade_num": 500 + i * 10,
                "taker_buy_base_asset_volume": 400.0,
                "taker_buy_quote_asset_volume": 40000.0,
                "avg_price": 100.0 + i * 0.5,
            })
    df = pd.DataFrame(rows)
    df["open_time"] = pd.to_datetime(df["open_time"])
    return df


def make_exginfo_df(market, symbols=None):
    if symbols is None:
        symbols = ["SYM1USDT", "SYM2USDT", "SYM3USDT"]
    rows = []
    for sym in symbols:
        rows.append({
            "market": market,
            "symbol": sym,
            "status": "TRADING",
            "base_asset": sym.replace("USDT", ""),
            "quote_asset": "USDT",
            "price_tick": "0.01",
            "lot_size": "0.001",
            "min_notional_value": "10",
            "contract_type": "PERPETUAL" if "perp" in market else "",
            "margin_asset": "USDT",
            "pre_market": False,
        })
    return pd.DataFrame(rows)


def main():
    print(f"\n{'='*60}")
    print("Phase 4 End-to-End Test: DuckportClient")
    print(f"{'='*60}\n")

    client = DuckportClient(addr=ADDR, schema="data")

    # ── 1: Ping ─────────────────────────────────────────────────
    def test_ping():
        resp = client.ping()
        assert resp["server"] == "duckport", f"unexpected: {resp}"
        assert "duckdb_version" in resp

    run_test("ping", test_ping)

    # ── 2: Init schema ──────────────────────────────────────────
    def test_init_schema():
        client.init_schema(MARKETS, INTERVAL, DATA_SOURCES)

    run_test("init_schema", test_init_schema)

    # ── 3: Verify kline interval ────────────────────────────────
    def test_verify_interval():
        client.verify_kline_interval(INTERVAL)

    run_test("verify_kline_interval", test_verify_interval)

    # ── 4: Write kline data ─────────────────────────────────────
    base_time = datetime(2026, 4, 22, 0, 0, 0)
    current_time = base_time + timedelta(minutes=5 * 5)

    def test_write_kline():
        df = make_kline_df(n_symbols=3, n_bars=5, base_time=base_time)
        resp = client.write_kline(df, "usdt_perp", INTERVAL, current_time)
        assert "rows_affected" in resp
        assert resp["rows_affected"][0] == 15

    run_test("write_kline (first write)", test_write_kline)

    # ── 5: Read back via Airport DoGet ──────────────────────────
    def test_read_back():
        table = client.read_table("data", "usdt_perp_5m")
        assert table.num_rows == 15, f"expected 15 rows, got {table.num_rows}"
        df = table.to_pandas()
        symbols = set(df["symbol"].unique())
        assert symbols == {"SYM1USDT", "SYM2USDT", "SYM3USDT"}, f"symbols: {symbols}"

    run_test("read_back via Airport DoGet", test_read_back)

    # ── 6: Read duck_time ───────────────────────────────────────
    def test_read_duck_time():
        dt = client.read_duck_time("usdt_perp")
        assert dt is not None, "duck_time is None"
        expected = pd.to_datetime(current_time).tz_localize(timezone.utc)
        assert dt == expected, f"expected {expected}, got {dt}"

    run_test("read_duck_time", test_read_duck_time)

    # ── 7: Write overlapping kline (ON CONFLICT DO NOTHING) ────
    def test_write_kline_overlap():
        df_overlap = make_kline_df(
            n_symbols=3, n_bars=7,
            base_time=base_time + timedelta(minutes=5 * 3),
        )
        new_time = base_time + timedelta(minutes=5 * 10)
        resp = client.write_kline(df_overlap, "usdt_perp", INTERVAL, new_time)
        assert "rows_affected" in resp

        table = client.read_table("data", "usdt_perp_5m")
        expected_rows = 5 * 3 + 5 * 3  # 30 rows (overlap deduped)
        assert table.num_rows == expected_rows, (
            f"expected {expected_rows}, got {table.num_rows}"
        )

    run_test("write_kline overlap (ON CONFLICT DO NOTHING)", test_write_kline_overlap)

    # ── 8: Save exginfo ─────────────────────────────────────────
    def test_save_exginfo():
        df = make_exginfo_df("usdt_perp")
        client.save_exginfo(df, "usdt_perp")

        table = client.read_table("data", "exginfo")
        assert table.num_rows == 3, f"expected 3, got {table.num_rows}"

    run_test("save_exginfo", test_save_exginfo)

    # ── 9: Replace exginfo (fewer symbols) ──────────────────────
    def test_replace_exginfo():
        df = make_exginfo_df("usdt_perp", symbols=["SYM1USDT", "SYM4USDT"])
        client.save_exginfo(df, "usdt_perp")

        table = client.read_table("data", "exginfo")
        rdf = table.to_pandas()
        perp_symbols = set(rdf[rdf["market"] == "usdt_perp"]["symbol"])
        assert perp_symbols == {"SYM1USDT", "SYM4USDT"}, f"got {perp_symbols}"

    run_test("replace_exginfo", test_replace_exginfo)

    # ── 10: Write kline for second market ───────────────────────
    def test_write_second_market():
        df = make_kline_df(n_symbols=2, n_bars=3)
        resp = client.write_kline(df, "usdt_spot", INTERVAL, current_time)
        assert "rows_affected" in resp

        table = client.read_table("data", "usdt_spot_5m")
        assert table.num_rows == 6, f"expected 6, got {table.num_rows}"

    run_test("write_kline second market (usdt_spot)", test_write_second_market)

    # ── 11: Batch read duck_times ───────────────────────────────
    def test_read_duck_times():
        times = client.read_duck_times(MARKETS)
        assert times["usdt_perp"] is not None
        assert times["usdt_spot"] is not None

    run_test("read_duck_times (batch)", test_read_duck_times)

    # ── Summary ─────────────────────────────────────────────────
    client.close()
    total = passed + failed
    print(f"\n{'='*60}")
    print(f"Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}\n")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
