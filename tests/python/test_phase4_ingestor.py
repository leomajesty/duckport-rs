#!/usr/bin/env python3
"""Phase 4 — DuckportClient ingestor migration end-to-end test.

Covers: ping, schema init, interval verify, write_kline, Airport read-back,
        duck_time watermark, anti-join dedup (overlap and duplicates inside a
        batch), tables without PRIMARY KEY + write_mode registration, writes into
        a legacy keyed table, save_exginfo, replace exginfo, exginfo refresh not
        growing, multi-market writes, batch duck_time read.

Prerequisites:
  duckport-rs server running (empty DB, no seed):
    DUCKPORT_DB_PATH=/tmp/duckport_phase4.db cargo run --bin duckport-server

  Run from duckport-rs root, pointing at a duckport-binance-ingestor checkout:
    BINANCE_INGESTOR_SRC=../duckport-binance-ingestor python tests/python/test_phase4_ingestor.py
"""

import os
import sys
import traceback
from datetime import datetime, timezone, timedelta

import json

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

INGESTOR_ROOT = os.getenv(
    "BINANCE_INGESTOR_SRC",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "duckport-binance-ingestor"),
)
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
            "underlying_type": "COIN",
        })
    return pd.DataFrame(rows)


def main():
    print(f"\n{'='*60}")
    print("Phase 4 End-to-End Test: DuckportClient")
    print(f"{'='*60}\n")

    client = DuckportClient(addr=ADDR, schema="data")
    reader = flight.FlightClient(f"grpc://{ADDR}")

    def scalar(sql):
        ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())
        rows = reader.do_get(ticket).read_all().to_pylist()
        return list(rows[0].values())[0]

    def keys(table):
        return scalar("SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' "
                      f"AND table_name = '{table}' AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')")

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

    def test_tables_have_no_key_and_register_write_mode():
        for t in ("usdt_perp_5m", "usdt_spot_5m", "exginfo"):
            assert keys(t) == 0, f"data.{t} still has a key"
            mode = scalar(f"SELECT value FROM data.config_dict WHERE key = 'write_mode:{t}'")
            assert mode == "anti_join", f"write_mode:{t} = {mode}"
        n = scalar("SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'data' AND table_name = 'retention_tasks'")
        assert n == 0, "data.retention_tasks should no longer be created"

    run_test("no PRIMARY KEY + write_mode registered", test_tables_have_no_key_and_register_write_mode)

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

    # ── 7: Write overlapping kline (anti-join dedup) ───────────
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

    run_test("write_kline overlap (anti-join dedup)", test_write_kline_overlap)

    def test_duplicates_inside_one_batch():
        df = make_kline_df(n_symbols=3, n_bars=2, base_time=base_time + timedelta(hours=2))
        resp = client.write_kline(pd.concat([df, df]), "usdt_perp", INTERVAL, current_time)
        assert resp["rows_affected"][0] == 6, resp
        dups = scalar("SELECT count(*) - count(DISTINCT (open_time, symbol)) FROM data.usdt_perp_5m")
        assert dups == 0, f"{dups} duplicate keys"

    run_test("duplicates inside one batch are written once", test_duplicates_inside_one_batch)

    def test_bulk_write_dedup():
        # 20 bars x 3 symbols starting at base_time; the first 10 bars are already stored.
        df = make_kline_df(n_symbols=3, n_bars=20, base_time=base_time)
        already = scalar("SELECT count(*) FROM data.usdt_perp_5m "
                         "WHERE open_time BETWEEN TIMESTAMP '2026-04-22 00:00:00' AND TIMESTAMP '2026-04-22 01:35:00' "
                         "AND symbol IN ('SYM1USDT', 'SYM2USDT', 'SYM3USDT')")
        before = scalar("SELECT count(*) FROM data.usdt_perp_5m")
        arrow = pa.Table.from_pandas(df, preserve_index=False)
        client.bulk_write_kline(arrow, "usdt_perp", INTERVAL, "", chunk_size=7, sync_duck_time=False)
        after = scalar("SELECT count(*) FROM data.usdt_perp_5m")
        assert already == 30 and after - before == 60 - already, (already, before, after)
        dups = scalar("SELECT count(*) - count(DISTINCT (open_time, symbol)) FROM data.usdt_perp_5m")
        assert dups == 0, f"{dups} duplicate keys"

    run_test("bulk_write_kline dedup across chunks", test_bulk_write_dedup)

    def test_bulk_write_time_zone_and_null_batches():
        before = scalar("SELECT count(*) FROM data.usdt_perp_5m")
        # Rows already stored, expressed in Asia/Shanghai: must be recognised as duplicates.
        df = make_kline_df(n_symbols=3, n_bars=4, base_time=base_time)
        df["open_time"] = df["open_time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
        client.bulk_write_kline(pa.Table.from_pandas(df, preserve_index=False), "usdt_perp", INTERVAL, "",
                                sync_duck_time=False)
        assert scalar("SELECT count(*) FROM data.usdt_perp_5m") == before, "tz-aware duplicates inserted"
        # A batch without any open_time is skipped instead of failing.
        nulls = make_kline_df(n_symbols=2, n_bars=1, base_time=base_time)
        nulls["open_time"] = pd.NaT
        client.bulk_write_kline(pa.Table.from_pandas(nulls, preserve_index=False), "usdt_perp", INTERVAL, "",
                                sync_duck_time=False)
        assert scalar("SELECT count(*) FROM data.usdt_perp_5m") == before

    run_test("bulk_write_kline: tz-aware duplicates and null-time batches", test_bulk_write_time_zone_and_null_batches)

    def test_legacy_keyed_table():
        # A table created by an older plugin keeps its key; the new write path must work on it.
        client.execute(f"CREATE TABLE IF NOT EXISTS data.usdt_perp_15m ({', '.join(client.KLINE_COL_DEFS)}, "
                       "PRIMARY KEY (open_time, symbol))")
        client.init_schema(["usdt_perp"], "15m", DATA_SOURCES)
        assert keys("usdt_perp_15m") == 1
        df = make_kline_df(n_symbols=2, n_bars=4, base_time=base_time)
        client.write_kline(df, "usdt_perp", "15m", current_time)
        client.write_kline(pd.concat([df, df]), "usdt_perp", "15m", current_time)
        assert scalar("SELECT count(*) FROM data.usdt_perp_15m") == 8

    run_test("new write path on a legacy keyed table", test_legacy_keyed_table)

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

    def test_exginfo_refresh_does_not_grow():
        df = make_exginfo_df("usdt_perp", symbols=[f"S{i}USDT" for i in range(300)])
        for _ in range(150):
            client.save_exginfo(df, "usdt_perp")
        client.execute("CHECKPOINT")
        assert scalar("SELECT count(*) FROM data.exginfo WHERE market = 'usdt_perp'") == 300
        groups = scalar("SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('data.exginfo')")
        assert groups <= 2, f"exginfo has {groups} row groups after 150 refreshes"

    run_test("exginfo refresh does not grow (no key)", test_exginfo_refresh_does_not_grow)

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
    reader.close()
    total = passed + failed
    print(f"\n{'='*60}")
    print(f"Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}\n")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
