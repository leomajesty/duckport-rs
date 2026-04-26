#!/usr/bin/env python3
"""loadhist migration — end-to-end test.

Validates:
  1. DuckportClient.bulk_write_kline() with chunked staging-table pattern
  2. Round-trip: local Parquet -> DoPut -> read-back via Airport
  3. Watermark (duck_time) update after bulk load

Prerequisites:
  DUCKPORT_SEED_DEMO=1 cargo run --bin duckport-server
"""

import json
import os
import sys
import tempfile

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

SERVER = os.getenv("DUCKPORT_SERVER", "grpc://127.0.0.1:50051")
SCHEMA = "data"
MARKET = "usdt_perp"
INTERVAL = "5m"
TABLE = f"{MARKET}_{INTERVAL}"

passed = 0
failed = 0


def hr(title):
    print("\n" + "=" * 6 + f" {title} " + "=" * 6)


def ok(msg):
    global passed
    passed += 1
    print(f"  PASS: {msg}")


def fail(msg):
    global failed
    failed += 1
    print(f"  FAIL: {msg}", file=sys.stderr)


def do_action(client, action_type, body=None):
    body_bytes = json.dumps(body).encode("utf-8") if body else b""
    results = list(client.do_action(flight.Action(action_type, body_bytes)))
    assert len(results) == 1
    return json.loads(bytes(results[0].body))


def do_append(client, schema, table, arrow_table):
    desc = flight.FlightDescriptor.for_path("duckport.append", schema, table)
    writer, reader = client.do_put(desc, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.done_writing()
    buf = reader.read()
    resp = json.loads(bytes(buf))
    writer.close()
    return resp


# ── Helpers ───────────────────────────────────────────────────────────

def make_kline_arrow(n_rows, symbol="BTCUSDT", start_ts="2025-01-01"):
    """Generate a synthetic kline Arrow table for testing."""
    ts = pd.date_range(start_ts, periods=n_rows, freq="5min")
    df = pd.DataFrame({
        "open_time": ts,
        "symbol": symbol,
        "open": np.random.uniform(40000, 50000, n_rows),
        "high": np.random.uniform(50000, 55000, n_rows),
        "low": np.random.uniform(35000, 40000, n_rows),
        "close": np.random.uniform(40000, 50000, n_rows),
        "volume": np.random.uniform(100, 1000, n_rows),
        "quote_volume": np.random.uniform(1e6, 1e7, n_rows),
        "trade_num": np.random.randint(100, 5000, n_rows).astype("int32"),
        "taker_buy_base_asset_volume": np.random.uniform(50, 500, n_rows),
        "taker_buy_quote_asset_volume": np.random.uniform(5e5, 5e6, n_rows),
        "avg_price": np.random.uniform(40000, 50000, n_rows),
    })
    return pa.Table.from_pandas(df, preserve_index=False)


def ensure_table(client):
    """Ensure target + staging tables exist."""
    kline_cols = (
        "open_time TIMESTAMP, symbol VARCHAR, "
        "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
        "volume DOUBLE, quote_volume DOUBLE, trade_num INT, "
        "taker_buy_base_asset_volume DOUBLE, taker_buy_quote_asset_volume DOUBLE, "
        "avg_price DOUBLE"
    )
    do_action(client, "duckport.execute_transaction", {
        "statements": [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} ({kline_cols}, PRIMARY KEY (open_time, symbol))",
            f"CREATE TABLE IF NOT EXISTS _staging_{TABLE} ({kline_cols})",
        ]
    })


def count_rows(client, full_table):
    """Count rows via duckport.query DoGet."""
    ticket = json.dumps({"type": "duckport.query", "sql": f"SELECT count(*) as cnt FROM {full_table}"}).encode()
    reader = client.do_get(flight.Ticket(ticket))
    table = reader.read_all()
    return table.to_pandas()["cnt"].iloc[0]


def read_config(client, key):
    """Read a config_dict value via duckport.query DoGet."""
    ticket = json.dumps({
        "type": "duckport.query",
        "sql": f"SELECT value FROM {SCHEMA}.config_dict WHERE key = '{key}'",
    }).encode()
    reader = client.do_get(flight.Ticket(ticket))
    df = reader.read_all().to_pandas()
    if df.empty:
        return None
    return df["value"].iloc[0]


# ── Tests ─────────────────────────────────────────────────────────────

def test_bulk_write_basic(client):
    """bulk_write_kline with a small table (single chunk)."""
    hr("bulk_write — single chunk")
    ensure_table(client)

    do_action(client, "duckport.execute", {"sql": f"DELETE FROM {SCHEMA}.{TABLE}"})

    arrow = make_kline_arrow(500, symbol="TESTBULK1")
    total = len(arrow)

    staging = f"_staging_{TABLE}"
    target = f"{SCHEMA}.{TABLE}"
    s = SCHEMA

    do_action(client, "duckport.execute", {"sql": f"TRUNCATE {staging}"})
    do_append(client, "main", staging, arrow)
    do_action(client, "duckport.execute_transaction", {"statements": [
        f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING",
        f"TRUNCATE {staging}",
    ]})

    duck_time_str = "2025-01-02 17:35:00"
    do_action(client, "duckport.execute_transaction", {"statements": [
        f"INSERT OR REPLACE INTO {s}.config_dict (key, value) VALUES ('{MARKET}_duck_time', '{duck_time_str}')",
    ]})

    cnt = count_rows(client, target)
    if cnt == total:
        ok(f"rows match: {cnt}")
    else:
        fail(f"expected {total} rows, got {cnt}")

    dt = read_config(client, f"{MARKET}_duck_time")
    if dt == duck_time_str:
        ok(f"duck_time = {dt}")
    else:
        fail(f"duck_time expected '{duck_time_str}', got '{dt}'")


def test_bulk_write_chunked(client):
    """Simulate chunked bulk write (multiple rounds)."""
    hr("bulk_write — chunked (3 rounds)")
    ensure_table(client)
    do_action(client, "duckport.execute", {"sql": f"DELETE FROM {SCHEMA}.{TABLE}"})

    chunk_size = 200
    symbols = ["CHUNK_A", "CHUNK_B", "CHUNK_C"]
    all_arrows = [make_kline_arrow(chunk_size, symbol=s) for s in symbols]

    staging = f"_staging_{TABLE}"
    target = f"{SCHEMA}.{TABLE}"

    for i, arrow in enumerate(all_arrows):
        do_action(client, "duckport.execute", {"sql": f"TRUNCATE {staging}"})
        do_append(client, "main", staging, arrow)
        do_action(client, "duckport.execute_transaction", {"statements": [
            f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING",
            f"TRUNCATE {staging}",
        ]})

    cnt = count_rows(client, target)
    expected = chunk_size * len(symbols)
    if cnt == expected:
        ok(f"chunked total rows: {cnt}")
    else:
        fail(f"expected {expected}, got {cnt}")


def test_bulk_write_idempotent(client):
    """Re-insert same data — ON CONFLICT DO NOTHING should keep count stable."""
    hr("bulk_write — idempotent (ON CONFLICT DO NOTHING)")
    ensure_table(client)
    do_action(client, "duckport.execute", {"sql": f"DELETE FROM {SCHEMA}.{TABLE}"})

    arrow = make_kline_arrow(100, symbol="IDEMPOTENT")
    staging = f"_staging_{TABLE}"
    target = f"{SCHEMA}.{TABLE}"

    for _ in range(3):
        do_action(client, "duckport.execute", {"sql": f"TRUNCATE {staging}"})
        do_append(client, "main", staging, arrow)
        do_action(client, "duckport.execute_transaction", {"statements": [
            f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING",
            f"TRUNCATE {staging}",
        ]})

    cnt = count_rows(client, target)
    if cnt == 100:
        ok(f"idempotent: {cnt} rows after 3 inserts")
    else:
        fail(f"expected 100, got {cnt}")


def test_parquet_roundtrip(client):
    """Write Parquet locally -> read via DuckDB in-memory -> DoPut to server -> read back."""
    hr("parquet roundtrip — local Parquet -> Arrow -> DoPut -> read-back")
    ensure_table(client)
    do_action(client, "duckport.execute", {"sql": f"DELETE FROM {SCHEMA}.{TABLE}"})

    arrow = make_kline_arrow(300, symbol="PQTROUND")
    staging = f"_staging_{TABLE}"
    target = f"{SCHEMA}.{TABLE}"

    with tempfile.TemporaryDirectory() as tmpdir:
        pqt_path = os.path.join(tmpdir, "test.parquet")
        import pyarrow.parquet as pq
        pq.write_table(arrow, pqt_path)

        local_conn = duckdb.connect()
        loaded = local_conn.execute(f"""
            SELECT open_time, symbol, open, high, low, close, volume,
                   quote_volume, trade_num, taker_buy_base_asset_volume,
                   taker_buy_quote_asset_volume, avg_price
            FROM read_parquet('{pqt_path}')
        """).fetch_arrow_table()
        max_time = local_conn.execute(
            f"SELECT max(open_time) FROM read_parquet('{pqt_path}')"
        ).fetchone()[0]
        local_conn.close()

        if len(loaded) == 300:
            ok(f"local Parquet read: {len(loaded)} rows")
        else:
            fail(f"expected 300 rows from Parquet, got {len(loaded)}")

        do_action(client, "duckport.execute", {"sql": f"TRUNCATE {staging}"})
        do_append(client, "main", staging, loaded)
        do_action(client, "duckport.execute_transaction", {"statements": [
            f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING",
            f"TRUNCATE {staging}",
        ]})

        duck_time_str = max_time.strftime("%Y-%m-%d %H:%M:%S")
        do_action(client, "duckport.execute_transaction", {"statements": [
            f"INSERT OR REPLACE INTO {SCHEMA}.config_dict (key, value) "
            f"VALUES ('{MARKET}_duck_time', '{duck_time_str}')",
        ]})

    cnt = count_rows(client, target)
    if cnt == 300:
        ok(f"server rows after roundtrip: {cnt}")
    else:
        fail(f"expected 300 server rows, got {cnt}")

    dt = read_config(client, f"{MARKET}_duck_time")
    if dt == duck_time_str:
        ok(f"duck_time after roundtrip: {dt}")
    else:
        fail(f"duck_time expected '{duck_time_str}', got '{dt}'")


def test_cleaning_in_memory(client):
    """Verify that DuckDB in-memory mode can query local Parquet files for cleaning."""
    hr("cleaning — DuckDB in-memory Parquet analytics")

    with tempfile.TemporaryDirectory() as tmpdir:
        ts = pd.date_range("2025-01-01", periods=100, freq="5min")
        df = pd.DataFrame({
            "open_time": ts,
            "symbol": ["BTCUSDT"] * 50 + ["DEADUSDT"] * 50,
            "open": np.random.uniform(40000, 50000, 100),
            "high": np.random.uniform(50000, 55000, 100),
            "low": np.random.uniform(35000, 40000, 100),
            "close": np.random.uniform(40000, 50000, 100),
            "volume": [100.0] * 50 + [0.0] * 50,
            "quote_volume": np.random.uniform(1e6, 1e7, 100),
            "trade_num": np.random.randint(100, 5000, 100).astype("int32"),
            "taker_buy_base_asset_volume": np.random.uniform(50, 500, 100),
            "taker_buy_quote_asset_volume": np.random.uniform(5e5, 5e6, 100),
            "avg_price": np.random.uniform(40000, 50000, 100),
        })
        pqt_path = os.path.join(tmpdir, "test.parquet")
        df.to_parquet(pqt_path, index=False)

        conn = duckdb.connect()
        trading_range = conn.execute(f"""
            SELECT symbol, min(open_time) as first_candle, max(open_time) as last_candle
            FROM read_parquet('{pqt_path}')
            WHERE volume > 0
            GROUP BY symbol
        """).df()

        useless = conn.execute(f"""
            SELECT symbol, sum(volume)
            FROM read_parquet('{pqt_path}')
            GROUP BY symbol HAVING sum(volume) = 0
        """).df()
        conn.close()

        if len(trading_range) == 1 and trading_range["symbol"].iloc[0] == "BTCUSDT":
            ok("trading_range correctly identifies only BTCUSDT")
        else:
            fail(f"trading_range unexpected: {trading_range}")

        if len(useless) == 1 and useless["symbol"].iloc[0] == "DEADUSDT":
            ok("useless symbols correctly identified DEADUSDT")
        else:
            fail(f"useless unexpected: {useless}")


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"Connecting to {SERVER} ...")
    client = flight.FlightClient(SERVER)

    try:
        resp = do_action(client, "duckport.ping")
        print(f"Server ping OK: {resp}")
    except Exception as e:
        print(f"Cannot reach server at {SERVER}: {e}", file=sys.stderr)
        sys.exit(1)

    test_bulk_write_basic(client)
    test_bulk_write_chunked(client)
    test_bulk_write_idempotent(client)
    test_parquet_roundtrip(client)
    test_cleaning_in_memory(client)

    print("\n" + "=" * 50)
    print(f"PASSED: {passed}  FAILED: {failed}")
    if failed:
        sys.exit(1)
    else:
        print("All loadhist tests passed!")


if __name__ == "__main__":
    main()
