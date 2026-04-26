#!/usr/bin/env python3
"""
End-to-end test for consumer read path migration.

Tests the duckport.query DoGet endpoint and the DuckportConsumer client.
Assumes duckport-rs is running on localhost:50051.
"""

import json
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.flight as flight

# tests/python/ → 项目根下 client/（一级 .. 会落到 tests/，需两级）
HERE = os.path.dirname(os.path.abspath(__file__))
CLIENT_ROOT = os.path.join(HERE, "..", "..", "client")
sys.path.insert(0, os.path.normpath(CLIENT_ROOT))

ADDR = "localhost:50051"
SCHEMA = "data"

passed = 0
failed = 0


def ok(name: str):
    global passed
    passed += 1
    print(f"  ✅ {name}")


def fail(name: str, err):
    global failed
    failed += 1
    print(f"  ❌ {name}: {err}")


# ──────────────────────────────────────────────────────────────────
# Phase 0: Seed test data via DoAction
# ──────────────────────────────────────────────────────────────────

def seed_data():
    """Create schema, tables, and insert sample data."""
    client = flight.FlightClient(f"grpc://{ADDR}")

    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.config_dict (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        )""",
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.kline_usdt_perp_1m (
            open_time TIMESTAMP,
            symbol VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trade_num INT,
            taker_buy_base_asset_volume DOUBLE,
            taker_buy_quote_asset_volume DOUBLE,
            avg_price DOUBLE
        )""",
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.exginfo (
            market VARCHAR,
            symbol VARCHAR,
            status VARCHAR,
            base_asset VARCHAR,
            quote_asset VARCHAR,
            price_tick VARCHAR,
            lot_size VARCHAR,
            min_notional_value VARCHAR,
            contract_type VARCHAR,
            margin_asset VARCHAR,
            pre_market VARCHAR
        )""",
    ]
    body = json.dumps({"statements": stmts}).encode()
    action = flight.Action("duckport.execute_transaction", body)
    list(client.do_action(action))

    # Clear existing test data for idempotency
    for tbl in ("kline_usdt_perp_1m", "exginfo"):
        body = json.dumps({"sql": f"DELETE FROM {SCHEMA}.{tbl}"}).encode()
        action = flight.Action("duckport.execute", body)
        list(client.do_action(action))

    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for i in range(60):
        t = now - timedelta(minutes=60 - i)
        rows.append(f"(TIMESTAMP '{t:%Y-%m-%d %H:%M:%S}', 'BTCUSDT', "
                     f"{40000 + i}, {40100 + i}, {39900 + i}, {40050 + i}, "
                     f"{100.0 + i}, {4000000 + i}, {500 + i}, "
                     f"{50.0 + i}, {2000000 + i}, {40025.0 + i})")
    insert_sql = (
        f"INSERT INTO {SCHEMA}.kline_usdt_perp_1m VALUES " + ",\n".join(rows)
    )
    body = json.dumps({"sql": insert_sql}).encode()
    action = flight.Action("duckport.execute", body)
    list(client.do_action(action))

    insert_exginfo = (
        f"INSERT INTO {SCHEMA}.exginfo VALUES "
        f"('usdt_perp', 'BTCUSDT', 'TRADING', 'BTC', 'USDT', "
        f"'0.01', '0.001', '5', 'PERPETUAL', 'USDT', 'false'), "
        f"('usdt_perp', 'ETHUSDT', 'TRADING', 'ETH', 'USDT', "
        f"'0.01', '0.01', '5', 'PERPETUAL', 'USDT', 'false')"
    )
    body = json.dumps({"sql": insert_exginfo}).encode()
    action = flight.Action("duckport.execute", body)
    list(client.do_action(action))

    insert_duck_time = (
        f"INSERT INTO {SCHEMA}.config_dict VALUES "
        f"('usdt_perp_duck_time', '{now:%Y-%m-%d %H:%M:%S}') "
        f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
    )
    body = json.dumps({"sql": insert_duck_time}).encode()
    action = flight.Action("duckport.execute", body)
    list(client.do_action(action))

    print("  Seeded test data")


# ──────────────────────────────────────────────────────────────────
# Phase 1: Raw Flight DoGet with duckport.query ticket
# ──────────────────────────────────────────────────────────────────

def test_raw_query():
    """Test raw duckport.query DoGet ticket."""
    print("\n📋 Phase 1: Raw duckport.query DoGet")
    client = flight.FlightClient(f"grpc://{ADDR}")

    # T1: Simple SELECT
    try:
        ticket = flight.Ticket(
            json.dumps({"type": "duckport.query", "sql": "SELECT 1 AS n, 'hello' AS msg"}).encode()
        )
        table = client.do_get(ticket).read_all()
        assert table.num_rows == 1, f"expected 1 row, got {table.num_rows}"
        assert table.column("n")[0].as_py() == 1
        assert table.column("msg")[0].as_py() == "hello"
        ok("simple SELECT 1")
    except Exception as e:
        fail("simple SELECT 1", e)
        traceback.print_exc()

    # T2: Query actual table
    try:
        ticket = flight.Ticket(
            json.dumps({
                "type": "duckport.query",
                "sql": f"SELECT count(*) AS cnt FROM {SCHEMA}.kline_usdt_perp_1m"
            }).encode()
        )
        table = client.do_get(ticket).read_all()
        cnt = table.column("cnt")[0].as_py()
        assert cnt == 60, f"expected 60 rows, got {cnt}"
        ok("count(*) on kline table")
    except Exception as e:
        fail("count(*) on kline table", e)
        traceback.print_exc()

    # T3: GROUP BY / aggregation
    try:
        sql = f"""
        SELECT symbol, count(*) AS cnt, avg(close) AS avg_close
        FROM {SCHEMA}.kline_usdt_perp_1m
        GROUP BY symbol
        """
        ticket = flight.Ticket(
            json.dumps({"type": "duckport.query", "sql": sql}).encode()
        )
        table = client.do_get(ticket).read_all()
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "BTCUSDT"
        assert table.column("cnt")[0].as_py() == 60
        ok("GROUP BY aggregation")
    except Exception as e:
        fail("GROUP BY aggregation", e)
        traceback.print_exc()

    # T4: WITH / CTE
    try:
        sql = f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (ORDER BY open_time DESC) AS rn
            FROM {SCHEMA}.kline_usdt_perp_1m
        )
        SELECT open_time, close FROM ranked WHERE rn = 1
        """
        ticket = flight.Ticket(
            json.dumps({"type": "duckport.query", "sql": sql}).encode()
        )
        table = client.do_get(ticket).read_all()
        assert table.num_rows == 1
        ok("WITH / CTE query")
    except Exception as e:
        fail("WITH / CTE query", e)
        traceback.print_exc()

    # T5: Reject DML
    try:
        ticket = flight.Ticket(
            json.dumps({
                "type": "duckport.query",
                "sql": f"INSERT INTO {SCHEMA}.kline_usdt_perp_1m VALUES (now(), 'X', 0,0,0,0,0,0,0,0,0,0)"
            }).encode()
        )
        try:
            client.do_get(ticket).read_all()
            fail("reject INSERT", "should have raised error")
        except flight.FlightError:
            ok("reject INSERT (permission_denied)")
    except Exception as e:
        fail("reject INSERT", e)
        traceback.print_exc()

    # T6: Reject DDL
    try:
        ticket = flight.Ticket(
            json.dumps({
                "type": "duckport.query",
                "sql": "DROP TABLE IF EXISTS data.kline_usdt_perp_1m"
            }).encode()
        )
        try:
            client.do_get(ticket).read_all()
            fail("reject DROP TABLE", "should have raised error")
        except flight.FlightError:
            ok("reject DROP TABLE (permission_denied)")
    except Exception as e:
        fail("reject DROP TABLE", e)
        traceback.print_exc()

    # T7: Non-duckport ticket falls through to airport (should handle gracefully)
    try:
        ticket = flight.Ticket(
            json.dumps({"schema": SCHEMA, "table": "kline_usdt_perp_1m"}).encode()
        )
        table = client.do_get(ticket).read_all()
        assert table.num_rows == 60
        ok("airport fallthrough (standard ticket)")
    except Exception as e:
        fail("airport fallthrough (standard ticket)", e)
        traceback.print_exc()


# ──────────────────────────────────────────────────────────────────
# Phase 2: DuckportConsumer high-level client
# ──────────────────────────────────────────────────────────────────

def test_consumer_client():
    """Test DuckportConsumer high-level API."""
    print("\n📋 Phase 2: DuckportConsumer client")

    from duckport_consumer import DuckportConsumer

    consumer = DuckportConsumer(
        addr=ADDR,
        schema=SCHEMA,
        kline_interval_minutes=1,
        suffix="_1m",
        pqt_path=None,
    )

    # T1: ping
    try:
        resp = consumer.ping()
        assert resp["server"] == "duckport"
        assert "duckdb_version" in resp
        ok(f"ping → server={resp['server']} duckdb={resp['duckdb_version']}")
    except Exception as e:
        fail("ping", e)
        traceback.print_exc()

    # T2: raw query
    try:
        table = consumer.query(f"SELECT count(*) AS cnt FROM {SCHEMA}.kline_usdt_perp_1m")
        assert table.column("cnt")[0].as_py() == 60
        ok("consumer.query()")
    except Exception as e:
        fail("consumer.query()", e)
        traceback.print_exc()

    # T3: read_duck_time
    try:
        dt = consumer.read_duck_time("usdt_perp")
        assert dt is not None
        ok(f"read_duck_time → {dt}")
    except Exception as e:
        fail("read_duck_time", e)
        traceback.print_exc()

    # T4: get_exginfo
    try:
        table = consumer.get_exginfo("usdt_perp")
        assert table.num_rows == 2
        symbols = set(table.column("symbol").to_pylist())
        assert "BTCUSDT" in symbols and "ETHUSDT" in symbols
        ok(f"get_exginfo → {table.num_rows} rows")
    except Exception as e:
        fail("get_exginfo", e)
        traceback.print_exc()

    # T5: get_market (DuckDB-only resample, Plan A)
    try:
        table = consumer.get_market("usdt_perp", interval=5, offset=0)
        ok(f"get_market(5m) → {table.num_rows} rows, {table.num_columns} cols")
    except Exception as e:
        fail("get_market", e)
        traceback.print_exc()

    # T6: get_symbol
    try:
        table = consumer.get_symbol("usdt_perp", "BTCUSDT", interval=5, offset=0)
        ok(f"get_symbol(BTCUSDT, 5m) → {table.num_rows} rows")
    except Exception as e:
        fail("get_symbol", e)
        traceback.print_exc()

    # T7: invalid market
    try:
        try:
            consumer.get_market("invalid_market", interval=5)
            fail("invalid market", "should have raised ValueError")
        except ValueError:
            ok("invalid market raises ValueError")
    except Exception as e:
        fail("invalid market", e)
        traceback.print_exc()

    # T8: Plan A — 传入已弃用的 pqt_path 时，SQL 仍不访问 read_parquet
    try:
        hy = DuckportConsumer(
            addr=ADDR,
            schema=SCHEMA,
            kline_interval_minutes=1,
            suffix="_1m",
            pqt_path="/tmp/should_not_be_used",
            redundancy_hours=2,
        )
        captured = []

        def cap(sql: str) -> pa.Table:
            captured.append(sql)
            return pa.table({})

        hy.query = cap  # type: ignore[assignment]
        tnow = datetime.now(tz=timezone.utc)
        past = tnow - timedelta(days=2)
        hy.get_market("usdt_perp", interval=5, offset=0, begin=past, end=tnow)
        sql = captured[0]
        if "read_parquet" in sql:
            fail("Plan A: no read_parquet when pqt_path set", "found read_parquet in SQL")
        else:
            ok("Plan A: resample SQL is DuckDB-only (no read_parquet)")
        hy.close()
    except Exception as e:
        fail("Plan A duckdb-only", e)
        traceback.print_exc()

    consumer.close()


# ──────────────────────────────────────────────────────────────────
# Phase 3: Resample SQL builder unit tests
# ──────────────────────────────────────────────────────────────────

def test_resample_builder():
    """Test resample.py (DuckDB-only) SQL construction."""
    print("\n📋 Phase 3: Resample SQL builder")

    from duckport_consumer.resample import build_duckdb_resample_sql

    now = datetime.now(tz=timezone.utc)
    past = now - timedelta(days=30)

    # T1: duckdb resample SQL
    try:
        sql = build_duckdb_resample_sql(
            "data.kline_usdt_perp_1m", 5, 0, past, now, 1
        )
        assert "DATE_TRUNC" in sql
        assert "GROUP BY" in sql
        assert "data.kline_usdt_perp_1m" in sql
        ok("build_duckdb_resample_sql")
    except Exception as e:
        fail("build_duckdb_resample_sql", e)
        traceback.print_exc()

    # T2: with symbol filter
    try:
        sql = build_duckdb_resample_sql(
            "data.kline_usdt_perp_1m", 5, 0, past, now, 1, symbol="BTCUSDT"
        )
        assert "BTCUSDT" in sql
        ok("resample with symbol filter")
    except Exception as e:
        fail("resample with symbol filter", e)
        traceback.print_exc()


# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🚀 Consumer read path migration — e2e test\n")
    print("Seeding test data...")
    seed_data()

    test_raw_query()
    test_consumer_client()
    test_resample_builder()

    print(f"\n{'='*50}")
    print(f"✅ Passed: {passed}  ❌ Failed: {failed}")
    if failed > 0:
        sys.exit(1)
    print("All tests passed!")
