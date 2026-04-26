#!/usr/bin/env python3
"""Phase 2b — DoPut bulk Arrow append end-to-end test.

Covers: multi-batch append, Airport read-back, schema mismatch rollback,
        missing table error, bad descriptor path, non-duckport path.

Prerequisites:
  DUCKPORT_SEED_DEMO=1 cargo run --bin duckport-server
"""

import json
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.flight as flight

SERVER = os.getenv("DUCKPORT_SERVER", "grpc://127.0.0.1:50051")


def hr(title):
    print("\n" + "=" * 6 + f" {title} " + "=" * 6)


def do_action(client, action_type, body):
    body_bytes = json.dumps(body).encode("utf-8") if body is not None else b""
    results = list(client.do_action(flight.Action(action_type, body_bytes)))
    assert len(results) == 1
    return json.loads(bytes(results[0].body))


def do_append(client, schema, table, record_batches):
    """Send a multi-batch Arrow stream as a DoPut with
    descriptor path ['duckport.append', schema, table]."""
    desc = flight.FlightDescriptor.for_path("duckport.append", schema, table)
    arrow_schema = record_batches[0].schema
    writer, reader = client.do_put(desc, arrow_schema)
    for b in record_batches:
        writer.write_batch(b)
    writer.done_writing()
    results = []
    while True:
        buf = reader.read()
        if buf is None:
            break
        results.append(bytes(buf))
    writer.close()
    return [json.loads(r) for r in results if r]


def main():
    client = flight.FlightClient(SERVER)

    # 1. Create a target table via the write plane.
    hr("create target table via duckport.execute_transaction")
    do_action(
        client,
        "duckport.execute_transaction",
        {
            "statements": [
                "CREATE TABLE IF NOT EXISTS app.ticks ("
                "  ts TIMESTAMP, symbol VARCHAR, price DOUBLE, qty BIGINT)",
                "DELETE FROM app.ticks",
            ]
        },
    )

    # 2. Bulk append two record batches.
    hr("bulk append 5 + 4 = 9 rows via DoPut")
    sch = pa.schema([
        ("ts", pa.timestamp("us")),
        ("symbol", pa.string()),
        ("price", pa.float64()),
        ("qty", pa.int64()),
    ])
    b1 = pa.record_batch([
        pa.array([1_800_000_000_000_000 + i for i in range(5)], type=pa.timestamp("us")),
        pa.array(["AAPL", "AAPL", "MSFT", "MSFT", "GOOG"], type=pa.string()),
        pa.array([150.1, 150.2, 310.5, 311.0, 2850.5], type=pa.float64()),
        pa.array([100, 200, 50, 75, 10], type=pa.int64()),
    ], schema=sch)
    b2 = pa.record_batch([
        pa.array([1_800_000_000_000_500 + i for i in range(4)], type=pa.timestamp("us")),
        pa.array(["GOOG", "TSLA", "TSLA", "NVDA"], type=pa.string()),
        pa.array([2851.0, 245.1, 245.2, 880.5], type=pa.float64()),
        pa.array([15, 5, 5, 25], type=pa.int64()),
    ], schema=sch)

    resp = do_append(client, "app", "ticks", [b1, b2])
    print("put result:", resp)
    assert len(resp) == 1
    assert resp[0]["schema"] == "app"
    assert resp[0]["table"] == "ticks"
    assert resp[0]["batches"] == 2
    assert resp[0]["rows_appended"] == 9

    # 3. Verify via Airport read plane.
    hr("verify rows landed via Airport SELECT")
    con = duckdb.connect(":memory:")
    con.execute("INSTALL airport FROM community; LOAD airport")
    con.execute(f"ATTACH '{SERVER}' AS dp (TYPE AIRPORT)")
    total = con.execute("SELECT COUNT(*) FROM dp.app.ticks").fetchone()[0]
    print("row count:", total)
    assert total == 9
    by_sym = dict(
        con.execute(
            "SELECT symbol, SUM(qty) FROM dp.app.ticks GROUP BY symbol ORDER BY symbol"
        ).fetchall()
    )
    print("qty by symbol:", by_sym)
    assert by_sym == {"AAPL": 300, "GOOG": 25, "MSFT": 125, "NVDA": 25, "TSLA": 10}
    con.close()

    # 4. Schema mismatch: wrong column count -> rollback.
    hr("schema mismatch: extra column -> rolled back, table unchanged")
    bad_schema = pa.schema([
        ("ts", pa.timestamp("us")),
        ("symbol", pa.string()),
        ("price", pa.float64()),
        ("qty", pa.int64()),
        ("extra_col", pa.int32()),
    ])
    bad_batch = pa.record_batch([
        pa.array([2_000_000_000_000_000], type=pa.timestamp("us")),
        pa.array(["BAD"], type=pa.string()),
        pa.array([999.0], type=pa.float64()),
        pa.array([999], type=pa.int64()),
        pa.array([42], type=pa.int32()),
    ], schema=bad_schema)
    try:
        do_append(client, "app", "ticks", [bad_batch])
        print("ERROR: expected schema mismatch failure"); sys.exit(1)
    except flight.FlightError as e:
        print("got expected error:", str(e)[:160], "...")

    con = duckdb.connect(":memory:")
    con.execute("INSTALL airport FROM community; LOAD airport")
    con.execute(f"ATTACH '{SERVER}' AS dp (TYPE AIRPORT)")
    total = con.execute("SELECT COUNT(*) FROM dp.app.ticks").fetchone()[0]
    has_bad = con.execute("SELECT COUNT(*) FROM dp.app.ticks WHERE symbol='BAD'").fetchone()[0]
    print("after bad append -> count=", total, "bad rows=", has_bad)
    assert total == 9
    assert has_bad == 0
    con.close()

    # 5. Missing table: appender open fails.
    hr("missing table -> error")
    try:
        do_append(client, "app", "no_such_table", [b1])
        print("ERROR: expected missing-table failure"); sys.exit(1)
    except flight.FlightError as e:
        print("got expected error:", str(e)[:160], "...")

    # 6. Bad descriptor path: wrong length.
    hr("bad descriptor path -> invalid_argument")
    try:
        desc = flight.FlightDescriptor.for_path("duckport.append", "app")
        writer, reader = client.do_put(desc, b1.schema)
        writer.write_batch(b1)
        writer.done_writing()
        while reader.read() is not None:
            pass
        writer.close()
        print("ERROR: expected bad-path failure"); sys.exit(1)
    except (flight.FlightError, pa.ArrowInvalid) as e:
        print("got expected error:", str(e)[:160], "...")

    # 7. Non-duckport descriptor path: Unimplemented.
    hr("non-duckport path -> Unimplemented")
    try:
        desc = flight.FlightDescriptor.for_path("some", "other", "thing")
        writer, reader = client.do_put(desc, b1.schema)
        writer.write_batch(b1)
        writer.done_writing()
        while reader.read() is not None:
            pass
        writer.close()
        print("ERROR: expected Unimplemented"); sys.exit(1)
    except (flight.FlightError, pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
        print("got expected error:", str(e)[:160], "...")

    print("\nALL GREEN (phase 2b)")


if __name__ == "__main__":
    main()
