#!/usr/bin/env python3
"""Phase 2a — DoAction write plane end-to-end test.

Covers: duckport.ping, duckport.execute, duckport.execute_transaction,
        rollback on error, argument validation, unknown action.

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
    assert len(results) == 1, f"expected 1 result, got {len(results)}"
    return json.loads(bytes(results[0].body))


def main():
    client = flight.FlightClient(SERVER)

    hr("list_actions")
    actions = list(client.list_actions())
    for a in actions:
        t = a.type if hasattr(a, "type") else a[0]
        print(" -", t)

    hr("duckport.ping")
    ping = do_action(client, "duckport.ping", None)
    print(ping)
    assert ping["server"] == "duckport"
    assert ping["catalog"] == "duckport"
    assert ping["duckdb_version"].startswith("v1.5")

    hr("duckport.execute: create table + insert rows")
    resp = do_action(
        client,
        "duckport.execute_transaction",
        {
            "statements": [
                "CREATE TABLE IF NOT EXISTS app.orders (id BIGINT, qty INT, side VARCHAR)",
                "DELETE FROM app.orders",
                "INSERT INTO app.orders VALUES (1, 100, 'BUY'), (2, 50, 'SELL'), (3, 75, 'BUY')",
            ],
        },
    )
    print(resp)
    assert resp["rows_affected"] == [0, 0, 3]

    hr("Airport client: verify writes are visible")
    con = duckdb.connect(":memory:")
    con.execute("INSTALL airport FROM community; LOAD airport")
    con.execute(f"ATTACH '{SERVER}' AS dp (TYPE AIRPORT)")
    rows = con.execute("SELECT * FROM dp.app.orders ORDER BY id").fetchall()
    print("rows:", rows)
    assert rows == [(1, 100, "BUY"), (2, 50, "SELL"), (3, 75, "BUY")]
    total_qty = con.execute("SELECT sum(qty) FROM dp.app.orders").fetchone()[0]
    print("sum(qty):", total_qty)
    assert total_qty == 225
    con.close()

    hr("duckport.execute: single UPDATE")
    resp = do_action(
        client,
        "duckport.execute",
        {"sql": "UPDATE app.orders SET qty = qty * 2 WHERE side = 'BUY'"},
    )
    print(resp)
    assert resp["rows_affected"] == 2

    hr("duckport.execute_transaction: test ROLLBACK on bad statement")
    try:
        do_action(
            client,
            "duckport.execute_transaction",
            {
                "statements": [
                    "UPDATE app.orders SET qty = 999 WHERE id = 1",
                    "UPDATE app.orders SET qty = 'not-a-number'",  # type error
                    "UPDATE app.orders SET qty = 777 WHERE id = 2",
                ],
            },
        )
        print("ERROR: expected failure, got success"); sys.exit(1)
    except flight.FlightError as e:
        print("got expected error:", str(e)[:120], "...")

    hr("Verify rollback: id=1 should still have doubled qty (200), not 999")
    con = duckdb.connect(":memory:")
    con.execute("INSTALL airport FROM community; LOAD airport")
    con.execute(f"ATTACH '{SERVER}' AS dp (TYPE AIRPORT)")
    rows = con.execute("SELECT * FROM dp.app.orders ORDER BY id").fetchall()
    print("rows:", rows)
    assert rows == [(1, 200, "BUY"), (2, 50, "SELL"), (3, 150, "BUY")], f"got {rows}"
    con.close()

    expected_errors = (flight.FlightError, pa.ArrowInvalid)

    hr("duckport.execute: empty SQL -> argument error")
    try:
        do_action(client, "duckport.execute", {"sql": "   "})
        print("ERROR: expected failure"); sys.exit(1)
    except expected_errors as e:
        print("got expected error:", str(e)[:120], "...")

    hr("duckport: unknown action -> argument error")
    try:
        do_action(client, "duckport.nonexistent", {})
        print("ERROR: expected failure"); sys.exit(1)
    except expected_errors as e:
        print("got expected error:", str(e)[:120], "...")

    print("\nALL GREEN (phase 2a)")


if __name__ == "__main__":
    main()
