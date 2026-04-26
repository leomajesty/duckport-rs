#!/usr/bin/env python3
"""Phase 1 — Airport read plane smoke test.

Verifies that a DuckDB client can ATTACH the duckport-rs server via the
Airport extension and run basic SELECTs against the demo seed data.

Prerequisites:
  DUCKPORT_SEED_DEMO=1 cargo run --bin duckport-server
"""

import os
import sys

import duckdb

SERVER = os.getenv("DUCKPORT_SERVER", "grpc://127.0.0.1:50051")


def main():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL airport FROM community")
    con.execute("LOAD airport")
    con.execute(f"ATTACH '{SERVER}' AS duckport (TYPE AIRPORT)")

    print(">>> SHOW ALL TABLES")
    for row in con.execute("SHOW ALL TABLES").fetchall():
        print(row)

    print("\n>>> SELECT * FROM duckport.app.users")
    rows = con.execute("SELECT * FROM duckport.app.users ORDER BY id").fetchall()
    print("rows =", rows)

    print("\n>>> SELECT * FROM duckport.app.users WHERE id > 1")
    rows = con.execute("SELECT * FROM duckport.app.users WHERE id > 1").fetchall()
    print("rows =", rows)

    print("\n>>> SELECT COUNT(*) FROM duckport.app.users")
    rows = con.execute("SELECT COUNT(*) FROM duckport.app.users").fetchall()
    print("rows =", rows)

    print("\n>>> SELECT * FROM duckport.app.ingestor_watermarks")
    rows = con.execute("SELECT * FROM duckport.app.ingestor_watermarks").fetchall()
    print("rows =", rows)

    print("\nALL GREEN (phase 1)")
    sys.exit(0)


if __name__ == "__main__":
    main()
