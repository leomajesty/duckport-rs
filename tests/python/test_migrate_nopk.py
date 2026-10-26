#!/usr/bin/env python3
"""scripts/migrate_nopk.py end-to-end test: plan, migrate, rollback, finalize.

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/mg.db cargo run --bin duckport-server    (fresh database)
"""

import json
import os
import subprocess
import sys

import pyarrow.flight as flight

SERVER = os.getenv("DUCKPORT_SERVER", "grpc://127.0.0.1:50051")
ADDR = SERVER.split("://", 1)[-1]
SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts", "migrate_nopk.py")


def hr(title):
    print("\n" + "=" * 6 + f" {title} " + "=" * 6)


def do_action(client, action_type, body):
    res = list(client.do_action(flight.Action(action_type, json.dumps(body).encode())))
    return json.loads(bytes(res[0].body))


def tx(client, *stmts):
    return do_action(client, "duckport.execute_transaction", {"statements": list(stmts)})


def scalar(client, sql):
    ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())
    rows = client.do_get(ticket).read_all().to_pylist()
    return list(rows[0].values())[0]


def keys(client, table):
    return scalar(client, "SELECT count(*) FROM duckdb_constraints() WHERE schema_name='data' "
                          f"AND table_name='{table}' AND constraint_type IN ('PRIMARY KEY','UNIQUE')")


def exists(client, table):
    return scalar(client, f"SELECT count(*) FROM duckdb_tables() WHERE schema_name='data' AND table_name='{table}'") == 1


def migrate(*args):
    cmd = [sys.executable, SCRIPT, "--addr", ADDR, *args]
    p = subprocess.run(cmd, capture_output=True, text=True)
    print(p.stdout + p.stderr)
    return p.returncode, p.stdout


def setup(client):
    tx(client,
       "DROP TABLE IF EXISTS data.mg_kline", "DROP TABLE IF EXISTS data.mg_kline__pk_old",
       "DROP TABLE IF EXISTS data.mg_other", "DROP TABLE IF EXISTS data.exginfo",
       "DROP TABLE IF EXISTS data.exginfo__pk_old",
       "DROP TABLE IF EXISTS data.mg_empty", "DROP TABLE IF EXISTS data.mg_empty__pk_old",
       "DROP TABLE IF EXISTS data.ab_pk_old", "DROP TABLE IF EXISTS data.orders_nopk",
       # decoys: their names end like the migration suffixes but are ordinary tables
       "CREATE TABLE data.ab_pk_old (x INTEGER)", "INSERT INTO data.ab_pk_old VALUES (1)",
       "CREATE TABLE data.orders_nopk (x INTEGER)", "INSERT INTO data.orders_nopk VALUES (2)",
       # empty at migration time; rows arrive afterwards
       "CREATE TABLE data.mg_empty (ts TIMESTAMP, k VARCHAR, PRIMARY KEY (ts, k))",
       "CREATE TABLE data.mg_kline (open_time TIMESTAMP, symbol VARCHAR, close DOUBLE, PRIMARY KEY (open_time, symbol))",
       # three months, inserted out of time order on purpose
       "INSERT INTO data.mg_kline SELECT TIMESTAMP '2026-01-01' + INTERVAL (h) HOUR, 'S' || s, h "
       "FROM range(24 * 90) a(h), range(5) b(s) ORDER BY s, h DESC",
       "CREATE TABLE data.mg_other (ts TIMESTAMP, k VARCHAR, PRIMARY KEY (ts, k))",
       "INSERT INTO data.mg_other VALUES (TIMESTAMP '2026-01-01', 'a')",
       "CREATE TABLE data.exginfo (market VARCHAR, symbol VARCHAR, status VARCHAR, "
       "created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (market, symbol))",
       "INSERT INTO data.exginfo (market, symbol, status) VALUES ('usdt_perp', 'BTCUSDT', 'TRADING'), "
       "('usdt_spot', 'ETHUSDT', 'TRADING')",
       # what duckdb/duckdb#22279 left in production: strings of earlier, reverted appends glued together
       "INSERT INTO data.exginfo (market, symbol, status) VALUES "
       "('usdt_spot' || repeat('usdt_perp', 300), repeat('BTCUSDTETHUSDT', 300), repeat('TRADING', 300))",
       "INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time) "
       "VALUES ('mg_kline', 'test', 3600, 'open_time', TIMESTAMP '2026-01-01', TIMESTAMP '2026-03-31 23:00:00') "
       "ON CONFLICT (table_name) DO UPDATE SET time_column = excluded.time_column",
       "INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column) "
       "VALUES ('mg_empty', 'test', 3600, 'ts') ON CONFLICT (table_name) DO UPDATE SET time_column = excluded.time_column",
       "INSERT OR REPLACE INTO data.config_dict VALUES ('write_mode:mg_kline', 'anti_join'), "
       "('write_mode:exginfo', 'anti_join'), ('write_mode:mg_empty', 'anti_join')",
       )


def main():
    client = flight.FlightClient(SERVER)
    setup(client)
    total = scalar(client, "SELECT count(*) FROM data.mg_kline")

    hr("plan lists ready and skipped tables, changes nothing")
    rc, out = migrate("plan")
    assert rc == 0, rc
    assert "mg_kline" in out and "mg_other" in out and "writer not upgraded" in out
    assert keys(client, "mg_kline") == 1

    hr("--tables comes after the subcommand and limits the plan")
    rc, out = migrate("plan", "--tables", "mg_kline")
    assert rc == 0 and "mg_kline" in out and "exginfo" not in out, out
    rc, _ = migrate("plan", "--tables", "mg_kline", "no_such_table")
    assert rc == 1
    rc, _ = migrate("finalize", "--tables", "mg_kline")
    assert rc != 0, "--tables must be refused for finalize"

    hr("migrate rebuilds ready tables without keys")
    rc, out = migrate("migrate")
    assert rc == 0, rc
    assert keys(client, "mg_kline") == 0
    assert scalar(client, "SELECT count(*) FROM data.mg_kline") == total
    assert exists(client, "mg_kline__pk_old") and keys(client, "mg_kline__pk_old") == 1
    assert keys(client, "mg_other") == 1, "table without write_mode marker must be left alone"
    assert not exists(client, "mg_other__pk_old")
    # rows are now stored in time order: the first row group starts at the earliest hour
    first = scalar(client, "SELECT CAST(open_time AS VARCHAR) FROM data.mg_kline LIMIT 1")
    assert first.startswith("2026-01-01 00:00"), first

    hr("exginfo: corrupted row dropped, column default kept")
    assert keys(client, "exginfo") == 0
    assert scalar(client, "SELECT count(*) FROM data.exginfo") == 2
    default = scalar(client, "SELECT column_default FROM duckdb_columns() WHERE schema_name='data' "
                             "AND table_name='exginfo' AND column_name='created_time'")
    assert default is not None and "CURRENT_TIMESTAMP" in default.upper(), default

    hr("plan refuses to start while a migration is not finalized")
    rc, out = migrate("plan")
    assert rc == 1 and "finalize" in out

    hr("rollback restores the keyed tables and keeps rows written after the migration")
    tx(client, "INSERT INTO data.mg_kline VALUES (TIMESTAMP '2026-04-01 00:00:00', 'NEW', 1)",
       # a backfilled row inside the old time range
       "INSERT INTO data.mg_kline VALUES (TIMESTAMP '2026-01-15 00:30:00', 'BACKFILL', 1)",
       "DELETE FROM data.exginfo WHERE symbol = 'ETHUSDT'",
       "INSERT INTO data.mg_empty VALUES (TIMESTAMP '2026-05-01', 'a'), (TIMESTAMP '2026-05-02', 'b')")
    assert keys(client, "mg_empty") == 0
    rc, _ = migrate("rollback")
    assert rc == 0
    assert keys(client, "mg_kline") == 1 and not exists(client, "mg_kline__pk_old")
    assert scalar(client, "SELECT count(*) FROM data.mg_kline") == total + 2
    assert keys(client, "exginfo") == 1
    assert scalar(client, "SELECT count(*) FROM data.exginfo") == 1
    assert keys(client, "mg_empty") == 1
    assert scalar(client, "SELECT count(*) FROM data.mg_empty") == 2, "rows written into an empty table were lost"
    assert scalar(client, "SELECT count(*) FROM data.ab_pk_old") == 1, "decoy table was touched"
    assert scalar(client, "SELECT count(*) FROM data.orders_nopk") == 1, "decoy table was touched"

    hr("migrate again, then finalize drops the old tables")
    rc, _ = migrate("migrate")
    assert rc == 0
    rc, _ = migrate("finalize")
    assert rc == 0
    assert not exists(client, "mg_kline__pk_old") and not exists(client, "exginfo__pk_old")
    assert keys(client, "mg_kline") == 0
    assert scalar(client, "SELECT count(*) FROM data.mg_kline") == total + 2
    assert exists(client, "ab_pk_old") and exists(client, "orders_nopk"), "finalize dropped a decoy table"

    tx(client, "DROP TABLE IF EXISTS data.mg_kline", "DROP TABLE IF EXISTS data.mg_other",
       "DROP TABLE IF EXISTS data.exginfo", "DROP TABLE IF EXISTS data.mg_empty",
       "DROP TABLE IF EXISTS data.ab_pk_old", "DROP TABLE IF EXISTS data.orders_nopk",
       "DELETE FROM data.watermark WHERE table_name IN ('mg_kline', 'mg_empty')",
       "DELETE FROM data.config_dict WHERE key LIKE 'write_mode:%'")
    print("\nALL MIGRATE_NOPK TESTS PASSED")


if __name__ == "__main__":
    main()
