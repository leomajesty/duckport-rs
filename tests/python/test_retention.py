#!/usr/bin/env python3
"""Rolling-window retention end-to-end test (duckport.retention_run).

Covers: dry run, real run, skip of tables with a PRIMARY KEY, keep_days below the
minimum, parquet archive verification, retention_log rows, concurrent writes during a
run, and argument validation.

Prerequisites (the archive directory must be writable by the server and readable here):
  DUCKPORT_DB_PATH=/tmp/rt.db cargo run --bin duckport-server
  DUCKPORT_TEST_ARCHIVE_DIR=/tmp/duckport-retention-archive python test_retention.py
"""

import glob
import json
import os
import shutil
import sys
import threading
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq

SERVER = os.getenv("DUCKPORT_SERVER", "grpc://127.0.0.1:50051")
ARCHIVE_DIR = os.getenv("DUCKPORT_TEST_ARCHIVE_DIR", "/tmp/duckport-retention-archive")
KEEP_DAYS = 10
DAYS = 30
SYMBOLS = 4
TABLES = ("rt_kline", "rt_pk", "rt_arch", "rt_small", "rt_side", "rt_big", "rt_fail")


def hr(title):
    print("\n" + "=" * 6 + f" {title} " + "=" * 6)


def do_action(client, action_type, body):
    body_bytes = json.dumps(body).encode("utf-8") if body is not None else b""
    results = list(client.do_action(flight.Action(action_type, body_bytes)))
    assert len(results) == 1, f"expected 1 result, got {len(results)}"
    return json.loads(bytes(results[0].body))


def query(client, sql):
    ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())
    return client.do_get(ticket).read_all().to_pylist()


def scalar(client, sql):
    return list(query(client, sql)[0].values())[0]


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_kline(client, table, last_hour, primary_key=False, symbols=SYMBOLS, minutes=False):
    """DAYS days of rows up to last_hour: hourly by default, one row per minute with minutes=True."""
    pk = ", PRIMARY KEY (open_time, symbol)" if primary_key else ""
    first = (last_hour - timedelta(days=DAYS - 1)).replace(hour=0)
    step, unit = (60, "MINUTE") if minutes else (3600, "HOUR")
    hours = int((last_hour - first).total_seconds() // step) + 1
    do_action(client, "duckport.execute_transaction", {"statements": [
        f"DROP TABLE IF EXISTS data.{table}",
        f"CREATE TABLE data.{table} (open_time TIMESTAMP, symbol VARCHAR, close DOUBLE{pk})",
        f"INSERT INTO data.{table} SELECT TIMESTAMP '{first:%Y-%m-%d %H:%M:%S}' + INTERVAL (h) {unit}, "
        f"'S' || s, random() FROM range({hours}) a(h), range({symbols}) b(s)",
        f"INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time) "
        f"VALUES ('{table}', 'test', 3600, 'open_time', TIMESTAMP '{first:%Y-%m-%d}', "
        f"TIMESTAMP '{last_hour:%Y-%m-%d %H:%M:%S}') "
        f"ON CONFLICT (table_name) DO UPDATE SET duck_time = excluded.duck_time, time_column = excluded.time_column",
    ]})


def set_policy(client, table, keep_days, archive_dir=None):
    archive = f"'{archive_dir}'" if archive_dir else "NULL"
    do_action(client, "duckport.execute", {"sql":
        f"INSERT OR REPLACE INTO data.retention_policy (table_name, keep_days, batch_days, archive_dir, enabled) "
        f"VALUES ('{table}', {keep_days}, 1, {archive}, true)"})


def cleanup(client):
    names = ", ".join(f"'{t}'" for t in TABLES)
    stmts = [f"DROP TABLE IF EXISTS data.{t}" for t in TABLES]
    stmts += [
        f"DELETE FROM data.retention_policy WHERE table_name IN ({names})",
        f"DELETE FROM data.watermark WHERE table_name IN ({names})",
    ]
    do_action(client, "duckport.execute_transaction", {"statements": stmts})
    shutil.rmtree(ARCHIVE_DIR, ignore_errors=True)


def table_report(resp, table):
    rows = [t for t in resp["tables"] if t["table"] == table]
    assert len(rows) == 1, f"no report for {table}: {resp}"
    return rows[0]


def main():
    client = flight.FlightClient(SERVER)
    cleanup(client)

    hr("list_actions advertises duckport.retention_run")
    types = [a.type for a in client.list_actions()]
    assert "duckport.retention_run" in types, types

    hr("system tables exist")
    for t in ("retention_policy", "retention_log"):
        n = scalar(client, f"SELECT count(*) FROM duckdb_tables() WHERE schema_name='data' AND table_name='{t}'")
        assert n == 1, f"data.{t} missing"

    last_hour = now_utc().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    cutoff = min(last_hour, now_utc()).date() - timedelta(days=KEEP_DAYS)
    first_day = (last_hour - timedelta(days=DAYS - 1)).date()
    expected_days = (cutoff - first_day).days
    for t in ("rt_kline", "rt_arch", "rt_small", "rt_side", "rt_fail"):
        create_kline(client, t, last_hour)
    create_kline(client, "rt_pk", last_hour, primary_key=True)
    total = scalar(client, "SELECT count(*) FROM data.rt_kline")
    expected = scalar(client, f"SELECT count(*) FROM data.rt_kline WHERE open_time < DATE '{cutoff}'")
    print(f"rows={total} expected_to_delete={expected} cutoff={cutoff}")
    assert expected > 0

    set_policy(client, "rt_kline", KEEP_DAYS)
    set_policy(client, "rt_pk", KEEP_DAYS)
    set_policy(client, "rt_small", 3)
    set_policy(client, "rt_arch", KEEP_DAYS, ARCHIVE_DIR)

    hr("dry run counts rows and deletes nothing")
    resp = do_action(client, "duckport.retention_run", {"table": "rt_kline", "dry_run": True})
    print(resp)
    r = table_report(resp, "rt_kline")
    assert resp["dry_run"] is True
    assert r["status"] == "dry_run", r
    assert r["rows_deleted"] == expected, r
    assert r["cutoff"] == str(cutoff), r
    assert r["first_day"] == str(first_day), r
    assert r["last_day"] == str(cutoff - timedelta(days=1)), r
    assert scalar(client, "SELECT count(*) FROM data.rt_kline") == total

    hr("real run deletes everything before the cutoff, writes going on meanwhile")
    errors = []

    def writer():
        try:
            for i in range(50):
                do_action(client, "duckport.execute", {"sql":
                    f"INSERT INTO data.rt_side VALUES (TIMESTAMP '{last_hour:%Y-%m-%d %H:%M:%S}' + INTERVAL ({i + 1}) MINUTE, 'W', {i})"})
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    th = threading.Thread(target=writer)
    th.start()
    resp = do_action(client, "duckport.retention_run", {"table": "rt_kline", "dry_run": False})
    th.join()
    print(resp)
    assert not errors, errors
    r = table_report(resp, "rt_kline")
    assert r["status"] in ("ok", "ok_pending_reclaim"), r
    assert r["rows_deleted"] == expected, r
    left_min = scalar(client, "SELECT CAST(min(open_time) AS DATE) FROM data.rt_kline")
    print("min(open_time) after run:", left_min)
    assert str(left_min) == str(cutoff), left_min
    assert scalar(client, "SELECT count(*) FROM data.rt_kline") == total - expected
    assert scalar(client, "SELECT count(*) FROM data.rt_side WHERE symbol = 'W'") == 50

    hr("second run has nothing to do")
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_kline", "dry_run": False}), "rt_kline")
    assert r["status"] == "ok_nothing_to_do", r

    hr("table with a PRIMARY KEY is skipped")
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_pk", "dry_run": False}), "rt_pk")
    assert r["status"] == "skipped_has_index", r
    assert scalar(client, "SELECT count(*) FROM data.rt_pk") == total

    hr("keep_days below DUCKPORT_RETENTION_MIN_KEEP_DAYS is refused")
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_small", "dry_run": False}), "rt_small")
    assert r["status"] == "skipped_keep_days_below_min", r
    assert scalar(client, "SELECT count(*) FROM data.rt_small") == total

    hr("archive: one verified parquet file per deleted day")
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_arch", "dry_run": False}), "rt_arch")
    print(r)
    assert r["status"] in ("ok", "ok_pending_reclaim"), r
    assert r["rows_archived"] == r["rows_deleted"] == expected, r
    files = sorted(glob.glob(os.path.join(ARCHIVE_DIR, "rt_arch", "rt_arch_*.parquet")))
    assert len(files) == expected_days, (len(files), expected_days)
    assert not glob.glob(os.path.join(ARCHIVE_DIR, "rt_arch", "*.tmp"))
    archived_rows = sum(pq.read_metadata(f).num_rows for f in files)
    assert archived_rows == expected, archived_rows

    hr("archive: rows backfilled into an archived day are merged into its file")
    day0 = first_day.strftime("%Y-%m-%d")
    do_action(client, "duckport.execute", {"sql":
        f"INSERT INTO data.rt_arch VALUES (TIMESTAMP '{day0} 12:34:00', 'BACKFILL', 1.0)"})
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_arch", "dry_run": False}), "rt_arch")
    print(r)
    assert r["status"] == "ok" and r["rows_archived"] == 1 and r["rows_deleted"] == 1, r
    day_file = os.path.join(ARCHIVE_DIR, "rt_arch", f"rt_arch_{day0}.parquet")
    merged = pq.read_table(day_file)
    assert merged.num_rows == SYMBOLS * 24 + 1, merged.num_rows
    assert "BACKFILL" in merged.column("symbol").to_pylist()

    hr("a failure after some batches still reports what was deleted")
    # A directory where the third day's archive file should go makes that batch fail.
    bad_day = (first_day + timedelta(days=2)).strftime("%Y-%m-%d")
    os.makedirs(os.path.join(ARCHIVE_DIR, "rt_fail", f"rt_fail_{bad_day}.parquet"), exist_ok=True)
    set_policy(client, "rt_fail", KEEP_DAYS, ARCHIVE_DIR)
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_fail", "dry_run": False}), "rt_fail")
    print(r)
    assert r["status"] == "error", r
    assert r["rows_deleted"] == 2 * SYMBOLS * 24 and r["rows_archived"] == r["rows_deleted"], r
    assert r["first_day"] == str(first_day) and r["cutoff"] == str(cutoff), r
    assert r["free_blocks_after"] is not None, r
    assert scalar(client, f"SELECT count(*) FROM data.rt_fail WHERE open_time < DATE '{bad_day}'") == 0
    logged = query(client, "SELECT rows_deleted, status FROM data.retention_log WHERE table_name = 'rt_fail'")
    assert logged == [{"rows_deleted": 2 * SYMBOLS * 24, "status": "error"}], logged

    hr("more than a row group deleted: CHECKPOINT frees blocks")
    create_kline(client, "rt_big", last_hour, symbols=8, minutes=True)
    do_action(client, "duckport.execute", {"sql": "CHECKPOINT"})
    set_policy(client, "rt_big", KEEP_DAYS)
    r = table_report(do_action(client, "duckport.retention_run", {"table": "rt_big", "dry_run": False}), "rt_big")
    print(r)
    assert r["rows_deleted"] >= 122_880, r
    assert r["status"] == "ok", r
    assert r["free_blocks_after"] > r["free_blocks_before"], r

    hr("retention_log has one row per table and run")
    names = ", ".join(f"'{t}'" for t in TABLES)
    logs = query(client, "SELECT table_name, status, dry_run, rows_deleted FROM data.retention_log "
                         f"WHERE table_name IN ({names}) ORDER BY started_at")
    for row in logs:
        print(" ", row)
    statuses = {(x["table_name"], x["status"]) for x in logs}
    for item in [("rt_kline", "dry_run"), ("rt_kline", "ok_nothing_to_do"), ("rt_pk", "skipped_has_index"),
                 ("rt_small", "skipped_keep_days_below_min")]:
        assert item in statuses, (item, statuses)

    hr("validation errors")
    for body in ({"table": "nope_missing"}, {"table": "bad; DROP TABLE x"}):
        try:
            do_action(client, "duckport.retention_run", body)
            print("ERROR: expected failure for", body); sys.exit(1)
        except (flight.FlightError, pa.ArrowException) as e:
            print("expected failure:", str(e).splitlines()[0])

    cleanup(client)
    print("\nALL RETENTION TESTS PASSED")


if __name__ == "__main__":
    main()
