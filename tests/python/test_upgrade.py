#!/usr/bin/env python3
"""scripts/upgrade.py end-to-end test: a legacy database is brought to the desired state.

Checks: blocked while plugins are old; online fixes, then the stop window with a backup
taken first; doctor clean afterwards; a second run does nothing; no row lost except the
quarantined ones (which are in their parquet files); rebuilt tables keep defaults and are
stored in time order; rollback restores the keyed tables with every row written since;
finalize removes the old tables and nothing else; compact verifies the rebuild per month
before dropping the copies and rewriting the catalog; not enough memory for the backup is
reported (exit 5); a failed backup changes nothing; corrupted and duplicate rows are only
deleted after an operator confirmed (--confirm-delete).

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/upgrade.db cargo run --bin duckport-server    (fresh database)
"""

import glob
import json
import os
import sys
import tempfile

import pyarrow.flight as flight
import pyarrow.parquet as pq

from reconcile_fixtures import (
    KLINE, NEW, OLD, SERVER, doctor, hr, legacy_state, reset, run_script, tx, unkeyed_with_duplicates,
)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
from reconcile import memory  # noqa: E402

QDIR = tempfile.mkdtemp(prefix="duckport-quarantine-")
BDIR = tempfile.mkdtemp(prefix="duckport-backup-")
STOPPED = NEW
RUNNING = [dict(i, active=True) for i in NEW]


def upgrade(instances, *args):
    rc, out = run_script("upgrade.py", instances, "--bloat-min-dead-rows", "10000", "--quarantine-dir", QDIR,
                         "--backup-dir", BDIR, *args)
    print(out)
    return rc, out


def execute(client, sql):
    list(client.do_action(flight.Action("duckport.execute", json.dumps({"sql": sql}).encode())))


def scalar(client, sql):
    t = client.do_get(flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())).read_all()
    return t.column(0)[0].as_py()


def keyed(client, table, db="current_database()"):
    return scalar(client, f"SELECT count(*) FROM duckdb_constraints() WHERE database_name = {db} "
                          f"AND schema_name = 'data' AND table_name = '{table}' "
                          "AND constraint_type = 'PRIMARY KEY'") > 0


def count(client, table):
    return scalar(client, f"SELECT count(*) FROM {table}")


def exists(client, table):
    return scalar(client, f"SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'data' "
                          f"AND table_name = '{table}'") == 1


def quarantined(run_id, rule, table):
    files = glob.glob(os.path.join(QDIR, run_id, f"{rule}.{table}*.parquet"))
    return sum(pq.read_table(f).num_rows for f in files)


def main():
    client = flight.FlightClient(SERVER)
    legacy_state(client)
    unkeyed_with_duplicates(client)
    tx(client,
       # the market's duck_time lives only in config_dict (plugin first days): L2 must move it first
       "UPDATE data.watermark SET duck_time = NULL WHERE table_name = 'usdt_perp_1m'",
       # a row from an earlier month written last: the rebuild must store rows in time order
       "INSERT INTO data.usdt_perp_1m (open_time, symbol) VALUES (TIMESTAMP '2026-09-30 23:59', 'S0')",
       # keyed and still empty when it is rebuilt; rows arrive afterwards
       f"CREATE TABLE data.usdt_perp_5m ({KLINE}, PRIMARY KEY (open_time, symbol))",
       # ordinary tables whose names merely end like the rebuild suffixes
       "CREATE TABLE data.ab_pk_old (x INTEGER)", "INSERT INTO data.ab_pk_old VALUES (1)",
       "CREATE TABLE data.orders_nopk (x INTEGER)", "INSERT INTO data.orders_nopk VALUES (2)")
    n1m = count(client, "data.usdt_perp_1m")

    hr("old plugins: blocked, nothing changed")
    rc, out = upgrade(OLD, "plan")
    assert rc == 2 and "阻断" in out, rc
    rc, _ = upgrade(OLD, "apply", "--run-id", "r0", "--levels", "online")
    assert rc == 2 and keyed(client, "usdt_perp_1m") and count(client, "data.retention_tasks") == 1

    hr("full-upgrade plan judges as if the plugins were upgraded: no block")
    rc, out = upgrade(OLD, "plan", "--assume-upgraded")
    assert rc == 4 and "按升级后估计" in out and "阻断" not in out, rc

    hr("plan with upgraded plugins: needs a stop window, backup planned, deletions shown")
    state_file = os.path.join(QDIR, "plan.state")
    rc, out = upgrade(RUNNING, "plan", "--state-out", state_file)
    assert rc == 4 and "停写窗口" in out and "备份到" in out, rc
    assert "需要人工确认的删除" in out and "copies=2" in out and "…(" in out, "samples of D1/D2 rows"
    assert open(state_file).read().split() == ["WINDOW=1", "BACKUP_OK=1", "DELETE_ROWS=11"]

    hr("not enough memory to rebuild the indexes in a backup: exit 5")
    limit = scalar(client, "SELECT current_setting('memory_limit')")
    tx(client, "SET memory_limit = '1GB'")
    rc, out = upgrade(RUNNING, "plan")
    tx(client, f"SET memory_limit = '{limit}'")
    assert rc == 5 and "内存不足以备份" in out, rc

    hr("online fixes without confirmation: corrupted and duplicate rows stay")
    rc, _ = upgrade(RUNNING, "apply", "--run-id", "r1a", "--levels", "online")
    assert rc == 0, rc
    assert count(client, "data.exginfo") == 501 and count(client, "data.usdt_spot_1m") == 20
    assert scalar(client, "SELECT string_agg(rule || ':' || status, ',' ORDER BY rule) FROM data.upgrade_log "
                          "WHERE run_id = 'r1a' AND rule IN ('D1', 'D2')") == "D1:skipped,D2:skipped"
    assert not exists(client, "retention_tasks"), "the other online fixes ran"

    hr("online fixes, deletions confirmed")
    rc, _ = upgrade(RUNNING, "apply", "--run-id", "r1", "--levels", "online", "--confirm-delete")
    assert rc == 0, rc
    assert scalar(client, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'watermark' "
                          "AND column_name = 'max_lag_seconds'") == 1
    assert quarantined("r1a", "L1", "retention_tasks") == 1
    assert scalar(client, "SELECT count(*) FROM data.config_dict WHERE ends_with(key, '_time')") == 0
    assert str(scalar(client, "SELECT duck_time FROM data.watermark WHERE table_name = 'usdt_perp_1m'")) \
        == "2026-10-26 02:00:00", "L2 moves the config_dict value into the empty watermark row"
    assert count(client, "data.exginfo") == 500 and quarantined("r1", "D1", "exginfo") == 1
    assert count(client, "data.usdt_spot_1m") == 10 and quarantined("r1", "D2", "usdt_spot_1m") == 10
    assert scalar(client, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'usdt_perp_1m' "
                          "AND column_name = 'avg_price'") == 1
    assert keyed(client, "usdt_perp_1m"), "rebuilds wait for the stop window"
    assert count(client, "main._staging_usdt_perp_1m") == 1, "staging is left alone while ingestors run"

    hr("backup fails (file exists): nothing changed")
    open(os.path.join(BDIR, "duckport-rx.db"), "w").close()
    rc, out = upgrade(STOPPED, "apply", "--run-id", "rx", "--levels", "online,stop", "--backup")
    assert rc == 1 and "备份失败" in out and keyed(client, "usdt_perp_1m"), rc
    assert count(client, "main._staging_usdt_perp_1m") == 1

    hr("stop window (ingestors stopped): backup, then fixes")
    rc, _ = upgrade(STOPPED, "apply", "--run-id", "r1", "--levels", "online,stop", "--backup")
    assert rc == 0, rc
    path = os.path.join(BDIR, "duckport-r1.db")
    tx(client, f"ATTACH '{path}' AS bk (READ_ONLY)")
    try:
        assert count(client, "bk.data.usdt_perp_1m") == n1m and keyed(client, "usdt_perp_1m", "'bk'"), \
            "the backup holds the state before the window"
        assert count(client, "bk.main._staging_usdt_perp_1m") == 1
    finally:
        tx(client, "DETACH bk")
    for t in ("usdt_perp_1m", "usdt_perp_5m", "exginfo", "usdt_perp_funding"):
        assert not keyed(client, t) and keyed(client, f"{t}__pk_old"), t
    assert count(client, "data.usdt_perp_1m") == n1m and count(client, "data.exginfo") == 500
    assert count(client, "data.usdt_perp_funding") == 1 and count(client, "data.usdt_perp_5m") == 0
    assert str(scalar(client, "SELECT open_time FROM data.usdt_perp_1m LIMIT 1")) == "2026-09-30 23:59:00", \
        "rebuilt rows are stored in time order"
    default = scalar(client, "SELECT column_default FROM duckdb_columns() WHERE table_name = 'exginfo' "
                             "AND column_name = 'created_time'")
    assert default and "CURRENT_TIMESTAMP" in default.upper(), default
    assert count(client, "main._staging_usdt_perp_1m") == 0 and quarantined("r1", "D3", "_staging_usdt_perp_1m") == 1
    assert str(scalar(client, "SELECT duck_time FROM data.watermark WHERE table_name = 'usdt_perp_funding'")) \
        == "2026-10-26 00:00:00", "D4 pulls the watermark back to the data"
    log = scalar(client, "SELECT count(*) FILTER (status = 'ok') || '/' || count(*) FROM data.upgrade_log "
                         "WHERE run_id = 'r1'")
    print("upgrade_log r1:", log)
    # online (r1a did S1 S2 L2 L1 B2): D1 D2; window: BACKUP D3 B1x4 D5 D4
    assert log == "10/10", log
    assert scalar(client, "SELECT detail FROM data.upgrade_log WHERE run_id = 'r1' AND rule = 'D4'") \
        == "duck_time 2026-11-30 00:00:00 → 2026-10-26 00:00:00"

    hr("doctor afterwards: hints only")
    rc, f = doctor(NEW)
    assert rc == 0, rc
    assert {x["rule"] for x in f} <= {"C1", "B2", "P1"}, f

    hr("second run does nothing")
    rc, _ = upgrade(STOPPED, "plan")
    assert rc == 0, rc

    hr("rollback restores the keyed tables with every row written since")
    tx(client, "INSERT INTO data.usdt_perp_1m (open_time, symbol) VALUES (TIMESTAMP '2026-10-26 02:00', 'S0')",
       # backfilled inside the old time range
       "INSERT INTO data.usdt_perp_1m (open_time, symbol) VALUES (TIMESTAMP '2026-10-26 00:00:30', 'BACKFILL')",
       "INSERT INTO data.usdt_perp_5m (open_time, symbol) VALUES (TIMESTAMP '2026-10-26', 'A'), "
       "(TIMESTAMP '2026-10-26 00:05', 'A')",
       "DELETE FROM data.exginfo WHERE symbol = 'S0'")
    rc, _ = upgrade(STOPPED, "rollback", "--run-id", "r2")
    assert rc == 0, rc
    assert keyed(client, "usdt_perp_1m") and count(client, "data.usdt_perp_1m") == n1m + 2
    assert keyed(client, "usdt_perp_5m") and count(client, "data.usdt_perp_5m") == 2, \
        "rows written into a table that was empty at the rebuild were lost"
    assert keyed(client, "exginfo") and count(client, "data.exginfo") == 499, "snapshot replaced by the current one"
    assert scalar(client, "SELECT count(*) FROM duckdb_tables() WHERE ends_with(table_name, '__pk_old')") == 0
    assert count(client, "data.ab_pk_old") == 1 and count(client, "data.orders_nopk") == 1, "decoy touched"

    hr("upgrade again (no backup), then finalize")
    rc, _ = upgrade(STOPPED, "apply", "--run-id", "r3", "--levels", "online,stop")
    assert rc == 0, rc
    rc, _ = upgrade(STOPPED, "finalize", "--run-id", "r3")
    assert rc == 0, rc
    rc, f = doctor(NEW)
    assert rc == 0 and not [x for x in f if x["rule"] == "C1"], f
    assert count(client, "data.usdt_perp_1m") == n1m + 2 and not keyed(client, "usdt_perp_1m")
    assert exists(client, "ab_pk_old") and exists(client, "orders_nopk"), "finalize dropped a decoy table"

    hr("compact: a rebuild that is a true superset passes verification")
    legacy_state(client)  # also drops the decoys, so put them back
    tx(client, "CREATE TABLE data.ab_pk_old (x INTEGER)", "INSERT INTO data.ab_pk_old VALUES (1)",
       "CREATE TABLE data.orders_nopk (x INTEGER)", "INSERT INTO data.orders_nopk VALUES (2)")
    # a second month, so the per-month comparison has more than one segment to compare
    tx(client, "INSERT INTO data.usdt_perp_1m (open_time, symbol) "
               "SELECT TIMESTAMP '2026-11-26 00:00' + INTERVAL (m) MINUTE, 'S' || s "
               "FROM range(60) a(m), range(5) b(s)")
    rc, _ = upgrade(STOPPED, "apply", "--run-id", "c1", "--levels", "online,stop")
    assert rc == 0, rc
    assert exists(client, "usdt_perp_1m__pk_old"), "no rebuild happened, nothing to compact"
    n_live = count(client, "data.usdt_perp_1m")
    rc, out = upgrade(STOPPED, "compact-verify")
    assert rc == 0, out

    hr("compact: a month that lost rows is caught even when the totals still match")
    tx(client,  # 10 rows out of October, 10 back in November: count and first/last unchanged
       "DELETE FROM data.usdt_perp_1m WHERE open_time BETWEEN TIMESTAMP '2026-10-26 00:10' "
       "AND TIMESTAMP '2026-10-26 00:11'",
       "INSERT INTO data.usdt_perp_1m (open_time, symbol) "
       "SELECT TIMESTAMP '2026-11-26 00:10', 'H' || s FROM range(10) r(s)")
    assert count(client, "data.usdt_perp_1m") == n_live, "totals must match or the test proves nothing"
    rc, out = upgrade(STOPPED, "compact-verify")
    assert rc == 1, "a hole inside October went unnoticed"
    assert "个月份行数少于旧表" in out, out

    hr("compact: drop the copies and write a new database that reproduces the catalog")
    tx(client, "DELETE FROM data.usdt_perp_1m WHERE starts_with(symbol, 'H')",
       "INSERT INTO data.usdt_perp_1m (open_time, symbol) "
       "SELECT TIMESTAMP '2026-10-26 00:00' + INTERVAL (m) MINUTE, 'S' || s "
       "FROM range(10, 12) a(m), range(5) b(s)")
    rc, out = upgrade(STOPPED, "compact-verify")
    assert rc == 0, out
    newdb = os.path.join(tempfile.mkdtemp(prefix="duckport-compact-"), "new.db")
    base = newdb + ".baseline.json"
    rc, out = upgrade(STOPPED, "compact-build", "--run-id", "c2", "--new-db", newdb, "--baseline-out", base)
    assert rc == 0, out
    assert os.path.getsize(newdb) > 0, "no database was written"
    assert scalar(client, "SELECT count(*) FROM duckdb_tables() WHERE ends_with(table_name, '__pk_old')") == 0
    assert count(client, "data.usdt_perp_1m") == n_live, "compacting changed the live data"
    assert exists(client, "ab_pk_old") and exists(client, "orders_nopk"), "compact dropped a decoy table"
    # the baseline covers the live tables only, so the server's database still has to match it
    rc, out = upgrade(STOPPED, "compact-check", "--baseline", base)
    assert rc == 0, out

    hr("memory: heavy work runs under a budget when memory_limit exceeds the host")
    G = 1024 ** 3
    assert memory.parse_size("3.7 GiB") == int(3.7 * G) and memory.parse_size("8GB") == 8 * 10 ** 9 \
        and memory.parse_size("953.6 MiB") == int(953.6 * 1024 ** 2) and memory.parse_size("junk") is None
    M = 1024 ** 2
    # grid102 at the second kill: 3.8 GB box, 1.26 GB available, DuckDB holding 473 MiB, 8GB configured
    assert memory.suggest(3831 * M, 1258 * M, 473 * M) == 768 * M, "half of (available + held), in 256 MiB steps"
    assert memory.budget(8 * G, 3831 * M, 1258 * M, 473 * M) == 768 * M
    assert memory.budget(8 * G, 64 * G, 60 * G, 0) == 8 * G, "a limit the host can honour is kept"
    assert memory.suggest(4 * G, 3 * G, 3 * G) == 2 * G, "never more than half the machine"
    assert memory.suggest(4 * G, 100 * M, 0) == memory.FLOOR and memory.suggest(4 * G, None, 0) is None
    assert memory.budget(None, None, None, None) is None and memory.mib(768 * M) == "768MiB"
    before = scalar(client, "SELECT current_setting('memory_limit')")
    execute(client, "SET memory_limit = '400GB'")
    rc, out = upgrade(STOPPED, "compact-build", "--run-id", "c5", "--new-db", newdb + ".mem", "--baseline-out", base)
    assert rc == 0 and "超过本机" in out and "，降为 " in out and "保持" in out, out
    assert memory.parse_size(scalar(client, "SELECT current_setting('memory_limit')")) < 100 * G, \
        "an oversized limit must stay lowered, not be put back"
    execute(client, f"SET memory_limit = '{memory.parse_size(before) // 1024 ** 2}MiB'")
    # current_setting() prints the limit rounded to 0.1 GiB
    assert abs(memory.parse_size(scalar(client, "SELECT current_setting('memory_limit')")) - memory.parse_size(before)) < 0.1 * G

    hr("compact: refuses a leftover file and an orphaned copy before dropping anything")
    rc, _ = upgrade(STOPPED, "compact-build", "--run-id", "c3", "--new-db", newdb, "--baseline-out", base)
    assert rc == 1, "wrote into the database a failed swap had left behind"
    tx(client, "CREATE TABLE data.ghost__pk_old (x INTEGER)")
    rc, _ = upgrade(STOPPED, "compact-verify")
    assert rc == 1, "an orphaned __pk_old copy passed as a rebuild product"
    rc, _ = upgrade(STOPPED, "compact-build", "--run-id", "c4",
                    "--new-db", newdb + ".2", "--baseline-out", base)
    assert rc == 1 and exists(client, "ghost__pk_old"), "dropped a copy with no rebuilt table"
    tx(client, "DROP TABLE data.ghost__pk_old")

    reset(client)
    print("\nALL UPGRADE TESTS PASSED")


if __name__ == "__main__":
    main()
