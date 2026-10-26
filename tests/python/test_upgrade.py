#!/usr/bin/env python3
"""scripts/upgrade.py end-to-end test: a legacy database is brought to the desired state.

Checks: blocked while plugins are old; online fixes, then the stop window; doctor clean
afterwards; a second run does nothing; no row lost except the quarantined ones (which are
in their parquet files); rollback restores the keyed tables; finalize removes them.

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/upgrade.db cargo run --bin duckport-server    (fresh database)
"""

import glob
import json
import os
import tempfile

import pyarrow.flight as flight
import pyarrow.parquet as pq

from reconcile_fixtures import NEW, OLD, SERVER, doctor, hr, legacy_state, reset, run_script, tx, unkeyed_with_duplicates

QDIR = tempfile.mkdtemp(prefix="duckport-quarantine-")
STOPPED = NEW
RUNNING = [dict(i, active=True) for i in NEW]


def upgrade(instances, *args):
    rc, out = run_script("upgrade.py", instances, "--bloat-min-dead-rows", "10000", "--quarantine-dir", QDIR, *args)
    print(out)
    return rc, out


def scalar(client, sql):
    t = client.do_get(flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())).read_all()
    return t.column(0)[0].as_py()


def keyed(client, table):
    return scalar(client, "SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' "
                          f"AND table_name = '{table}' AND constraint_type = 'PRIMARY KEY'") > 0


def count(client, table):
    return scalar(client, f"SELECT count(*) FROM {table}")


def quarantined(run_id, rule, table):
    files = glob.glob(os.path.join(QDIR, run_id, f"{rule}.{table}*.parquet"))
    return sum(pq.read_table(f).num_rows for f in files)


def main():
    client = flight.FlightClient(SERVER)
    legacy_state(client)
    unkeyed_with_duplicates(client)
    # the market's duck_time lives only in config_dict (plugin first days): L2 must move it first
    tx(client, "UPDATE data.watermark SET duck_time = NULL WHERE table_name = 'usdt_perp_1m'")

    hr("old plugins: blocked, nothing changed")
    rc, out = upgrade(OLD, "plan")
    assert rc == 2 and "阻断" in out, rc
    rc, _ = upgrade(OLD, "apply", "--run-id", "r0", "--levels", "online")
    assert rc == 2 and keyed(client, "usdt_perp_1m") and count(client, "data.retention_tasks") == 1

    hr("plan with upgraded plugins: needs a stop window")
    rc, out = upgrade(RUNNING, "plan")
    assert rc == 4 and "停写窗口" in out, rc

    hr("online fixes while ingestors run")
    rc, _ = upgrade(RUNNING, "apply", "--run-id", "r1", "--levels", "online")
    assert rc == 0, rc
    assert scalar(client, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'watermark' "
                          "AND column_name = 'max_lag_seconds'") == 1
    assert scalar(client, "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'retention_tasks'") == 0
    assert quarantined("r1", "L1", "retention_tasks") == 1
    assert scalar(client, "SELECT count(*) FROM data.config_dict WHERE ends_with(key, '_time')") == 0
    assert str(scalar(client, "SELECT duck_time FROM data.watermark WHERE table_name = 'usdt_perp_1m'")) \
        == "2026-10-26 02:00:00", "L2 moves the config_dict value into the empty watermark row"
    assert count(client, "data.exginfo") == 500 and quarantined("r1", "D1", "exginfo") == 1
    assert count(client, "data.usdt_spot_1m") == 10 and quarantined("r1", "D2", "usdt_spot_1m") == 10
    assert scalar(client, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'usdt_perp_1m' "
                          "AND column_name = 'avg_price'") == 1
    assert keyed(client, "usdt_perp_1m"), "rebuilds wait for the stop window"
    assert count(client, "main._staging_usdt_perp_1m") == 1, "staging is left alone while ingestors run"

    hr("stop window (ingestors stopped)")
    rc, out = upgrade(STOPPED, "plan")
    assert rc == 4, rc
    rc, _ = upgrade(STOPPED, "apply", "--run-id", "r1", "--levels", "online,stop")
    assert rc == 0, rc
    for t in ("usdt_perp_1m", "exginfo", "usdt_perp_funding"):
        assert not keyed(client, t) and keyed(client, f"{t}__pk_old"), t
    assert count(client, "data.usdt_perp_1m") == 600 and count(client, "data.exginfo") == 500
    assert count(client, "data.usdt_perp_funding") == 1
    assert count(client, "main._staging_usdt_perp_1m") == 0 and quarantined("r1", "D3", "_staging_usdt_perp_1m") == 1
    assert str(scalar(client, "SELECT duck_time FROM data.watermark WHERE table_name = 'usdt_perp_funding'")) \
        == "2026-10-26 00:00:00", "D4 pulls the watermark back to the data"
    log = scalar(client, "SELECT count(*) FILTER (status = 'ok') || '/' || count(*) FROM data.upgrade_log "
                         "WHERE run_id = 'r1'")
    print("upgrade_log r1:", log)
    assert log == "13/13", log  # S1 L2 L3 L1 D1 D2 B2 online; D3 B1x3 D5 D4 in the window
    assert scalar(client, "SELECT detail FROM data.upgrade_log WHERE run_id = 'r1' AND rule = 'D4'") \
        == "duck_time 2026-11-30 00:00:00 → 2026-10-26 00:00:00"

    hr("doctor afterwards: hints only")
    rc, f = doctor(NEW)
    assert rc == 0, rc
    assert {x["rule"] for x in f} <= {"C1", "B2", "P1"}, f

    hr("second run does nothing")
    rc, _ = upgrade(STOPPED, "plan")
    assert rc == 0, rc

    hr("rollback restores the keyed tables, rows written since kept")
    tx(client, "INSERT INTO data.usdt_perp_1m SELECT TIMESTAMP '2026-10-26 02:00', 'S0', "
               "1, 1, 1, 1, 1, 1, 1, 1, 1, 1")
    rc, _ = upgrade(STOPPED, "rollback", "--run-id", "r2")
    assert rc == 0, rc
    assert keyed(client, "usdt_perp_1m") and count(client, "data.usdt_perp_1m") == 601
    assert scalar(client, "SELECT count(*) FROM duckdb_tables() WHERE ends_with(table_name, '__pk_old')") == 0

    hr("upgrade again, then finalize")
    rc, _ = upgrade(STOPPED, "apply", "--run-id", "r3", "--levels", "online,stop")
    assert rc == 0, rc
    rc, _ = upgrade(STOPPED, "finalize", "--run-id", "r3")
    assert rc == 0, rc
    rc, f = doctor(NEW)
    assert rc == 0 and not [x for x in f if x["rule"] == "C1"], f
    assert count(client, "data.usdt_perp_1m") == 601 and not keyed(client, "usdt_perp_1m")

    reset(client)
    print("\nALL UPGRADE TESTS PASSED")


if __name__ == "__main__":
    main()
