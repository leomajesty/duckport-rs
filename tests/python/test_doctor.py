#!/usr/bin/env python3
"""scripts/doctor.py end-to-end test: target state, a legacy database, mixed versions, and
a plugin start far before the retention cutoff (R2, plus `retention.py check` / `set`),
including tables no instance has written yet.

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/doctor.db cargo run --bin duckport-server    (fresh database)
"""

from datetime import datetime, timedelta, timezone

import pyarrow.flight as flight

from reconcile_fixtures import (
    NEW, OLD, SERVER, doctor, has, hr, legacy_state, reset, run_script, target_state, tx,
    unkeyed_with_duplicates,
)


def main():
    client = flight.FlightClient(SERVER)

    hr("target state: nothing to fix")
    target_state(client)
    rc, f = doctor(NEW)
    assert rc == 0, rc
    assert not [x for x in f if x["level"] != "hint"], f

    hr("legacy database (v1.0.x + plugin 0.1.0)")
    legacy_state(client)
    tx(client, "CREATE TABLE data.usdt_perp_1m__pk_old (x INTEGER)")
    rc, f = doctor(OLD)
    assert rc == 2, rc
    for rule, level, target in [
        ("V2", "block", "bn1m"), ("V2", "block", "binance-funding"),
        ("B1", "block", "usdt_perp_1m"), ("B1", "block", "exginfo"), ("B1", "block", "usdt_perp_funding"),
        ("S1", "online", "watermark"), ("B2", "online", "usdt_perp_1m"), ("B2", "hint", "usdt_perp_funding"),
        ("L1", "online", "retention_tasks"), ("L2", "online", "config_dict:usdt_perp_duck_time"),
        ("C1", "hint", "usdt_perp_1m__pk_old"),
        ("D1", "online", "exginfo"), ("D3", "stop", "main._staging_usdt_perp_1m"),
        ("D4", "stop", "usdt_perp_funding"), ("D5", "stop", "exginfo"),
    ]:
        assert has(f, rule, level, target), (rule, level, target)
    assert not has(f, "V3", "block"), "keyed tables are compatible with old writers"

    rc, f = doctor(NEW)
    assert rc == 2 and has(f, "B1", "block", "usdt_perp_1m"), "usdt_perp_1m__pk_old blocks a second rebuild"

    hr("table without a key written by an old plugin: V3 blocks")
    unkeyed_with_duplicates(client)
    rc, f = doctor(OLD)
    assert rc == 2 and has(f, "V3", "block", "usdt_spot_1m"), f

    hr("after upgrading the plugins: no V3, keyed tables become stop-window fixes")
    rc, f = doctor(NEW)
    assert not has(f, "V3", "block") and not has(f, "V2", "block")
    assert has(f, "B1", "block", "usdt_perp_1m"), "leftover __pk_old must block a second rebuild"
    assert has(f, "B1", "stop", "exginfo") and has(f, "B1", "stop", "usdt_perp_funding"), f
    assert any(x["rule"] == "D2" and x["target"] == "usdt_spot_1m" and "10 行重复" in x["message"] for x in f), f

    hr("unknown keyed table: hint only, never blocks")
    tx(client, "DROP TABLE data.usdt_perp_1m__pk_old", "CREATE TABLE data.oi_5m (t TIMESTAMP, s VARCHAR, PRIMARY KEY (t, s))")
    rc, f = doctor(NEW)
    assert has(f, "B1", "hint", "oi_5m") and rc == 1, (rc, f)
    tx(client, "DROP TABLE data.oi_5m")

    hr("registered spec of a third-party table: full checks")
    tx(client,
       "CREATE TABLE data.oi_5m (t TIMESTAMP, s VARCHAR)",
       "INSERT INTO data.oi_5m VALUES (TIMESTAMP '2026-10-26', 'BTCUSDT'), (TIMESTAMP '2026-10-26', 'BTCUSDT'), "
       "(TIMESTAMP '2026-10-26 00:05', repeat('X', 50))",
       "INSERT INTO data.table_specs (table_name, writer, writer_version, kind, time_column, period_seconds, "
       "dedup_key, write_mode, columns, column_defaults, string_checks, legacy_columns) VALUES "
       "('oi_5m', 'oi-ingestor', '1.0.0', 'append', 't', 300, ['t', 's'], 'anti_join', "
       "'{\"t\": \"TIMESTAMP\", \"s\": \"VARCHAR\", \"oi\": \"DOUBLE\"}', '{}', "
       "'{\"s\": {\"max_len\": 20}}', []), "
       "('gone_5m', 'oi-ingestor', '1.0.0', 'append', 't', 300, ['t'], 'anti_join', '{}', '{}', '{}', []), "
       "('broken_5m', 'oi-ingestor', '1.0.0', 'append', 't', 300, ['t'], 'anti_join', 'not json', '{}', '{}', [])")
    rc, f = doctor(NEW)
    for rule, level, target in [("B2", "online", "oi_5m"), ("D1", "online", "oi_5m"), ("D2", "online", "oi_5m"),
                                ("S2", "online", "oi_5m"), ("S3", "hint", "gone_5m"), ("S3", "hint", "broken_5m")]:
        assert has(f, rule, level, target), (rule, level, target)
    tx(client, "DROP TABLE data.oi_5m", "CREATE TABLE data.oi_5m (t TIMESTAMP, s VARCHAR, oi DOUBLE, PRIMARY KEY (t, s))")
    rc, f = doctor(NEW)
    assert has(f, "B1", "stop", "oi_5m"), "a registered anti_join writer makes the rebuild possible"
    tx(client, "DROP TABLE data.oi_5m", "DELETE FROM data.table_specs")

    hr("ingestor running: staging rows are only a hint")
    rc, f = doctor([dict(NEW[0], active=True), NEW[1]])
    assert has(f, "D3", "hint", "main._staging_usdt_perp_1m"), f

    hr("plugin start far before the retention cutoff: R2 hint, check and set warn")
    target_state(client)  # data.usdt_perp_1m: no key, watermark owned by bn1m
    today = datetime.now(timezone.utc).date()
    tx(client, "UPDATE data.watermark SET start_time = TIMESTAMP '2021-01-01' WHERE table_name = 'usdt_perp_1m'",
       "INSERT OR REPLACE INTO data.retention_policy (table_name, keep_days, batch_days, enabled, updated_at) "
       "VALUES ('usdt_perp_1m', 120, 1, true, now()::TIMESTAMP)")

    def with_start(days_before_today):
        return [dict(NEW[0], start_date=str(today - timedelta(days=days_before_today))), NEW[1]]

    try:
        rc, f = doctor(NEW)
        assert rc == 0, "R2 is a hint: it must not change doctor's exit code (upgrade's recheck relies on it)"
        assert has(f, "R2", "hint", "usdt_perp_1m"), f
        rc, out = run_script("retention.py", NEW, "check", "--instance", "bn1m")
        assert rc == 1 and "2021-01-01" in out and "duckport config bn1m" in out, (rc, out)
        rc, out = run_script("retention.py", NEW, "check", "--instance", "binance-funding")
        assert rc == 0 and not out.strip(), "warned about a table another instance writes"

        # the instance's START_DATE wins over the start_time written when the table was created
        rc, f = doctor(with_start(120 + 7))
        assert not has(f, "R2", "hint", "usdt_perp_1m"), "START_DATE close to the window still warned"
        # warn only when more history would be deleted than kept (120 days here)
        rc, out = run_script("retention.py", with_start(120 + 120), "check", "--table", "usdt_perp_1m")
        assert rc == 0, out
        rc, out = run_script("retention.py", with_start(120 + 121), "check", "--table", "usdt_perp_1m")
        assert rc == 1, out

        tx(client, "UPDATE data.watermark SET duck_time = NULL WHERE table_name = 'usdt_perp_1m'")
        rc, out = run_script("retention.py", NEW, "check", "--table", "usdt_perp_1m")
        assert rc == 1 and "还没有 duck_time" in out, "a fresh table backfills on its next start"

        rc, out = run_script("retention.py", NEW, "set", "usdt_perp_1m", "120")
        assert rc == 0 and "policy set" in out and "2021-01-01" in out, "set warns without failing"

        tx(client, "UPDATE data.retention_policy SET enabled = false WHERE table_name = 'usdt_perp_1m'")
        rc, f = doctor(NEW)
        assert not has(f, "R2", "hint", "usdt_perp_1m"), "a disabled policy deletes nothing"
    finally:
        tx(client, "DELETE FROM data.retention_policy")  # test_upgrade.py runs next on the same server

    hr("a policy table nobody has written yet: checked against the instance that may start writing it")
    target_state(client)  # usdt_perp_1m is written by bn1m; usdt_spot_1m does not exist yet
    tx(client, "INSERT INTO data.retention_policy (table_name, keep_days, batch_days, enabled, updated_at) VALUES "
       "('usdt_spot_1m', 120, 1, true, now()::TIMESTAMP), ('usdt_perp_1m', 120, 1, true, now()::TIMESTAMP)",
       "UPDATE data.watermark SET start_time = TIMESTAMP '2021-01-01' WHERE table_name = 'usdt_perp_1m'")
    early = [dict(NEW[0], start_date="2021-01-01"), NEW[1]]
    try:
        # before its first start: a new instance, or bn1m gaining usdt_spot as a data source
        rc, out = run_script("retention.py", early, "check", "--instance", "bn1m", "--fresh-only")
        assert rc == 1 and "usdt_spot_1m" in out and "还没有实例写入过" in out, (rc, out)
        assert "usdt_perp_1m" not in out, "--fresh-only reported a table that is already being written"
        rc, out = run_script("retention.py", early, "check", "--instance", "binance-funding", "--fresh-only")
        assert rc == 1 and "usdt_spot_1m" in out and "没有设置 START_DATE" in out, \
            "an instance without START_DATE backfills from its plugin's default: warned as unknown"
        rc, out = run_script("retention.py", with_start(120 + 7), "check", "--instance", "bn1m")
        assert rc == 0, out
        rc, f = doctor(early)
        assert has(f, "R2", "hint", "usdt_spot_1m"), f
        rc, out = run_script("retention.py", early, "set", "usdt_spot_1m", "120")
        assert rc == 0 and "还没有实例写入过" in out, "set on an empty table warns for each possible writer"

        # once the plugin has written its watermark row, the table is attributed, not assumed
        tx(client, "INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, "
           "duck_time) VALUES ('usdt_spot_1m', 'bn1m', 900, 'open_time', TIMESTAMP '2021-01-01', NULL)")
        rc, out = run_script("retention.py", early, "check", "--instance", "bn1m", "--fresh-only")
        assert rc == 1 and "还没有 duck_time" in out and "还没有实例写入过" not in out, out
        rc, out = run_script("retention.py", early, "check", "--instance", "binance-funding")
        assert rc == 0, "a table bn1m owns was blamed on binance-funding"
        # its start_time is the plugin's default as applied; with neither, the start is unknown
        tx(client, "UPDATE data.watermark SET start_time = NULL WHERE table_name = 'usdt_spot_1m'")
        rc, out = run_script("retention.py", NEW, "check", "--instance", "bn1m", "--fresh-only")
        assert rc == 1 and "水位表也没有记录 start_time" in out and "设为" in out, out
    finally:
        tx(client, "DELETE FROM data.retention_policy")

    reset(client)
    print("\nALL DOCTOR TESTS PASSED")


if __name__ == "__main__":
    main()
