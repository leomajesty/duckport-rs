#!/usr/bin/env python3
"""scripts/doctor.py end-to-end test: target state, a legacy database, mixed versions.

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/doctor.db cargo run --bin duckport-server    (fresh database)
"""

import pyarrow.flight as flight

from reconcile_fixtures import (
    NEW, OLD, SERVER, doctor, has, hr, legacy_state, reset, target_state, tx, unkeyed_with_duplicates,
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
        ("L3", "online", "config_dict:usdt_perp_1m_pqt_time"), ("C1", "hint", "usdt_perp_1m__pk_old"),
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

    hr("ingestor running: staging rows are only a hint")
    rc, f = doctor([dict(NEW[0], active=True), NEW[1]])
    assert has(f, "D3", "hint", "main._staging_usdt_perp_1m"), f

    reset(client)
    print("\nALL DOCTOR TESTS PASSED")


if __name__ == "__main__":
    main()
