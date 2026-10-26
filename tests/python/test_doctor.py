#!/usr/bin/env python3
"""scripts/doctor.py end-to-end test: target state, a legacy database, mixed versions.

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/doctor.db cargo run --bin duckport-server    (fresh database)
"""

import json
import os
import subprocess
import sys
import tempfile

import pyarrow.flight as flight

SERVER = os.getenv("DUCKPORT_SERVER", "grpc://127.0.0.1:50051")
ADDR = SERVER.split("://", 1)[-1]
DOCTOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts", "doctor.py")

KLINE = ("open_time TIMESTAMP, symbol VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, "
         "quote_volume DOUBLE, trade_num INTEGER, taker_buy_base_asset_volume DOUBLE, "
         "taker_buy_quote_asset_volume DOUBLE, avg_price DOUBLE")
EXGINFO = ("market VARCHAR, symbol VARCHAR, status VARCHAR, base_asset VARCHAR, quote_asset VARCHAR, "
           "price_tick VARCHAR, lot_size VARCHAR, min_notional_value VARCHAR, contract_type VARCHAR, "
           "margin_asset VARCHAR, pre_market BOOLEAN, created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
           "underlying_type VARCHAR")
FUNDING = "funding_time TIMESTAMP, symbol VARCHAR, funding_rate DOUBLE, mark_price DOUBLE"
NEW = [{"instance": "bn1m", "exec": "binance-ingestor", "version": "0.2.0", "active": False},
       {"instance": "binance-funding", "exec": "binance-funding", "version": "0.2.0", "active": False}]
OLD = [dict(NEW[0], version="0.1.0"), dict(NEW[1], version="0.1.0")]


def hr(title):
    print("\n" + "=" * 6 + f" {title} " + "=" * 6)


def tx(client, *stmts):
    body = json.dumps({"statements": list(stmts)}).encode()
    list(client.do_action(flight.Action("duckport.execute_transaction", body)))


def doctor(instances, *extra):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(instances, f)
    try:
        p = subprocess.run([sys.executable, DOCTOR, "--addr", ADDR, "--instances-file", f.name, "--json",
                            "--bloat-min-dead-rows", "10000", *extra], capture_output=True, text=True)
    finally:
        os.unlink(f.name)
    if p.returncode == 3:
        print(p.stderr)
    report = json.loads(p.stdout)
    for x in report["findings"]:
        print(f"  {x['level']:<6} [{x['rule']}] {x['target']}: {x['message']}")
    return p.returncode, report["findings"]


def has(findings, rule, level, target=None):
    return any(f["rule"] == rule and f["level"] == level and (target is None or f["target"] == target)
               for f in findings)


def target_state(client):
    tx(client,
       *[f"DROP TABLE IF EXISTS data.{t}" for t in ("usdt_perp_1m", "usdt_spot_1m", "usdt_perp_funding", "exginfo",
                                                    "retention_tasks", "usdt_perp_1m__pk_old")],
       "DROP TABLE IF EXISTS main._staging_usdt_perp_1m",
       "DELETE FROM data.config_dict", "DELETE FROM data.watermark",
       f"CREATE TABLE data.usdt_perp_1m ({KLINE})",
       "INSERT INTO data.usdt_perp_1m SELECT TIMESTAMP '2026-10-26 00:00' + INTERVAL (m) MINUTE, 'S' || s, "
       "1, 1, 1, 1, 1, 1, 1, 1, 1, 1 FROM range(120) a(m), range(5) b(s)",
       f"CREATE TABLE data.exginfo ({EXGINFO})",
       "INSERT INTO data.exginfo (market, symbol, status) VALUES ('usdt_perp', 'BTCUSDT', 'TRADING'), "
       "('usdt_spot', '币安人生USDT', 'TRADING')",
       f"CREATE TABLE data.usdt_perp_funding ({FUNDING})",
       "INSERT INTO data.usdt_perp_funding VALUES (TIMESTAMP '2026-10-26 00:00', 'BTCUSDT', 0.0001, 1)",
       f"CREATE TABLE main._staging_usdt_perp_1m ({KLINE})",
       "INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time) VALUES "
       "('usdt_perp_1m', 'bn1m', 900, 'open_time', TIMESTAMP '2026-10-26', TIMESTAMP '2026-10-26 02:00'), "
       "('usdt_perp_funding', 'binance-funding', 32400, 'funding_time', TIMESTAMP '2026-10-26', TIMESTAMP '2026-10-26 08:00')",
       "INSERT INTO data.config_dict VALUES ('kline_interval', '1m'), ('write_mode:usdt_perp_1m', 'anti_join'), "
       "('write_mode:exginfo', 'anti_join'), ('write_mode:usdt_perp_funding', 'anti_join')",
       )


def main():
    client = flight.FlightClient(SERVER)

    hr("target state: nothing to fix")
    target_state(client)
    rc, f = doctor(NEW)
    assert rc == 0, rc
    assert not [x for x in f if x["level"] != "hint"], f

    hr("legacy database (v1.0.x + plugin 0.1.0)")
    tx(client,
       "DROP TABLE data.usdt_perp_1m", "DROP TABLE data.exginfo",
       "DELETE FROM data.config_dict WHERE starts_with(key, 'write_mode:')",
       # keyed kline table that also misses a column
       f"CREATE TABLE data.usdt_perp_1m ({KLINE.replace(', avg_price DOUBLE', '')}, PRIMARY KEY (open_time, symbol))",
       "INSERT INTO data.usdt_perp_1m SELECT TIMESTAMP '2026-10-26 00:00' + INTERVAL (m) MINUTE, 'S' || s, "
       "1, 1, 1, 1, 1, 1, 1, 1, 1 FROM range(120) a(m), range(5) b(s)",
       # keyed exginfo with a duckdb#22279-style row
       f"CREATE TABLE data.exginfo ({EXGINFO}, PRIMARY KEY (market, symbol))",
       "INSERT INTO data.exginfo (market, symbol, status) VALUES ('usdt_perp', 'BTCUSDT', 'TRADING'), "
       "('usdt_spot' || repeat('usdt_perp', 50), repeat('BTCUSDT', 50), repeat('TRADING', 50))",
       "DROP TABLE data.usdt_perp_funding",
       f"CREATE TABLE data.usdt_perp_funding ({FUNDING}, source VARCHAR, PRIMARY KEY (funding_time, symbol))",
       "INSERT INTO data.usdt_perp_funding VALUES (TIMESTAMP '2026-10-26 00:00', 'BTCUSDT', 0.0001, 1, 'api')",
       "CREATE TABLE data.retention_tasks (market VARCHAR, interval VARCHAR, PRIMARY KEY (market, interval))",
       "INSERT INTO data.config_dict VALUES ('usdt_perp_duck_time', '2026-10-26 02:00:00'), "
       "('usdt_perp_1m_pqt_time', '2026-10-01 00:00:00')",
       "CREATE TABLE data.usdt_perp_1m__pk_old (x INTEGER)",
       "INSERT INTO main._staging_usdt_perp_1m (open_time, symbol) VALUES (TIMESTAMP '2026-10-26 01:00', 'S1')",
       "UPDATE data.watermark SET duck_time = TIMESTAMP '2026-11-30' WHERE table_name = 'usdt_perp_funding'",
       )
    tx(client, "ALTER TABLE data.watermark DROP COLUMN max_lag_seconds")  # its own transaction
    # exginfo refreshed with DELETE + INSERT under its key: deleted rows pile up
    for _ in range(40):
        tx(client, "DELETE FROM data.exginfo WHERE market = 'usdt_perp'",
           "INSERT INTO data.exginfo (market, symbol, status) SELECT 'usdt_perp', 'S' || s, 'TRADING' FROM range(500) r(s)")
    tx(client, "CHECKPOINT")
    rc, f = doctor(OLD)
    assert rc == 2, rc
    for rule, level, target in [
        ("V2", "block", "bn1m"), ("V2", "block", "binance-funding"),
        ("B1", "block", "usdt_perp_1m"), ("B1", "block", "exginfo"), ("B1", "block", "usdt_perp_funding"),
        ("S1", "online", "watermark"), ("B2", "online", "usdt_perp_1m"), ("B2", "hint", "usdt_perp_funding"),
        ("L1", "online", "retention_tasks"), ("L2", "online", "config_dict:usdt_perp_duck_time"),
        ("L3", "online", "config_dict:usdt_perp_1m_pqt_time"), ("C1", "hint", "usdt_perp_1m__pk_old"),
        ("D1", "online", "exginfo"), ("D3", "stop", "main._staging_usdt_perp_1m"),
        ("D4", "online", "usdt_perp_funding"), ("D5", "stop", "exginfo"),
    ]:
        assert has(f, rule, level, target), (rule, level, target)
    assert not has(f, "V3", "block"), "keyed tables are compatible with old writers"

    hr("summary line skips slow checks")
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(NEW, fh)
    p = subprocess.run([sys.executable, DOCTOR, "--addr", ADDR, "--instances-file", fh.name, "--summary"],
                       capture_output=True, text=True)
    os.unlink(fh.name)
    print(p.stdout)
    assert p.returncode == 1 and "待对齐" in p.stdout, (p.returncode, p.stdout)

    hr("table without a key written by an old plugin: V3 blocks")
    tx(client, f"CREATE TABLE data.usdt_spot_1m ({KLINE})",
       "INSERT INTO data.usdt_spot_1m SELECT TIMESTAMP '2026-10-26 00:00' + INTERVAL (m) MINUTE, 'S' || (m % 3), "
       "1, 1, 1, 1, 1, 1, 1, 1, 1, 1 FROM range(10) a(m), range(2) b(dup)",
       "INSERT INTO data.watermark (table_name, ingestor, time_column, duck_time) "
       "VALUES ('usdt_spot_1m', 'bn1m', 'open_time', NULL)")
    rc, f = doctor(OLD)
    assert rc == 2 and has(f, "V3", "block", "usdt_spot_1m"), f

    hr("after upgrading the plugins: no V3, keyed tables become stop-window fixes")
    rc, f = doctor(NEW)
    assert not has(f, "V3", "block") and not has(f, "V2", "block")
    assert has(f, "B1", "stop", "usdt_perp_1m") and has(f, "B1", "stop", "exginfo"), f
    assert any(x["rule"] == "D2" and x["target"] == "usdt_spot_1m" and "10 行重复" in x["message"] for x in f), f
    assert rc == 1, rc

    hr("ingestor running: staging rows are only a hint")
    rc, f = doctor([dict(NEW[0], active=True), NEW[1]])
    assert has(f, "D3", "hint", "main._staging_usdt_perp_1m"), f

    tx(client, *[f"DROP TABLE IF EXISTS data.{t}" for t in ("usdt_perp_1m", "usdt_spot_1m", "usdt_perp_funding",
                                                           "exginfo", "retention_tasks", "usdt_perp_1m__pk_old")],
       "DROP TABLE IF EXISTS main._staging_usdt_perp_1m", "DELETE FROM data.config_dict",
       "DELETE FROM data.watermark")
    tx(client, "ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS max_lag_seconds INTEGER")
    print("\nALL DOCTOR TESTS PASSED")


if __name__ == "__main__":
    main()
