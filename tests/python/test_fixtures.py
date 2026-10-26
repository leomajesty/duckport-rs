#!/usr/bin/env python3
"""Every sample database (tests/fixtures/build_fixtures.py) upgrades to the desired state.

For each fixture, on a copy, with the duckport-server of this checkout:
  - doctor with the fixture's own plugin instances reports the expected findings
    (old writers block where they must);
  - upgrade (plugins at the current version, ingestors stopped: online + stop window,
    backup, deletions confirmed) succeeds;
  - doctor then finds nothing to fix and a second plan does nothing;
  - no row is lost: for every table, rows after + rows in the quarantine files = rows
    before, and the same holds for the sum of row hashes over the original columns;
  - finalize removes the old tables; tables nobody registered (F6) are left alone.

    FIXTURES_DIR=/tmp/fixtures DUCKPORT_BIN=target/release/duckport-server python test_fixtures.py [F2 ...]
"""

import glob
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, "..", "..", "scripts")
sys.path.insert(0, SCRIPTS)
from duckport_flight import Duckport, q  # noqa: E402

FIXTURES_DIR = os.environ["FIXTURES_DIR"]
BIN = os.environ.get("DUCKPORT_BIN", os.path.join(HERE, "..", "..", "target", "release", "duckport-server"))
CURRENT = {"binance-ingestor": "0.3.0", "binance-funding": "0.3.0"}

# Findings each fixture must show before the upgrade (rule, level, target), with its own instances.
EXPECT = {
    # the 5-column watermark of F1 is completed by the current server at startup, so no S1
    "F1": [("L1", "online", "retention_tasks"), ("L2", "online", "config_dict:usdt_perp_duck_time"),
           ("B1", "block", "usdt_perp_5m"), ("B2", "online", "exginfo"), ("V2", "block", "bn5m"),
           ("D4", "stop", "usdt_perp_5m")],
    "F2": [("B1", "block", "usdt_perp_5m"), ("B1", "block", "exginfo"), ("B1", "block", "usdt_perp_funding"),
           ("D1", "online", "exginfo"), ("D3", "stop", "main._staging_usdt_perp_5m"), ("D5", "stop", "exginfo")],
    "F3": [("B1", "block", "usdt_perp_5m"), ("D4", "stop", "usdt_perp_5m"), ("V2", "block", "binance-funding")],
    "F4": [("V3", "block", "usdt_perp_5m"), ("V3", "block", "usdt_perp_funding"), ("D2", "online", "usdt_perp_5m")],
    "F5": [],
    "F6": [("B1", "hint", "oi_5m")],
}
ALLOWED_AFTER = {"C1", "P1", "B2", "B1", "S3"}   # hints only; checked below


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, db):
        self.port = free_port()
        env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
        env.update(DUCKPORT_DB_PATH=db, DUCKPORT_LISTEN_ADDR=f"127.0.0.1:{self.port}",
                   DUCKPORT_DUCKDB_MEMORY_LIMIT="2GB", RUST_LOG="warn")
        self.log = open(db + ".server.log", "w")
        self.proc = subprocess.Popen([BIN], env=env, stdout=self.log, stderr=subprocess.STDOUT)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("server did not start")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        self.proc.wait(timeout=180)
        self.log.close()


def run(script, port, instances, *args):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(instances, f)
    try:
        p = subprocess.run([sys.executable, os.path.join(SCRIPTS, script), "--addr", f"127.0.0.1:{port}",
                            "--instances-file", f.name, "--bloat-min-dead-rows", "1000", *args],
                           capture_output=True, text=True)
    finally:
        os.unlink(f.name)
    if p.returncode == 3 or p.stderr.strip():
        print(p.stderr)
    return p.returncode, p.stdout


def doctor(port, instances):
    rc, out = run("doctor.py", port, instances, "--json")
    findings = json.loads(out)["findings"]
    for x in findings:
        print(f"    {x['level']:<6} [{x['rule']}] {x['target']}: {x['message']}")
    return rc, findings


def snapshot(dp):
    """{relation: (columns, rows, hash sum)} for every table in schemas data and main."""
    out = {}
    for t in dp.query("SELECT schema_name, table_name FROM duckdb_tables() "
                      "WHERE schema_name IN ('data', 'main') AND table_name <> 'upgrade_log'"):
        rel = f"{t['schema_name']}.{t['table_name']}"
        cols = [c["column_name"] for c in dp.query(
            "SELECT column_name FROM duckdb_columns() WHERE schema_name = "
            f"'{t['schema_name']}' AND table_name = '{t['table_name']}' ORDER BY column_index")]
        out[rel] = (cols, *checksum(dp, f"{q(t['schema_name'])}.{q(t['table_name'])}", cols))
    return out


def checksum(dp, relation, cols):
    packed = ", ".join(f"{q(c)} := {q(c)}" for c in cols)
    r = dp.query(f"SELECT count(*) AS n, CAST(coalesce(sum(hash(struct_pack({packed}))), 0) AS VARCHAR) AS h "
                 f"FROM {relation}")[0]
    return r["n"], int(r["h"])


def check_conservation(dp, before, quarantine_dir):
    """rows/hashes before == after + quarantined, per table (over the columns it had before)."""
    after_tables = {f"{r['schema_name']}.{r['table_name']}" for r in dp.query(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE schema_name IN ('data', 'main')")}
    for rel, (cols, n0, h0) in sorted(before.items()):
        if rel.endswith(("__pk_old", "__nopk")) or rel.split(".")[1] in ("watermark", "config_dict",
                                                                         "table_specs", "retention_log"):
            continue
        name = rel.split(".", 1)[1]
        n1 = h1 = 0
        if rel in after_tables:
            n1, h1 = checksum(dp, f"{q(rel.split('.')[0])}.{q(name)}", cols)
        nq = hq = 0
        for f in glob.glob(os.path.join(quarantine_dir, "*", f"*.{name}.parquet")) + \
                glob.glob(os.path.join(quarantine_dir, "*", f"*.{name}.*.parquet")):
            a, b = checksum(dp, f"read_parquet('{f}')", cols)
            nq, hq = nq + a, hq + b
        ok = (n0 == n1 + nq) and (h0 == h1 + hq)
        print(f"    {rel:<36} before {n0:>6}  after {n1:>6}  quarantined {nq:>4}  {'ok' if ok else 'MISMATCH'}")
        assert ok, rel


def upgraded(instances):
    return [dict(i, version=CURRENT[i["exec"]]) for i in instances]


def check_fixture(name):
    meta = json.load(open(os.path.join(FIXTURES_DIR, f"{name}.json")))
    print(f"\n====== {name}: {meta['about']}")
    work = tempfile.mkdtemp(prefix=f"fixture-{name}-")
    db = os.path.join(work, "duckport.db")
    shutil.copy(os.path.join(FIXTURES_DIR, f"{name}.db"), db)
    if os.path.exists(os.path.join(FIXTURES_DIR, f"{name}.db.wal")):
        shutil.copy(os.path.join(FIXTURES_DIR, f"{name}.db.wal"), db + ".wal")
    qdir = os.path.join(work, "quarantine")
    server = Server(db)
    try:
        dp = Duckport(f"127.0.0.1:{server.port}")
        port = server.port
        before = snapshot(dp)
        old, new = meta["instances"], upgraded(meta["instances"])

        print("  before, with the fixture's own plugin instances:")
        rc, f = doctor(port, old)
        for rule, level, target in EXPECT[name]:
            assert any(x["rule"] == rule and x["level"] == level and x["target"] == target for x in f), \
                (name, rule, level, target)
        if any(x["level"] == "block" and x["rule"] in ("V2", "V3") for x in f):
            rc, _ = run("upgrade.py", port, old, "--db-path", db, "--quarantine-dir", qdir,
                        "apply", "--levels", "online,stop")
            assert rc == 2, "apply must refuse while old writers block"

        print("  upgrade with the current plugins, ingestors stopped:")
        rc, out = run("upgrade.py", port, new, "--db-path", db, "plan")
        print("    plan exit", rc)
        assert rc in (0, 1, 4, 5), out
        rc, out = run("upgrade.py", port, new, "--db-path", db, "--quarantine-dir", qdir, "--run-id", "fx",
                      "apply", "--levels", "online,stop", "--backup", "--confirm-delete")
        print(out)
        assert rc == 0, rc

        rc, f = doctor(port, new)
        assert rc == 0, (name, rc)
        assert {x["rule"] for x in f} <= ALLOWED_AFTER and all(x["level"] == "hint" for x in f), f
        rc, _ = run("upgrade.py", port, new, "--db-path", db, "plan")
        assert rc == 0, "a second run must do nothing"
        check_conservation(dp, before, qdir)

        keyed = dp.scalar("SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' "
                          "AND constraint_type = 'PRIMARY KEY' AND table_name NOT IN "
                          "('watermark', 'config_dict', 'retention_policy', 'table_specs') "
                          "AND NOT ends_with(table_name, '__pk_old')")
        assert keyed == (1 if name == "F6" else 0), keyed   # only the unregistered oi_5m keeps its key

        rc, _ = run("upgrade.py", port, new, "--db-path", db, "finalize")
        assert rc == 0
        rc, f = doctor(port, new)
        assert rc == 0 and not [x for x in f if x["rule"] == "C1"], f
    finally:
        server.stop()
    shutil.rmtree(work)
    print(f"  {name} ok")


def main():
    names = sys.argv[1:] or sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(FIXTURES_DIR, "F*.json")))
    assert names, f"no fixtures in {FIXTURES_DIR}"
    for name in names:
        check_fixture(name)
    print("\nALL FIXTURE TESTS PASSED")


if __name__ == "__main__":
    main()
