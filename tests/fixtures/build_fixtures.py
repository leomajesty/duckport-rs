#!/usr/bin/env python3
"""Build sample databases F1–F6: the states older duckport versions leave behind.

Each fixture is made by the code of that version: the old release binary of duckport-server
creates its system tables, and the plugins' own init_schema (installed at the matching
commit in a throwaway venv) creates the business tables. A small synthetic data set and
the drift that version is known for are written on top. Every run regenerates them.

    build_fixtures.py --out DIR --current-bin target/release/duckport-server [--only F2 F3]
                      [--cache DIR] [--plugin-git https://github.com/sjmc-quant]

Next to each <name>.db a <name>.json records how it was made and which plugin instances
(and versions) write to it; tests/python/test_fixtures.py upgrades copies of them.
Needs: uv, network for the release binaries and the plugin repositories (or a local
--plugin-git file:///path containing duckport-binance-ingestor / -funding).
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
from duckport_flight import Duckport  # noqa: E402

# Plugin commits by version.
INGESTOR_FIRST = "01927eb"   # 2026-04-28, first standalone release; watermark has 5 columns
INGESTOR_010 = "4c8f295"     # 0.1.0: keyed tables, ON CONFLICT DO NOTHING
INGESTOR_020 = "029dd54"     # 0.2.0: no keys, anti-join writes
FUNDING_010 = "a049f7f"
FUNDING_020 = "331ea03"

FIXTURES = {
    "F1": {"about": "v1.0.1 + binance-ingestor 首版",
           "server": "leomajesty:v1.0.1", "plugins": {"binance-ingestor": INGESTOR_FIRST},
           "drift": ["duck_time_in_config", "retention_task"]},
    "F2": {"about": "v1.0.4 + 插件 0.1.0，exginfo 膨胀、1 行损坏、staging 残留",
           "server": "sjmc-quant:v1.0.4",
           "plugins": {"binance-ingestor": INGESTOR_010, "binance-funding": FUNDING_010},
           "drift": ["exginfo_bloat", "corrupt_exginfo", "staging_rows"]},
    "F3": {"about": "半升级：服务端 v1.1.0、插件 0.1.0（带主键的表），watermark 超前",
           "server": "sjmc-quant:v1.1.0",
           "plugins": {"binance-ingestor": INGESTOR_010, "binance-funding": FUNDING_010},
           "drift": ["watermark_ahead"]},
    "F4": {"about": "半升级：表已无主键、插件仍是 0.1.0，最近的数据有重复",
           "server": "sjmc-quant:v1.1.0",
           "plugins": {"binance-ingestor": INGESTOR_020, "binance-funding": FUNDING_020},
           # the tables come from 0.2.0, but the instances were rolled back to 0.1.0
           "instance_versions": {"binance-ingestor": "0.1.0", "binance-funding": "0.1.0"},
           "drift": ["duplicates"]},
    "F5": {"about": "当前版本的目标状态",
           "server": "current", "plugins": {"binance-ingestor": "HEAD", "binance-funding": "HEAD"},
           "drift": []},
    "F6": {"about": "v1.1.0 + 一张没有规格、带主键的第三方表",
           "server": "sjmc-quant:v1.1.0",
           "plugins": {"binance-ingestor": INGESTOR_020, "binance-funding": FUNDING_020},
           "drift": ["unknown_keyed_table"]},
}
INSTANCE = {"binance-ingestor": "bn5m", "binance-funding": "binance-funding"}
START = "2026-10-20"
DAYS = 3
SYMBOLS = 8


def log(msg):
    print(msg, flush=True)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def server_binary(spec, cache, current_bin):
    if spec == "current":
        return os.path.abspath(current_bin)
    owner, tag = spec.split(":")
    path = os.path.join(cache, f"duckport-server-{owner}-{tag}")
    if not os.path.exists(path):
        url = f"https://github.com/{owner}/duckport-rs/releases/download/{tag}/duckport-server-linux-x86_64"
        log(f"  download {url}")
        with urllib.request.urlopen(url, timeout=600) as r, open(path + ".part", "wb") as f:
            shutil.copyfileobj(r, f)
        os.chmod(path + ".part", 0o755)
        os.rename(path + ".part", path)
    return path


def plugin_python(plugin, commit, cache, git_base):
    venv = os.path.join(cache, f"venv-{plugin}-{commit}")
    py = os.path.join(venv, "bin", "python3")
    if not os.path.exists(os.path.join(venv, ".done")):
        shutil.rmtree(venv, ignore_errors=True)
        subprocess.run(["uv", "venv", "-q", "--python", "3.12", venv], check=True)
        src = f"git+{git_base}/duckport-{plugin}" + ("" if commit == "HEAD" else f"@{commit}")
        log(f"  install {src}")
        subprocess.run(["uv", "pip", "install", "-q", "--python", py, src], check=True)
        open(os.path.join(venv, ".done"), "w").close()
    return py


INIT = r"""
import os, sys
plugin, addr, start = sys.argv[1:4]
if plugin == "binance-ingestor":
    os.environ["INGESTOR_NAME"] = "bn5m"
    from binance_ingestor.duckport_client import DuckportClient
    DuckportClient(addr, schema="data", interval="5m").init_schema(
        ["usdt_perp", "usdt_spot"], "5m", {"usdt_perp", "usdt_spot"}, start_date=start)
else:
    os.environ["INGESTOR_NAME"] = "binance-funding"
    from binance_funding.duckport_client import DuckportClient
    DuckportClient(addr).init_schema(start_date=start)
"""


class Server:
    def __init__(self, binary, db, port):
        env = dict(os.environ, DUCKPORT_DB_PATH=db, DUCKPORT_LISTEN_ADDR=f"127.0.0.1:{port}",
                   DUCKPORT_DUCKDB_MEMORY_LIMIT="2GB", RUST_LOG="warn")
        for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            env.pop(k, None)
        self.log = open(db + ".server.log", "w")
        self.proc = subprocess.Popen([binary], env=env, stdout=self.log, stderr=subprocess.STDOUT)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"{binary} did not start; see {db}.server.log")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=180)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()


def table_names(dp):
    return {r["table_name"] for r in dp.query(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'data'")}


def keyed(dp, table):
    return bool(dp.scalar("SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' "
                          f"AND table_name = '{table}' AND constraint_type = 'PRIMARY KEY'"))


def insert(dp, table, select_sql):
    """INSERT BY NAME, skipping rows whose key exists when the table has one."""
    conflict = " ON CONFLICT DO NOTHING" if keyed(dp, table) else ""
    return dp.execute(f"INSERT INTO data.{table} BY NAME {select_sql}{conflict}")


def synthetic_data(dp, tables):
    for market in ("usdt_perp", "usdt_spot"):
        t = f"{market}_5m"
        if t in tables:
            insert(dp, t, f"SELECT TIMESTAMP '{START}' + INTERVAL (m * 5) MINUTE AS open_time, "
                          f"'S' || s || 'USDT' AS symbol, 1.0 + m AS open, 2.0 + m AS high, 0.5 + m AS low, "
                          f"1.5 + m AS close, 10.0 * s AS volume "
                          f"FROM range({DAYS * 288}) a(m), range({SYMBOLS}) b(s)")
            dp.execute(f"UPDATE data.watermark SET duck_time = (SELECT max(open_time) FROM data.{t}) "
                       f"WHERE table_name = '{t}'")
    if "exginfo" in tables:
        insert(dp, "exginfo", "SELECT m AS market, 'S' || s || 'USDT' AS symbol, 'TRADING' AS status "
                              f"FROM (VALUES ('usdt_perp'), ('usdt_spot')) v(m), range({SYMBOLS}) b(s)")
    if "usdt_perp_funding" in tables:
        insert(dp, "usdt_perp_funding",
               f"SELECT TIMESTAMP '{START}' + INTERVAL (h * 8) HOUR AS funding_time, 'S' || s || 'USDT' AS symbol, "
               f"0.0001 * s AS funding_rate, 100.0 + h AS mark_price FROM range({DAYS * 3}) a(h), range({SYMBOLS}) b(s)")
        dp.execute("UPDATE data.watermark SET duck_time = (SELECT max(funding_time) FROM data.usdt_perp_funding) "
                   "WHERE table_name = 'usdt_perp_funding'")


def drift(dp, kind, tables):
    if kind == "duck_time_in_config":
        # the first plugin days kept duck_time in config_dict, watermark not yet filled
        dp.execute("INSERT INTO data.config_dict VALUES ('usdt_perp_duck_time', "
                   "(SELECT CAST(max(open_time) AS VARCHAR) FROM data.usdt_perp_5m)) ON CONFLICT DO NOTHING")
        dp.execute("UPDATE data.watermark SET duck_time = NULL WHERE table_name = 'usdt_perp_5m'")
    elif kind == "retention_task":
        dp.execute("INSERT INTO data.retention_tasks BY NAME SELECT 'usdt_perp' AS market, '5m' AS interval, "
                   "'data' AS schema_name, '/data/duckport/pqt' AS parquet_dir")
    elif kind == "exginfo_bloat":
        # DELETE + INSERT under the key: the deleted rows are never reclaimed
        for _ in range(30):
            dp.transaction(["DELETE FROM data.exginfo WHERE market = 'usdt_perp'",
                            "INSERT INTO data.exginfo BY NAME SELECT 'usdt_perp' AS market, 'S' || s || 'USDT' AS symbol, "
                            "'TRADING' AS status FROM range(300) r(s)"])
    elif kind == "corrupt_exginfo":
        # what duckdb#22279 left behind: strings of reverted appends glued together
        dp.execute("INSERT INTO data.exginfo BY NAME SELECT 'usdt_spot' || repeat('usdt_perp', 40) AS market, "
                   "repeat('BTCUSDT', 40) AS symbol, repeat('TRADING', 40) AS status")
    elif kind == "staging_rows":
        dp.execute("INSERT INTO main._staging_usdt_perp_5m BY NAME SELECT TIMESTAMP '2026-10-22 23:55' AS open_time, "
                   "'S1USDT' AS symbol")
    elif kind == "watermark_ahead":
        dp.execute("UPDATE data.watermark SET duck_time = duck_time + INTERVAL 30 DAY WHERE table_name = 'usdt_perp_5m'")
    elif kind == "duplicates":
        dp.execute(f"INSERT INTO data.usdt_perp_5m SELECT * FROM data.usdt_perp_5m "
                   f"WHERE open_time >= TIMESTAMP '{START}' + INTERVAL {DAYS - 1} DAY AND symbol = 'S0USDT' LIMIT 12")
    elif kind == "unknown_keyed_table":
        dp.execute("CREATE TABLE data.oi_5m (ts TIMESTAMP, symbol VARCHAR, oi DOUBLE, PRIMARY KEY (ts, symbol))")
        dp.execute(f"INSERT INTO data.oi_5m SELECT TIMESTAMP '{START}' + INTERVAL (m * 5) MINUTE, 'S' || s, m "
                   f"FROM range(288) a(m), range(4) b(s)")
    else:
        raise ValueError(kind)


def build(name, spec, args):
    db = os.path.join(args.out, f"{name}.db")
    for f in (db, db + ".wal"):
        if os.path.exists(f):
            os.remove(f)
    log(f"\n{name}: {spec['about']}")
    binary = server_binary(spec["server"], args.cache, args.current_bin)
    pythons = {p: plugin_python(p, c, args.cache, args.plugin_git) for p, c in spec["plugins"].items()}
    port = free_port()
    server = Server(binary, db, port)
    try:
        dp = Duckport(f"127.0.0.1:{port}")
        for plugin, py in pythons.items():
            env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
            subprocess.run([py, "-c", INIT, plugin, f"127.0.0.1:{port}", START], check=True, env=env,
                           capture_output=not args.verbose)
        tables = table_names(dp)
        synthetic_data(dp, tables)
        for kind in spec["drift"]:
            drift(dp, kind, tables)
        dp.execute("CHECKPOINT")
        summary = dp.query("SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY 1")
    finally:
        server.stop()
    versions = {p: subprocess.run([py, "-c", f"from importlib import metadata; print(metadata.version('{p}'))"],
                                  capture_output=True, text=True, check=True).stdout.strip()
                for p, py in pythons.items()}
    versions.update(spec.get("instance_versions", {}))
    instances = [{"instance": INSTANCE[p], "exec": p, "version": versions[p], "active": False}
                 for p in spec["plugins"]]
    meta = dict(spec, name=name, instances=instances, tables=summary)
    with open(os.path.join(args.out, f"{name}.json"), "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1, default=str)
    log("  " + ", ".join(f"{r['table_name']}={r['estimated_size']}" for r in summary))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--current-bin", required=True, help="duckport-server built from this checkout (F5)")
    ap.add_argument("--cache", default=os.path.expanduser("~/.cache/duckport-fixtures"))
    ap.add_argument("--plugin-git", default="https://github.com/sjmc-quant")
    ap.add_argument("--only", nargs="+", choices=sorted(FIXTURES))
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    os.makedirs(args.cache, exist_ok=True)
    for name in args.only or sorted(FIXTURES):
        build(name, FIXTURES[name], args)
    log("\nfixtures written to " + os.path.abspath(args.out))


if __name__ == "__main__":
    main()
