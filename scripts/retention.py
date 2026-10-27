#!/usr/bin/env python3
"""Manage rolling-window retention on a running duckport-server.

Usually run through `duckport retention ...`.

    retention.py --addr 127.0.0.1:50051 status [--brief]
    retention.py ... set <table> <keep_days> [--batch-days N] [--archive-dir DIR]
    retention.py ... disable <table>
    retention.py ... dry-run [table]
    retention.py ... run [table]
    retention.py ... log [table] [-n 20]
    retention.py ... --instances-file F check [--instance I | --table T]

`set` and `check` warn when a table's history starts far before the retention cutoff
(reconcile.startgap); --instances-file supplies each instance's START_DATE. check exits
0 nothing to warn about, 1 warned, 3 failed — the CLI runs it before loadhist and after
editing an instance's config.
"""

import argparse
import re
import sys

import pyarrow as pa

from doctor import load_instances
from duckport_flight import Duckport, lit
from reconcile import startgap

IDENT = re.compile(r"^[A-Za-z0-9_]{1,128}$")


def check_table(name):
    if not IDENT.match(name):
        sys.exit(f"invalid table name '{name}': only letters, digits and '_' are allowed")


def keyed(dp, table):
    rows = dp.query(
        "SELECT count(*) AS n FROM duckdb_constraints() WHERE schema_name = 'data' "
        f"AND table_name = {lit(table)} AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')")
    rows += dp.query(f"SELECT count(*) AS n FROM duckdb_indexes() WHERE schema_name = 'data' AND table_name = {lit(table)}")
    return sum(r["n"] for r in rows) > 0


def start_gaps(dp, args, table=None, instance=None):
    starts = {i.name: i.start_date for i in load_instances(args.instances_file) if i.start_date}
    policies, watermark = startgap.load(dp)
    return [g for g in startgap.gaps(policies, watermark, starts)
            if (table is None or g.table == table) and (instance is None or g.instance == instance)]


def print_gaps(gaps):
    for g in gaps:
        gap, cost, fix = g.describe()
        print(f"\033[1;33m⚠ data.{g.table}：{gap}\033[0m\n    {cost}\n    {fix}")


def cmd_status(dp, args):
    policies = dp.query(
        "SELECT p.table_name, p.keep_days, p.batch_days, p.archive_dir, p.enabled, "
        "l.status, l.cutoff, l.rows_deleted, l.dry_run, l.started_at "
        "FROM data.retention_policy p LEFT JOIN ("
        "  SELECT * FROM data.retention_log QUALIFY row_number() OVER (PARTITION BY table_name ORDER BY started_at DESC) = 1"
        ") l ON l.table_name = p.table_name ORDER BY p.table_name")
    if not policies:
        print("  \033[2m（未配置策略：duckport retention set <表名> <保留天数>）\033[0m")
        return 0
    for p in policies:
        state = "on " if p["enabled"] else "off"
        extra = f" archive={p['archive_dir']}" if p["archive_dir"] else ""
        last = "never run"
        if p["status"]:
            mode = " (dry-run)" if p["dry_run"] else ""
            last = (f"{p['status']}{mode} cutoff={p['cutoff']} rows={p['rows_deleted']:,} "
                    f"at {str(p['started_at'])[:16]} UTC")
        line = f"  [{state}] {p['table_name']:<22} keep {p['keep_days']:>4}d  {last}"
        print(line if args.brief else line + f"  batch={p['batch_days']}d{extra}")
    return 0


def cmd_set(dp, args):
    check_table(args.table)
    if args.keep_days < 1:
        sys.exit("keep_days must be >= 1")
    if keyed(dp, args.table):
        print(f"warning: data.{args.table} still has a PRIMARY KEY / index; retention will skip it "
              "until it is rebuilt without the key (sudo duckport upgrade --data).")
    archive = lit(args.archive_dir) if args.archive_dir else "NULL"
    dp.action("duckport.execute", {"sql":
        "INSERT OR REPLACE INTO data.retention_policy "
        "(table_name, keep_days, batch_days, archive_dir, enabled, updated_at) VALUES "
        f"({lit(args.table)}, {args.keep_days}, {args.batch_days}, {archive}, true, now()::TIMESTAMP)"})
    print(f"policy set: data.{args.table} keep {args.keep_days} days")
    print_gaps(start_gaps(dp, args, table=args.table))
    return 0


def cmd_check(dp, args):
    gaps = start_gaps(dp, args, table=args.table, instance=args.instance)
    print_gaps(gaps)
    return 1 if gaps else 0


def cmd_disable(dp, args):
    check_table(args.table)
    n = dp.action("duckport.execute", {"sql":
        f"UPDATE data.retention_policy SET enabled = false, updated_at = now()::TIMESTAMP "
        f"WHERE table_name = {lit(args.table)}"})["rows_affected"]
    print(f"disabled {n} policy row(s)")
    return 0


def run(dp, table, dry_run):
    body = {"dry_run": dry_run}
    if table:
        check_table(table)
        body["table"] = table
    report = dp.action("duckport.retention_run", body)
    print(f"run {report['run_id']}  dry_run={report['dry_run']}")
    if not report["tables"]:
        print("  （没有启用的策略）")
    for t in report["tables"]:
        span = f"{t['first_day']}..{t['last_day']}" if t["first_day"] else "-"
        verb = "would delete" if report["dry_run"] else "deleted"
        line = f"  {t['table']:<22} {t['status']:<28} cutoff={t['cutoff']}  {verb} {t['rows_deleted']:,} rows  days {span}"
        if t["rows_archived"]:
            line += f"  archived {t['rows_archived']:,}"
        if t["error"]:
            line += f"\n      error: {t['error']}"
        print(line)
    return 0 if all(t["status"] != "error" for t in report["tables"]) else 1


def cmd_log(dp, args):
    where = ""
    if args.table:
        check_table(args.table)
        where = f"WHERE table_name = {lit(args.table)}"
    rows = dp.query(
        "SELECT started_at, table_name, status, dry_run, cutoff, first_day, last_day, rows_deleted, "
        f"rows_archived, free_blocks_before, free_blocks_after, error FROM data.retention_log {where} "
        f"ORDER BY started_at DESC LIMIT {int(args.n)}")
    for r in rows:
        mode = "dry" if r["dry_run"] else "   "
        print(f"  {str(r['started_at'])[:19]}  {r['table_name']:<22} {r['status']:<28} {mode} "
              f"cutoff={r['cutoff']} rows={r['rows_deleted']:,} free={r['free_blocks_before']}->{r['free_blocks_after']}"
              + (f"  error={r['error']}" if r["error"] else ""))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--addr", default="127.0.0.1:50051")
    ap.add_argument("--instances-file", default="", help="instances with their START_DATE (from the CLI)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("status"); s.add_argument("--brief", action="store_true")
    s = sub.add_parser("set"); s.add_argument("table"); s.add_argument("keep_days", type=int)
    s.add_argument("--batch-days", type=int, default=1); s.add_argument("--archive-dir", default="")
    s = sub.add_parser("disable"); s.add_argument("table")
    s = sub.add_parser("dry-run"); s.add_argument("table", nargs="?")
    s = sub.add_parser("run"); s.add_argument("table", nargs="?")
    s = sub.add_parser("log"); s.add_argument("table", nargs="?"); s.add_argument("-n", type=int, default=20)
    s = sub.add_parser("check"); s.add_argument("--table"); s.add_argument("--instance")
    args = ap.parse_args()
    dp = Duckport(args.addr)
    try:
        if args.cmd == "status":
            return cmd_status(dp, args)
        if args.cmd == "set":
            return cmd_set(dp, args)
        if args.cmd == "disable":
            return cmd_disable(dp, args)
        if args.cmd in ("run", "dry-run"):
            return run(dp, args.table, args.cmd == "dry-run")
        if args.cmd == "check":
            return cmd_check(dp, args)
        return cmd_log(dp, args)
    except (pa.ArrowException, OSError) as e:
        print(f"error: {str(e).splitlines()[0]}", file=sys.stderr)
        return 3 if args.cmd == "check" else 1


if __name__ == "__main__":
    sys.exit(main())
