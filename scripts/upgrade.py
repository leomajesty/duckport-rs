#!/usr/bin/env python3
"""Bring a duckport database to the desired state of this release (data part of `duckport upgrade`).

Usually run through `sudo duckport upgrade --data`, which stops the ingestors around the
stop window. The same checks as `duckport doctor`; every online/stop finding has a fix.

    upgrade.py --addr ... --instances-file f.json plan
    upgrade.py ... apply --levels online --run-id R --quarantine-dir D
    upgrade.py ... apply --levels online,stop ...    # ingestors stopped
    upgrade.py ... finalize --run-id R               # drop <table>__pk_old
    upgrade.py ... rollback --run-id R               # put <table>__pk_old back

plan exit code: 0 nothing to do, 1 online fixes only, 4 needs a stop window, 2 blocked, 3 failed.
apply exit code: 0 done, 1 some fixes failed, 2 blocked (nothing changed), 3 failed.
"""

import argparse
import os
import shutil
import sys
from datetime import datetime

import pyarrow as pa

import migrate_nopk
from doctor import load_instances, print_report
from duckport_flight import Duckport
from reconcile.fixes import (
    Context, Result, actionable, apply, log_result, rebuild_space_needed, stop_window_estimate,
)
from reconcile.rules import Finding, Options, run_rules
from reconcile.specs import UPGRADE_LOG_DDL
from reconcile.state import collect


def check(dp, args):
    state = collect(dp, load_instances(args.instances_file))
    opts = Options(deep=args.deep, bloat_min_dead_rows=args.bloat_min_dead_rows)
    return state, opts, run_rules(state, dp, opts)


def disk_finding(state, findings, db_path):
    if not db_path or not os.path.exists(db_path):
        return None
    need = rebuild_space_needed(state, findings, os.path.getsize(db_path))
    free = shutil.disk_usage(os.path.dirname(os.path.abspath(db_path))).free
    if need and free < need:
        return Finding("DISK", "block", db_path,
                       f"重建需要约 {need / 1e9:.1f} GB，空闲只有 {free / 1e9:.1f} GB（新旧两份数据会同时存在）",
                       "腾出空间后重试")
    return None


def cmd_plan(dp, args):
    state, _, findings = check(dp, args)
    disk = disk_finding(state, findings, args.db_path)
    if disk:
        findings.insert(0, disk)
    print_report(state, findings)
    if any(f.level == "block" for f in findings):
        print("\n有阻断项：按上面的处理办法解决后重新执行，未做任何修改。")
        return 2
    online, stop = actionable(findings, {"online"}), actionable(findings, {"stop"})
    if not online and not stop:
        return 0
    print(f"\n计划：在线修正 {len(online)} 项" + (f"，停写窗口 {len(stop)} 项" if stop else ""))
    if stop:
        rows, secs = stop_window_estimate(state, findings)
        print(f"  停写窗口：停止 ingestor，服务端保持运行；预计 {secs:.0f}s" +
              (f"（重建复制约 {rows:,} 行）" if rows else ""))
        print("  重建的表保留为 <表>__pk_old，可 --rollback；确认无误后 --finalize 删除")
    if any(f.rule in ("D1", "D2", "D3", "L1") for f in online + stop):
        print(f"  删除的行先导出到 {args.quarantine_dir or '<quarantine>'}/<run_id>/（parquet）")
    return 4 if stop else 1


def cmd_apply(dp, args):
    levels = set(args.levels.split(","))
    state, opts, findings = check(dp, args)
    blocked = [f for f in findings if f.level == "block"]
    if blocked:
        for f in blocked:
            print(f"  ✗ [{f.rule}] {f.target}: {f.message}")
        print("有阻断项，未做任何修改。")
        return 2
    ctx = Context(dp, args.run_id, "+".join(sorted(levels)), os.path.join(args.quarantine_dir, args.run_id), opts)
    if not actionable(findings, levels):
        print("  （无）")
        return 0
    results = apply(ctx, state, findings, levels)
    if "stop" in levels:
        dp.execute("CHECKPOINT")
    failed = [r for r in results if r.status != "ok"]
    return 1 if failed else 0


def _log_command(dp, args, rule, status, detail):
    dp.execute(UPGRADE_LOG_DDL)
    ctx = Context(dp, args.run_id, args.command, "", Options())
    log_result(ctx, Result(Finding(rule, "stop", "*", args.command), status, 0, detail, 0), datetime.now())


def cmd_finalize(dp, args):
    rc = migrate_nopk.cmd_finalize(dp, args)
    _log_command(dp, args, "C1", "ok" if rc == 0 else "error", "删除 <表>__pk_old")
    return rc


def cmd_rollback(dp, args):
    rc = migrate_nopk.cmd_rollback(dp, args)
    _log_command(dp, args, "B1", "ok" if rc == 0 else "error", "恢复 <表>__pk_old")
    return rc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--addr", default="127.0.0.1:50051")
    ap.add_argument("--instances-file", default="")
    ap.add_argument("--db-path", default="")
    ap.add_argument("--deep", action="store_true", help="whole-table duplicate scan")
    ap.add_argument("--bloat-min-dead-rows", type=int, default=Options.bloat_min_dead_rows)
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--quarantine-dir", default="")
    ap.add_argument("--levels", default="online", help="apply: online or online,stop")
    ap.add_argument("command", choices=["plan", "apply", "finalize", "rollback"])
    args = ap.parse_args()
    if not args.quarantine_dir and args.db_path:
        args.quarantine_dir = os.path.join(os.path.dirname(os.path.abspath(args.db_path)), "quarantine")
    if args.command == "apply":
        if not set(args.levels.split(",")) <= {"online", "stop"}:
            ap.error("--levels: online or online,stop")
        if not args.quarantine_dir:
            ap.error("apply needs --quarantine-dir or --db-path")
    try:
        dp = Duckport(args.addr)
        return {"plan": cmd_plan, "apply": cmd_apply, "finalize": cmd_finalize,
                "rollback": cmd_rollback}[args.command](dp, args)
    except (pa.ArrowException, OSError) as e:
        print(f"upgrade failed: {str(e).splitlines()[0]}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
