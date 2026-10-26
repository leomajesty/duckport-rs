#!/usr/bin/env python3
"""Bring a duckport database to the desired state of this release (data part of `duckport upgrade`).

Usually run through `sudo duckport upgrade --data`, which stops the ingestors around the
stop window. The same checks as `duckport doctor`; every online/stop finding has a fix.

    upgrade.py --addr ... --instances-file f.json --db-path DB plan [--state-out F]
    upgrade.py ... apply --levels online                    # ingestors running
    upgrade.py ... apply --levels online,stop [--backup]    # ingestors stopped; backup first
    upgrade.py ... apply ... --confirm-delete               # also delete corrupted/duplicate rows (D1/D2)
    upgrade.py ... finalize                                 # drop <table>__pk_old
    upgrade.py ... rollback                                 # put <table>__pk_old back

plan exit code: 0 nothing to do, 1 online fixes only, 4 needs a stop window,
                5 needs a stop window but there is not enough disk or memory for the backup,
                2 blocked, 3 failed.
--state-out writes WINDOW=0|1, BACKUP_OK=0|1, DELETE_ROWS=<n> for the CLI.
apply exit code: 0 done (unconfirmed D1/D2 skipped), 1 some fixes failed (or the backup
                 failed: nothing changed), 2 blocked (nothing changed), 3 failed.
Corrupted or duplicate rows (D1/D2) are deleted only with --confirm-delete: the plan shows
how many and a sample, the operator decides.
"""

import argparse
import os
import sys
from datetime import datetime

import pyarrow as pa

from doctor import load_instances, print_report
from duckport_flight import Duckport
from reconcile import backup, rebuild
from reconcile.fixes import NEEDS_CONFIRMATION, Context, Result, actionable, apply, log_result, preview
from reconcile.rules import Finding, Options, run_rules
from reconcile.specs import UPGRADE_LOG_DDL
from reconcile.state import collect


def check(dp, args):
    state = collect(dp, load_instances(args.instances_file))
    opts = Options(deep=args.deep, bloat_min_dead_rows=args.bloat_min_dead_rows)
    return state, opts, run_rules(state, dp, opts)


def backup_path(args):
    return os.path.join(args.backup_dir, f"duckport-{args.run_id}.db")


def write_state(path, **values):
    if path:
        with open(path, "w") as f:
            f.writelines(f"{k.upper()}={int(v)}\n" for k, v in values.items())


def show_deletions(dp, state, findings, opts):
    """Print what D1/D2 would delete; returns the number of rows."""
    todo = [f for f in actionable(findings, {"online", "stop"}) if f.rule in NEEDS_CONFIRMATION]
    if not todo:
        return 0
    total = 0
    print("\n需要人工确认的删除（先导出到隔离文件，再删除）：")
    for f in todo:
        n, rows = preview(dp, state, f, opts)
        total += n
        print(f"  [{f.rule}] {f.target}: {n:,} 行（{f.message}）")
        for r in rows:
            print("      " + "  ".join(f"{k}={v}" for k, v in r.items()))
        if n > len(rows) and f.rule == "D1":
            print(f"      …（共 {n:,} 行）")
    return total


def cmd_plan(dp, args):
    state, opts, findings = check(dp, args)
    res = backup.estimate(dp, state, findings, args.db_path)
    if res.fatal and actionable(findings, {"stop"}):
        findings.insert(0, Finding("DISK", "block", args.db_path, res.fatal, "腾出空间后重试"))
    print_report(state, findings)
    if any(f.level == "block" for f in findings):
        print("\n有阻断项：按上面的处理办法解决后重新执行，未做任何修改。")
        return 2
    online, stop = actionable(findings, {"online"}), actionable(findings, {"stop"})
    if not online and not stop:
        write_state(args.state_out, window=0, backup_ok=1, delete_rows=0)
        return 0
    if not stop:
        print(f"\n计划：在线修正 {len(online)} 项，ingestor 不停")
    else:
        print(f"\n计划：停写窗口内完成全部 {len(online) + len(stop)} 项（停止 ingestor，服务端保持运行）")
        if res.backup_possible:
            print(f"  1. 备份到 {backup_path(args)}（约 {backup.gb(res.backup_bytes)}，"
                  f"预计 {res.backup_seconds:.0f}s）")
        else:
            print("  1. 备份：\033[1;33m无法备份\033[0m")
            for w in res.warnings:
                print(f"     ⚠ {w}")
        print(f"  2. 修正；预计窗口共 {res.window_seconds(res.backup_possible):.0f}s"
              + (f"（重建复制约 {res.rebuild_rows:,} 行）" if res.rebuild_rows else ""))
        print("  重建的表保留为 <表>__pk_old，可 --rollback；确认无误后 --finalize 删除")
    if any(f.rule in ("D3", "L1") for f in online + stop):
        print(f"  删除的行先导出到 {args.quarantine_dir or '<quarantine>'}/<run_id>/（parquet）")
    deletions = show_deletions(dp, state, findings, opts)
    write_state(args.state_out, window=bool(stop), backup_ok=res.backup_possible, delete_rows=deletions)
    if stop:
        return 4 if res.backup_possible else 5
    return 1


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
    dp.execute(UPGRADE_LOG_DDL)
    if args.backup:
        path = backup_path(args)
        print(f"  备份到 {path} …", flush=True)
        started = datetime.now()
        try:
            detail = backup.take(dp, path)
        except Exception as e:
            detail = str(e).splitlines()[0][:500]
            print(f"    ✗ 备份失败：{detail}\n  未做任何修改。")
            log_result(ctx, Result(Finding("BACKUP", "stop", path, "备份"), "error", 0, detail, 0), started)
            return 1
        print(f"    ✓ {detail}（{(datetime.now() - started).total_seconds():.0f}s）", flush=True)
        log_result(ctx, Result(Finding("BACKUP", "stop", path, "备份"), "ok", 0, detail, 0), started)
    results = apply(ctx, state, findings, levels, confirmed=args.confirm_delete)
    if "stop" in levels:
        dp.execute("CHECKPOINT")
    return 1 if any(r.status == "error" for r in results) else 0


def _log_command(dp, args, rule, detail):
    dp.execute(UPGRADE_LOG_DDL)
    ctx = Context(dp, args.run_id, args.command, "", Options())
    log_result(ctx, Result(Finding(rule, "stop", "*", args.command), "ok", 0, detail, 0), datetime.now())


def cmd_finalize(dp, args):
    dropped = rebuild.finalize(dp)
    if not dropped:
        print("没有待清理的旧表。")
        return 0
    for t in dropped:
        print(f"  dropped data.{t}")
    print("完成。释放的空间会被后续写入复用；库文件本身不会变小。")
    _log_command(dp, args, "C1", "删除 " + ", ".join(dropped))
    return 0


def cmd_rollback(dp, args):
    state = collect(dp, [])
    time_columns, snapshots = {}, set()
    for name in state.tables:
        spec = state.spec(name)
        if spec and spec.kind == "snapshot":
            snapshots.add(name)
        col = spec.time_column if spec else (state.watermark.get(name) or {}).get("time_column")
        if col:
            time_columns[name] = col
    restored = rebuild.rollback(dp, time_columns, snapshots)
    if not restored:
        print("没有可回退的旧表。")
        return 0
    for table, note in restored:
        print(f"  restored data.{table}：{note}")
    _log_command(dp, args, "B1", "恢复 " + ", ".join(t for t, _ in restored))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--addr", default="127.0.0.1:50051")
    ap.add_argument("--instances-file", default="")
    ap.add_argument("--db-path", default="")
    ap.add_argument("--deep", action="store_true", help="whole-table duplicate scan")
    ap.add_argument("--bloat-min-dead-rows", type=int, default=Options.bloat_min_dead_rows)
    ap.add_argument("--run-id", default=f"{datetime.now():%Y%m%d-%H%M%S}-{os.getpid()}")
    ap.add_argument("--quarantine-dir", default="")
    ap.add_argument("--backup-dir", default="")
    ap.add_argument("--backup", action="store_true", help="apply: back up the database before any fix")
    ap.add_argument("--levels", default="online", help="apply: online or online,stop")
    ap.add_argument("--confirm-delete", action="store_true", help="apply: delete corrupted/duplicate rows (D1/D2)")
    ap.add_argument("--state-out", default="", help="plan: write WINDOW/BACKUP_OK/DELETE_ROWS for the CLI")
    ap.add_argument("command", choices=["plan", "apply", "finalize", "rollback"])
    args = ap.parse_args()
    db_dir = os.path.dirname(os.path.abspath(args.db_path)) if args.db_path else ""
    args.quarantine_dir = args.quarantine_dir or (db_dir and os.path.join(db_dir, "quarantine"))
    args.backup_dir = args.backup_dir or (db_dir and os.path.join(db_dir, "backup"))
    if args.command == "apply":
        if not set(args.levels.split(",")) <= {"online", "stop"}:
            ap.error("--levels: online or online,stop")
        if not args.quarantine_dir:
            ap.error("apply needs --quarantine-dir or --db-path")
        if args.backup and not args.backup_dir:
            ap.error("--backup needs --backup-dir or --db-path")
    try:
        dp = Duckport(args.addr)
        return {"plan": cmd_plan, "apply": cmd_apply, "finalize": cmd_finalize,
                "rollback": cmd_rollback}[args.command](dp, args)
    except (pa.ArrowException, OSError) as e:
        print(f"upgrade failed: {str(e).splitlines()[0]}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
