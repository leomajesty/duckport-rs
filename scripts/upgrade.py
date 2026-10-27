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
    upgrade.py ... compact-verify                           # read-only: are the copies safe to drop?
    upgrade.py ... compact-build --new-db D --baseline-out F # drop them, rewrite the catalog into D
    upgrade.py ... compact-check --baseline F               # after the CLI swapped the files

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
import json
import os
import shutil
import sys
from datetime import datetime

import pyarrow as pa

from doctor import load_instances, print_report
from duckport_flight import Duckport
from reconcile import backup, compact, rebuild
from reconcile.fixes import NEEDS_CONFIRMATION, Context, Result, actionable, apply, log_result, preview
from reconcile.rules import Finding, Options, run_rules
from reconcile.specs import PLUGIN_MIN_VERSION, UPGRADE_LOG_DDL, version_at_least
from reconcile.state import collect


GiB = 1024 ** 3


def check(dp, args):
    instances = load_instances(args.instances_file)
    if args.assume_upgraded:
        # plan of a full `duckport upgrade`: stage 2 brings known plugins to at least their minimum
        for i in instances:
            minimum = PLUGIN_MIN_VERSION.get(i.exec)
            if minimum and version_at_least(i.version, minimum) is not True:
                print(f"（按升级后估计：{i.name} {i.exec} v{i.version or '?'} → ≥ {minimum}）")
                i.version = minimum
    state = collect(dp, instances)
    opts = Options(deep=args.deep, bloat_min_dead_rows=args.bloat_min_dead_rows)
    findings = run_rules(state, dp, opts)
    if args.assume_upgraded:
        # ... and stage 1 replaces the server
        findings = [f for f in findings if f.rule != "V1"]
    return state, opts, findings


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


def _time_columns(dp):
    """(time column per table, names of the snapshot tables), from the specs or the watermark."""
    state = collect(dp, [])
    time_columns, snapshots = {}, set()
    for name in state.tables:
        spec = state.spec(name)
        if spec and spec.kind == "snapshot":
            snapshots.add(name)
        col = spec.time_column if spec else (state.watermark.get(name) or {}).get("time_column")
        if col:
            time_columns[name] = col
    return time_columns, snapshots


def cmd_rollback(dp, args):
    time_columns, snapshots = _time_columns(dp)
    restored = rebuild.rollback(dp, time_columns, snapshots)
    if not restored:
        print("没有可回退的旧表。")
        return 0
    for table, note in restored:
        print(f"  restored data.{table}：{note}")
    _log_command(dp, args, "B1", "恢复 " + ", ".join(t for t, _ in restored))
    return 0


def cmd_compact_verify(dp, args):
    """Read-only: is every <table>__pk_old safe to drop? Nothing is modified."""
    stray = compact.orphans(dp)
    if stray:
        for t in stray:
            print(f"  ✗ data.{t}: 找不到对应的重建表")
        return 1
    results = compact.verify(dp, _time_columns(dp)[0])
    if not results:
        print("没有待清理的旧表。")
        return 0
    ok = True
    for table, problems, summary in results:
        if problems:
            ok = False
            print(f"  ✗ data.{table}: " + "；".join(problems))
        else:
            print(f"  ✓ data.{table}: {summary}")
    return 0 if ok else 1


def cmd_compact_build(dp, args):
    """Drop the verified <table>__pk_old copies, then write the live catalog to --new-db.

    The baseline is taken with the ingestors already stopped, so it is the exact state the
    new file has to reproduce; the CLI keeps it for the check after the swap.
    """
    # Both checks have to come before anything is dropped. A leftover file is what a failed
    # swap leaves behind for inspection, and ATTACH would write into it rather than replace it.
    if os.path.exists(args.new_db):
        print(f"{args.new_db} 已存在（上次压缩失败留下的）。确认不再需要后删除，再执行。", file=sys.stderr)
        return 1
    stray = compact.orphans(dp)
    if stray:
        print("以下旧表找不到对应的重建表，不能当作重建产物删除："
              + "、".join(f"data.{t}" for t in stray), file=sys.stderr)
        return 1

    time_columns = _time_columns(dp)[0]
    names = compact.tables(dp)
    baseline = compact.snapshot(dp, names, time_columns)
    with open(args.baseline_out, "w") as f:
        json.dump({"tables": [list(n) for n in names], "time_columns": time_columns,
                   "baseline": {k: list(v) for k, v in baseline.items()}}, f)
    print(f"  基线快照：{len(baseline)} 张表，{sum(v[0] for v in baseline.values()):,} 行")

    dropped = rebuild.finalize(dp)
    for t in dropped:
        print(f"  dropped data.{t}")
    _log_command(dp, args, "C1", "删除 " + ", ".join(dropped) if dropped else "无旧表可删")

    payload = compact.payload_bytes(dp)
    free = shutil.disk_usage(os.path.dirname(os.path.abspath(args.new_db))).free
    print(f"  存活数据 {payload / GiB:.1f} GiB，可用 {free / GiB:.1f} GiB")
    if free < payload * 1.1:
        print(f"磁盘不足以重写：需要约 {payload * 1.1 / GiB:.1f} GiB。\n"
              f"旧表已删除，库可正常使用；腾出空间后可再压缩。", file=sys.stderr)
        return 1

    started = datetime.now()
    try:
        compact.rewrite(dp, args.new_db, names, time_columns, baseline)
    except (pa.ArrowException, OSError, RuntimeError) as e:
        print(f"重写失败：{e}", file=sys.stderr)
        return 1
    took = (datetime.now() - started).total_seconds()
    size = os.path.getsize(args.new_db)
    print(f"  ✓ 新库 {size / GiB:.1f} GiB，逐表行数与时间覆盖一致（{took:.0f}s）")
    _log_command(dp, args, "C2", f"重写 {args.new_db}：{size} 字节，{took:.0f}s")
    return 0


def cmd_compact_check(dp, args):
    """After the swap: does the server's database still match the baseline?"""
    with open(args.baseline) as f:
        saved = json.load(f)
    names = [tuple(n) for n in saved["tables"]]
    actual = compact.snapshot(dp, names, saved["time_columns"])
    baseline = {k: tuple(v) for k, v in saved["baseline"].items()}
    problems = compact.differences(baseline, actual, "换文件后")
    if problems:
        print("换文件后与基线不一致：\n    " + "\n    ".join(problems), file=sys.stderr)
        return 1
    size = os.path.getsize(args.db_path) if args.db_path else 0
    print(f"  ✓ 换文件后校验通过：{len(actual)} 张表"
          + (f"，库文件 {size / GiB:.1f} GiB" if size else ""))
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
    ap.add_argument("--assume-upgraded", action="store_true",
                    help="plan: judge as after stages 1-2 of a full upgrade (server replaced, known plugins "
                         "at least at their minimum version)")
    ap.add_argument("--new-db", default="", help="compact-build: path of the rewritten database")
    ap.add_argument("--baseline-out", default="", help="compact-build: where to save the baseline")
    ap.add_argument("--baseline", default="", help="compact-check: the baseline to check against")
    ap.add_argument("command", choices=["plan", "apply", "finalize", "rollback",
                                        "compact-verify", "compact-build", "compact-check"])
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
    if args.command == "compact-build" and not (args.new_db and args.baseline_out):
        ap.error("compact-build needs --new-db and --baseline-out")
    if args.command == "compact-check" and not args.baseline:
        ap.error("compact-check needs --baseline")
    try:
        dp = Duckport(args.addr)
        return {"plan": cmd_plan, "apply": cmd_apply, "finalize": cmd_finalize,
                "rollback": cmd_rollback, "compact-verify": cmd_compact_verify,
                "compact-build": cmd_compact_build, "compact-check": cmd_compact_check}[
                    args.command](dp, args)
    except (pa.ArrowException, OSError) as e:
        print(f"upgrade failed: {str(e).splitlines()[0]}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
