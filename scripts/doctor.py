#!/usr/bin/env python3
"""Compare a running duckport database with the desired state of this release.

Read-only. Usually run through `duckport doctor`, which also passes the installed plugin
instances so plugin versions and table states can be checked against each other.

    doctor.py --addr 127.0.0.1:50051 [--instances-file f.json] [--deep] [--json]

Exit code: 0 nothing to do (hints only), 1 something to fix, 2 blocked, 3 doctor failed.
"""

import argparse
import json
import sys

import pyarrow as pa

from duckport_flight import Duckport
from reconcile.rules import LEVELS, Options, run_rules
from reconcile.state import Instance, collect

LEVEL_LABEL = {"block": "阻断", "stop": "停写修正", "online": "在线修正", "hint": "提示"}
LEVEL_MARK = {"block": "\033[0;31m✗\033[0m", "stop": "\033[1;33m●\033[0m",
              "online": "\033[0;36m●\033[0m", "hint": "\033[2m·\033[0m"}


def load_instances(path):
    if not path:
        return []
    with open(path) as f:
        return [Instance(name=i["instance"], exec=i.get("exec", ""), version=i.get("version") or None,
                         active=bool(i.get("active"))) for i in json.load(f)]


def exit_code(findings):
    levels = {f.level for f in findings}
    if "block" in levels:
        return 2
    if levels & {"stop", "online"}:
        return 1
    return 0


def print_report(state, findings):
    print(f"duckport-server v{state.server_version}")
    if state.instances:
        print("插件实例：" + "，".join(f"{i.name}={i.exec} v{i.version or '?'}{'' if i.active else '（未运行）'}"
                                    for i in state.instances))
    if not findings:
        print("\n\033[0;32m✓ 与期望状态一致\033[0m")
    for lvl in LEVELS:
        group = [f for f in findings if f.level == lvl]
        if not group:
            continue
        print(f"\n{LEVEL_MARK[lvl]} {LEVEL_LABEL[lvl]}（{len(group)}）")
        for f in group:
            print(f"  [{f.rule}] {f.target}: {f.message}")
            if f.fix:
                print(f"         → {f.fix}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--addr", default="127.0.0.1:50051")
    ap.add_argument("--instances-file", default="")
    ap.add_argument("--deep", action="store_true", help="scan whole tables for duplicate keys")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--bloat-min-dead-rows", type=int, default=Options.bloat_min_dead_rows)
    args = ap.parse_args()
    try:
        dp = Duckport(args.addr)
        state = collect(dp, load_instances(args.instances_file))
        opts = Options(deep=args.deep, bloat_min_dead_rows=args.bloat_min_dead_rows)
        findings = run_rules(state, dp, opts)
    except (pa.ArrowException, OSError) as e:
        print(f"doctor failed: {str(e).splitlines()[0]}", file=sys.stderr)
        return 3
    if args.json:
        print(json.dumps({"server_version": state.server_version, "exit_code": exit_code(findings),
                          "findings": [f.to_dict() for f in findings]}, ensure_ascii=False, indent=1))
    else:
        print_report(state, findings)
    return exit_code(findings)


if __name__ == "__main__":
    sys.exit(main())
