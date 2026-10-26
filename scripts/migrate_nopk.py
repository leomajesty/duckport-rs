#!/usr/bin/env python3
"""Rebuild `data.*` business tables without PRIMARY KEY / UNIQUE constraints.

DuckDB 1.5 cannot vacuum deleted rows of tables that carry an index (a PRIMARY KEY is
one), so retention would never free space on them. This tool copies such a table into
a constraint-free twin, verifies the row count and swaps the names.

Usually run through `sudo duckport migrate nopk`, which stops the ingestors first.

    migrate_nopk.py --addr 127.0.0.1:50051 --db-path /data/duckport/duckport.db plan
    migrate_nopk.py ... migrate [--tables t1 t2]  # copy + verify + swap; old table kept as <t>__pk_old
    migrate_nopk.py ... finalize                  # drop every <t>__pk_old, then CHECKPOINT
    migrate_nopk.py ... rollback                  # put every <t>__pk_old back under its original name

A table is only migrated when the plugin writing it is ready for constraint-free tables,
i.e. it registered `write_mode:<table> = anti_join` in `data.config_dict`. Older writers
use `INSERT ... ON CONFLICT DO NOTHING`, which fails on a table without a key.
"""

import argparse
import os
import shutil
import sys
import time

from duckport_flight import Duckport, lit, q

SYSTEM_TABLES = {"config_dict", "watermark", "retention_policy", "retention_log"}
OLD_SUFFIX = "__pk_old"
NEW_SUFFIX = "__nopk"
MIN_SERVER = (1, 1, 0)
# exginfo rows whose market is not a plain identifier were written corrupted by
# duckdb/duckdb#22279 (DuckDB <= 1.5.1); they are dropped while rebuilding.
EXGINFO_KEEP = "regexp_full_match(market, '[a-z0-9_]{1,32}') AND length(symbol) <= 64"


def data_tables(dp):
    """Every base table in schema data. Suffixes are matched in Python: `_` is a LIKE wildcard."""
    return [r["table_name"] for r in dp.query(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'data' ORDER BY table_name")]


def has_key(dp, table):
    return bool(dp.scalar(
        "SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' "
        f"AND table_name = {lit(table)} AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')"))


def keyed_tables(dp):
    """Business tables in schema data that still carry a PK/UNIQUE constraint or an index."""
    rows = dp.query(
        "SELECT t.table_name, t.estimated_size FROM duckdb_tables() t "
        "WHERE t.schema_name = 'data' AND ("
        "  EXISTS (SELECT 1 FROM duckdb_constraints() c WHERE c.schema_name = 'data' "
        "          AND c.table_name = t.table_name AND c.constraint_type IN ('PRIMARY KEY', 'UNIQUE'))"
        "  OR EXISTS (SELECT 1 FROM duckdb_indexes() i WHERE i.schema_name = 'data' "
        "          AND i.table_name = t.table_name)) "
        "ORDER BY t.table_name"
    )
    return [
        r for r in rows
        if r["table_name"] not in SYSTEM_TABLES
        and not r["table_name"].endswith(OLD_SUFFIX)
        and not r["table_name"].endswith(NEW_SUFFIX)
    ]


def ready_tables(dp):
    rows = dp.query("SELECT key, value FROM data.config_dict WHERE starts_with(key, 'write_mode:')")
    return {r["key"].split(":", 1)[1] for r in rows if r["value"] == "anti_join"}


def time_columns(dp):
    rows = dp.query("SELECT table_name, time_column FROM data.watermark WHERE time_column IS NOT NULL")
    return {r["table_name"]: r["time_column"] for r in rows}


def create_twin_sql(dp, table):
    """CREATE TABLE for <table>__nopk: same columns, types, defaults and NOT NULL, no keys."""
    cols = dp.query(
        "SELECT column_name, data_type, column_default, is_nullable FROM duckdb_columns() "
        f"WHERE schema_name = 'data' AND table_name = {lit(table)} ORDER BY column_index"
    )
    if not cols:
        raise RuntimeError(f"no columns found for data.{table}")
    parts = []
    for c in cols:
        part = f"{q(c['column_name'])} {c['data_type']}"
        if c["column_default"] is not None:
            part += f" DEFAULT {c['column_default']}"
        if not c["is_nullable"]:
            part += " NOT NULL"
        parts.append(part)
    return f"CREATE TABLE data.{q(table + NEW_SUFFIX)} ({', '.join(parts)})"


def server_version(dp):
    v = dp.action("duckport.ping").get("server_version", "0.0.0")
    try:
        return tuple(int(x) for x in v.split(".")[:3]), v
    except ValueError:
        return (0, 0, 0), v


def preflight(dp, args):
    """Print the plan; return (tables_to_migrate, ok)."""
    ok = True
    ver, ver_s = server_version(dp)
    print(f"duckport-server v{ver_s}")
    if ver < MIN_SERVER:
        print(f"  ✗ need duckport-server >= {'.'.join(map(str, MIN_SERVER))}")
        ok = False

    keyed = keyed_tables(dp)
    if args.tables:
        wanted = set(args.tables)
        unknown = wanted - {t["table_name"] for t in keyed}
        if unknown:
            print(f"  ✗ 不是带主键的业务表：{', '.join(sorted(unknown))}")
            ok = False
        keyed = [t for t in keyed if t["table_name"] in wanted]
    ready = ready_tables(dp)
    tcols = time_columns(dp)
    todo = []
    print("\n表（带主键/唯一约束/索引）:")
    if not keyed:
        print("  （无）")
    for t in keyed:
        name = t["table_name"]
        state = "ready" if name in ready else "writer not upgraded"
        print(f"  {'✓' if name in ready else '-'} data.{name:<28} ~{t['estimated_size']:>14,} rows  "
              f"time_column={tcols.get(name, '-')}  [{state}]")
        if name in ready:
            todo.append(name)
    skipped = [t["table_name"] for t in keyed if t["table_name"] not in ready]
    if skipped:
        print("\n  跳过的表：写它的插件还没升级到无主键写法（未在 data.config_dict 登记 "
              "write_mode:<表名>=anti_join）。先升级并重启对应插件，再重新执行。")

    if args.db_path and os.path.exists(args.db_path):
        size = os.path.getsize(args.db_path)
        free = shutil.disk_usage(os.path.dirname(os.path.abspath(args.db_path))).free
        print(f"\n磁盘：库文件 {size / 1e9:.1f} GB，空闲 {free / 1e9:.1f} GB")
        if todo and free < size:
            print("  ✗ 空闲空间小于库文件大小；迁移期间新旧两份数据会同时存在")
            ok = False
    leftovers = [t for t in data_tables(dp) if t.endswith(OLD_SUFFIX) or t.endswith(NEW_SUFFIX)]
    if leftovers:
        print("\n  ✗ 上一次迁移还没收尾：" + ", ".join(leftovers) + "（先执行 finalize 或 rollback）")
        ok = False
    return todo, ok


def copy_table(dp, table, time_col):
    src, dst = f"data.{q(table)}", f"data.{q(table + NEW_SUFFIX)}"
    dp.execute(create_twin_sql(dp, table))
    where = f" WHERE {EXGINFO_KEEP}" if table == "exginfo" else ""
    if time_col:
        months = dp.query(
            f"SELECT DISTINCT CAST(date_trunc('month', {q(time_col)}) AS VARCHAR) AS m FROM {src} "
            f"WHERE {q(time_col)} IS NOT NULL ORDER BY m")
        for r in months:
            t0 = time.time()
            n = dp.execute(
                f"INSERT INTO {dst} SELECT * FROM {src} WHERE {q(time_col)} >= TIMESTAMP {lit(r['m'])} "
                f"AND {q(time_col)} < TIMESTAMP {lit(r['m'])} + INTERVAL 1 MONTH "
                f"ORDER BY {q(time_col)}")
            print(f"    {r['m'][:7]}  {n:>12,} rows  {time.time() - t0:5.1f}s", flush=True)
        dp.execute(f"INSERT INTO {dst} SELECT * FROM {src} WHERE {q(time_col)} IS NULL")
    else:
        n = dp.execute(f"INSERT INTO {dst} SELECT * FROM {src}{where}")
        print(f"    {n:,} rows", flush=True)

    src_n = dp.scalar(f"SELECT count(*) FROM {src}{where}")
    dst_n = dp.scalar(f"SELECT count(*) FROM {dst}")
    dropped = dp.scalar(f"SELECT count(*) FROM {src}") - src_n if where else 0
    if src_n != dst_n:
        dp.execute(f"DROP TABLE {dst}")
        raise RuntimeError(f"row count mismatch for data.{table}: {src_n} vs {dst_n}; twin dropped")
    if dropped:
        print(f"    dropped {dropped} corrupted row(s) (market/symbol not plain identifiers)")
    dp.transaction([
        f"ALTER TABLE {src} RENAME TO {q(table + OLD_SUFFIX)}",
        f"ALTER TABLE {dst} RENAME TO {q(table)}",
    ])
    return dst_n


def cmd_migrate(dp, args):
    todo, ok = preflight(dp, args)
    if not ok:
        print("\n预检未通过，未做任何修改。")
        return 1
    if not todo:
        print("\n没有需要迁移的表。")
        return 0
    tcols = time_columns(dp)
    t_all = time.time()
    for table in todo:
        print(f"\n迁移 data.{table}", flush=True)
        t0 = time.time()
        n = copy_table(dp, table, None if table == "exginfo" else tcols.get(table))
        print(f"  ✓ {n:,} rows，{time.time() - t0:.1f}s；原表保留为 data.{table}{OLD_SUFFIX}", flush=True)
    if "retention_tasks" in data_tables(dp):
        dp.execute("DROP TABLE data.retention_tasks")
        print("\n已删除废弃的 data.retention_tasks")
    dp.execute("CHECKPOINT")
    print(f"\n完成，用时 {time.time() - t_all:.0f}s。确认 ingestor 正常写入后执行 finalize 删除旧表；"
          "有问题执行 rollback。")
    return 0


def cmd_finalize(dp, _args):
    olds = [t for t in data_tables(dp) if t.endswith(OLD_SUFFIX)]
    if not olds:
        print("没有待清理的旧表。")
        return 0
    for old in olds:
        dp.execute(f"DROP TABLE data.{q(old)}")
        print(f"  dropped data.{old}")
    dp.execute("CHECKPOINT")
    print("完成。释放的空间会被后续写入复用；库文件本身不会变小。")
    return 0


def restore_statements(dp, table, old, time_col):
    """Statements that move rows written since the migration from data.<table> into
    data.<old> (which still has its key); returns (statements, note)."""
    new, prev = f"data.{q(table)}", f"data.{q(old)}"
    if table == "exginfo":
        # Snapshot table: the new copy is the current state.
        return [f"DELETE FROM {prev}", f"INSERT INTO {prev} SELECT * FROM {new}"], "exginfo 用当前快照替换"
    if not has_key(dp, old):
        # Only an index, no constraint to resolve conflicts against: copy rows not present.
        return [f"INSERT INTO {prev} SELECT * FROM {new} EXCEPT SELECT * FROM {prev}"], "补回了新表中多出的行"
    if not time_col:
        return [f"INSERT INTO {prev} SELECT * FROM {new} ON CONFLICT DO NOTHING"], "按主键补回了新写入的数据"

    col = q(time_col)
    old_max = dp.scalar(f"SELECT CAST(max({col}) AS VARCHAR) FROM {prev}")
    if old_max is None:
        # The old table was empty: everything in the new one was written after the migration.
        return [f"INSERT INTO {prev} SELECT * FROM {new} WHERE {col} IS NOT NULL ON CONFLICT DO NOTHING"], \
            "原表为空，新写入的数据已全部补回"
    stmts = [f"INSERT INTO {prev} SELECT * FROM {new} WHERE {col} > TIMESTAMP {lit(old_max)} ON CONFLICT DO NOTHING"]
    note = "迁移后新写入的数据已补回"
    older_new = dp.scalar(f"SELECT count(*) FROM {new} WHERE {col} <= TIMESTAMP {lit(old_max)}")
    older_old = dp.scalar(f"SELECT count(*) FROM {prev} WHERE {col} <= TIMESTAMP {lit(old_max)}")
    if older_new != older_old:
        # Backfilled (or deleted) rows inside the old range: copy whatever the old table lacks.
        stmts.append(f"INSERT INTO {prev} SELECT * FROM {new} WHERE {col} <= TIMESTAMP {lit(old_max)} "
                     "ON CONFLICT DO NOTHING")
        note += f"；{lit(old_max)} 之前的行数不一致（{older_new} vs {older_old}），已按主键补齐"
    return stmts, note


def cmd_rollback(dp, _args):
    tables = data_tables(dp)
    olds = [t for t in tables if t.endswith(OLD_SUFFIX)]
    if not olds:
        print("没有可回退的旧表。")
        return 0
    tcols = time_columns(dp)
    for old in olds:
        table = old[: -len(OLD_SUFFIX)]
        stmts, note = [], ""
        if table in tables:
            stmts, note = restore_statements(dp, table, old, tcols.get(table))
            stmts.append(f"DROP TABLE data.{q(table)}")
            note = "，" + note
        stmts.append(f"ALTER TABLE data.{q(old)} RENAME TO {q(table)}")
        dp.transaction(stmts)
        print(f"  restored data.{table}{note}")
    for t in data_tables(dp):
        if t.endswith(NEW_SUFFIX):
            dp.execute(f"DROP TABLE data.{q(t)}")
    dp.execute("CHECKPOINT")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--addr", default="127.0.0.1:50051")
    ap.add_argument("--db-path", default="")
    ap.add_argument("command", choices=["plan", "migrate", "finalize", "rollback"])
    ap.add_argument("--tables", nargs="+", help="plan/migrate only these tables (default: every ready table)")
    args = ap.parse_args()
    if args.tables and args.command not in ("plan", "migrate"):
        ap.error("--tables only applies to plan and migrate")
    dp = Duckport(args.addr)
    if args.command == "plan":
        _, ok = preflight(dp, args)
        return 0 if ok else 1
    return {"migrate": cmd_migrate, "finalize": cmd_finalize, "rollback": cmd_rollback}[args.command](dp, args)


if __name__ == "__main__":
    sys.exit(main())
