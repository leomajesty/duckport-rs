---
name: duckport-rs
description: Use this skill when operating, debugging, querying, deploying, or developing against a local duckport-rs installation, including duckport CLI service management, duckport-server configuration, Arrow Flight RPC checks, DuckDB/Airport reads, Python consumer usage, ingestor plugin development, loadhist, and staging/watermark write workflows.
---

# duckport-rs

## Scope

Use this skill for local or server-side duckport-rs work: checking service health, reading or writing through Arrow Flight, managing ingestor instances with `duckport`, reviewing deployment/configuration, developing new ingestor plugins, or debugging data freshness and loadhist.

This skill lives in the repo at `.cursor/skills/duckport-rs/`. Treat the repository root (parent of `crates/`, `duckport`, `deploy.sh`) as the source checkout. Always verify paths before assuming them. Production installs commonly use `/opt/duckport` for runtime files and `/data/duckport` for data.

## First Checks

Prefer fast, non-mutating checks first:

```bash
command -v duckport
duckport status
```

If source context is needed, inspect the repository root (e.g. `rg --files .` from checkout).

For detailed command references, read `references/operations.md`.

## Safe Operating Rules

- Use `duckport status` before `start`, `stop`, `restart`, `loadhist`, or config changes.
- Treat `loadhist` as mutually exclusive with the related ingestor process. Prefer `duckport loadhist <instance>` because it performs stop/load/restore orchestration.
- Do not directly edit `/opt/duckport/server.env` or ingestor `config.env` until current values are inspected.
- Do not delete DuckDB files, WAL files, ingestor directories, or Parquet history without explicit user approval.
- For remote Linux/systemd operations, surface the exact command and effect when escalation or SSH is required.

## Flight RPC Patterns

Use Arrow Flight for direct service checks and SQL access:

- `duckport.ping` checks server, catalog, and DuckDB version.
- `duckport.execute` runs one DDL/DML statement.
- `duckport.execute_transaction` runs multiple statements atomically.
- `duckport.query` is read-only and should start with `SELECT`, `WITH`, or `EXPLAIN`.
- `duckport.append` writes Arrow batches through DoPut, usually into staging tables.

For Python snippets and JSON request shapes, read `references/flight-rpc.md`.

## Ingestor Development

Use the staging write model:

```text
TRUNCATE _staging_table
DoPut -> main._staging_table
execute_transaction:
  INSERT INTO data.target SELECT * FROM _staging_table ON CONFLICT DO NOTHING
  UPSERT data.watermark
  TRUNCATE _staging_table
```

Core conventions:

- Business tables live in `data`.
- Staging tables live in `main` and are named `_staging_<target>`.
- `data.watermark` and `data.config_dict` are created/migrated by duckport-rs; ingestors upsert rows, they do not redefine these tables.
- DoPut chunks should stay below roughly 500 MB; the existing ingestor convention is about 200,000 rows per chunk.
- `duckdb` client package version should match the server DuckDB version, currently `1.5.1`.

For plugin layout, config rules, and loadhist behavior, read `references/ingestor.md`.

## Build And Test

From the local source checkout:

```bash
cargo build --release --bin duckport-server
```

Python integration tests live under `tests/python`. They require a running test server and Python dependencies such as `pyarrow` and `pandas`; inspect the test file before running a subset.

## Reporting

When answering operational questions, include:

- Current service state or the command output summary.
- Which address/schema/table/instance was checked.
- Any non-mutating verification performed.
- Clear warning if the next step would stop services, mutate data, or require elevated permissions.
