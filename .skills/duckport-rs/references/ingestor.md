# duckport-rs Ingestor Development

## Repository Layout

Recommended structure:

```text
duckport-my-ingestor/
├── pyproject.toml
├── config.env.example
├── my_ingestor/
│   ├── __init__.py
│   ├── config.py
│   ├── duckport_client.py
│   ├── data_jobs.py
│   ├── loadhist.py
│   └── main.py
└── start_ingestor.py
```

Required files: `pyproject.toml`, `config.py`, `main.py`, `loadhist.py`, and `config.env.example`.

## Packaging

`pyproject.toml` should expose both the main ingestor process and loadhist:

```toml
[project]
requires-python = ">=3.12"
dependencies = [
    "pandas>=2.0",
    "pyarrow>=15.0",
    "aiohttp>=3.9",
    "python-dotenv>=1.0",
    "duckdb==1.5.6",
]

[tool.setuptools.packages.find]
include = ["my_ingestor*"]

[project.scripts]
my-ingestor = "my_ingestor.main:main"
loadhist = "my_ingestor.loadhist:main"
```

The registry `exec` field must match the main script name.

## Configuration

`config.py` should be environment driven and safe to import:

```python
import os
from dotenv import load_dotenv

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)
load_dotenv("/opt/duckport/server.env", override=False)

def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"

DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"
```

Only expose user-tunable values in `config.env.example`, such as:

```bash
KLINE_INTERVAL=5m
DATA_SOURCES=usdt_perp,usdt_spot
CONCURRENCY=2
START_DATE=2021-01-01
PROXY_URL=
PARQUET_DIR=/data/duckport/pqt
RESOURCE_PATH=/data/duckport/hist
```

Do not ask users to fill `DUCKPORT_ADDR`; derive it.

## Schema Rules

- Business tables are in `data`, for example `data.usdt_perp_5m`.
- Staging tables are in `main`, for example `_staging_usdt_perp_5m`.
- Business tables carry no PRIMARY KEY / UNIQUE / index (DuckDB 1.5 never reclaims deleted rows of indexed tables, and composite keys are not used by queries). Deduplicate in the write SQL instead.
- Register `write_mode:<table> = anti_join` in `data.config_dict` from `init_schema` so `duckport migrate nopk` knows the table can drop its key.
- `data.watermark` and `data.config_dict` are system tables created by duckport-rs.
- Use `CREATE TABLE IF NOT EXISTS` and `CREATE SCHEMA IF NOT EXISTS`.
- If a schema is created and used in the same setup flow, put those statements in one `execute_transaction`.

## Write Flow

Real-time write:

```text
TRUNCATE _staging_target
DoPut Arrow data into _staging_target
execute_transaction:
  INSERT INTO data.target ... ANTI JOIN data.target (dedup, see ingestor-dev-guide.md section 6)
  UPSERT data.watermark for target
  TRUNCATE _staging_target
```

Bulk load:

```text
for each chunk:
  TRUNCATE _staging_target
  DoPut chunk into _staging_target
  INSERT target with anti-join dedup (sort chunk by time first)
  TRUNCATE _staging_target
after all chunks:
  replace duck_time once
```

Anti-join template (`lo`/`hi` = batch min/max time, inlined as constants):

```sql
INSERT INTO data.target
SELECT s.* FROM (SELECT DISTINCT ON (open_time, symbol) * FROM _staging_target) s
ANTI JOIN (SELECT open_time, symbol FROM data.target WHERE open_time BETWEEN TIMESTAMP 'lo' AND TIMESTAMP 'hi') t
  ON s.open_time = t.open_time AND s.symbol = t.symbol
```

It makes repeated loads idempotent; keep both `DISTINCT ON` and the two bounds.

## Watermark

`data.watermark` tracks one row per business table:

```sql
INSERT INTO data.watermark
  (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at)
VALUES
  ('usdt_perp_5m', 'binance-5m', 1800, 'open_time', NULL, '2026-04-28 08:00:00', CURRENT_TIMESTAMP)
ON CONFLICT (table_name) DO UPDATE SET
  ingestor = excluded.ingestor,
  max_lag_seconds = excluded.max_lag_seconds,
  duck_time = excluded.duck_time,
  updated_at = CURRENT_TIMESTAMP
```

Do not update `start_time` in the conflict branch unless the user explicitly wants to rewrite the initial boundary.

## Main Process

The main entry should:

1. Set `TZ=UTC`.
2. Construct `DuckportClient`.
3. Install `SIGINT` and `SIGTERM` handlers that call `client.close()`.
4. Run `ping()` and exit nonzero on failure so systemd can retry.
5. Run idempotent `init_schema()`.
6. Start data jobs and keep the main thread alive.

## loadhist

`loadhist` is a separate script and must not run concurrently with the ingestor for the same instance because they share staging tables.

Expected phases:

1. `ping + init_schema`
2. Download source files
3. Clean/normalize to Parquet or Arrow
4. Read chunks, write through staging, then update watermark

Prefer `duckport loadhist <instance>` for operational runs.
