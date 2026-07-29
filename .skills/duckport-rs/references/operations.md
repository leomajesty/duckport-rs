# duckport-rs Operations

## Runtime Layout

Common production paths:

- `/opt/duckport/server.env`: server environment.
- `/opt/duckport/bin/duckport-server`: server binary.
- `/opt/duckport/ingestors/<instance>/config.env`: ingestor instance config.
- `/data/duckport/duckport.db`: DuckDB database file.
- `/data/duckport/pqt`: Parquet history/archive directory.
- `/data/duckport/hist`: historical download scratch directory.

Known local source checkout: this repository root (contains `Cargo.toml`, `duckport`, `deploy.sh`).

## CLI Commands

Start with:

```bash
duckport status
```

Common commands:

```bash
duckport list
duckport start
duckport start all
duckport stop
duckport restart
duckport start <instance>
duckport stop <instance>
duckport logs server
duckport logs <instance>
duckport logs all
duckport config <instance>
duckport install binance_ingestor
duckport loadhist <instance>
duckport upgrade
```

`duckport loadhist <instance>` is preferred over manual loadhist because loadhist and the matching ingestor must not run concurrently.

## Server Environment

Important variables:

```bash
DUCKPORT_DB_PATH=/data/duckport/duckport.db
DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
DUCKPORT_ADVERTISED_ADDR=<client-reachable-host>:50051
DUCKPORT_CATALOG_NAME=duckport
DUCKPORT_READ_POOL_SIZE=4
DUCKPORT_DUCKDB_THREADS=0
DUCKPORT_DUCKDB_MEMORY_LIMIT=4GB
DUCKPORT_RETENTION_ENABLED=false
RUST_LOG=info
```

If `DUCKPORT_LISTEN_ADDR` uses `0.0.0.0`, set `DUCKPORT_ADVERTISED_ADDR` to a concrete address reachable by Airport clients.

## Health Checks

Use CLI first:

```bash
duckport status
```

Use Python Flight when direct RPC verification is needed:

```bash
python3 -c "import json, pyarrow.flight as flight; c=flight.connect('grpc://localhost:50051'); r=list(c.do_action(flight.Action('duckport.ping', b'{}'))); print(json.loads(r[0].body.to_pybytes()))"
```

List Flight actions:

```bash
python3 -c "import pyarrow.flight as flight; c=flight.connect('grpc://localhost:50051'); print([a.type for a in c.list_actions()])"
```

Airport read path from DuckDB:

```sql
INSTALL airport FROM community;
LOAD airport;
ATTACH 'grpc://localhost:50051' AS dp (TYPE AIRPORT);
SELECT * FROM dp.data.usdt_perp_5m LIMIT 10;
```

## Build

From source:

```bash
cargo build --release --bin duckport-server
```

The release binary is:

```bash
target/release/duckport-server
```

Manual debug run:

```bash
DUCKPORT_DB_PATH=./duckport.db \
DUCKPORT_LISTEN_ADDR=0.0.0.0:50051 \
RUST_LOG=duckport_server=info,airport=info \
./target/release/duckport-server
```

## Low-Risk Troubleshooting Order

1. `duckport status`
2. `duckport logs server` or `duckport logs <instance>`
3. Inspect `/opt/duckport/server.env` and the instance `config.env`
4. Run `duckport.ping`
5. Query `data.watermark` through `duckport.query`
6. Only then consider restart, loadhist, schema changes, or data repair
