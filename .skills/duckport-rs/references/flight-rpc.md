# duckport-rs Flight RPC

## Connection

```python
import json
import pyarrow as pa
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:50051")
```

Loopback (`127.0.0.0/8`, `::1`) does not require a token.

## Remote access with a bearer token

The server reads `DUCKPORT_AUTH_TOKENS`. Entries are comma-separated. Each entry is `name:token`, or a bare token stored under the name `client`. The client sends only the secret, not the `name:` prefix.

```bash
DUCKPORT_AUTH_TOKENS=laptop:s3cret,job:other-secret
DUCKPORT_TLS_CERT=/path/server.crt
DUCKPORT_TLS_KEY=/path/server.key
```

`DUCKPORT_TLS_CERT` and `DUCKPORT_TLS_KEY` are set together. The client address uses the `grpc+tls://` scheme when the server is listening with TLS. A host:port without a scheme is plain `grpc://`.

```python
import os
from duckport_consumer import DuckportConsumer, open_flight_client

os.environ["DUCKPORT_TOKEN"] = "s3cret"
consumer = DuckportConsumer(
    "grpc+tls://db.example.com:50051",
    schema="data",
    kline_interval_minutes=5,
    suffix="_5m",
)

# The same header, without the consumer wrapper:
client = open_flight_client("grpc+tls://db.example.com:50051", token="s3cret")
```

`DuckportConsumer(..., token=...)` overrides `DUCKPORT_TOKEN`. An empty token sends no `Authorization` header. A non-empty token is sent as `Authorization: Bearer <token>` on every Flight call.

The zone comes from the TCP peer, not from a forwarding header:

- Loopback never checks the token. Reads and writes are not capped.
- LAN (RFC1918, IPv6 `fc00::/7`, plus `DUCKPORT_TRUSTED_CIDRS`) may read and write. Once `DUCKPORT_AUTH_TOKENS` is set, a LAN peer must present a configured bearer token. DoGet has no byte or time cap.
- Any other address is public. With no tokens configured, public peers are rejected. With tokens configured, a valid bearer token can read (`duckport.query`, `GetFlightInfo`, list actions, `duckport.ping`). Writes (`duckport.execute`, `duckport.execute_transaction`, `duckport.append`) are rejected. Each public DoGet stops at `DUCKPORT_DOGET_MAX_BYTES` (default 16 MiB) or `DUCKPORT_DOGET_TIMEOUT_SECS` (default 10 seconds).

## Ping

```python
resp = list(client.do_action(flight.Action("duckport.ping", b"{}")))
info = json.loads(resp[0].body.to_pybytes())
print(info)
```

Expected fields include `server`, `server_version`, `catalog`, and `duckdb_version`.

## Execute One Statement

```python
body = json.dumps({"sql": "CREATE SCHEMA IF NOT EXISTS data"}).encode()
resp = list(client.do_action(flight.Action("duckport.execute", body)))
print(json.loads(resp[0].body.to_pybytes()))
```

Use this for single DDL/DML statements. Do not use it for reads.

## Execute Transaction

```python
stmts = [
    "CREATE SCHEMA IF NOT EXISTS data",
    "CREATE TABLE IF NOT EXISTS data.events (ts TIMESTAMP, device_id VARCHAR, value DOUBLE)",  # no PRIMARY KEY; dedupe with an anti-join
    "CREATE TABLE IF NOT EXISTS _staging_events (ts TIMESTAMP, device_id VARCHAR, value DOUBLE)",
]
body = json.dumps({"statements": stmts}).encode()
resp = list(client.do_action(flight.Action("duckport.execute_transaction", body)))
print(json.loads(resp[0].body.to_pybytes()))
```

If any statement fails, the transaction rolls back.

## Query

```python
ticket_data = json.dumps({
    "type": "duckport.query",
    "sql": "SELECT * FROM data.watermark ORDER BY table_name",
})
reader = client.do_get(flight.Ticket(ticket_data.encode()))
table = reader.read_all()
print(table.to_pandas())
```

`duckport.query` is read-only. Use SQL beginning with `SELECT`, `WITH`, or `EXPLAIN`. This path runs one statement. It does not split a large scan.

## Large resample windows

`DuckportConsumer.get_market` and `get_symbol` split a wide window before sending SQL. `query()` does not. The caller still receives one Arrow table.

The split keeps each DuckDB hash aggregation near a 2 GiB budget. The estimate is `symbol_count * buckets * 2048` bytes. A chunk is at most 45 days, and its length is a multiple of `interval`. One symbol, or any window whose estimate fits in the budget, stays a single query. The symbol count is `count(DISTINCT symbol)` over the same inclusive window; a `get_symbol` call uses 1 and does not issue that count.

Cuts fall on resample-bucket starts, using the same offset as the aggregation. For `interval=60` and `offset=5`, a cut is at minute `:05`, including a bucket that crosses midnight (`2026-02-28 23:05`, not `2026-03-01 00:00`). Chunks before the last use `open_time < cut`. The last chunk keeps `open_time <= end`. A calendar-day cut drops the bucket that straddles the cut, because neither side satisfies `HAVING count(open_time) = interval / kline_interval_minutes`.

If a chunk still returns an out-of-memory error, that chunk is halved on the next bucket boundary and retried. A chunk that is already one bucket wide is returned as the original error.

Rows are concatenated in chunk order. Each chunk is `ORDER BY resample_time`. Symbol order inside one `resample_time` is already not fixed by the SQL. `open`, `high`, `low`, `close`, and `avg_price` match a single query. `sum(...)` columns can differ in the last bits of a float, which also happens between two runs of the same statement.

```python
from datetime import datetime
from duckport_consumer import DuckportConsumer

consumer = DuckportConsumer(
    "grpc://127.0.0.1:50051",
    schema="data",
    kline_interval_minutes=5,
    suffix="_5m",
)
table = consumer.get_market(
    "usdt_perp",
    interval=60,
    offset=5,
    begin=datetime(2026, 2, 22),
    end=datetime(2026, 10, 22),
)
```

On the public zone the server still applies `DUCKPORT_DOGET_MAX_BYTES` and `DUCKPORT_DOGET_TIMEOUT_SECS` to each chunk. A 45-day all-symbol chunk can exceed the default 16 MiB / 10 second public cap even though it fits the server memory budget. Loopback and LAN are not under that cap.

## Append With DoPut

```python
table = pa.Table.from_pylist([
    {"ts": "2026-04-22 10:00:00", "device_id": "dev-1", "value": 1.23},
])

descriptor = flight.FlightDescriptor.for_path("duckport.append", "main", "_staging_events")
writer, reader = client.do_put(descriptor, table.schema)
writer.write_table(table)
writer.done_writing()
result = json.loads(bytes(reader.read()))
writer.close()
print(result)
```

Use DoPut for staging writes, then use `duckport.execute_transaction` to merge into the target table and truncate staging.

## Data Type Notes

- Timestamp columns used by kline ingestors should be timezone-naive Arrow timestamps before writing.
- Varchar columns that may contain only nulls should be explicitly cast to string to avoid Arrow `Null` type inference.
- Keep a DoPut stream below roughly 500 MB; split large data into chunks, commonly about 200,000 rows each.
