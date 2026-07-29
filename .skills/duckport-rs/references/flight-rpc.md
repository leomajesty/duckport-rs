# duckport-rs Flight RPC

## Connection

```python
import json
import pyarrow as pa
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:50051")
```

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
    "CREATE TABLE IF NOT EXISTS data.events (ts TIMESTAMP, device_id VARCHAR, value DOUBLE, PRIMARY KEY(ts, device_id))",
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

`duckport.query` is read-only. Use SQL beginning with `SELECT`, `WITH`, or `EXPLAIN`.

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
