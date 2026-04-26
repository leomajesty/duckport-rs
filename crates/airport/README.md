# airport-rs — Apache Arrow Flight Server for DuckDB (Rust)

A Rust library crate for building Apache Arrow Flight servers compatible with the [DuckDB Airport Extension](https://airport.query.farm).

## Features

- **Simple API**: Build a Flight server in under 40 lines of code
- **Catalog Builder**: Define schemas and tables with a builder pattern
- **Dynamic Catalogs**: Implement custom catalog logic for live schema reflection
- **Multi-Catalog Server**: Serve multiple named catalogs from a single endpoint
- **Bearer Token Auth**: Built-in sync/async token validation
- **DDL Support**: `CREATE`/`DROP SCHEMA`, `CREATE`/`DROP`/`ALTER TABLE` via trait extension
- **DML Support**: `INSERT`, `UPDATE`, `DELETE` via optional table traits
- **Streaming Efficiency**: Async streams — zero rebuffering, Arrow batches pass through directly
- **Filter Pushdown**: Parse and use DuckDB filter predicates to minimize data transfer
- **Scalar & Table Functions**: Expose custom functions callable from DuckDB SQL
- **Geometry Support**: GeoArrow WKB extension type compatible with DuckDB spatial (optional feature)
- **Context Cancellation**: Request context carries a cancellation token that fires on client disconnect

## Requirements

- Rust 1.75+
- Tokio async runtime

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
airport = { git = "https://github.com/hugr-lab/airport-rs" }

# You'll also need these to build Arrow data in your scan functions:
arrow-array  = "53"
arrow-schema = "53"
futures      = "0.3"
tokio        = { version = "1", features = ["full"] }
```

Optional features:

```toml
# Enable GeoArrow geometry support (adds geo-types dependency)
airport = { git = "...", features = ["geometry"] }

# Disable TLS (enabled by default via tonic/tls)
airport = { git = "...", default-features = false }
```

## Quick Start

```rust
use airport::catalog::static_catalog::{CatalogBuilder, StaticSchema, StaticTable};
use airport::catalog::types::ScanOptions;
use airport::config::ServerConfig;
use airport::error::AirportError;
use airport::flight::context::RequestContext;
use airport::server::new_server;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define the table schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id",   DataType::Int64,    false),
        Field::new("name", DataType::LargeUtf8, true),
    ]));

    let schema_clone = schema.clone();

    // Create a table with an inline scan function
    let users_table = StaticTable::new(
        "users",
        "User accounts",
        schema.clone(),
        move |_ctx: RequestContext, _opts: ScanOptions| {
            let schema = schema_clone.clone();
            async move {
                let ids   = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
                let names = Arc::new(arrow_array::LargeStringArray::from(
                    vec!["Alice", "Bob", "Charlie"],
                ));
                let batch = RecordBatch::try_new(schema, vec![ids, names])
                    .map_err(AirportError::Arrow)?;

                Ok(Box::pin(stream::once(async move { Ok(batch) }))
                    as airport::catalog::table::SendableRecordBatchStream)
            }
        },
    );

    // Build the catalog
    let catalog = Arc::new(
        CatalogBuilder::new("demo")
            .add_schema(StaticSchema::new("main", "Main schema", vec![Arc::new(users_table)]))
            .build(),
    );

    // Create and start the gRPC server
    let service = new_server(ServerConfig::new(catalog).with_address("localhost:50051"));

    tonic::transport::Server::builder()
        .add_service(service)
        .serve("0.0.0.0:50051".parse()?)
        .await?;

    Ok(())
}
```

Run it:

```bash
cargo run --example basic
```

## Connect from DuckDB

```sql
-- Install the Airport extension (one-time)
INSTALL airport FROM community;
LOAD airport;

-- Attach the Flight server
ATTACH 'grpc://localhost:50051' AS demo (TYPE AIRPORT);

-- Query
SELECT * FROM demo.main.users;
```

Expected output:

```
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ Alice   │
│     2 │ Bob     │
│     3 │ Charlie │
└───────┴─────────┘
```

## Authentication

Add bearer token authentication with a synchronous validator:

```rust
use airport::auth::BearerAuth;
use airport::error::AirportError;
use airport::config::ServerConfig;

let auth = BearerAuth::sync(|token: &str| {
    if token == "secret-api-key" {
        Ok("user123".to_string())     // return the identity string
    } else {
        Err(AirportError::Unauthorized("invalid token".to_string()))
    }
});

let service = new_server(
    ServerConfig::new(catalog)
        .with_auth(Arc::new(auth))
        .with_address("localhost:50051"),
);
```

For async validation (e.g. database lookup):

```rust
use airport::auth::BearerAuth;

let auth = BearerAuth::new(|token: String| async move {
    let identity = db_lookup_token(&token).await?;  // your async call
    Ok(identity)
});
```

Connect from DuckDB with a token:

```sql
-- Using inline secret
ATTACH 'grpc://localhost:50051' AS demo (
    TYPE AIRPORT,
    SECRET (TYPE BEARER, TOKEN 'secret-api-key')
);

-- Or create a persistent secret scoped to the endpoint
CREATE PERSISTENT SECRET my_token (
    TYPE airport,
    auth_token 'secret-api-key',
    scope 'grpc://localhost:50051'
);
ATTACH '' AS demo (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');
```

> **Note:** DuckDB 1.5 does not send any RPC during `ATTACH` — authentication is enforced on the first actual query. `ATTACH` will succeed even with an invalid token; the error appears on the first `SELECT`.

## Custom Catalog (Dynamic)

For live, programmatic schema reflection, implement the `Catalog` and `Schema` traits directly:

```rust
use airport::catalog::{Catalog, Schema, Result};
use airport::flight::context::RequestContext;
use std::sync::Arc;

struct MyCatalog {
    db: Arc<MyDatabase>,
}

#[async_trait::async_trait]
impl Catalog for MyCatalog {
    async fn schemas(&self, ctx: &RequestContext) -> Result<Vec<Arc<dyn Schema>>> {
        let names = self.db.list_schemas().await?;
        Ok(names.into_iter()
            .map(|name| Arc::new(MySchema { db: self.db.clone(), name }) as Arc<dyn Schema>)
            .collect())
    }

    async fn schema(&self, ctx: &RequestContext, name: &str) -> Result<Option<Arc<dyn Schema>>> {
        if self.db.schema_exists(name).await? {
            Ok(Some(Arc::new(MySchema { db: self.db.clone(), name: name.to_string() })))
        } else {
            Ok(None)
        }
    }
}
```

Implementing `Table` for a custom scan:

```rust
use airport::catalog::table::{Table, SendableRecordBatchStream};
use airport::catalog::types::ScanOptions;
use airport::flight::context::RequestContext;
use std::sync::Arc;

struct MyTable {
    name: String,
    db:   Arc<MyDatabase>,
}

#[async_trait::async_trait]
impl Table for MyTable {
    fn name(&self) -> &str { &self.name }
    fn comment(&self) -> &str { "" }

    fn arrow_schema(&self, _columns: &[&str]) -> Arc<arrow_schema::Schema> {
        // Return the table's Arrow schema
        todo!()
    }

    async fn scan(
        &self,
        ctx: &RequestContext,
        opts: &ScanOptions,
    ) -> airport::catalog::Result<SendableRecordBatchStream> {
        // Return an async stream of RecordBatches
        let stream = self.db.query_as_arrow_stream().await?;
        Ok(Box::pin(stream))
    }
}
```

## DDL Operations (CREATE / DROP / ALTER)

Implement `DynamicCatalog` and `DynamicSchema` to support SQL DDL from DuckDB:

```rust
use airport::catalog::dynamic::{DynamicCatalog, DynamicSchema};
use airport::catalog::types::{
    CreateSchemaOptions, DropSchemaOptions,
    CreateTableOptions,
};
use airport::catalog::table::Table;

// --- Schema-level DDL (CREATE SCHEMA / DROP SCHEMA) ---
#[async_trait::async_trait]
impl DynamicCatalog for MyCatalog {
    async fn create_schema(
        &self,
        ctx: &RequestContext,
        name: &str,
        _comment: &str,
        _opts: &CreateSchemaOptions,
    ) -> Result<()> {
        self.db.create_schema(name).await?;
        Ok(())
    }

    async fn drop_schema(
        &self,
        ctx: &RequestContext,
        name: &str,
        _opts: &DropSchemaOptions,
    ) -> Result<()> {
        self.db.drop_schema(name).await?;
        Ok(())
    }
}

// --- Table-level DDL (CREATE TABLE / DROP TABLE) ---
#[async_trait::async_trait]
impl DynamicSchema for MySchema {
    async fn create_table(
        &self,
        ctx: &RequestContext,
        name: &str,
        schema: Arc<arrow_schema::Schema>,
        _opts: &CreateTableOptions,
    ) -> Result<Arc<dyn Table>> {
        self.db.create_table(name, &schema).await?;
        Ok(Arc::new(MyTable { name: name.to_string(), db: self.db.clone() }))
    }
}
```

Also expose these via the trait extension hooks on `Catalog` and `Schema`:

```rust
impl Catalog for MyCatalog {
    // ...
    fn as_dynamic(&self) -> Option<&dyn DynamicCatalog> { Some(self) }
}

impl Schema for MySchema {
    // ...
    fn as_dynamic_schema(&self) -> Option<&dyn DynamicSchema> { Some(self) }
}
```

Test from DuckDB:

```sql
ATTACH 'grpc://localhost:50051' AS demo (TYPE AIRPORT);

CREATE SCHEMA demo.analytics;
DROP SCHEMA demo.analytics;

CREATE TABLE demo.main.events (id BIGINT, ts TIMESTAMP, payload VARCHAR);
ALTER TABLE demo.main.events ADD COLUMN user_id BIGINT;
DROP TABLE demo.main.events;
```

## DML Operations (INSERT / UPDATE / DELETE)

Implement the optional DML traits on your `Table`:

```rust
use airport::catalog::table::{InsertableTable, UpdatableBatchTable, DeletableBatchTable};
use airport::catalog::types::{DMLOptions, DMLResult};

struct MyTable { /* ... */ }

// INSERT: receives a stream of RecordBatches
#[async_trait::async_trait]
impl InsertableTable for MyTable {
    async fn insert(
        &self,
        ctx: &RequestContext,
        stream: SendableRecordBatchStream,
        opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rows = collect_and_insert(stream).await?;
        Ok(DMLResult { affected_rows: rows as i64, returning_data: None })
    }
}

// UPDATE (batch interface — preferred): receives the full batch with updated values
#[async_trait::async_trait]
impl UpdatableBatchTable for MyTable {
    async fn update(
        &self,
        ctx: &RequestContext,
        batch: RecordBatch,
        opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rows = self.db.update_from_batch(&batch).await?;
        Ok(DMLResult { affected_rows: rows as i64, returning_data: None })
    }
}

// DELETE (batch interface — preferred)
#[async_trait::async_trait]
impl DeletableBatchTable for MyTable {
    async fn delete(
        &self,
        ctx: &RequestContext,
        batch: RecordBatch,
        opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rows = self.db.delete_from_batch(&batch).await?;
        Ok(DMLResult { affected_rows: rows as i64, returning_data: None })
    }
}

// Wire up the optional methods
impl Table for MyTable {
    // ...
    fn as_insertable(&self)       -> Option<&dyn InsertableTable>      { Some(self) }
    fn as_updatable_batch(&self)  -> Option<&dyn UpdatableBatchTable>  { Some(self) }
    fn as_deletable_batch(&self)  -> Option<&dyn DeletableBatchTable>  { Some(self) }
}
```

Test from DuckDB:

```sql
INSERT INTO demo.main.users VALUES (4, 'Diana');
UPDATE demo.main.users SET name = 'Bob Smith' WHERE id = 2;
DELETE FROM demo.main.users WHERE id = 3;
```

## Filter Pushdown

DuckDB pushes predicate filters to the Flight server to avoid scanning unnecessary rows. Filters arrive in `ScanOptions::filter`:

```rust
use airport::catalog::types::ScanOptions;
use airport::filter::parse::FilterPushdown;

async fn scan(
    &self,
    ctx: &RequestContext,
    opts: &ScanOptions,
) -> Result<SendableRecordBatchStream> {
    if let Some(filter_json) = &opts.filter {
        let fp = FilterPushdown::from_json(filter_json)?;
        // Encode to a SQL WHERE clause (DuckDB dialect):
        let where_clause = fp.to_sql();
        // Use where_clause in your downstream query
    }
    // ... return stream
}
```

Supported filter types:
- Comparisons: `=`, `<>`, `<`, `>`, `<=`, `>=`
- Range: `BETWEEN`, `IN`, `NOT IN`
- Logic: `AND`, `OR`, `NOT`
- Null checks: `IS NULL`, `IS NOT NULL`
- Functions: `LOWER`, `UPPER`, `LENGTH`, etc.
- Type casts and `CASE` expressions

## Scalar and Table Functions

### Scalar Function (per-row transformation)

```rust
use airport::catalog::function::ScalarFunction;
use airport::catalog::types::FunctionSignature;

struct DoubleFunc;

#[async_trait::async_trait]
impl ScalarFunction for DoubleFunc {
    fn name(&self) -> &str { "double_value" }
    fn comment(&self) -> &str { "Returns input × 2" }

    fn signature(&self) -> &FunctionSignature {
        &FunctionSignature {
            parameters: vec![arrow_schema::DataType::Int64],
            return_type: Some(arrow_schema::DataType::Int64),
        }
    }

    async fn execute(
        &self,
        _ctx: &RequestContext,
        batch: &RecordBatch,
    ) -> Result<arrow_array::ArrayRef> {
        let col = batch.column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let result: arrow_array::Int64Array = col.iter()
            .map(|v| v.map(|x| x * 2))
            .collect();
        Ok(Arc::new(result))
    }
}
```

Expose from your `Schema` implementation:

```rust
async fn scalar_functions(&self, ctx: &RequestContext)
    -> Result<Vec<Arc<dyn ScalarFunction>>>
{
    Ok(vec![Arc::new(DoubleFunc)])
}
```

### Table Function (parametric query)

```rust
use airport::catalog::function::TableFunction;

struct RangeFunc;

#[async_trait::async_trait]
impl TableFunction for RangeFunc {
    fn name(&self) -> &str { "generate_range" }
    // ...

    async fn schema_for_parameters(
        &self,
        _ctx: &RequestContext,
        _params: &[serde_json::Value],
    ) -> Result<Arc<arrow_schema::Schema>> {
        Ok(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("n", arrow_schema::DataType::Int64, false),
        ])))
    }

    async fn execute(
        &self,
        _ctx: &RequestContext,
        params: &[serde_json::Value],
        _opts: &ScanOptions,
    ) -> Result<SendableRecordBatchStream> {
        let n = params[0].as_i64().unwrap_or(10);
        // build and return a stream of 0..n
        todo!()
    }
}
```

Call from DuckDB:

```sql
SELECT * FROM demo.main.generate_range(100);
```

## Multi-Catalog Server

Serve multiple named catalogs from a single gRPC endpoint. DuckDB routes requests via the `airport-catalog` metadata header.

```rust
use airport::multicatalog::{MultiCatalogConfig, MultiCatalogServer};

let server = MultiCatalogServer::new(
    MultiCatalogConfig::new()
        .with_catalog(Arc::new(SalesCatalog::new()))      // name = "sales"
        .with_catalog(Arc::new(AnalyticsCatalog::new()))  // name = "analytics"
        .with_address("0.0.0.0:50051"),
)?;

// Add / remove at runtime
server.add_catalog(Arc::new(InventoryCatalog::new()), None)?;
server.remove_catalog("inventory");

let service = server.into_service();

tonic::transport::Server::builder()
    .add_service(service)
    .serve("0.0.0.0:50051".parse()?)
    .await?;
```

Connect to a specific catalog from DuckDB:

```sql
ATTACH 'sales' AS sales_db (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');
SELECT * FROM sales_db.main.orders;
```

> Requires DuckDB 1.5+ with the Airport extension.

## Geometry (GeoArrow)

Enable the `geometry` feature to use Arrow geometry columns compatible with DuckDB's spatial extension:

```toml
airport = { git = "...", features = ["geometry"] }
```

```rust
use airport::catalog::geometry::{
    geometry_field,        // creates an Arrow Field with GeoArrow metadata
    ARROW_EXTENSION_NAME_KEY,
    GEOARROW_WKB,
};
use arrow_schema::{DataType, Field, Schema};

let schema = Arc::new(Schema::new(vec![
    Field::new("id",   DataType::Int64,    false),
    Field::new("name", DataType::LargeUtf8, true),
    geometry_field("geom", true, 4326, "Point"),  // WGS84 point
]));
```

Use DuckDB spatial to query geometry data:

```sql
-- REQUIRED for geometry support
INSTALL spatial; LOAD spatial;

SELECT name, ST_AsText(geom) AS wkt FROM demo.geo.locations;
SELECT name FROM demo.geo.locations WHERE ST_X(geom) < -100;
```

## Architecture

The library follows a trait-based design where each capability is an optional extension:

| Trait | Responsibility |
|---|---|
| `Catalog` | Top-level: list and look up schemas |
| `NamedCatalog` | Extends `Catalog` with a name for `ATTACH` |
| `VersionedCatalog` | Extends `Catalog` with a version number |
| `Schema` | List tables, functions, and table refs |
| `Table` | Arrow schema + async scan stream |
| `InsertableTable` | INSERT support |
| `UpdatableBatchTable` | UPDATE support (preferred batch form) |
| `DeletableBatchTable` | DELETE support (preferred batch form) |
| `DynamicCatalog` | CREATE / DROP SCHEMA |
| `DynamicSchema` | CREATE / DROP TABLE |
| `DynamicTable` | ADD / DROP / RENAME COLUMN |
| `ScalarFunction` | Per-row scalar function |
| `TableFunction` | Parametric table-valued function |
| `TableFunctionInOut` | Stream-in / stream-out function |
| `TransactionManager` | BEGIN / COMMIT / ROLLBACK |

Optional capabilities are exposed via `as_*()` methods on `Table` and `Catalog` (returning `Option<&dyn Trait>`). Your structs only need to implement what they support.

## ServerConfig

```rust
use airport::config::ServerConfig;

let service = new_server(
    ServerConfig::new(catalog)
        .with_auth(Arc::new(auth))               // optional: BearerAuth or custom Authenticator
        .with_address("localhost:50051")          // used for FlightEndpoint URIs
        .with_max_message_size(16 * 1024 * 1024) // override default tonic message size
        .with_tx_manager(Arc::new(tx_manager)),  // optional: transaction support
);
```

## Project Structure

```
airport-rs/
├── Cargo.toml
└── src/
    ├── lib.rs                         # Public re-exports
    ├── config.rs                      # ServerConfig
    ├── server.rs                      # new_server() entry point
    ├── error.rs                       # AirportError
    ├── auth/
    │   ├── mod.rs                     # Authenticator trait
    │   └── bearer.rs                  # BearerAuth
    ├── catalog/
    │   ├── mod.rs                     # Catalog, Schema traits
    │   ├── table.rs                   # Table + DML traits
    │   ├── function.rs                # Scalar / Table function traits
    │   ├── types.rs                   # ScanOptions, DMLOptions, FunctionSignature …
    │   ├── dynamic.rs                 # DynamicCatalog, DynamicSchema
    │   ├── transaction.rs             # TransactionManager
    │   ├── tableref.rs                # TableRef (data:// URIs)
    │   ├── static_catalog.rs          # StaticCatalog / CatalogBuilder
    │   └── geometry.rs                # GeoArrow field helpers
    ├── filter/
    │   ├── mod.rs
    │   ├── types.rs                   # Expression AST
    │   └── parse.rs                   # JSON → FilterPushdown
    ├── flight/                        # Arrow Flight RPC layer (internal)
    │   ├── server.rs                  # FlightService implementation
    │   ├── context.rs                 # RequestContext
    │   ├── do_get.rs                  # DoGet handler
    │   ├── do_action*.rs              # DoAction handlers (metadata, DDL, stats …)
    │   ├── do_exchange*.rs            # DoExchange handlers (DML, functions)
    │   └── …
    ├── internal/
    │   ├── msgpack.rs                 # MessagePack encode/decode
    │   └── serialize.rs               # Catalog IPC + ZStandard compression
    └── multicatalog.rs                # MultiCatalogServer
examples/
├── basic/src/main.rs                  # In-memory table, no auth
└── auth/src/main.rs                   # Bearer token authentication
```

## Performance Tips

**Batch size**: Aim for 10 000 – 100 000 rows per `RecordBatch`. Many tiny batches add gRPC framing overhead.

**Streaming**: Return a `SendableRecordBatchStream` that produces batches lazily — do not load the full result set into a `Vec` before returning.

**Connection reuse**: Store your database connection pool in the `Table` struct (shared via `Arc`) and reuse it across scan calls.

**Cancellation**: Check `ctx.cancellation.is_cancelled()` in your scan loop (or use `ctx.cancellation.cancelled().await` in a `tokio::select!`) to abort early when the DuckDB client disconnects.

**Message size**: If DuckDB sends large Arrow batches on INSERT/UPDATE, increase the message limit via `ServerConfig::with_max_message_size`.

## Testing

Run library unit tests:

```bash
cargo test
```

Build all examples:

```bash
cargo build --examples
```

Run a specific example:

```bash
cargo run --example basic
cargo run --example auth
```

## License

MIT — see [LICENSE](LICENSE).

## References

- [DuckDB Airport Extension](https://airport.query.farm)
- [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
- [arrow-rs](https://github.com/apache/arrow-rs)
- [tonic gRPC](https://github.com/hyperium/tonic)
