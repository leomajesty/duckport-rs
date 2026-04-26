//! Basic Airport Flight server example.
//!
//! Creates a server with a single "users" table containing in-memory data.
//!
//! Run with:
//!   cargo run --example basic
//!
//! Connect from DuckDB:
//!   INSTALL airport FROM 'http://get.airport.duckdb.org';
//!   LOAD airport;
//!   ATTACH 'grpc://localhost:50052' AS demo (TYPE AIRPORT);
//!   SELECT * FROM demo.main.users;

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
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Define schema for the users table
    let user_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let user_schema_clone = user_schema.clone();

    // Create the users table with an in-memory scan function
    let users_table = StaticTable::new(
        "users",
        "User accounts",
        user_schema.clone(),
        move |_ctx: RequestContext, _opts: ScanOptions| {
            let schema = user_schema_clone.clone();
            async move {
                let ids = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
                let names = Arc::new(arrow_array::StringArray::from(vec![
                    "Alice", "Bob", "Charlie",
                ]));
                let batch = RecordBatch::try_new(schema, vec![ids, names])
                    .map_err(AirportError::Arrow)?;

                let batch_stream = stream::once(async move { Ok(batch) });
                Ok(Box::pin(batch_stream)
                    as airport::catalog::table::SendableRecordBatchStream)
            }
        },
    );

    // Build the catalog
    let catalog = CatalogBuilder::new("demo")
        .add_schema(StaticSchema::new(
            "app",
            "Application schema",
            vec![Arc::new(users_table)],
        ))
        .build();

    // Create the Flight service
    let service = new_server(
        ServerConfig::new(Arc::new(catalog))
            .with_address("localhost:50052"),
    );

    println!("Airport server listening on :50052");
    println!("Catalog contains:");
    println!("  - Schema: app");
    println!("    - Table: users (3 rows: Alice, Bob, Charlie)");
    println!();
    println!("Connect from DuckDB:");
    println!("  INSTALL airport FROM 'http://get.airport.duckdb.org';");
    println!("  LOAD airport;");
    println!("  ATTACH 'grpc://localhost:50052' AS demo (TYPE AIRPORT);");
    println!("  SELECT * FROM demo.app.users;");

    // Start the gRPC server
    tonic::transport::Server::builder()
        .add_service(service)
        .serve("0.0.0.0:50052".parse()?)
        .await?;

    Ok(())
}
