//! Airport Flight server with bearer token authentication.
//!
//! Demonstrates how to configure token-based authentication so that only
//! requests with a valid `Authorization: Bearer <token>` header are accepted.
//!
//! Run with:
//!   cargo run --example auth
//!
//! Connect from DuckDB:
//!   INSTALL airport FROM community;
//!   LOAD airport;
//!   ATTACH 'grpc://localhost:50053' AS app (TYPE AIRPORT,
//!       SECRET (TYPE BEARER, TOKEN 'secret-admin-token'));
//!   SELECT * FROM app.app.users;

use airport::auth::BearerAuth;
use airport::catalog::static_catalog::{CatalogBuilder, StaticSchema, StaticTable};
use airport::catalog::types::ScanOptions;
use airport::config::ServerConfig;
use airport::error::AirportError;
use airport::flight::context::RequestContext;
use airport::server::new_server;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

// Valid tokens → user identity
fn valid_tokens() -> HashMap<&'static str, &'static str> {
    [
        ("secret-admin-token", "admin"),
        ("secret-user1-token", "user1"),
        ("secret-user2-token", "user2"),
        ("secret-guest-token", "guest"),
    ]
    .into_iter()
    .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Parse --port <N> from command-line args (default 50053)
    let args: Vec<String> = env::args().collect();
    let port: u16 = args
        .windows(2)
        .find(|w| w[0] == "--port")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(50053);

    // Define schema for the users table
    let user_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::LargeUtf8, true),
        Field::new("email", DataType::LargeUtf8, true),
    ]));

    let user_schema_clone = user_schema.clone();

    // Create the users table with an in-memory scan function
    let users_table = StaticTable::new(
        "users",
        "User accounts - authenticated access only",
        user_schema.clone(),
        move |_ctx: RequestContext, _opts: ScanOptions| {
            let schema = user_schema_clone.clone();
            async move {
                let ids = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
                let names = Arc::new(arrow_array::LargeStringArray::from(vec![
                    "Alice", "Bob", "Charlie",
                ]));
                let emails = Arc::new(arrow_array::LargeStringArray::from(vec![
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                ]));
                let batch = RecordBatch::try_new(schema, vec![ids, names, emails])
                    .map_err(AirportError::Arrow)?;
                let batch_stream = stream::once(async move { Ok(batch) });
                Ok(Box::pin(batch_stream)
                    as airport::catalog::table::SendableRecordBatchStream)
            }
        },
    );

    // Build the catalog
    // Note: schema is named "app" (not "main") because DuckDB treats any schema
    // named "main" as the default and flattens its tables into the catalog root,
    // breaking 3-part name resolution (e.g. auth_flight.main.users fails).
    let catalog = Arc::new(
        CatalogBuilder::new("app")
            .add_schema(StaticSchema::new(
                "app",
                "Application schema - requires authentication",
                vec![Arc::new(users_table)],
            ))
            .build(),
    );

    // Create bearer token authenticator using a sync validation function
    let tokens = valid_tokens();
    let auth = BearerAuth::sync(move |token: &str| {
        tokens
            .get(token)
            .map(|identity| identity.to_string())
            .ok_or_else(|| AirportError::Unauthorized("invalid token".to_string()))
    });

    // Create the Flight service with authentication
    let service = new_server(
        ServerConfig::new(catalog)
            .with_auth(Arc::new(auth))
            .with_address(format!("localhost:{port}")),
    );

    let tokens_info = valid_tokens();
    println!("Authenticated Airport server listening on :{port}");
    println!("Catalog contains:");
    println!("  - Schema: app (requires authentication)");
    println!("    - Table: users (3 rows)");
    println!();
    println!("Valid bearer tokens:");
    for (token, identity) in &tokens_info {
        println!("  {} → {}", token, identity);
    }
    println!();
    println!("Connect from DuckDB:");
    println!("  INSTALL airport FROM community;");
    println!("  LOAD airport;");
    println!("  ATTACH 'grpc://localhost:{port}' AS app (TYPE AIRPORT,");
    println!("      SECRET (TYPE BEARER, TOKEN 'secret-admin-token'));");
    println!("  SELECT * FROM app.app.users;");

    // Start the gRPC server
    tonic::transport::Server::builder()
        .add_service(service)
        .serve(format!("0.0.0.0:{port}").parse()?)
        .await?;

    Ok(())
}
