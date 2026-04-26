//! # airport
//!
//! A Rust library for building Apache Arrow Flight servers compatible with
//! the [DuckDB Airport Extension](https://github.com/Query-farm/airport).
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use airport::catalog::static_catalog::{CatalogBuilder, StaticSchema, StaticTable};
//! use airport::server::new_server;
//! use airport::config::ServerConfig;
//! use arrow_schema::{DataType, Field, Schema};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int64, false),
//!         Field::new("name", DataType::Utf8, true),
//!     ]));
//!
//!     let catalog = CatalogBuilder::new("my_catalog")
//!         .add_schema(
//!             StaticSchema::new("main", "Main schema", vec![])
//!         )
//!         .build();
//!
//!     let service = new_server(ServerConfig::new(Arc::new(catalog)));
//!
//!     tonic::transport::Server::builder()
//!         .add_service(service)
//!         .serve("0.0.0.0:50052".parse().unwrap())
//!         .await
//!         .unwrap();
//! }
//! ```

// Core error type
pub mod error;

// Catalog interfaces and implementations
pub mod catalog;

// Authentication
#[path = "auth/mod.rs"]
pub mod auth;

// Filter pushdown
pub mod filter;

// Internal utilities (not part of the public API)
pub(crate) mod internal;

// Arrow Flight server implementation
pub mod flight;

// Server configuration and entry point
pub mod config;
pub mod server;

// Multi-catalog routing
pub mod multicatalog;

// Re-exports for convenience
pub use catalog::{Catalog, NamedCatalog, Schema, DEFAULT_SCHEMA_NAME};
pub use catalog::table::{
    DeletableBatchTable, DeletableTable, DynamicSchemaTable, DynamicTable, InsertableTable,
    SendableRecordBatchStream, StatisticsTable, Table, UpdatableBatchTable, UpdatableTable,
};
pub use catalog::function::{ScalarFunction, TableFunction, TableFunctionInOut};
pub use catalog::transaction::{CatalogTransactionManager, TransactionManager, TransactionState};
pub use catalog::types::{
    CatalogVersion, ColumnStats, DMLOptions, DMLResult, FunctionSignature, OnConflict, ScanOptions,
    TimePoint,
};
pub use catalog::static_catalog::{CatalogBuilder, StaticCatalog, StaticSchema, StaticTable};
pub use config::ServerConfig;
pub use error::AirportError;
pub use filter::{FilterPushdown, Filters};
pub use flight::{RequestContext, new_flight_service};
pub use auth::{Authenticator, AuthenticatorRef, BearerAuth, CatalogAuthorizer, NoAuth, no_auth};
pub use server::new_server;
pub use multicatalog::{MultiCatalogConfig, MultiCatalogServer};
