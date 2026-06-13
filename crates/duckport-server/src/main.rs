//! duckport-server — gRPC database service on top of DuckDB.
//!
//! A DuckDB-backed `airport::Catalog` is exposed over a tonic Flight service for the
//! read plane, wrapped by `DuckportService` which adds the custom `duckport.*` write
//! plane. Writes and DDL go exclusively through `duckport.execute` /
//! `duckport.execute_transaction` / `duckport.append`; Airport's DynamicCatalog and
//! TransactionManager are intentionally left unwired (single-track DDL).

mod airport_adapter;
mod backend;
mod config;
mod retention;
mod write_plane;

use std::sync::Arc;

use airport::flight::Server as AirportServer;
use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::airport_adapter::DuckDbCatalog;
use crate::backend::Backend;
use crate::config::Config;
use crate::write_plane::DuckportService;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env()?;
    info!(
        db_path = %cfg.db_path.display(),
        listen = %cfg.listen_addr,
        catalog = %cfg.catalog_name,
        read_pool_size = cfg.read_pool_size,
        "duckport starting"
    );

    let backend = Backend::open(
        &cfg.db_path,
        cfg.read_pool_size,
        cfg.duckdb_threads,
        &cfg.duckdb_memory_limit,
    )?;

    init_system_schema(&backend).await?;

    // Seed demo data if requested (handy for Phase 1 end-to-end tests).
    if std::env::var("DUCKPORT_SEED_DEMO").ok().as_deref() == Some("1") {
        seed_demo(&backend).await?;
    }

    let catalog: Arc<dyn airport::catalog::Catalog> =
        DuckDbCatalog::new(cfg.catalog_name.clone(), backend.clone());

    let advertised = if cfg.advertised_addr.is_empty() {
        cfg.listen_addr.to_string()
    } else {
        cfg.advertised_addr.clone()
    };

    // Build the airport inner server directly so we can wrap it in DuckportService
    // (which intercepts `duckport.*` DoActions for the custom write plane).
    let airport_server = Arc::new(AirportServer::new(
        catalog,
        None, // no auth yet — see roadmap (Bearer token)
        advertised.clone(),
        None, // tx_manager intentionally unwired: transactions go via duckport.execute_transaction (single-track)
        cfg.catalog_name.clone(),
    ));

    if cfg.retention_enabled {
        info!(table = %cfg.retention_table, "retention scheduler enabled");
        retention::spawn(backend.clone(), cfg.retention_table.clone());
    } else {
        info!("retention scheduler disabled (DUCKPORT_RETENTION_ENABLED=false)");
    }

    let duckport_svc =
        DuckportService::new(airport_server, backend.clone(), cfg.catalog_name.clone());
    let max_msg = 64 * 1024 * 1024;
    let flight_svc = FlightServiceServer::new(duckport_svc)
        .max_decoding_message_size(max_msg)
        .max_encoding_message_size(max_msg);

    info!(%advertised, "duckport Flight service ready (airport read plane + duckport.* write plane)");
    tonic::transport::Server::builder()
        .add_service(flight_svc)
        .serve(cfg.listen_addr)
        .await
        .context("tonic transport")?;

    Ok(())
}

/// Create and migrate duckport-managed metadata tables.
///
/// Ingestors own their business tables, but duckport-server owns shared metadata
/// schema such as `data.watermark`. This keeps `duckport status` and future
/// plugins on one stable contract.
async fn init_system_schema(backend: &Backend) -> Result<()> {
    backend
        .with_writer(|conn| {
            conn.execute_batch(
                r#"
                CREATE SCHEMA IF NOT EXISTS data;
                CREATE TABLE IF NOT EXISTS data.config_dict (
                    key   VARCHAR PRIMARY KEY,
                    value VARCHAR
                );
                CREATE TABLE IF NOT EXISTS data.watermark (
                    table_name      VARCHAR PRIMARY KEY,
                    ingestor        VARCHAR,
                    max_lag_seconds INTEGER,
                    time_column     VARCHAR NOT NULL,
                    start_time      TIMESTAMP,
                    duck_time       TIMESTAMP,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS ingestor VARCHAR;
                ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS max_lag_seconds INTEGER;
                ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS time_column VARCHAR;
                ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS start_time TIMESTAMP;
                ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS duck_time TIMESTAMP;
                ALTER TABLE data.watermark ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                "#,
            )?;
            Ok(())
        })
        .await?;
    backend.bump_catalog_epoch();
    info!("initialised duckport system schema data.config_dict + data.watermark");
    Ok(())
}

/// Create a minimal schema + table so that a freshly-started server can be queried
/// end-to-end without an external ingestor.
///
/// We run the seed on the WRITER connection so that the writer's snapshot sees the
/// new schema immediately. Running it via the reader pool leaves the writer stuck on
/// its pre-seed snapshot and every subsequent write RPC fails with "schema not found".
async fn seed_demo(backend: &Backend) -> Result<()> {
    backend
        .with_writer(|conn| {
            conn.execute_batch(
                r#"
                CREATE SCHEMA IF NOT EXISTS app;
                CREATE TABLE IF NOT EXISTS app.users (id BIGINT, name VARCHAR);
                DELETE FROM app.users;
                INSERT INTO app.users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
                "#,
            )?;
            Ok(())
        })
        .await?;
    backend.bump_catalog_epoch();
    info!("seeded demo schema 'app' with table users");
    Ok(())
}
