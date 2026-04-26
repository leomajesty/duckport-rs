//! Configuration loaded from environment variables with sensible defaults.
//!
//! All env vars share the `DUCKPORT_` prefix.

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the DuckDB database file. Use `:memory:` for an in-memory DB.
    pub db_path: PathBuf,

    /// Flight/gRPC listen address.
    pub listen_addr: SocketAddr,

    /// Advertised address used when building FlightEndpoint URIs.
    /// If empty, clients fall back to the address they connected to.
    pub advertised_addr: String,

    /// Catalog name exposed via Airport (what DuckDB clients `ATTACH ... AS <name>`).
    pub catalog_name: String,

    /// Size of the read-side DuckDB connection pool.
    pub read_pool_size: u32,

    /// DuckDB `threads` PRAGMA (0 = DuckDB default).
    pub duckdb_threads: u32,

    /// DuckDB `memory_limit` PRAGMA (empty = DuckDB default).
    pub duckdb_memory_limit: String,

    /// Whether the periodic retention scheduler (COPY+DELETE) is active.
    /// **Plan A default: off** — keep all data in the hot DuckDB file.
    /// Set `DUCKPORT_RETENTION_ENABLED=true` to re-enable legacy export+delete.
    pub retention_enabled: bool,

    /// Fully-qualified table that stores per-market retention tasks.
    /// Default: `data.retention_tasks`.
    /// Set via `DUCKPORT_RETENTION_TABLE`.
    pub retention_table: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let db_path = env::var("DUCKPORT_DB_PATH").unwrap_or_else(|_| "./duckport.db".to_string());
        let listen = env::var("DUCKPORT_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string());
        let advertised =
            env::var("DUCKPORT_ADVERTISED_ADDR").unwrap_or_else(|_| String::new());
        let catalog = env::var("DUCKPORT_CATALOG_NAME").unwrap_or_else(|_| "duckport".to_string());
        let pool = parse_env_u32("DUCKPORT_READ_POOL_SIZE", 4)?;
        let threads = parse_env_u32("DUCKPORT_DUCKDB_THREADS", 0)?;
        let mem = env::var("DUCKPORT_DUCKDB_MEMORY_LIMIT").unwrap_or_else(|_| String::new());

        let retention_enabled = env::var("DUCKPORT_RETENTION_ENABLED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);
        let retention_table = env::var("DUCKPORT_RETENTION_TABLE")
            .unwrap_or_else(|_| "data.retention_tasks".to_string());

        Ok(Self {
            db_path: PathBuf::from(db_path),
            listen_addr: listen
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid DUCKPORT_LISTEN_ADDR '{listen}': {e}"))?,
            advertised_addr: advertised,
            catalog_name: catalog,
            read_pool_size: pool.max(1),
            duckdb_threads: threads,
            duckdb_memory_limit: mem,
            retention_enabled,
            retention_table,
        })
    }
}

fn parse_env_u32(key: &str, default: u32) -> anyhow::Result<u32> {
    match env::var(key) {
        Ok(v) => v
            .parse::<u32>()
            .map_err(|e| anyhow::anyhow!("{key}='{v}' is not a u32: {e}")),
        Err(_) => Ok(default),
    }
}
