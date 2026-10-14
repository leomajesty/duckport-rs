//! Configuration loaded from environment variables with sensible defaults.
//!
//! All env vars share the `DUCKPORT_` prefix.

use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use ipnet::IpNet;

use crate::access::parse_cidrs;

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

    /// `name:token` entries. Empty disables every public peer; LAN stays open.
    pub auth_tokens: String,

    /// Extra CIDRs treated as LAN for writes. Loopback is never taken from this list.
    pub trusted_cidrs: Vec<IpNet>,

    /// Public DoGet Arrow-batch byte ceiling. Zero disables the byte check.
    pub doget_max_bytes: u64,

    /// Public DoGet timeout. Zero disables the timeout.
    pub doget_timeout: Duration,

    /// PEM material when both TLS env vars are set.
    pub tls: Option<TlsMaterial>,
}

/// Server certificate and private key, already read from disk.
#[derive(Clone)]
pub struct TlsMaterial {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsMaterial")
            .field("cert_pem_len", &self.cert_pem.len())
            .field("key_pem_len", &self.key_pem.len())
            .finish()
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let db_path = env::var("DUCKPORT_DB_PATH").unwrap_or_else(|_| "./duckport.db".to_string());
        let listen =
            env::var("DUCKPORT_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
        let advertised = env::var("DUCKPORT_ADVERTISED_ADDR").unwrap_or_else(|_| String::new());
        let catalog = env::var("DUCKPORT_CATALOG_NAME").unwrap_or_else(|_| "duckport".to_string());
        let pool = parse_env_u32("DUCKPORT_READ_POOL_SIZE", 4)?;
        let threads = parse_env_u32("DUCKPORT_DUCKDB_THREADS", 0)?;
        let mem = env::var("DUCKPORT_DUCKDB_MEMORY_LIMIT").unwrap_or_else(|_| String::new());

        let retention_enabled = env::var("DUCKPORT_RETENTION_ENABLED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);
        let retention_table = env::var("DUCKPORT_RETENTION_TABLE")
            .unwrap_or_else(|_| "data.retention_tasks".to_string());
        let auth_tokens = env::var("DUCKPORT_AUTH_TOKENS").unwrap_or_default();
        let trusted_raw = env::var("DUCKPORT_TRUSTED_CIDRS").unwrap_or_default();
        let trusted_cidrs = parse_cidrs(&trusted_raw)?;
        let doget_max_bytes = parse_env_u64("DUCKPORT_DOGET_MAX_BYTES", 16_777_216)?;
        let doget_timeout_secs = parse_env_u64("DUCKPORT_DOGET_TIMEOUT_SECS", 10)?;
        let tls = load_tls()?;

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
            auth_tokens,
            trusted_cidrs,
            doget_max_bytes,
            doget_timeout: Duration::from_secs(doget_timeout_secs),
            tls,
        })
    }
}

fn load_tls() -> anyhow::Result<Option<TlsMaterial>> {
    let cert = env::var("DUCKPORT_TLS_CERT").unwrap_or_default();
    let key = env::var("DUCKPORT_TLS_KEY").unwrap_or_default();
    match (cert.is_empty(), key.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => {
            let cert_pem = fs::read(&cert)
                .map_err(|e| anyhow::anyhow!("read DUCKPORT_TLS_CERT '{cert}': {e}"))?;
            let key_pem = fs::read(&key)
                .map_err(|e| anyhow::anyhow!("read DUCKPORT_TLS_KEY '{key}': {e}"))?;
            Ok(Some(TlsMaterial { cert_pem, key_pem }))
        }
        _ => anyhow::bail!(
            "DUCKPORT_TLS_CERT and DUCKPORT_TLS_KEY must both be set or both be empty"
        ),
    }
}

fn parse_env_u64(key: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(key) {
        Ok(v) => v
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("{key}='{v}' is not a u64: {e}")),
        Err(_) => Ok(default),
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
