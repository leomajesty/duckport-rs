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

    /// Whether the rolling-window retention scheduler runs (`DUCKPORT_RETENTION_ENABLED`).
    /// Default off: keep all data. Policies live in `data.retention_policy`.
    pub retention_enabled: bool,

    /// Count rows only, never archive or delete (`DUCKPORT_RETENTION_DRY_RUN`, default true).
    pub retention_dry_run: bool,

    /// Pause between scheduled runs (`DUCKPORT_RETENTION_INTERVAL_SECS`, default 3600, min 60).
    pub retention_interval: Duration,

    /// Policies keeping fewer days are refused (`DUCKPORT_RETENTION_MIN_KEEP_DAYS`, default 7).
    pub retention_min_keep_days: u32,

    /// Pause between two DELETE batches (`DUCKPORT_RETENTION_BATCH_PAUSE_MS`, default 200).
    pub retention_batch_pause: Duration,

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

        let retention_enabled = parse_env_bool("DUCKPORT_RETENTION_ENABLED", false)?;
        let retention_dry_run = parse_env_bool("DUCKPORT_RETENTION_DRY_RUN", true)?;
        let retention_interval_secs =
            parse_env_u64("DUCKPORT_RETENTION_INTERVAL_SECS", 3600)?.max(60);
        let retention_min_keep_days = parse_env_u32("DUCKPORT_RETENTION_MIN_KEEP_DAYS", 7)?;
        let retention_batch_pause_ms = parse_env_u64("DUCKPORT_RETENTION_BATCH_PAUSE_MS", 200)?;
        if let Ok(v) = env::var("DUCKPORT_RETENTION_TABLE") {
            tracing::warn!(
                value = %v,
                "DUCKPORT_RETENTION_TABLE is deprecated and ignored; retention policies now live in data.retention_policy"
            );
        }
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
            retention_dry_run,
            retention_interval: Duration::from_secs(retention_interval_secs),
            retention_min_keep_days,
            retention_batch_pause: Duration::from_millis(retention_batch_pause_ms),
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

/// Unset means `default`. Anything other than true/false/1/0/yes/no/on/off (including an
/// empty value) is an error: these flags guard data deletion, so a typo must not silently
/// flip them.
fn parse_env_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(key) {
        Ok(v) => parse_bool(&v)
            .ok_or_else(|| anyhow::anyhow!("{key}='{v}' is not a boolean (use true or false)")),
        Err(_) => Ok(default),
    }
}

fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::parse_bool;

    #[test]
    fn booleans_parse_strictly() {
        for v in ["true", "TRUE", " 1 ", "yes", "on"] {
            assert_eq!(parse_bool(v), Some(true), "{v}");
        }
        for v in ["false", "False", "0", "no", "off"] {
            assert_eq!(parse_bool(v), Some(false), "{v}");
        }
        for v in ["", " ", "ture", "2", "enabled"] {
            assert_eq!(parse_bool(v), None, "{v:?}");
        }
    }
}
