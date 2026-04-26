//! JSON wire types for `duckport.*` DoActions.
//!
//! We use JSON (not msgpack) for debuggability. Switch to msgpack later if the
//! round-trip overhead shows up in profiles.

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// SQL to execute. Must be a single statement. Parameters are not yet supported;
    /// callers should inline literals (integers, quoted strings) or use `execute_transaction`
    /// for multi-statement batches.
    pub sql: String,
}

#[derive(Debug, Serialize)]
pub struct ExecuteResponse {
    pub rows_affected: u64,
}

#[derive(Debug, Deserialize)]
pub struct ExecuteTransactionRequest {
    /// Ordered list of statements. Executed inside a single `BEGIN ... COMMIT` block
    /// on the writer connection. On any failure the whole batch rolls back.
    pub statements: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ExecuteTransactionResponse {
    pub rows_affected: Vec<u64>,
}

#[derive(Debug, Serialize)]
pub struct PingResponse {
    pub server: &'static str,
    pub server_version: &'static str,
    pub catalog: String,
    pub duckdb_version: String,
}

/// Ticket for custom `duckport.query` DoGet requests.
///
/// Consumers send `{"type": "duckport.query", "sql": "SELECT ..."}` as the
/// Flight ticket. The `type` discriminator allows `do_get` to distinguish
/// custom query tickets from airport's standard `{schema, table, ...}` tickets.
#[derive(Debug, Deserialize)]
pub struct QueryTicket {
    /// Must be `"duckport.query"`.
    pub r#type: String,
    /// SQL to execute on a reader connection. Only SELECT/WITH/EXPLAIN allowed.
    pub sql: String,
}

/// Response for `DoPut` with descriptor path `["duckport.append", schema, table]`.
/// Emitted once as the final `PutResult.app_metadata` after the full Arrow stream
/// is appended atomically.
#[derive(Debug, Serialize)]
pub struct AppendResponse {
    pub schema: String,
    pub table: String,
    pub batches: u64,
    pub rows_appended: u64,
}
