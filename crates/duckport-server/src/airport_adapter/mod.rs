//! Airport-protocol adapter backed by a DuckDB file.
//!
//! Phase 1 scope: read plane only. Implements [`airport::catalog::Catalog`],
//! [`airport::catalog::Schema`], and [`airport::catalog::table::Table`] by
//! delegating to DuckDB's system views (`duckdb_schemas()`, `duckdb_tables()`,
//! `information_schema.columns`).
//!
//! Phase 2 will add the write plane (DynamicCatalog / DynamicSchema / InsertableTable
//! etc.) through a custom Flight service wrapper.

mod catalog;
mod schema;
mod table;

pub use catalog::DuckDbCatalog;
pub use schema::DuckDbSchema;
pub use table::DuckDbTable;

use airport::error::AirportError;

/// Convert a backend/anyhow error into an AirportError with context.
pub(crate) fn to_airport(err: anyhow::Error) -> AirportError {
    AirportError::Internal(format!("{err:#}"))
}

/// Double-quote an SQL identifier, escaping embedded quotes.
/// Used when composing dynamic SQL fragments (schema.table, column list, ...).
pub(crate) fn quote_ident(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}
