//! Custom DoGet handler for `duckport.query` tickets.
//!
//! Accepts arbitrary read-only SQL, executes it on a **reader** connection
//! (never the writer), and streams Arrow RecordBatches back to the client.

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::FlightData;
use futures::TryStreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::debug;

use crate::access::DoGetLimits;
use crate::backend::Backend;
use crate::read_limit::{self, recover_status};

use super::{truncate_for_log, DuckportStream};

const QUERY_CHANNEL_BUFFER: usize = 4;

/// Allowed SQL statement prefixes (case-insensitive, after trimming whitespace).
const ALLOWED_PREFIXES: &[&str] = &["select", "with", "explain"];

/// Validates that the SQL is a read-only statement.
fn validate_read_only(sql: &str) -> Result<(), Status> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument("sql cannot be empty"));
    }
    let lower = trimmed.to_ascii_lowercase();
    for prefix in ALLOWED_PREFIXES {
        if lower.starts_with(prefix) {
            return Ok(());
        }
    }
    Err(Status::permission_denied(format!(
        "only SELECT/WITH/EXPLAIN statements are allowed via duckport.query; \
         got: {}",
        &trimmed[..trimmed.len().min(60)]
    )))
}

/// Entry point: parse + validate SQL, execute on a reader connection, and
/// stream results back as Flight data.
pub async fn handle_query(
    backend: Backend,
    sql: &str,
    limits: Option<DoGetLimits>,
) -> Result<Response<DuckportStream<FlightData>>, Status> {
    validate_read_only(sql)?;

    let sql = sql.to_string();
    let sql_for_log = sql.clone();
    debug!(sql = %truncate_for_log(&sql_for_log, 200), "duckport.query");

    let (tx, rx) = mpsc::channel(QUERY_CHANNEL_BUFFER);

    tokio::task::spawn_blocking(move || {
        let conn = match backend.reader() {
            Ok(c) => c,
            Err(e) => {
                let _ = tx.blocking_send(Err(read_limit::ScanFail::Internal(format!(
                    "acquire reader conn: {e:#}"
                ))));
                return;
            }
        };
        read_limit::execute_arrow_query(&conn, &sql, limits, |item| tx.blocking_send(item).is_ok());
    });

    let batch_stream = ReceiverStream::new(rx);

    let flight_stream = FlightDataEncoderBuilder::new()
        .build(batch_stream.map_err(|fail| {
            let marked = fail.marked();
            arrow_flight::error::FlightError::Arrow(arrow_schema::ArrowError::IoError(
                marked.clone(),
                std::io::Error::other(marked),
            ))
        }))
        .map_err(|e| recover_status(&e.to_string()));

    Ok(Response::new(Box::pin(flight_stream)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_read_only() {
        assert!(validate_read_only("SELECT 1").is_ok());
        assert!(validate_read_only("  select * from t").is_ok());
        assert!(validate_read_only("WITH cte AS (SELECT 1) SELECT * FROM cte").is_ok());
        assert!(validate_read_only("EXPLAIN SELECT 1").is_ok());
        assert!(validate_read_only("explain analyze SELECT 1").is_ok());

        assert!(validate_read_only("INSERT INTO t VALUES (1)").is_err());
        assert!(validate_read_only("DELETE FROM t").is_err());
        assert!(validate_read_only("UPDATE t SET x = 1").is_err());
        assert!(validate_read_only("DROP TABLE t").is_err());
        assert!(validate_read_only("CREATE TABLE t (x INT)").is_err());
        assert!(validate_read_only("").is_err());
        assert!(validate_read_only("  ").is_err());
    }
}
