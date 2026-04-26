//! `DoPut` handler for bulk Arrow append.
//!
//! Protocol:
//! - FlightDescriptor path = `["duckport.append", <schema>, <table>]`
//! - Body is a standard Arrow IPC stream (first message carries the schema,
//!   subsequent messages carry RecordBatches).
//! - The server returns a single `PutResult` whose `app_metadata` is a JSON
//!   `AppendResponse`.
//!
//! Semantics:
//! - The whole stream is appended **atomically** inside a single
//!   `BEGIN ... COMMIT` on the dedicated writer connection. Any error — decode,
//!   schema mismatch, DuckDB constraint — rolls the transaction back, so the
//!   table is unchanged.
//! - We buffer all batches in memory before acquiring the writer lock. This
//!   simplifies error handling (no half-applied state) and keeps the writer
//!   lock hold time proportional to compute, not to network I/O. Clients that
//!   push many-GB streams should chunk their DoPut calls.

use anyhow::{anyhow, Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightData, PutResult};
use bytes::Bytes;
use futures::{stream, StreamExt};
use tonic::{Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::backend::Backend;

use super::proto::AppendResponse;
use super::DuckportStream;

/// Descriptor path prefix that routes a DoPut to this handler.
pub const APPEND_PATH_PREFIX: &str = "duckport.append";

/// Entry point. Must be given the full incoming stream; the first FlightData
/// has already been peeked to extract `schema`/`table` but is re-fed here via
/// `first`.
pub async fn handle_append(
    backend: Backend,
    schema: String,
    table: String,
    first: FlightData,
    rest: Streaming<FlightData>,
) -> Result<Response<DuckportStream<PutResult>>, Status> {
    debug!(%schema, %table, "duckport.append do_put: decoding stream");

    // Re-attach the peeked first message and map tonic errors into the Flight
    // decoder's error type. The decoder expects a `Stream<Item=Result<FlightData, FlightError>>`.
    let rest = rest.map(|r| r.map_err(|s| FlightError::Tonic(Box::new(s))));
    let merged = stream::once(async move { Ok::<FlightData, FlightError>(first) }).chain(rest);
    let mut rb_stream = FlightRecordBatchStream::new_from_flight_data(merged);

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(item) = rb_stream.next().await {
        match item {
            Ok(b) => batches.push(b),
            Err(e) => {
                warn!(%schema, %table, err = %e, "decode error");
                return Err(Status::invalid_argument(format!(
                    "invalid Arrow IPC stream: {e}"
                )));
            }
        }
    }

    let num_batches = batches.len() as u64;
    let input_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    debug!(
        %schema,
        %table,
        batches = num_batches,
        rows = input_rows,
        "decoded stream; acquiring writer"
    );

    // Clone names for the writer closure; we still need the originals for the
    // response.
    let schema_w = schema.clone();
    let table_w = table.clone();
    let rows = backend
        .with_writer(move |conn| run_append(conn, &schema_w, &table_w, batches))
        .await
        .map_err(|e| {
            warn!(%schema, %table, err = ?e, "duckport.append failed");
            Status::internal(format!("{e:#}"))
        })?;

    let epoch = backend.bump_catalog_epoch();
    info!(
        %schema,
        %table,
        rows,
        batches = num_batches,
        epoch,
        "duckport.append ok"
    );

    let resp = AppendResponse {
        schema,
        table,
        batches: num_batches,
        rows_appended: rows,
    };
    let body = serde_json::to_vec(&resp)
        .map_err(|e| Status::internal(format!("encode AppendResponse: {e}")))?;
    let put = PutResult {
        app_metadata: Bytes::from(body),
    };
    let out: DuckportStream<PutResult> = Box::pin(stream::iter(std::iter::once(Ok(put))));
    Ok(Response::new(out))
}

/// Blocking writer-side work: open a DuckDB Appender on the target table and
/// append every RecordBatch, wrapped in `BEGIN/COMMIT`.
fn run_append(
    conn: &mut duckdb::Connection,
    schema: &str,
    table: &str,
    batches: Vec<RecordBatch>,
) -> Result<u64> {
    conn.execute_batch("BEGIN TRANSACTION")
        .context("BEGIN TRANSACTION")?;

    let result: Result<u64> = (|| {
        let mut appender = conn
            .appender_to_db(table, schema)
            .with_context(|| format!("open appender {schema}.{table}"))?;

        let mut total: u64 = 0;
        for (idx, batch) in batches.into_iter().enumerate() {
            let rows = batch.num_rows() as u64;
            appender
                .append_record_batch(batch)
                .with_context(|| format!("append_record_batch #{idx}"))?;
            total += rows;
        }
        appender.flush().context("appender flush")?;
        Ok(total)
    })();

    match result {
        Ok(total) => {
            conn.execute_batch("COMMIT").context("COMMIT")?;
            Ok(total)
        }
        Err(err) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                warn!(err = ?rb, "ROLLBACK failed after append error");
            }
            Err(err.context(anyhow!("append rolled back")))
        }
    }
}
