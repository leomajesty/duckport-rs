use super::server::{BoxStream, Server};
use crate::catalog::table::{find_row_id_column, SendableRecordBatchStream};
use crate::catalog::types::DMLOptions;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use arrow_schema::Schema as ArrowSchema;
use futures::{Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tonic::{Response, Status, Streaming};
use tracing::{debug, error};

/// Final metadata sent back after DML operations.
#[derive(serde::Serialize)]
struct AirportChangedFinalMetadata {
    total_changed: u64,
}

/// Build a FlightData metadata frame carrying the total row count.
fn build_metadata_flight_data(total: i64) -> Result<FlightData, FlightError> {
    let meta = AirportChangedFinalMetadata { total_changed: total as u64 };
    let bytes = msgpack::encode(&meta).map_err(|e| {
        FlightError::Arrow(arrow_schema::ArrowError::IoError(
            e.to_string(),
            std::io::Error::other(e.to_string()),
        ))
    })?;
    Ok(FlightData { app_metadata: bytes.into(), ..Default::default() })
}

/// Build a schema-only response prefix (an encoder over an empty stream).
fn schema_only_stream(
    output_schema: Arc<ArrowSchema>,
) -> impl Stream<Item = Result<FlightData, FlightError>> {
    let empty: SendableRecordBatchStream = Box::pin(futures::stream::empty());
    FlightDataEncoderBuilder::new()
        .with_schema(output_schema)
        .build(empty.map_err(|e| {
            FlightError::Arrow(arrow_schema::ArrowError::IoError(
                e.to_string(),
                std::io::Error::other(e.to_string()),
            ))
        }))
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT
// ─────────────────────────────────────────────────────────────────────────────

/// Handles INSERT operations via DoExchange.
///
/// # Deadlock-free design
///
/// DoExchange is bidirectional: the client sends its schema first and **waits
/// for the server's schema response** before sending data rows.  If we block
/// the handler waiting to read all input before returning the response, both
/// sides stall forever.
///
/// Fix: spawn a background task that reads + processes input, then return the
/// response stream **immediately**.  Tonic polls the stream at once, sends the
/// schema to the client, the client unblocks and sends rows, and the background
/// task drains them.  A oneshot channel delivers the final row count; an mpsc
/// channel carries any RETURNING batches.
pub async fn handle_insert(
    server: &Server,
    ctx: RequestContext,
    stream: Streaming<FlightData>,
    schema_name: &str,
    table_name: &str,
    return_data: bool,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    debug!("DoExchange INSERT: {}.{}", schema_name, table_name);

    let schema_obj = server
        .catalog
        .schema(&ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let table = schema_obj
        .table(&ctx, table_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| {
            Status::not_found(format!("table not found: {}.{}", schema_name, table_name))
        })?;

    let output_schema = table.arrow_schema(&[]);
    let returning_columns: Vec<String> = output_schema
        .fields()
        .iter()
        .filter(|f| f.name() != "rowid" && f.metadata().get("is_rowid").is_none())
        .map(|f| f.name().clone())
        .collect();
    let opts = DMLOptions { returning: return_data, returning_columns };

    // Channels: returning batches (mpsc) + completion signal (oneshot).
    let (returning_tx, returning_rx) = mpsc::channel::<RecordBatch>(4);
    let (done_tx, done_rx) = oneshot::channel::<Result<i64, Status>>();

    let table_clone = table.clone();
    let ctx_clone = ctx.clone();
    let opts_clone = opts.clone();

    // Background task: read input rows, insert, forward any RETURNING batches.
    tokio::spawn(async move {
        let insertable = match table_clone.as_insertable() {
            Some(i) => i,
            None => {
                let _ = done_tx.send(Err(Status::failed_precondition(format!(
                    "table '{}' does not support INSERT",
                    table_clone.name()
                ))));
                return;
            }
        };

        let mut decoder = match FlightRecordBatchStream::new_from_stream(stream) {
            Some(d) => d,
            None => {
                let _ = done_tx.send(Ok(0));
                return;
            }
        };

        let mut total = 0i64;
        while let Some(result) = decoder.next().await {
            let batch = match result {
                Ok(b) => b,
                Err(e) => {
                    error!("INSERT reader: {}", e);
                    let _ = done_tx.send(Err(Status::internal(e.to_string())));
                    return;
                }
            };

            let rows = batch.num_rows() as i64;
            let batch_stream: SendableRecordBatchStream =
                Box::pin(futures::stream::once(async move { Ok(batch) }));

            match insertable.insert(&ctx_clone, batch_stream, &opts_clone).await {
                Ok(result) => {
                    total += result.affected_rows;
                    if return_data {
                        if let Some(reader) = result.returning_data {
                            if let Err(e) =
                                send_returning_data(reader, &returning_tx).await
                            {
                                let _ = done_tx.send(Err(e));
                                return;
                            }
                        }
                    }
                    debug!("INSERT: inserted {} rows (total so far: {})", rows, total);
                }
                Err(e) => {
                    error!("INSERT processor: {}", e);
                    let _ = done_tx.send(Err(e.to_status()));
                    return;
                }
            }
        }

        // Signal completion; dropping returning_tx closes the mpsc channel.
        let _ = done_tx.send(Ok(total));
    });

    // Return response immediately so tonic can start serving it.
    let output_stream = schema_only_stream(output_schema.clone())
        .chain(
            tokio_stream::wrappers::ReceiverStream::new(returning_rx)
                .then(|batch| async move {
                    encode_record_batch(batch).await.map_err(|e| FlightError::Tonic(Box::new(e)))
                }),
        )
        .chain(futures::stream::once(async move {
            match done_rx.await {
                Ok(Ok(total)) => build_metadata_flight_data(total),
                Ok(Err(e)) => Err(FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::other(e.to_string()),
                ))),
                Err(_) => Err(FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    "insert background task dropped".into(),
                    std::io::Error::other("insert background task dropped"),
                ))),
            }
        }))
        .map_err(|e| Status::internal(e.to_string()));

    Ok(Response::new(Box::pin(output_stream)))
}

// ─────────────────────────────────────────────────────────────────────────────
// UPDATE
// ─────────────────────────────────────────────────────────────────────────────

/// Handles UPDATE operations via DoExchange.
/// See [`handle_insert`] for the deadlock-free design rationale.
pub async fn handle_update(
    server: &Server,
    ctx: RequestContext,
    stream: Streaming<FlightData>,
    schema_name: &str,
    table_name: &str,
    return_data: bool,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    debug!("DoExchange UPDATE: {}.{}", schema_name, table_name);

    let schema_obj = server
        .catalog
        .schema(&ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let table = schema_obj
        .table(&ctx, table_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| {
            Status::not_found(format!("table not found: {}.{}", schema_name, table_name))
        })?;

    let output_schema = table.arrow_schema(&[]);
    let returning_columns: Vec<String> = output_schema
        .fields()
        .iter()
        .filter(|f| f.name() != "rowid" && f.metadata().get("is_rowid").is_none())
        .map(|f| f.name().clone())
        .collect();
    let opts = DMLOptions { returning: return_data, returning_columns };

    let (done_tx, done_rx) = oneshot::channel::<Result<i64, Status>>();

    let table_clone = table.clone();
    let ctx_clone = ctx.clone();
    let opts_clone = opts.clone();

    tokio::spawn(async move {
        let batches = match collect_dml_input(stream).await {
            Ok(b) => b,
            Err(e) => { let _ = done_tx.send(Err(e)); return; }
        };

        let mut total = 0i64;
        for batch in &batches {
            let result = if let Some(u) = table_clone.as_updatable_batch() {
                u.update(&ctx_clone, batch.clone(), &opts_clone)
                    .await
                    .map_err(|e| e.to_status())
            } else if let Some(u) = table_clone.as_updatable() {
                let row_ids = match extract_row_ids(batch) {
                    Ok(ids) => ids,
                    Err(e) => { let _ = done_tx.send(Err(e)); return; }
                };
                let batch_owned = batch.clone();
                let rows_stream: SendableRecordBatchStream =
                    Box::pin(futures::stream::once(async move { Ok(batch_owned) }));
                u.update(&ctx_clone, row_ids, rows_stream, &opts_clone)
                    .await
                    .map_err(|e| e.to_status())
            } else {
                Err(Status::failed_precondition(format!(
                    "table '{}' does not support UPDATE",
                    table_clone.name()
                )))
            };

            match result {
                Ok(r) => total += r.affected_rows,
                Err(e) => { let _ = done_tx.send(Err(e)); return; }
            }
        }
        let _ = done_tx.send(Ok(total));
    });

    let output_stream = schema_only_stream(output_schema.clone())
        .chain(futures::stream::once(async move {
            match done_rx.await {
                Ok(Ok(total)) => build_metadata_flight_data(total),
                Ok(Err(e)) => Err(FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::other(e.to_string()),
                ))),
                Err(_) => Err(FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    "update background task dropped".into(),
                    std::io::Error::other("update background task dropped"),
                ))),
            }
        }))
        .map_err(|e| Status::internal(e.to_string()));

    Ok(Response::new(Box::pin(output_stream)))
}

// ─────────────────────────────────────────────────────────────────────────────
// DELETE
// ─────────────────────────────────────────────────────────────────────────────

/// Handles DELETE operations via DoExchange.
/// See [`handle_insert`] for the deadlock-free design rationale.
pub async fn handle_delete(
    server: &Server,
    ctx: RequestContext,
    stream: Streaming<FlightData>,
    schema_name: &str,
    table_name: &str,
    return_data: bool,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    debug!("DoExchange DELETE: {}.{}", schema_name, table_name);

    let schema_obj = server
        .catalog
        .schema(&ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let table = schema_obj
        .table(&ctx, table_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| {
            Status::not_found(format!("table not found: {}.{}", schema_name, table_name))
        })?;

    let output_schema = table.arrow_schema(&[]);
    let returning_columns: Vec<String> = output_schema
        .fields()
        .iter()
        .filter(|f| f.name() != "rowid" && f.metadata().get("is_rowid").is_none())
        .map(|f| f.name().clone())
        .collect();
    let opts = DMLOptions { returning: return_data, returning_columns };

    let (done_tx, done_rx) = oneshot::channel::<Result<i64, Status>>();

    let table_clone = table.clone();
    let ctx_clone = ctx.clone();
    let opts_clone = opts.clone();

    tokio::spawn(async move {
        let batches = match collect_dml_input(stream).await {
            Ok(b) => b,
            Err(e) => { let _ = done_tx.send(Err(e)); return; }
        };

        let mut total = 0i64;
        for batch in &batches {
            let result = if let Some(d) = table_clone.as_deletable_batch() {
                d.delete(&ctx_clone, batch.clone(), &opts_clone)
                    .await
                    .map_err(|e| e.to_status())
            } else if let Some(d) = table_clone.as_deletable() {
                let row_ids = match extract_row_ids(batch) {
                    Ok(ids) => ids,
                    Err(e) => { let _ = done_tx.send(Err(e)); return; }
                };
                d.delete(&ctx_clone, row_ids, &opts_clone)
                    .await
                    .map_err(|e| e.to_status())
            } else {
                Err(Status::failed_precondition(format!(
                    "table '{}' does not support DELETE",
                    table_clone.name()
                )))
            };

            match result {
                Ok(r) => total += r.affected_rows,
                Err(e) => { let _ = done_tx.send(Err(e)); return; }
            }
        }
        let _ = done_tx.send(Ok(total));
    });

    let output_stream = schema_only_stream(output_schema.clone())
        .chain(futures::stream::once(async move {
            match done_rx.await {
                Ok(Ok(total)) => build_metadata_flight_data(total),
                Ok(Err(e)) => Err(FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::other(e.to_string()),
                ))),
                Err(_) => Err(FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    "delete background task dropped".into(),
                    std::io::Error::other("delete background task dropped"),
                ))),
            }
        }))
        .map_err(|e| Status::internal(e.to_string()));

    Ok(Response::new(Box::pin(output_stream)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Sends RETURNING data from a RecordBatchReader through an mpsc channel.
async fn send_returning_data(
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    output_tx: &mpsc::Sender<RecordBatch>,
) -> Result<(), Status> {
    let mut reader = reader;
    while let Some(result) = futures::stream::iter(reader.as_mut()).next().await {
        let batch = result.map_err(|e| Status::internal(e.to_string()))?;
        output_tx
            .send(batch)
            .await
            .map_err(|_| Status::internal("output channel closed"))?;
    }
    Ok(())
}

/// Encodes a single RecordBatch into a FlightData frame.
async fn encode_record_batch(batch: RecordBatch) -> Result<FlightData, Status> {
    let schema = batch.schema();
    let stream: SendableRecordBatchStream =
        Box::pin(futures::stream::once(async move { Ok(batch) }));
    let mut encoder = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream.map_err(|e| {
            FlightError::Arrow(arrow_schema::ArrowError::IoError(
                e.to_string(),
                std::io::Error::other(e.to_string()),
            ))
        }));
    futures::TryStreamExt::try_next(&mut encoder)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::internal("encoder produced no output"))
}

fn extract_row_ids(batch: &RecordBatch) -> Result<Vec<i64>, Status> {
    let rowid_idx = find_row_id_column(batch.schema_ref())
        .ok_or_else(|| Status::invalid_argument("rowid column not found"))?;
    let col = batch.column(rowid_idx);
    let int_arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Status::invalid_argument("rowid column is not Int64"))?;
    let mut ids = Vec::with_capacity(int_arr.len());
    for i in 0..int_arr.len() {
        if int_arr.is_null(i) {
            return Err(Status::invalid_argument("null rowid not allowed"));
        }
        ids.push(int_arr.value(i));
    }
    Ok(ids)
}

/// Reads all RecordBatches from a DoExchange input stream.
///
/// Control messages (data_header.len() < 4) are filtered out before IPC
/// decoding so that the FlightRecordBatchStream only sees valid Arrow IPC
/// messages.
async fn collect_dml_input(
    stream: Streaming<FlightData>,
) -> Result<Vec<RecordBatch>, Status> {
    debug!("collect_dml_input: starting");

    use arrow_flight::decode::FlightRecordBatchStream as ArrowFlightStream;

    let filtered = stream
        .map_err(|e| FlightError::Tonic(Box::new(e)))
        .filter_map(|data| async move {
            match data {
                Ok(d) if d.data_header.len() < 4 => {
                    debug!(
                        "collect_dml_input: skipping control message (header_len={})",
                        d.data_header.len()
                    );
                    None
                }
                Ok(d) => Some(Ok(d)),
                Err(e) => Some(Err(e)),
            }
        });

    let mut decoder = ArrowFlightStream::new_from_flight_data(filtered);
    let mut batches = Vec::new();

    while let Some(result) = decoder.next().await {
        match result {
            Ok(batch) => {
                debug!(
                    "collect_dml_input: received batch {} rows × {} cols",
                    batch.num_rows(),
                    batch.num_columns()
                );
                batches.push(batch);
            }
            Err(e) => {
                debug!("collect_dml_input: stream error: {}", e);
                break;
            }
        }
    }

    debug!("collect_dml_input: done, {} batches", batches.len());
    Ok(batches)
}

// ─────────────────────────────────────────────────────────────────────────────
// FlightRecordBatchStream – wrapper used by the INSERT reader task
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps `arrow_flight::decode::FlightRecordBatchStream`, filtering control
/// messages (data_header.len() < 4) before IPC decoding.
struct FlightRecordBatchStream {
    inner: arrow_flight::decode::FlightRecordBatchStream,
}

impl FlightRecordBatchStream {
    fn new_from_stream(stream: Streaming<FlightData>) -> Option<Self> {
        let filtered = stream
            .map_err(|e| FlightError::Tonic(Box::new(e)))
            .filter_map(|data| async move {
                match data {
                    Ok(d) if d.data_header.len() < 4 => None,
                    Ok(d) => Some(Ok(d)),
                    Err(e) => Some(Err(e)),
                }
            });
        let inner =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(filtered);
        Some(Self { inner })
    }
}

impl Stream for FlightRecordBatchStream {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(
                arrow_schema::ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::other(e.to_string()),
                ),
            ))),
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Pending => Poll::Pending,
        }
    }
}
