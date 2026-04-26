//! Wrapper around arrow-flight's `FlightRecordBatchStream` that gracefully handles
//! DuckDB control messages with empty data_header.
//!
//! DuckDB sends messages with `data_header.len() < 4` as control messages that
//! should be skipped. This wrapper handles those gracefully by filtering them
//! before passing to FlightRecordBatchStream.

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::{FlightData, error::Result as FlightResult};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::Status;

/// Wrapper that provides a filtered FlightData stream for FlightRecordBatchStream.
/// Filters out control messages (short data_header) before passing to the decoder.
pub struct AirportRecordBatchStream {
    inner: FlightRecordBatchStream,
}

impl AirportRecordBatchStream {
    /// Create from a stream of FlightData (from DoExchange).
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = FlightResult<FlightData>> + Send + 'static,
    {
        // Filter control messages before passing to FlightRecordBatchStream
        let filtered = stream
            .filter_map(|data| async move {
                match data {
                    Ok(data) if data.data_header.len() < 4 => None,
                    other => Some(other),
                }
            })
            .boxed();
        let inner = FlightRecordBatchStream::new_from_flight_data(filtered);
        Self { inner }
    }

    /// Returns the current schema for this stream.
    pub fn schema(&self) -> Option<&Arc<arrow_schema::Schema>> {
        self.inner.schema()
    }

    /// Consume self and return the wrapped stream.
    pub fn into_inner(self) -> FlightRecordBatchStream {
        self.inner
    }
}

impl Stream for AirportRecordBatchStream {
    type Item = Result<RecordBatch, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Delegate to inner FlightRecordBatchStream
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => {
                let status = Status::internal(e.to_string());
                Poll::Ready(Some(Err(status)))
            }
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Pending => Poll::Pending,
        }
    }
}