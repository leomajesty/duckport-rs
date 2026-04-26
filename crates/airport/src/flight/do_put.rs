use super::server::{BoxStream, Server};
use crate::flight::context::RequestContext;
use arrow_flight::{FlightData, PutResult};
use arrow_flight::error::FlightError;
use futures::TryStreamExt;
use tonic::{Response, Status, Streaming};
use tracing::debug;

/// DoPut handler receives data from clients for general data transfer.
/// For DML operations, data arrives via DoExchange instead.
pub async fn handle_do_put(
    server: &Server,
    ctx: RequestContext,
    stream: Streaming<FlightData>,
) -> Result<Response<BoxStream<PutResult>>, Status> {
    debug!("DoPut called");

    // Collect all data from the stream to consume it properly
    let mut stream = stream.map_err(|e| FlightError::Tonic(Box::new(e)));
    let mut batch_count = 0;

    loop {
        match futures::StreamExt::next(&mut stream).await {
            None => {
                debug!("DoPut: stream ended ({} batches)", batch_count);
                break;
            }
            Some(Err(e)) => {
                debug!("DoPut: stream error: {}", e);
                break;
            }
            Some(Ok(_data)) => {
                batch_count += 1;
            }
        }
    }

    debug!("DoPut: received {} batches", batch_count);

    // Return empty result
    let empty_stream = futures::stream::empty();
    Ok(Response::new(Box::pin(empty_stream)))
}