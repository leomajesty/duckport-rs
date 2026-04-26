use super::server::{BoxStream, Server};
use crate::flight::context::RequestContext;
use arrow_flight::{Criteria, FlightInfo};
use tonic::{Response, Status};

/// Handles ListFlights RPC: returns all available tables as FlightInfo objects.
pub async fn handle_list_flights(
    _server: &Server,
    _ctx: &RequestContext,
    _criteria: Criteria,
) -> Result<Response<BoxStream<FlightInfo>>, Status> {
    // ListFlights is not commonly used by DuckDB Airport Extension.
    // Return empty stream for now.
    let stream = futures::stream::empty();
    Ok(Response::new(Box::pin(stream)))
}
