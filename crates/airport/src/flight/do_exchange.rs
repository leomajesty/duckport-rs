use super::server::{BoxStream, Server};
use crate::flight::context::RequestContext;
use arrow_flight::FlightData;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

/// Handles DoExchange RPC: dispatches to appropriate handler based on operation type.
pub async fn handle_do_exchange(
    server: &Server,
    ctx: RequestContext,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    let metadata = request.metadata();

    // Extract required headers
    let operation = metadata
        .get("airport-operation")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::invalid_argument("missing airport-operation header"))?
        .to_string();

    let return_chunks = metadata
        .get("return-chunks")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "1")
        .unwrap_or(false);

    let flight_path = metadata
        .get("airport-flight-path")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::invalid_argument("missing airport-flight-path header"))?
        .to_string();

    // Parse flight path: "schema/table" or "schema/function"
    let parts: Vec<&str> = flight_path.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "invalid flight path format: {}",
            flight_path
        )));
    }
    let schema_name = parts[0].to_string();
    let target_name = parts[1].to_string();

    debug!(
        "DoExchange: operation={} schema={} target={} return_chunks={}",
        operation, schema_name, target_name, return_chunks
    );

    let stream = request.into_inner();

    match operation.as_str() {
        "scalar_function" => {
            if !return_chunks {
                return Err(Status::invalid_argument(
                    "missing or invalid return-chunks header for scalar_function",
                ));
            }
            super::do_exchange_functions::handle_scalar_function(
                server,
                ctx,
                stream,
                &schema_name,
                &target_name,
            )
            .await
        }
        "table_function" | "table_function_in_out" => {
            if !return_chunks {
                return Err(Status::invalid_argument(
                    "missing or invalid return-chunks header for table_function",
                ));
            }
            super::do_exchange_functions::handle_table_function_in_out(
                server,
                ctx,
                stream,
                &schema_name,
                &target_name,
            )
            .await
        }
        "insert" => {
            super::do_exchange_dml::handle_insert(
                server,
                ctx,
                stream,
                &schema_name,
                &target_name,
                return_chunks,
            )
            .await
        }
        "update" => {
            super::do_exchange_dml::handle_update(
                server,
                ctx,
                stream,
                &schema_name,
                &target_name,
                return_chunks,
            )
            .await
        }
        "delete" => {
            super::do_exchange_dml::handle_delete(
                server,
                ctx,
                stream,
                &schema_name,
                &target_name,
                return_chunks,
            )
            .await
        }
        op => Err(Status::invalid_argument(format!(
            "invalid airport-operation: {} (expected scalar_function, table_function, table_function_in_out, insert, update, or delete)",
            op
        ))),
    }
}
