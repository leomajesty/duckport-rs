use super::server::{BoxStream, Server};
use super::ticket::TicketData;
use crate::flight::context::RequestContext;
use arrow_flight::{FlightData, Ticket};
use arrow_flight::encode::FlightDataEncoderBuilder;
use futures::{StreamExt, TryStreamExt};
use tonic::{Response, Status};
use tracing::debug;

/// Handles DoGet RPC: streams Arrow record batches for a table query.
pub async fn handle_do_get(
    server: &Server,
    ctx: &RequestContext,
    ticket: Ticket,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    // Decode ticket
    let ticket_data =
        TicketData::decode(&ticket.ticket).map_err(|e| Status::invalid_argument(e.to_string()))?;

    tracing::warn!(
        "=== DoGet called! catalog={} schema={} table={} table_function={} server_catalog={}",
        ticket_data.catalog, ticket_data.schema, ticket_data.table, ticket_data.table_function,
        server.catalog_name()
    );

    // Validate catalog name
    if ticket_data.catalog != server.catalog_name() && !ticket_data.catalog.is_empty() {
        return Err(Status::invalid_argument(format!(
            "catalog name mismatch: expected {:?}, got {:?}",
            server.catalog_name(),
            ticket_data.catalog
        )));
    }

    // Look up schema
    let schema = server
        .catalog
        .schema(ctx, &ticket_data.schema)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", ticket_data.schema)))?;

    // Get scan options from ticket
    let scan_opts = ticket_data.to_scan_options();

    // Branch on table vs table function
    let record_stream = if !ticket_data.table_function.is_empty() {
        // Table function execution
        let functions = schema
            .table_functions(ctx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let func = functions
            .iter()
            .find(|f| f.name() == ticket_data.table_function)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "table function not found: {}.{}",
                    ticket_data.schema, ticket_data.table_function
                ))
            })?
            .clone();

        let params = if let Some(ref param_bytes) = ticket_data.function_params {
            crate::internal::msgpack::decode_function_params(param_bytes)
        } else {
            vec![]
        };

        func.execute(ctx, &params, &scan_opts)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
    } else {
        // Regular table scan
        let table = schema
            .table(ctx, &ticket_data.table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "table not found: {}.{}",
                    ticket_data.schema, ticket_data.table
                ))
            })?;

        table
            .scan(ctx, &scan_opts)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
    };

    // Build Flight data stream using FlightDataEncoderBuilder
    // FlightDataEncoderBuilder::build expects Stream<Item=Result<RecordBatch, FlightError>>
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .build(record_stream.map(|r| r.map_err(|e| {
            arrow_flight::error::FlightError::Arrow(
                arrow_schema::ArrowError::IoError(e.to_string(), std::io::Error::other(e.to_string()))
            )
        })))
        .map_err(|e| Status::internal(e.to_string()));

    Ok(Response::new(Box::pin(flight_data_stream)))
}
