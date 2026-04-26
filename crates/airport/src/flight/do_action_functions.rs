use super::do_action::BoxStream;
use super::get_flight_info::encode_schema;
use super::server::Server;
use super::ticket::TicketData;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use prost::Message;
use tonic::{Response, Status};
use tracing::debug;

/// Handles table_function_flight_info action: returns FlightInfo for a table function call.
pub async fn handle_table_function_flight_info(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Use rmpv to decode the body: DuckDB sends `descriptor` as binary bytes
    // (protobuf-encoded FlightDescriptor), not a UTF-8 string.
    let val = if !action.body.is_empty() {
        msgpack::decode_value(&action.body)
            .map_err(|e| Status::invalid_argument(format!("serialization error: {}", e)))?
    } else {
        return Err(Status::invalid_argument("missing descriptor"));
    };

    let map = if let rmpv::Value::Map(m) = val {
        m
    } else {
        return Err(Status::invalid_argument("expected msgpack map body"));
    };

    let mut descriptor_bytes: Vec<u8> = vec![];
    let mut parameters_bytes: Option<Vec<u8>> = None;

    for (k, v) in &map {
        let key = if let rmpv::Value::String(s) = k {
            s.as_str().unwrap_or("")
        } else {
            continue;
        };
        match key {
            "descriptor" => match v {
                rmpv::Value::Binary(b) => descriptor_bytes = b.clone(),
                rmpv::Value::String(s) => descriptor_bytes = s.as_bytes().to_vec(),
                _ => {}
            },
            "parameters" => match v {
                rmpv::Value::Binary(b) => parameters_bytes = Some(b.clone()),
                rmpv::Value::String(s) => parameters_bytes = Some(s.as_bytes().to_vec()),
                _ => {}
            },
            _ => {}
        }
    }

    // Parse the FlightDescriptor from protobuf-encoded bytes.
    let desc = if !descriptor_bytes.is_empty() {
        FlightDescriptor::decode(bytes::Bytes::from(descriptor_bytes))
            .map_err(|e| Status::invalid_argument(format!("invalid descriptor: {}", e)))?
    } else {
        return Err(Status::invalid_argument("missing descriptor"));
    };

    if desc.path.len() != 2 {
        return Err(Status::invalid_argument("invalid descriptor path"));
    }

    let schema_name = &desc.path[0];
    let function_name = &desc.path[1];

    debug!(
        "table_function_flight_info: {}.{}",
        schema_name, function_name
    );

    // Look up schema
    let schema = server
        .catalog
        .schema(ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    // Decode function parameters
    let params = parameters_bytes
        .as_deref()
        .map(msgpack::decode_function_params)
        .unwrap_or_default();

    // Look up table function
    let functions = schema
        .table_functions(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let func = functions
        .iter()
        .find(|f| f.name() == function_name)
        .ok_or_else(|| {
            Status::not_found(format!(
                "table function not found: {}.{}",
                schema_name, function_name
            ))
        })?
        .clone();

    // Get output schema for these parameters
    let output_schema = func
        .schema_for_parameters(ctx, &params)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let schema_bytes = encode_schema(&output_schema)?;

    // Build ticket with function parameters
    let ticket_data = TicketData {
        catalog: server.catalog_name().to_string(),
        schema: schema_name.to_string(),
        table_function: function_name.to_string(),
        function_params: parameters_bytes,
        ..Default::default()
    };
    let ticket_bytes = ticket_data
        .encode()
        .map_err(|e| Status::internal(e.to_string()))?;

    let fi = FlightInfo {
        schema: schema_bytes,
        flight_descriptor: Some(desc),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket_bytes.into(),
            }),
            ..Default::default()
        }],
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: vec![].into(),
    };

    let mut fi_bytes = Vec::new();
    fi.encode(&mut fi_bytes)
        .map_err(|e| Status::internal(format!("failed to encode FlightInfo: {}", e)))?;

    Ok(Response::new(super::do_action::single_result(fi_bytes)))
}
