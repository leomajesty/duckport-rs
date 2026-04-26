use super::server::Server;
use super::ticket::TicketData;
use crate::flight::context::RequestContext;
use arrow_flight::{FlightDescriptor, FlightInfo, FlightEndpoint, Ticket};
use tonic::{Response, Status};
use tracing::debug;

/// Handles GetFlightInfo RPC: returns schema metadata and ticket for table queries.
pub async fn handle_get_flight_info(
    server: &Server,
    ctx: &RequestContext,
    desc: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    use arrow_flight::flight_descriptor::DescriptorType;

    if desc.r#type != DescriptorType::Path as i32 {
        return Err(Status::invalid_argument("descriptor must be PATH type"));
    }

    if desc.path.len() == 1 {
        return Err(Status::invalid_argument(
            "path must contain at least 2 elements: [schema_name, table_name] or [catalog_name, table_name]",
        ));
    }

    let schema_name: &str;
    let table_name = desc.path.last().unwrap();

    // The DuckDB Airport extension sends paths in two possible formats:
    //  1. [schema_name, table_name]  — when DuckDB's catalog == server's catalog name
    //  2. [catalog_name, table_name] — when DuckDB attaches with a different alias
    //
    // We first try path[0] as a schema name (case 1). If that fails we fall back
    // to using the server's default schema (case 2), so tables are always reachable
    // regardless of how DuckDB's ATTACH alias relates to the catalog's internal name.
    let candidate_schema = &desc.path[0];
    debug!("GetFlightInfo: path[0]={}, path.last()={}, catalog_name={}", candidate_schema, table_name, server.catalog_name());
    let schema = match server.catalog.schema(ctx, candidate_schema).await {
        Ok(Some(s)) => {
            schema_name = candidate_schema;
            s
        }
        _ => {
            // path[0] is not a schema — use the default schema ("main")
            let default_schema = crate::catalog::DEFAULT_SCHEMA_NAME;
            schema_name = default_schema;
            server
                .catalog
                .schema(ctx, default_schema)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| {
                    Status::not_found(format!("default schema '{}' not found", default_schema))
                })?
        }
    };

    // Look up table
    let table = schema
        .table(ctx, table_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| {
            Status::not_found(format!("table not found: {}.{}", schema_name, table_name))
        })?;

    // Get Arrow schema
    let arrow_schema = table.arrow_schema(&[]);

    // Encode schema as IPC bytes
    let schema_bytes = encode_schema(&arrow_schema)?;

    // Generate ticket
    let ticket_bytes =
        TicketData::encode_table_ticket(server.catalog_name(), schema_name, table_name)
            .map_err(|e| Status::internal(e.to_string()))?;

    // Build FlightInfo
    let endpoint = FlightEndpoint {
        ticket: Some(Ticket { ticket: ticket_bytes.into() }),
        location: if server.address.is_empty() {
            vec![]
        } else {
            vec![arrow_flight::Location {
                uri: server.address.clone(),
            }]
        },
        ..Default::default()
    };

    let flight_info = FlightInfo {
        schema: schema_bytes.into(),
        flight_descriptor: Some(desc),
        endpoint: vec![endpoint],
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: vec![].into(),
    };

    Ok(Response::new(flight_info))
}

/// Encodes an Arrow schema to IPC bytes for FlightInfo.schema field.
///
/// Produces a fully-framed Arrow IPC message:
///   [0xFFFFFFFF continuation (4B)][message length (4B)][flatbuffer bytes][padding]
/// This is what the DuckDB Airport extension expects when calling GetSchema.
pub fn encode_schema(schema: &arrow_schema::Schema) -> Result<bytes::Bytes, Status> {
    let options = arrow_ipc::writer::IpcWriteOptions::default();
    let data_gen = arrow_ipc::writer::IpcDataGenerator::default();
    let mut dict_tracker = arrow_ipc::writer::DictionaryTracker::new(false);
    let encoded = data_gen
        .schema_to_bytes_with_dictionary_tracker(schema, &mut dict_tracker, &options);

    let mut buf: Vec<u8> = Vec::new();
    arrow_ipc::writer::write_message(&mut buf, encoded, &options)
        .map_err(|e| Status::internal(format!("failed to encode schema: {}", e)))?;
    Ok(buf.into())
}
