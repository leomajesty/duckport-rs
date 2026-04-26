use super::do_action::{single_result, BoxStream};
use super::get_flight_info::encode_schema;
use super::server::Server;
use super::ticket::TicketData;
use crate::catalog::Schema;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use crate::internal::serialize::{compress_catalog, wrap_compressed_content};
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use prost::Message;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tonic::{Response, Status};
use tracing::debug;

type ActionStream = BoxStream<arrow_flight::Result>;

/// Handles list_schemas action: returns catalog metadata with all schemas and tables.
pub async fn handle_list_schemas(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<ActionStream>, Status> {
    // Decode optional catalog_name parameter
    let catalog_name_param = if !action.body.is_empty() {
        #[derive(serde::Deserialize, Default)]
        struct Params {
            #[serde(default)]
            catalog_name: String,
        }
        msgpack::decode::<Params>(&action.body)
            .map(|p| p.catalog_name)
            .unwrap_or_default()
    } else {
        String::new()
    };

    if !catalog_name_param.is_empty() && catalog_name_param != server.catalog_name() {
        return Err(Status::invalid_argument(format!(
            "catalog name mismatch: expected {:?}, got {:?}",
            server.catalog_name(),
            catalog_name_param
        )));
    }

    debug!("handle_list_schemas: catalog={}, param={:?}", server.catalog_name(), catalog_name_param);

    // Get all schemas
    let schemas = server
        .catalog
        .schemas(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    // Collect schema data, then build the catalog msgpack manually so that the
    // binary `serialized` field is encoded as msgpack str (matching Go's
    // `string(bytes)` behavior) rather than as msgpack bin.
    let mut entries: Vec<SchemaEntry> = Vec::new();
    for schema in &schemas {
        let (serialized_bytes, sha256_hash) =
            serialize_schema_contents(server, ctx, schema.as_ref()).await?;
        entries.push(SchemaEntry {
            name: schema.name().to_string(),
            description: schema.comment().to_string(),
            // Always false: setting is_default=true causes the Airport extension
            // to register tables without a schema prefix (e.g. flight.users
            // instead of flight.main.users), breaking 3-part name resolution.
            // airport-go also sends false for its schemas.
            is_default: false,
            serialized_bytes,
            sha256: sha256_hash,
        });
    }

    // Encode the entire catalog root as msgpack using low-level rmp encoding.
    // This is necessary so we can write raw bytes as msgpack str without
    // UTF-8 validation (matching Go's `string(bytes)` semantics).
    let ver = server.catalog.version_info();
    let uncompressed = encode_catalog_root_msgpack(&entries, ver.version, ver.is_fixed)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Compress with ZStandard
    let compressed =
        compress_catalog(&uncompressed).map_err(|e| Status::internal(e.to_string()))?;

    // Wrap in AirportSerializedCompressedContent [length, data]
    let response_body =
        wrap_compressed_content(&uncompressed, &compressed)
            .map_err(|e| Status::internal(e.to_string()))?;

    debug!(
        "list_schemas: {} schemas, {} bytes uncompressed, {} bytes compressed",
        schemas.len(),
        uncompressed.len(),
        compressed.len()
    );

    Ok(Response::new(single_result(response_body)))
}

/// Serializes schema contents to msgpack-compressed Flight IPC format.
/// Returns (raw_serialized_bytes, sha256_hex).
pub async fn serialize_schema_contents(
    server: &Server,
    ctx: &RequestContext,
    schema: &dyn Schema,
) -> Result<(Vec<u8>, String), Status> {
    let tables = schema
        .tables(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let table_functions = schema
        .table_functions(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let scalar_functions = schema
        .scalar_functions(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let table_functions_in_out = schema
        .table_functions_in_out(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let table_refs = schema
        .table_refs(ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut flight_info_bytes_array: Vec<Vec<u8>> = Vec::new();

    // Serialize tables
    for table in &tables {
        let mut arrow_schema = table.arrow_schema(&[]);

        // Add can_produce_statistics metadata if table supports it
        if table.as_statistics().is_some() {
            arrow_schema = add_schema_metadata(&arrow_schema, "can_produce_statistics", "true");
        }

        let schema_bytes = encode_schema(&arrow_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let app_metadata = build_table_app_metadata(
            "table",
            schema.name(),
            server.catalog_name(),
            table.name(),
            table.comment(),
            None,
            None,
        );
        let app_metadata_bytes = msgpack::encode_rmpv(&app_metadata)
            .map_err(|e| Status::internal(e.to_string()))?;

        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![schema.name().to_string(), table.name().to_string()],
            ..Default::default()
        };

        let ticket_bytes =
            TicketData::encode_table_ticket(server.catalog_name(), schema.name(), table.name())
                .map_err(|e| Status::internal(e.to_string()))?;

        let fi = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                // Empty location tells DuckDB to reuse the current connection.
                // A non-empty location with a different hostname/IP than the
                // ATTACH URI would prevent table registration.  This matches
                // airport-go's behavior where list_schemas FlightInfo objects
                // always carry an empty location.
                location: vec![],
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: app_metadata_bytes.into(),
        };

        let fi_bytes = encode_flight_info(&fi)?;
        flight_info_bytes_array.push(fi_bytes);
    }

    // Serialize table refs
    for tref in &table_refs {
        let arrow_schema = tref.arrow_schema();
        let schema_bytes = encode_schema(&arrow_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let app_metadata = build_table_app_metadata(
            "table",
            schema.name(),
            server.catalog_name(),
            tref.name(),
            tref.comment(),
            None,
            None,
        );
        let app_metadata_bytes = msgpack::encode_rmpv(&app_metadata)
            .map_err(|e| Status::internal(e.to_string()))?;

        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![schema.name().to_string(), tref.name().to_string()],
            ..Default::default()
        };

        let ticket_bytes =
            TicketData::encode_table_ticket(server.catalog_name(), schema.name(), tref.name())
                .map_err(|e| Status::internal(e.to_string()))?;

        let fi = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                // Empty location tells DuckDB to reuse the current connection.
                // A non-empty location with a different hostname/IP than the
                // ATTACH URI would prevent table registration.  This matches
                // airport-go's behavior where list_schemas FlightInfo objects
                // always carry an empty location.
                location: vec![],
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: app_metadata_bytes.into(),
        };
        let fi_bytes = encode_flight_info(&fi)?;
        flight_info_bytes_array.push(fi_bytes);
    }

    // Serialize table functions
    for func in &table_functions {
        let sig = func.signature();

        // Build input schema from parameters
        let input_fields: Vec<Field> = sig
            .parameters
            .iter()
            .enumerate()
            .map(|(i, dt)| Field::new(format!("param{}", i + 1), dt.clone(), true))
            .collect();
        let input_schema = ArrowSchema::new(input_fields);
        let input_schema_bytes = encode_schema(&input_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let output_schema = ArrowSchema::new(Vec::<arrow_schema::Field>::new());
        let output_schema_bytes = encode_schema(&output_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let app_metadata = build_table_app_metadata(
            "table_function",
            schema.name(),
            server.catalog_name(),
            func.name(),
            func.comment(),
            Some(&input_schema_bytes),
            Some("table_function_flight_info"),
        );
        let app_metadata_bytes = msgpack::encode_rmpv(&app_metadata)
            .map_err(|e| Status::internal(e.to_string()))?;

        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![schema.name().to_string(), func.name().to_string()],
            ..Default::default()
        };

        let fi = FlightInfo {
            schema: output_schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: b"{}".to_vec().into(),
                }),
                // Empty location tells DuckDB to reuse the current connection.
                // A non-empty location with a different hostname/IP than the
                // ATTACH URI would prevent table registration.  This matches
                // airport-go's behavior where list_schemas FlightInfo objects
                // always carry an empty location.
                location: vec![],
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: app_metadata_bytes.into(),
        };
        let fi_bytes = encode_flight_info(&fi)?;
        flight_info_bytes_array.push(fi_bytes);
    }

    // Serialize scalar functions
    for func in &scalar_functions {
        let sig = func.signature();

        let input_fields: Vec<Field> = sig
            .parameters
            .iter()
            .enumerate()
            .map(|(i, dt)| Field::new(format!("param{}", i + 1), dt.clone(), true))
            .collect();
        let input_schema = ArrowSchema::new(input_fields);
        let input_schema_bytes = encode_schema(&input_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let return_type = sig.return_type.clone().unwrap_or(DataType::Null);
        let output_schema = ArrowSchema::new(vec![Field::new("result", return_type, true)]);
        let output_schema_bytes = encode_schema(&output_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let app_metadata = build_table_app_metadata(
            "scalar_function",
            schema.name(),
            server.catalog_name(),
            func.name(),
            func.comment(),
            Some(&input_schema_bytes),
            Some(func.name()),
        );
        let app_metadata_bytes = msgpack::encode_rmpv(&app_metadata)
            .map_err(|e| Status::internal(e.to_string()))?;

        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![schema.name().to_string(), func.name().to_string()],
            ..Default::default()
        };

        let fi = FlightInfo {
            schema: output_schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: b"{}".to_vec().into(),
                }),
                // Empty location tells DuckDB to reuse the current connection.
                // A non-empty location with a different hostname/IP than the
                // ATTACH URI would prevent table registration.  This matches
                // airport-go's behavior where list_schemas FlightInfo objects
                // always carry an empty location.
                location: vec![],
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: app_metadata_bytes.into(),
        };
        let fi_bytes = encode_flight_info(&fi)?;
        flight_info_bytes_array.push(fi_bytes);
    }

    // Serialize table functions in/out
    for func in &table_functions_in_out {
        let sig = func.signature();
        let param_count = sig.parameters.len();

        let input_fields: Vec<Field> = sig
            .parameters
            .iter()
            .enumerate()
            .map(|(i, dt)| {
                let name = format!("param{}", i + 1);
                // Last parameter is the table input - mark with is_table_type metadata
                if i == param_count.saturating_sub(1) {
                    let mut meta = std::collections::HashMap::new();
                    meta.insert("is_table_type".to_string(), "1".to_string());
                    Field::new(name, dt.clone(), true).with_metadata(meta)
                } else {
                    Field::new(name, dt.clone(), true)
                }
            })
            .collect();
        let input_schema = ArrowSchema::new(input_fields);
        let input_schema_bytes = encode_schema(&input_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let output_schema = ArrowSchema::new(Vec::<arrow_schema::Field>::new());
        let output_schema_bytes = encode_schema(&output_schema)
            .map_err(|e| Status::internal(e.to_string()))?;

        let app_metadata = build_table_app_metadata(
            "table_function",
            schema.name(),
            server.catalog_name(),
            func.name(),
            func.comment(),
            Some(&input_schema_bytes),
            Some("table_function_flight_info"),
        );
        let app_metadata_bytes = msgpack::encode_rmpv(&app_metadata)
            .map_err(|e| Status::internal(e.to_string()))?;

        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![schema.name().to_string(), func.name().to_string()],
            ..Default::default()
        };

        let fi = FlightInfo {
            schema: output_schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: b"{}".to_vec().into(),
                }),
                // Empty location tells DuckDB to reuse the current connection.
                // A non-empty location with a different hostname/IP than the
                // ATTACH URI would prevent table registration.  This matches
                // airport-go's behavior where list_schemas FlightInfo objects
                // always carry an empty location.
                location: vec![],
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: app_metadata_bytes.into(),
        };
        let fi_bytes = encode_flight_info(&fi)?;
        flight_info_bytes_array.push(fi_bytes);
    }

    // Serialize the array of FlightInfo bytes to msgpack.
    // Each element must be encoded as msgpack bin (not str): Go's airport-go
    // encodes [][]byte as msgpack bin, and the Airport C++ extension expects
    // bin-format bytes for FlightInfo entries.
    let uncompressed = {
        let mut buf: Vec<u8> = Vec::new();
        rmp::encode::write_array_len(&mut buf, flight_info_bytes_array.len() as u32)
            .map_err(|e| Status::internal(e.to_string()))?;
        for fi_bytes in &flight_info_bytes_array {
            rmp::encode::write_bin_len(&mut buf, fi_bytes.len() as u32)
                .map_err(|e| Status::internal(e.to_string()))?;
            buf.extend_from_slice(fi_bytes);
        }
        buf
    };

    // Compress
    let compressed =
        compress_catalog(&uncompressed).map_err(|e| Status::internal(e.to_string()))?;

    // Wrap in compressed content format [length, data]
    let serialized = wrap_compressed_content(&uncompressed, &compressed)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Calculate SHA256
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    let hash_hex = hex::encode(hasher.finalize());

    Ok((serialized, hash_hex))
}

/// Builds app_metadata for a Flight item, encoded as msgpack bytes.
///
/// `input_schema` is Arrow IPC binary data and MUST be encoded as msgpack bin
/// (not str), otherwise DuckDB's Airport extension gets a corrupt IPC stream.
/// We use rmpv::Value::Binary for that field and encode the whole map with
/// encode_rmpv so every byte is preserved exactly — matching airport-go's
/// behavior where []byte is encoded as msgpack bin.
fn build_table_app_metadata(
    item_type: &str,
    schema_name: &str,
    _catalog_name: &str,
    item_name: &str,
    comment: &str,
    input_schema: Option<&bytes::Bytes>,
    action_name: Option<&str>,
) -> rmpv::Value {
    use rmpv::Value as V;
    V::Map(vec![
        (V::String("type".into()),         V::String(item_type.into())),
        (V::String("schema".into()),       V::String(schema_name.into())),
        // catalog must be empty: the Airport extension validates it against ""
        (V::String("catalog".into()),      V::String("".into())),
        (V::String("name".into()),         V::String(item_name.into())),
        (V::String("comment".into()),      V::String(comment.into())),
        (V::String("input_schema".into()), match input_schema {
            // Arrow IPC bytes — keep as binary so msgpack encodes them as bin
            Some(b) => V::Binary(b.to_vec()),
            None => V::Nil,
        }),
        (V::String("action_name".into()), match action_name {
            Some(a) => V::String(a.into()),
            None => V::Nil,
        }),
        (V::String("description".into()), V::String(comment.into())),
        (V::String("extra_data".into()),  V::Nil),
    ])
}

/// Encodes a FlightInfo as protobuf bytes.
fn encode_flight_info(fi: &FlightInfo) -> Result<Vec<u8>, Status> {
    let mut buf = Vec::new();
    fi.encode(&mut buf)
        .map_err(|e| Status::internal(format!("failed to encode FlightInfo: {}", e)))?;
    Ok(buf)
}

/// Adds a key-value pair to an Arrow schema's metadata.
fn add_schema_metadata(schema: &ArrowSchema, key: &str, value: &str) -> Arc<ArrowSchema> {
    let mut meta = schema.metadata().clone();
    meta.insert(key.to_string(), value.to_string());
    Arc::new(ArrowSchema::new_with_metadata(
        schema.fields().to_vec(),
        meta,
    ))
}

/// Handles flight_info action: returns FlightInfo for time travel queries.
pub async fn handle_flight_info(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize, Default)]
    struct Request {
        #[serde(default)]
        descriptor: String,
        #[serde(default)]
        at_unit: String,
        #[serde(default)]
        at_value: String,
    }

    let request: Request = if !action.body.is_empty() {
        msgpack::decode(&action.body).unwrap_or_default()
    } else {
        Request::default()
    };

    // Parse the FlightDescriptor from protobuf bytes
    let desc = FlightDescriptor::decode(bytes::Bytes::from(request.descriptor.into_bytes()))
        .map_err(|e| Status::invalid_argument(format!("invalid descriptor: {}", e)))?;

    if desc.path.len() != 2 {
        return Err(Status::invalid_argument("invalid descriptor path"));
    }

    let schema_name = &desc.path[0];
    let table_name = &desc.path[1];

    let schema = server
        .catalog
        .schema(ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let table = schema
        .table(ctx, table_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("table not found: {}", table_name)))?;

    // Get schema (with time travel support if applicable)
    let arrow_schema = if !request.at_unit.is_empty() && !request.at_value.is_empty() {
        if let Some(dyn_table) = table.as_dynamic_schema() {
            let req = crate::catalog::SchemaRequest {
                time_point: Some(crate::catalog::TimePoint {
                    unit: request.at_unit.clone(),
                    value: request.at_value.clone(),
                }),
                ..Default::default()
            };
            dyn_table
                .schema_for_request(ctx, &req)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        } else {
            table.arrow_schema(&[])
        }
    } else {
        table.arrow_schema(&[])
    };

    let schema_bytes = encode_schema(&arrow_schema)?;

    // Build ticket with time point
    let ticket_data = TicketData {
        catalog: server.catalog_name().to_string(),
        schema: schema_name.to_string(),
        table: table_name.to_string(),
        time_point_unit: request.at_unit,
        time_point_value: request.at_value,
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

    let fi_bytes = encode_flight_info(&fi)?;
    Ok(Response::new(single_result(fi_bytes)))
}

///// Handles the `endpoints` action: returns serialized FlightEndpoints for a table.
///
/// The response is a msgpack array of strings, where each string is a
/// protobuf-serialized FlightEndpoint.  This matches airport-go's
/// `sendEndpointResponse`.
pub async fn handle_endpoints(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Use rmpv to decode: DuckDB sends `descriptor` and binary fields as msgpack bin,
    // which serde_msgpack cannot decode into Rust String.
    struct EndpointsDecodedRequest {
        descriptor_bytes: Vec<u8>,
        json_filters: Vec<u8>,
        table_function_parameters: Vec<u8>,
        at_unit: String,
        at_value: String,
    }

    let request = {
        let mut descriptor_bytes: Vec<u8> = vec![];
        let mut json_filters: Vec<u8> = vec![];
        let mut table_function_parameters: Vec<u8> = vec![];
        let mut at_unit = String::new();
        let mut at_value = String::new();

        if !action.body.is_empty() {
            if let Ok(rmpv::Value::Map(top)) = msgpack::decode_value(&action.body) {
                for (k, v) in &top {
                    let key = match k {
                        rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                        _ => continue,
                    };
                    match key.as_str() {
                        "descriptor" => match v {
                            rmpv::Value::Binary(b) => descriptor_bytes = b.clone(),
                            rmpv::Value::String(s) => descriptor_bytes = s.as_bytes().to_vec(),
                            _ => {}
                        },
                        "parameters" => {
                            if let rmpv::Value::Map(params) = v {
                                for (pk, pv) in params {
                                    let pkey = match pk {
                                        rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                                        _ => continue,
                                    };
                                    match pkey.as_str() {
                                        "json_filters" => match pv {
                                            rmpv::Value::Binary(b) => json_filters = b.clone(),
                                            rmpv::Value::String(s) => json_filters = s.as_bytes().to_vec(),
                                            _ => {}
                                        },
                                        "table_function_parameters" => match pv {
                                            rmpv::Value::Binary(b) => table_function_parameters = b.clone(),
                                            rmpv::Value::String(s) => table_function_parameters = s.as_bytes().to_vec(),
                                            _ => {}
                                        },
                                        "at_unit" => {
                                            if let rmpv::Value::String(s) = pv {
                                                at_unit = s.as_str().unwrap_or("").to_string();
                                            }
                                        }
                                        "at_value" => {
                                            if let rmpv::Value::String(s) = pv {
                                                at_value = s.as_str().unwrap_or("").to_string();
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        EndpointsDecodedRequest { descriptor_bytes, json_filters, table_function_parameters, at_unit, at_value }
    };

    // Decode the FlightDescriptor from protobuf-encoded binary bytes.
    let desc = if !request.descriptor_bytes.is_empty() {
        FlightDescriptor::decode(bytes::Bytes::from(request.descriptor_bytes))
            .map_err(|e| Status::invalid_argument(format!("invalid descriptor: {}", e)))?
    } else {
        return Err(Status::invalid_argument("missing descriptor in endpoints request"));
    };

    if desc.path.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "descriptor path must have exactly 2 elements: [schema, table], got {:?}",
            desc.path
        )));
    }

    let schema_name = &desc.path[0];
    let table_name = &desc.path[1];

    // Look up schema to verify it exists
    let schema_obj = server
        .catalog
        .schema(ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    // Check if the requested table is a TableRef; if so, return data:// URI endpoints.
    {
        let table_refs = schema_obj
            .table_refs(ctx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(tref) = table_refs.iter().find(|r| r.name() == table_name) {
            let fc_req = crate::catalog::tableref::FunctionCallRequest::default();
            let calls = tref
                .function_calls(ctx, &fc_req)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            if calls.is_empty() {
                return Err(Status::internal(format!(
                    "table ref {}.{} returned no function calls",
                    schema_name, table_name
                )));
            }
            return encode_tableref_endpoints(&calls);
        }
    }

    // If table_function_parameters is present, build a table function ticket.
    let (ticket_bytes, table_name_for_endpoint) = if !request.table_function_parameters.is_empty() {
        let ticket_data = TicketData {
            catalog: server.catalog_name().to_string(),
            schema: schema_name.to_string(),
            table_function: table_name.to_string(),
            function_params: Some(request.table_function_parameters),
            ..Default::default()
        };
        let tb = ticket_data.encode().map_err(|e| Status::internal(e.to_string()))?;
        (tb, table_name.clone())
    } else {
        // Build ticket - bake in filters and time travel so DoGet can use them
        let ticket_data = TicketData {
            catalog: server.catalog_name().to_string(),
            schema: schema_name.to_string(),
            table: table_name.to_string(),
            filters: if request.json_filters.is_empty() {
                None
            } else {
                Some(request.json_filters)
            },
            time_point_unit: if request.at_unit.is_empty() {
                String::new()
            } else {
                normalize_time_unit(&request.at_unit).to_string()
            },
            time_point_value: request.at_value,
            ..Default::default()
        };
        let tb = ticket_data.encode().map_err(|e| Status::internal(e.to_string()))?;
        (tb, table_name.clone())
    };
    let _ = table_name_for_endpoint; // used only for clarity

    // Build FlightEndpoint with location set so DuckDB knows where to connect
    let endpoint = arrow_flight::FlightEndpoint {
        ticket: Some(Ticket {
            ticket: ticket_bytes.into(),
        }),
        location: if server.address.is_empty() {
            vec![]
        } else {
            vec![arrow_flight::Location {
                uri: server.address.clone(),
            }]
        },
        ..Default::default()
    };

    // Serialize FlightEndpoint as protobuf bytes
    let endpoint_bytes = {
        let mut buf = Vec::new();
        endpoint
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("failed to encode FlightEndpoint: {}", e)))?;
        buf
    };

    // Return as msgpack array of one str element (proto bytes written as msgpack str).
    // This matches airport-go's sendEndpointResponse: []string{string(endpointBytes)}.
    let mut body = Vec::new();
    rmp::encode::write_array_len(&mut body, 1)
        .map_err(|e| Status::internal(e.to_string()))?;
    rmp::encode::write_str_len(&mut body, endpoint_bytes.len() as u32)
        .map_err(|e| Status::internal(e.to_string()))?;
    body.extend_from_slice(&endpoint_bytes);

    Ok(Response::new(single_result(body)))
}

/// Normalizes DuckDB time-travel unit strings to lowercase.
fn normalize_time_unit(unit: &str) -> &str {
    match unit {
        "TIMESTAMP" => "timestamp",
        "TIMESTAMP_NS" => "timestamp_ns",
        "VERSION" => "version",
        other => other,
    }
}

/// Encodes a list of FunctionCalls as data:// URI FlightEndpoints and returns them
/// as an Airport-style msgpack array response.
///
/// Each function call becomes a `data:application/x-msgpack-duckdb-function-call;base64,...`
/// URI in its own FlightEndpoint.  DuckDB decodes these locally and executes the
/// referenced function (e.g. `generate_series`) without making a DoGet to the server.
fn encode_tableref_endpoints(
    calls: &[crate::catalog::tableref::FunctionCall],
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    use base64::Engine as _;
    use crate::catalog::tableref::{FunctionCall, FunctionCallValue};

    let mut endpoint_bufs: Vec<Vec<u8>> = Vec::with_capacity(calls.len());

    for call in calls {
        // Build Arrow IPC bytes from the function args.
        let ipc_bytes = build_ipc_from_function_call(call)?;

        // Encode as msgpack map {"function_name": str, "data": bin}.
        let mut mp = Vec::new();
        rmp::encode::write_map_len(&mut mp, 2)
            .map_err(|e| Status::internal(e.to_string()))?;
        rmp::encode::write_str(&mut mp, "function_name")
            .map_err(|e| Status::internal(e.to_string()))?;
        rmp::encode::write_str(&mut mp, &call.function_name)
            .map_err(|e| Status::internal(e.to_string()))?;
        rmp::encode::write_str(&mut mp, "data")
            .map_err(|e| Status::internal(e.to_string()))?;
        rmp::encode::write_bin(&mut mp, &ipc_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Build the data:// URI.
        let b64 = base64::engine::general_purpose::STANDARD.encode(&mp);
        let uri = format!("data:application/x-msgpack-duckdb-function-call;base64,{}", b64);

        // Create FlightEndpoint with empty ticket and data:// URI location.
        let endpoint = arrow_flight::FlightEndpoint {
            ticket: Some(Ticket { ticket: b"{}".to_vec().into() }),
            location: vec![arrow_flight::Location { uri }],
            ..Default::default()
        };
        let mut buf = Vec::new();
        endpoint
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("failed to encode FlightEndpoint: {}", e)))?;
        endpoint_bufs.push(buf);
    }

    // Return msgpack array of str elements (protobuf bytes as msgpack str).
    // This matches airport-go's sendEndpointResponse: []string{string(endpointBytes)}.
    let mut body = Vec::new();
    rmp::encode::write_array_len(&mut body, endpoint_bufs.len() as u32)
        .map_err(|e| Status::internal(e.to_string()))?;
    for ep_bytes in &endpoint_bufs {
        rmp::encode::write_str_len(&mut body, ep_bytes.len() as u32)
            .map_err(|e| Status::internal(e.to_string()))?;
        body.extend_from_slice(ep_bytes);
    }

    Ok(Response::new(single_result(body)))
}

/// Builds an Arrow IPC stream from FunctionCall args.
///
/// Positional args become fields "arg_0", "arg_1", …; named args use their name.
/// A single-row RecordBatch is produced.
fn build_ipc_from_function_call(
    call: &crate::catalog::tableref::FunctionCall,
) -> Result<Vec<u8>, Status> {
    use arrow_array::{
        array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array},
        RecordBatch,
    };
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use crate::catalog::tableref::{FunctionCallArg, FunctionCallValue};

    if call.args.is_empty() {
        // Empty schema, empty record.
        let schema = Arc::new(ArrowSchema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        return ipc_write(&schema, &batch);
    }

    let mut fields: Vec<Field> = Vec::with_capacity(call.args.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(call.args.len());
    let mut positional = 0usize;

    for arg in &call.args {
        let name = if arg.name.is_empty() {
            let n = format!("arg_{}", positional);
            positional += 1;
            n
        } else {
            arg.name.clone()
        };

        let (dt, arr): (DataType, ArrayRef) = match &arg.value {
            FunctionCallValue::Int64(v) => {
                let dt = arg.data_type.clone().unwrap_or(DataType::Int64);
                (dt, Arc::new(Int64Array::from(vec![*v])))
            }
            FunctionCallValue::UInt64(v) => {
                let dt = arg.data_type.clone().unwrap_or(DataType::UInt64);
                (dt, Arc::new(UInt64Array::from(vec![*v])))
            }
            FunctionCallValue::Float64(v) => {
                let dt = arg.data_type.clone().unwrap_or(DataType::Float64);
                (dt, Arc::new(Float64Array::from(vec![*v])))
            }
            FunctionCallValue::Bool(v) => {
                (DataType::Boolean, Arc::new(BooleanArray::from(vec![*v])))
            }
            FunctionCallValue::String(s) => {
                (DataType::Utf8, Arc::new(StringArray::from(vec![s.as_str()])))
            }
            FunctionCallValue::Bytes(b) => {
                use arrow_array::array::BinaryArray;
                (DataType::Binary, Arc::new(BinaryArray::from_vec(vec![b.as_slice()])))
            }
            FunctionCallValue::Null => {
                let dt = arg.data_type.clone().unwrap_or(DataType::Null);
                use arrow_array::array::NullArray;
                (dt, Arc::new(NullArray::new(1)))
            }
            // List/Map not supported for function call args.
            _ => {
                return Err(Status::internal(format!(
                    "unsupported FunctionCallValue variant for arg {:?}",
                    arg.name
                )));
            }
        };

        fields.push(Field::new(&name, dt, true));
        arrays.push(arr);
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| Status::internal(format!("failed to build RecordBatch: {}", e)))?;

    ipc_write(&schema, &batch)
}

/// Serializes a RecordBatch to an Arrow IPC stream.
fn ipc_write(schema: &Arc<ArrowSchema>, batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, Status> {
    use arrow_ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| Status::internal(format!("IPC writer init failed: {}", e)))?;
    writer
        .write(batch)
        .map_err(|e| Status::internal(format!("IPC write failed: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Status::internal(format!("IPC writer finish failed: {}", e)))?;
    drop(writer);
    Ok(buf)
}

/// Handles catalog_version action.
pub async fn handle_catalog_version(
    server: &Server,
    _ctx: &RequestContext,
    _action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    let ver = server.catalog.version_info();
    let version_info: serde_json::Map<String, JsonValue> = [
        ("catalog_version".to_string(), JsonValue::Number(ver.version.into())),
        ("is_fixed".to_string(), JsonValue::Bool(ver.is_fixed)),
    ]
    .into_iter()
    .collect();

    let body = msgpack::encode(&version_info)
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(Response::new(single_result(body)))
}

// ── Low-level msgpack helpers ──────────────────────────────────────────────────

/// Schema entry used by `encode_catalog_root_msgpack`.
struct SchemaEntry {
    name: String,
    description: String,
    is_default: bool,
    serialized_bytes: Vec<u8>,
    sha256: String,
}

/// Builds the catalog-root msgpack payload manually so that the `serialized`
/// field (which contains arbitrary binary) is written as msgpack **str** format
/// (raw bytes, no UTF-8 validation).  This mirrors Go's `string(bytes)`
/// behaviour: Go strings are byte sequences, and Go's msgpack library encodes
/// them as str.  Using `rmp_serde` / `serde_json::Value::String` would corrupt
/// any byte that is not valid UTF-8.
fn encode_catalog_root_msgpack(entries: &[SchemaEntry], catalog_version: u64, is_fixed: bool) -> Result<Vec<u8>, crate::error::AirportError> {
    let mut buf: Vec<u8> = Vec::new();

    // catalog_root = { "contents": …, "schemas": […], "version_info": … }
    mp_map_len(&mut buf, 3)?;

    // ── "contents" (empty placeholder) ────────────────────────────────────
    mp_str(&mut buf, "contents")?;
    mp_map_len(&mut buf, 3)?;
    mp_str(&mut buf, "sha256")?;
    mp_str(&mut buf, "0000000000000000000000000000000000000000000000000000000000000000")?;
    mp_str(&mut buf, "url")?;
    mp_nil(&mut buf)?;
    mp_str(&mut buf, "serialized")?;
    mp_nil(&mut buf)?;

    // ── "schemas" ─────────────────────────────────────────────────────────
    mp_str(&mut buf, "schemas")?;
    mp_array_len(&mut buf, entries.len() as u32)?;
    for e in entries {
        // schema_obj = { "name", "description", "tags", "contents", "is_default" }
        mp_map_len(&mut buf, 5)?;

        mp_str(&mut buf, "name")?;
        mp_str(&mut buf, &e.name)?;

        mp_str(&mut buf, "description")?;
        mp_str(&mut buf, &e.description)?;

        mp_str(&mut buf, "tags")?;
        mp_map_len(&mut buf, 0)?; // empty map

        mp_str(&mut buf, "contents")?;
        // schema_contents = { "sha256": …, "url": null, "serialized": <raw bytes as str> }
        mp_map_len(&mut buf, 3)?;
        mp_str(&mut buf, "sha256")?;
        mp_str(&mut buf, &e.sha256)?;
        mp_str(&mut buf, "url")?;
        mp_nil(&mut buf)?;
        mp_str(&mut buf, "serialized")?;
        // Write raw bytes as msgpack str (same as Go's string(bytes)).
        mp_str_raw(&mut buf, &e.serialized_bytes)?;

        mp_str(&mut buf, "is_default")?;
        mp_bool(&mut buf, e.is_default)?;
    }

    // ── "version_info" ────────────────────────────────────────────────────
    mp_str(&mut buf, "version_info")?;
    mp_map_len(&mut buf, 2)?;
    mp_str(&mut buf, "catalog_version")?;
    mp_u64(&mut buf, catalog_version)?;
    mp_str(&mut buf, "is_fixed")?;
    mp_bool(&mut buf, is_fixed)?;

    Ok(buf)
}

// ── Thin wrappers that map rmp errors to AirportError ────────────────────────

type MpResult = Result<(), crate::error::AirportError>;

#[inline]
fn mp_map_len(buf: &mut Vec<u8>, len: u32) -> MpResult {
    rmp::encode::write_map_len(buf, len)
        .map(|_| ())
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))
}

#[inline]
fn mp_array_len(buf: &mut Vec<u8>, len: u32) -> MpResult {
    rmp::encode::write_array_len(buf, len)
        .map(|_| ())
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))
}

#[inline]
fn mp_str(buf: &mut Vec<u8>, s: &str) -> MpResult {
    rmp::encode::write_str(buf, s)
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))
}

/// Write raw bytes as a msgpack **str** (not bin), matching Go's string encoding.
#[inline]
fn mp_str_raw(buf: &mut Vec<u8>, bytes: &[u8]) -> MpResult {
    rmp::encode::write_str_len(buf, bytes.len() as u32)
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))?;
    buf.extend_from_slice(bytes);
    Ok(())
}

#[inline]
fn mp_nil(buf: &mut Vec<u8>) -> MpResult {
    rmp::encode::write_nil(buf)
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))
}

#[inline]
fn mp_bool(buf: &mut Vec<u8>, v: bool) -> MpResult {
    rmp::encode::write_bool(buf, v)
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))
}

#[inline]
fn mp_u64(buf: &mut Vec<u8>, v: u64) -> MpResult {
    rmp::encode::write_u64(buf, v)
        .map_err(|e| crate::error::AirportError::Serialization(e.to_string()))
}
