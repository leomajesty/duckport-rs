use super::do_action::{empty_result, single_result, BoxStream};
use super::do_action_metadata::serialize_schema_contents;
use super::get_flight_info::encode_schema;
use super::server::Server;
use super::ticket::TicketData;
use crate::catalog::types::*;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tonic::{Response, Status};

pub async fn handle_create_schema(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Use rmpv to tolerate DuckDB sending comment as nil instead of a string.
    let val = msgpack::decode_value(&action.body)
        .map_err(|e| Status::invalid_argument(format!("serialization error: {}", e)))?;
    let map = if let rmpv::Value::Map(m) = val {
        m
    } else {
        return Err(Status::invalid_argument("expected msgpack map body"));
    };

    let mut schema_name_opt: Option<String> = None;
    let mut comment = String::new();
    let mut ignore_if_exists = false;

    for (k, v) in &map {
        let key = if let rmpv::Value::String(s) = k {
            s.as_str().unwrap_or("")
        } else {
            continue;
        };
        match key {
            "schema" => {
                if let rmpv::Value::String(s) = v {
                    schema_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "comment" => {
                // DuckDB may send nil — treat nil as empty string.
                if let rmpv::Value::String(s) = v {
                    comment = s.as_str().unwrap_or("").to_string();
                }
            }
            "ignore_if_exists" => {
                if let rmpv::Value::Boolean(b) = v {
                    ignore_if_exists = *b;
                }
            }
            _ => {}
        }
    }

    let schema_name = schema_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'schema'"))?;

    let dyn_catalog = server
        .catalog
        .as_dynamic()
        .ok_or_else(|| Status::unimplemented("catalog does not support DDL operations"))?;

    let opts = CreateSchemaOptions {
        on_conflict: if ignore_if_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        },
    };

    dyn_catalog
        .create_schema(ctx, &schema_name, &comment, &opts)
        .await
        .map_err(|e| e.to_status())?;

    // DuckDB expects a msgpack response with {"sha256", "url", "serialized"}
    // containing the schema contents (tables/functions) in compressed format.
    let schema_obj = server
        .catalog
        .schema(ctx, &schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found after creation: {}", schema_name)))?;

    let (serialized_bytes, sha256_hash) = serialize_schema_contents(server, ctx, schema_obj.as_ref())
        .await?;

    let response_body = build_schema_contents_response(&sha256_hash, &serialized_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(Response::new(single_result(response_body)))
}

pub async fn handle_drop_schema(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        #[serde(default)]
        name: String, // schema name (primary field per airport-go protocol)
        #[serde(default)]
        schema_name: String, // fallback field
        #[serde(default)]
        ignore_not_found: bool,
        #[serde(default)]
        cascade: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    // Use `name` preferentially, fall back to `schema_name`
    let schema_name = if !params.name.is_empty() {
        params.name
    } else {
        params.schema_name
    };

    let dyn_catalog = server
        .catalog
        .as_dynamic()
        .ok_or_else(|| Status::unimplemented("catalog does not support DDL operations"))?;

    let opts = DropSchemaOptions {
        ignore_not_found: params.ignore_not_found,
        cascade: params.cascade,
    };

    dyn_catalog
        .drop_schema(ctx, &schema_name, &opts)
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_create_table(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Decode body with rmpv so that `arrow_schema` bytes are accepted as
    // either msgpack bin *or* msgpack str (the DuckDB Airport C++ extension
    // sends them as str with raw IPC bytes, which rmp_serde would reject as
    // invalid UTF-8 — 0xFF is the Arrow IPC continuation token).
    let val = msgpack::decode_value(&action.body)
        .map_err(|e| Status::invalid_argument(format!("serialization error: {}", e)))?;
    let map = if let rmpv::Value::Map(m) = val {
        m
    } else {
        return Err(Status::invalid_argument("expected msgpack map body"));
    };

    // DuckDB Airport uses schema_name / table_name / on_conflict (str)
    let mut schema_name_opt: Option<String> = None;
    let mut table_name_opt: Option<String> = None;
    let mut arrow_schema_bytes: Vec<u8> = vec![];
    let mut on_conflict = String::new();

    for (k, v) in &map {
        let key = if let rmpv::Value::String(s) = k {
            s.as_str().unwrap_or("")
        } else {
            continue;
        };
        match key {
            "schema_name" => {
                if let rmpv::Value::String(s) = v {
                    schema_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "table_name" => {
                if let rmpv::Value::String(s) = v {
                    table_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "arrow_schema" => match v {
                rmpv::Value::Binary(b) => arrow_schema_bytes = b.clone(),
                // DuckDB Airport extension sends IPC bytes as msgpack str
                rmpv::Value::String(s) => arrow_schema_bytes = s.as_bytes().to_vec(),
                _ => {}
            },
            "on_conflict" => {
                if let rmpv::Value::String(s) = v {
                    on_conflict = s.as_str().unwrap_or("error").to_string();
                }
            }
            _ => {}
        }
    }
    let schema_name = schema_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'schema_name'"))?;
    let table_name = table_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'table_name'"))?;
    let ignore_if_exists = on_conflict == "ignore";

    // Decode Arrow schema from IPC message bytes
    let arrow_schema = decode_arrow_schema(&arrow_schema_bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid arrow schema: {}", e)))?;

    let schema_obj = server
        .catalog
        .schema(ctx, &schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let dyn_schema = schema_obj
        .as_dynamic_schema()
        .ok_or_else(|| Status::unimplemented("schema does not support DDL table operations"))?;

    let opts = CreateTableOptions {
        on_conflict: if ignore_if_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        },
        comment: None,
    };

    let table = dyn_schema
        .create_table(ctx, &table_name, arrow_schema, &opts)
        .await
        .map_err(|e| e.to_status())?;

    // Return a FlightInfo for the newly created table, matching airport-go behavior.
    let fi_bytes = build_table_flight_info_bytes(
        server,
        &schema_name,
        &table_name,
        table.arrow_schema(&[]),
    )?;
    Ok(Response::new(single_result(fi_bytes)))
}

pub async fn handle_drop_table(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema_name: String,
        name: String, // table name
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = server
        .catalog
        .schema(ctx, &params.schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", params.schema_name)))?;

    let table = schema_obj
        .table(ctx, &params.name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("table not found: {}.{}", params.schema_name, params.name)))?;

    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = DropTableOptions {
        ignore_not_found: params.ignore_not_found,
    };

    dyn_table.drop(ctx, &opts).await.map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_add_column(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Use rmpv to handle column_schema bytes (may be sent as msgpack str with raw IPC bytes)
    let val = msgpack::decode_value(&action.body)
        .map_err(|e| Status::invalid_argument(format!("serialization error: {}", e)))?;
    let map = if let rmpv::Value::Map(m) = val {
        m
    } else {
        return Err(Status::invalid_argument("expected msgpack map body"));
    };

    let mut schema_name_opt: Option<String> = None;
    let mut table_name_opt: Option<String> = None;
    let mut column_schema_bytes: Vec<u8> = vec![];
    let mut ignore_if_exists = false;

    for (k, v) in &map {
        let key = if let rmpv::Value::String(s) = k {
            s.as_str().unwrap_or("")
        } else {
            continue;
        };
        match key {
            "schema" => {
                if let rmpv::Value::String(s) = v {
                    schema_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "name" => {
                if let rmpv::Value::String(s) = v {
                    table_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "column_schema" => match v {
                rmpv::Value::Binary(b) => column_schema_bytes = b.clone(),
                rmpv::Value::String(s) => column_schema_bytes = s.as_bytes().to_vec(),
                _ => {}
            },
            "if_column_not_exists" => {
                if let rmpv::Value::Boolean(b) = v {
                    ignore_if_exists = *b;
                }
            }
            _ => {}
        }
    }

    let schema_name = schema_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'schema'"))?;
    let table_name = table_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'name'"))?;

    let schema_obj = get_schema(server, ctx, &schema_name).await?;
    let table = get_table(&schema_obj, ctx, &schema_name, &table_name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    // Decode the field from column_schema IPC bytes
    let field = if column_schema_bytes.is_empty() {
        return Err(Status::invalid_argument("column_schema is required"));
    } else {
        let arrow_schema = decode_arrow_schema(&column_schema_bytes)
            .map_err(|e| Status::invalid_argument(format!("invalid column_schema: {}", e)))?;
        arrow_schema
            .fields()
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("column_schema must have at least one field"))?
    };

    let opts = AddColumnOptions {
        ignore_if_exists,
    };

    dyn_table
        .add_column(ctx, (*field).clone(), &opts)
        .await
        .map_err(|e| e.to_status())?;

    // Re-fetch the table to get the updated schema and return FlightInfo.
    let updated_table = get_table(&schema_obj, ctx, &schema_name, &table_name).await?;
    let fi_bytes = build_table_flight_info_bytes(
        server,
        &schema_name,
        &table_name,
        updated_table.arrow_schema(&[]),
    )?;
    Ok(Response::new(single_result(fi_bytes)))
}

pub async fn handle_remove_column(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        removed_column: String,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = RemoveColumnOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .remove_column(ctx, &params.removed_column, &opts)
        .await
        .map_err(|e| e.to_status())?;

    let updated_table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let fi_bytes = build_table_flight_info_bytes(
        server,
        &params.schema,
        &params.name,
        updated_table.arrow_schema(&[]),
    )?;
    Ok(Response::new(single_result(fi_bytes)))
}

pub async fn handle_rename_column(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        old_name: String,
        new_name: String,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = RenameColumnOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .rename_column(ctx, &params.old_name, &params.new_name, &opts)
        .await
        .map_err(|e| e.to_status())?;

    let updated_table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let fi_bytes = build_table_flight_info_bytes(
        server,
        &params.schema,
        &params.name,
        updated_table.arrow_schema(&[]),
    )?;
    Ok(Response::new(single_result(fi_bytes)))
}

pub async fn handle_rename_table(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // current table name
        new_table_name: String,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = RenameTableOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .rename_table(ctx, &params.new_table_name, &opts)
        .await
        .map_err(|e| e.to_status())?;

    // Return FlightInfo for the table under its new name.
    let updated_table = get_table(&schema_obj, ctx, &params.schema, &params.new_table_name).await?;
    let fi_bytes = build_table_flight_info_bytes(
        server,
        &params.schema,
        &params.new_table_name,
        updated_table.arrow_schema(&[]),
    )?;
    Ok(Response::new(single_result(fi_bytes)))
}

pub async fn handle_change_column_type(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Use rmpv to handle column_schema IPC bytes
    let val = msgpack::decode_value(&action.body)
        .map_err(|e| Status::invalid_argument(format!("serialization error: {}", e)))?;
    let map = if let rmpv::Value::Map(m) = val {
        m
    } else {
        return Err(Status::invalid_argument("expected msgpack map body"));
    };

    let mut schema_name_opt: Option<String> = None;
    let mut table_name_opt: Option<String> = None;
    let mut column_schema_bytes: Vec<u8> = vec![];
    let mut ignore_not_found = false;

    for (k, v) in &map {
        let key = if let rmpv::Value::String(s) = k {
            s.as_str().unwrap_or("")
        } else {
            continue;
        };
        match key {
            "schema" => {
                if let rmpv::Value::String(s) = v {
                    schema_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "name" => {
                if let rmpv::Value::String(s) = v {
                    table_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "column_schema" => match v {
                rmpv::Value::Binary(b) => column_schema_bytes = b.clone(),
                rmpv::Value::String(s) => column_schema_bytes = s.as_bytes().to_vec(),
                _ => {}
            },
            "ignore_not_found" => {
                if let rmpv::Value::Boolean(b) = v {
                    ignore_not_found = *b;
                }
            }
            _ => {}
        }
    }

    let schema_name = schema_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'schema'"))?;
    let table_name = table_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'name'"))?;

    let schema_obj = get_schema(server, ctx, &schema_name).await?;
    let table = get_table(&schema_obj, ctx, &schema_name, &table_name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    // Decode the column field from column_schema IPC bytes to get column_name and new type
    let field = if !column_schema_bytes.is_empty() {
        let arrow_schema = decode_arrow_schema(&column_schema_bytes)
            .map_err(|e| Status::invalid_argument(format!("invalid column_schema: {}", e)))?;
        arrow_schema
            .fields()
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("column_schema must have at least one field"))?
    } else {
        return Err(Status::invalid_argument("column_schema is required"));
    };

    let opts = ChangeColumnTypeOptions {
        ignore_not_found,
    };
    dyn_table
        .change_column_type(ctx, field.name(), field.data_type().clone(), &opts)
        .await
        .map_err(|e| e.to_status())?;

    let updated_table = get_table(&schema_obj, ctx, &schema_name, &table_name).await?;
    let fi_bytes = build_table_flight_info_bytes(
        server,
        &schema_name,
        &table_name,
        updated_table.arrow_schema(&[]),
    )?;
    Ok(Response::new(single_result(fi_bytes)))
}

pub async fn handle_set_not_null(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        column_name: String,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = SetNotNullOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .set_not_null(ctx, &params.column_name, &opts)
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_drop_not_null(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        column_name: String,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = DropNotNullOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .drop_not_null(ctx, &params.column_name, &opts)
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_set_default(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        column_name: String,
        #[serde(default)]
        expression: Option<String>, // default value expression
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = SetDefaultOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .set_default(
            ctx,
            &params.column_name,
            params.expression.as_deref(),
            &opts,
        )
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_add_field(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    // Use rmpv to handle column_schema IPC bytes
    let val = msgpack::decode_value(&action.body)
        .map_err(|e| Status::invalid_argument(format!("serialization error: {}", e)))?;
    let map = if let rmpv::Value::Map(m) = val {
        m
    } else {
        return Err(Status::invalid_argument("expected msgpack map body"));
    };

    let mut schema_name_opt: Option<String> = None;
    let mut table_name_opt: Option<String> = None;
    let mut column_schema_bytes: Vec<u8> = vec![];
    let mut column_path: Vec<String> = vec![];
    let mut ignore_if_exists = false;

    for (k, v) in &map {
        let key = if let rmpv::Value::String(s) = k {
            s.as_str().unwrap_or("")
        } else {
            continue;
        };
        match key {
            "schema" => {
                if let rmpv::Value::String(s) = v {
                    schema_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "name" => {
                if let rmpv::Value::String(s) = v {
                    table_name_opt = s.as_str().map(|s| s.to_string());
                }
            }
            "column_schema" => match v {
                rmpv::Value::Binary(b) => column_schema_bytes = b.clone(),
                rmpv::Value::String(s) => column_schema_bytes = s.as_bytes().to_vec(),
                _ => {}
            },
            "column_path" => {
                if let rmpv::Value::Array(arr) = v {
                    column_path = arr.iter().filter_map(|x| {
                        if let rmpv::Value::String(s) = x { s.as_str().map(|s| s.to_string()) } else { None }
                    }).collect();
                }
            }
            "if_field_not_exists" => {
                if let rmpv::Value::Boolean(b) = v {
                    ignore_if_exists = *b;
                }
            }
            _ => {}
        }
    }

    let schema_name = schema_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'schema'"))?;
    let table_name = table_name_opt
        .ok_or_else(|| Status::invalid_argument("missing 'name'"))?;

    let schema_obj = get_schema(server, ctx, &schema_name).await?;
    let table = get_table(&schema_obj, ctx, &schema_name, &table_name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    // The column_path identifies which column to add a field to
    let column_name = column_path.first()
        .ok_or_else(|| Status::invalid_argument("column_path is required"))?;

    let field = if !column_schema_bytes.is_empty() {
        let arrow_schema = decode_arrow_schema(&column_schema_bytes)
            .map_err(|e| Status::invalid_argument(format!("invalid column_schema: {}", e)))?;
        arrow_schema
            .fields()
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("column_schema must have at least one field"))?
    } else {
        return Err(Status::invalid_argument("column_schema is required"));
    };

    let opts = AddFieldOptions {
        ignore_if_exists,
    };
    dyn_table
        .add_field(ctx, column_name, (*field).clone(), &opts)
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_rename_field(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        column_path: Vec<String>,
        new_name: String,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let column_name = params.column_path.first()
        .ok_or_else(|| Status::invalid_argument("column_path is required"))?;
    let old_field_name = params.column_path.get(1).map(|s| s.as_str()).unwrap_or(column_name);

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = RenameFieldOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .rename_field(
            ctx,
            column_name,
            old_field_name,
            &params.new_name,
            &opts,
        )
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

pub async fn handle_remove_field(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        schema: String,
        name: String, // table name
        column_path: Vec<String>,
        #[serde(default)]
        ignore_not_found: bool,
    }
    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let column_name = params.column_path.first()
        .ok_or_else(|| Status::invalid_argument("column_path is required"))?;
    let field_name = params.column_path.get(1).map(|s| s.as_str()).unwrap_or(column_name);

    let schema_obj = get_schema(server, ctx, &params.schema).await?;
    let table = get_table(&schema_obj, ctx, &params.schema, &params.name).await?;
    let dyn_table = table
        .as_dynamic_table()
        .ok_or_else(|| Status::unimplemented("table does not support DDL operations"))?;

    let opts = RemoveFieldOptions {
        ignore_not_found: params.ignore_not_found,
    };
    dyn_table
        .remove_field(ctx, column_name, field_name, &opts)
        .await
        .map_err(|e| e.to_status())?;

    Ok(Response::new(empty_result()))
}

// Helper functions

/// Decodes Arrow schema from IPC message bytes.
///
/// The DuckDB Airport extension sends an Arrow IPC schema message in wire format:
///   - 4 bytes: continuation token (0xFFFFFFFF)
///   - 4 bytes: flatbuffer size (little-endian u32)
///   - N bytes: flatbuffer data
///
/// `root_as_message` expects raw flatbuffer bytes without the 8-byte wire header,
/// so we must strip it first.
fn decode_arrow_schema(bytes: &[u8]) -> Result<Arc<ArrowSchema>, String> {
    if bytes.is_empty() {
        return Ok(Arc::new(ArrowSchema::empty()));
    }

    // Strip the 8-byte IPC wire header (continuation token + size) if present.
    let fb_bytes = if bytes.len() >= 8 && bytes[0] == 0xFF && bytes[1] == 0xFF && bytes[2] == 0xFF && bytes[3] == 0xFF {
        let size = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        let end = 8 + size;
        if end <= bytes.len() {
            &bytes[8..end]
        } else {
            &bytes[8..]
        }
    } else {
        bytes
    };

    match arrow_ipc::root_as_message(fb_bytes) {
        Ok(msg) => {
            if let Some(ipc_schema) = msg.header_as_schema() {
                let schema = arrow_ipc::convert::fb_to_schema(ipc_schema);
                Ok(Arc::new(schema))
            } else {
                Err(format!("IPC message is not a schema message (header type: {:?})", msg.header_type()))
            }
        }
        Err(e) => Err(format!("failed to parse IPC schema flatbuffer: {}", e)),
    }
}

async fn get_schema(
    server: &Server,
    ctx: &RequestContext,
    schema_name: &str,
) -> Result<Arc<dyn crate::catalog::Schema>, Status> {
    server
        .catalog
        .schema(ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))
}

async fn get_table(
    schema: &Arc<dyn crate::catalog::Schema>,
    ctx: &RequestContext,
    schema_name: &str,
    table_name: &str,
) -> Result<Arc<dyn crate::catalog::table::Table>, Status> {
    schema
        .table(ctx, table_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| {
            Status::not_found(format!("table not found: {}.{}", schema_name, table_name))
        })
}

/// Parses a DuckDB type string to an Arrow DataType.
/// This is a simplified mapping; full type mapping would be more complex.
fn parse_data_type(type_str: &str) -> DataType {
    match type_str.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" | "INT8" => DataType::Int8,
        "SMALLINT" | "INT16" | "INT2" => DataType::Int16,
        "INTEGER" | "INT" | "INT32" | "INT4" => DataType::Int32,
        "BIGINT" | "INT64" | "INT8_ALIAS" => DataType::Int64,
        "UTINYINT" | "UINT8" => DataType::UInt8,
        "USMALLINT" | "UINT16" => DataType::UInt16,
        "UINTEGER" | "UINT32" => DataType::UInt32,
        "UBIGINT" | "UINT64" => DataType::UInt64,
        "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
        "DOUBLE" | "FLOAT8" => DataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" => DataType::Utf8,
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        "TIMESTAMP" | "DATETIME" => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        }
        "INTERVAL" => DataType::Duration(arrow_schema::TimeUnit::Microsecond),
        "JSON" => DataType::Utf8,
        "UUID" => DataType::FixedSizeBinary(16),
        _ => DataType::Utf8, // fallback
    }
}

/// Builds a protobuf-encoded FlightInfo for a table, matching airport-go's
/// `buildTableFlightInfo` response used by DDL create_table and alter-column actions.
fn build_table_flight_info_bytes(
    server: &Server,
    schema_name: &str,
    table_name: &str,
    arrow_schema: Arc<ArrowSchema>,
) -> Result<Vec<u8>, Status> {
    let schema_bytes = encode_schema(&arrow_schema)?;

    let app_metadata = {
        use rmpv::Value as V;
        let v = V::Map(vec![
            (V::String("type".into()),         V::String("table".into())),
            (V::String("schema".into()),       V::String(schema_name.into())),
            (V::String("catalog".into()),      V::String("".into())),
            (V::String("name".into()),         V::String(table_name.into())),
            (V::String("comment".into()),      V::String("".into())),
            (V::String("input_schema".into()), V::Nil),
            (V::String("action_name".into()),  V::Nil),
            (V::String("description".into()),  V::Nil),
            (V::String("extra_data".into()),   V::Nil),
        ]);
        crate::internal::msgpack::encode_rmpv(&v)
            .map_err(|e| Status::internal(e.to_string()))?
    };

    let ticket_bytes =
        TicketData::encode_table_ticket(server.catalog_name(), schema_name, table_name)
            .map_err(|e| Status::internal(e.to_string()))?;

    let fi = FlightInfo {
        schema: schema_bytes,
        flight_descriptor: Some(FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![schema_name.to_string(), table_name.to_string()],
            ..Default::default()
        }),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket_bytes.into(),
            }),
            location: vec![],
            ..Default::default()
        }],
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: app_metadata.into(),
    };

    let mut buf = Vec::new();
    fi.encode(&mut buf)
        .map_err(|e| Status::internal(format!("failed to encode FlightInfo: {}", e)))?;
    Ok(buf)
}

/// Builds the msgpack response body for create_schema:
/// {"sha256": <hex>, "url": nil, "serialized": <bytes>}
/// Matching Go's airport-go `create_schema` response format.
fn build_schema_contents_response(
    sha256_hash: &str,
    serialized_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::new();
    // 3-field map
    rmp::encode::write_map_len(&mut buf, 3)
        .map_err(|e| e.to_string())?;
    // "sha256" -> sha256_hash string
    rmp::encode::write_str(&mut buf, "sha256")
        .map_err(|e| e.to_string())?;
    rmp::encode::write_str(&mut buf, sha256_hash)
        .map_err(|e| e.to_string())?;
    // "url" -> nil
    rmp::encode::write_str(&mut buf, "url")
        .map_err(|e| e.to_string())?;
    rmp::encode::write_nil(&mut buf)
        .map_err(|e| e.to_string())?;
    // "serialized" -> bytes (written as msgpack bin to match Go's []byte encoding)
    rmp::encode::write_str(&mut buf, "serialized")
        .map_err(|e| e.to_string())?;
    rmp::encode::write_bin_len(&mut buf, serialized_bytes.len() as u32)
        .map_err(|e| e.to_string())?;
    buf.extend_from_slice(serialized_bytes);
    Ok(buf)
}
