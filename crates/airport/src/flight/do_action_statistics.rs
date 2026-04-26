use super::do_action::{single_result, BoxStream};
use super::server::Server;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use arrow_flight::Action;
use serde_json::Value as JsonValue;
use tonic::{Response, Status};

/// Handles column_statistics action: returns column statistics for query optimization.
pub async fn handle_column_statistics(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Request {
        schema: String,
        table: String,
        column: String,
        #[serde(default)]
        column_type: String,
    }

    let request: Request = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(format!("invalid request: {}", e)))?;

    // Look up schema and table
    let schema = server
        .catalog
        .schema(ctx, &request.schema)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", request.schema)))?;

    let table = schema
        .table(ctx, &request.table)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| {
            Status::not_found(format!("table not found: {}.{}", request.schema, request.table))
        })?;

    // Check if table supports statistics
    let stats_table = match table.as_statistics() {
        Some(st) => st,
        None => {
            return Err(Status::unimplemented(format!(
                "table {}.{} does not support column statistics",
                request.schema, request.table
            )));
        }
    };

    let stats = stats_table
        .column_statistics(ctx, &request.column, &request.column_type)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    // Build response
    let mut response: serde_json::Map<String, JsonValue> = serde_json::Map::new();

    if let Some(v) = stats.has_not_null {
        response.insert("has_not_null".to_string(), JsonValue::Bool(v));
    }
    if let Some(v) = stats.has_null {
        response.insert("has_null".to_string(), JsonValue::Bool(v));
    }
    if let Some(v) = stats.distinct_count {
        response.insert("distinct_count".to_string(), JsonValue::Number(v.into()));
    }
    if let Some(v) = stats.min {
        response.insert("min".to_string(), v);
    }
    if let Some(v) = stats.max {
        response.insert("max".to_string(), v);
    }
    if let Some(v) = stats.max_string_length {
        response.insert("max_string_length".to_string(), JsonValue::Number(v.into()));
    }
    if let Some(v) = stats.contains_unicode {
        response.insert("contains_unicode".to_string(), JsonValue::Bool(v));
    }

    let body = msgpack::encode(&response)
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(Response::new(single_result(body)))
}
