use super::server::{BoxStream, Server};
use crate::catalog::ScanOptions;
use crate::catalog::table::SendableRecordBatchStream;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use arrow_schema::Schema as ArrowSchema;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;
use tonic::{Response, Status, Streaming};
use tracing::debug;

/// Wraps a `Streaming<FlightData>` and filters out control messages
/// (those with `data_header.len() < 4`) before IPC decoding.
fn filtered_flight_stream(
    stream: Streaming<FlightData>,
) -> FlightRecordBatchStream {
    let filtered = stream
        .map_err(|e| FlightError::Tonic(Box::new(e)))
        .filter_map(|data| async move {
            match data {
                Ok(d) if d.data_header.len() < 4 => None,
                Ok(d) => Some(Ok(d)),
                Err(e) => Some(Err(e)),
            }
        });
    FlightRecordBatchStream::new_from_flight_data(filtered)
}

/// Handles scalar function execution via DoExchange.
pub async fn handle_scalar_function(
    server: &Server,
    ctx: RequestContext,
    stream: Streaming<FlightData>,
    schema_name: &str,
    function_name: &str,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    debug!(
        "DoExchange scalar_function: {}.{}",
        schema_name, function_name
    );

    let schema_obj = server
        .catalog
        .schema(&ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let functions = schema_obj
        .scalar_functions(&ctx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let func = functions
        .iter()
        .find(|f| f.name() == function_name)
        .ok_or_else(|| {
            Status::not_found(format!(
                "scalar function not found: {}.{}",
                schema_name, function_name
            ))
        })?
        .clone();

    let sig = func.signature();
    let output_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new(
            "result",
            sig.return_type.clone().unwrap_or(arrow_schema::DataType::Null),
            true,
        ),
    ]));

    // Filter control messages (data_header.len() < 4) before IPC decoding.
    let mut input_stream = filtered_flight_stream(stream);

    let output_schema_clone = output_schema.clone();
    let ctx_clone = ctx.clone();

    // Process batches
    let output_stream = async_stream::try_stream! {
        while let Some(batch) = input_stream.next().await {
            // FlightRecordBatchStream yields Result<RecordBatch, ArrowError>
            let batch = batch.map_err(|e| crate::error::AirportError::Internal(e.to_string()))?;
            let expected_rows = batch.num_rows() as i64;

            // Execute scalar function
            let result_array = func
                .execute(&ctx_clone, &batch)
                .await
                .map_err(|e| crate::error::AirportError::Internal(e.to_string()))?;

            if result_array.len() as i64 != expected_rows {
                // In try_stream!, use `?` to propagate errors (not `return Err(...)`)
                Err(crate::error::AirportError::Internal(format!(
                    "output rows must match input rows, expected {} got {}",
                    expected_rows,
                    result_array.len()
                )))?;
            }

            let out_batch = RecordBatch::try_new(
                output_schema_clone.clone(),
                vec![result_array],
            )
            .map_err(|e| crate::error::AirportError::Arrow(e))?;

            yield out_batch;
        }
    };

    let flight_stream = FlightDataEncoderBuilder::new()
        .with_schema(output_schema)
        .build(
            output_stream
                .map_err(|e: crate::error::AirportError| {
                    FlightError::Arrow(arrow_schema::ArrowError::IoError(
                        e.to_string(),
                        std::io::Error::other(e.to_string()),
                    ))
                }),
        )
        .map_err(|e| Status::internal(e.to_string()));

    Ok(Response::new(Box::pin(flight_stream)))
}

/// Handles table function (in/out) execution via DoExchange.
pub async fn handle_table_function_in_out(
    server: &Server,
    ctx: RequestContext,
    mut stream: Streaming<FlightData>,
    schema_name: &str,
    function_name: &str,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    debug!(
        "DoExchange table_function_in_out: {}.{}",
        schema_name, function_name
    );

    let schema_obj = server
        .catalog
        .schema(&ctx, schema_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found(format!("schema not found: {}", schema_name)))?;

    let functions = schema_obj
        .table_functions_in_out(&ctx)
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

    // Read initial messages: schema message + parameter message
    let _schema_msg = stream.next().await.ok_or(Status::internal("expected schema message"))??;
    let param_msg = stream.next().await.ok_or(Status::internal("expected parameter message"))??;

    // Decode function parameters from app_metadata
    let params = if !param_msg.app_metadata.is_empty() {
        msgpack::decode_function_params(&param_msg.app_metadata)
    } else {
        vec![]
    };

    // Filter control messages before IPC decoding.
    let input_stream: SendableRecordBatchStream = Box::pin(
        filtered_flight_stream(stream)
            .map_err(|e| crate::error::AirportError::Internal(e.to_string())),
    );

    // Determine output schema for the function
    let empty_input_schema = Arc::new(ArrowSchema::empty());
    let output_schema = func
        .schema_for_parameters(&ctx, &params, &empty_input_schema)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    // Execute the function
    let output_stream = func
        .execute(&ctx, &params, input_stream, &ScanOptions::default())
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let flight_stream = FlightDataEncoderBuilder::new()
        .with_schema(output_schema)
        .build(
            output_stream.map_err(|e| {
                FlightError::Arrow(arrow_schema::ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::other(e.to_string()),
                ))
            }),
        )
        .map_err(|e| Status::internal(e.to_string()));

    Ok(Response::new(Box::pin(flight_stream)))
}
