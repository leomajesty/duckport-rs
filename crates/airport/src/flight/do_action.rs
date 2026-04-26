pub use super::server::BoxStream;
use super::server::Server;
use crate::flight::context::RequestContext;
use arrow_flight::Action;
use tonic::{Response, Status};
use tracing::debug;

/// Handles DoAction RPC: dispatches to appropriate action handler.
pub async fn handle_do_action(
    server: &Server,
    ctx: &RequestContext,
    action: Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    debug!("DoAction: type={}", action.r#type);

    match action.r#type.as_str() {
        "list_schemas" => {
            super::do_action_metadata::handle_list_schemas(server, ctx, &action).await
        }
        "flight_info" => {
            super::do_action_metadata::handle_flight_info(server, ctx, &action).await
        }
        "table_function_flight_info" => {
            super::do_action_functions::handle_table_function_flight_info(server, ctx, &action).await
        }
        "endpoints" => {
            super::do_action_metadata::handle_endpoints(server, ctx, &action).await
        }
        "catalog_version" => {
            super::do_action_metadata::handle_catalog_version(server, ctx, &action).await
        }
        "column_statistics" => {
            super::do_action_statistics::handle_column_statistics(server, ctx, &action).await
        }
        // DDL operations
        "create_schema" => {
            super::do_action_ddl::handle_create_schema(server, ctx, &action).await
        }
        "drop_schema" => {
            super::do_action_ddl::handle_drop_schema(server, ctx, &action).await
        }
        "create_table" => {
            super::do_action_ddl::handle_create_table(server, ctx, &action).await
        }
        "drop_table" => {
            super::do_action_ddl::handle_drop_table(server, ctx, &action).await
        }
        "add_column" => {
            super::do_action_ddl::handle_add_column(server, ctx, &action).await
        }
        "remove_column" => {
            super::do_action_ddl::handle_remove_column(server, ctx, &action).await
        }
        "rename_column" => {
            super::do_action_ddl::handle_rename_column(server, ctx, &action).await
        }
        "rename_table" => {
            super::do_action_ddl::handle_rename_table(server, ctx, &action).await
        }
        "change_column_type" => {
            super::do_action_ddl::handle_change_column_type(server, ctx, &action).await
        }
        "set_not_null" => {
            super::do_action_ddl::handle_set_not_null(server, ctx, &action).await
        }
        "drop_not_null" => {
            super::do_action_ddl::handle_drop_not_null(server, ctx, &action).await
        }
        "set_default" => {
            super::do_action_ddl::handle_set_default(server, ctx, &action).await
        }
        "add_field" => {
            super::do_action_ddl::handle_add_field(server, ctx, &action).await
        }
        "rename_field" => {
            super::do_action_ddl::handle_rename_field(server, ctx, &action).await
        }
        "remove_field" => {
            super::do_action_ddl::handle_remove_field(server, ctx, &action).await
        }
        // Transaction management
        "create_transaction" => {
            super::transaction::handle_create_transaction(server, ctx, &action).await
        }
        "get_transaction_status" => {
            super::transaction::handle_get_transaction_status(server, ctx, &action).await
        }
        // Explicit SQL COMMIT / ROLLBACK sent by DuckDB Airport extension.
        "commit_transaction" => {
            super::transaction::handle_commit_transaction(server, ctx, &action).await
        }
        "abort_transaction" => {
            super::transaction::handle_abort_transaction(server, ctx, &action).await
        }
        unknown => Err(Status::unimplemented(format!(
            "unknown action type: {}",
            unknown
        ))),
    }
}

/// Helper: sends a single empty result back to the client.
pub fn empty_result() -> BoxStream<arrow_flight::Result> {
    let result = arrow_flight::Result {
        body: bytes::Bytes::new(),
    };
    Box::pin(futures::stream::once(async move { Ok(result) }))
}

/// Helper: sends a single result with body back to the client.
pub fn single_result(body: Vec<u8>) -> BoxStream<arrow_flight::Result> {
    let result = arrow_flight::Result {
        body: body.into(),
    };
    Box::pin(futures::stream::once(async move { Ok(result) }))
}
