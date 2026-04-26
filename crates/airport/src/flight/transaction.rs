use super::do_action::{single_result, BoxStream};
use super::server::Server;
use crate::flight::context::RequestContext;
use crate::internal::msgpack;
use arrow_flight::Action;
use tonic::{Response, Status};

/// Handles create_transaction action: begins a new transaction.
///
/// If no TransactionManager is configured, returns `{"identifier": null}` —
/// DuckDB interprets a null identifier as "no server-side transaction needed"
/// and proceeds without transaction coordination.  Returning an error here
/// causes DuckDB to abort every query, so we must always succeed.
pub async fn handle_create_transaction(
    server: &Server,
    ctx: &RequestContext,
    _action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    let identifier: serde_json::Value = match server.tx_manager.as_ref() {
        None => serde_json::Value::Null,
        Some(tx_manager) => {
            let tx_id = tx_manager
                .begin_transaction(ctx)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            serde_json::Value::String(tx_id)
        }
    };

    let response = serde_json::json!({ "identifier": identifier });
    let body = msgpack::encode(&response).map_err(|e| Status::internal(e.to_string()))?;

    Ok(Response::new(single_result(body)))
}

/// Handles commit_transaction action: called by DuckDB on SQL COMMIT.
///
/// The TransactionManager should release any snapshot held since BeginTransaction.
/// Returns an empty result on success.
///
/// DuckDB may send the transaction ID either in the gRPC metadata header
/// (`airport-transaction-id`) or in the action body as a msgpack-encoded
/// `{"transaction_id": "..."}` object.  We try both.
pub async fn handle_commit_transaction(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    let tx_id = resolve_tx_id(ctx, action);

    if tx_id.is_empty() {
        // No transaction in context — nothing to commit.
        return Ok(Response::new(super::do_action::empty_result()));
    }

    if let Some(tx_manager) = server.tx_manager.as_ref() {
        tx_manager
            .commit_transaction(ctx, &tx_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
    }

    Ok(Response::new(super::do_action::empty_result()))
}

/// Handles abort_transaction action: called by DuckDB on SQL ROLLBACK.
///
/// The TransactionManager should restore the snapshot taken at BeginTransaction.
/// Returns an empty result on success.
///
/// DuckDB may send the transaction ID either in the gRPC metadata header
/// (`airport-transaction-id`) or in the action body as a msgpack-encoded
/// `{"transaction_id": "..."}` object.  We try both.
pub async fn handle_abort_transaction(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    let tx_id = resolve_tx_id(ctx, action);

    if tx_id.is_empty() {
        return Ok(Response::new(super::do_action::empty_result()));
    }

    if let Some(tx_manager) = server.tx_manager.as_ref() {
        tx_manager
            .rollback_transaction(ctx, &tx_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
    }

    Ok(Response::new(super::do_action::empty_result()))
}

/// Resolves the transaction ID from context metadata or action body.
///
/// DuckDB may place the transaction ID in:
///   1. The `airport-transaction-id` gRPC metadata header (regular DML path)
///   2. The msgpack-encoded action body as `{"transaction_id": "..."}` (explicit commit/abort)
fn resolve_tx_id(ctx: &RequestContext, action: &Action) -> String {
    if !ctx.transaction_id.is_empty() {
        return ctx.transaction_id.clone();
    }
    // Fall back to action body.
    #[derive(serde::Deserialize)]
    struct Body {
        transaction_id: Option<String>,
    }
    if !action.body.is_empty() {
        if let Ok(b) = crate::internal::msgpack::decode::<Body>(&action.body) {
            if let Some(id) = b.transaction_id {
                if !id.is_empty() {
                    return id;
                }
            }
        }
    }
    String::new()
}

/// Handles get_transaction_status action.
pub async fn handle_get_transaction_status(
    server: &Server,
    ctx: &RequestContext,
    action: &Action,
) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
    #[derive(serde::Deserialize)]
    struct Params {
        transaction_id: String,
    }

    let params: Params = msgpack::decode(&action.body)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    let state_str = if let Some(tx_manager) = server.tx_manager.as_ref() {
        let status_opt = tx_manager
            .get_transaction_status(ctx, &params.transaction_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        match status_opt {
            None => "not_found",
            Some(crate::catalog::transaction::TransactionState::Active) => "active",
            Some(crate::catalog::transaction::TransactionState::Committed) => "committed",
            Some(crate::catalog::transaction::TransactionState::RolledBack) => "rolled_back",
        }
    } else {
        "not_found"
    };

    let response = serde_json::json!({ "state": state_str });
    let body = msgpack::encode(&response).map_err(|e| Status::internal(e.to_string()))?;

    Ok(Response::new(single_result(body)))
}
