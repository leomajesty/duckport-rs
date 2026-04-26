//! Custom Flight write plane.
//!
//! `DuckportService` wraps `airport::flight::Server` and intercepts RPCs for
//! `duckport.*` DoActions (and, later, `DoPut` with a `duckport.*` descriptor path).
//! Everything else is delegated unchanged to the airport server, so DuckDB
//! Airport-extension clients keep working for SELECTs.
//!
//! Design notes:
//! - We intentionally do NOT try to expose transactions via airport's
//!   `TransactionManager` trait. The DuckDB Airport extension does not forward
//!   user `BEGIN`/`COMMIT` to the server, so any multi-statement atomicity has
//!   to happen via an explicit RPC (`duckport.execute_transaction`) called by
//!   ingestor clients directly.
//! - Custom writes serialise through `Backend::with_writer`, matching DuckDB's
//!   single-writer model.

pub mod actions;
pub mod proto;
pub mod put;
pub mod query;

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use crate::backend::Backend;

/// Shorthand for the boxed, pinned, `Send + 'static` streams every Flight RPC uses.
pub type DuckportStream<T> =
    Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

/// The Flight service tonic exposes. Owns both the airport server (delegate) and
/// the backend (for the custom write plane).
pub struct DuckportService {
    airport: Arc<airport::flight::Server>,
    backend: Backend,
    catalog_name: String,
}

impl DuckportService {
    pub fn new(
        airport: Arc<airport::flight::Server>,
        backend: Backend,
        catalog_name: String,
    ) -> Self {
        Self {
            airport,
            backend,
            catalog_name,
        }
    }
}

/// Rewraps an airport RPC response's stream into our own `DuckportStream` alias.
///
/// The two aliases resolve to identical concrete types (both
/// `Pin<Box<dyn Stream<Item=Result<T,Status>> + Send + 'static>>`), so this is a
/// zero-cost re-wrap. Keeping it as a helper avoids copy-pasting the `.map` /
/// `Box::pin` dance in every delegated RPC.
fn forward<T, S>(resp: Response<S>) -> Response<DuckportStream<T>>
where
    S: Stream<Item = std::result::Result<T, Status>> + Send + 'static,
    T: Send + 'static,
{
    let (meta, inner, ext) = resp.into_parts();
    let stream: DuckportStream<T> = Box::pin(inner);
    Response::from_parts(meta, stream, ext)
}

#[tonic::async_trait]
impl FlightService for DuckportService {
    type HandshakeStream = DuckportStream<HandshakeResponse>;
    type ListFlightsStream = DuckportStream<FlightInfo>;
    type DoGetStream = DuckportStream<FlightData>;
    type DoPutStream = DuckportStream<PutResult>;
    type DoExchangeStream = DuckportStream<FlightData>;
    type DoActionStream = DuckportStream<arrow_flight::Result>;
    type ListActionsStream = DuckportStream<ActionType>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.airport.handshake(request).await.map(forward)
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.airport.list_flights(request).await.map(forward)
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.airport.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.airport.poll_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.airport.get_schema(request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_bytes = &request.get_ref().ticket;
        if let Ok(qt) = serde_json::from_slice::<proto::QueryTicket>(ticket_bytes) {
            if qt.r#type == "duckport.query" {
                debug!(sql_len = qt.sql.len(), "intercepted duckport.query DoGet");
                return query::handle_query(self.backend.clone(), &qt.sql).await;
            }
        }
        self.airport.do_get(request).await.map(forward)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Peek the first FlightData to read the FlightDescriptor. Bulk append uses
        // path = ["duckport.append", <schema>, <table>]. Anything else is rejected:
        // tonic::Streaming can't be rebuilt from arbitrary streams so we can't
        // faithfully delegate to airport, and the DuckDB Airport extension never
        // issues DoPut (DML goes through DoExchange), so nothing real is lost.
        let (_meta, _ext, mut stream) = request.into_parts();
        let first = match stream.message().await? {
            Some(fd) => fd,
            None => {
                return Err(Status::invalid_argument(
                    "empty DoPut stream: expected at least a schema message",
                ));
            }
        };

        let path: Vec<String> = first
            .flight_descriptor
            .as_ref()
            .map(|d| d.path.clone())
            .unwrap_or_default();

        if path.first().map(String::as_str) == Some(put::APPEND_PATH_PREFIX) {
            if path.len() != 3 {
                return Err(Status::invalid_argument(format!(
                    "expected descriptor path ['{}', <schema>, <table>], got {:?}",
                    put::APPEND_PATH_PREFIX,
                    path
                )));
            }
            let schema = path[1].clone();
            let table = path[2].clone();
            return put::handle_append(self.backend.clone(), schema, table, first, stream).await;
        }

        Err(Status::unimplemented(format!(
            "DoPut only accepts descriptor path ['{}', <schema>, <table>]; got path={:?}",
            put::APPEND_PATH_PREFIX,
            path
        )))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.airport.do_exchange(request).await.map(forward)
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Split off metadata/extensions so we can either act locally or rebuild the
        // Request and hand it to airport. We must preserve metadata because airport's
        // `authenticate_request` reads auth headers from it.
        let (meta, ext, action) = request.into_parts();

        if action.r#type.starts_with(actions::PREFIX) {
            debug!(action = %action.r#type, "handling duckport.* action locally");
            return actions::handle(&self.backend, &self.catalog_name, action).await;
        }

        let req = Request::from_parts(meta, ext, action);
        self.airport.do_action(req).await.map(forward)
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let airport_resp = self.airport.list_actions(request).await?;
        let (meta, airport_stream, ext) = airport_resp.into_parts();

        // Prepend the duckport.* actions so clients that just read the head of the
        // stream also see them.
        let extras = actions::advertised()
            .into_iter()
            .map(|(ty, desc)| ActionType {
                r#type: ty.to_string(),
                description: desc.to_string(),
            });
        let extra_stream = futures::stream::iter(extras.map(Ok));
        let combined: DuckportStream<ActionType> =
            Box::pin(extra_stream.chain(airport_stream));

        Ok(Response::from_parts(meta, combined, ext))
    }
}
