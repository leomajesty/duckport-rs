use crate::auth::AuthenticatorRef;
use crate::catalog::Catalog;
use crate::catalog::transaction::TransactionManager;
use crate::flight::context::RequestContext;
use crate::flight::interceptor::maybe_authenticate;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Shared DML operation state for correlating DoExchange metadata with DoPut row data.
/// DuckDB sends operation metadata via DoExchange but row data via DoPut.
#[derive(Clone)]
pub struct DMLOperationState {
    pub operation: String,
    pub schema_name: String,
    pub table_name: String,
    pub return_data: bool,
    pub batches: Vec<arrow_array::RecordBatch>,
    pub completed: bool,
}

impl Default for DMLOperationState {
    fn default() -> Self {
        Self {
            operation: String::new(),
            schema_name: String::new(),
            table_name: String::new(),
            return_data: false,
            batches: Vec::new(),
            completed: false,
        }
    }
}

/// Server implements the Arrow Flight service for the Airport protocol.
pub struct Server {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) auth: Option<AuthenticatorRef>,
    pub(crate) address: String,
    pub(crate) tx_manager: Option<Arc<dyn TransactionManager>>,
    pub(crate) catalog_name: String,
    /// Shared state for correlating DoExchange metadata with DoPut row data
    pub(crate) dml_state: Arc<Mutex<HashMap<String, DMLOperationState>>>,
}

impl Server {
    /// Creates a new Flight server.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        auth: Option<AuthenticatorRef>,
        address: impl Into<String>,
        tx_manager: Option<Arc<dyn TransactionManager>>,
        catalog_name: impl Into<String>,
    ) -> Self {
        let address = normalize_address(address.into());
        Server {
            catalog,
            auth,
            address,
            tx_manager,
            catalog_name: catalog_name.into(),
            dml_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the catalog name for this server.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Authenticates a request and returns the enriched RequestContext.
    async fn authenticate_request(&self, metadata: &tonic::metadata::MetadataMap) -> Result<RequestContext, Status> {
        maybe_authenticate(self.auth.as_ref(), metadata).await
    }
}

/// Normalizes a server address to include the grpc:// scheme.
fn normalize_address(address: String) -> String {
    if address.is_empty() {
        // Use reuse-connection sentinel
        return String::new();
    }
    if address.starts_with("grpc://") || address.starts_with("grpc+tls://") {
        address
    } else {
        format!("grpc://{}", address)
    }
}

#[tonic::async_trait]
impl FlightService for Server {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // DuckDB Airport extension does not call Handshake.
        // Auth is enforced by the interceptor on every RPC call.
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        super::list_flights::handle_list_flights(self, &ctx, request.into_inner()).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        super::get_flight_info::handle_get_flight_info(self, &ctx, request.into_inner()).await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        let desc = request.into_inner();
        tracing::debug!("get_schema called: path={:?}", desc.path);
        // Delegate to get_flight_info and extract schema
        let fi = super::get_flight_info::handle_get_flight_info(self, &ctx, desc).await?;
        Ok(Response::new(SchemaResult {
            schema: fi.into_inner().schema,
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        let ticket = request.into_inner();
        tracing::warn!("do_get RPC called: ticket={}", String::from_utf8_lossy(&ticket.ticket));
        super::do_get::handle_do_get(self, &ctx, ticket).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        tracing::debug!("do_put called");
        super::do_put::handle_do_put(self, ctx, request.into_inner()).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        tracing::debug!("do_exchange called");
        super::do_exchange::handle_do_exchange(self, ctx, request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let ctx = self.authenticate_request(request.metadata()).await?;
        super::do_action::handle_do_action(self, &ctx, request.into_inner()).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let _ctx = self.authenticate_request(request.metadata()).await?;
        let actions = vec![
            ActionType {
                r#type: "list_schemas".to_string(),
                description: "List all schemas in the catalog".to_string(),
            },
            ActionType {
                r#type: "endpoints".to_string(),
                description: "Get endpoints for a table".to_string(),
            },
            ActionType {
                r#type: "table_function_flight_info".to_string(),
                description: "Get flight info for a table function".to_string(),
            },
            ActionType {
                r#type: "flight_info".to_string(),
                description: "Get flight info (time travel support)".to_string(),
            },
            ActionType {
                r#type: "create_schema".to_string(),
                description: "Create a schema".to_string(),
            },
            ActionType {
                r#type: "drop_schema".to_string(),
                description: "Drop a schema".to_string(),
            },
            ActionType {
                r#type: "create_table".to_string(),
                description: "Create a table".to_string(),
            },
            ActionType {
                r#type: "drop_table".to_string(),
                description: "Drop a table".to_string(),
            },
            ActionType {
                r#type: "column_statistics".to_string(),
                description: "Get column statistics".to_string(),
            },
            ActionType {
                r#type: "create_transaction".to_string(),
                description: "Begin a transaction".to_string(),
            },
            ActionType {
                r#type: "get_transaction_status".to_string(),
                description: "Get transaction status".to_string(),
            },
        ];
        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}
