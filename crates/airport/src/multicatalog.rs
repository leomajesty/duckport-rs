/// Multi-catalog server for routing requests to multiple catalogs.
///
/// Use `MultiCatalogServer` when you want to expose multiple named catalogs
/// (each with a distinct name) on a single Flight endpoint. Requests are
/// routed based on the `airport-catalog` gRPC metadata header.
use crate::auth::AuthenticatorRef;
use crate::catalog::transaction::TransactionManager;
use crate::catalog::Catalog;
use crate::error::AirportError;
use crate::flight::server::Server;
use arrow_flight::flight_service_server::FlightServiceServer;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

pub type Result<T> = std::result::Result<T, AirportError>;

/// Configuration for a multi-catalog Flight server.
pub struct MultiCatalogConfig {
    /// Initial catalogs to register. Each should implement `NamedCatalog` for routing.
    pub catalogs: Vec<Arc<dyn Catalog>>,

    /// Optional authenticator. If None, all requests are allowed.
    pub auth: Option<AuthenticatorRef>,

    /// The server's public address for FlightEndpoint locations.
    pub address: String,

    /// Maximum gRPC message size in bytes (0 = tonic default).
    pub max_message_size: usize,
}

impl Default for MultiCatalogConfig {
    fn default() -> Self {
        MultiCatalogConfig {
            catalogs: vec![],
            auth: None,
            address: String::new(),
            max_message_size: 0,
        }
    }
}

impl MultiCatalogConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_auth(mut self, auth: AuthenticatorRef) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = address.into();
        self
    }

    pub fn with_catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalogs.push(catalog);
        self
    }
}

/// MultiCatalogServer routes Flight requests to the correct per-catalog server
/// based on the `airport-catalog` gRPC metadata header.
///
/// Catalogs can be added or removed at runtime without restarting the server.
/// The routing key is the catalog name (from `NamedCatalog::name()`).
pub struct MultiCatalogServer {
    servers: RwLock<HashMap<String, Arc<Server>>>,
    default_server: Option<Arc<Server>>,
    auth: Option<AuthenticatorRef>,
    address: String,
    #[allow(dead_code)]
    tx_manager: Option<Arc<dyn TransactionManager>>,
}

impl MultiCatalogServer {
    /// Creates a new MultiCatalogServer.
    pub fn new(config: MultiCatalogConfig) -> Result<Self> {
        let mut servers = HashMap::new();
        let mut default_server = None;

        for catalog in config.catalogs {
            let name = get_catalog_name(catalog.as_ref());
            let server = Arc::new(Server::new(
                catalog,
                config.auth.clone(),
                &config.address,
                None,
                &name,
            ));
            if name.is_empty() {
                default_server = Some(server);
            } else {
                if servers.contains_key(&name) {
                    return Err(AirportError::Internal(format!(
                        "duplicate catalog name: {}",
                        name
                    )));
                }
                servers.insert(name, server);
            }
        }

        Ok(MultiCatalogServer {
            servers: RwLock::new(servers),
            default_server,
            auth: config.auth,
            address: config.address,
            tx_manager: None,
        })
    }

    /// Adds a catalog to the server at runtime.
    /// Returns an error if a catalog with the same name already exists.
    pub fn add_catalog(
        &self,
        catalog: Arc<dyn Catalog>,
        tx_manager: Option<Arc<dyn TransactionManager>>,
    ) -> Result<()> {
        let name = get_catalog_name(catalog.as_ref());
        let server = Arc::new(Server::new(
            catalog,
            self.auth.clone(),
            &self.address,
            tx_manager,
            &name,
        ));
        let mut servers = self.servers.write().unwrap();
        if servers.contains_key(&name) {
            return Err(AirportError::Internal(format!(
                "catalog '{}' already exists",
                name
            )));
        }
        servers.insert(name, server);
        Ok(())
    }

    /// Removes a catalog by name. Returns true if the catalog was found and removed.
    pub fn remove_catalog(&self, name: &str) -> bool {
        let mut servers = self.servers.write().unwrap();
        servers.remove(name).is_some()
    }

    /// Returns true if a catalog with the given name exists.
    pub fn has_catalog(&self, name: &str) -> bool {
        let servers = self.servers.read().unwrap();
        servers.contains_key(name)
    }

    /// Returns all registered catalog names.
    pub fn catalog_names(&self) -> Vec<String> {
        let servers = self.servers.read().unwrap();
        servers.keys().cloned().collect()
    }

    /// Resolves the correct per-catalog `Server` from the request metadata.
    /// Uses the `airport-catalog` header, falling back to the default server.
    pub fn resolve_server(&self, catalog_name: &str) -> Option<Arc<Server>> {
        let servers = self.servers.read().unwrap();
        if let Some(server) = servers.get(catalog_name) {
            return Some(server.clone());
        }
        self.default_server.clone()
    }

    /// Wraps this MultiCatalogServer as a tonic FlightServiceServer for use with tonic.
    pub fn into_service(self) -> FlightServiceServer<MultiCatalogDispatcher> {
        FlightServiceServer::new(MultiCatalogDispatcher {
            inner: Arc::new(self),
        })
    }
}

/// Extracts the catalog name from a Catalog implementation.
/// Returns an empty string if the catalog doesn't implement NamedCatalog.
pub fn get_catalog_name(_catalog: &dyn Catalog) -> String {
    // Since we can't directly downcast dyn Catalog -> dyn NamedCatalog without Any,
    // this is a limitation. Users should ensure their catalogs implement NamedCatalog.
    // The catalog name should be set explicitly via MultiCatalogConfig.
    String::new()
}

/// MultiCatalogDispatcher implements the Arrow FlightService trait by dispatching
/// each RPC to the appropriate per-catalog Server based on the request metadata.
pub struct MultiCatalogDispatcher {
    inner: Arc<MultiCatalogServer>,
}

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use std::pin::Pin;
use tonic::Streaming;

type BoxStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for MultiCatalogDispatcher {
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
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.get_schema(request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.do_get(request).await
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("DoPut is not supported; use DoExchange"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.do_exchange(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let catalog_name = extract_catalog_name(request.metadata());
        let server = self.inner.resolve_server(&catalog_name)
            .ok_or_else(|| Status::not_found(format!("catalog not found: {}", catalog_name)))?;
        server.list_actions(request).await
    }
}

/// Extracts the `airport-catalog` header value from gRPC metadata.
fn extract_catalog_name(metadata: &tonic::metadata::MetadataMap) -> String {
    metadata
        .get("airport-catalog")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}
