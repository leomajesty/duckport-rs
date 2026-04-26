use crate::auth::AuthenticatorRef;
use crate::catalog::Catalog;
use crate::catalog::transaction::TransactionManager;
use std::sync::Arc;

/// ServerConfig contains configuration for an Airport Flight server.
pub struct ServerConfig {
    /// The catalog to serve. REQUIRED.
    pub catalog: Arc<dyn Catalog>,

    /// Optional authenticator. If None, all requests are allowed.
    pub auth: Option<AuthenticatorRef>,

    /// The server's public address (e.g. "localhost:50052").
    /// Used to populate FlightEndpoint locations.
    /// If empty, endpoints will not include a URI.
    pub address: String,

    /// Maximum gRPC message size in bytes.
    /// If 0, uses the tonic default.
    pub max_message_size: usize,

    /// Optional transaction manager for multi-operation transactions.
    pub tx_manager: Option<Arc<dyn TransactionManager>>,
}

impl ServerConfig {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        ServerConfig {
            catalog,
            auth: None,
            address: String::new(),
            max_message_size: 0,
            tx_manager: None,
        }
    }

    pub fn with_auth(mut self, auth: AuthenticatorRef) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = address.into();
        self
    }

    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    pub fn with_tx_manager(mut self, tx_manager: Arc<dyn TransactionManager>) -> Self {
        self.tx_manager = Some(tx_manager);
        self
    }
}
