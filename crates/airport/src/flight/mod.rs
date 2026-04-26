pub mod context;
pub mod do_action;
pub mod do_action_ddl;
pub mod do_action_functions;
pub mod do_action_metadata;
pub mod do_action_statistics;
pub mod do_exchange;
pub mod do_exchange_dml;
pub mod do_exchange_functions;
pub mod do_get;
pub mod do_put;
pub mod errors;
pub mod flight_data_decoder;
pub mod get_flight_info;
pub mod interceptor;
pub mod list_flights;
pub mod multicatalog;
pub mod projection;
pub mod server;
pub mod ticket;
pub mod transaction;

pub use server::Server;
pub use context::RequestContext;

use crate::auth::AuthenticatorRef;
use crate::catalog::Catalog;
use crate::catalog::transaction::TransactionManager;
use arrow_flight::flight_service_server::FlightServiceServer;
use std::sync::Arc;

/// Creates a new Flight service that can be registered with a tonic server.
pub fn new_flight_service(
    catalog: Arc<dyn Catalog>,
    auth: Option<AuthenticatorRef>,
    address: impl Into<String>,
    tx_manager: Option<Arc<dyn TransactionManager>>,
    catalog_name: impl Into<String>,
) -> FlightServiceServer<Server> {
    let server = Server::new(catalog, auth, address, tx_manager, catalog_name);
    FlightServiceServer::new(server)
}
