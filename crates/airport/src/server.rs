/// Server creation entry points for Airport Flight servers.
///
/// Use `new_flight_service()` to create a single-catalog Flight service,
/// or use `MultiCatalogServer` from the `multicatalog` module for multi-catalog routing.
use crate::catalog::Catalog;
use crate::config::ServerConfig;
use crate::error::AirportError;
use crate::flight;
use arrow_flight::flight_service_server::FlightServiceServer;

pub type Result<T> = std::result::Result<T, AirportError>;

/// Creates a new single-catalog Flight service from a `ServerConfig`.
///
/// Returns a `FlightServiceServer` that can be registered with a tonic server.
///
/// # Example
///
/// ```rust,no_run
/// use airport::server::new_server;
/// use airport::config::ServerConfig;
/// use airport::catalog::static_catalog::StaticCatalog;
/// use std::sync::Arc;
///
/// #[tokio::main]
/// async fn main() {
///     let catalog = Arc::new(/* your catalog */);
///     let service = new_server(ServerConfig::new(catalog));
///
///     tonic::transport::Server::builder()
///         .add_service(service)
///         .serve("0.0.0.0:50052".parse().unwrap())
///         .await
///         .unwrap();
/// }
/// ```
pub fn new_server(
    config: ServerConfig,
) -> FlightServiceServer<crate::flight::server::Server> {
    let catalog_name = get_catalog_name(config.catalog.as_ref());
    let svc = flight::new_flight_service(
        config.catalog,
        config.auth,
        config.address,
        config.tx_manager,
        catalog_name,
    );
    if config.max_message_size > 0 {
        svc.max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size)
    } else {
        svc
    }
}

/// Returns the catalog name by calling `Catalog::name()`.
fn get_catalog_name(catalog: &dyn Catalog) -> String {
    catalog.name().to_string()
}
