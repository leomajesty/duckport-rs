pub mod bearer;

use crate::error::AirportError;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, AirportError>;

/// Type alias for a heap-allocated, thread-safe Authenticator.
pub type AuthenticatorRef = Arc<dyn Authenticator>;

/// Authenticator validates bearer tokens and returns user identity.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    /// Validate the given token and return the user identity string.
    /// Returns Err(AirportError::Unauthorized) if token is invalid.
    async fn authenticate(&self, token: &str) -> Result<String>;
}

/// CatalogAuthorizer extends Authenticator with per-catalog authorization.
#[async_trait::async_trait]
pub trait CatalogAuthorizer: Authenticator {
    /// Called after successful authentication to check catalog access.
    async fn authorize_catalog(&self, identity: &str, catalog_name: &str) -> Result<()>;
}

/// NoAuth returns an Authenticator that allows all requests without validation.
pub struct NoAuth;

#[async_trait::async_trait]
impl Authenticator for NoAuth {
    async fn authenticate(&self, _token: &str) -> Result<String> {
        Ok(String::new())
    }
}

/// Creates an Authenticator that allows all requests.
pub fn no_auth() -> NoAuth {
    NoAuth
}

pub use bearer::BearerAuth;
