use super::{Authenticator, Result};
use crate::error::AirportError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type ValidateFn = Arc<
    dyn Fn(
            String,
        ) -> Pin<Box<dyn Future<Output = std::result::Result<String, AirportError>> + Send>>
        + Send
        + Sync,
>;

/// BearerAuth creates an Authenticator from a validation function.
///
/// # Example
///
/// ```rust
/// use airport::auth::BearerAuth;
/// use airport::error::AirportError;
///
/// let auth = BearerAuth::new(|token: String| async move {
///     if token == "secret" {
///         Ok("user123".to_string())
///     } else {
///         Err(AirportError::Unauthorized("invalid token".to_string()))
///     }
/// });
/// ```
pub struct BearerAuth {
    validate_fn: ValidateFn,
}

impl BearerAuth {
    /// Creates a new BearerAuth from an async validation function.
    pub fn new<F, Fut>(validate_fn: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<String, AirportError>> + Send + 'static,
    {
        BearerAuth {
            validate_fn: Arc::new(move |token| Box::pin(validate_fn(token))),
        }
    }

    /// Creates a BearerAuth from a synchronous validation function (convenience wrapper).
    pub fn sync<F>(validate_fn: F) -> Self
    where
        F: Fn(&str) -> std::result::Result<String, AirportError> + Send + Sync + 'static,
    {
        let validate_fn = Arc::new(validate_fn);
        BearerAuth::new(move |token: String| {
            let validate_fn = validate_fn.clone();
            async move { validate_fn(&token) }
        })
    }
}

#[async_trait::async_trait]
impl Authenticator for BearerAuth {
    async fn authenticate(&self, token: &str) -> Result<String> {
        (self.validate_fn)(token.to_string()).await
    }
}
