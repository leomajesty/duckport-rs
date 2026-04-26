use tonic::metadata::MetadataMap;

/// RequestContext contains metadata about the current request.
/// Passed to all handler methods for authentication and context.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    /// Bearer token from Authorization header.
    pub token: String,

    /// Unique client session identifier.
    pub session_id: String,

    /// Transaction identifier (empty if no transaction).
    pub transaction_id: String,

    /// Identifies the Airport client version.
    pub user_agent: String,

    /// Authenticated user identity (set after auth).
    pub identity: Option<String>,

    /// Catalog name (used for multi-catalog routing).
    pub catalog_name: String,
}

impl RequestContext {
    /// Creates a RequestContext from tonic metadata.
    pub fn from_metadata(metadata: &MetadataMap) -> Self {
        let token = extract_bearer_token(metadata).unwrap_or_default();
        let session_id = get_metadata_str(metadata, "airport-session-id").unwrap_or_default();
        let transaction_id =
            get_metadata_str(metadata, "airport-transaction-id").unwrap_or_default();
        let user_agent = get_metadata_str(metadata, "user-agent").unwrap_or_default();
        let catalog_name = get_metadata_str(metadata, "airport-catalog").unwrap_or_default();

        RequestContext {
            token,
            session_id,
            transaction_id,
            user_agent,
            catalog_name,
            identity: None,
        }
    }

    /// Returns true if the request has a transaction ID.
    pub fn has_transaction(&self) -> bool {
        !self.transaction_id.is_empty()
    }

    /// Returns the authenticated identity, if any.
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    /// Sets the authenticated identity.
    pub fn with_identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }
}

/// Extracts bearer token from Authorization header.
pub fn extract_bearer_token(metadata: &MetadataMap) -> Option<String> {
    let auth = get_metadata_str(metadata, "authorization")?;
    let token = auth.strip_prefix("Bearer ")?;
    Some(token.to_string())
}

/// Gets a metadata value as a string.
fn get_metadata_str(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
