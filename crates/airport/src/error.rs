use thiserror::Error;

/// AirportError is the unified error type for the airport library.
#[derive(Debug, Error)]
pub enum AirportError {
    /// Table, schema, or catalog not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Object already exists.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// Schema is not empty (for DROP SCHEMA without CASCADE).
    #[error("schema not empty: {0}")]
    SchemaNotEmpty(String),

    /// Authentication failed.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// Invalid configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Invalid parameters.
    #[error("invalid parameters: {0}")]
    InvalidParameters(String),

    /// Operation not supported.
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Null rowid encountered in UPDATE or DELETE.
    #[error("null rowid value not allowed")]
    NullRowId,

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Transaction error.
    #[error("transaction error: {0}")]
    Transaction(String),
}

impl AirportError {
    /// Convert to a tonic gRPC Status.
    pub fn to_status(&self) -> tonic::Status {
        match self {
            AirportError::NotFound(msg) => tonic::Status::not_found(msg),
            AirportError::AlreadyExists(msg) => {
                tonic::Status::already_exists(msg)
            }
            AirportError::Unauthorized(msg) => {
                tonic::Status::unauthenticated(msg)
            }
            AirportError::InvalidConfig(msg) | AirportError::InvalidParameters(msg) => {
                tonic::Status::invalid_argument(msg)
            }
            AirportError::NotSupported(msg) => tonic::Status::unimplemented(msg),
            AirportError::NullRowId => {
                tonic::Status::invalid_argument("null rowid value not allowed")
            }
            _ => tonic::Status::internal(self.to_string()),
        }
    }
}

impl From<AirportError> for tonic::Status {
    fn from(e: AirportError) -> Self {
        e.to_status()
    }
}

impl From<rmp_serde::decode::Error> for AirportError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        AirportError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for AirportError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        AirportError::Serialization(e.to_string())
    }
}

impl From<serde_json::Error> for AirportError {
    fn from(e: serde_json::Error) -> Self {
        AirportError::Serialization(e.to_string())
    }
}

impl From<prost::EncodeError> for AirportError {
    fn from(e: prost::EncodeError) -> Self {
        AirportError::Serialization(e.to_string())
    }
}

impl From<prost::DecodeError> for AirportError {
    fn from(e: prost::DecodeError) -> Self {
        AirportError::Serialization(e.to_string())
    }
}
