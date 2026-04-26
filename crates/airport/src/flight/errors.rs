use crate::error::AirportError;
use tonic::{Code, Status};

/// Converts AirportError to tonic Status.
pub fn to_status(err: AirportError) -> Status {
    err.to_status()
}

/// Creates a not_found Status with context.
pub fn not_found(msg: impl std::fmt::Display) -> Status {
    Status::new(Code::NotFound, msg.to_string())
}

/// Creates an internal Status with context.
pub fn internal(msg: impl std::fmt::Display) -> Status {
    Status::new(Code::Internal, msg.to_string())
}

/// Creates an invalid_argument Status with context.
pub fn invalid_argument(msg: impl std::fmt::Display) -> Status {
    Status::new(Code::InvalidArgument, msg.to_string())
}

/// Creates an unimplemented Status.
pub fn unimplemented(action: &str) -> Status {
    Status::new(Code::Unimplemented, format!("unknown action type: {}", action))
}

/// Creates an unauthenticated Status.
pub fn unauthenticated(msg: impl std::fmt::Display) -> Status {
    Status::new(Code::Unauthenticated, msg.to_string())
}
