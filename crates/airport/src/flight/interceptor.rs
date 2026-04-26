use crate::auth::{Authenticator, AuthenticatorRef};
use crate::error::AirportError;
use crate::flight::context::RequestContext;
use tonic::Status;

/// Performs authentication on a tonic request, enriching it with identity.
/// Returns the enriched RequestContext or an unauthenticated Status error.
pub async fn authenticate_request<A: Authenticator>(
    auth: &A,
    ctx: &mut RequestContext,
) -> Result<(), Status> {
    if ctx.token.is_empty() {
        return Err(Status::unauthenticated("missing bearer token"));
    }
    match auth.authenticate(&ctx.token).await {
        Ok(identity) => {
            ctx.identity = Some(identity);
            Ok(())
        }
        Err(AirportError::Unauthorized(msg)) => Err(Status::unauthenticated(msg)),
        Err(e) => Err(Status::internal(e.to_string())),
    }
}

/// Authenticates a tonic Request and extracts the RequestContext.
pub async fn authenticate<A: Authenticator>(
    auth: Option<&A>,
    request_metadata: &tonic::metadata::MetadataMap,
) -> Result<RequestContext, Status> {
    let mut ctx = RequestContext::from_metadata(request_metadata);

    if let Some(auth) = auth {
        authenticate_request(auth, &mut ctx).await?;
    }

    Ok(ctx)
}

/// Authenticates using an optional AuthenticatorRef.
pub async fn maybe_authenticate(
    auth: Option<&AuthenticatorRef>,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<RequestContext, Status> {
    let mut ctx = RequestContext::from_metadata(metadata);

    if let Some(auth) = auth {
        if !ctx.token.is_empty() {
            match auth.authenticate(&ctx.token).await {
                Ok(identity) => {
                    ctx.identity = Some(identity);
                }
                Err(AirportError::Unauthorized(msg)) => {
                    return Err(Status::unauthenticated(msg));
                }
                Err(e) => {
                    return Err(Status::internal(e.to_string()));
                }
            }
        } else {
            // Auth is configured but no token provided
            return Err(Status::unauthenticated("missing bearer token"));
        }
    }

    Ok(ctx)
}
