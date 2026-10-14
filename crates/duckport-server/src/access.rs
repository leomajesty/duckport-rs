//! Peer zone and bearer-token gate for cross-network access.
//!
//! Loopback skips authentication. LAN and extra trusted CIDRs may write.
//! Public addresses are rejected entirely when `DUCKPORT_AUTH_TOKENS` is empty.
//! With tokens configured, public peers may read after a bearer check.
//! A missing peer address is treated as public.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use ipnet::IpNet;
use sha2::{Digest, Sha256};
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::config::Config;

/// Where the TCP peer sits. Write permission and DoGet limits follow this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerZone {
    /// `127.0.0.0/8` or `::1`, including IPv4-mapped loopback.
    Local,
    /// RFC1918, IPv6 ULA (`fc00::/7`), or `DUCKPORT_TRUSTED_CIDRS`.
    Lan,
    /// Everything else, including CGNAT `100.64.0.0/10` and a missing address.
    Public,
}

/// Byte and timeout ceiling applied only to public DoGet.
///
/// A zero field disables that one check. `None` at the call site means the
/// peer is local or LAN and is not capped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoGetLimits {
    pub max_bytes: u64,
    pub timeout: Duration,
}

/// Identity established for this RPC. Inserted into request extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessContext {
    pub zone: PeerZone,
    pub identity: Option<String>,
    pub doget_limits: Option<DoGetLimits>,
}

impl AccessContext {
    pub fn public_anonymous() -> Self {
        Self {
            zone: PeerZone::Public,
            identity: None,
            doget_limits: None,
        }
    }

    pub fn ensure_write(&self) -> Result<(), Status> {
        match self.zone {
            PeerZone::Local | PeerZone::Lan => Ok(()),
            PeerZone::Public => Err(Status::permission_denied(
                "write operations are allowed only from loopback and LAN",
            )),
        }
    }

    /// Public callers may only ping. Other DoActions are not part of the
    /// public read surface (`GetFlightInfo`, `DoGet`, `List*`).
    pub fn ensure_action(&self, action_type: &str) -> Result<(), Status> {
        if self.zone != PeerZone::Public || action_type == "duckport.ping" {
            return Ok(());
        }
        Err(Status::permission_denied(
            "this action is allowed only from loopback and LAN",
        ))
    }
}

pub fn from_request<T>(req: &tonic::Request<T>) -> AccessContext {
    req.extensions()
        .get::<AccessContext>()
        .cloned()
        .unwrap_or_else(|| {
            tracing::warn!("missing AccessContext; treating peer as public");
            AccessContext::public_anonymous()
        })
}

#[derive(Clone)]
struct TokenEntry {
    name: String,
    digest: [u8; 32],
}

/// Parsed tokens and network zones. Cheap to clone via `Arc` at the call site.
#[derive(Clone)]
pub struct AccessPolicy {
    tokens: Vec<TokenEntry>,
    trusted: Vec<IpNet>,
    public_limits: Option<DoGetLimits>,
}

impl AccessPolicy {
    pub fn from_config(cfg: &Config) -> anyhow::Result<Self> {
        Ok(Self {
            tokens: parse_tokens(&cfg.auth_tokens)?,
            trusted: cfg.trusted_cidrs.clone(),
            public_limits: public_limits(cfg.doget_max_bytes, cfg.doget_timeout),
        })
    }

    pub fn auth_enabled(&self) -> bool {
        !self.tokens.is_empty()
    }

    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

    pub fn authorize(
        &self,
        peer: Option<SocketAddr>,
        metadata: &MetadataMap,
    ) -> Result<AccessContext, Status> {
        let zone = classify(peer.map(|addr| addr.ip()), &self.trusted);
        if zone == PeerZone::Public && self.tokens.is_empty() {
            return Err(Status::permission_denied(
                "public access is disabled until DUCKPORT_AUTH_TOKENS is set",
            ));
        }
        let identity = if zone == PeerZone::Local || self.tokens.is_empty() {
            None
        } else {
            Some(self.require_token(metadata)?)
        };
        let doget_limits = if zone == PeerZone::Public {
            self.public_limits
        } else {
            None
        };
        Ok(AccessContext {
            zone,
            identity,
            doget_limits,
        })
    }

    fn require_token(&self, metadata: &MetadataMap) -> Result<String, Status> {
        let header = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        let token = header
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        if token.is_empty() {
            return Err(Status::unauthenticated("missing bearer token"));
        }
        match self.match_token(token) {
            Some(name) => Ok(name.to_string()),
            None => Err(Status::unauthenticated("invalid bearer token")),
        }
    }

    /// Hash the presented token once, then compare every configured digest.
    /// The loop does not return early, so a match in the middle does not
    /// shorten the comparison.
    fn match_token(&self, token: &str) -> Option<&str> {
        let digest = sha256(token.as_bytes());
        let mut found: Option<&str> = None;
        for entry in &self.tokens {
            if ct_eq(&digest, &entry.digest) {
                found = Some(entry.name.as_str());
            }
        }
        found
    }
}

pub fn classify(ip: Option<IpAddr>, trusted: &[IpNet]) -> PeerZone {
    let Some(ip) = ip else {
        return PeerZone::Public;
    };
    let ip = match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    };
    match ip {
        IpAddr::V4(v4) if v4.is_loopback() => PeerZone::Local,
        IpAddr::V6(v6) if v6.is_loopback() => PeerZone::Local,
        IpAddr::V4(v4) if is_rfc1918(v4) => PeerZone::Lan,
        IpAddr::V6(v6) if is_ula(v6) => PeerZone::Lan,
        other if trusted.iter().any(|net| net.contains(&other)) => PeerZone::Lan,
        _ => PeerZone::Public,
    }
}

fn public_limits(max_bytes: u64, timeout: Duration) -> Option<DoGetLimits> {
    if max_bytes == 0 && timeout.is_zero() {
        None
    } else {
        Some(DoGetLimits { max_bytes, timeout })
    }
}

fn is_rfc1918(ip: Ipv4Addr) -> bool {
    match ip.octets() {
        [10, ..] => true,
        [172, second, ..] if (16..32).contains(&second) => true,
        [192, 168, ..] => true,
        _ => false,
    }
}

fn is_ula(ip: Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xfe00) == 0xfc00
}

fn parse_tokens(raw: &str) -> anyhow::Result<Vec<TokenEntry>> {
    let mut out = Vec::new();
    if raw.trim().is_empty() {
        return Ok(out);
    }
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, token) = match part.split_once(':') {
            Some((name, token)) => {
                if name.is_empty() || token.is_empty() {
                    anyhow::bail!(
                        "DUCKPORT_AUTH_TOKENS entry '{part}' must be name:token or a bare token"
                    );
                }
                (name.to_string(), token)
            }
            None => ("client".to_string(), part),
        };
        out.push(TokenEntry {
            name,
            digest: sha256(token.as_bytes()),
        });
    }
    Ok(out)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn ct_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

pub fn parse_cidrs(raw: &str) -> anyhow::Result<Vec<IpNet>> {
    let mut out = Vec::new();
    if raw.trim().is_empty() {
        return Ok(out);
    }
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let net: IpNet = part
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid DUCKPORT_TRUSTED_CIDRS entry '{part}': {e}"))?;
        out.push(net);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tonic::metadata::MetadataValue;

    fn policy(tokens: &str, cidrs: &str, bytes: u64, secs: u64) -> AccessPolicy {
        AccessPolicy {
            tokens: parse_tokens(tokens).unwrap(),
            trusted: parse_cidrs(cidrs).unwrap(),
            public_limits: public_limits(bytes, Duration::from_secs(secs)),
        }
    }

    fn meta(token: Option<&str>) -> MetadataMap {
        let mut map = MetadataMap::new();
        if let Some(token) = token {
            let value = format!("Bearer {token}");
            map.insert("authorization", MetadataValue::try_from(value).unwrap());
        }
        map
    }

    fn ip(addr: &str) -> Option<IpAddr> {
        Some(addr.parse().unwrap())
    }

    #[test]
    fn classifies_loopback_lan_and_public() {
        let trusted = parse_cidrs("203.0.113.10/32").unwrap();
        assert_eq!(classify(ip("127.0.0.1"), &trusted), PeerZone::Local);
        assert_eq!(classify(ip("127.1.2.3"), &trusted), PeerZone::Local);
        assert_eq!(classify(ip("::1"), &trusted), PeerZone::Local);
        assert_eq!(classify(ip("::ffff:127.0.0.1"), &trusted), PeerZone::Local);
        assert_eq!(classify(ip("10.1.2.3"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("172.16.0.1"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("172.31.255.255"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("172.15.0.1"), &trusted), PeerZone::Public);
        assert_eq!(classify(ip("172.32.0.1"), &trusted), PeerZone::Public);
        assert_eq!(classify(ip("192.168.1.1"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("100.64.0.1"), &trusted), PeerZone::Public);
        assert_eq!(classify(ip("8.8.8.8"), &trusted), PeerZone::Public);
        assert_eq!(classify(ip("fc00::1"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("fd12::1"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("fe80::1"), &trusted), PeerZone::Public);
        assert_eq!(classify(ip("203.0.113.10"), &trusted), PeerZone::Lan);
        assert_eq!(classify(ip("203.0.113.11"), &trusted), PeerZone::Public);
        assert_eq!(classify(None, &trusted), PeerZone::Public);
    }

    #[test]
    fn trusted_cidr_does_not_become_loopback() {
        let trusted = parse_cidrs("8.8.8.8/32").unwrap();
        assert_eq!(classify(ip("8.8.8.8"), &trusted), PeerZone::Lan);
        assert_ne!(classify(ip("8.8.8.8"), &trusted), PeerZone::Local);
    }

    #[test]
    fn local_skips_auth_even_when_tokens_are_configured() {
        let policy = policy("research:secret", "", 64, 30);
        let peer: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        let ctx = policy.authorize(Some(peer), &meta(None)).unwrap();
        assert_eq!(ctx.zone, PeerZone::Local);
        assert!(ctx.identity.is_none());
        assert!(ctx.doget_limits.is_none());
        assert!(ctx.ensure_write().is_ok());
    }

    #[test]
    fn lan_requires_token_and_may_write() {
        let policy = policy("ingestor:s3cret", "", 64, 30);
        let peer: SocketAddr = "10.1.0.5:50051".parse().unwrap();
        let missing = policy.authorize(Some(peer), &meta(None)).unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let ctx = policy.authorize(Some(peer), &meta(Some("s3cret"))).unwrap();
        assert_eq!(ctx.zone, PeerZone::Lan);
        assert_eq!(ctx.identity.as_deref(), Some("ingestor"));
        assert!(ctx.doget_limits.is_none());
        assert!(ctx.ensure_write().is_ok());
    }

    #[test]
    fn public_read_is_capped_and_cannot_write() {
        let policy = policy("research:secret", "", 1024, 30);
        let peer: SocketAddr = "203.0.113.8:443".parse().unwrap();
        let ctx = policy.authorize(Some(peer), &meta(Some("secret"))).unwrap();
        assert_eq!(ctx.zone, PeerZone::Public);
        assert_eq!(ctx.identity.as_deref(), Some("research"));
        let limits = ctx.doget_limits.unwrap();
        assert_eq!(limits.max_bytes, 1024);
        assert_eq!(limits.timeout, Duration::from_secs(30));
        assert_eq!(
            ctx.ensure_write().unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert!(ctx.ensure_action("duckport.ping").is_ok());
        assert_eq!(
            ctx.ensure_action("duckport.execute").unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn unnamed_token_uses_client_identity() {
        let policy = policy("only-token", "", 0, 0);
        let peer: SocketAddr = "1.2.3.4:1".parse().unwrap();
        let ctx = policy
            .authorize(Some(peer), &meta(Some("only-token")))
            .unwrap();
        assert_eq!(ctx.identity.as_deref(), Some("client"));
        assert!(ctx.doget_limits.is_none());
    }

    #[test]
    fn rejects_wrong_token_after_checking_every_entry() {
        let policy = policy("a:one,b:two", "", 1, 1);
        let peer: SocketAddr = "8.8.8.8:1".parse().unwrap();
        let err = policy
            .authorize(Some(peer), &meta(Some("nope")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        let ctx = policy.authorize(Some(peer), &meta(Some("two"))).unwrap();
        assert_eq!(ctx.identity.as_deref(), Some("b"));
    }

    #[test]
    fn empty_tokens_reject_public_and_allow_lan() {
        let policy = policy("", "", 32, 5);
        let public_peer: SocketAddr = "8.8.8.8:9".parse().unwrap();
        let err = policy.authorize(Some(public_peer), &meta(None)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        let lan_peer: SocketAddr = "10.1.0.5:9".parse().unwrap();
        let ctx = policy.authorize(Some(lan_peer), &meta(None)).unwrap();
        assert_eq!(ctx.zone, PeerZone::Lan);
        assert!(ctx.identity.is_none());
        assert!(ctx.doget_limits.is_none());
        assert!(ctx.ensure_write().is_ok());
    }

    #[test]
    fn invalid_token_entry_is_rejected() {
        assert!(parse_tokens(":secret").is_err());
        assert!(parse_tokens("name:").is_err());
        assert!(parse_cidrs("not-a-cidr").is_err());
    }
}
