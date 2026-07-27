//! How a node opens a link to another node.
//!
//! Every outbound node-to-node link — the per-follower replication connection, the
//! gossip anti-entropy exchange, and the SWIM ping-req — goes through a
//! [`PeerDialer`]. It holds the three things a dial needs and nothing else: the
//! cluster secret the link presents once it is open, the client-side TLS config
//! that authenticates the *acceptor* before that secret is written, and whether
//! this deployment tolerates a plaintext peer at all.
//!
//! **The transport is per member, declared by its advertise address.** A
//! `wss://host:port` member terminates TLS; a `ws://host:port` member, or a bare
//! `host:port`, does not. Nothing else can decide it: whether *this* node
//! terminates TLS says nothing about whether the member it is dialing does, and the
//! advertise address is the only per-member datum the cluster already agrees on and
//! already disseminates through gossip. A cluster part-way through a TLS rollout
//! therefore holds both kinds at once and every node still dials every other
//! correctly — see [`ServeConfig::require_peer_tls`] for declaring the rollout over.
//!
//! Server-authenticated TLS is what makes the peer credential safe to send. The
//! cluster secret is a *bearer* credential, so on a plaintext link a passive
//! listener captures it and replays it into write access to every room the node
//! replicates, and an active squatter answering a member's advertise address is
//! simply handed it. Verifying the acceptor's certificate against configured trust
//! anchors happens *before* the first frame is written, so neither works: the
//! handshake fails and the dialer writes nothing.
//!
//! [`ServeConfig::require_peer_tls`]: crate::runtime::ServeConfig::require_peer_tls

use std::sync::Arc;

use tokio_rustls::rustls::ClientConfig;
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};

/// The stream a peer link runs over: a WebSocket on a TCP socket that is
/// plaintext or rustls-wrapped depending on the member's advertised transport.
pub type PeerStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// The longest a peer dial may spend opening — connect, TLS handshake and
/// WebSocket upgrade together. Generous for a cross-datacenter handshake, and the
/// same bound the accept loop puts on the handshake it terminates; without it a far
/// end that accepts the socket and then goes silent wedges the dialing task, and
/// the link it owns is never redialed.
pub const PEER_DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The transport a member's advertise address declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerTransport {
    /// `ws://host:port`, or a bare `host:port`.
    Plain,
    /// `wss://host:port` — the member terminates TLS.
    Tls,
}

impl PeerTransport {
    /// The URL scheme this transport dials under.
    pub fn scheme(self) -> &'static str {
        match self {
            PeerTransport::Plain => "ws",
            PeerTransport::Tls => "wss",
        }
    }
}

/// A member's advertise address resolved into the URL a dial uses and the
/// transport it speaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoint {
    pub url: String,
    pub transport: PeerTransport,
}

impl PeerEndpoint {
    /// Resolve a member's advertise address. A `ws://` or `wss://` prefix (either
    /// case) declares the transport and the rest is the authority; without one the
    /// address is a bare authority and the transport is plaintext. Any other
    /// scheme is a configuration error rather than part of a hostname — folding
    /// `http://host` into an authority would dial `ws://http://host/` and fail
    /// forever.
    pub fn parse(addr: &str) -> Result<Self, BadPeerAddress> {
        let addr = addr.trim();
        let (transport, authority) = match addr.split_once("://") {
            Some((scheme, rest)) => match scheme.to_ascii_lowercase().as_str() {
                "ws" => (PeerTransport::Plain, rest),
                "wss" => (PeerTransport::Tls, rest),
                _ => return Err(BadPeerAddress::UnknownScheme(scheme.to_string())),
            },
            None => (PeerTransport::Plain, addr),
        };
        if authority.is_empty() {
            return Err(BadPeerAddress::Empty);
        }
        let scheme = transport.scheme();
        // A bare authority is dialed at the root path, as every advertise address
        // in a cluster is; one that already carries a path keeps it verbatim.
        let url = match authority.contains('/') {
            true => format!("{scheme}://{authority}"),
            false => format!("{scheme}://{authority}/"),
        };
        Ok(Self { url, transport })
    }

    /// Whether this endpoint terminates TLS.
    pub fn is_tls(&self) -> bool {
        self.transport == PeerTransport::Tls
    }
}

/// An advertise address no dial can be built from — a configuration error,
/// surfaced at startup for a configured member and per dial for a gossiped one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadPeerAddress {
    /// The address, or its authority after a scheme, is empty.
    Empty,
    /// The address carries a scheme that is not `ws` or `wss`.
    UnknownScheme(String),
}

impl std::fmt::Display for BadPeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BadPeerAddress::Empty => write!(f, "the address is empty"),
            BadPeerAddress::UnknownScheme(scheme) => write!(
                f,
                "`{scheme}://` is not a peer transport — use `wss://host:port`, \
                 `ws://host:port`, or a bare `host:port`"
            ),
        }
    }
}

impl std::error::Error for BadPeerAddress {}

/// Why a peer dial did not produce a link.
#[derive(Debug)]
pub enum DialError {
    /// The member's advertise address does not resolve to an endpoint.
    Address(BadPeerAddress),
    /// The member advertises plaintext and this deployment requires TLS on every
    /// peer link.
    PlaintextRefused,
    /// The member advertises TLS and this node has no trust anchors to
    /// authenticate it with, so it will not hand the cluster secret over.
    NoTrustAnchors,
    /// The dial, TLS handshake, or WebSocket upgrade failed — the peer is down,
    /// unreachable, presenting a cert that does not chain to the configured
    /// anchors, or refusing this node's client cert.
    Unreachable,
}

impl std::fmt::Display for DialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DialError::Address(e) => write!(f, "{e}"),
            DialError::PlaintextRefused => write!(
                f,
                "the member advertises plaintext and this node requires TLS on every peer link"
            ),
            DialError::NoTrustAnchors => write!(
                f,
                "the member advertises TLS and this node has no peer trust anchors configured"
            ),
            DialError::Unreachable => write!(
                f,
                "the peer did not complete a link — unreachable, or its certificate does not \
                 chain to this node's peer trust anchors"
            ),
        }
    }
}

impl std::error::Error for DialError {}

/// Everything an outbound node-to-node link needs: the cluster secret it presents
/// once open, the trust anchors (and optional client identity) that authenticate
/// the acceptor first, and whether a plaintext member may be dialed at all.
///
/// One dialer is shared by every peer link a node opens, so the transport policy
/// is decided in exactly one place.
#[derive(Clone)]
pub struct PeerDialer {
    secret: Arc<[u8]>,
    tls: Option<Arc<ClientConfig>>,
    require_tls: bool,
}

impl PeerDialer {
    pub fn new(secret: Arc<[u8]>, tls: Option<Arc<ClientConfig>>, require_tls: bool) -> Self {
        Self {
            secret,
            tls,
            require_tls,
        }
    }

    /// The cluster secret a link presents in its `PeerAuth` once open.
    pub fn secret(&self) -> &[u8] {
        &self.secret
    }

    /// The endpoint this dialer would dial `addr` at, or why it will not — the
    /// pure half of a dial, so a configured member's transport is validated at
    /// startup rather than discovered one failed round at a time.
    pub fn endpoint(&self, addr: &str) -> Result<PeerEndpoint, DialError> {
        let endpoint = PeerEndpoint::parse(addr).map_err(DialError::Address)?;
        match endpoint.transport {
            PeerTransport::Plain if self.require_tls => Err(DialError::PlaintextRefused),
            PeerTransport::Tls if self.tls.is_none() => Err(DialError::NoTrustAnchors),
            _ => Ok(endpoint),
        }
    }

    /// Dial `addr` and return the open WebSocket. A TLS member's certificate is
    /// verified against this dialer's trust anchors during the handshake, so a
    /// link that returns here has already authenticated the far end — nothing of
    /// the cluster secret has been written yet.
    ///
    /// The whole opening is bounded by [`PEER_DIAL_TIMEOUT`]: a far end that
    /// accepts the socket and then says nothing would otherwise stall the TLS or
    /// WebSocket handshake indefinitely, and the caller that owns a follower's link
    /// would never redial it — the same pre-authentication blocking point the
    /// accept loop bounds from its side.
    pub async fn connect(&self, addr: &str) -> Result<PeerStream, DialError> {
        let endpoint = self.endpoint(addr)?;
        let connector = match endpoint.transport {
            PeerTransport::Plain => None,
            PeerTransport::Tls => match self.tls.clone() {
                Some(tls) => Some(Connector::Rustls(tls)),
                None => return Err(DialError::NoTrustAnchors),
            },
        };
        let opening = connect_async_tls_with_config(&endpoint.url, None, false, connector);
        match tokio::time::timeout(PEER_DIAL_TIMEOUT, opening).await {
            Ok(Ok((ws, _))) => Ok(ws),
            Ok(Err(_)) | Err(_) => Err(DialError::Unreachable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_dialer() -> PeerDialer {
        PeerDialer::new(Arc::from(&b"secret"[..]), None, false)
    }

    #[test]
    fn a_bare_address_dials_plaintext_at_the_root_path() {
        let endpoint = PeerEndpoint::parse("10.0.0.1:9000").unwrap();
        assert_eq!(endpoint.transport, PeerTransport::Plain);
        assert_eq!(endpoint.url, "ws://10.0.0.1:9000/");
    }

    #[test]
    fn an_explicit_ws_scheme_dials_plaintext() {
        let endpoint = PeerEndpoint::parse("ws://10.0.0.1:9000").unwrap();
        assert_eq!(endpoint.transport, PeerTransport::Plain);
        assert_eq!(endpoint.url, "ws://10.0.0.1:9000/");
    }

    #[test]
    fn a_wss_scheme_dials_tls() {
        let endpoint = PeerEndpoint::parse("wss://10.0.0.1:9000").unwrap();
        assert_eq!(endpoint.transport, PeerTransport::Tls);
        assert_eq!(endpoint.url, "wss://10.0.0.1:9000/");
    }

    #[test]
    fn a_scheme_is_matched_without_regard_to_case() {
        assert!(PeerEndpoint::parse("WSS://10.0.0.1:9000").unwrap().is_tls());
        assert!(!PeerEndpoint::parse("Ws://10.0.0.1:9000").unwrap().is_tls());
    }

    #[test]
    fn an_address_is_trimmed_before_it_is_parsed() {
        // The comma-separated peer list pads its entries, and a padded address must
        // resolve to the same endpoint as the id it derives.
        assert_eq!(
            PeerEndpoint::parse("  wss://10.0.0.1:9000  ").unwrap().url,
            "wss://10.0.0.1:9000/"
        );
    }

    #[test]
    fn an_address_that_carries_a_path_keeps_it() {
        assert_eq!(
            PeerEndpoint::parse("wss://10.0.0.1:9000/sync").unwrap().url,
            "wss://10.0.0.1:9000/sync"
        );
    }

    #[test]
    fn an_unknown_scheme_is_refused_rather_than_folded_into_a_hostname() {
        assert_eq!(
            PeerEndpoint::parse("http://10.0.0.1:9000"),
            Err(BadPeerAddress::UnknownScheme("http".to_string()))
        );
    }

    #[test]
    fn an_empty_address_is_refused() {
        assert_eq!(PeerEndpoint::parse(""), Err(BadPeerAddress::Empty));
        assert_eq!(PeerEndpoint::parse("   "), Err(BadPeerAddress::Empty));
        assert_eq!(PeerEndpoint::parse("wss://"), Err(BadPeerAddress::Empty));
    }

    #[test]
    fn a_dialer_without_trust_anchors_refuses_a_tls_member() {
        // It has nothing to authenticate the acceptor with, so dialing would mean
        // writing the bearer secret to an unverified far end.
        assert!(matches!(
            plain_dialer().endpoint("wss://10.0.0.1:9000"),
            Err(DialError::NoTrustAnchors)
        ));
    }

    #[test]
    fn a_dialer_without_trust_anchors_still_dials_a_plaintext_member() {
        assert!(plain_dialer().endpoint("10.0.0.1:9000").is_ok());
    }

    #[test]
    fn a_dialer_that_requires_tls_refuses_a_plaintext_member() {
        // The end of a rollout: a member still advertising plaintext is not dialed
        // at all rather than handed the secret in the clear.
        let dialer = PeerDialer::new(Arc::from(&b"secret"[..]), None, true);
        assert!(matches!(
            dialer.endpoint("10.0.0.1:9000"),
            Err(DialError::PlaintextRefused)
        ));
        assert!(matches!(
            dialer.endpoint("ws://10.0.0.1:9000"),
            Err(DialError::PlaintextRefused)
        ));
    }

    #[test]
    fn a_bad_address_surfaces_as_a_dial_error() {
        assert!(matches!(
            plain_dialer().endpoint("http://10.0.0.1:9000"),
            Err(DialError::Address(BadPeerAddress::UnknownScheme(_)))
        ));
    }

    #[test]
    fn a_dialer_carries_the_cluster_secret_the_link_presents() {
        assert_eq!(plain_dialer().secret(), b"secret");
    }
}
