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
        // An advertise address is an authority and nothing else. A `@`, a path, a query
        // or a fragment would make the URL the dialer builds resolve somewhere other
        // than the host read out of the same string: `wss://a.example:1@b.example:9000`
        // reads as host `a.example` here and connects to `b.example`, so a certificate
        // for `a.example` would bind an id that every peer verifies by dialing *`b`*.
        // A path is refused for the second half of the same reason — it is a free
        // alias, so one endpoint would answer under unboundedly many node ids, each
        // separately placed and none of which ever speaks.
        // Only the characters a host and a port are written with. `@`, `/`, `?` and
        // `#` are the ones that change where a dial lands or hand one endpoint a
        // second id; the rest of the refusal is what keeps an unreachable address from
        // being *retried forever* — a control character or a space builds a URL the
        // dialer rejects at send time, which reads as "unreachable" and is redialed on
        // the fast cadence rather than classified a permanent address error.
        if !authority
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b':' | b'[' | b']'))
        {
            return Err(BadPeerAddress::NotAnAuthority);
        }
        // An authority that names no host — an IPv6 literal left unbracketed, or one
        // opening with its port separator — is dialable by nobody and bindable to no
        // certificate. Refused here so every consumer inherits it: a configured member
        // fails startup, a gossiped one is classified a *permanent* dial failure rather
        // than redialed forever, and the host is the one part of an address this crate
        // reads twice.
        if host_of(authority).is_none() {
            return Err(BadPeerAddress::NoHost);
        }
        let scheme = transport.scheme();
        // Every advertise address is dialed at the root path.
        let url = format!("{scheme}://{authority}/");
        Ok(Self { url, transport })
    }

    /// Whether this endpoint terminates TLS.
    pub fn is_tls(&self) -> bool {
        self.transport == PeerTransport::Tls
    }
}

/// The host an advertise address names, with its scheme, port and path stripped —
/// the part of a member's identity a certificate can carry. A bracketed IPv6
/// literal keeps its brackets off but its colons intact, so `[::1]:9000` is `::1`
/// rather than everything up to the last colon. `None` when the address resolves to
/// no endpoint, or names no host at all.
///
/// This is the whole of the node-id↔certificate-subject mapping: a member's
/// advertise address already agrees cluster-wide and already rides gossip, and its
/// host is exactly what a TLS certificate legitimately names — the same fact the
/// dialer verifies in the other direction when it authenticates the acceptor.
pub fn member_host(addr: &[u8]) -> Option<String> {
    let addr = std::str::from_utf8(addr).ok()?;
    let endpoint = PeerEndpoint::parse(addr).ok()?;
    let authority = endpoint
        .url
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&endpoint.url);
    host_of(authority).map(|host| host.to_ascii_lowercase())
}

/// The one spelling of a member's advertise address: no redundant `ws://`, the host
/// lowercased with its root label dropped, and an IP literal reserialized in its
/// canonical form. `None` when the address is not one a peer can dial.
///
/// A node id **is** an advertise address and placement hashes it, so two spellings of
/// one endpoint are two positions in the ring that one node answers for — each
/// verified *truthfully* by every peer that dials it, each adopted, and none of which
/// ever speaks or acks. A room whose replica set filled up with them would wait on
/// acks that never come. One spelling per endpoint is what keeps the ring's positions
/// and the cluster's members the same set.
pub fn canonical_member_addr(addr: &str) -> Option<String> {
    let endpoint = PeerEndpoint::parse(addr).ok()?;
    let authority = endpoint.url.split_once("://")?.1.trim_end_matches('/');
    let host = host_of(authority)?;
    // Whatever follows the host — its port separator and port, or nothing.
    let after_host = match authority.strip_prefix('[') {
        Some(after) => after.split_once(']').map(|(_, rest)| rest)?,
        None => authority.find(':').map_or("", |at| &authority[at..]),
    };
    // The port is a *number*, so it is canonicalized as one: `:9000`, `:09000` and
    // `:+9000` are one port on one listener, and leaving them as text would give one
    // endpoint unboundedly many node ids — separately placed, and only one of them
    // ever answering. An absent port and an empty one are the same absence. A port
    // that is no port has no canonical form and the address is refused.
    let port = match after_host.strip_prefix(':') {
        None => None,
        Some("") => None,
        Some(digits) => Some(digits.parse::<u16>().ok()?),
    };
    let host = normalized(host);
    let host = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(v6)) => format!("[{v6}]"),
        Ok(ip) => ip.to_string(),
        Err(_) => host,
    };
    let scheme = match endpoint.transport {
        PeerTransport::Tls => "wss://",
        PeerTransport::Plain => "",
    };
    Some(match port {
        Some(port) => format!("{scheme}{host}:{port}"),
        None => format!("{scheme}{host}"),
    })
}

/// The **trust unit** an advertise address belongs to: its host reduced to the one
/// form the certificate binding compares. `None` when the address names no host.
///
/// A host is what a certificate names, so a host is the unit that decides how many
/// *independent* parties have vouched for a member. That count is only meaningful if
/// two spellings of one host reduce to one unit — and the binding
/// ([`cert_names_member`]) already treats them as one: it lowercases, drops the root
/// label, and compares IP literals as addresses rather than as text. Reading the host
/// as raw text here while the binding read it semantically would let one machine
/// present as several: `evil.example` beside `evil.example.`, or an IPv6 literal
/// beside its expanded form, are one certificate and would have been two vouchers.
pub fn member_trust_unit(addr: &[u8]) -> Option<String> {
    let host = normalized(&member_host(addr)?);
    Some(match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.to_string(),
        Err(_) => host,
    })
}

/// The host an authority names, with its port and path stripped. A bracketed IPv6
/// literal keeps its brackets off but its colons intact, so `[::1]:9000` is `::1`
/// rather than everything up to the last colon. `None` when it names none.
fn host_of(authority: &str) -> Option<&str> {
    let authority = authority.split('/').next().unwrap_or(authority);
    let host = match authority.strip_prefix('[') {
        Some(rest) => rest.split(']').next()?,
        None => authority.split(':').next()?,
    };
    (!host.is_empty()).then_some(host)
}

/// Whether a verified peer certificate's subject `name` establishes the connection
/// as the member at advertise address `addr` — the binding from a certificate the
/// cluster's CA issued to a place in the placement set.
///
/// The rule is one comparison: the certificate names the member's *host*, matched
/// without regard to case as DNS names are. The port is deliberately not part of it
/// — no certificate can carry one — so nodes sharing a host are one trust unit and
/// may speak for each other.
///
/// An IP literal is compared as an *address*, not as a string, since one address has
/// many spellings: a certificate's IP SAN arrives canonicalized from its octets while
/// an advertise address holds whatever the operator wrote, and `[2001:0db8::0:1]` and
/// `2001:db8::1` are the same member.
pub fn cert_names_member(name: &[u8], addr: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(name) else {
        return false;
    };
    let name = normalized(name);
    if name.is_empty() {
        return false;
    }
    let Some(host) = member_host(addr) else {
        return false;
    };
    let host = normalized(&host);
    match (
        host.parse::<std::net::IpAddr>(),
        name.parse::<std::net::IpAddr>(),
    ) {
        (Ok(host), Ok(name)) => host == name,
        _ => host == name,
    }
}

/// A certificate name or advertise host reduced to the one form the binding compares:
/// trimmed, lowercased as DNS names are, and without its root label — `node-a.` and
/// `node-a` name one host, and a certificate and an advertise address need not agree on
/// which spelling to use.
fn normalized(name: &str) -> String {
    let name = name.trim();
    name.strip_suffix('.').unwrap_or(name).to_ascii_lowercase()
}

/// Whether `name` is an IP address rather than a host name, judged after the same
/// normalization the binding applies. The reader that decides which certificate SANs
/// name a host consults this, so it and [`cert_names_member`] can never disagree about
/// what an address is: a `dNSName` of `10.0.0.6.` must not pass the reader as a name
/// and then match an IP-advertised member as an address.
pub(crate) fn is_ip_literal(name: &str) -> bool {
    normalized(name).parse::<std::net::IpAddr>().is_ok()
}

/// An advertise address no dial can be built from — a configuration error,
/// surfaced at startup for a configured member and per dial for a gossiped one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadPeerAddress {
    /// The address, or its authority after a scheme, is empty.
    Empty,
    /// The authority names no host — an unbracketed IPv6 literal, or an address
    /// opening with its port separator.
    NoHost,
    /// The address carries more than an authority — userinfo, a path, a query or a
    /// fragment. Each is a way for the host a reader takes from the string to differ
    /// from the host a dialer connects to, or for one endpoint to answer under many
    /// node ids.
    NotAnAuthority,
    /// The address carries a scheme that is not `ws` or `wss`.
    UnknownScheme(String),
}

impl std::fmt::Display for BadPeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BadPeerAddress::Empty => write!(f, "the address is empty"),
            BadPeerAddress::NoHost => write!(
                f,
                "the address names no host, so no peer can dial it — use `host:port`, \
                 bracketing an IPv6 literal"
            ),
            BadPeerAddress::NotAnAuthority => write!(
                f,
                "the address carries more than a host and port — use `host:port`, with \
                 no user, path, query or fragment"
            ),
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

impl DialError {
    /// Whether redialing can never succeed without a configuration change. The
    /// three configuration arms are decided before a socket is opened, so retrying
    /// one costs nothing but produces nothing either — a caller redials it on the
    /// slow cadence rather than the one it uses for a peer that is merely down and
    /// may come back at any moment.
    pub fn is_permanent(&self) -> bool {
        match self {
            DialError::Address(_) | DialError::PlaintextRefused | DialError::NoTrustAnchors => true,
            DialError::Unreachable => false,
        }
    }
}

impl std::error::Error for DialError {}

/// Everything an outbound node-to-node link needs: the node id it claims and the
/// cluster secret it presents once open, the trust anchors (and optional client
/// identity) that authenticate the acceptor first, and whether a plaintext member
/// may be dialed at all.
///
/// One dialer is shared by every peer link a node opens, so the transport policy
/// is decided in exactly one place.
#[derive(Clone)]
pub struct PeerDialer {
    node: Arc<[u8]>,
    secret: Arc<[u8]>,
    tls: Option<Arc<ClientConfig>>,
    require_tls: bool,
}

impl PeerDialer {
    /// A dialer claiming `node` and presenting `secret` on every link it opens,
    /// authenticating a TLS member against `tls` when one is configured, and
    /// refusing plaintext members outright when `require_tls`.
    pub fn new(
        node: Arc<[u8]>,
        secret: Arc<[u8]>,
        tls: Option<Arc<ClientConfig>>,
        require_tls: bool,
    ) -> Self {
        Self {
            node,
            secret,
            tls,
            require_tls,
        }
    }

    /// The node id a link claims in its `PeerAuth` — this node's own advertise
    /// address, which the acceptor binds the connection to (and, under peer mTLS,
    /// checks against the certificate this dial presented).
    pub fn node(&self) -> &[u8] {
        &self.node
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
        PeerDialer::new(
            Arc::from(&b"10.0.0.1:9000"[..]),
            Arc::from(&b"secret"[..]),
            None,
            false,
        )
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
    fn an_address_that_carries_more_than_an_authority_is_refused() {
        // An advertise address is a host and a port. Userinfo would make the host read
        // out of the string differ from the host the dial connects to; a path, a query
        // and a fragment are free aliases, so one endpoint would answer under
        // unboundedly many node ids.
        for addr in [
            "wss://10.0.0.1:9000/sync",
            "wss://a.example:1@b.example:9000",
            "a.example@b.example:9000",
            "10.0.0.1:9000?x=1",
            "10.0.0.1:9000#f",
        ] {
            assert_eq!(
                PeerEndpoint::parse(addr),
                Err(BadPeerAddress::NotAnAuthority),
                "{addr}"
            );
            assert_eq!(member_host(addr.as_bytes()), None, "{addr}");
            assert_eq!(canonical_member_addr(addr), None, "{addr}");
        }
    }

    #[test]
    fn a_dialed_url_names_the_same_host_the_binding_reads() {
        // The one invariant the refusal above exists for: whatever a reader takes out
        // of an address, the dialer connects to the same host.
        for addr in ["10.0.0.1:9000", "wss://node-a.example:9000", "[::1]:9000"] {
            let url = PeerEndpoint::parse(addr).unwrap().url;
            let authority = url.split_once("://").unwrap().1.trim_end_matches('/');
            assert_eq!(
                host_of(authority).map(str::to_ascii_lowercase),
                member_host(addr.as_bytes()),
                "{addr}",
            );
        }
    }

    #[test]
    fn one_endpoint_has_one_canonical_address() {
        // Two spellings of one endpoint would be two node ids, so two positions in the
        // ring that one node answers for and only one of which ever speaks.
        for (a, b) in [
            ("10.0.0.1:9000", "ws://10.0.0.1:9000"),
            ("WS://Node-A.Example.:9000", "node-a.example:9000"),
            (
                "[2001:0db8:0000:0000:0000:0000:0000:0006]:9000",
                "[2001:db8::6]:9000",
            ),
            ("  wss://Node-A.Example:9000  ", "wss://node-a.example:9000"),
        ] {
            assert_eq!(
                canonical_member_addr(a),
                canonical_member_addr(b),
                "{a}/{b}"
            );
            assert!(canonical_member_addr(a).is_some(), "{a}");
        }
        // A member's transport is part of its identity, so it is not folded away.
        assert_ne!(
            canonical_member_addr("wss://node-a.example:9000"),
            canonical_member_addr("node-a.example:9000"),
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
    fn an_address_naming_no_host_is_refused() {
        // Dialable by nobody and bindable to no certificate, so it fails at the parse
        // every consumer shares: a configured member at startup, a gossiped one as a
        // *permanent* dial failure rather than one redialed forever.
        for addr in ["::1:9000", ":9000", "[]:9000", "wss://:9000"] {
            assert_eq!(
                PeerEndpoint::parse(addr),
                Err(BadPeerAddress::NoHost),
                "{addr}"
            );
            assert_eq!(member_host(addr.as_bytes()), None, "{addr}");
        }
        assert!(DialError::Address(BadPeerAddress::NoHost).is_permanent());
    }

    #[test]
    fn a_bracketed_literal_and_a_bare_host_still_parse() {
        // The refusal must not catch a legitimate address on its way past.
        for addr in ["[::1]:9000", "wss://[2001:db8::1]:9000", "node-a"] {
            assert!(PeerEndpoint::parse(addr).is_ok(), "{addr}");
            assert!(member_host(addr.as_bytes()).is_some(), "{addr}");
        }
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
        let dialer = PeerDialer::new(
            Arc::from(&b"10.0.0.1:9000"[..]),
            Arc::from(&b"secret"[..]),
            None,
            true,
        );
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
    fn a_configuration_refusal_is_permanent_and_an_unreachable_peer_is_not() {
        // A peer that is merely down may come back at any moment and is redialed on
        // the fast cadence; a transport or trust refusal is decided before a socket
        // opens, so retrying it four times a second produces nothing.
        assert!(DialError::Address(BadPeerAddress::Empty).is_permanent());
        assert!(DialError::PlaintextRefused.is_permanent());
        assert!(DialError::NoTrustAnchors.is_permanent());
        assert!(!DialError::Unreachable.is_permanent());
    }

    #[test]
    fn a_dialer_carries_the_cluster_secret_the_link_presents() {
        assert_eq!(plain_dialer().secret(), b"secret");
    }

    #[test]
    fn a_dialer_carries_the_node_id_the_link_claims() {
        assert_eq!(plain_dialer().node(), b"10.0.0.1:9000");
    }

    // --- the node-id to certificate-subject binding ---

    #[test]
    fn a_members_host_is_read_out_of_its_advertise_address() {
        assert_eq!(
            member_host(b"node-a.internal:9000").as_deref(),
            Some("node-a.internal")
        );
        assert_eq!(
            member_host(b"wss://node-a.internal:9000").as_deref(),
            Some("node-a.internal")
        );
        assert_eq!(member_host(b"10.0.0.1:9000").as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn a_bracketed_ipv6_literal_keeps_its_colons() {
        // Splitting on the last colon would make `[::1]:9000` the host `[:`.
        assert_eq!(member_host(b"wss://[::1]:9000").as_deref(), Some("::1"));
        assert_eq!(
            member_host(b"[2001:db8::1]:9000").as_deref(),
            Some("2001:db8::1")
        );
    }

    #[test]
    fn an_address_that_resolves_to_no_endpoint_names_no_host() {
        assert_eq!(member_host(b""), None);
        assert_eq!(member_host(b"http://node-a:9000"), None);
        assert_eq!(member_host(&[0xff, 0xfe]), None);
    }

    #[test]
    fn a_certificate_naming_a_members_host_establishes_that_member() {
        assert!(cert_names_member(
            b"node-a.internal",
            b"wss://node-a.internal:9000"
        ));
        // The port is no part of the binding — no certificate can carry one — so a
        // member keeps its identity across a port change.
        assert!(cert_names_member(
            b"node-a.internal",
            b"wss://node-a.internal:9443"
        ));
    }

    #[test]
    fn an_ip_literal_is_matched_as_an_address_rather_than_as_a_string() {
        // A certificate's IP SAN arrives canonicalized from its octets; an advertise
        // address holds whatever the operator wrote. Both spell one member.
        assert!(cert_names_member(
            b"2001:db8::1",
            b"wss://[2001:0db8:0000:0000:0000:0000:0000:0001]:9000"
        ));
        assert!(cert_names_member(b"127.0.0.1", b"wss://127.0.0.1:9000"));
        assert!(!cert_names_member(
            b"2001:db8::2",
            b"wss://[2001:db8::1]:9000"
        ));
    }

    #[test]
    fn an_address_and_a_name_never_bind_each_other() {
        assert!(!cert_names_member(
            b"node-a.internal",
            b"wss://10.0.0.1:9000"
        ));
        assert!(!cert_names_member(
            b"10.0.0.1",
            b"wss://node-a.internal:9000"
        ));
    }

    #[test]
    fn a_certificate_subject_is_matched_without_regard_to_case() {
        assert!(cert_names_member(
            b"Node-A.Internal",
            b"wss://node-a.INTERNAL:9000"
        ));
    }

    #[test]
    fn a_certificate_naming_another_host_establishes_nothing() {
        assert!(!cert_names_member(
            b"node-b.internal",
            b"wss://node-a.internal:9000"
        ));
        // Nor does a prefix or suffix of the host, which a substring rule would admit.
        assert!(!cert_names_member(b"node-a", b"wss://node-a.internal:9000"));
        assert!(!cert_names_member(
            b"internal",
            b"wss://node-a.internal:9000"
        ));
    }

    #[test]
    fn a_root_label_is_no_part_of_the_name() {
        // A fully-qualified name and the same name without its trailing dot are one
        // host, and a certificate and an advertise address need not agree on which
        // spelling to use.
        assert!(cert_names_member(
            b"node-a.internal.",
            b"wss://node-a.internal:9000"
        ));
        assert!(cert_names_member(
            b"node-a.internal",
            b"wss://node-a.internal.:9000"
        ));
    }

    #[test]
    fn an_address_with_a_root_label_is_still_an_address() {
        // The reader that decides which SANs name a host and this comparison must agree
        // on what an address is, or a `dNSName` of `10.0.0.6.` passes the reader as a
        // name and then matches an IP-advertised member here as an address.
        assert!(is_ip_literal("10.0.0.6."));
        assert!(is_ip_literal(" 10.0.0.6 "));
        assert!(is_ip_literal("::1."));
        assert!(!is_ip_literal("node-a.internal."));
        assert!(cert_names_member(b"10.0.0.6.", b"wss://10.0.0.6:9000"));
    }

    #[test]
    fn only_one_root_label_folds() {
        // A name carries at most one root label, so folding a run of them would widen
        // what binds a member for no reason a certificate or an address ever needs.
        assert!(!cert_names_member(
            b"node-a.internal..",
            b"wss://node-a.internal:9000"
        ));
        assert!(!cert_names_member(b"10.0.0.6..", b"wss://10.0.0.6:9000"));
    }

    #[test]
    fn a_wildcard_names_no_member() {
        // Deliberate, and the reason the comparison is whole-name: `*.internal` would
        // bind every member of that domain to one certificate, which gives up the
        // per-member identity rather than establishing it. A TLS client accepts a
        // wildcard for the host it dialed; a member's identity is not that question.
        assert!(!cert_names_member(
            b"*.internal",
            b"wss://node-a.internal:9000"
        ));
        assert!(!cert_names_member(b"*", b"wss://node-a.internal:9000"));
    }

    #[test]
    fn an_empty_or_unusable_certificate_subject_establishes_nothing() {
        assert!(!cert_names_member(b"", b"wss://node-a.internal:9000"));
        assert!(!cert_names_member(b"   ", b"wss://node-a.internal:9000"));
        assert!(!cert_names_member(
            &[0xff, 0xfe],
            b"wss://node-a.internal:9000"
        ));
        // Nor does any subject establish a member whose address names no host.
        assert!(!cert_names_member(
            b"node-a.internal",
            b"http://node-a.internal:9000"
        ));
    }
}
