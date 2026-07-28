//! TLS for both ends of a connection: termination at this node's listener, and
//! the client-side config its outbound peer dials run under.
//!
//! [`server_config_from_pem`] loads a PEM certificate chain + private key into a
//! [`rustls::ServerConfig`] the [`serve`](crate::runtime::serve) accept loop wraps
//! each connection in, speaking the existing wire protocol over the encrypted
//! stream. TLS is opt-in: with no cert configured in [`ServeConfig`], the listener
//! binds plaintext exactly as before.
//!
//! A configured-but-broken cert is a loud startup error, never a silent fall back
//! to plaintext — a silent downgrade turns a deployment that asked for encryption
//! into an unencrypted one, a security regression.
//!
//! mTLS (client-cert authentication) is opt-in on top of that: configure a
//! trust-anchor bundle and [`server_config_from_pem_with_client_ca`] swaps the
//! `with_no_client_auth` slot for a [`WebPkiClientVerifier`] against those roots.
//! A verified client cert is read by two rules that are deliberately not the same.
//! [`actor_from_client_cert`] is its subject as an ACL principal — a DNS, e-mail or
//! URI SAN, else an IP SAN, else the Common Name — bound as the connection's
//! authenticated actor, the same principal an in-band credential establishes, reached
//! over the transport instead. [`host_names_from_client_cert`] is the narrower one the
//! peer plane binds a *member* to: only the `dNSName` and `iPAddress` SANs, which are
//! the kinds that name a host a CA vouches for reaching.
//!
//! [`ClientAuthMode`] selects how strict the client-cert requirement is:
//!
//! - [`Require`](ClientAuthMode::Require) (the secure default): fail-closed by
//!   construction — a client presenting no cert, or one that does not chain to a
//!   configured root, is rejected at the handshake and never reaches the wire
//!   protocol.
//! - [`Request`](ClientAuthMode::Request): opportunistic mTLS — the server still
//!   *validates* a presented cert against the roots (an untrusted/invalid presented
//!   cert is still rejected), but a client presenting *no* cert is allowed through
//!   (`allow_unauthenticated` on the verifier builder) and falls through to the
//!   ordinary certless session path (in-band credential / anonymous rules). Only
//!   true *absence* of a cert is admitted — a bad presented cert is never treated
//!   as anonymous.
//!
//! The dial side is [`client_config_from_pem`]: the trust anchors a node
//! authenticates a TLS *member* against before its peer link writes the cluster
//! secret, optionally with [`client_config_from_pem_with_identity`] presenting this
//! node's own certificate so one handshake authenticates both ends. The anchors are
//! always explicit — see [`client_config_from_pem`] for why no ambient root store
//! stands in for them. [`crate::dial`] decides which members it applies to.
//!
//! [`WebPkiClientVerifier`]: rustls::server::WebPkiClientVerifier

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

/// Why building a [`ServerConfig`] from PEM files failed. Each arm names the file
/// at fault so a misconfigured deployment reads a precise startup error rather
/// than a bare I/O message.
#[derive(Debug)]
pub enum TlsConfigError {
    /// A cert or key file could not be read.
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The cert file held no PEM certificate.
    NoCertificate(PathBuf),
    /// The key file held no PEM private key.
    NoPrivateKey(PathBuf),
    /// The client-CA trust bundle held no certificate — mTLS was asked for with
    /// nothing to anchor client certs to. Never fall through to server-auth-only
    /// (a silent drop of the client-auth requirement is a security regression).
    NoClientCa(PathBuf),
    /// The peer trust bundle held no certificate — outbound peer dials were asked
    /// to verify TLS members against nothing. Never fall through to trusting
    /// whatever answers (nor to an ambient root store): a bearer credential is
    /// written over that link.
    NoPeerCa(PathBuf),
    /// rustls rejected building the client-cert verifier from the trust bundle.
    ClientVerifier(rustls::server::VerifierBuilderError),
    /// rustls rejected the cert/key pair (e.g. the key does not match the cert).
    Rustls(rustls::Error),
    /// `CRDTSYNC_TLS_CLIENT_AUTH` held a value that is neither `require` nor
    /// `request` — an unrecognized mode is a loud startup error, never silently
    /// resolved to the permissive `request` mode.
    BadClientAuthMode(String),
}

/// How strictly the server enforces client-cert authentication when a client-CA
/// trust bundle is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuthMode {
    /// Require a valid client cert: a client presenting no cert, or one that does
    /// not chain to a configured root, is rejected at the handshake. The secure
    /// default when a client-CA is set.
    #[default]
    Require,
    /// Opportunistic mTLS: authenticate-if-presented, don't-require. A presented
    /// cert is still validated against the roots (an untrusted/invalid one is
    /// rejected), but a client presenting *no* cert is allowed to connect and falls
    /// through to the ordinary certless session path. Only cert *absence* is
    /// relaxed — a bad presented cert is never admitted.
    Request,
}

impl ClientAuthMode {
    /// Parse the `CRDTSYNC_TLS_CLIENT_AUTH` value. Absence (`None`) resolves to the
    /// secure default [`Require`](ClientAuthMode::Require); `"require"` / `"request"`
    /// select the mode (case-insensitively). Any other value is a
    /// [`BadClientAuthMode`](TlsConfigError::BadClientAuthMode) error — an
    /// unrecognized mode never silently degrades to the permissive one.
    pub fn parse(value: Option<&str>) -> Result<Self, TlsConfigError> {
        match value {
            None => Ok(ClientAuthMode::Require),
            Some(v) => match v.trim().to_ascii_lowercase().as_str() {
                "require" => Ok(ClientAuthMode::Require),
                "request" => Ok(ClientAuthMode::Request),
                _ => Err(TlsConfigError::BadClientAuthMode(v.to_string())),
            },
        }
    }
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfigError::Io { path, source } => {
                write!(f, "reading TLS file {}: {source}", path.display())
            }
            TlsConfigError::NoCertificate(path) => {
                write!(f, "TLS cert file {} holds no certificate", path.display())
            }
            TlsConfigError::NoPrivateKey(path) => {
                write!(f, "TLS key file {} holds no private key", path.display())
            }
            TlsConfigError::NoClientCa(path) => write!(
                f,
                "TLS client-CA file {} holds no certificate",
                path.display()
            ),
            TlsConfigError::NoPeerCa(path) => write!(
                f,
                "peer trust bundle {} holds no certificate",
                path.display()
            ),
            TlsConfigError::ClientVerifier(e) => {
                write!(f, "building TLS client-cert verifier: {e}")
            }
            TlsConfigError::Rustls(e) => write!(f, "building TLS config: {e}"),
            TlsConfigError::BadClientAuthMode(value) => write!(
                f,
                "CRDTSYNC_TLS_CLIENT_AUTH must be `require` or `request`, got `{value}`"
            ),
        }
    }
}

impl std::error::Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TlsConfigError::Io { source, .. } => Some(source),
            TlsConfigError::Rustls(e) => Some(e),
            TlsConfigError::ClientVerifier(e) => Some(e),
            TlsConfigError::NoCertificate(_)
            | TlsConfigError::NoPrivateKey(_)
            | TlsConfigError::NoClientCa(_)
            | TlsConfigError::NoPeerCa(_)
            | TlsConfigError::BadClientAuthMode(_) => None,
        }
    }
}

/// Build a [`ServerConfig`] from a PEM certificate chain and private key on disk,
/// server-authenticated only (no client cert required — the [`with_no_client_auth`]
/// slot). The result is shared behind an `Arc` because one config backs every
/// accepted connection. Errors loudly — a missing, empty, or mismatched cert/key
/// is a startup failure, not a plaintext fall back.
///
/// [`with_no_client_auth`]: rustls::ConfigBuilder::with_no_client_auth
pub fn server_config_from_pem(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    build_server_config(cert_path.as_ref(), key_path.as_ref(), None)
}

/// Build a [`ServerConfig`] as [`server_config_from_pem`] does, additionally
/// *requiring* every client to present a certificate that chains to a trust anchor
/// in the PEM bundle at `client_ca_path` — mutual TLS in [`Require`] mode. This is
/// fail-closed at the handshake: a client presenting no cert, or one that does not
/// chain to a configured root, is rejected by rustls before the connection ever
/// reaches the wire protocol. A verified connection's peer cert is later mapped to
/// an actor by [`actor_from_client_cert`].
///
/// An empty client-CA bundle is a loud [`NoClientCa`](TlsConfigError::NoClientCa)
/// error, never a silent fall back to server-auth-only: a deployment that asked
/// for mTLS must not quietly run without it.
///
/// [`Require`]: ClientAuthMode::Require
pub fn server_config_from_pem_with_client_ca(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    client_ca_path: impl AsRef<Path>,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    server_config_from_pem_with_client_ca_mode(
        cert_path,
        key_path,
        client_ca_path,
        ClientAuthMode::Require,
    )
}

/// Build an mTLS [`ServerConfig`] as [`server_config_from_pem_with_client_ca`]
/// does, with an explicit [`ClientAuthMode`] selecting how strict the client-cert
/// requirement is:
///
/// - [`Require`](ClientAuthMode::Require) rejects a certless/untrusted client at
///   the handshake (fail-closed).
/// - [`Request`](ClientAuthMode::Request) is opportunistic — a *presented* cert is
///   still validated against the roots (an untrusted/invalid one is still
///   rejected), but a client presenting *no* cert is allowed through and falls
///   through to the ordinary certless session path.
///
/// The trust bundle is validated identically in both modes; the only relaxation in
/// `Request` is admitting cert *absence*.
pub fn server_config_from_pem_with_client_ca_mode(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    client_ca_path: impl AsRef<Path>,
    mode: ClientAuthMode,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    build_server_config(
        cert_path.as_ref(),
        key_path.as_ref(),
        Some((client_ca_path.as_ref(), mode)),
    )
}

/// Build the [`ServerConfig`], with a client-cert verifier when `client_ca` is set
/// (its [`ClientAuthMode`] selecting require vs. request) and
/// [`with_no_client_auth`](rustls::ConfigBuilder::with_no_client_auth) when it is
/// not.
fn build_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<(&Path, ClientAuthMode)>,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    // Pin the ring provider explicitly rather than lean on a process-default that
    // other TLS users (reqwest) in the same binary may or may not have installed.
    // The verifier shares it so both halves of the handshake speak the same crypto.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(TlsConfigError::Rustls)?;
    let config = match client_ca {
        Some((ca_path, mode)) => {
            let roots = load_roots(ca_path, TlsConfigError::NoClientCa)?;
            let verifier_builder =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            // Both modes validate a *presented* cert against the trust anchors; in
            // request mode `allow_unauthenticated` additionally admits a client that
            // presents no cert at all, so an untrusted/invalid presented cert is
            // still rejected while cert absence falls through to the certless path.
            let verifier_builder = match mode {
                ClientAuthMode::Require => verifier_builder,
                ClientAuthMode::Request => verifier_builder.allow_unauthenticated(),
            };
            let verifier = verifier_builder
                .build()
                .map_err(TlsConfigError::ClientVerifier)?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(certs, key)
    .map_err(TlsConfigError::Rustls)?;

    Ok(Arc::new(config))
}

/// Build a [`ClientConfig`] for this node's outbound peer dials, trusting the PEM
/// trust-anchor bundle at `ca_path` and presenting no client certificate. A dial
/// under it verifies the acceptor's certificate against those anchors before the
/// link carries a single frame, which is what lets a node tell a member from
/// anything else answering the member's advertise address.
///
/// The anchors are explicit and nothing else is trusted: no platform store, no
/// bundled public root set. The peer link carries a bearer credential granting
/// write access to every room the cluster replicates, so trusting an ambient store
/// would widen the set of issuers that can impersonate a member to every CA on the
/// host — and a cluster's certificates are an operator-controlled input, so naming
/// them costs one line of configuration.
pub fn client_config_from_pem(
    ca_path: impl AsRef<Path>,
) -> Result<Arc<ClientConfig>, TlsConfigError> {
    build_client_config(ca_path.as_ref(), None)
}

/// Build a peer-dial [`ClientConfig`] as [`client_config_from_pem`] does, also
/// presenting the PEM certificate chain + key at `cert_path`/`key_path` as this
/// node's client identity. With the acceptor configured for mTLS
/// ([`server_config_from_pem_with_client_ca`]) the one handshake then authenticates
/// both ends of the peer link.
///
/// The client identity is its own configuration, never inferred from the node's
/// listener certificate: a server certificate commonly carries `serverAuth` alone,
/// so reusing it as a client identity would work in a lab and be rejected at the
/// handshake in a deployment that issues its certificates properly.
pub fn client_config_from_pem_with_identity(
    ca_path: impl AsRef<Path>,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<Arc<ClientConfig>, TlsConfigError> {
    build_client_config(
        ca_path.as_ref(),
        Some((cert_path.as_ref(), key_path.as_ref())),
    )
}

/// Build the [`ClientConfig`], presenting a client identity when `identity` is set.
fn build_client_config(
    ca_path: &Path,
    identity: Option<(&Path, &Path)>,
) -> Result<Arc<ClientConfig>, TlsConfigError> {
    let roots = load_roots(ca_path, TlsConfigError::NoPeerCa)?;
    // The same pinned provider the listener uses, so both directions of a peer
    // handshake speak the same crypto.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsConfigError::Rustls)?
        .with_root_certificates(roots);
    let config = match identity {
        Some((cert_path, key_path)) => builder
            .with_client_auth_cert(load_certs(cert_path)?, load_private_key(key_path)?)
            .map_err(TlsConfigError::Rustls)?,
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

/// Load the PEM certificate chain at `path`. An empty file is a
/// [`NoCertificate`](TlsConfigError::NoCertificate) error rather than an empty
/// chain rustls would reject less legibly.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let bytes = std::fs::read(path).map_err(|source| TlsConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut bytes.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|source| TlsConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsConfigError::NoCertificate(path.to_path_buf()));
    }
    Ok(certs)
}

/// Load the PEM private key at `path`.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let bytes = std::fs::read(path).map_err(|source| TlsConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    rustls_pemfile::private_key(&mut bytes.as_slice())
        .map_err(|source| TlsConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| TlsConfigError::NoPrivateKey(path.to_path_buf()))
}

/// Load trust anchors from the PEM bundle at `path` into a [`RootCertStore`],
/// reporting a bundle that holds no usable certificate through `empty` — the
/// caller's error arm naming which side asked for it. A trust bundle never
/// silently resolves to no anchors: on the listener that would drop the client-auth
/// requirement, and on the dial it would refuse every TLS peer at every round
/// instead of at startup.
fn load_roots(
    path: &Path,
    empty: fn(PathBuf) -> TlsConfigError,
) -> Result<RootCertStore, TlsConfigError> {
    let bytes = std::fs::read(path).map_err(|source| TlsConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let cas: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut bytes.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|source| TlsConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(cas);
    if added == 0 {
        return Err(empty(path.to_path_buf()));
    }
    Ok(roots)
}

/// The textual form of an IP-address SAN's octets — dotted-quad for a v4 entry and
/// the canonical compressed form for a v6 one, matching how an advertise address
/// spells a host. `None` for any other length, which is not an IP address at all.
fn ip_san(octets: &[u8]) -> Option<String> {
    match octets.len() {
        4 => {
            let v4: [u8; 4] = octets.try_into().ok()?;
            Some(std::net::Ipv4Addr::from(v4).to_string())
        }
        16 => {
            let v6: [u8; 16] = octets.try_into().ok()?;
            Some(std::net::Ipv6Addr::from(v6).to_string())
        }
        _ => None,
    }
}

/// The actor a verified client certificate authenticates as: its first Subject
/// Alternative Name that names a principal — a DNS name, an e-mail address or a URI —
/// then its first IP-address SAN, then its Subject Common Name. `None` when the cert
/// parses but names nothing at all: the caller treats that as a rejection, never as an
/// anonymous or default actor, so an identity-less cert cannot slip past
/// authentication.
///
/// The IP fallback sits *after* the named kinds so that a certificate spelling its
/// subject both ways — `DNS:alice` alongside `IP:10.0.0.6`, whatever order the issuer
/// emitted them in — authorizes as the name rather than the address. An ACL principal
/// should not depend on SAN ordering.
///
/// The returned bytes are the UTF-8 of the name, fed into the same authenticated-actor
/// plumbing an in-band credential's actor uses.
pub fn actor_from_client_cert(leaf: &CertificateDer<'_>) -> Option<Vec<u8>> {
    let (_, cert) = X509Certificate::from_der(leaf.as_ref()).ok()?;
    let mut ip = None;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            match name {
                GeneralName::DNSName(s) | GeneralName::RFC822Name(s) | GeneralName::URI(s)
                    if !s.is_empty() =>
                {
                    return Some(s.as_bytes().to_vec())
                }
                GeneralName::IPAddress(octets) if ip.is_none() => ip = ip_san(octets),
                _ => continue,
            }
        }
    }
    if let Some(addr) = ip {
        return Some(addr.into_bytes());
    }
    // Bound rather than returned directly: the iterator borrows `cert`, which does not
    // outlive the tail expression.
    let cn = cert
        .subject()
        .iter_common_name()
        .filter_map(|cn| cn.as_str().ok())
        .find(|cn| !cn.is_empty())
        .map(|cn| cn.as_bytes().to_vec());
    cn
}

/// Every *host* a verified client certificate names: its `dNSName` and `iPAddress`
/// Subject Alternative Names, in certificate order. Empty when it names no host.
///
/// This is deliberately a narrower reading than [`actor_from_client_cert`], and the
/// difference is the whole of what makes a peer binding mean anything. A member's
/// identity is the host of its advertise address, so only the SAN kinds that *are*
/// host names may establish it — the same two kinds a TLS client checks when it
/// verifies an acceptor against the address it dialed, which is the symmetry the
/// binding rests on. An e-mail or URI SAN, and the Common Name, are free text a CA
/// fills in for a subject rather than an address it vouches for reaching; admitting
/// them would let a certificate legitimately issued for one node carry another node's
/// host and speak as it, which is exactly the impersonation the binding exists to
/// stop. There is no Common Name fallback here for the same reason.
///
/// All of them, not just the first, because a node certificate conventionally names
/// its host several ways at once — `DNS:node-a.internal` alongside `IP:10.0.0.6` — and
/// a cluster member is addressed by whichever of those its advertise address happens to
/// spell.
///
/// A wildcard SAN is returned verbatim and therefore binds nothing, since
/// [`cert_names_member`](crate::dial::cert_names_member) compares whole names. That is
/// deliberate: `*.internal` would bind *every* member of that domain to one
/// certificate, which is the per-member identity given up rather than established.
pub fn host_names_from_client_cert(leaf: &CertificateDer<'_>) -> Vec<Vec<u8>> {
    let Ok((_, cert)) = X509Certificate::from_der(leaf.as_ref()) else {
        return Vec::new();
    };
    let mut hosts = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            let value = match name {
                // A DNS name that is an IP literal is not one: RFC 6125 matches an
                // address only against an `iPAddress` SAN, and admitting it here would
                // undo the kind filter one field over — a certificate issued for one
                // node carrying another node's address in a field that is not an
                // address.
                GeneralName::DNSName(s)
                    if !s.is_empty() && s.parse::<std::net::IpAddr>().is_err() =>
                {
                    (*s).to_string()
                }
                GeneralName::IPAddress(octets) => match ip_san(octets) {
                    Some(addr) => addr,
                    None => continue,
                },
                _ => continue,
            };
            hosts.push(value.into_bytes());
        }
    }
    hosts
}

/// The hosts the leading certificate in the PEM file at `path` names — this node's
/// own peer identity, read from the same file its dials present. A deployment that
/// requires peer identity checks it against its own advertise address before anything
/// dials, so a certificate that names the wrong host is a startup error rather than a
/// cluster whose every peer refuses this node.
pub fn host_names_from_pem(path: impl AsRef<Path>) -> Result<Vec<Vec<u8>>, TlsConfigError> {
    let path = path.as_ref();
    let certs = load_certs(path)?;
    let leaf = certs
        .first()
        .ok_or_else(|| TlsConfigError::NoCertificate(path.to_path_buf()))?;
    Ok(host_names_from_client_cert(leaf))
}

#[cfg(test)]
mod tests {
    use super::{actor_from_client_cert, host_names_from_client_cert, ip_san};
    use tokio_rustls::rustls::pki_types::CertificateDer;

    /// A self-signed leaf naming `sans` (rcgen reads an IP literal as an IP SAN and
    /// anything else as a DNS name) with Common Name `cn`, as DER.
    fn leaf(cn: &str, sans: &[&str]) -> CertificateDer<'static> {
        let mut params =
            rcgen::CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        CertificateDer::from(cert.der().to_vec())
    }

    /// A self-signed leaf whose only SAN is the e-mail address `email`.
    fn leaf_with_email(cn: &str, email: &str) -> CertificateDer<'static> {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.subject_alt_names = vec![rcgen::SanType::Rfc822Name(email.try_into().unwrap())];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        CertificateDer::from(cert.der().to_vec())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn every_host_a_certificate_names_is_read_in_certificate_order() {
        // A node cert conventionally spells its host both ways at once, and the member
        // is addressed by whichever one its advertise address uses — so the peer
        // binding needs all of them, not the leading one.
        let der = leaf("node-a", &["node-a.internal", "10.0.0.6"]);
        assert_eq!(
            host_names_from_client_cert(&der),
            vec![b"node-a.internal".to_vec(), b"10.0.0.6".to_vec()],
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn an_email_san_names_no_host() {
        // The binding's whole point: a certificate an authority issued for one subject
        // must not be able to carry another node's host in a field that is free text
        // rather than an address. It still authenticates an actor.
        let der = leaf_with_email("node-a", "node-b.internal");
        assert!(host_names_from_client_cert(&der).is_empty());
        assert_eq!(
            actor_from_client_cert(&der).as_deref(),
            Some(&b"node-b.internal"[..]),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn a_uri_san_names_no_host() {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.subject_alt_names = vec![rcgen::SanType::URI("node-b.internal".try_into().unwrap())];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "node-a");
        let key = rcgen::KeyPair::generate().unwrap();
        let der = CertificateDer::from(params.self_signed(&key).unwrap().der().to_vec());
        assert!(host_names_from_client_cert(&der).is_empty());
        assert_eq!(
            actor_from_client_cert(&der).as_deref(),
            Some(&b"node-b.internal"[..]),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn a_dns_name_that_is_an_address_names_no_host() {
        // RFC 6125 matches an address only against an `iPAddress` SAN. Admitting a
        // `dNSName` that happens to be an IP literal would undo the kind filter one
        // field over — a certificate for one node carrying another node's address in a
        // field that is not an address.
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.subject_alt_names = vec![rcgen::SanType::DnsName("10.0.0.6".try_into().unwrap())];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "node-a");
        let key = rcgen::KeyPair::generate().unwrap();
        let der = CertificateDer::from(params.self_signed(&key).unwrap().der().to_vec());
        assert!(host_names_from_client_cert(&der).is_empty());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn a_common_name_names_no_host() {
        // The same, for the CN — which the actor rule *does* fall back to.
        let der = leaf("node-b.internal", &[]);
        assert!(host_names_from_client_cert(&der).is_empty());
        assert_eq!(
            actor_from_client_cert(&der).as_deref(),
            Some(&b"node-b.internal"[..]),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn a_session_authenticates_as_a_name_rather_than_an_address() {
        // An ACL principal must not depend on the order an issuer emitted its SANs in,
        // so the named kinds are preferred over an IP whichever way round they appear.
        for sans in [
            ["10.0.0.6", "alice"].as_slice(),
            ["alice", "10.0.0.6"].as_slice(),
        ] {
            let der = leaf("node-a", sans);
            assert_eq!(
                actor_from_client_cert(&der).as_deref(),
                Some(&b"alice"[..]),
                "{sans:?}",
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn a_certificate_naming_only_an_address_authenticates_as_it() {
        let der = leaf("node-a", &["10.0.0.6"]);
        assert_eq!(
            actor_from_client_cert(&der).as_deref(),
            Some(&b"10.0.0.6"[..]),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // generates a key pair, which reads system entropy
    fn a_certificate_naming_nothing_authenticates_as_nothing() {
        // Fail-closed: the caller rejects such a link rather than admitting it
        // anonymously.
        let der = leaf("", &[]);
        assert_eq!(actor_from_client_cert(&der), None);
        assert!(host_names_from_client_cert(&der).is_empty());
    }

    #[test]
    fn an_ip_address_san_reads_as_the_address_a_member_advertises() {
        assert_eq!(ip_san(&[127, 0, 0, 1]).as_deref(), Some("127.0.0.1"));
        assert_eq!(ip_san(&[10, 0, 0, 6]).as_deref(), Some("10.0.0.6"));
        let mut v6 = [0u8; 16];
        v6[15] = 1;
        assert_eq!(ip_san(&v6).as_deref(), Some("::1"));
    }

    #[test]
    fn octets_of_no_address_length_read_as_nothing() {
        assert_eq!(ip_san(&[]), None);
        assert_eq!(ip_san(&[127, 0, 0]), None);
        assert_eq!(ip_san(&[0u8; 17]), None);
    }
}
