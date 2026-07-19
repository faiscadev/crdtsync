//! TLS termination at the server listener.
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
//! A verified client cert's identity (its SAN, falling back to CN) is extracted
//! with [`actor_from_client_cert`] and bound as the connection's authenticated
//! actor — the same ACL principal an in-band credential establishes, reached over
//! the transport instead.
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
//! [`WebPkiClientVerifier`]: rustls::server::WebPkiClientVerifier

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{self, RootCertStore, ServerConfig};
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
    let cert_bytes = std::fs::read(cert_path).map_err(|source| TlsConfigError::Io {
        path: cert_path.to_path_buf(),
        source,
    })?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|source| TlsConfigError::Io {
            path: cert_path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsConfigError::NoCertificate(cert_path.to_path_buf()));
    }

    let key_bytes = std::fs::read(key_path).map_err(|source| TlsConfigError::Io {
        path: key_path.to_path_buf(),
        source,
    })?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .map_err(|source| TlsConfigError::Io {
            path: key_path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| TlsConfigError::NoPrivateKey(key_path.to_path_buf()))?;

    // Pin the ring provider explicitly rather than lean on a process-default that
    // other TLS users (reqwest) in the same binary may or may not have installed.
    // The verifier shares it so both halves of the handshake speak the same crypto.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(TlsConfigError::Rustls)?;
    let config = match client_ca {
        Some((ca_path, mode)) => {
            let roots = load_client_roots(ca_path)?;
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

/// Load the client-cert trust anchors from the PEM bundle at `path` into a
/// [`RootCertStore`]. An unreadable file is an [`Io`](TlsConfigError::Io) error and
/// a bundle holding no usable certificate is a
/// [`NoClientCa`](TlsConfigError::NoClientCa) error — mTLS never silently degrades
/// to server-auth-only.
fn load_client_roots(path: &Path) -> Result<RootCertStore, TlsConfigError> {
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
        return Err(TlsConfigError::NoClientCa(path.to_path_buf()));
    }
    Ok(roots)
}

/// The actor identity a verified client certificate authenticates as: the leaf
/// cert's Subject Alternative Name (the first DNS, email, or URI entry), falling
/// back to its Subject Common Name. `None` when the cert parses but carries
/// neither — the caller treats that as a rejection, never as an anonymous or
/// default actor, so an identity-less cert cannot slip past authentication.
///
/// The returned bytes are the UTF-8 of the name, fed into the same
/// authenticated-actor plumbing an in-band credential's actor uses.
pub fn actor_from_client_cert(leaf: &CertificateDer<'_>) -> Option<Vec<u8>> {
    let (_, cert) = X509Certificate::from_der(leaf.as_ref()).ok()?;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            let value = match name {
                GeneralName::DNSName(s) | GeneralName::RFC822Name(s) | GeneralName::URI(s) => *s,
                _ => continue,
            };
            if !value.is_empty() {
                return Some(value.as_bytes().to_vec());
            }
        }
    }
    let cn = cert
        .subject()
        .iter_common_name()
        .filter_map(|cn| cn.as_str().ok())
        .find(|cn| !cn.is_empty())
        .map(|cn| cn.as_bytes().to_vec());
    cn
}
