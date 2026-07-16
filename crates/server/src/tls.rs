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
//! The client-cert-verifier slot is [`with_no_client_auth`]: mTLS (verifying a
//! client certificate) replaces that call with a client-cert verifier without
//! touching the cert/key loading here.
//!
//! [`with_no_client_auth`]: rustls::ConfigBuilder::with_no_client_auth

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};

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
    /// rustls rejected the cert/key pair (e.g. the key does not match the cert).
    Rustls(rustls::Error),
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
            TlsConfigError::Rustls(e) => write!(f, "building TLS config: {e}"),
        }
    }
}

impl std::error::Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TlsConfigError::Io { source, .. } => Some(source),
            TlsConfigError::Rustls(e) => Some(e),
            TlsConfigError::NoCertificate(_) | TlsConfigError::NoPrivateKey(_) => None,
        }
    }
}

/// Build a [`ServerConfig`] from a PEM certificate chain and private key on disk.
/// The result is shared behind an `Arc` because one config backs every accepted
/// connection. Errors loudly — a missing, empty, or mismatched cert/key is a
/// startup failure, not a plaintext fall back.
pub fn server_config_from_pem(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();

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

    // Pin the ring provider explicitly rather than lean on a process-default
    // that other TLS users (reqwest) in the same binary may or may not have
    // installed. `with_no_client_auth` is the mTLS seam — a later unit swaps it
    // for a client-cert verifier.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsConfigError::Rustls)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsConfigError::Rustls)?;

    Ok(Arc::new(config))
}
