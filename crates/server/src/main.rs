//! The crdtsync sync server binary.
//!
//! Binds `CRDTSYNC_ADDR` (default `127.0.0.1:9000`) and serves the wire
//! protocol over WebSocket. Set `CRDTSYNC_DATA_DIR` to persist each room's op
//! log there and replay it on restart; unset, the replicas are in-memory. Set
//! `CRDTSYNC_POLICY_FILE` to enforce a declarative authorization policy; unset,
//! every authenticated actor is permitted. Set `CRDTSYNC_CREDENTIALS_FILE` to
//! authenticate actors against a static secret-token table; unset, the dev-mode
//! verifier admits any credential.
//!
//! A policy's `actor:` and subject-class (`authenticated` / `anonymous`) rules
//! are only real boundaries when the actor is server-derived. With a credentials
//! file the actor comes from the validated token, so those rules are enforced.
//! Without one, the dev-mode verifier takes the credential verbatim as the actor
//! — the client controls its whole actor id, including the `anon:` prefix that
//! separates anonymous from authenticated — so every subject but `anyone` is
//! spoofable. A richer verifier (signed tokens, OIDC) is injected by embedding
//! the library and calling `serve_with_verifier` / `serve_with_authorizer`.

use std::env::VarError;

use crdtsync_core::ClientId;
use crdtsync_server::acl::{Acl, PolicyFileError};
use crdtsync_server::auth::CredentialsFileError;
use crdtsync_server::runtime::{serve_with_authorizer, ServeConfig};
use crdtsync_server::{AllowAll, Authorizer, PermitAll, StaticTokens, Store, Verifier};
use tokio::net::TcpListener;

/// Read an environment variable that names a filesystem path, mapping absence to
/// `None` and non-unicode to an error the caller returns.
fn path_var(name: &'static str) -> std::io::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} is not valid unicode"),
        )),
    }
}

/// The verifier for the run: a static credential table if `CRDTSYNC_CREDENTIALS_FILE`
/// is set, else the dev-mode `AllowAll`. A malformed table surfaces the full
/// [`CredentialsFileError`] (its "credentials file" context, with the underlying
/// error as the source), keeping the original [`io::ErrorKind`](std::io::ErrorKind)
/// so a missing file still reads as `NotFound`.
fn verifier() -> std::io::Result<Box<dyn Verifier + Send>> {
    match path_var("CRDTSYNC_CREDENTIALS_FILE")? {
        Some(path) => {
            let table = StaticTokens::from_credentials_file(path).map_err(|e| {
                let kind = match &e {
                    CredentialsFileError::Io(io) => io.kind(),
                    CredentialsFileError::Parse(_) => std::io::ErrorKind::InvalidData,
                };
                std::io::Error::new(kind, e)
            })?;
            Ok(Box::new(table))
        }
        None => Ok(Box::new(AllowAll)),
    }
}

/// The authorizer for the run: a declared policy if `CRDTSYNC_POLICY_FILE` is set,
/// else the permissive `PermitAll`. A malformed policy surfaces the full
/// [`PolicyFileError`] the way [`verifier`] surfaces its own.
fn authorizer() -> std::io::Result<Box<dyn Authorizer + Send>> {
    match path_var("CRDTSYNC_POLICY_FILE")? {
        Some(path) => {
            let acl = Acl::from_policy_file(path).map_err(|e| {
                let kind = match &e {
                    PolicyFileError::Io(io) => io.kind(),
                    PolicyFileError::Parse(_) => std::io::ErrorKind::InvalidData,
                };
                std::io::Error::new(kind, e)
            })?;
            Ok(Box::new(acl))
        }
        None => Ok(Box::new(PermitAll)),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = match std::env::var("CRDTSYNC_ADDR") {
        Ok(addr) => addr,
        Err(VarError::NotPresent) => "127.0.0.1:9000".to_string(),
        Err(VarError::NotUnicode(_)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "CRDTSYNC_ADDR is not valid unicode",
            ));
        }
    };
    let store = match path_var("CRDTSYNC_DATA_DIR")? {
        Some(dir) => Some(Store::open(dir)?),
        None => None,
    };
    let verifier = verifier()?;
    let authorizer = authorizer()?;
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("crdtsync serving on ws://{addr}");
    // The server never mints ops; its replicas only merge, so a fixed id is fine.
    // Both seams default to their permissive dev-mode value when unconfigured, so
    // one serve path covers every combination.
    serve_with_authorizer(
        listener,
        ClientId::from_bytes([0; 16]),
        store,
        ServeConfig::default(),
        verifier,
        authorizer,
    )
    .await
}
