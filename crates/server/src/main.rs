//! The crdtsync sync server binary.
//!
//! Binds `CRDTSYNC_ADDR` (default `127.0.0.1:9000`) and serves the wire
//! protocol over WebSocket. Set `CRDTSYNC_DATA_DIR` to persist each room's op
//! log there and replay it on restart; unset, the replicas are in-memory. Set
//! `CRDTSYNC_POLICY_FILE` to enforce a declarative authorization policy; unset,
//! every authenticated actor is permitted.
//!
//! The stock binary authenticates with the dev-mode `AllowAll` verifier, which
//! takes the presented credential verbatim as the actor id. So a policy's
//! subject-class rules (`authenticated` / `anonymous` / `anyone`) hold, but its
//! `actor:<id>` rules are only advisory here — any client can name itself that
//! actor by sending that credential. A production deployment embeds the library
//! and injects a real `Verifier` (`serve_with_verifier` / `serve_with_authorizer`)
//! that derives the actor from a validated credential; only then are `actor:`
//! rules enforceable.

use std::env::VarError;

use crdtsync_core::ClientId;
use crdtsync_server::acl::{Acl, PolicyFileError};
use crdtsync_server::runtime::{serve, serve_with_authorizer, ServeConfig};
use crdtsync_server::{AllowAll, Store};
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
    let policy = match path_var("CRDTSYNC_POLICY_FILE")? {
        // Keep the underlying failure intact: an unreadable file surfaces its own
        // io error (kind and source), a malformed one is invalid data carrying the
        // parse error (which names the offending line) as its source.
        Some(path) => Some(Acl::from_policy_file(path).map_err(|e| match e {
            PolicyFileError::Io(io) => io,
            PolicyFileError::Parse(parse) => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, parse)
            }
        })?),
        None => None,
    };
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("crdtsync serving on ws://{addr}");
    // The server never mints ops; its replicas only merge, so a fixed id is fine.
    let server = ClientId::from_bytes([0; 16]);
    match policy {
        // A declared policy gates every authenticated actor; without one, the
        // runtime's default permits them all (dev mode). The actor each rule sees
        // comes from the dev-mode verifier above — see the module note on why
        // `actor:` rules need a real verifier to be enforceable.
        Some(acl) => {
            serve_with_authorizer(
                listener,
                server,
                store,
                ServeConfig::default(),
                Box::new(AllowAll),
                Box::new(acl),
            )
            .await
        }
        None => serve(listener, server, store).await,
    }
}
