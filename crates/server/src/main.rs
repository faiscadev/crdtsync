//! The crdtsync sync server binary.
//!
//! Binds `CRDTSYNC_ADDR` (default `127.0.0.1:9000`) and serves the wire
//! protocol over WebSocket. Set `CRDTSYNC_DATA_DIR` to persist each room's op
//! log there and replay it on restart; unset, the replicas are in-memory. Set
//! `CRDTSYNC_POLICY_FILE` to enforce a declarative authorization policy; unset,
//! every authenticated actor is permitted.

use std::env::VarError;

use crdtsync_core::ClientId;
use crdtsync_server::acl::Acl;
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
        Some(path) => Some(
            Acl::from_policy_file(path)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
        ),
        None => None,
    };
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("crdtsync serving on ws://{addr}");
    // The server never mints ops; its replicas only merge, so a fixed id is fine.
    let server = ClientId::from_bytes([0; 16]);
    match policy {
        // A declared policy is enforced; without one, the runtime's default
        // permits every authenticated actor (dev mode).
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
