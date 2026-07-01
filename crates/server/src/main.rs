//! The crdtsync sync server binary.
//!
//! Binds `CRDTSYNC_ADDR` (default `127.0.0.1:9000`) and serves the wire
//! protocol over WebSocket.

use std::env::VarError;

use crdtsync_core::ClientId;
use crdtsync_server::runtime::serve;
use tokio::net::TcpListener;

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
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("crdtsync serving on ws://{addr}");
    // The server never mints ops; its replicas only merge, so a fixed id is fine.
    serve(listener, ClientId::from_bytes([0; 16])).await
}
