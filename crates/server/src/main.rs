//! The crdtsync sync server binary.
//!
//! Binds `CRDTSYNC_ADDR` (default `127.0.0.1:9000`) and serves the wire
//! protocol over WebSocket. Set `CRDTSYNC_DATA_DIR` to persist each room's op
//! log there and replay it on restart; unset, the replicas are in-memory. Set
//! `CRDTSYNC_POLICY_FILE` to enforce a declarative authorization policy; unset,
//! every authenticated actor is permitted. Set `CRDTSYNC_CREDENTIALS_FILE` to
//! authenticate actors against a static secret-token table; unset, the dev-mode
//! verifier admits any credential. Set `CRDTSYNC_WEBHOOK_URL` to POST each
//! room-bearing lifecycle event to an HTTP endpoint (best-effort, off the commit
//! path), with `CRDTSYNC_WEBHOOK_SECRET` attached as a shared-secret header for
//! the receiver to verify; unset, no webhook fires. Set `CRDTSYNC_CLUSTER_PEERS`
//! to a comma-separated list of peer advertise addresses to join a horizontal
//! cluster — the node holds its member view and placement, deriving its own id
//! from `CRDTSYNC_NODE_ID` or `CRDTSYNC_ADVERTISE_ADDR`, with
//! `CRDTSYNC_REPLICATION_FACTOR` overriding the per-room replica count; unset, the
//! node is single-node and serves every room locally. Set `CRDTSYNC_BLOB_ADDR` to
//! serve the out-of-band blob upload/fetch HTTP plane there — a client stores a
//! large blob and fetches it by handle; its store root is `CRDTSYNC_BLOB_ROOT` or
//! a `blobs/` subdirectory of `CRDTSYNC_DATA_DIR`, and requests authenticate
//! through the same verifier as the data plane; unset, no blob plane. Set
//! `CRDTSYNC_TLS_CERT` + `CRDTSYNC_TLS_KEY` to PEM cert-chain + private-key paths
//! to terminate TLS at the listener — the wire protocol then runs over an
//! encrypted stream (`wss://`); both must be set together, and a malformed or
//! mismatched pair fails startup loudly rather than downgrading to plaintext.
//! Unset, the listener binds plaintext exactly as before.
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
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crdtsync_core::ClientId;
use crdtsync_server::acl::{Acl, PolicyFileError};
use crdtsync_server::auth::CredentialsFileError;
use crdtsync_server::membership::{Membership, MembershipConfigError};
use crdtsync_server::runtime::{serve_with_authorizer_handle, ServeConfig};
use crdtsync_server::{
    serve_admin, serve_blobs, server_config_from_pem, server_config_from_pem_with_client_ca,
    AllowAll, Authorizer, BlobStore, PermitAll, SchemaRegistry, StaticTokens, Store,
    TlsConfigError, Verifier, WebhookConfig, DEFAULT_REPLICATION_FACTOR,
};
use tokio::net::TcpListener;
use tokio_rustls::rustls;

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
fn verifier() -> std::io::Result<Box<dyn Verifier + Send + Sync>> {
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
fn authorizer() -> std::io::Result<Box<dyn Authorizer + Send + Sync>> {
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

/// The outbound webhook config for the run: an endpoint from `CRDTSYNC_WEBHOOK_URL`,
/// carrying the optional shared secret `CRDTSYNC_WEBHOOK_SECRET` the receiver
/// checks. Unset URL registers no webhook sink, so events cost nothing.
fn webhook() -> std::io::Result<Option<WebhookConfig>> {
    match path_var("CRDTSYNC_WEBHOOK_URL")? {
        Some(url) => Ok(Some(WebhookConfig {
            url,
            secret: path_var("CRDTSYNC_WEBHOOK_SECRET")?,
        })),
        None => Ok(None),
    }
}

/// The node's static cluster membership for the run. Set `CRDTSYNC_CLUSTER_PEERS`
/// to a comma-separated list of peer advertise addresses (`host:port,...`) to
/// join a cluster; the node's own identity comes from `CRDTSYNC_NODE_ID` if set,
/// else its `CRDTSYNC_ADVERTISE_ADDR`. `CRDTSYNC_REPLICATION_FACTOR` overrides the
/// default per-room replica count. Unset `CRDTSYNC_CLUSTER_PEERS` is single-node
/// mode — no cluster, every room served locally, exactly the current behavior.
/// A malformed peer list or replication factor is a clean startup error.
fn membership() -> std::io::Result<Option<Membership>> {
    let Some(peers) = path_var("CRDTSYNC_CLUSTER_PEERS")? else {
        return Ok(None);
    };
    let node_id = path_var("CRDTSYNC_NODE_ID")?;
    let advertise = path_var("CRDTSYNC_ADVERTISE_ADDR")?;
    let factor = match path_var("CRDTSYNC_REPLICATION_FACTOR")? {
        Some(raw) => match raw.trim().parse::<usize>() {
            Ok(0) | Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "CRDTSYNC_REPLICATION_FACTOR must be a positive integer",
                ))
            }
            Ok(n) => n,
        },
        None => DEFAULT_REPLICATION_FACTOR,
    };
    let m =
        Membership::from_static_config(node_id.as_deref(), advertise.as_deref(), &peers, factor)
            .map_err(|e| {
                let kind = match e {
                    MembershipConfigError::EmptyPeer | MembershipConfigError::MissingSelfId => {
                        std::io::ErrorKind::InvalidInput
                    }
                };
                std::io::Error::new(kind, e)
            })?;
    Ok(Some(m))
}

/// The TLS termination config for the run: a `rustls::ServerConfig` loaded from
/// the PEM cert at `CRDTSYNC_TLS_CERT` + key at `CRDTSYNC_TLS_KEY` when both are
/// set, else `None` (the listener binds plaintext, unchanged). Setting only one
/// of the pair is a clean startup error — a half-configured TLS is a
/// misconfiguration, not a plaintext fall back. A malformed or mismatched
/// cert/key fails startup loudly rather than silently downgrading to plaintext.
///
/// Setting `CRDTSYNC_TLS_CLIENT_CA` to a PEM trust-anchor bundle additionally
/// turns on mTLS: every client must then present a certificate that chains to one
/// of those roots, verified at the handshake (fail-closed), and its SAN/CN becomes
/// the connection's authenticated actor. It requires TLS to be enabled — a client
/// CA with no server cert/key is a clean startup error.
fn tls_config() -> std::io::Result<Option<Arc<rustls::ServerConfig>>> {
    let client_ca = path_var("CRDTSYNC_TLS_CLIENT_CA")?;
    let build = |e: TlsConfigError| {
        let kind = match &e {
            TlsConfigError::Io { source, .. } => source.kind(),
            _ => std::io::ErrorKind::InvalidData,
        };
        std::io::Error::new(kind, e)
    };
    match (
        path_var("CRDTSYNC_TLS_CERT")?,
        path_var("CRDTSYNC_TLS_KEY")?,
    ) {
        (Some(cert), Some(key)) => {
            let config = match client_ca {
                Some(ca) => server_config_from_pem_with_client_ca(cert, key, ca).map_err(build)?,
                None => server_config_from_pem(cert, key).map_err(build)?,
            };
            Ok(Some(config))
        }
        (None, None) if client_ca.is_some() => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CRDTSYNC_TLS_CLIENT_CA requires CRDTSYNC_TLS_CERT and CRDTSYNC_TLS_KEY (mTLS needs TLS)",
        )),
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CRDTSYNC_TLS_CERT and CRDTSYNC_TLS_KEY must both be set to enable TLS",
        )),
    }
}

/// The blob store root for the run: `CRDTSYNC_BLOB_ROOT` if set, else a `blobs`
/// subdirectory of `CRDTSYNC_DATA_DIR`. Serving blobs (`CRDTSYNC_BLOB_ADDR`)
/// without either is a clean startup error — there is nowhere to persist blob
/// bytes.
fn blob_root() -> std::io::Result<PathBuf> {
    if let Some(root) = path_var("CRDTSYNC_BLOB_ROOT")? {
        return Ok(PathBuf::from(root));
    }
    if let Some(dir) = path_var("CRDTSYNC_DATA_DIR")? {
        return Ok(PathBuf::from(dir).join("blobs"));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "CRDTSYNC_BLOB_ADDR requires CRDTSYNC_BLOB_ROOT or CRDTSYNC_DATA_DIR",
    ))
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
    let tls = tls_config()?;
    let listener = TcpListener::bind(&addr).await?;
    let scheme = if tls.is_some() { "wss" } else { "ws" };
    eprintln!("crdtsync serving on {scheme}://{addr}");
    // One schema registry, shared between the data plane (which resolves each
    // handshake against it) and the admin plane (which registers into it), so a
    // registration is at once visible to connecting clients. Empty until the
    // admin plane writes it — with no admin plane, every connection is a relay.
    let schema = Arc::new(Mutex::new(SchemaRegistry::new()));
    // The server never mints ops; its replicas only merge, so a fixed id is fine.
    // Both seams default to their permissive dev-mode value when unconfigured, so
    // one serve path covers every combination.
    // A handle onto the running registry accompanies the data plane: the blob
    // plane, an out-of-band listener that owns no replicas, resolves each fetch's
    // reference-site read authorization through it against the same live rooms.
    let (blob_authority, data) = serve_with_authorizer_handle(
        listener,
        ClientId::from_bytes([0; 16]),
        store,
        ServeConfig {
            schema: schema.clone(),
            webhook: webhook()?,
            membership: membership()?,
            tls,
            ..ServeConfig::default()
        },
        verifier()?,
        authorizer()?,
    )
    .await?;

    // Every plane the node serves runs concurrently over the shared runtime;
    // the first to error stops the process. The data plane always runs; the
    // control-plane HTTP listeners are opt-in.
    let mut servers: Vec<Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>> =
        vec![Box::pin(data)];

    // The schema-registration admin plane is a separate control-plane listener,
    // enabled only when CRDTSYNC_ADMIN_ADDR is set (unset → relay-only, no
    // registration). It gates registration through the same verifier + policy as
    // the data plane, differing only in the action + resource it checks.
    if let Some(admin_addr) = path_var("CRDTSYNC_ADMIN_ADDR")? {
        let admin_listener = TcpListener::bind(&admin_addr).await?;
        eprintln!("crdtsync admin on http://{admin_addr}");
        servers.push(Box::pin(serve_admin(
            admin_listener,
            verifier()?,
            authorizer()?,
            schema,
        )));
    }

    // The blob upload/fetch plane is the out-of-band byte channel a client uses
    // to store a large blob and fetch it by handle. Enabled only when
    // CRDTSYNC_BLOB_ADDR is set; its store root is CRDTSYNC_BLOB_ROOT or a
    // `blobs/` subdir of CRDTSYNC_DATA_DIR. It gates upload/fetch through the same
    // verifier as the data plane; per-reference authorization is a later slice.
    if let Some(blob_addr) = path_var("CRDTSYNC_BLOB_ADDR")? {
        let store = Arc::new(Mutex::new(BlobStore::open(blob_root()?)?));
        let blob_listener = TcpListener::bind(&blob_addr).await?;
        eprintln!("crdtsync blobs on http://{blob_addr}");
        servers.push(Box::pin(serve_blobs(
            blob_listener,
            verifier()?,
            store,
            Arc::new(blob_authority),
        )));
    }

    futures_util::future::try_join_all(servers).await?;
    Ok(())
}
