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
//! node is single-node and serves every room locally. A clustered node also needs
//! `CRDTSYNC_CLUSTER_SECRET` — the shared credential that admits a link to this
//! node's replication/gossip plane, at least 32 bytes and identical across the
//! cluster (`openssl rand -hex 32`); without it a node with peers refuses to start.
//! A member's advertise address declares the transport its peers dial it over —
//! `wss://host:port` terminates TLS, `ws://host:port` or a bare `host:port` does
//! not — so a cluster part-way through a TLS rollout may hold both. Dialing a
//! `wss://` member needs `CRDTSYNC_CLUSTER_CA`, a PEM trust bundle the member's
//! certificate must chain to (nothing ambient stands in for it: the cluster secret
//! is a bearer credential written over that link); `CRDTSYNC_CLUSTER_CLIENT_CERT` +
//! `CRDTSYNC_CLUSTER_CLIENT_KEY` additionally present this node's own identity, so
//! one handshake authenticates both ends when peers also set
//! `CRDTSYNC_TLS_CLIENT_CA`. Set `CRDTSYNC_CLUSTER_REQUIRE_TLS=1` to declare the
//! rollout finished and refuse a plaintext member outright. A node whose advertised
//! transport disagrees with the one it terminates refuses to start rather than
//! binding a listener its own peers cannot speak to.
//!
//! Those peer certificates are also what tell one member from another. The cluster
//! secret is deployment-wide, so it says a link belongs to *a* member and never
//! which; with per-node certificates a link is bound to the member whose advertise
//! **host** its certificate names, and the gates downstream of admission —
//! replication, leadership, membership growth, durability watermarks — decide
//! against that member. Only a `dNSName` or `iPAddress` SAN names a host, never an
//! e-mail, a URI or the Common Name, and never a wildcard; the conventional
//! DNS-plus-IP node certificate works either way its address is spelled.
//!
//! To set it up: issue each node a certificate whose SAN is that node's own advertise
//! host, point `CRDTSYNC_CLUSTER_CLIENT_CERT`/`_KEY` at it, and set
//! `CRDTSYNC_TLS_CLIENT_CA` + `CRDTSYNC_TLS_CLIENT_AUTH=request` so inbound peer links
//! carry a certificate while ordinary clients still connect without one (the default
//! `require` would reject every certless client). **`CRDTSYNC_TLS_CLIENT_CA` is the
//! authority for peer identity**, not `CRDTSYNC_CLUSTER_CA` — the certificate is
//! verified by the listener, so that bundle must be no broader than the cluster's own
//! CA, or any certificate it issues naming a member's host can speak as that member.
//! Once every node has an identity, set `CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY=1` to
//! refuse any peer link that presents none; a node that requires it while verifying no
//! client certificate, while presenting no identity of its own, or while holding a
//! member whose advertise address names no host, refuses to start. Two refusals do not
//! wait for that policy: an advertise address that names no host at all is refused
//! outright (no peer could dial it), and a node that sets `CRDTSYNC_TLS_CLIENT_CA`
//! while presenting a peer certificate that names no host matching the id it dials
//! under refuses to start too, since every peer applying the same binding refuses its
//! links. A node holding such a certificate *without* `CRDTSYNC_TLS_CLIENT_CA` cannot
//! tell whether its peers ask for one, so it warns and starts — **keep the client-CA
//! and peer-certificate settings uniform across the cluster**, or a node may run on
//! only that warning while its peers refuse every link it opens. A member that
//! advertises plaintext still identifies itself — the link carrying its identity here
//! is the one it dials — so this requires no TLS rollout of its own. A certificate
//! that *is* presented is decisive either way, so a cluster already running peer mTLS
//! wants each node's certificate naming that node's advertise host before the upgrade. Set `CRDTSYNC_BLOB_ADDR` to
//! serve the out-of-band blob upload/fetch HTTP plane there — a client stores a
//! large blob and fetches it by handle; its store root is `CRDTSYNC_BLOB_ROOT` or
//! a `blobs/` subdirectory of `CRDTSYNC_DATA_DIR`, and requests authenticate
//! through the same verifier as the data plane; unset, no blob plane. Set
//! `CRDTSYNC_TLS_CERT` + `CRDTSYNC_TLS_KEY` to PEM cert-chain + private-key paths
//! to terminate TLS at the listener — the wire protocol then runs over an
//! encrypted stream (`wss://`); both must be set together, and a malformed or
//! mismatched pair fails startup loudly rather than downgrading to plaintext.
//! Unset, the listener binds plaintext exactly as before. Set `CRDTSYNC_ZONE_KEY`
//! to 64 hex digits (a 32-byte zone-master key) to enable the authorized
//! cross-zone-move escape hatch — the server seals a capability token per
//! authorized move under it; unset, every cross-zone move stays rejected.
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
    client_config_from_pem, client_config_from_pem_with_identity, host_names_from_pem, serve_admin,
    serve_audit, serve_blobs, server_config_from_pem, server_config_from_pem_with_client_ca_mode,
    AllowAll, AuditLog, Authorizer, BlobStore, ClientAuthMode, PermitAll, SchemaRegistry,
    StaticTokens, Store, SystemClock, TlsConfigError, Verifier, WebhookConfig,
    DEFAULT_REPLICATION_FACTOR,
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

/// The node's cluster secret for the run, read from `CRDTSYNC_CLUSTER_SECRET`. It
/// is the whole of a node's peer authentication — what admits a link to this node's
/// replication, gossip and probe plane — so every node in one cluster carries the
/// same value and nobody else does. Required alongside `CRDTSYNC_CLUSTER_PEERS`, and
/// at least `MIN_CLUSTER_SECRET_LEN` bytes; `serve` refuses to start otherwise, so a
/// clustered node never comes up with an open or a closed peer plane by accident.
/// Generate one with `openssl rand -hex 32`.
///
/// Surrounding whitespace is trimmed, as the peer list's entries are: the secret is
/// compared byte for byte, so a value sourced from a file or a mounted k8s secret —
/// which carries a trailing newline — would otherwise differ from the same value
/// exported in a shell and the cluster would simply never converge. A secret that is
/// only whitespace is no secret and reads as unset.
fn cluster_secret() -> std::io::Result<Option<Vec<u8>>> {
    Ok(path_var("CRDTSYNC_CLUSTER_SECRET")?
        .map(|raw| raw.trim().as_bytes().to_vec())
        .filter(|secret| !secret.is_empty()))
}

/// The TLS termination config for the run: a `rustls::ServerConfig` loaded from
/// the PEM cert at `CRDTSYNC_TLS_CERT` + key at `CRDTSYNC_TLS_KEY` when both are
/// set, else `None` (the listener binds plaintext, unchanged). Setting only one
/// of the pair is a clean startup error — a half-configured TLS is a
/// misconfiguration, not a plaintext fall back. A malformed or mismatched
/// cert/key fails startup loudly rather than silently downgrading to plaintext.
///
/// Setting `CRDTSYNC_TLS_CLIENT_CA` to a PEM trust-anchor bundle additionally
/// turns on mTLS: a client presenting a certificate that chains to one of those
/// roots authenticates as its SAN/CN actor. `CRDTSYNC_TLS_CLIENT_AUTH` selects the
/// strictness — `require` (the default) rejects a certless/untrusted client at the
/// handshake (fail-closed); `request` is opportunistic, still validating a
/// presented cert but letting a client with *no* cert connect and fall through to
/// the ordinary certless session path. An unrecognized mode is a clean startup
/// error. mTLS requires TLS to be enabled — a client CA with no server cert/key is
/// a clean startup error.
/// It is paired with whether it verifies client certificates, because
/// `rustls::ServerConfig` does not expose its verifier and the peer-identity policy
/// needs to know whether an inbound link can carry an identity at all.
fn tls_config_with_client_auth() -> std::io::Result<(Option<Arc<rustls::ServerConfig>>, bool)> {
    let client_ca = path_var("CRDTSYNC_TLS_CLIENT_CA")?;
    let verifies_clients = client_ca.is_some();
    let build = |e: TlsConfigError| {
        let kind = match &e {
            TlsConfigError::Io { source, .. } => source.kind(),
            _ => std::io::ErrorKind::InvalidData,
        };
        std::io::Error::new(kind, e)
    };
    let client_auth =
        ClientAuthMode::parse(path_var("CRDTSYNC_TLS_CLIENT_AUTH")?.as_deref()).map_err(build)?;
    match (
        path_var("CRDTSYNC_TLS_CERT")?,
        path_var("CRDTSYNC_TLS_KEY")?,
    ) {
        (Some(cert), Some(key)) => {
            let config = match client_ca {
                Some(ca) => server_config_from_pem_with_client_ca_mode(cert, key, ca, client_auth)
                    .map_err(build)?,
                None => server_config_from_pem(cert, key).map_err(build)?,
            };
            Ok((Some(config), verifies_clients))
        }
        (None, None) if client_ca.is_some() => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CRDTSYNC_TLS_CLIENT_CA requires CRDTSYNC_TLS_CERT and CRDTSYNC_TLS_KEY (mTLS needs TLS)",
        )),
        // A client CA with no TLS is refused above, so a node with no listener
        // certificate verifies no client certificate either.
        (None, None) => Ok((None, false)),
        (Some(_), None) | (None, Some(_)) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CRDTSYNC_TLS_CERT and CRDTSYNC_TLS_KEY must both be set to enable TLS",
        )),
    }
}

/// The client-side TLS config for this run's outbound peer dials: the trust
/// anchors at `CRDTSYNC_CLUSTER_CA` a TLS member is authenticated against before
/// the link presents the cluster secret, optionally presenting this node's own
/// identity from `CRDTSYNC_CLUSTER_CLIENT_CERT` + `CRDTSYNC_CLUSTER_CLIENT_KEY` so
/// one handshake authenticates both ends. Unset, the node dials plaintext members
/// only — and refuses to start if any member advertises `wss://`.
///
/// The client identity is deliberately its own pair rather than the listener's
/// cert: a server certificate commonly carries `serverAuth` alone, so reusing it
/// would work in a lab and be rejected at the handshake in a deployment that issues
/// its certificates properly. Setting only one half of the pair, or either half
/// without the trust bundle, is a clean startup error.
#[allow(clippy::type_complexity)]
fn peer_tls_config() -> std::io::Result<(Option<Arc<rustls::ClientConfig>>, Option<Vec<Vec<u8>>>)> {
    let invalid =
        |msg: &str| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg.to_string());
    let build = |e: TlsConfigError| {
        let kind = match &e {
            TlsConfigError::Io { source, .. } => source.kind(),
            _ => std::io::ErrorKind::InvalidData,
        };
        std::io::Error::new(kind, e)
    };
    let identity =
        match (
            path_var("CRDTSYNC_CLUSTER_CLIENT_CERT")?,
            path_var("CRDTSYNC_CLUSTER_CLIENT_KEY")?,
        ) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => return Err(invalid(
                "CRDTSYNC_CLUSTER_CLIENT_CERT and CRDTSYNC_CLUSTER_CLIENT_KEY must both be set to \
                 present a peer client identity",
            )),
        };
    match (path_var("CRDTSYNC_CLUSTER_CA")?, identity) {
        (Some(ca), Some((cert, key))) => {
            // The hosts this node's own certificate names, read from the same file the
            // dials present, so the peer-identity policy can check them against this
            // node's advertise address before anything dials.
            let hosts = host_names_from_pem(&cert).map_err(build)?;
            Ok((
                Some(client_config_from_pem_with_identity(ca, cert, key).map_err(build)?),
                Some(hosts),
            ))
        }
        (Some(ca), None) => Ok((Some(client_config_from_pem(ca).map_err(build)?), None)),
        (None, Some(_)) => Err(invalid(
            "CRDTSYNC_CLUSTER_CLIENT_CERT requires CRDTSYNC_CLUSTER_CA: a peer identity is only \
             presented on a link whose far end this node can authenticate",
        )),
        (None, None) => Ok((None, None)),
    }
}

/// Whether this deployment refuses to dial a member that advertises plaintext,
/// from `CRDTSYNC_CLUSTER_REQUIRE_TLS` — how an operator declares a TLS rollout
/// finished. A cluster may otherwise mix transports, which is the only way to reach
/// an all-TLS cluster without restarting every node at one instant.
///
/// Absence is `false`; `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off` select it
/// (case-insensitively). Any other value is a clean startup error — resolving an
/// unrecognized value would resolve it to the *permissive* setting, which is the
/// one an operator setting this variable is trying to leave.
fn require_peer_tls() -> std::io::Result<bool> {
    parse_bool_var(
        "CRDTSYNC_CLUSTER_REQUIRE_TLS",
        path_var("CRDTSYNC_CLUSTER_REQUIRE_TLS")?.as_deref(),
    )
}

/// Whether this deployment refuses a peer link that presents no verified client
/// certificate naming a member, from `CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY` — how
/// an operator declares that every member is identified rather than merely holding
/// the deployment-wide cluster secret. Off by default, because a cluster cannot
/// acquire per-node certificates at one instant any more than it can acquire TLS at
/// one instant; the node refuses to start with it on until it both verifies client
/// certificates and presents one of its own.
fn require_peer_identity() -> std::io::Result<bool> {
    parse_bool_var(
        "CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY",
        path_var("CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY")?.as_deref(),
    )
}

/// Parse a boolean environment value for `name`: absent is off, and a value that is
/// neither spelling of a boolean is a startup error rather than the permissive
/// setting — a typo must not silently disable a policy an operator asked for.
fn parse_bool_var(name: &str, value: Option<&str>) -> std::io::Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} must be a boolean, got `{other}`"),
        )),
    }
}

/// The zone-master key for the run: the 32 bytes sealing cross-zone-move capability
/// tokens, read from `CRDTSYNC_ZONE_KEY` as 64 hex digits, else `None` (the
/// cross-zone-move escape hatch stays off — every crossing rejected). A key of the
/// wrong length or with a non-hex digit is a clean startup error, not a silent
/// disable — a misconfigured secret must fail loudly. Server config, like the TLS
/// cert; the key never leaves the server.
fn zone_key() -> std::io::Result<Option<[u8; 32]>> {
    match path_var("CRDTSYNC_ZONE_KEY")? {
        Some(hex) => decode_zone_key(&hex).map(Some),
        None => Ok(None),
    }
}

/// Decode a 64-hex-digit zone-master key to its 32 bytes. Total — a value of the
/// wrong length, or one holding any non-hex byte (a non-ASCII byte included), is a
/// clean `InvalidInput`, never a panic: the length gate counts bytes and the pairs
/// are sliced off the byte string, so a multi-byte character neither passes the
/// length check nor splits a hex pair at a char boundary.
fn decode_zone_key(hex: &str) -> std::io::Result<[u8; 32]> {
    let invalid =
        |msg: &str| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg.to_string());
    let digits = hex.as_bytes();
    if digits.len() != 64 {
        return Err(invalid(
            "CRDTSYNC_ZONE_KEY must be 64 hex digits (32 bytes)",
        ));
    }
    let unhex = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    };
    let mut key = [0u8; 32];
    for (byte, pair) in key.iter_mut().zip(digits.chunks_exact(2)) {
        let (Some(hi), Some(lo)) = (unhex(pair[0]), unhex(pair[1])) else {
            return Err(invalid("CRDTSYNC_ZONE_KEY must be valid hex"));
        };
        *byte = (hi << 4) | lo;
    }
    Ok(key)
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
    let (tls, client_cert_verification) = tls_config_with_client_auth()?;
    let (peer_tls, peer_client_identity) = peer_tls_config()?;
    let listener = TcpListener::bind(&addr).await?;
    let scheme = if tls.is_some() { "wss" } else { "ws" };
    eprintln!("crdtsync serving on {scheme}://{addr}");
    // One schema registry, shared between the data plane (which resolves each
    // handshake against it) and the admin plane (which registers into it), so a
    // registration is at once visible to connecting clients. Empty until the
    // admin plane writes it — with no admin plane, every connection is a relay.
    let schema = Arc::new(Mutex::new(SchemaRegistry::new()));
    // The append-only audit trail, enabled only when CRDTSYNC_AUDIT_LOG names its
    // file. One shared handle backs three planes: the data plane (its Audited
    // authorizer persists each security-relevant decision + the connect / export /
    // version-read events), the blob plane (records an export on each fetch), and the
    // operator query plane below. Unset, the node runs unaudited.
    let audit_log = match path_var("CRDTSYNC_AUDIT_LOG")? {
        Some(path) => Some(Arc::new(AuditLog::open(path, Arc::new(SystemClock))?)),
        None => None,
    };
    // The server never mints ops; its replicas only merge, so a fixed id is fine.
    // It is reserved rather than secret: the op gate refuses a client batch
    // authored under it, so no write on the client path enters a room's log as
    // the node.
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
            cluster_secret: cluster_secret()?,
            tls,
            peer_tls,
            require_peer_tls: require_peer_tls()?,
            require_peer_identity: require_peer_identity()?,
            client_cert_verification,
            peer_client_identity,
            zone_key: zone_key()?,
            audit_log: audit_log.clone(),
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
            audit_log.clone(),
        )));
    }

    // The operator audit-query plane is a read-only control-plane listener over the
    // append-only trail, enabled only when both a trail (CRDTSYNC_AUDIT_LOG) and its
    // address (CRDTSYNC_AUDIT_ADDR) are set. It gates each query through the same
    // verifier + policy as the data plane, requiring Read on the reserved `$audit`
    // app resource — so the trail is never exposed to an app client.
    if let (Some(audit_addr), Some(log)) = (path_var("CRDTSYNC_AUDIT_ADDR")?, audit_log) {
        let audit_listener = TcpListener::bind(&audit_addr).await?;
        eprintln!("crdtsync audit query on http://{audit_addr}");
        servers.push(Box::pin(serve_audit(
            audit_listener,
            verifier()?,
            authorizer()?,
            log,
        )));
    }

    futures_util::future::try_join_all(servers).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_zone_key, parse_bool_var};

    #[test]
    fn a_valid_64_hex_key_decodes() {
        let hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let key = decode_zone_key(hex).expect("valid hex decodes");
        assert_eq!(key[0], 0x00);
        assert_eq!(key[1], 0x11);
        assert_eq!(key[31], 0xff);
    }

    #[test]
    fn a_wrong_length_key_is_a_clean_error() {
        assert!(decode_zone_key("").is_err());
        assert!(decode_zone_key("abcd").is_err());
        // 63 and 65 digits both rejected.
        assert!(decode_zone_key(&"a".repeat(63)).is_err());
        assert!(decode_zone_key(&"a".repeat(65)).is_err());
    }

    #[test]
    fn a_non_hex_digit_is_a_clean_error() {
        // 64 ASCII chars, one not a hex digit.
        let mut hex = "a".repeat(63);
        hex.push('g');
        assert!(decode_zone_key(&hex).is_err());
    }

    #[test]
    fn a_non_ascii_64_byte_value_errors_without_panicking() {
        // 62 ASCII hex digits + one 2-byte char = 64 bytes but a non-hex,
        // multi-byte tail. Must be a clean error, never a char-boundary panic.
        let mut hex = "a".repeat(62);
        hex.push('é'); // 2 bytes (0xC3 0xA9)
        assert_eq!(hex.len(), 64);
        assert!(decode_zone_key(&hex).is_err());
    }

    #[test]
    fn an_absent_boolean_setting_is_off() {
        assert!(!parse_bool_var("CRDTSYNC_CLUSTER_REQUIRE_TLS", None).unwrap());
    }

    #[test]
    fn a_boolean_setting_reads_either_spelling() {
        for on in ["1", "true", "TRUE", " Yes ", "on"] {
            assert!(
                parse_bool_var("CRDTSYNC_CLUSTER_REQUIRE_TLS", Some(on)).unwrap(),
                "{on}"
            );
        }
        for off in ["0", "false", "No", "off"] {
            assert!(
                !parse_bool_var("CRDTSYNC_CLUSTER_REQUIRE_TLS", Some(off)).unwrap(),
                "{off}"
            );
        }
    }

    /// An unrecognized value resolves to the *permissive* setting if it resolves at
    /// all — which is the one an operator setting one of these variables is leaving.
    /// So it does not resolve.
    #[test]
    fn an_unrecognized_boolean_setting_is_a_startup_error() {
        for bad in ["", "yes please", "2", "require"] {
            let e =
                parse_bool_var("CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY", Some(bad)).expect_err(bad);
            // The refusal names the variable that carried it, so an operator with
            // several set knows which to fix.
            assert!(
                e.to_string()
                    .contains("CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY"),
                "{e}"
            );
        }
    }
}
