//! The schema-registration admin surface — the control-plane operation an app
//! owner's CI performs to register a schema version.
//!
//! Registration is separate from the data-plane sync connection but reuses its
//! trust seams: the request's credential authenticates to an [`Identity`] via the
//! [`Verifier`], and the [`Authorizer`] decides the `RegisterSchema` action on
//! the request's [`Resource::App`] — the app-admin meta-auth, distinct from any
//! room action. Only a permitted identity reaches the [`SchemaRegistry`].
//!
//! [`register_schema`] decides over an already-decoded request, running the trust
//! seams lock-free and locking the shared registry only for the write. The
//! HTTP transport is axum over the tokio runtime the server already runs — the
//! admin plane is an untrusted network boundary, so its HTTP/1.1 parsing is
//! hyper's (battle-tested against request smuggling and framing edge cases)
//! rather than hand-rolled; the crate's dep-minimal boundary is `core`, which
//! this never touches.
//!
//! [`Identity`]: crate::auth::Identity

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;

use crate::audit::{AuditLog, AuditQuery, AuditResource, Decision};
use crate::auth::{Identity, Verifier};
use crate::authz::{Action, Authorizer, Resource};
use crate::blobs::{self, BlobStore};
use crate::schema_registry::{RegisterError, Registered, SchemaRegistry};

/// A decoded registration request: which app and version, the schema and
/// migration bytes to lock, and the credential presented on the admin transport
/// (absent if none was supplied).
pub struct RegisterRequest<'a> {
    pub app_id: &'a [u8],
    pub version: u32,
    pub schema: &'a [u8],
    pub migration: &'a [u8],
    pub credential: Option<&'a [u8]>,
}

/// The outcome of an admin registration, which the transport maps to a status.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegisterOutcome {
    /// Registered — either a new version or an idempotent no-op retry.
    Accepted(Registered),
    /// No credential was presented, or the verifier rejected the one that was:
    /// the caller is not an authenticated identity.
    Unauthenticated,
    /// An authenticated identity without the `register_schema` capability on the
    /// request's app.
    Forbidden,
    /// The registry refused the chain write — a gap, a backward version, or a
    /// content change under a locked version.
    Rejected(RegisterError),
}

/// Register a schema version on behalf of `req`'s credential: authenticate it,
/// authorize `RegisterSchema` on the request's app, then append to `registry` —
/// authenticate → authorize → register, the order every data-plane enforcement
/// point uses, so an unauthenticated or forbidden request never reaches the
/// registry.
pub fn register_schema(
    req: &RegisterRequest,
    verifier: &dyn Verifier,
    authorizer: &dyn Authorizer,
    registry: &Mutex<SchemaRegistry>,
) -> RegisterOutcome {
    let Some(credential) = req.credential else {
        return RegisterOutcome::Unauthenticated;
    };
    let Some(identity) = verifier.verify(credential) else {
        return RegisterOutcome::Unauthenticated;
    };
    let resource = Resource::App(req.app_id);
    if !authorizer.authorize(&identity, Action::RegisterSchema, &resource) {
        return RegisterOutcome::Forbidden;
    }
    // Authentication and authorization ran lock-free above; the registry — shared
    // with the data plane, whose handshake reads it — is locked only for the
    // write, so a slow verifier cannot stall data-plane message processing. Recover
    // a poisoned lock rather than propagate the panic across the plane boundary:
    // the registry validates a version before it mutates, so a panic in either
    // plane leaves its map intact.
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match registry.register(req.app_id, req.version, req.schema, req.migration) {
        Ok(registered) => RegisterOutcome::Accepted(registered),
        Err(error) => RegisterOutcome::Rejected(error),
    }
}

/// The largest schema body accepted, guarding the admin plane against an
/// oversized (or falsely-declared) body.
const MAX_BODY: usize = 1 << 20;

/// How long one admin request may take before it is dropped — a slow or stalled
/// client cannot wedge the plane.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The registry plus the trust seams every request consults. The registry is the
/// same `Arc<Mutex<SchemaRegistry>>` the data plane resolves handshakes against,
/// so a registration is at once visible there. Registration is rare, so a plain
/// mutex around the whole registry is enough — no per-app locking; the write is
/// taken only for the pure `register`, after the trust seams run lock-free.
struct AdminState {
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    registry: Arc<Mutex<SchemaRegistry>>,
}

/// The admin control-plane router: the single registration route, a body cap
/// (over-large → `413`), and a per-request timeout (stalled → `408`). `registry`
/// is shared with the data plane. Exposed so it can be driven in-process by tests
/// without a socket.
pub fn admin_router(
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    registry: Arc<Mutex<SchemaRegistry>>,
) -> Router {
    let state = Arc::new(AdminState {
        verifier,
        authorizer,
        registry,
    });
    Router::new()
        .route("/apps/{app_id}/schemas/{version}", post(register))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .with_state(state)
}

/// Serve the schema-registration admin plane on `listener`: a dedicated
/// control-plane HTTP endpoint separate from the data-plane sync socket. axum
/// over hyper handles connection lifecycle, HTTP/1.1 framing, method / route
/// matching, and status responses; this crate supplies only the one route and
/// its trust decision.
pub async fn serve_admin(
    listener: TcpListener,
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    registry: Arc<Mutex<SchemaRegistry>>,
) -> std::io::Result<()> {
    let router = admin_router(verifier, authorizer, registry);
    axum::serve(listener, router).await
}

/// `POST /apps/{app_id}/schemas/{version}` — the only route. The credential is
/// the `Authorization` header verbatim (the data plane's carrier), the body is
/// the schema bytes. A non-`u32` version never reaches here (axum answers `400`);
/// a non-`POST` method or any other path is axum's `405` / `404`.
async fn register(
    State(state): State<Arc<AdminState>>,
    Path((app_id, version)): Path<(String, u32)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let req = RegisterRequest {
        app_id: app_id.as_bytes(),
        version,
        schema: &body,
        migration: b"",
        credential: headers.get("authorization").map(|v| v.as_bytes()),
    };
    // `register_schema` runs the trust seams lock-free and locks the shared
    // registry only for the write — no lock is held across an axum await here.
    let outcome = register_schema(
        &req,
        state.verifier.as_ref(),
        state.authorizer.as_ref(),
        &state.registry,
    );
    match outcome {
        RegisterOutcome::Accepted(_) => (StatusCode::OK, "registered"),
        RegisterOutcome::Unauthenticated => {
            (StatusCode::UNAUTHORIZED, "a valid credential is required")
        }
        RegisterOutcome::Forbidden => (StatusCode::FORBIDDEN, "not permitted to register this app"),
        RegisterOutcome::Rejected(_) => (
            StatusCode::CONFLICT,
            "registration rejected: not the next contiguous version, or a locked version changed",
        ),
    }
    .into_response()
}

// --- blob upload / fetch --------------------------------------------------

/// The largest blob body the upload route accepts; a body past it is axum's
/// `413`. Sized for out-of-band media a client stores by handle rather than
/// inlining in an op — larger objects, chunking, and range requests are a later
/// slice.
pub const MAX_BLOB_BODY: usize = 8 << 20;

/// The reference-site authorization seam a blob **fetch** consults after
/// authenticating: whether an [`Identity`] may retrieve the blob whose public
/// handle is `blob_id`. A blob is content-addressed and immutable, so authority
/// cannot attach to the bytes — it attaches to the paths that *reference* the
/// blob, and the answer is yes iff the identity holds read authority on at least
/// one live reference (resolved through the same doc-ACL evaluator op redaction
/// uses). Fail-closed: an unreferenced or unreadable id is denied.
///
/// The production implementation ([`RegistryHandle`](crate::runtime::RegistryHandle))
/// queries the running registry actor; tests supply their own. Upload is not gated
/// here — a producer stores bytes before any reference exists; the gate is on fetch.
#[async_trait::async_trait]
pub trait BlobAccess: Send + Sync {
    /// Whether `identity` may fetch the blob handle `blob_id`. Must fail closed on
    /// every ambiguous case (unreferenced id, unreadable reference, unresolvable
    /// lookup) — this is a security-authorization boundary, default DENY.
    async fn may_read_blob(&self, identity: &Identity, blob_id: &[u8; 16]) -> bool;
}

/// A blob-access seam that authorizes every fetch — the dev-mode default, matching
/// the permissive [`PermitAll`](crate::authz::PermitAll) authorizer. A deployment
/// that wants the reference-site gate wires the registry-backed
/// [`RegistryHandle`](crate::runtime::RegistryHandle) instead.
pub struct PermitAllBlobs;

#[async_trait::async_trait]
impl BlobAccess for PermitAllBlobs {
    async fn may_read_blob(&self, _identity: &Identity, _blob_id: &[u8; 16]) -> bool {
        true
    }
}

/// The blob store plus the credential seam every blob request authenticates
/// against and the reference-site [`BlobAccess`] gate a fetch is authorized
/// through. The store is behind a mutex because an upload mutates its handle
/// index; a fetch only reads but shares the one lock — blob traffic is
/// out-of-band and infrequent relative to the op stream, so a single lock is
/// enough.
struct BlobState {
    verifier: Box<dyn Verifier + Send + Sync>,
    store: Arc<Mutex<BlobStore>>,
    access: Arc<dyn BlobAccess>,
    /// The audit trail a blob fetch — a bytes-out-of-band export — records to, when
    /// the deployment enables auditing. `None` leaves the fetch unaudited.
    audit: Option<Arc<AuditLog>>,
}

/// The handle an upload returns: the public id as lowercase hex, the byte size,
/// and whether the blob is small enough to ride inline in an op's ref (so a
/// client may skip a later fetch). The bytes are fetchable by id regardless.
#[derive(Serialize)]
struct BlobHandle {
    id: String,
    size: u64,
    inline: bool,
}

/// The blob upload/fetch router: `POST /blobs` stores a body and returns its
/// [`BlobHandle`]; `GET /blobs/{id}` serves the stored bytes. Both authenticate
/// the `Authorization` credential through `verifier` — a missing or unknown one
/// is `401` — mirroring the schema-register gate. A fetch is additionally
/// authorized through `access`: an authenticated caller reaches the bytes only if
/// it holds read authority on a live reference to the blob (else `403`); upload is
/// authentication-only, since a producer stores bytes before any reference exists.
/// The upload body cap is [`MAX_BLOB_BODY`] (over it → `413`). Exposed so it can be
/// driven in-process by tests without a socket.
pub fn blob_router(
    verifier: Box<dyn Verifier + Send + Sync>,
    store: Arc<Mutex<BlobStore>>,
    access: Arc<dyn BlobAccess>,
    audit: Option<Arc<AuditLog>>,
) -> Router {
    let state = Arc::new(BlobState {
        verifier,
        store,
        access,
        audit,
    });
    Router::new()
        .route("/blobs", post(blob_upload))
        .route("/blobs/{id}", get(blob_fetch))
        .layer(DefaultBodyLimit::max(MAX_BLOB_BODY))
        .with_state(state)
}

/// Serve the blob upload/fetch plane on `listener` — the out-of-band byte channel
/// a client uses to store a large blob and fetch it back by handle, separate from
/// the op-stream data plane. `access` is the reference-site authorization gate a
/// fetch clears (see [`blob_router`]).
pub async fn serve_blobs(
    listener: TcpListener,
    verifier: Box<dyn Verifier + Send + Sync>,
    store: Arc<Mutex<BlobStore>>,
    access: Arc<dyn BlobAccess>,
    audit: Option<Arc<AuditLog>>,
) -> std::io::Result<()> {
    axum::serve(listener, blob_router(verifier, store, access, audit)).await
}

/// Authenticate a blob request's `Authorization` credential to an [`Identity`],
/// or the status to answer with. Mirrors the schema-register gate: no credential,
/// or one the verifier rejects, is `401`. Authentication only — a fetch's
/// reference-site authorization ([`BlobAccess`]) is a separate gate on the
/// resolved identity.
fn authenticate(state: &BlobState, headers: &HeaderMap) -> Result<Identity, StatusCode> {
    let credential = headers
        .get("authorization")
        .map(|v| v.as_bytes())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .verifier
        .verify(credential)
        .ok_or(StatusCode::UNAUTHORIZED)
}

/// `POST /blobs` — store the request body and return its handle. The `Content-Type`
/// header is the blob's mime (defaulting to `application/octet-stream`); the store
/// records the bytes content-addressed and mints a fresh public handle. A body past
/// [`MAX_BLOB_BODY`] never reaches here (axum answers `413`).
async fn blob_upload(
    State(state): State<Arc<BlobState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(status) = authenticate(&state, &headers) {
        return status.into_response();
    }
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    // The store's write is brief blocking IO under the mutex, held across no
    // await — blob traffic is out-of-band and infrequent.
    let stored = {
        let mut store = state.store.lock().unwrap_or_else(|p| p.into_inner());
        store.put_fetchable(&body, &mime)
    };
    match stored {
        Ok(handle) => (
            StatusCode::OK,
            Json(BlobHandle {
                id: blobs::hex(&handle.id),
                size: handle.size,
                inline: handle.inline.is_some(),
            }),
        )
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "blob store write failed").into_response(),
    }
}

/// `GET /blobs/{id}` — serve the stored bytes for a handle. A malformed id is
/// `400` (never a panic); an unknown one is `404`. The store does not persist a
/// blob's mime, so the bytes are served as generic `application/octet-stream`.
///
/// The caller must both authenticate (`401` otherwise) and be authorized to the
/// blob through the reference-site [`BlobAccess`] gate: it may retrieve the bytes
/// only if it holds read authority on a live reference to the id, else `403`. The
/// gate is checked on the parsed id before the store is touched — a malformed id
/// never reaches it, and an id no path references is denied (fail-closed) rather
/// than `404`, so the response never distinguishes an unauthorized id from a
/// missing one.
async fn blob_fetch(
    State(state): State<Arc<BlobState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let identity = match authenticate(&state, &headers) {
        Ok(identity) => identity,
        Err(status) => return status.into_response(),
    };
    let Some(id) = blobs::parse_uuid(&id) else {
        return (StatusCode::BAD_REQUEST, "malformed blob id").into_response();
    };
    // A blob fetch is a bytes-out-of-band export — an auditable exfiltration
    // surface. Record a `Denied` export when access is refused, and a `Permitted`
    // one only once bytes actually leave the server (below), so the trail's
    // `Export` records mean an export happened — a 404 / read-error transfers
    // nothing and is not recorded as one.
    let audit_export = |decision: Decision| {
        if let Some(audit) = &state.audit {
            if let Err(err) = audit.record(
                identity.actor(),
                Action::Export,
                AuditResource::App(id.to_vec()),
                decision,
            ) {
                eprintln!("audit: failed to persist a blob-export event: {err}");
            }
        }
    };
    if !state.access.may_read_blob(&identity, &id).await {
        audit_export(Decision::Denied);
        return StatusCode::FORBIDDEN.into_response();
    }
    let fetched = {
        let store = state.store.lock().unwrap_or_else(|p| p.into_inner());
        store.get(&id)
    };
    match fetched {
        Ok(Some(bytes)) => {
            audit_export(Decision::Permitted);
            ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "blob store read failed").into_response(),
    }
}

// --- audit query surface --------------------------------------------------

/// The reserved app id the audit-query gate authorizes against: an operator reads
/// the trail only if the deployment grants [`Read`](Action::Read) on
/// `Resource::App("$audit")`. This reuses the admin-plane trust seams (verifier +
/// authorizer) so the audit trail is never exposed to an app client — only an
/// operator the deployment admits to this reserved resource. The `$` prefix is not a
/// legal app id, so it cannot collide with a real app.
pub const AUDIT_APP: &[u8] = b"$audit";

/// The audit-query plane's state: the trust seams every request clears and the
/// shared, append-only [`AuditLog`] a request reads (never writes).
struct AuditState {
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    log: Arc<AuditLog>,
}

/// The operator audit-query router: the single read-only `GET /audit` route,
/// filtered by actor / action / room / time-range. `log` is the shared append-only
/// trail; this surface only reads it. Exposed so it can be driven in-process by
/// tests without a socket.
pub fn audit_router(
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    log: Arc<AuditLog>,
) -> Router {
    let state = Arc::new(AuditState {
        verifier,
        authorizer,
        log,
    });
    Router::new()
        .route("/audit", get(audit_query))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .with_state(state)
}

/// Serve the operator audit-query plane on `listener` — a read-only control-plane
/// endpoint over the append-only audit trail, gated by the same verifier +
/// authorizer as the schema-registration admin plane.
pub async fn serve_audit(
    listener: TcpListener,
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    log: Arc<AuditLog>,
) -> std::io::Result<()> {
    axum::serve(listener, audit_router(verifier, authorizer, log)).await
}

/// The query-string filters of an audit query. Every field is optional; an empty
/// query returns every record (bounded by the log's size).
#[derive(Deserialize)]
struct AuditQueryParams {
    /// Only records whose actor equals this (matched as bytes).
    actor: Option<String>,
    /// Only records of this action keyword — `read`, `write`, `publish_awareness`,
    /// `register_schema`, `connect`, `export`, `version_read`.
    action: Option<String>,
    /// Only records naming this room (a room or a zone within it).
    room: Option<String>,
    /// Only records at or after this wall-clock millisecond (inclusive).
    since: Option<u64>,
    /// Only records strictly before this wall-clock millisecond (exclusive).
    until: Option<u64>,
}

/// One audit record rendered for the operator response.
#[derive(Serialize)]
struct AuditRecordJson {
    timestamp: u64,
    actor: String,
    action: &'static str,
    resource: AuditResourceJson,
    decision: &'static str,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AuditResourceJson {
    Room { room: String },
    App { app: String },
    Zone { room: String, zone: String },
}

/// The action keyword an [`Action`] renders as, and its inverse.
fn action_keyword(action: Action) -> &'static str {
    match action {
        Action::Read => "read",
        Action::Write => "write",
        Action::PublishAwareness => "publish_awareness",
        Action::RegisterSchema => "register_schema",
        Action::Connect => "connect",
        Action::Export => "export",
        Action::VersionRead => "version_read",
    }
}

fn action_from_keyword(keyword: &str) -> Option<Action> {
    Some(match keyword {
        "read" => Action::Read,
        "write" => Action::Write,
        "publish_awareness" => Action::PublishAwareness,
        "register_schema" => Action::RegisterSchema,
        "connect" => Action::Connect,
        "export" => Action::Export,
        "version_read" => Action::VersionRead,
        _ => return None,
    })
}

/// `GET /audit` — the read-only operator query over the append-only trail. The
/// caller must authenticate (`401` on a missing/invalid credential) and be
/// authorized [`Read`](Action::Read) on the reserved [`AUDIT_APP`] resource (`403`
/// otherwise), so the trail is never exposed to an app client. A malformed `action`
/// keyword is `400`; a latched audit-write failure is `500` (a dropped security
/// event must surface, never be hidden behind a clean read). The response is a JSON
/// array of the matching records, in time order.
async fn audit_query(
    State(state): State<Arc<AuditState>>,
    headers: HeaderMap,
    Query(params): Query<AuditQueryParams>,
) -> Response {
    let Some(credential) = headers.get("authorization").map(|v| v.as_bytes()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(identity) = state.verifier.verify(credential) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !state
        .authorizer
        .authorize(&identity, Action::Read, &Resource::App(AUDIT_APP))
    {
        return StatusCode::FORBIDDEN.into_response();
    }

    let action = match &params.action {
        Some(keyword) => match action_from_keyword(keyword) {
            Some(action) => Some(action),
            None => return (StatusCode::BAD_REQUEST, "unknown action keyword").into_response(),
        },
        None => None,
    };
    let query = AuditQuery {
        actor: params.actor.map(String::into_bytes),
        action,
        room: params.room.map(String::into_bytes),
        since: params.since,
        until: params.until,
    };

    // A latched append failure means a security event was dropped — surface it
    // rather than serving a clean-looking (but incomplete) read.
    if !state.log.healthy() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "audit log has a dropped record",
        )
            .into_response();
    }
    match state.log.query(&query) {
        Ok(records) => Json(records.iter().map(render_record).collect::<Vec<_>>()).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "audit log read failed").into_response(),
    }
}

fn render_record(record: &crate::audit::AuditRecord) -> AuditRecordJson {
    let render_bytes = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    let resource = match &record.resource {
        AuditResource::Room(room) => AuditResourceJson::Room {
            room: render_bytes(room),
        },
        AuditResource::App(app) => AuditResourceJson::App {
            app: render_bytes(app),
        },
        AuditResource::Zone { room, zone } => AuditResourceJson::Zone {
            room: render_bytes(room),
            zone: render_bytes(zone),
        },
    };
    AuditRecordJson {
        timestamp: record.timestamp,
        actor: render_bytes(&record.actor),
        action: action_keyword(record.action),
        resource,
        decision: match record.decision {
            Decision::Permitted => "permitted",
            Decision::Denied => "denied",
        },
    }
}
