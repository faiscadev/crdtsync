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
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;

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
) -> Router {
    let state = Arc::new(BlobState {
        verifier,
        store,
        access,
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
) -> std::io::Result<()> {
    axum::serve(listener, blob_router(verifier, store, access)).await
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
    if !state.access.may_read_blob(&identity, &id).await {
        return StatusCode::FORBIDDEN.into_response();
    }
    let fetched = {
        let store = state.store.lock().unwrap_or_else(|p| p.into_inner());
        store.get(&id)
    };
    match fetched {
        Ok(Some(bytes)) => {
            ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "blob store read failed").into_response(),
    }
}
