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
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;

use crate::auth::Verifier;
use crate::authz::{Action, Authorizer, Resource};
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
