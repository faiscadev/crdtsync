//! The schema-registration admin surface — the control-plane operation an app
//! owner's CI performs to register a schema version.
//!
//! Registration is separate from the data-plane sync connection but reuses its
//! trust seams: the request's credential authenticates to an [`Identity`] via the
//! [`Verifier`], and the [`Authorizer`] decides the `RegisterSchema` action on
//! the request's [`Resource::App`] — the app-admin meta-auth, distinct from any
//! room action. Only a permitted identity reaches the [`SchemaRegistry`]. This
//! handler is pure over an already-decoded request; the HTTP transport that
//! decodes one and maps the outcome to a status is a separate layer.
//!
//! [`Identity`]: crate::auth::Identity

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::auth::Verifier;
use crate::authz::{Action, Authorizer, Resource};
use crate::http::parse_head;
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
    registry: &mut SchemaRegistry,
) -> RegisterOutcome {
    let Some(credential) = req.credential else {
        return RegisterOutcome::Unauthenticated;
    };
    let Some(identity) = verifier.verify(credential) else {
        return RegisterOutcome::Unauthenticated;
    };
    let resource = Resource::App(req.app_id);
    if !authorizer.authorize(identity.actor(), Action::RegisterSchema, &resource) {
        return RegisterOutcome::Forbidden;
    }
    match registry.register(req.app_id, req.version, req.schema, req.migration) {
        Ok(registered) => RegisterOutcome::Accepted(registered),
        Err(error) => RegisterOutcome::Rejected(error),
    }
}

/// The largest schema body accepted, guarding the admin plane against an
/// oversized (or falsely-declared) `Content-Length`.
const MAX_BODY: usize = 1 << 20;

/// How long one admin request may take to arrive in full before its connection
/// is dropped — a slow or stalled client cannot wedge the sequential loop.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// An HTTP response the admin transport writes back: a status, its reason
/// phrase, and a short plain-text body.
pub struct AdminResponse {
    status: u16,
    reason: &'static str,
    body: String,
}

impl AdminResponse {
    fn text(status: u16, reason: &'static str, body: &str) -> Self {
        Self {
            status,
            reason,
            body: body.to_string(),
        }
    }

    /// The HTTP status code — the observable result of a request.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// The response serialized as an HTTP/1.1 message. `Connection: close`, since
    /// the admin plane serves one request per connection.
    fn to_http(&self) -> Vec<u8> {
        format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n{}",
            self.status,
            self.reason,
            self.body.len(),
            self.body,
        )
        .into_bytes()
    }
}

/// Route a parsed admin request to the registration handler and map its outcome
/// to an HTTP response. The only route is `POST /apps/{app_id}/schemas/{version}`;
/// the credential is the `Authorization` header verbatim, matching the data
/// plane's header carrier, and the body is the schema bytes.
pub fn dispatch(
    head: &crate::http::Head,
    body: &[u8],
    verifier: &dyn Verifier,
    authorizer: &dyn Authorizer,
    registry: &mut SchemaRegistry,
) -> AdminResponse {
    if head.method() != "POST" {
        return AdminResponse::text(405, "Method Not Allowed", "only POST is supported");
    }
    let Some((app_id, version)) = parse_register_target(head.target()) else {
        return AdminResponse::text(404, "Not Found", "unknown route");
    };
    let Ok(version) = version.parse::<u32>() else {
        return AdminResponse::text(400, "Bad Request", "version must be a positive integer");
    };
    let req = RegisterRequest {
        app_id: app_id.as_bytes(),
        version,
        schema: body,
        migration: b"",
        credential: head.header("authorization").map(str::as_bytes),
    };
    match register_schema(&req, verifier, authorizer, registry) {
        RegisterOutcome::Accepted(_) => AdminResponse::text(200, "OK", "registered"),
        RegisterOutcome::Unauthenticated => {
            AdminResponse::text(401, "Unauthorized", "a valid credential is required")
        }
        RegisterOutcome::Forbidden => {
            AdminResponse::text(403, "Forbidden", "not permitted to register this app")
        }
        RegisterOutcome::Rejected(_) => AdminResponse::text(
            409,
            "Conflict",
            "registration rejected: not the next contiguous version, or a locked version changed",
        ),
    }
}

/// Match `/apps/{app_id}/schemas/{version}`, returning the raw `app_id` and
/// version segments; `None` for any other shape. The `app_id` segment is taken
/// as literal bytes, so an app id must be a URL-path-safe token.
fn parse_register_target(target: &str) -> Option<(&str, &str)> {
    let path = target.split('?').next().unwrap_or(target);
    let mut segments = path.strip_prefix('/')?.split('/');
    let apps = segments.next()?;
    let app_id = segments.next()?;
    let schemas = segments.next()?;
    let version = segments.next()?;
    if segments.next().is_some()
        || apps != "apps"
        || schemas != "schemas"
        || app_id.is_empty()
        || version.is_empty()
    {
        return None;
    }
    Some((app_id, version))
}

/// Serve the schema-registration admin plane on `listener`: a dedicated,
/// control-plane HTTP endpoint separate from the data-plane sync socket. Requests
/// are served one at a time — registration is rare, so the registry needs no
/// lock — each bounded by [`REQUEST_TIMEOUT`] so a stalled client cannot wedge
/// the loop. A per-connection I/O error or timeout drops that connection; the
/// plane keeps serving.
pub async fn serve_admin(
    listener: TcpListener,
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    mut registry: SchemaRegistry,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let _ = tokio::time::timeout(
            REQUEST_TIMEOUT,
            handle_admin(
                &mut stream,
                verifier.as_ref(),
                authorizer.as_ref(),
                &mut registry,
            ),
        )
        .await;
    }
}

/// Read one HTTP request off `stream`, dispatch it, and write the response.
/// Reads until the head is complete (the parser caps its size), rejects an
/// over-large body before reading it, then reads exactly the declared body.
async fn handle_admin(
    stream: &mut TcpStream,
    verifier: &(dyn Verifier + Sync),
    authorizer: &(dyn Authorizer + Sync),
    registry: &mut SchemaRegistry,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head = loop {
        match parse_head(&buf) {
            Ok(Some(head)) => break head,
            Ok(None) => {}
            Err(_) => {
                return respond(
                    stream,
                    &AdminResponse::text(400, "Bad Request", "malformed request"),
                )
                .await;
            }
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(()); // closed before a complete head
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    if head.content_length() > MAX_BODY {
        return respond(
            stream,
            &AdminResponse::text(413, "Payload Too Large", "schema body too large"),
        )
        .await;
    }
    let total = head.head_len() + head.content_length();
    while buf.len() < total {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(()); // closed before the whole body arrived
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    let body = &buf[head.head_len()..total];
    let response = dispatch(&head, body, verifier, authorizer, registry);
    respond(stream, &response).await
}

/// Write a response and flush it.
async fn respond(stream: &mut TcpStream, response: &AdminResponse) -> std::io::Result<()> {
    stream.write_all(&response.to_http()).await?;
    stream.flush().await
}
