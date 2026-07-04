//! The admin HTTP transport — the control-plane endpoint that turns a `POST` of
//! a schema into a registration.
//!
//! The status matrix (route + method + version + auth + registry outcome) is
//! driven in-process through the axum router with `oneshot`, no socket. One
//! socket test then proves `serve_admin` wires the router to a real listener and
//! that the registry retains state across requests — an idempotent re-`POST` is
//! `200`, a changed body under a locked version is `409`.

use axum::body::Body;
use axum::http::Request;
use crdtsync_server::{
    admin_router, serve_admin, Action, Authorizer, Resource, SchemaRegistry, StaticTokens,
};
use tower::ServiceExt;

const APP: &[u8] = b"app-x";

fn verifier() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert(b"admin-cred".to_vec(), b"admin".to_vec());
    t.insert(b"user-cred".to_vec(), b"user".to_vec());
    t
}

fn only_admin_on_app_x() -> impl Authorizer + Clone {
    |actor: &[u8], action: Action, res: &Resource| {
        action == Action::RegisterSchema
            && actor == b"admin"
            && matches!(res, Resource::App(a) if *a == APP)
    }
}

/// Drive one request through a fresh router over `registry`, returning the
/// status. The router owns the registry, so state that must persist across
/// requests (idempotency, hash-lock) is exercised by the socket test below,
/// where one server handles the whole sequence.
async fn send(
    registry: SchemaRegistry,
    method: &str,
    target: &str,
    cred: Option<&str>,
    body: &str,
) -> u16 {
    let router = admin_router(
        Box::new(verifier()),
        Box::new(only_admin_on_app_x()),
        registry,
    );
    let mut builder = Request::builder().method(method).uri(target);
    if let Some(c) = cred {
        builder = builder.header("authorization", c);
    }
    let request = builder.body(Body::from(body.to_owned())).unwrap();
    router.oneshot(request).await.unwrap().status().as_u16()
}

#[tokio::test]
async fn a_well_formed_post_registers_and_returns_200() {
    assert_eq!(
        send(
            SchemaRegistry::new(),
            "POST",
            "/apps/app-x/schemas/1",
            Some("admin-cred"),
            "S1"
        )
        .await,
        200
    );
}

#[tokio::test]
async fn a_missing_credential_is_401() {
    assert_eq!(
        send(
            SchemaRegistry::new(),
            "POST",
            "/apps/app-x/schemas/1",
            None,
            "S1"
        )
        .await,
        401
    );
}

#[tokio::test]
async fn an_unpermitted_credential_is_403() {
    assert_eq!(
        send(
            SchemaRegistry::new(),
            "POST",
            "/apps/app-x/schemas/1",
            Some("user-cred"),
            "S1"
        )
        .await,
        403
    );
}

#[tokio::test]
async fn a_hash_lock_gap_is_409() {
    // A gap (version 3 while head is 0) is refused by the registry.
    assert_eq!(
        send(
            SchemaRegistry::new(),
            "POST",
            "/apps/app-x/schemas/3",
            Some("admin-cred"),
            "S3"
        )
        .await,
        409
    );
}

#[tokio::test]
async fn a_non_post_method_is_405() {
    assert_eq!(
        send(
            SchemaRegistry::new(),
            "GET",
            "/apps/app-x/schemas/1",
            Some("admin-cred"),
            ""
        )
        .await,
        405
    );
}

#[tokio::test]
async fn an_unknown_route_is_404() {
    for target in [
        "/",
        "/apps/app-x",
        "/apps/app-x/schemas",
        "/rooms/app-x/schemas/1",
    ] {
        assert_eq!(
            send(
                SchemaRegistry::new(),
                "POST",
                target,
                Some("admin-cred"),
                ""
            )
            .await,
            404,
            "{target}"
        );
    }
}

#[tokio::test]
async fn a_non_numeric_version_is_400() {
    assert_eq!(
        send(
            SchemaRegistry::new(),
            "POST",
            "/apps/app-x/schemas/latest",
            Some("admin-cred"),
            "S1"
        )
        .await,
        400
    );
}

// --- socket integration ---------------------------------------------------

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Send one raw HTTP request to `addr` and return the response status code.
async fn post(addr: std::net::SocketAddr, request: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request).await.unwrap();
    stream.flush().await.unwrap();
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    // Parse "HTTP/1.1 <code> <reason>".
    let line = std::str::from_utf8(&resp).unwrap().lines().next().unwrap();
    line.split(' ').nth(1).unwrap().parse().unwrap()
}

fn register(app_id: &str, version: u32, credential: Option<&str>, body: &str) -> Vec<u8> {
    let mut req = format!("POST /apps/{app_id}/schemas/{version} HTTP/1.1\r\nHost: admin\r\n");
    if let Some(c) = credential {
        req.push_str(&format!("Authorization: {c}\r\n"));
    }
    req.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    ));
    req.into_bytes()
}

// Socket tests do real network syscalls Miri cannot execute; skip them there.
// The `oneshot` router tests above still run under Miri.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_admin_plane_serves_registration_over_a_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_admin(
        listener,
        Box::new(verifier()),
        Box::new(only_admin_on_app_x()),
        SchemaRegistry::new(),
    ));

    // A permitted admin registers version 1.
    assert_eq!(
        post(addr, &register("app-x", 1, Some("admin-cred"), "SCHEMA-1")).await,
        200
    );
    // The same registration again is an idempotent 200 — the registry kept state.
    assert_eq!(
        post(addr, &register("app-x", 1, Some("admin-cred"), "SCHEMA-1")).await,
        200
    );
    // A changed body under the locked version is a 409 — proving v1 was retained.
    assert_eq!(
        post(addr, &register("app-x", 1, Some("admin-cred"), "OTHER")).await,
        409
    );
    // A gap (version 3 while head is 1) is a 409.
    assert_eq!(
        post(addr, &register("app-x", 3, Some("admin-cred"), "SCHEMA-3")).await,
        409
    );
    // The next contiguous version registers.
    assert_eq!(
        post(addr, &register("app-x", 2, Some("admin-cred"), "SCHEMA-2")).await,
        200
    );

    // No credential is refused; an unpermitted one is forbidden.
    assert_eq!(
        post(addr, &register("app-x", 3, None, "SCHEMA-3")).await,
        401
    );
    assert_eq!(
        post(addr, &register("app-x", 3, Some("user-cred"), "SCHEMA-3")).await,
        403
    );
}

// A request whose framing hyper cannot parse is answered 400 over the socket,
// not left hanging — the transport swap must keep rejecting malformed HTTP.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_malformed_request_over_the_socket_is_400() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_admin(
        listener,
        Box::new(verifier()),
        Box::new(only_admin_on_app_x()),
        SchemaRegistry::new(),
    ));

    let bad =
        b"POST /apps/app-x/schemas/1 HTTP/9.9\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    assert_eq!(post(addr, bad).await, 400);
}
