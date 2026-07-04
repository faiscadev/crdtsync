//! The admin HTTP transport — the control-plane endpoint that turns a `POST` of
//! a schema into a registration, over a real socket.
//!
//! `dispatch` is exercised directly (route + method + version + status mapping)
//! against heads the request parser produced, and `serve_admin` is driven over a
//! live TCP connection to prove the read/decode/respond loop and that the
//! registry retains state across requests — an idempotent re-`POST` is `200`, a
//! changed body under a locked version is `409`.

use crdtsync_server::admin::dispatch;
use crdtsync_server::http::parse_head;
use crdtsync_server::{serve_admin, Action, Authorizer, Resource, SchemaRegistry, StaticTokens};

const APP: &[u8] = b"app-x";

fn verifier() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert(b"admin-cred".to_vec(), b"admin".to_vec());
    t.insert(b"user-cred".to_vec(), b"user".to_vec());
    t
}

fn only_admin_on_app_x() -> impl Authorizer {
    |actor: &[u8], action: Action, res: &Resource| {
        action == Action::RegisterSchema
            && actor == b"admin"
            && matches!(res, Resource::App(a) if *a == APP)
    }
}

/// Build a `Head` the way the transport does — by parsing bytes — then dispatch.
fn dispatch_raw(raw: &[u8], body: &[u8], registry: &mut SchemaRegistry) -> u16 {
    let head = parse_head(raw).unwrap().expect("complete head");
    dispatch(&head, body, &verifier(), &only_admin_on_app_x(), registry).status()
}

#[test]
fn a_well_formed_post_registers_and_returns_200() {
    let mut reg = SchemaRegistry::new();
    let raw = b"POST /apps/app-x/schemas/1 HTTP/1.1\r\nAuthorization: admin-cred\r\nContent-Length: 4\r\n\r\n";
    assert_eq!(dispatch_raw(raw, b"S1", &mut reg), 200);
    assert_eq!(reg.resolve(APP, 1), Some(&b"S1"[..]));
}

#[test]
fn a_missing_credential_is_401() {
    let mut reg = SchemaRegistry::new();
    let raw = b"POST /apps/app-x/schemas/1 HTTP/1.1\r\nContent-Length: 2\r\n\r\n";
    assert_eq!(dispatch_raw(raw, b"S1", &mut reg), 401);
}

#[test]
fn an_unpermitted_credential_is_403() {
    let mut reg = SchemaRegistry::new();
    let raw = b"POST /apps/app-x/schemas/1 HTTP/1.1\r\nAuthorization: user-cred\r\nContent-Length: 2\r\n\r\n";
    assert_eq!(dispatch_raw(raw, b"S1", &mut reg), 403);
}

#[test]
fn a_hash_lock_refusal_is_409() {
    let mut reg = SchemaRegistry::new();
    let v1 = b"POST /apps/app-x/schemas/1 HTTP/1.1\r\nAuthorization: admin-cred\r\nContent-Length: 2\r\n\r\n";
    assert_eq!(dispatch_raw(v1, b"S1", &mut reg), 200);
    // Same version, different body — locked-content change.
    assert_eq!(dispatch_raw(v1, b"S2", &mut reg), 409);
    // A gap is also 409.
    let v3 = b"POST /apps/app-x/schemas/3 HTTP/1.1\r\nAuthorization: admin-cred\r\nContent-Length: 2\r\n\r\n";
    assert_eq!(dispatch_raw(v3, b"S3", &mut reg), 409);
}

#[test]
fn a_non_post_method_is_405() {
    let mut reg = SchemaRegistry::new();
    let raw = b"GET /apps/app-x/schemas/1 HTTP/1.1\r\nAuthorization: admin-cred\r\n\r\n";
    assert_eq!(dispatch_raw(raw, b"", &mut reg), 405);
}

#[test]
fn an_unknown_route_is_404() {
    let mut reg = SchemaRegistry::new();
    for target in [
        "/",
        "/apps/app-x",
        "/apps/app-x/schemas",
        "/rooms/app-x/schemas/1",
    ] {
        let raw = format!("POST {target} HTTP/1.1\r\nAuthorization: admin-cred\r\n\r\n");
        assert_eq!(dispatch_raw(raw.as_bytes(), b"", &mut reg), 404, "{target}");
    }
}

#[test]
fn a_non_numeric_version_is_400() {
    let mut reg = SchemaRegistry::new();
    let raw = b"POST /apps/app-x/schemas/latest HTTP/1.1\r\nAuthorization: admin-cred\r\nContent-Length: 2\r\n\r\n";
    assert_eq!(dispatch_raw(raw, b"S1", &mut reg), 400);
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
    req.push_str(&format!("Content-Length: {}\r\n\r\n{}", body.len(), body));
    req.into_bytes()
}

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

    // A bad HTTP version — the parser rejects it, the transport answers 400.
    let bad = b"POST /apps/app-x/schemas/1 HTTP/9.9\r\nContent-Length: 0\r\n\r\n";
    assert_eq!(post(addr, bad).await, 400);
}
