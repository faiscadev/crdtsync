//! The blob HTTP transport — the out-of-band byte channel that turns a `POST` of
//! bytes into a handle and serves the bytes back on `GET`.
//!
//! The status matrix (round-trip, unknown / malformed id, auth, authorization,
//! body cap) is driven in-process through the axum router with `oneshot`, sharing
//! one store so an upload's handle is fetchable by a later request. One socket test
//! then proves `serve_blobs` wires the router to a real listener and that the store
//! persists across requests. Every test here touches the filesystem (the store
//! root) and/or binds a loopback socket, so each is skipped under Miri, which
//! cannot execute those syscalls.
//!
//! The reference-site authorization *policy* — which identities may fetch which
//! blob given the doc-ACL — is proved end-to-end over the registry in `blob_acl`;
//! here the `BlobAccess` gate is a stub, exercising only the route's wiring of it
//! (a deny becomes `403`, an allow reaches the store).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::Request;
use axum::Router;
use crdtsync_server::{
    blob_router, serve_blobs, BlobAccess, BlobStore, Identity, PermitAllBlobs, StaticTokens,
    MAX_BLOB_BODY,
};
use tower::ServiceExt;

static NONCE: AtomicU64 = AtomicU64::new(0);

/// A fresh, empty store rooted at a unique temp directory.
fn store() -> Arc<Mutex<BlobStore>> {
    let root = std::env::temp_dir().join(format!(
        "crdtsync-blob-http-{}-{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    Arc::new(Mutex::new(BlobStore::open(root).unwrap()))
}

/// The credential table: `user-cred` authenticates, anything else is refused.
fn verifier() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert(b"user-cred".to_vec(), b"user".to_vec());
    t
}

/// A `BlobAccess` stub that answers every fetch with a fixed verdict — the route
/// wiring under test, not the policy (that is `blob_acl`'s job).
struct FixedAccess(bool);

#[async_trait::async_trait]
impl BlobAccess for FixedAccess {
    async fn may_read_blob(&self, _identity: &Identity, _blob_id: &[u8; 16]) -> bool {
        self.0
    }
}

/// A router whose fetch gate authorizes every request — the default for the
/// non-authorization status-matrix tests.
fn router(store: Arc<Mutex<BlobStore>>) -> Router {
    blob_router(Box::new(verifier()), store, Arc::new(PermitAllBlobs), None)
}

/// A router whose fetch gate answers every fetch with `verdict`.
fn router_with_access(store: Arc<Mutex<BlobStore>>, verdict: bool) -> Router {
    blob_router(
        Box::new(verifier()),
        store,
        Arc::new(FixedAccess(verdict)),
        None,
    )
}

/// Drive one request through a clone of `router` (so the shared store persists
/// across calls), returning the status and the response body bytes.
async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    cred: Option<&str>,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> (u16, Vec<u8>) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(c) = cred {
        builder = builder.header("authorization", c);
    }
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    let request = builder.body(Body::from(body)).unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, bytes)
}

/// Upload `body` and return the parsed `{id, size, inline}` handle.
async fn upload(router: &Router, body: Vec<u8>) -> (u16, serde_json::Value) {
    let (status, resp) = send(
        router,
        "POST",
        "/blobs",
        Some("user-cred"),
        Some("application/octet-stream"),
        body,
    )
    .await;
    let json = if status == 200 {
        serde_json::from_slice(&resp).unwrap()
    } else {
        serde_json::Value::Null
    };
    (status, json)
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_stored_blob_round_trips_by_handle() {
    let r = router(store());
    let payload = vec![7u8; 10_000]; // past the inline threshold — a stored object.

    let (status, handle) = upload(&r, payload.clone()).await;
    assert_eq!(status, 200);
    assert_eq!(handle["size"], 10_000);
    assert_eq!(handle["inline"], false);
    let id = handle["id"].as_str().unwrap();

    let (status, bytes) = send(
        &r,
        "GET",
        &format!("/blobs/{id}"),
        Some("user-cred"),
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(bytes, payload);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_small_inline_blob_is_still_fetchable() {
    let r = router(store());
    let payload = b"hi".to_vec();

    let (status, handle) = upload(&r, payload.clone()).await;
    assert_eq!(status, 200);
    assert_eq!(handle["size"], 2);
    assert_eq!(handle["inline"], true);
    let id = handle["id"].as_str().unwrap();

    let (status, bytes) = send(
        &r,
        "GET",
        &format!("/blobs/{id}"),
        Some("user-cred"),
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(bytes, payload);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_unknown_id_is_404() {
    let r = router(store());
    let (status, _) = send(
        &r,
        "GET",
        "/blobs/00000000000000000000000000000000",
        Some("user-cred"),
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 404);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_malformed_id_is_400() {
    let r = router(store());
    for bad in ["not-hex", "zz", &"a".repeat(31), &"a".repeat(33)] {
        let (status, _) = send(
            &r,
            "GET",
            &format!("/blobs/{bad}"),
            Some("user-cred"),
            None,
            Vec::new(),
        )
        .await;
        assert_eq!(status, 400, "{bad}");
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_unauthenticated_request_is_401() {
    let r = router(store());
    // Missing credential on upload and fetch.
    let (status, _) = send(&r, "POST", "/blobs", None, None, b"x".to_vec()).await;
    assert_eq!(status, 401);
    let (status, _) = send(
        &r,
        "GET",
        "/blobs/00000000000000000000000000000000",
        None,
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 401);
    // A credential the verifier does not know is refused too.
    let (status, _) = send(&r, "POST", "/blobs", Some("nope"), None, b"x".to_vec()).await;
    assert_eq!(status, 401);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_oversized_body_is_413() {
    let r = router(store());
    let (status, _) = send(
        &r,
        "POST",
        "/blobs",
        Some("user-cred"),
        Some("application/octet-stream"),
        vec![0u8; MAX_BLOB_BODY + 1],
    )
    .await;
    assert_eq!(status, 413);
}

// --- fetch authorization wiring -------------------------------------------

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_authenticated_but_unauthorized_fetch_is_403() {
    // The store holds the bytes and the caller authenticates, yet the reference-site
    // gate denies: the fetch is 403, not 200 — the authenticated-but-not-authorized
    // gap closed. The response never reaches the store.
    let s = store();
    let r = router_with_access(s.clone(), true);
    let (status, handle) = upload(&r, vec![4u8; 9000]).await;
    assert_eq!(status, 200);
    let id = handle["id"].as_str().unwrap().to_string();

    // Same store, a denying gate.
    let denied = router_with_access(s, false);
    let (status, _) = send(
        &denied,
        "GET",
        &format!("/blobs/{id}"),
        Some("user-cred"),
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 403, "an authorized-denied fetch is forbidden");
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_authorized_fetch_reaches_the_bytes() {
    // The allow verdict lets the fetch through to the store — the gate is a gate,
    // not a wall.
    let r = router_with_access(store(), true);
    let payload = vec![5u8; 9000];
    let (status, handle) = upload(&r, payload.clone()).await;
    assert_eq!(status, 200);
    let id = handle["id"].as_str().unwrap();

    let (status, bytes) = send(
        &r,
        "GET",
        &format!("/blobs/{id}"),
        Some("user-cred"),
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(bytes, payload);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn authorization_is_checked_after_authentication() {
    // An unauthenticated fetch is 401 even when the gate would allow — authentication
    // is the outer gate; a missing credential never reaches the authorization check
    // (and there is no identity to check).
    let r = router_with_access(store(), true);
    let (status, _) = send(
        &r,
        "GET",
        "/blobs/00000000000000000000000000000000",
        None,
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 401);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_malformed_id_is_rejected_before_authorization() {
    // A malformed id is 400 even under a denying gate — parsing precedes the
    // reference-site check, so a bad id never becomes a lookup.
    let r = router_with_access(store(), false);
    let (status, _) = send(
        &r,
        "GET",
        "/blobs/not-a-valid-id",
        Some("user-cred"),
        None,
        Vec::new(),
    )
    .await;
    assert_eq!(status, 400);
}

// --- socket integration ---------------------------------------------------

use tokio::net::TcpListener;

// Binds a real loopback socket and touches the store on disk — skip under Miri.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn serves_upload_and_fetch_over_a_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_blobs(
        listener,
        Box::new(verifier()),
        store(),
        Arc::new(PermitAllBlobs),
        None,
    ));

    let client = reqwest::Client::new();
    let payload = vec![3u8; 9000];

    let resp = client
        .post(format!("http://{addr}/blobs"))
        .header("authorization", "user-cred")
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let handle: serde_json::Value = resp.json().await.unwrap();
    let id = handle["id"].as_str().unwrap().to_string();

    let got = client
        .get(format!("http://{addr}/blobs/{id}"))
        .header("authorization", "user-cred")
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), 200);
    assert_eq!(got.bytes().await.unwrap().to_vec(), payload);

    // An unknown handle is 404 over the socket.
    let missing = client
        .get(format!(
            "http://{addr}/blobs/ffffffffffffffffffffffffffffffff"
        ))
        .header("authorization", "user-cred")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}
