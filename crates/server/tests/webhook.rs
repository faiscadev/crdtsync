//! The outbound webhook sink: a committed lifecycle event is POSTed to the
//! configured endpoint with the expected JSON shape and shared-secret header,
//! no sink registered delivers nothing, and a failing endpoint never blocks or
//! panics the hub. Hermetic — the receiver is an in-process capture server bound
//! to a loopback ephemeral port, never real outbound network.

use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use crdtsync_core::{ClientId, Document, Scalar};
use crdtsync_server::webhook::SECRET_HEADER;
use crdtsync_server::{Hub, WebhookConfig, WebhookSink};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";

/// Create `ROOM` with a single register op, so a version capture and a
/// compaction have state to act on.
fn populate(h: &mut Hub) {
    let mut a = Document::new(cid(1));
    h.ingest(
        ROOM,
        a.transact(|tx| tx.register(b"a", Scalar::Int(1))),
        None,
    )
    .unwrap();
}

/// One received POST: its headers and decoded JSON body.
struct Received {
    headers: HeaderMap,
    body: serde_json::Value,
}

async fn capture(
    State(tx): State<UnboundedSender<Received>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    let _ = tx.send(Received { headers, body });
    StatusCode::OK
}

/// Bind a capture server on a loopback ephemeral port; return its endpoint URL
/// and a receiver of every POST it gets.
async fn capture_server() -> (String, UnboundedReceiver<Received>) {
    let (tx, rx) = unbounded_channel();
    let app = Router::new().route("/hook", post(capture)).with_state(tx);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, rx)
}

/// The next POST the capture server received, failing if none arrives promptly.
async fn recv(rx: &mut UnboundedReceiver<Received>) -> Received {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a webhook POST arrives within the timeout")
        .expect("the capture channel stays open")
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback capture server over a real socket
async fn a_version_create_is_posted_with_the_secret_header() {
    let (url, mut rx) = capture_server().await;
    let sink = WebhookSink::spawn(WebhookConfig {
        url,
        secret: Some("s3cret".to_string()),
    });
    let mut h = Hub::new(cid(0xFF));
    h.add_event_sink(Box::new(sink));
    populate(&mut h);
    assert!(h.create_version(ROOM, b"v1").unwrap());

    let got = recv(&mut rx).await;
    assert_eq!(got.body["type"], "version-created");
    assert_eq!(got.body["room"], "room-1");
    assert_eq!(got.body["name"], "v1");
    assert_eq!(got.headers.get(SECRET_HEADER).unwrap(), "s3cret");
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback capture server over a real socket
async fn a_compaction_is_posted_with_its_floor_and_no_secret_header() {
    let (url, mut rx) = capture_server().await;
    let sink = WebhookSink::spawn(WebhookConfig { url, secret: None });
    let mut h = Hub::new(cid(0xFF));
    h.add_event_sink(Box::new(sink));
    populate(&mut h);
    h.compact(ROOM).unwrap();

    let got = recv(&mut rx).await;
    assert_eq!(got.body["type"], "compacted");
    assert_eq!(got.body["room"], "room-1");
    // The one ingested op folds into the snapshot: the floor advances to 1.
    assert_eq!(got.body["floor"], 1);
    // No secret configured → no secret header attached.
    assert!(got.headers.get(SECRET_HEADER).is_none());
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback capture server over a real socket
async fn no_sink_registered_delivers_nothing() {
    let (_url, mut rx) = capture_server().await;
    let mut h = Hub::new(cid(0xFF));
    // No webhook sink registered — the hub emits, but nothing is POSTed.
    populate(&mut h);
    h.create_version(ROOM, b"v1").unwrap();
    h.compact(ROOM).unwrap();
    let idle = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
    assert!(idle.is_err(), "no POST arrives when no sink is registered");
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // connects out over a real socket
async fn a_failing_endpoint_never_blocks_or_panics_the_hub() {
    // Port 1 has no listener: every POST fails fast at connect. The sink must
    // still never block or panic the commit path, even past the queue capacity.
    let sink = WebhookSink::spawn(WebhookConfig {
        url: "http://127.0.0.1:1/hook".to_string(),
        secret: None,
    });
    let mut h = Hub::new(cid(0xFF));
    h.add_event_sink(Box::new(sink));
    populate(&mut h);
    for i in 0..3000u32 {
        let name = format!("v{i}");
        assert!(h.create_version(ROOM, name.as_bytes()).unwrap());
    }
    // The hub is fully live afterward — emission never wedged on the dead sink.
    assert!(h.create_version(ROOM, b"final").unwrap());
}
