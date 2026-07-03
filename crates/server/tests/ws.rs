//! WebSocket transport — the runnable server end to end.
//!
//! A connection opens with an 8-byte header (magic + version) the server
//! negotiates, then exchanges framed messages: Hello, Subscribe (drawing a
//! catch-up batch), then Ops the server ingests and broadcasts to the room's
//! other connections. These tests drive a real server over a loopback socket.
//!
//! Excluded under Miri, which cannot run tokio's real I/O.
#![cfg(not(miri))]

use crdtsync_core::protocol::{Channel, PROTOCOL_VERSION};
use crdtsync_core::{
    decode_message, encode_header, encode_message, ClientId, Document, ErrorCode, Message, Op,
    Scalar,
};
use crdtsync_server::acl::Acl;
use crdtsync_server::runtime::{
    serve, serve_with, serve_with_authorizer, serve_with_verifier, ServeConfig,
};
use crdtsync_server::{AllowAll, Authorizer, Verifier};
use std::time::Duration;

const CH: Channel = Channel(0);

fn ops_msg(ops: Vec<Op>) -> Message {
    Message::Ops { channel: CH, ops }
}

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-1";

/// A running test server: its ws:// URL and the accept-loop task. The handle is
/// retained so the task isn't dropped, and aborted when the test ends. The
/// server loop runs until the listener errors, so it is never awaited.
struct Server {
    url: String,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start a server on an ephemeral loopback port.
async fn start_server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve(listener, cid(0xFF), None));
    Server {
        url: format!("ws://{addr}"),
        task,
    }
}

/// Start a server with an explicit awareness grace + sweep cadence, so a test
/// can drive presence expiry without waiting the multi-second default.
async fn start_server_with(config: ServeConfig) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with(listener, cid(0xFF), None, config));
    Server {
        url: format!("ws://{addr}"),
        task,
    }
}

async fn open(url: &str) -> Ws {
    let (ws, _) = connect_async(url).await.unwrap();
    ws
}

/// Start a server whose credentials are checked by `verifier`.
async fn start_server_with_verifier(verifier: Box<dyn Verifier + Send>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with_verifier(
        listener,
        cid(0xFF),
        None,
        ServeConfig::default(),
        verifier,
    ));
    Server {
        url: format!("ws://{addr}"),
        task,
    }
}

/// Open a connection presenting `credential` in the `Authorization` header — the
/// upgrade fast path the server verifies during accept.
async fn open_with_auth(url: &str, credential: &[u8]) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{header::AUTHORIZATION, HeaderValue};
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert(AUTHORIZATION, HeaderValue::from_bytes(credential).unwrap());
    let (ws, _) = connect_async(request).await.unwrap();
    ws
}

/// Open a connection setting the upgrade request header `name` to `value` — used
/// to present a credential over the cookie and subprotocol carriers.
async fn open_with_header(url: &str, name: &'static str, value: &str) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        HeaderName::from_static(name),
        HeaderValue::from_str(value).unwrap(),
    );
    let (ws, _) = connect_async(request).await.unwrap();
    ws
}

async fn send_bytes(ws: &mut Ws, bytes: Vec<u8>) {
    ws.send(WsMessage::Binary(bytes.into())).await.unwrap();
}

async fn send(ws: &mut Ws, msg: &Message) {
    send_bytes(ws, encode_message(msg)).await;
}

/// Read the next binary frame and decode it as a protocol message.
async fn recv(ws: &mut Ws) -> Message {
    loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Binary(b) => return decode_message(&b).unwrap(),
            WsMessage::Close(_) => panic!("connection closed before a message"),
            _ => continue,
        }
    }
}

/// A handshaked, subscribed connection with its catch-up drained.
async fn join(url: &str, client: u8) -> Ws {
    let mut ws = open(url).await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION).to_vec()).await;
    send(
        &mut ws,
        &Message::Hello {
            client: cid(client),
        },
    )
    .await;
    // The dev-mode verifier accepts any credential and echoes it as the actor.
    send(
        &mut ws,
        &Message::Auth {
            credential: b"cred".to_vec(),
        },
    )
    .await;
    assert_eq!(
        recv(&mut ws).await,
        Message::AuthOk {
            actor: b"cred".to_vec(),
        }
    );
    send(
        &mut ws,
        &Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    ws
}

/// Start a server that enforces `authorizer` at every read/write/awareness point.
/// The dev-mode verifier still echoes the presented credential as the actor, so a
/// test picks the actor a policy sees by choosing the credential it authenticates
/// with.
async fn start_server_with_authorizer(authorizer: Box<dyn Authorizer + Send>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with_authorizer(
        listener,
        cid(0xFF),
        None,
        ServeConfig::default(),
        Box::new(AllowAll),
        authorizer,
    ));
    Server {
        url: format!("ws://{addr}"),
        task,
    }
}

/// Handshake and authenticate as the actor named by `credential`, returning the
/// connection without subscribing so a test can drive the subscribe itself.
async fn auth_as(url: &str, client: u8, credential: &[u8]) -> Ws {
    let mut ws = open(url).await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION).to_vec()).await;
    send(
        &mut ws,
        &Message::Hello {
            client: cid(client),
        },
    )
    .await;
    send(
        &mut ws,
        &Message::Auth {
            credential: credential.to_vec(),
        },
    )
    .await;
    assert_eq!(
        recv(&mut ws).await,
        Message::AuthOk {
            actor: credential.to_vec(),
        }
    );
    ws
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// A policy loaded from a declarative file is enforced by the running server: the
/// granted actor subscribes to the permitted room, and everyone else is refused
/// over the real transport — the deploy-seam half of declarative enforcement.
#[tokio::test]
async fn a_declared_policy_gates_subscribe_over_the_transport() {
    // Only "reader" may read "room-1"; every other actor is default-denied.
    let policy = format!("allow actor:{} read room:room-1", hex(b"reader"));
    let acl = Acl::from_policy(&policy).unwrap();
    let server = start_server_with_authorizer(Box::new(acl)).await;
    let url = &server.url;

    let mut granted = auth_as(url, 1, b"reader").await;
    send(
        &mut granted,
        &Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    assert_eq!(
        recv(&mut granted).await,
        ops_msg(Vec::new()),
        "the permitted actor subscribes"
    );

    let mut denied = auth_as(url, 2, b"intruder").await;
    send(
        &mut denied,
        &Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    assert!(
        matches!(
            recv(&mut denied).await,
            Message::Error {
                code: ErrorCode::Forbidden,
                ..
            }
        ),
        "an actor outside the policy is forbidden"
    );
}

fn sample_ops() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

#[tokio::test]
async fn subscribe_returns_a_catch_up_batch() {
    let server = start_server().await;
    let url = &server.url;
    let mut a = join(url, 1).await;
    // A fresh room's catch-up is empty.
    assert_eq!(recv(&mut a).await, ops_msg(Vec::new()));
}

#[tokio::test]
async fn an_op_broadcasts_to_another_subscriber() {
    let server = start_server().await;
    let url = &server.url;
    let mut a = join(url, 1).await;
    let mut b = join(url, 2).await;
    assert_eq!(recv(&mut a).await, ops_msg(Vec::new()));
    assert_eq!(recv(&mut b).await, ops_msg(Vec::new()));

    let ops = sample_ops();
    send(&mut a, &ops_msg(ops.clone())).await;

    assert_eq!(recv(&mut b).await, ops_msg(ops));
}

#[tokio::test]
async fn a_late_joiner_catches_up() {
    let server = start_server().await;
    let url = &server.url;
    let mut a = join(url, 1).await;
    assert_eq!(recv(&mut a).await, ops_msg(Vec::new()));

    let ops = sample_ops();
    let through = ops.iter().map(|o| o.id.seq).max().unwrap();
    send(&mut a, &ops_msg(ops.clone())).await;
    // The server acknowledges the author's batch before anything else.
    assert_eq!(
        recv(&mut a).await,
        Message::Accepted {
            channel: Channel(0),
            through
        }
    );

    // Barrier: a subscribes the room again on a second channel and reads its own
    // op back, proving the server ingested it before the late joiner subscribes.
    send(
        &mut a,
        &Message::Subscribe {
            channel: Channel(1),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    assert_eq!(
        recv(&mut a).await,
        Message::Ops {
            channel: Channel(1),
            ops: ops.clone(),
        }
    );

    // A connection that subscribes afterward draws the room's history.
    let mut b = join(url, 2).await;
    assert_eq!(recv(&mut b).await, ops_msg(ops));
}

#[tokio::test]
async fn a_foreign_version_is_refused() {
    let server = start_server().await;
    let url = &server.url;
    let mut ws = open(url).await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION + 1).to_vec()).await;
    assert!(matches!(
        recv(&mut ws).await,
        Message::Error {
            code: ErrorCode::UnsupportedVersion,
            ..
        }
    ));
}

#[tokio::test]
async fn a_departed_clients_presence_clears_after_the_grace_window() {
    // A short grace and fast sweep so the expiry fires within the test rather
    // than after the multi-second production default.
    let server = start_server_with(ServeConfig {
        grace: Duration::from_millis(150),
        sweep_interval: Duration::from_millis(20),
        ..ServeConfig::default()
    })
    .await;
    let url = &server.url;

    let mut a = join(url, 1).await;
    assert_eq!(recv(&mut a).await, ops_msg(Vec::new()));
    send(
        &mut a,
        &Message::AwarenessSet {
            channel: CH,
            key: b"cursor".to_vec(),
            value: vec![1],
        },
    )
    .await;

    // B joins and is replayed A's presence.
    let mut b = join(url, 2).await;
    assert_eq!(recv(&mut b).await, ops_msg(Vec::new()));
    assert_eq!(
        recv(&mut b).await,
        Message::AwarenessUpdate {
            channel: CH,
            actor: b"cred".to_vec(),
            key: b"cursor".to_vec(),
            value: vec![1],
        }
    );

    // A drops; past the grace window the periodic sweep clears its presence and
    // tells B on B's own channel.
    a.close(None).await.unwrap();
    drop(a);
    assert_eq!(
        recv(&mut b).await,
        Message::AwarenessClear {
            channel: CH,
            actor: b"cred".to_vec(),
        }
    );
}

#[tokio::test]
async fn a_credential_at_the_upgrade_skips_the_auth_phase() {
    // The dev verifier accepts any credential and echoes it as the actor.
    let server = start_server().await;
    let url = &server.url;
    let mut ws = open_with_auth(url, b"cred").await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION).to_vec()).await;

    // The server establishes the actor at the upgrade and tells us, no Auth sent.
    assert_eq!(
        recv(&mut ws).await,
        Message::AuthOk {
            actor: b"cred".to_vec(),
        }
    );

    // Straight from Hello to Subscribe.
    send(&mut ws, &Message::Hello { client: cid(1) }).await;
    send(
        &mut ws,
        &Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    assert_eq!(recv(&mut ws).await, ops_msg(Vec::new()));
}

#[tokio::test]
async fn anonymous_mode_mints_an_actor_without_a_credential() {
    let server = start_server_with(ServeConfig {
        anonymous: true,
        ..ServeConfig::default()
    })
    .await;
    let url = &server.url;
    // No Authorization header, but anonymous mode is on.
    let mut ws = open(url).await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION).to_vec()).await;

    match recv(&mut ws).await {
        Message::AuthOk { actor } => {
            assert!(
                actor.starts_with(b"anon:"),
                "expected anon actor, got {actor:?}"
            );
        }
        other => panic!("expected an AuthOk, got {other:?}"),
    }
}

#[tokio::test]
async fn an_injected_verifier_maps_a_good_upgrade_credential_to_its_actor() {
    let verifier: Box<dyn Verifier + Send> =
        Box::new(|cred: &[u8]| (cred == b"good").then(|| b"alice".to_vec()));
    let server = start_server_with_verifier(verifier).await;
    let url = &server.url;

    let mut ws = open_with_auth(url, b"good").await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION).to_vec()).await;
    // The actor is what the verifier derived, not the raw credential.
    assert_eq!(
        recv(&mut ws).await,
        Message::AuthOk {
            actor: b"alice".to_vec(),
        }
    );
}

#[tokio::test]
async fn an_injected_verifier_refuses_a_bad_upgrade_credential() {
    let verifier: Box<dyn Verifier + Send> =
        Box::new(|cred: &[u8]| (cred == b"good").then(|| b"alice".to_vec()));
    let server = start_server_with_verifier(verifier).await;
    let url = &server.url;

    // A refused credential closes the connection with AuthFailed before the loop.
    let mut ws = open_with_auth(url, b"nope").await;
    assert!(matches!(
        recv(&mut ws).await,
        Message::Error {
            code: ErrorCode::AuthFailed,
            ..
        }
    ));
}

// --- additional fast-path credential carriers ---

/// Handshake a fast-path connection and assert the server established `actor`
/// without an in-band Auth exchange.
async fn assert_fast_path_actor(ws: &mut Ws, actor: &[u8]) {
    send_bytes(ws, encode_header(PROTOCOL_VERSION).to_vec()).await;
    assert_eq!(
        recv(ws).await,
        Message::AuthOk {
            actor: actor.to_vec(),
        }
    );
}

#[tokio::test]
async fn a_credential_in_a_cookie_skips_the_auth_phase() {
    let server = start_server().await;
    let mut ws = open_with_header(&server.url, "cookie", "crdtsync_credential=cred").await;
    assert_fast_path_actor(&mut ws, b"cred").await;
}

#[tokio::test]
async fn a_credential_in_the_subprotocol_skips_the_auth_phase() {
    let server = start_server().await;
    // The client offers the app protocol plus the auth-carrying one.
    let mut ws = open_with_header(
        &server.url,
        "sec-websocket-protocol",
        "crdtsync, crdtsync.auth.cred",
    )
    .await;
    assert_fast_path_actor(&mut ws, b"cred").await;
}

#[tokio::test]
async fn a_credential_in_the_query_string_skips_the_auth_phase() {
    let server = start_server().await;
    let mut ws = open(&format!("{}/?credential=cred", server.url)).await;
    assert_fast_path_actor(&mut ws, b"cred").await;
}

#[tokio::test]
async fn the_authorization_header_wins_over_other_carriers() {
    let server = start_server().await;
    // Both a header and a cookie are present; the header takes precedence.
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{
        header::{AUTHORIZATION, COOKIE},
        HeaderValue,
    };
    let mut request = server.url.as_str().into_client_request().unwrap();
    request
        .headers_mut()
        .insert(AUTHORIZATION, HeaderValue::from_static("from-header"));
    request.headers_mut().insert(
        COOKIE,
        HeaderValue::from_static("crdtsync_credential=from-cookie"),
    );
    let (mut ws, _) = connect_async(request).await.unwrap();
    assert_fast_path_actor(&mut ws, b"from-header").await;
}
