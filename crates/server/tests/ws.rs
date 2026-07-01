//! WebSocket transport — the runnable server end to end.
//!
//! A connection opens with an 8-byte header (magic + version) the server
//! negotiates, then exchanges framed messages: Hello, Subscribe (drawing a
//! catch-up batch), then Ops the server ingests and broadcasts to the room's
//! other connections. These tests drive a real server over a loopback socket.
//!
//! Excluded under Miri, which cannot run tokio's real I/O.
#![cfg(not(miri))]

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{
    decode_message, encode_header, encode_message, ClientId, Document, ErrorCode, Message, Op,
    Scalar,
};
use crdtsync_server::runtime::serve;

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

/// Start a server on an ephemeral loopback port; return its ws:// URL.
async fn start_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, cid(0xFF)));
    format!("ws://{addr}")
}

async fn open(url: &str) -> Ws {
    let (ws, _) = connect_async(url).await.unwrap();
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
    send(
        &mut ws,
        &Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    ws
}

fn sample_ops() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

#[tokio::test]
async fn subscribe_returns_a_catch_up_batch() {
    let url = start_server().await;
    let mut a = join(&url, 1).await;
    // A fresh room's catch-up is empty.
    assert_eq!(recv(&mut a).await, Message::Ops(Vec::new()));
}

#[tokio::test]
async fn an_op_broadcasts_to_another_subscriber() {
    let url = start_server().await;
    let mut a = join(&url, 1).await;
    let mut b = join(&url, 2).await;
    assert_eq!(recv(&mut a).await, Message::Ops(Vec::new()));
    assert_eq!(recv(&mut b).await, Message::Ops(Vec::new()));

    let ops = sample_ops();
    send(&mut a, &Message::Ops(ops.clone())).await;

    assert_eq!(recv(&mut b).await, Message::Ops(ops));
}

#[tokio::test]
async fn a_late_joiner_catches_up() {
    let url = start_server().await;
    let mut a = join(&url, 1).await;
    assert_eq!(recv(&mut a).await, Message::Ops(Vec::new()));

    let ops = sample_ops();
    send(&mut a, &Message::Ops(ops.clone())).await;

    // Barrier: a re-subscribes and reads its own op back, proving the server
    // ingested it before the late joiner subscribes.
    send(
        &mut a,
        &Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    )
    .await;
    assert_eq!(recv(&mut a).await, Message::Ops(ops.clone()));

    // A connection that subscribes afterward draws the room's history.
    let mut b = join(&url, 2).await;
    assert_eq!(recv(&mut b).await, Message::Ops(ops));
}

#[tokio::test]
async fn a_foreign_version_is_refused() {
    let url = start_server().await;
    let mut ws = open(&url).await;
    send_bytes(&mut ws, encode_header(PROTOCOL_VERSION + 1).to_vec()).await;
    assert!(matches!(
        recv(&mut ws).await,
        Message::Error {
            code: ErrorCode::UnsupportedVersion,
            ..
        }
    ));
}
