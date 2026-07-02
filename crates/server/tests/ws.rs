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
use crdtsync_server::runtime::serve;

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
            channel: CH,
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
    send(&mut a, &ops_msg(ops.clone())).await;

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
