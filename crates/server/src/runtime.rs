//! The WebSocket transport: the runnable server.
//!
//! [`serve`] accepts connections on a listener and drives each over the wire
//! protocol. A connection opens with the 8-byte header (magic + version) the
//! server negotiates, then exchanges framed messages.
//!
//! The [`Registry`] holds the CRDT replicas, which are single-threaded, so it
//! lives alone on a dedicated thread as an actor. Connection tasks — pure I/O,
//! and thus `Send` — reach it over channels: they forward decoded messages in
//! and receive outbound messages back through a per-connection channel. A
//! deliver's broadcast reaches the room's other connections because the actor
//! flushes every connection's outbox after each step. A connection whose
//! outbound queue overflows is too slow to keep up: it is dropped and its
//! socket closed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crdtsync_core::{
    decode_header, decode_message, encode_message, ClientId, Document, ErrorCode, Message,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, unbounded_channel, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::auth::{AllowAll, Verifier};
use crate::{negotiate, ConnId, Registry, RoomId, RoomLog, Store};

/// How many outbound messages may queue for one connection before it is judged
/// too slow and dropped — a bound on per-connection memory.
const OUTBOX_CAPACITY: usize = 1024;

/// How long teardown lets the writer flush queued messages (e.g. a refusal)
/// before forcing the socket closed — a peer that has stopped reading can wedge
/// the writer in `send`.
const WRITER_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Runtime policy: how the ephemeral-awareness sweep runs (how long a
/// disconnected client's presence lingers, and how often the sweep checks), and
/// whether a connection with no credential is admitted anonymously. The defaults
/// suit interactive use — a 5s grace absorbs brief reconnects, checked once a
/// second — and refuse anonymous connections.
#[derive(Clone, Copy)]
pub struct ServeConfig {
    pub grace: std::time::Duration,
    pub sweep_interval: std::time::Duration,
    /// Admit a credential-less connection by minting `actor = anon:<random>`,
    /// if the deployment permits it. Off by default.
    pub anonymous: bool,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            grace: std::time::Duration::from_secs(5),
            sweep_interval: std::time::Duration::from_secs(1),
            anonymous: false,
        }
    }
}

/// Mint an anonymous actor id, `anon:` followed by 128 random bits in hex, from
/// system entropy — kept at the transport layer, out of the pure-logic core.
fn anon_actor() -> Vec<u8> {
    use std::fmt::Write;
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("system entropy is available");
    let mut actor = String::from("anon:");
    for byte in bytes {
        let _ = write!(actor, "{byte:02x}");
    }
    actor.into_bytes()
}

/// A request to the registry actor from a connection task.
enum Cmd {
    /// Open a connection, registering its outbound sink and a one-shot the actor
    /// fires to close a dropped connection. Any credential presented at the
    /// upgrade travels with it; the actor verifies it and replies with the
    /// [`ConnOutcome`].
    Connect {
        writer: Sender<Message>,
        closer: oneshot::Sender<()>,
        credential: Option<Vec<u8>>,
        reply: oneshot::Sender<ConnOutcome>,
    },
    /// Route one inbound message, replying whether the connection stays open.
    Deliver {
        id: ConnId,
        msg: Message,
        reply: oneshot::Sender<bool>,
    },
    /// Close a connection.
    Disconnect { id: ConnId },
}

/// What the actor makes of a connect request after weighing any upgrade
/// credential against the verifier and the anonymous-mode policy.
enum ConnOutcome {
    /// The connection is open. `authok` carries the server-derived actor when
    /// the upgrade established one (fast path or anonymous), which the client is
    /// told before the message loop; `None` means the client must authenticate
    /// in band.
    Open { id: ConnId, authok: Option<Vec<u8>> },
    /// A credential was presented at the upgrade but the verifier refused it.
    Refused,
}

/// The actor's view of a live connection: where to send its outbound messages,
/// and how to tell it to close.
struct Peer {
    writer: Sender<Message>,
    closer: Option<oneshot::Sender<()>>,
}

/// Serve the wire protocol on `listener` until it errors, with room replicas
/// owned by `server`. A `store` makes the replicas durable: the hub replays it
/// on startup and every ingested op is appended before it fans out.
pub async fn serve(
    listener: TcpListener,
    server: ClientId,
    store: Option<Store>,
) -> std::io::Result<()> {
    serve_with(listener, server, store, ServeConfig::default()).await
}

/// Serve the wire protocol as [`serve`] does, with an explicit [`ServeConfig`]
/// instead of the defaults. Credentials are checked by the dev-mode
/// [`AllowAll`]; use [`serve_with_verifier`] to supply a real one.
pub async fn serve_with(
    listener: TcpListener,
    server: ClientId,
    store: Option<Store>,
    config: ServeConfig,
) -> std::io::Result<()> {
    serve_with_verifier(listener, server, store, config, Box::new(AllowAll)).await
}

/// Serve the wire protocol as [`serve_with`] does, authenticating credentials
/// with `verifier` — the deployment's identity seam (JWT, OIDC, API key). It
/// derives the actor for both the in-band Auth phase and the upgrade fast path.
pub async fn serve_with_verifier(
    listener: TcpListener,
    server: ClientId,
    store: Option<Store>,
    config: ServeConfig,
    verifier: Box<dyn Verifier + Send>,
) -> std::io::Result<()> {
    // Replay the persisted log here, before serving: a corrupt log fails
    // startup rather than panicking inside the detached actor thread and
    // leaving a live port with no registry behind it. The read is blocking, so
    // it runs on the blocking pool to keep the runtime free for other tasks.
    let (rooms, store) = match store {
        Some(store) => {
            let (result, store) = tokio::task::spawn_blocking(move || {
                let result = store.load().and_then(validated);
                (result, store)
            })
            .await
            .expect("replay task panicked");
            (result?, Some(store))
        }
        None => (Vec::new(), None),
    };
    let (cmds, cmd_rx) = unbounded_channel::<Cmd>();
    // The replicas are single-threaded; keep them on one dedicated thread.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build registry runtime");
        rt.block_on(registry_actor(
            server, rooms, store, config, verifier, cmd_rx,
        ));
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let cmds = cmds.clone();
        tokio::spawn(handle(stream, cmds));
    }
}

/// Surface a corrupt persisted snapshot as a startup error: every snapshot must
/// decode. The rooms pass through unchanged for the actor to rebuild, so this
/// runs on the blocking pool alongside the load, off the async runtime.
fn validated(rooms: Vec<(RoomId, RoomLog)>) -> std::io::Result<Vec<(RoomId, RoomLog)>> {
    for (_, log) in &rooms {
        if let Some(snapshot) = &log.snapshot {
            Document::decode_state(&snapshot.state).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:?}"))
            })?;
        }
    }
    Ok(rooms)
}

/// Own the registry and serve connection commands, flushing outboxes to each
/// connection's sink after every routed message.
async fn registry_actor(
    server: ClientId,
    rooms: Vec<(RoomId, RoomLog)>,
    store: Option<Store>,
    config: ServeConfig,
    verifier: Box<dyn Verifier + Send>,
    mut cmds: UnboundedReceiver<Cmd>,
) {
    // The rooms were validated during startup, so reconstruction can't fail.
    let mut hub = crate::Hub::from_rooms(server, rooms).expect("startup validated the store");
    if let Some(store) = store {
        hub.attach_store(store);
    }
    let mut reg = Registry::from_hub(hub);
    reg.set_verifier(verifier);
    reg.set_grace_millis(config.grace.as_millis() as u64);
    let mut peers: HashMap<ConnId, Peer> = HashMap::new();
    // The sweep expires the presence of clients past their grace deadline; its
    // first immediate tick is a harmless no-op with nothing yet stale.
    let mut sweep = tokio::time::interval(config.sweep_interval);
    loop {
        tokio::select! {
            cmd = cmds.recv() => {
                let Some(cmd) = cmd else { break };
                match cmd {
                    Cmd::Connect {
                        writer,
                        closer,
                        credential,
                        reply,
                    } => {
                        // A credential presented at the upgrade is verified now,
                        // so a good one skips the in-band Auth phase; a bad one is
                        // refused. With no credential the connection is anonymous
                        // if policy allows, else it must authenticate in band.
                        let outcome = match credential {
                            Some(cred) => match reg.verify_credential(&cred) {
                                Some(actor) => ConnOutcome::Open {
                                    id: reg.connect_authenticated(actor.clone()),
                                    authok: Some(actor),
                                },
                                None => ConnOutcome::Refused,
                            },
                            None if config.anonymous => {
                                let actor = anon_actor();
                                ConnOutcome::Open {
                                    id: reg.connect_authenticated(actor.clone()),
                                    authok: Some(actor),
                                }
                            }
                            None => ConnOutcome::Open {
                                id: reg.connect(),
                                authok: None,
                            },
                        };
                        if let ConnOutcome::Open { id, .. } = &outcome {
                            peers.insert(
                                *id,
                                Peer {
                                    writer,
                                    closer: Some(closer),
                                },
                            );
                        }
                        let _ = reply.send(outcome);
                    }
                    Cmd::Deliver { id, msg, reply } => {
                        let keep = reg.deliver(id, msg);
                        flush(&mut reg, &mut peers);
                        let _ = reply.send(keep);
                    }
                    Cmd::Disconnect { id } => {
                        reg.disconnect(id);
                        peers.remove(&id);
                    }
                }
            }
            _ = sweep.tick() => {
                reg.sweep();
                flush(&mut reg, &mut peers);
            }
        }
    }
}

/// Push every connection's queued outbox into its sink — how a deliver's
/// broadcast reaches the room's other connections. A connection whose sink is
/// full is too slow: it is dropped from the registry and signalled to close.
fn flush(reg: &mut Registry, peers: &mut HashMap<ConnId, Peer>) {
    let mut dropped = Vec::new();
    for (id, peer) in peers.iter() {
        for out in reg.take_outbox(*id) {
            if peer.writer.try_send(out).is_err() {
                dropped.push(*id);
                break;
            }
        }
    }
    for id in dropped {
        reg.disconnect(id);
        if let Some(mut peer) = peers.remove(&id) {
            if let Some(closer) = peer.closer.take() {
                let _ = closer.send(());
            }
        }
    }
}

/// Drive one connection: handshake, then the message loop, then teardown.
async fn handle(stream: TcpStream, cmds: UnboundedSender<Cmd>) {
    // Read any credential off the upgrade request — the fast-path carrier is the
    // `Authorization` header. The callback runs during the accept, so it stashes
    // the bytes for the connect that follows.
    let carried = Arc::new(Mutex::new(None));
    let sink = carried.clone();
    let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        if let Some(value) = req.headers().get(AUTHORIZATION) {
            *sink.lock().unwrap() = Some(value.as_bytes().to_vec());
        }
        Ok(resp)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let credential = carried.lock().unwrap().take();

    let (mut write, mut read) = ws.split();

    let (out, mut out_rx) = channel::<Message>(OUTBOX_CAPACITY);
    let (close_tx, mut close_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    if cmds
        .send(Cmd::Connect {
            writer: out.clone(),
            closer: close_tx,
            credential,
            reply: reply_tx,
        })
        .is_err()
    {
        return;
    }
    let Ok(outcome) = reply_rx.await else {
        return;
    };

    // The writer task owns the sink, draining queued messages until the last
    // sender is dropped at teardown.
    let mut writer = tokio::spawn(async move {
        while let Some(m) = out_rx.recv().await {
            if write
                .send(WsMessage::Binary(encode_message(&m).into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let id = match outcome {
        // A credential was presented at the upgrade and refused: report it and
        // close without ever entering the message loop.
        ConnOutcome::Refused => {
            let _ = out
                .send(Message::Error {
                    code: ErrorCode::AuthFailed,
                    message: "credential rejected".to_string(),
                })
                .await;
            None
        }
        ConnOutcome::Open { id, authok } => {
            // The first frame is the connection header: negotiate the version
            // before any message, queueing a refusal the client can read before
            // the close. Once negotiated, a fast-path or anonymous connection is
            // told its server-derived actor without having sent an Auth.
            match next_binary(&mut read).await {
                Some(bytes) => match decode_header(&bytes).map(negotiate) {
                    Ok(Ok(())) => {
                        if let Some(actor) = authok {
                            let _ = out.send(Message::AuthOk { actor }).await;
                        }
                        run_messages(id, &mut read, &cmds, &mut close_rx).await;
                    }
                    Ok(Err(refusal)) => {
                        let _ = out.send(refusal).await;
                    }
                    Err(_) => {}
                },
                None => {}
            }
            Some(id)
        }
    };

    if let Some(id) = id {
        let _ = cmds.send(Cmd::Disconnect { id });
    }
    drop(out);
    // Let the writer flush what's queued, but don't let a peer that stopped
    // reading wedge it in `send` and keep the socket half-open.
    if tokio::time::timeout(WRITER_GRACE, &mut writer)
        .await
        .is_err()
    {
        writer.abort();
        let _ = writer.await;
    }
}

/// Read and route messages until the peer closes, sends garbage, violates the
/// protocol, or the server drops the connection for falling behind.
async fn run_messages<R>(
    id: ConnId,
    read: &mut R,
    cmds: &UnboundedSender<Cmd>,
    close_rx: &mut oneshot::Receiver<()>,
) where
    R: StreamExt<Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let bytes = tokio::select! {
            biased;
            _ = &mut *close_rx => break,
            frame = next_binary(read) => match frame {
                Some(bytes) => bytes,
                None => break,
            },
        };
        let Ok(msg) = decode_message(&bytes) else {
            break;
        };
        let (reply, keep_rx) = oneshot::channel();
        if cmds.send(Cmd::Deliver { id, msg, reply }).is_err() {
            break;
        }
        match keep_rx.await {
            Ok(true) => continue,
            _ => break,
        }
    }
}

/// The next binary frame's bytes, or `None` once the stream ends. A text frame
/// is a protocol violation (the wire is binary) and ends the stream; control
/// frames are tolerated.
async fn next_binary<R>(read: &mut R) -> Option<Vec<u8>>
where
    R: StreamExt<Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(frame) = read.next().await {
        match frame {
            Ok(WsMessage::Binary(b)) => return Some(b.into()),
            Ok(WsMessage::Text(_)) | Ok(WsMessage::Close(_)) | Err(_) => return None,
            Ok(_) => continue,
        }
    }
    None
}
