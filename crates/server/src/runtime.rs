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

use cookie::Cookie;
use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{
    decode_header, decode_message, encode_header, encode_message, ClientId, Document, ErrorCode,
    Message,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::sync::oneshot;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, COOKIE, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::auth::{AllowAll, Identity, Verifier};
use crate::authz::{Authorizer, PermitAll};
use crate::membership::Membership;
use crate::placement::NodeId;
use crate::webhook::{WebhookConfig, WebhookSink};
use crate::{negotiate, ConnId, Registry, RoomId, RoomLog, Store};

/// How many outbound messages may queue for one connection before it is judged
/// too slow and dropped — a bound on per-connection memory.
const OUTBOX_CAPACITY: usize = 1024;

/// How long teardown lets the writer flush queued messages (e.g. a refusal)
/// before forcing the socket closed — a peer that has stopped reading can wedge
/// the writer in `send`.
const WRITER_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// How many replication frames may queue for one follower before the leader
/// drops further ones — a bound on per-follower memory when a peer falls behind.
/// A dropped frame is not fatal: the follower catches up on the next commit, and
/// majority-ack durability (a later unit) gates a client on a follower actually
/// holding the write.
const PEER_FRAME_CAPACITY: usize = 1024;

/// How long a peer connection waits before redialing a follower that is
/// unreachable or has dropped — long enough not to spin on a down peer, short
/// enough to reconverge promptly once it returns.
const PEER_REDIAL_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

/// Runtime policy: how the ephemeral-awareness sweep runs (how long a
/// disconnected client's presence lingers, and how often the sweep checks), and
/// whether a connection with no credential is admitted anonymously. The defaults
/// suit interactive use — a 5s grace absorbs brief reconnects, checked once a
/// second — and refuse anonymous connections.
#[derive(Clone)]
pub struct ServeConfig {
    pub grace: std::time::Duration,
    pub sweep_interval: std::time::Duration,
    /// Admit a credential-less connection by minting `actor = anon:<random>`,
    /// if the deployment permits it. Off by default.
    pub anonymous: bool,
    /// The schema registry the handshake resolves each client's `{app_id,
    /// version}` against. Share the same handle with the registration admin plane
    /// so a registration becomes visible to the data plane; the default is an
    /// empty registry, so every connection resolves to a relay.
    pub schema: Arc<Mutex<crate::SchemaRegistry>>,
    /// An outbound webhook endpoint that receives every room-bearing lifecycle
    /// event as a POST. `None` registers no sink, so a deployment that wants no
    /// webhooks pays nothing per event.
    pub webhook: Option<WebhookConfig>,
    /// The node's static cluster membership + placement. `None` is single-node
    /// mode — every room is served locally, the current behavior. When set, the
    /// node holds its member view; routing on it is Unit 3, so a set membership
    /// changes nothing here yet.
    pub membership: Option<Membership>,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            grace: std::time::Duration::from_secs(5),
            sweep_interval: std::time::Duration::from_secs(1),
            anonymous: false,
            schema: Arc::default(),
            webhook: None,
            membership: None,
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
    /// A follower's replication acknowledgement, read off its peer connection and
    /// carrying the follower's node id so the leader records the watermark for the
    /// right `(room, follower)`.
    PeerAck {
        follower: NodeId,
        room: RoomId,
        through_seq: u64,
    },
    /// A peer's reachability changed — its relay link connected (`live`) or
    /// dropped/failed to dial — the failover liveness signal (Unit 6a). The
    /// registry updates its membership view so a down member is skipped when
    /// electing a room's effective leader.
    PeerLive { node: NodeId, live: bool },
    /// The gossip task asks for this node's current known members, so it can pick
    /// a peer to gossip to and advertise the up-to-date set.
    GossipSnapshot {
        reply: oneshot::Sender<Vec<(NodeId, Vec<u8>)>>,
    },
    /// The gossip task delivers the members a peer advertised back — the registry
    /// unions them into its membership, growing the set toward convergence.
    GossipLearned { members: Vec<(Vec<u8>, Vec<u8>)> },
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
    verifier: Box<dyn Verifier + Send + Sync>,
) -> std::io::Result<()> {
    serve_with_authorizer(
        listener,
        server,
        store,
        config,
        verifier,
        Box::new(PermitAll),
    )
    .await
}

/// Serve the wire protocol as [`serve_with_verifier`] does, additionally gating
/// what each authenticated actor may do through `authorizer` — the deployment's
/// policy seam. A deployment loads a declared policy (e.g. via
/// [`Acl::from_policy`](crate::acl::Acl::from_policy)) and hands it here so the
/// running server enforces it at every read/write/awareness point; the default
/// on the other entry points is the permissive dev-mode [`PermitAll`].
pub async fn serve_with_authorizer(
    listener: TcpListener,
    server: ClientId,
    store: Option<Store>,
    config: ServeConfig,
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
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
    // Start the webhook delivery worker here, on this I/O-enabled runtime — the
    // registry's own single-threaded runtime carries only the time driver. The
    // sink it hands back feeds that worker over a channel, so the registry
    // thread emits without ever touching the network.
    let webhook = config.webhook.clone().map(WebhookSink::spawn);
    // Dial an outbound peer connection to every other cluster member here, on this
    // I/O-enabled runtime — the registry thread has no network. Each holds a frame
    // channel the registry actor routes replication to, and reads the follower's
    // acks back into the actor as `Cmd::PeerAck`. Single-node (no membership) opens
    // no peer connections, so a plain deployment is unchanged.
    let peer_conns = spawn_peers(server, config.membership.as_ref(), &cmds);
    // Run the anti-entropy gossip loop here, on this I/O-enabled runtime, behind the
    // same cluster gate as replication: it periodically gossips this node's member
    // set with a random peer and unions back what the peer knows, so a node that
    // booted knowing only a seed converges on the full cluster. Single-node (no
    // membership) spawns no gossip task.
    spawn_gossip(server, config.membership.as_ref(), &cmds);
    // The replicas are single-threaded; keep them on one dedicated thread.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build registry runtime");
        rt.block_on(registry_actor(
            server, rooms, store, config, verifier, authorizer, webhook, peer_conns, cmd_rx,
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
    verifier: Box<dyn Verifier + Send + Sync>,
    authorizer: Box<dyn Authorizer + Send + Sync>,
    webhook: Option<WebhookSink>,
    peer_conns: HashMap<NodeId, Sender<Message>>,
    mut cmds: UnboundedReceiver<Cmd>,
) {
    // The rooms were validated during startup, so reconstruction can't fail.
    let mut hub = crate::Hub::from_rooms(server, rooms).expect("startup validated the store");
    if let Some(store) = store {
        hub.attach_store(store);
    }
    let mut reg = Registry::from_hub(hub);
    reg.set_verifier(verifier);
    reg.set_authorizer(authorizer);
    reg.set_schema_registry(config.schema.clone());
    reg.set_grace_millis(config.grace.as_millis() as u64);
    if let Some(membership) = config.membership.clone() {
        reg.set_membership(membership);
    }
    if let Some(webhook) = webhook {
        reg.add_event_sink(Box::new(webhook));
    }
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
                                Some(identity) => {
                                    let actor = identity.actor().to_vec();
                                    ConnOutcome::Open {
                                        id: reg.connect_authenticated(identity),
                                        authok: Some(actor),
                                    }
                                }
                                None => ConnOutcome::Refused,
                            },
                            None if config.anonymous => {
                                let actor = anon_actor();
                                ConnOutcome::Open {
                                    id: reg.connect_authenticated(Identity::new(actor.clone())),
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
                        // Route any commit the leader just made to its followers.
                        dispatch_replication(&mut reg, &peer_conns);
                        let _ = reply.send(keep);
                    }
                    Cmd::Disconnect { id } => {
                        reg.disconnect(id);
                        peers.remove(&id);
                    }
                    Cmd::PeerAck {
                        follower,
                        room,
                        through_seq,
                    } => {
                        // The ack may carry a withheld client write to a majority;
                        // recording it releases the owed `Accepted` into the
                        // author's outbox, so flush to deliver it.
                        reg.record_replica_ack(follower, &room, through_seq);
                        flush(&mut reg, &mut peers);
                    }
                    Cmd::PeerLive { node, live } => {
                        // The next client delivery recomputes leadership off the
                        // updated view, so there is nothing to flush here.
                        reg.set_peer_liveness(node, live);
                    }
                    Cmd::GossipSnapshot { reply } => {
                        let _ = reply.send(reg.known_members());
                    }
                    Cmd::GossipLearned { members } => {
                        // The next client delivery recomputes placement off the
                        // grown member set, so there is nothing to flush here.
                        reg.merge_gossip(members);
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

/// Route every replication frame the leader queued during a delivery to its
/// follower's peer connection. Best-effort: a frame for an unknown or backed-up
/// follower is dropped, not blocked on — the follower reconverges on the next
/// commit, and majority-ack durability (a later unit) is what actually gates a
/// client on a follower holding the write.
fn dispatch_replication(reg: &mut Registry, peer_conns: &HashMap<NodeId, Sender<Message>>) {
    for (follower, frame) in reg.take_replication() {
        if let Some(tx) = peer_conns.get(&follower) {
            let _ = tx.try_send(frame);
        }
    }
}

/// Open an outbound peer connection to every cluster member other than self,
/// returning the frame channel for each. Each spawned task owns the socket to one
/// follower: it dials, redials on drop, sends the replication frames the registry
/// routes to it, and reads that follower's acks back into the registry. With no
/// membership the map is empty — a single-node deployment dials no peers.
fn spawn_peers(
    server: ClientId,
    membership: Option<&Membership>,
    cmds: &UnboundedSender<Cmd>,
) -> HashMap<NodeId, Sender<Message>> {
    let mut peer_conns = HashMap::new();
    let Some(membership) = membership else {
        return peer_conns;
    };
    for member in membership.members() {
        if membership.is_self(member) {
            continue;
        }
        let addr = String::from_utf8_lossy(member.as_bytes()).into_owned();
        let (tx, rx) = channel::<Message>(PEER_FRAME_CAPACITY);
        tokio::spawn(peer_connection(
            server,
            member.clone(),
            addr,
            rx,
            cmds.clone(),
        ));
        peer_conns.insert(member.clone(), tx);
    }
    peer_conns
}

/// Spawn the periodic gossip loop, gated on cluster membership: with no
/// membership (single-node) nothing is spawned, so a plain deployment runs no
/// gossip. The loop drives anti-entropy against the registry over the command
/// channel — reading the current member set out and unioning learned members
/// back in.
fn spawn_gossip(server: ClientId, membership: Option<&Membership>, cmds: &UnboundedSender<Cmd>) {
    let Some(membership) = membership else {
        return;
    };
    let self_id = membership.self_id().clone();
    tokio::spawn(gossip_loop(server, self_id, cmds.clone()));
}

/// The anti-entropy gossip round loop: each tick, snapshot this node's known
/// members from the registry, pick a random peer, exchange member sets with it,
/// and feed what the peer advertised back into the registry. A dead or slow peer
/// is abandoned for the round and retried next tick. The loop ends when the
/// command channel closes (the registry shut down).
async fn gossip_loop(server: ClientId, self_id: NodeId, cmds: UnboundedSender<Cmd>) {
    let mut ticker = tokio::time::interval(crate::gossip::GOSSIP_INTERVAL);
    // A round can block on a slow peer up to the gossip timeout; delay the next
    // tick past that rather than firing a catch-up burst of rounds (the default
    // Burst behavior) once the peer clears.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately; skip it so a just-booted node settles
    // before its first round rather than gossiping an empty seed set.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let (reply, snapshot) = oneshot::channel();
        if cmds.send(Cmd::GossipSnapshot { reply }).is_err() {
            return;
        }
        let Ok(members) = snapshot.await else {
            return;
        };
        let Some(peer_addr) = crate::gossip::choose_peer(&members, &self_id) else {
            continue;
        };
        let addr = String::from_utf8_lossy(&peer_addr).into_owned();
        let frame = crate::gossip::gossip_frame(&members);
        if let Some(learned) = crate::gossip::gossip_exchange(&addr, server, frame).await {
            if cmds.send(Cmd::GossipLearned { members: learned }).is_err() {
                return;
            }
        }
    }
}

/// Own the socket to one follower: dial it, relay the replication frames that
/// arrive on `frames`, and forward the follower's acks back to the registry as
/// [`Cmd::PeerAck`]. A dial failure or a dropped socket redials after a short
/// delay, so a follower that starts late or restarts reconverges. The task ends
/// only when the frame channel closes (the registry shut down).
async fn peer_connection(
    server: ClientId,
    follower: NodeId,
    addr: String,
    mut frames: Receiver<Message>,
    cmds: UnboundedSender<Cmd>,
) {
    let url = format!("ws://{addr}/");
    // Report the follower's reachability to the registry — the failover liveness
    // signal (Unit 6a). A down follower is skipped when electing a room's effective
    // leader, so a dead primary's rooms promote to the next live replica.
    let mark = |live: bool| {
        let _ = cmds.send(Cmd::PeerLive {
            node: follower.clone(),
            live,
        });
    };
    loop {
        match connect_peer(&url, server).await {
            Some((write, read)) => {
                mark(true);
                // Pump until the socket or the frame channel closes, then redial.
                if pump_peer(write, read, &follower, &mut frames, &cmds).await {
                    // The frame channel closed — the registry is gone; stop.
                    return;
                }
                // The link dropped: the follower is unreachable until it redials.
                mark(false);
            }
            None => {
                mark(false);
                // The frame channel closed while unreachable — nothing more to do.
                if frames.is_closed() {
                    return;
                }
            }
        }
        tokio::time::sleep(PEER_REDIAL_DELAY).await;
    }
}

type PeerWrite = futures_util::stream::SplitSink<WsStream, WsMessage>;
type PeerRead = futures_util::stream::SplitStream<WsStream>;
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Dial the follower and open the relay peer connection: the 8-byte header, then
/// an empty-`app_id` `Hello` that resolves to a relay so the follower accepts the
/// peer. `None` if the dial or either opening frame fails.
async fn connect_peer(url: &str, server: ClientId) -> Option<(PeerWrite, PeerRead)> {
    let (ws, _) = connect_async(url).await.ok()?;
    let (mut write, read) = ws.split();
    write
        .send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .ok()?;
    let hello = Message::Hello {
        client: server,
        app_id: Vec::new(),
        schema_version: 0,
    };
    write
        .send(WsMessage::Binary(encode_message(&hello)))
        .await
        .ok()?;
    Some((write, read))
}

/// Relay frames to the follower and its acks back, until the socket errors or the
/// frame channel closes. Returns whether the frame channel closed (the registry
/// shut down), so the caller stops rather than redials.
async fn pump_peer(
    mut write: PeerWrite,
    mut read: PeerRead,
    follower: &NodeId,
    frames: &mut Receiver<Message>,
    cmds: &UnboundedSender<Cmd>,
) -> bool {
    loop {
        tokio::select! {
            frame = frames.recv() => match frame {
                Some(frame) => {
                    if write
                        .send(WsMessage::Binary(encode_message(&frame)))
                        .await
                        .is_err()
                    {
                        return false;
                    }
                }
                None => return true,
            },
            inbound = read.next() => match inbound {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    if let Ok(Message::ReplicaAck { room, through_seq }) = decode_message(&bytes) {
                        let _ = cmds.send(Cmd::PeerAck {
                            follower: follower.clone(),
                            room,
                            through_seq,
                        });
                    }
                }
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => return false,
            },
        }
    }
}

/// The cookie holding a credential, when the carrier is a cookie.
const AUTH_COOKIE: &str = "crdtsync_credential";
/// The WebSocket subprotocol prefix carrying a credential — the value follows.
const AUTH_SUBPROTOCOL_PREFIX: &str = "crdtsync.auth.";
/// The plain application subprotocol a client offers alongside the auth one; the
/// server echoes it so the client's subprotocol negotiation succeeds.
const APP_SUBPROTOCOL: &str = "crdtsync";
/// The query-string key holding a credential, when the carrier is the URL.
const AUTH_QUERY_KEY: &str = "credential";

/// Pull a credential off the upgrade request, trying carriers in precedence
/// order: `Authorization` header, WebSocket subprotocol, cookie, then query
/// param. A browser cannot set the `Authorization` header on a WebSocket, so the
/// subprotocol and query carriers are the ones a browser client can reach; the
/// query carrier is convenient but logs-leak-prone (URLs land in access logs).
fn extract_credential(req: &Request) -> Option<Vec<u8>> {
    if let Some(value) = req.headers().get(AUTHORIZATION) {
        return Some(value.as_bytes().to_vec());
    }
    if let Some(cred) = subprotocol_credential(req) {
        return Some(cred);
    }
    if let Some(cred) = cookie_credential(req) {
        return Some(cred);
    }
    query_credential(req)
}

/// Each comma-separated subprotocol the client offered.
fn offered_subprotocols(req: &Request) -> impl Iterator<Item = &str> {
    req.headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|list| list.split(','))
        .map(str::trim)
}

/// The credential carried in a `crdtsync.auth.<value>` subprotocol offer.
fn subprotocol_credential(req: &Request) -> Option<Vec<u8>> {
    offered_subprotocols(req)
        .find_map(|p| p.strip_prefix(AUTH_SUBPROTOCOL_PREFIX))
        .map(|cred| cred.as_bytes().to_vec())
}

/// The credential in the `crdtsync_credential=<value>` cookie.
fn cookie_credential(req: &Request) -> Option<Vec<u8>> {
    req.headers()
        .get_all(COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(Cookie::split_parse)
        .filter_map(Result::ok)
        .find(|cookie| cookie.name() == AUTH_COOKIE)
        .map(|cookie| cookie.value().as_bytes().to_vec())
}

/// The credential in the `?credential=<value>` query param.
fn query_credential(req: &Request) -> Option<Vec<u8>> {
    form_urlencoded::parse(req.uri().query()?.as_bytes())
        .find(|(key, _)| key == AUTH_QUERY_KEY)
        .map(|(_, value)| value.into_owned().into_bytes())
}

/// Drive one connection: handshake, then the message loop, then teardown.
async fn handle(stream: TcpStream, cmds: UnboundedSender<Cmd>) {
    // Read any credential off the upgrade request across the supported carriers.
    // The callback runs during the accept, so it stashes the bytes for the
    // connect that follows, and echoes the app subprotocol when the client
    // offered it so its subprotocol negotiation succeeds.
    let carried = Arc::new(Mutex::new(None));
    let sink = carried.clone();
    let callback = move |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
        *sink.lock().unwrap() = extract_credential(req);
        if offered_subprotocols(req).any(|p| p == APP_SUBPROTOCOL) {
            resp.headers_mut().insert(
                SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static(APP_SUBPROTOCOL),
            );
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
                    details: Vec::new(),
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
