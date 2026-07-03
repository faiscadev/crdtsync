//! The many-connection fan-out over one hub.
//!
//! A [`Registry`] holds every live connection, each with its own session and
//! an outbox of messages awaiting send. [`Registry::deliver`] drives one
//! connection's session, queues its replies, and fans a broadcast out to the
//! room's other connections. Pure, synchronous routing; the async transport
//! pumps bytes through it.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use crdtsync_core::{ClientId, Message};

use crate::auth::{AllowAll, Verifier};
use crate::authz::{Action, Authorizer, PermitAll, Resource};
use crate::clock::{Clock, SystemClock};
use crate::{step, Hub, Session, Store};

/// How long a departed client's presence is retained before a sweep clears it,
/// so a brief reconnect keeps its awareness alive across the gap.
const DEFAULT_GRACE_MILLIS: u64 = 5000;

/// A live connection's handle, minted by [`Registry::connect`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ConnId(u64);

/// One connection: its protocol session and the messages queued to send it.
struct Conn {
    session: Session,
    outbox: Vec<Message>,
}

/// The set of live connections sharing one hub.
pub struct Registry {
    hub: Hub,
    conns: HashMap<ConnId, Conn>,
    next: u64,
    verifier: Box<dyn Verifier>,
    authorizer: Box<dyn Authorizer>,
    clock: Arc<dyn Clock>,
    grace_millis: u64,
    /// Departed clients whose presence is retained until the wall-clock deadline,
    /// keyed by client. A reconnect cancels the entry; a [`sweep`](Registry::sweep)
    /// past the deadline clears the presence and tells the room.
    stale: HashMap<ClientId, u64>,
}

impl Registry {
    /// An in-memory registry whose hub's replicas are owned by `server`.
    pub fn new(server: ClientId) -> Self {
        Self::from_hub(Hub::new(server))
    }

    /// A registry over an existing hub — durable or not. Defaults to the
    /// dev-mode [`AllowAll`] verifier; set one with [`Registry::set_verifier`].
    pub(crate) fn from_hub(hub: Hub) -> Self {
        Self {
            hub,
            conns: HashMap::new(),
            next: 0,
            verifier: Box::new(AllowAll),
            authorizer: Box::new(PermitAll),
            clock: Arc::new(SystemClock),
            grace_millis: DEFAULT_GRACE_MILLIS,
            stale: HashMap::new(),
        }
    }

    /// Use `verifier` to authenticate connections' credentials.
    pub fn set_verifier(&mut self, verifier: Box<dyn Verifier>) {
        self.verifier = verifier;
    }

    /// Use `authorizer` to decide what each authenticated actor may do.
    pub fn set_authorizer(&mut self, authorizer: Box<dyn Authorizer>) {
        self.authorizer = authorizer;
    }

    /// Verify a credential presented at the transport upgrade, returning the
    /// server-derived actor, or `None` if refused. The fast path uses this to
    /// establish auth during accept, so the connection skips the in-band Auth.
    pub fn verify_credential(&self, credential: &[u8]) -> Option<Vec<u8>> {
        self.verifier.verify(credential)
    }

    /// Read wall time from `clock` for the reconnect-grace window — a shared
    /// [`ManualClock`](crate::clock::ManualClock) drives it deterministically in
    /// tests.
    pub fn set_clock(&mut self, clock: Arc<dyn Clock>) {
        self.clock = clock;
    }

    /// How long a departed client's presence lingers before a sweep may clear it.
    pub fn set_grace_millis(&mut self, millis: u64) {
        self.grace_millis = millis;
    }

    /// A registry backed by `store`: its hub replays the persisted log, and
    /// every op the hub ingests is appended before it fans out to peers.
    pub fn with_store(server: ClientId, store: Store) -> io::Result<Self> {
        let mut hub = Hub::from_rooms(server, store.load()?)?;
        hub.attach_store(store);
        Ok(Self::from_hub(hub))
    }

    /// Open a connection whose client authenticates in band, returning its
    /// handle.
    pub fn connect(&mut self) -> ConnId {
        self.insert_conn(Session::new())
    }

    /// Open a connection already authenticated as `actor` — the upgrade fast path
    /// (credential verified at accept) or anonymous mode. Its client skips the
    /// in-band Auth phase.
    pub fn connect_authenticated(&mut self, actor: Vec<u8>) -> ConnId {
        self.insert_conn(Session::authenticated(actor))
    }

    fn insert_conn(&mut self, session: Session) -> ConnId {
        let id = ConnId(self.next);
        self.next += 1;
        self.conns.insert(
            id,
            Conn {
                session,
                outbox: Vec::new(),
            },
        );
        id
    }

    /// Close a connection, dropping its session and any queued messages. Its
    /// ephemeral awareness is not cleared at once: the client is marked stale
    /// with a grace deadline, so a reconnect within the window keeps its presence
    /// and only a later [`sweep`](Registry::sweep) past the deadline drops it.
    pub fn disconnect(&mut self, id: ConnId) {
        if let Some(conn) = self.conns.remove(&id) {
            if let Some(client) = conn.session.client() {
                // Another live connection under the same client still owns that
                // presence, so a sweep must not clear it — this covers a
                // reconnect race (the new connection registered before the old
                // one's close) and a second connection asserting the same id.
                let still_held = self
                    .conns
                    .values()
                    .any(|c| c.session.client() == Some(client));
                // Only a client with live presence and no other live connection
                // needs a grace timer; otherwise there is nothing a sweep should
                // clear.
                if !still_held && self.hub.has_client_awareness(client) {
                    let deadline = self.clock.now_millis().saturating_add(self.grace_millis);
                    self.stale.insert(client, deadline);
                }
            }
        }
    }

    /// Clear the presence of every client whose grace deadline has passed,
    /// telling each affected room's remaining subscribers with an AwarenessClear
    /// on their own channel. Idempotent; a reconnected client is no longer stale
    /// and is left untouched.
    pub fn sweep(&mut self) {
        let now = self.clock.now_millis();
        let due: Vec<ClientId> = self
            .stale
            .iter()
            .filter(|(_, &deadline)| deadline <= now)
            .map(|(client, _)| *client)
            .collect();
        for client in due {
            self.stale.remove(&client);
            for (room, actor) in self.hub.clear_client_awareness(client) {
                for conn in self.conns.values_mut() {
                    for channel in conn.session.channels_for_room(&room) {
                        conn.outbox.push(Message::AwarenessClear {
                            channel,
                            actor: actor.clone(),
                        });
                    }
                }
            }
        }
    }

    /// Drive one inbound message through the connection's session, queueing its
    /// replies and fanning any broadcast out to the room's other connections.
    /// Returns whether the connection should stay open.
    pub fn deliver(&mut self, id: ConnId, msg: Message) -> bool {
        // A client reappearing within its grace window cancels the pending clear,
        // so its presence survives the reconnect gap.
        if let Message::Hello { client } = &msg {
            self.stale.remove(client);
        }
        let (broadcast, close, room, awareness) = {
            let Some(conn) = self.conns.get_mut(&id) else {
                return false;
            };
            let resp = step(
                &mut self.hub,
                &mut conn.session,
                &*self.verifier,
                &*self.authorizer,
                msg,
            );
            conn.outbox.extend(resp.replies);
            (
                resp.broadcast,
                resp.close,
                resp.broadcast_room,
                resp.awareness,
            )
        };
        // A broadcast holds only ops the hub durably logged (see `Hub::ingest`),
        // so fanning it out never advertises an unpersisted write. Each peer is
        // sent the ops on the channel it opened for the room, so a peer
        // multiplexing several rooms can route what it receives.
        if !broadcast.is_empty() {
            if let Some(room) = room {
                let authorizer = &*self.authorizer;
                for (peer, conn) in self.conns.iter_mut() {
                    if *peer == id {
                        continue;
                    }
                    // Per-recipient redaction: a peer whose read was revoked
                    // mid-session stops receiving the room's ops at once, without
                    // waiting for it to resubscribe.
                    if !peer_may_read(authorizer, &conn.session, &room) {
                        continue;
                    }
                    for channel in conn.session.channels_for_room(&room) {
                        conn.outbox.push(Message::Ops {
                            channel,
                            ops: broadcast.clone(),
                        });
                    }
                }
            }
        }
        // Awareness is ephemeral: fan the entry out to the room's other
        // subscribers on each peer's channel; nothing is stored or echoed back.
        if let Some(a) = awareness {
            let authorizer = &*self.authorizer;
            for (peer, conn) in self.conns.iter_mut() {
                if *peer == id {
                    continue;
                }
                // Seeing a peer's presence is a read of the room, so the same
                // per-recipient check gates the awareness fan-out.
                if !peer_may_read(authorizer, &conn.session, &a.room) {
                    continue;
                }
                for channel in conn.session.channels_for_room(&a.room) {
                    conn.outbox.push(Message::AwarenessUpdate {
                        channel,
                        actor: a.actor.clone(),
                        key: a.key.clone(),
                        value: a.value.clone(),
                    });
                }
            }
        }
        !close
    }

    /// Take and clear the messages queued to send a connection.
    pub fn take_outbox(&mut self, id: ConnId) -> Vec<Message> {
        self.conns
            .get_mut(&id)
            .map(|c| std::mem::take(&mut c.outbox))
            .unwrap_or_default()
    }

    /// The shared hub, for reading merged room state.
    pub fn hub(&self) -> &Hub {
        &self.hub
    }
}

/// Whether a peer connection may currently read `room` — the per-recipient gate
/// on every fan-out. An unauthenticated connection holds no room subscription,
/// so it never qualifies.
fn peer_may_read(authorizer: &dyn Authorizer, session: &Session, room: &[u8]) -> bool {
    match session.actor() {
        Some(actor) => authorizer.authorize(actor, Action::Read, &Resource::Room(room)),
        None => false,
    }
}
