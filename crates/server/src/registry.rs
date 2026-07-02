//! The many-connection fan-out over one hub.
//!
//! A [`Registry`] holds every live connection, each with its own session and
//! an outbox of messages awaiting send. [`Registry::deliver`] drives one
//! connection's session, queues its replies, and fans a broadcast out to the
//! room's other connections. Pure, synchronous routing; the async transport
//! pumps bytes through it.

use std::collections::HashMap;
use std::io;

use crdtsync_core::{ClientId, Message};

use crate::auth::{AllowAll, Verifier};
use crate::{step, Hub, Session, Store};

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
        }
    }

    /// Use `verifier` to authenticate connections' credentials.
    pub fn set_verifier(&mut self, verifier: Box<dyn Verifier>) {
        self.verifier = verifier;
    }

    /// A registry backed by `store`: its hub replays the persisted log, and
    /// every op the hub ingests is appended before it fans out to peers.
    pub fn with_store(server: ClientId, store: Store) -> io::Result<Self> {
        let mut hub = Hub::from_rooms(server, store.load()?)?;
        hub.attach_store(store);
        Ok(Self::from_hub(hub))
    }

    /// Open a connection, returning its handle.
    pub fn connect(&mut self) -> ConnId {
        let id = ConnId(self.next);
        self.next += 1;
        self.conns.insert(
            id,
            Conn {
                session: Session::new(),
                outbox: Vec::new(),
            },
        );
        id
    }

    /// Close a connection, dropping its session and any queued messages.
    pub fn disconnect(&mut self, id: ConnId) {
        self.conns.remove(&id);
    }

    /// Drive one inbound message through the connection's session, queueing its
    /// replies and fanning any broadcast out to the room's other connections.
    /// Returns whether the connection should stay open.
    pub fn deliver(&mut self, id: ConnId, msg: Message) -> bool {
        let (broadcast, close, room) = {
            let Some(conn) = self.conns.get_mut(&id) else {
                return false;
            };
            let resp = step(&mut self.hub, &mut conn.session, &*self.verifier, msg);
            conn.outbox.extend(resp.replies);
            (resp.broadcast, resp.close, resp.broadcast_room)
        };
        // A broadcast holds only ops the hub durably logged (see `Hub::ingest`),
        // so fanning it out never advertises an unpersisted write. Each peer is
        // sent the ops on the channel it opened for the room, so a peer
        // multiplexing several rooms can route what it receives.
        if !broadcast.is_empty() {
            if let Some(room) = room {
                for (peer, conn) in self.conns.iter_mut() {
                    if *peer == id {
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
