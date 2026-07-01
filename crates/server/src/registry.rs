//! The many-connection fan-out over one hub.
//!
//! A [`Registry`] holds every live connection, each with its own session and
//! an outbox of messages awaiting send. [`Registry::deliver`] drives one
//! connection's session, queues its replies, and fans a broadcast out to the
//! room's other connections. Pure, synchronous routing; the async transport
//! pumps bytes through it.

use std::collections::HashMap;

use crdtsync_core::{ClientId, Message};

use crate::{step, Hub, Session};

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
}

impl Registry {
    /// A registry whose hub's replicas are owned by `server`.
    pub fn new(server: ClientId) -> Self {
        Self {
            hub: Hub::new(server),
            conns: HashMap::new(),
            next: 0,
        }
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
            let resp = step(&mut self.hub, &mut conn.session, msg);
            conn.outbox.extend(resp.replies);
            let room = conn.session.room().map(<[u8]>::to_vec);
            (resp.broadcast, resp.close, room)
        };
        if !broadcast.is_empty() {
            if let Some(room) = room {
                for (peer, conn) in self.conns.iter_mut() {
                    if *peer != id && conn.session.room() == Some(room.as_slice()) {
                        conn.outbox.push(Message::Ops(broadcast.clone()));
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
