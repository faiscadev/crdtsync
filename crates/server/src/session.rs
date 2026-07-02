//! The connection's protocol driver.
//!
//! A [`Session`] is one client connection. [`step`] sequences the protocol —
//! Hello, then Subscribe, then a stream of Ops — turning each inbound
//! [`Message`] into hub mutations plus a [`Response`]: messages to reply to
//! this client, ops to broadcast to the room's other subscribers, and whether
//! to close. Anything out of order is a protocol violation. Pure logic; the
//! async transport drives it.

use std::collections::HashMap;

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{Channel, ClientId, ErrorCode, Message, Op};

use crate::{Catchup, Hub, RoomId};

/// One client connection's protocol state. A connection multiplexes several
/// room subscriptions, each on its own [`Channel`]; the client assigns the
/// handle at Subscribe and every later frame names it.
pub struct Session {
    client: Option<ClientId>,
    channels: HashMap<Channel, RoomId>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            client: None,
            channels: HashMap::new(),
        }
    }

    /// The client named at Hello, if the handshake is done.
    pub fn client(&self) -> Option<ClientId> {
        self.client
    }

    /// The channels this connection has bound to `room`. A broadcast for the
    /// room is delivered on each — one connection may hold the same room on
    /// more than one channel.
    pub fn channels_for_room(&self, room: &[u8]) -> Vec<Channel> {
        self.channels
            .iter()
            .filter(|(_, r)| r.as_slice() == room)
            .map(|(c, _)| *c)
            .collect()
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// What a [`step`] yields: replies to this client, ops to broadcast to the
/// other subscribers of `broadcast_room`, and whether the connection should
/// close. `broadcast_room` is `None` when there is nothing to fan out.
#[derive(Default)]
pub struct Response {
    pub replies: Vec<Message>,
    pub broadcast: Vec<Op>,
    pub broadcast_room: Option<RoomId>,
    pub close: bool,
}

/// Drive one inbound message through the session, mutating the hub and
/// returning what to send and whether to close.
pub fn step(hub: &mut Hub, session: &mut Session, msg: Message) -> Response {
    match msg {
        Message::Hello { client } => {
            if session.client.is_some() {
                return violation("already said hello");
            }
            // Hello establishes quietly; the version was negotiated at the
            // connection header before any message.
            session.client = Some(client);
            Response::default()
        }
        Message::Subscribe {
            channel,
            room,
            last_seen_seq,
        } => {
            if session.client.is_none() {
                return violation("subscribe before hello");
            }
            if session.channels.contains_key(&channel) {
                return violation("channel already subscribed");
            }
            let reply = match hub.catch_up(&room, last_seen_seq) {
                Catchup::Ops(ops) => Message::Ops { channel, ops },
                Catchup::Snapshot { seq, state } => Message::Snapshot {
                    channel,
                    seq,
                    state,
                },
            };
            session.channels.insert(channel, room);
            Response {
                replies: vec![reply],
                ..Response::default()
            }
        }
        Message::Unsubscribe { channel } => {
            if session.client.is_none() {
                return violation("unsubscribe before hello");
            }
            if session.channels.remove(&channel).is_none() {
                return violation("unsubscribe of an unbound channel");
            }
            Response::default()
        }
        Message::Ops { channel, ops } => {
            let Some(client) = session.client else {
                return violation("ops before hello");
            };
            let Some(room) = session.channels.get(&channel).cloned() else {
                return violation("ops on an unbound channel");
            };
            // Every op must carry the client declared at Hello, so a
            // connection's ops stay self-consistent. Authenticating that the
            // client is who it claims is the transport's credential check;
            // this driver only enforces consistency.
            if ops.iter().any(|op| op.id.client != client) {
                return violation("op client mismatch");
            }
            // The deduped ops fan out to the room's other subscribers; nothing
            // echoes back to the sender. A hub that cannot durably record the
            // ops rejects the write rather than advertising an unpersisted one.
            match hub.ingest(&room, ops) {
                Ok(applied) => Response {
                    broadcast: applied,
                    broadcast_room: Some(room),
                    ..Response::default()
                },
                Err(_) => Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::Internal,
                        message: "failed to persist ops".to_string(),
                    }],
                    close: true,
                    ..Response::default()
                },
            }
        }
        Message::Snapshot { .. } => violation("client sent a snapshot"),
        Message::Error { .. } => violation("client sent an error"),
    }
}

/// Accept a peer's protocol version, or refuse it with an Error to send back.
pub fn negotiate(version: u32) -> Result<(), Message> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Message::Error {
            code: ErrorCode::UnsupportedVersion,
            message: "unsupported protocol version".to_string(),
        })
    }
}

fn violation(reason: &str) -> Response {
    Response {
        replies: vec![Message::Error {
            code: ErrorCode::ProtocolViolation,
            message: reason.to_string(),
        }],
        close: true,
        ..Response::default()
    }
}
