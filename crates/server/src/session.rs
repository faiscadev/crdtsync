//! The connection's protocol driver.
//!
//! A [`Session`] is one client connection. [`step`] sequences the protocol —
//! Hello, then Subscribe, then a stream of Ops — turning each inbound
//! [`Message`] into hub mutations plus a [`Response`]: messages to reply to
//! this client, ops to broadcast to the room's other subscribers, and whether
//! to close. Anything out of order is a protocol violation. Pure logic; the
//! async transport drives it.

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{ClientId, ErrorCode, Message, Op};

use crate::{Hub, RoomId};

/// One client connection's protocol state. A connection binds a single active
/// room; Subscribe switches it. Multiplexing many rooms over one connection is
/// a later concern.
pub struct Session {
    client: Option<ClientId>,
    room: Option<RoomId>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            client: None,
            room: None,
        }
    }

    /// The client named at Hello, if the handshake is done.
    pub fn client(&self) -> Option<ClientId> {
        self.client
    }

    /// The room bound by the latest Subscribe, if any.
    pub fn room(&self) -> Option<&[u8]> {
        self.room.as_deref()
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// What a [`step`] yields: replies to this client, ops to broadcast to the
/// room's other subscribers, and whether the connection should close.
#[derive(Default)]
pub struct Response {
    pub replies: Vec<Message>,
    pub broadcast: Vec<Op>,
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
            room,
            last_seen_seq,
        } => {
            if session.client.is_none() {
                return violation("subscribe before hello");
            }
            let catchup = hub.catch_up(&room, last_seen_seq);
            session.room = Some(room);
            Response {
                replies: vec![Message::Ops(catchup)],
                ..Response::default()
            }
        }
        Message::Ops(ops) => {
            let Some(client) = session.client else {
                return violation("ops before hello");
            };
            let Some(room) = session.room.as_deref() else {
                return violation("ops before subscribe");
            };
            // Every op must carry the client declared at Hello, so a
            // connection's ops stay self-consistent. Authenticating that the
            // client is who it claims is the transport's credential check;
            // this driver only enforces consistency.
            if ops.iter().any(|op| op.id.client != client) {
                return violation("op client mismatch");
            }
            // The deduped ops fan out to the room's other subscribers; nothing
            // echoes back to the sender.
            let applied = hub.ingest(room, ops);
            Response {
                broadcast: applied,
                ..Response::default()
            }
        }
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
        broadcast: Vec::new(),
        close: true,
    }
}
