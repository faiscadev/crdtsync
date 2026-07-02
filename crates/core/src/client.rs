//! The replica's side of the wire protocol.
//!
//! A [`ClientSession`] is the client-side mirror of the server's session
//! driver. It opens with Hello, then holds several room subscriptions at once —
//! each on its own [`Channel`], with its own local [`Document`] and caught-up
//! sequence. Subscribe assigns the next channel and draws the server's catch-up
//! — an op delta or a whole-replica [`Message::Snapshot`]. Inbound frames route
//! to a room by their channel; a reconnect resumes each room from where it left
//! off instead of replaying from zero. Pure logic: messages in, local replicas
//! and messages out; the transport moves the bytes.

use std::collections::HashMap;

use crate::doc::MapCursor;
use crate::{Channel, ClientId, Document, ErrorCode, Message};

/// Why an inbound message could not be folded into a replica.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ClientError {
    /// A frame that only travels client-to-server arrived from the server.
    UnexpectedMessage(&'static str),
    /// A snapshot's state bytes did not decode into a replica.
    BadSnapshot,
    /// A routed frame named a channel this session does not hold.
    UnknownChannel(Channel),
    /// The server reported a failure.
    Server { code: ErrorCode, message: String },
}

/// One subscribed room: its local replica, the room name, how far it has caught
/// up, and the peers' ephemeral awareness entries keyed by `(actor, key)`.
struct Room {
    room: Vec<u8>,
    doc: Document,
    last_seen_seq: u64,
    awareness: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
}

/// A replica's connection carrying several room subscriptions, each keyed by the
/// channel the client assigned it.
pub struct ClientSession {
    client: ClientId,
    actor: Option<Vec<u8>>,
    rooms: HashMap<Channel, Room>,
    next_channel: u32,
}

impl ClientSession {
    /// A session for `client` holding no rooms yet.
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            actor: None,
            rooms: HashMap::new(),
            next_channel: 0,
        }
    }

    /// The opening frame, naming this replica to the server.
    pub fn hello(&self) -> Message {
        Message::Hello {
            client: self.client,
        }
    }

    /// Present an opaque credential for the server to verify. The server derives
    /// the actor and returns it in AuthOk; the client never asserts its own.
    pub fn auth(&self, credential: &[u8]) -> Message {
        Message::Auth {
            credential: credential.to_vec(),
        }
    }

    /// The server-derived actor for this session, once AuthOk has arrived.
    pub fn actor(&self) -> Option<&[u8]> {
        self.actor.as_deref()
    }

    /// Join `room` on a fresh channel, requesting everything from the start.
    /// Returns the assigned channel and the Subscribe frame to send.
    pub fn subscribe(&mut self, room: &[u8]) -> (Channel, Message) {
        let channel = Channel(self.next_channel);
        self.next_channel += 1;
        self.rooms.insert(
            channel,
            Room {
                room: room.to_vec(),
                doc: Document::new(self.client),
                last_seen_seq: 0,
                awareness: HashMap::new(),
            },
        );
        (
            channel,
            Message::Subscribe {
                channel,
                room: room.to_vec(),
                last_seen_seq: 0,
            },
        )
    }

    /// Re-issue the Subscribe for a held channel from its caught-up position, so
    /// a reconnect resumes with a delta rather than the whole room. `None` if
    /// the channel isn't held.
    pub fn resume(&self, channel: Channel) -> Option<Message> {
        let room = self.rooms.get(&channel)?;
        Some(Message::Subscribe {
            channel,
            room: room.room.clone(),
            last_seen_seq: room.last_seen_seq,
        })
    }

    /// Apply a local edit to `channel`'s room and return the ops to broadcast.
    /// The seen sequence is the server's, so an unacknowledged local write
    /// leaves it untouched until the ops come back with a sequence assigned.
    /// `None` if the channel isn't held.
    pub fn edit<F>(&mut self, channel: Channel, f: F) -> Option<Message>
    where
        F: FnOnce(&mut MapCursor),
    {
        let room = self.rooms.get_mut(&channel)?;
        Some(Message::Ops {
            channel,
            ops: room.doc.transact(f),
        })
    }

    /// Publish an ephemeral awareness entry on `channel`'s room, returning the
    /// frame to send. `None` if the channel isn't held. The entry is transient —
    /// it is not stored locally or reflected back.
    pub fn set_awareness(&self, channel: Channel, key: &[u8], value: &[u8]) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::AwarenessSet {
            channel,
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    /// A peer's awareness entry on `channel`, by publishing actor and key.
    pub fn awareness(&self, channel: Channel, actor: &[u8], key: &[u8]) -> Option<&[u8]> {
        self.rooms
            .get(&channel)?
            .awareness
            .get(&(actor.to_vec(), key.to_vec()))
            .map(Vec::as_slice)
    }

    /// How many awareness entries `channel` currently holds.
    pub fn awareness_len(&self, channel: Channel) -> usize {
        self.rooms.get(&channel).map_or(0, |r| r.awareness.len())
    }

    /// Leave the room on `channel`, dropping its replica. Returns the Unsubscribe
    /// frame to send, or `None` if the channel isn't held.
    pub fn unsubscribe(&mut self, channel: Channel) -> Option<Message> {
        self.rooms.remove(&channel)?;
        Some(Message::Unsubscribe { channel })
    }

    /// Fold one server message into the addressed room. An op delta applies in
    /// place; a snapshot replaces that room's replica with the server's state up
    /// to its tagged sequence. Frames the server never sends, a frame for a
    /// channel this session does not hold, and a snapshot that fails to decode
    /// are refused without touching any replica.
    pub fn receive(&mut self, msg: Message) -> Result<(), ClientError> {
        match msg {
            Message::Ops { channel, ops } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // The delta is a contiguous run of ops at the head, each holding
                // one server sequence, so the seen sequence advances by the batch
                // length — a redelivered op still occupies its slot even though
                // the replica deduplicates it away.
                let count = ops.len() as u64;
                for op in &ops {
                    room.doc.apply(op);
                }
                room.last_seen_seq += count;
                Ok(())
            }
            Message::Snapshot {
                channel,
                seq,
                state,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // Adopt the server's state but keep our own identity for the ops
                // we author next. A decode failure leaves the room untouched.
                let doc = Document::decode_state_as(self.client, &state)
                    .map_err(|_| ClientError::BadSnapshot)?;
                room.doc = doc;
                room.last_seen_seq = seq;
                Ok(())
            }
            Message::AuthOk { actor } => {
                self.actor = Some(actor);
                Ok(())
            }
            Message::AwarenessUpdate {
                channel,
                actor,
                key,
                value,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // Last-writer-wins per (actor, key); the peer's latest entry
                // replaces any prior one.
                room.awareness.insert((actor, key), value);
                Ok(())
            }
            Message::AwarenessClear { channel, actor } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // The actor's presence expired; drop all of its entries.
                room.awareness.retain(|(a, _), _| *a != actor);
                Ok(())
            }
            Message::Error { code, message } => Err(ClientError::Server { code, message }),
            Message::Auth { .. } => Err(ClientError::UnexpectedMessage("server sent auth")),
            Message::AwarenessSet { .. } => Err(ClientError::UnexpectedMessage(
                "server sent an awareness set",
            )),
            Message::Hello { .. } => Err(ClientError::UnexpectedMessage("server sent hello")),
            Message::Subscribe { .. } => {
                Err(ClientError::UnexpectedMessage("server sent subscribe"))
            }
            Message::Unsubscribe { .. } => {
                Err(ClientError::UnexpectedMessage("server sent unsubscribe"))
            }
        }
    }

    /// The local replica for `channel`'s room, if held.
    pub fn document(&self, channel: Channel) -> Option<&Document> {
        self.rooms.get(&channel).map(|r| &r.doc)
    }

    /// The highest server sequence `channel`'s room has caught up to.
    pub fn last_seen_seq(&self, channel: Channel) -> Option<u64> {
        self.rooms.get(&channel).map(|r| r.last_seen_seq)
    }

    /// The room name bound to `channel`, if held.
    pub fn room(&self, channel: Channel) -> Option<&[u8]> {
        self.rooms.get(&channel).map(|r| r.room.as_slice())
    }
}
