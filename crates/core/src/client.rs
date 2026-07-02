//! The replica's side of the wire protocol.
//!
//! A [`ClientSession`] is the client-side mirror of the server's session
//! driver. It opens with Hello, joins a room with Subscribe carrying the last
//! sequence it saw, and folds the server's catch-up — an op delta or a
//! whole-replica [`Message::Snapshot`] — into a local [`Document`]. It tracks
//! how far it has caught up so a reconnect resumes from there instead of
//! replaying the room from zero. Pure logic: messages in, a local replica and
//! messages out; the transport moves the bytes.

use crate::doc::MapCursor;
use crate::{ClientId, Document, ErrorCode, Message};

/// Why an inbound message could not be folded into the replica.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ClientError {
    /// A frame that only travels client-to-server arrived from the server.
    UnexpectedMessage(&'static str),
    /// A snapshot's state bytes did not decode into a replica.
    BadSnapshot,
    /// The server reported a failure.
    Server { code: ErrorCode, message: String },
}

/// One replica's connection to a room: its local state and how far it has
/// caught up.
pub struct ClientSession {
    client: ClientId,
    doc: Document,
    room: Option<Vec<u8>>,
    last_seen_seq: u64,
}

impl ClientSession {
    /// A replica for `client` with empty state, joined to no room yet.
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            doc: Document::new(client),
            room: None,
            last_seen_seq: 0,
        }
    }

    /// The opening frame, naming this replica to the server.
    pub fn hello(&self) -> Message {
        Message::Hello {
            client: self.client,
        }
    }

    /// Join `room`, requesting everything past the sequence already seen. A
    /// fresh session asks from zero; a reconnecting one carries its caught-up
    /// position, so the server can answer with a delta rather than the whole
    /// room.
    pub fn subscribe(&mut self, room: &[u8]) -> Message {
        self.room = Some(room.to_vec());
        Message::Subscribe {
            room: room.to_vec(),
            last_seen_seq: self.last_seen_seq,
        }
    }

    /// Apply a local edit and return the ops to broadcast. The seen sequence is
    /// the server's, so an unacknowledged local write leaves it untouched until
    /// the ops come back with a sequence assigned.
    pub fn edit<F>(&mut self, f: F) -> Message
    where
        F: FnOnce(&mut MapCursor),
    {
        Message::Ops(self.doc.transact(f))
    }

    /// Fold one server message into the replica. An op delta applies in place; a
    /// snapshot replaces the replica with the server's state up to its tagged
    /// sequence. Frames the server never sends, and a snapshot that fails to
    /// decode, are refused without touching the replica.
    pub fn receive(&mut self, msg: Message) -> Result<(), ClientError> {
        match msg {
            Message::Ops(ops) => {
                // The delta is a contiguous run of ops at the head, each holding
                // one server sequence, so the seen sequence advances by the
                // batch length — a redelivered op still occupies its slot even
                // though the replica deduplicates it away.
                let count = ops.len() as u64;
                for op in &ops {
                    self.doc.apply(op);
                }
                self.last_seen_seq += count;
                Ok(())
            }
            Message::Snapshot { seq, state } => {
                // Adopt the server's state but keep our own identity for the ops
                // we author next.
                let doc = Document::decode_state_as(self.client, &state)
                    .map_err(|_| ClientError::BadSnapshot)?;
                self.doc = doc;
                self.last_seen_seq = seq;
                Ok(())
            }
            Message::Error { code, message } => Err(ClientError::Server { code, message }),
            Message::Hello { .. } => Err(ClientError::UnexpectedMessage("server sent hello")),
            Message::Subscribe { .. } => {
                Err(ClientError::UnexpectedMessage("server sent subscribe"))
            }
        }
    }

    /// The local replica.
    pub fn document(&self) -> &Document {
        &self.doc
    }

    /// The highest server sequence this replica has caught up to.
    pub fn last_seen_seq(&self) -> u64 {
        self.last_seen_seq
    }

    /// The room joined by the latest Subscribe, if any.
    pub fn room(&self) -> Option<&[u8]> {
        self.room.as_deref()
    }
}
