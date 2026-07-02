//! Wire protocol — the framed messages two replicas exchange over a connection.
//!
//! A connection opens with an 8-byte header: a 4-byte [`MAGIC`] identifying the
//! protocol and a 4-byte version for codec negotiation. Every frame after that
//! is one [`Message`]: a tag byte and a payload. Op batches reuse the op codec,
//! so the wire and the durable log share one encoding. Decoding is total —
//! malformed bytes yield a [`ProtocolError`], never a panic.

use crate::clientid::ClientId;
use crate::codec::{
    decode_ops, encode_ops, put_bytes, put_u16, put_u32, put_u64, put_u8, Cursor, DecodeError,
};
use crate::op::Op;

/// Identifies a crdtsync stream, so a foreign connection is rejected at once.
pub const MAGIC: u32 = u32::from_le_bytes(*b"CRDT");

/// The protocol version this build speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Why a byte string could not be decoded into a header or message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ProtocolError {
    /// The header did not lead with [`MAGIC`].
    BadMagic,
    /// The input ended before a field was fully read.
    UnexpectedEof,
    /// A tag byte named no known variant.
    BadTag { what: &'static str, tag: u8 },
    /// A text field held bytes that are not valid UTF-8.
    BadUtf8,
    /// Bytes remained after decoding a complete header or message.
    TrailingBytes,
    /// An op batch payload was itself malformed.
    Op(DecodeError),
}

impl From<DecodeError> for ProtocolError {
    fn from(e: DecodeError) -> Self {
        match e {
            DecodeError::UnexpectedEof => ProtocolError::UnexpectedEof,
            DecodeError::BadUtf8 => ProtocolError::BadUtf8,
            DecodeError::TrailingBytes => ProtocolError::TrailingBytes,
            DecodeError::BadTag { what, tag } => ProtocolError::BadTag { what, tag },
        }
    }
}

/// A connection-local handle for one room subscription. The client assigns it
/// at Subscribe; every op batch, snapshot, and unsubscribe on that subscription
/// names it, so several rooms multiplex over one connection. The handle is what
/// stays stable as a subscription later widens to `(room, branch, zone)`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Channel(pub u32);

/// A closed set of failure reasons the server reports to a client.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ErrorCode {
    ProtocolViolation,
    UnsupportedVersion,
    AuthFailed,
    UnknownRoom,
    Internal,
    /// The authenticated actor is not permitted the requested action.
    Forbidden,
}

/// One framed message on the wire.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message {
    /// Opens a connection, naming the client.
    Hello { client: ClientId },
    /// Presents an opaque credential for the server to verify. The core does not
    /// parse it; a deployment-configured verifier interprets the bytes.
    Auth { credential: Vec<u8> },
    /// Reports a verified credential, carrying the server-derived actor id. The
    /// client never asserts its own actor — it learns it here.
    AuthOk { actor: Vec<u8> },
    /// Joins a room on `channel`, requesting every op past `last_seen_seq`.
    Subscribe {
        channel: Channel,
        room: Vec<u8>,
        last_seen_seq: u64,
    },
    /// Leaves the room bound to `channel`, freeing the handle.
    Unsubscribe { channel: Channel },
    /// A batch of ops to fold into `channel`'s room.
    Ops { channel: Channel, ops: Vec<Op> },
    /// A whole-replica state snapshot the server sends a subscriber that fell
    /// below a room's compaction floor, tagged with the channel it answers and
    /// the sequence it lands at.
    Snapshot {
        channel: Channel,
        seq: u64,
        state: Vec<u8>,
    },
    /// Publishes this client's ephemeral awareness entry `key` on `channel`'s
    /// room, replacing any prior value. Not durable — never logged or snapshotted.
    AwarenessSet {
        channel: Channel,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// A peer's awareness entry, fanned out to the room, tagged with the
    /// publishing actor so receivers know whose it is.
    AwarenessUpdate {
        channel: Channel,
        actor: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Drops all of `actor`'s awareness on `channel` — sent when that actor's
    /// presence expires (disconnect past the grace window, or TTL).
    AwarenessClear { channel: Channel, actor: Vec<u8> },
    /// A failure the server reports to the client.
    Error { code: ErrorCode, message: String },
}

/// Encode the 8-byte connection header: [`MAGIC`] then the version.
pub fn encode_header(version: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&MAGIC.to_le_bytes());
    out[4..].copy_from_slice(&version.to_le_bytes());
    out
}

/// Decode the connection header, returning the peer's negotiated version.
pub fn decode_header(bytes: &[u8]) -> Result<u32, ProtocolError> {
    if bytes.len() < 8 {
        return Err(ProtocolError::UnexpectedEof);
    }
    if bytes.len() > 8 {
        return Err(ProtocolError::TrailingBytes);
    }
    let magic = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    Ok(u32::from_le_bytes(bytes[4..].try_into().unwrap()))
}

/// Encode one message to its byte string.
pub fn encode_message(m: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    match m {
        Message::Hello { client } => {
            put_u8(&mut out, 0);
            out.extend_from_slice(&client.as_bytes());
        }
        Message::Subscribe {
            channel,
            room,
            last_seen_seq,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, room);
            put_u64(&mut out, *last_seen_seq);
        }
        Message::Ops { channel, ops } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, channel.0);
            out.extend_from_slice(&encode_ops(ops));
        }
        Message::Snapshot {
            channel,
            seq,
            state,
        } => {
            put_u8(&mut out, 4);
            put_u32(&mut out, channel.0);
            put_u64(&mut out, *seq);
            put_bytes(&mut out, state);
        }
        Message::Unsubscribe { channel } => {
            put_u8(&mut out, 5);
            put_u32(&mut out, channel.0);
        }
        Message::Auth { credential } => {
            put_u8(&mut out, 6);
            put_bytes(&mut out, credential);
        }
        Message::AuthOk { actor } => {
            put_u8(&mut out, 7);
            put_bytes(&mut out, actor);
        }
        Message::AwarenessSet {
            channel,
            key,
            value,
        } => {
            put_u8(&mut out, 8);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        Message::AwarenessUpdate {
            channel,
            actor,
            key,
            value,
        } => {
            put_u8(&mut out, 9);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, actor);
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        Message::AwarenessClear { channel, actor } => {
            put_u8(&mut out, 10);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, actor);
        }
        Message::Error { code, message } => {
            put_u8(&mut out, 3);
            put_u16(&mut out, error_code_tag(*code));
            put_bytes(&mut out, message.as_bytes());
        }
    }
    out
}

/// Decode exactly one message; surplus bytes are an error.
pub fn decode_message(bytes: &[u8]) -> Result<Message, ProtocolError> {
    let mut cur = Cursor::new(bytes);
    let msg = match cur.u8()? {
        0 => Message::Hello {
            client: cur.client()?,
        },
        1 => {
            let channel = Channel(cur.u32()?);
            let room = cur.bytes()?;
            let last_seen_seq = cur.u64()?;
            Message::Subscribe {
                channel,
                room,
                last_seen_seq,
            }
        }
        // An op batch is length-framed and consumes the remainder after the
        // channel, so decoding it is already total.
        2 => {
            let channel = Channel(cur.u32()?);
            return Ok(Message::Ops {
                channel,
                ops: decode_ops(cur.rest()).map_err(ProtocolError::Op)?,
            });
        }
        3 => {
            let code = error_code(cur.u16()?)?;
            let message = cur.string()?;
            Message::Error { code, message }
        }
        4 => {
            let channel = Channel(cur.u32()?);
            let seq = cur.u64()?;
            let state = cur.bytes()?;
            Message::Snapshot {
                channel,
                seq,
                state,
            }
        }
        5 => Message::Unsubscribe {
            channel: Channel(cur.u32()?),
        },
        6 => Message::Auth {
            credential: cur.bytes()?,
        },
        7 => Message::AuthOk {
            actor: cur.bytes()?,
        },
        8 => {
            let channel = Channel(cur.u32()?);
            let key = cur.bytes()?;
            let value = cur.bytes()?;
            Message::AwarenessSet {
                channel,
                key,
                value,
            }
        }
        9 => {
            let channel = Channel(cur.u32()?);
            let actor = cur.bytes()?;
            let key = cur.bytes()?;
            let value = cur.bytes()?;
            Message::AwarenessUpdate {
                channel,
                actor,
                key,
                value,
            }
        }
        10 => {
            let channel = Channel(cur.u32()?);
            let actor = cur.bytes()?;
            Message::AwarenessClear { channel, actor }
        }
        tag => {
            return Err(ProtocolError::BadTag {
                what: "message",
                tag,
            })
        }
    };
    if !cur.at_end() {
        return Err(ProtocolError::TrailingBytes);
    }
    Ok(msg)
}

fn error_code_tag(code: ErrorCode) -> u16 {
    match code {
        ErrorCode::ProtocolViolation => 0,
        ErrorCode::UnsupportedVersion => 1,
        ErrorCode::AuthFailed => 2,
        ErrorCode::UnknownRoom => 3,
        ErrorCode::Internal => 4,
        ErrorCode::Forbidden => 5,
    }
}

fn error_code(tag: u16) -> Result<ErrorCode, ProtocolError> {
    match tag {
        0 => Ok(ErrorCode::ProtocolViolation),
        1 => Ok(ErrorCode::UnsupportedVersion),
        2 => Ok(ErrorCode::AuthFailed),
        3 => Ok(ErrorCode::UnknownRoom),
        4 => Ok(ErrorCode::Internal),
        5 => Ok(ErrorCode::Forbidden),
        tag => Err(ProtocolError::BadTag {
            what: "error code",
            tag: tag as u8,
        }),
    }
}
