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
    /// Opens a connection, naming the client and the app it speaks for. `app_id`
    /// selects the tenant whose registered schema (if any) the server enforces —
    /// empty means no app, a relay connection. `schema_version` is the single
    /// version the client speaks; `0` declares none (a dynamic client that adopts
    /// whatever the server serves). The server resolves `{app_id, schema_version}`
    /// against its registry — an unknown version for a registered app is refused.
    Hello {
        client: ClientId,
        app_id: Vec<u8>,
        schema_version: u32,
    },
    /// Presents an opaque credential for the server to verify. The core does not
    /// parse it; a deployment-configured verifier interprets the bytes.
    Auth { credential: Vec<u8> },
    /// Reports a verified credential, carrying the server-derived actor id. The
    /// client never asserts its own actor — it learns it here.
    AuthOk { actor: Vec<u8> },
    /// The enforcing server's advertisement of the schema it is serving this
    /// connection: `schema_version` is the active version, `schema` its bytes (a
    /// dynamic client that did not bundle adopts them; a client that already holds
    /// the version can ignore the body). A relay connection is never sent one.
    SchemaAdvert {
        schema_version: u32,
        schema: Vec<u8>,
    },
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
    /// Acknowledges an author's durably-logged ops on `channel`: `through` is the
    /// highest per-client op sequence (`OpId.seq`) the server has committed for
    /// this client, so the author drains its outbox up to it. Sent to the author
    /// only — never fanned out to the room.
    Accepted { channel: Channel, through: u64 },
    /// Reports the server sequence the client has applied on `channel`, so the
    /// server can advance this client's tombstone-GC watermark.
    Ack { channel: Channel, seq: u64 },
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
    /// presence expires (disconnect past the grace window, or a session-lifetime
    /// entry going away).
    AwarenessClear { channel: Channel, actor: Vec<u8> },
    /// Drops a single one of `actor`'s awareness entries — `key` — on `channel`,
    /// sent when that entry's timed TTL expires while the actor's other entries
    /// (and connection) live on.
    AwarenessClearKey {
        channel: Channel,
        actor: Vec<u8>,
        key: Vec<u8>,
    },
    /// Captures the current state of `channel`'s room as a named version.
    VersionCreate { channel: Channel, name: Vec<u8> },
    /// Renames a version of `channel`'s room.
    VersionRename {
        channel: Channel,
        from: Vec<u8>,
        to: Vec<u8>,
    },
    /// Deletes a named version of `channel`'s room.
    VersionDelete { channel: Channel, name: Vec<u8> },
    /// Requests the names of the versions of `channel`'s room.
    VersionList { channel: Channel },
    /// Requests the captured state of a named version of `channel`'s room.
    VersionFetch { channel: Channel, name: Vec<u8> },
    /// The current version names of `channel`'s room — the server's reply to a
    /// list request and the authoritative post-state after any version mutation.
    Versions {
        channel: Channel,
        names: Vec<Vec<u8>>,
    },
    /// A named version's captured state, the server's reply to a fetch of a
    /// version that exists, tagged with the sequence it covered.
    VersionState {
        channel: Channel,
        name: Vec<u8>,
        seq: u64,
        state: Vec<u8>,
    },
    /// A failure the server reports to the client: a closed-enum `code`, a
    /// human-readable `message`, and opaque `details` the core never parses —
    /// machine-readable specifics a client interprets by code. `details` is
    /// empty until a producer populates it.
    Error {
        code: ErrorCode,
        message: String,
        details: Vec<u8>,
    },
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
        Message::Hello {
            client,
            app_id,
            schema_version,
        } => {
            put_u8(&mut out, 0);
            out.extend_from_slice(&client.as_bytes());
            put_bytes(&mut out, app_id);
            put_u32(&mut out, *schema_version);
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
        Message::SchemaAdvert {
            schema_version,
            schema,
        } => {
            put_u8(&mut out, 21);
            put_u32(&mut out, *schema_version);
            put_bytes(&mut out, schema);
        }
        Message::Accepted { channel, through } => {
            put_u8(&mut out, 18);
            put_u32(&mut out, channel.0);
            put_u64(&mut out, *through);
        }
        Message::Ack { channel, seq } => {
            put_u8(&mut out, 19);
            put_u32(&mut out, channel.0);
            put_u64(&mut out, *seq);
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
        Message::AwarenessClearKey {
            channel,
            actor,
            key,
        } => {
            put_u8(&mut out, 20);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, actor);
            put_bytes(&mut out, key);
        }
        Message::VersionCreate { channel, name } => {
            put_u8(&mut out, 11);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
        }
        Message::VersionRename { channel, from, to } => {
            put_u8(&mut out, 12);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, from);
            put_bytes(&mut out, to);
        }
        Message::VersionDelete { channel, name } => {
            put_u8(&mut out, 13);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
        }
        Message::VersionList { channel } => {
            put_u8(&mut out, 14);
            put_u32(&mut out, channel.0);
        }
        Message::VersionFetch { channel, name } => {
            put_u8(&mut out, 15);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
        }
        Message::Versions { channel, names } => {
            put_u8(&mut out, 16);
            put_u32(&mut out, channel.0);
            put_u32(
                &mut out,
                u32::try_from(names.len()).expect("version count exceeds u32"),
            );
            for name in names {
                put_bytes(&mut out, name);
            }
        }
        Message::VersionState {
            channel,
            name,
            seq,
            state,
        } => {
            put_u8(&mut out, 17);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
            put_u64(&mut out, *seq);
            put_bytes(&mut out, state);
        }
        Message::Error {
            code,
            message,
            details,
        } => {
            put_u8(&mut out, 3);
            put_u16(&mut out, error_code_tag(*code));
            put_bytes(&mut out, message.as_bytes());
            put_bytes(&mut out, details);
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
            app_id: cur.bytes()?,
            schema_version: cur.u32()?,
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
            let details = cur.bytes()?;
            Message::Error {
                code,
                message,
                details,
            }
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
        21 => {
            let schema_version = cur.u32()?;
            let schema = cur.bytes()?;
            Message::SchemaAdvert {
                schema_version,
                schema,
            }
        }
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
        20 => {
            let channel = Channel(cur.u32()?);
            let actor = cur.bytes()?;
            let key = cur.bytes()?;
            Message::AwarenessClearKey {
                channel,
                actor,
                key,
            }
        }
        11 => Message::VersionCreate {
            channel: Channel(cur.u32()?),
            name: cur.bytes()?,
        },
        12 => {
            let channel = Channel(cur.u32()?);
            let from = cur.bytes()?;
            let to = cur.bytes()?;
            Message::VersionRename { channel, from, to }
        }
        13 => Message::VersionDelete {
            channel: Channel(cur.u32()?),
            name: cur.bytes()?,
        },
        14 => Message::VersionList {
            channel: Channel(cur.u32()?),
        },
        15 => Message::VersionFetch {
            channel: Channel(cur.u32()?),
            name: cur.bytes()?,
        },
        16 => {
            let channel = Channel(cur.u32()?);
            let count = cur.u32()?;
            // Grow as records are read rather than trusting `count` to size the
            // allocation — a bogus count then fails on the missing bytes, not on
            // a giant up-front reservation.
            let mut names = Vec::new();
            for _ in 0..count {
                names.push(cur.bytes()?);
            }
            Message::Versions { channel, names }
        }
        17 => {
            let channel = Channel(cur.u32()?);
            let name = cur.bytes()?;
            let seq = cur.u64()?;
            let state = cur.bytes()?;
            Message::VersionState {
                channel,
                name,
                seq,
                state,
            }
        }
        18 => {
            let channel = Channel(cur.u32()?);
            let through = cur.u64()?;
            Message::Accepted { channel, through }
        }
        19 => {
            let channel = Channel(cur.u32()?);
            let seq = cur.u64()?;
            Message::Ack { channel, seq }
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
