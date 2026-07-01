//! Wire protocol — the framed messages two replicas exchange over a connection.
//!
//! A connection opens with an 8-byte header: a 4-byte [`MAGIC`] identifying the
//! protocol and a 4-byte version for codec negotiation. Every frame after that
//! is one [`Message`]: a tag byte and a payload. Op batches reuse the op codec,
//! so the wire and the durable log share one encoding. Decoding is total —
//! malformed bytes yield a [`ProtocolError`], never a panic.

use crate::clientid::ClientId;
use crate::codec::{
    decode_ops, encode_ops, put_bytes, put_u16, put_u64, put_u8, Cursor, DecodeError,
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

/// A closed set of failure reasons the server reports to a client.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ErrorCode {
    ProtocolViolation,
    UnsupportedVersion,
    AuthFailed,
    UnknownRoom,
    Internal,
}

/// One framed message on the wire.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message {
    /// Opens a connection, naming the client.
    Hello { client: ClientId },
    /// Joins a room, requesting every op past `last_seen_seq`.
    Subscribe { room: Vec<u8>, last_seen_seq: u64 },
    /// A batch of ops to fold in.
    Ops(Vec<Op>),
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
            room,
            last_seen_seq,
        } => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, room);
            put_u64(&mut out, *last_seen_seq);
        }
        Message::Ops(ops) => {
            put_u8(&mut out, 2);
            out.extend_from_slice(&encode_ops(ops));
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
            let room = cur.bytes()?;
            let last_seen_seq = cur.u64()?;
            Message::Subscribe {
                room,
                last_seen_seq,
            }
        }
        // An op batch is length-framed and consumes the remainder, so decoding
        // it is already total.
        2 => {
            return Ok(Message::Ops(
                decode_ops(cur.rest()).map_err(ProtocolError::Op)?,
            ))
        }
        3 => {
            let code = error_code(cur.u16()?)?;
            let message = cur.string()?;
            Message::Error { code, message }
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
    }
}

fn error_code(tag: u16) -> Result<ErrorCode, ProtocolError> {
    match tag {
        0 => Ok(ErrorCode::ProtocolViolation),
        1 => Ok(ErrorCode::UnsupportedVersion),
        2 => Ok(ErrorCode::AuthFailed),
        3 => Ok(ErrorCode::UnknownRoom),
        4 => Ok(ErrorCode::Internal),
        tag => Err(ProtocolError::BadTag {
            what: "error code",
            tag: tag as u8,
        }),
    }
}
