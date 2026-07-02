//! Wire protocol — the framed messages two replicas exchange over a connection.
//!
//! A connection opens with an 8-byte header: a 4-byte magic identifying the
//! protocol and a 4-byte version for codec negotiation. After that, each frame
//! is one [`Message`]: a tag byte and a payload. Op batches reuse the op codec,
//! so the wire and the log share one encoding. Decoding is total — malformed
//! bytes yield a [`ProtocolError`], never a panic.

use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::protocol::{
    decode_header, decode_message, encode_header, encode_message, Channel, ErrorCode, Message,
    ProtocolError, MAGIC, PROTOCOL_VERSION,
};
use crdtsync_core::Scalar;

mod common;
use common::{cid, eid, stmp};

fn op(kind: OpKind) -> Op {
    Op::new(
        OpId {
            client: cid(1),
            seq: 7,
        },
        stmp(42, 1),
        eid(0xAB, 0xCD),
        kind,
    )
}

fn sample_ops() -> Vec<Op> {
    vec![
        op(OpKind::RegisterSet {
            key: b"name".to_vec(),
            value: Scalar::Int(5),
        }),
        op(OpKind::CounterInc {
            key: b"hits".to_vec(),
            amount: 3,
        }),
    ]
}

// --- header ---

#[test]
fn header_round_trips_the_version() {
    let bytes = encode_header(PROTOCOL_VERSION);
    assert_eq!(bytes.len(), 8);
    assert_eq!(decode_header(&bytes), Ok(PROTOCOL_VERSION));
}

#[test]
fn header_carries_an_arbitrary_version() {
    let bytes = encode_header(0xDEAD_BEEF);
    assert_eq!(decode_header(&bytes), Ok(0xDEAD_BEEF));
}

#[test]
fn header_leads_with_the_magic() {
    let bytes = encode_header(1);
    assert_eq!(&bytes[..4], &MAGIC.to_le_bytes());
}

#[test]
fn header_rejects_a_foreign_magic() {
    let mut bytes = encode_header(1);
    bytes[0] ^= 0xFF;
    assert_eq!(decode_header(&bytes), Err(ProtocolError::BadMagic));
}

#[test]
fn header_rejects_a_short_input() {
    assert_eq!(decode_header(&[0u8; 7]), Err(ProtocolError::UnexpectedEof));
}

#[test]
fn header_rejects_trailing_bytes() {
    let mut bytes = encode_header(1).to_vec();
    bytes.push(0);
    assert_eq!(decode_header(&bytes), Err(ProtocolError::TrailingBytes));
}

// --- messages round-trip ---

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

#[test]
fn hello_round_trips() {
    round_trips(Message::Hello { client: cid(9) });
}

#[test]
fn subscribe_round_trips() {
    round_trips(Message::Subscribe {
        channel: Channel(1),
        room: b"room-42".to_vec(),
        last_seen_seq: 1_000_000,
    });
}

#[test]
fn subscribe_with_an_empty_room_round_trips() {
    round_trips(Message::Subscribe {
        channel: Channel(0),
        room: Vec::new(),
        last_seen_seq: 0,
    });
}

#[test]
fn ops_round_trips_a_batch() {
    let ops = sample_ops();
    let bytes = encode_message(&Message::Ops {
        channel: Channel(0),
        ops: ops.clone(),
    });
    assert_eq!(
        decode_message(&bytes),
        Ok(Message::Ops {
            channel: Channel(0),
            ops,
        })
    );
}

#[test]
fn ops_round_trips_an_empty_batch() {
    round_trips(Message::Ops {
        channel: Channel(0),
        ops: Vec::new(),
    });
}

#[test]
fn snapshot_round_trips() {
    round_trips(Message::Snapshot {
        channel: Channel(0),
        seq: 4_200_000,
        state: vec![1, 2, 3, 0, 255, 128],
    });
}

#[test]
fn snapshot_round_trips_an_empty_state() {
    round_trips(Message::Snapshot {
        channel: Channel(0),
        seq: 0,
        state: Vec::new(),
    });
}

#[test]
fn snapshot_round_trips_a_large_state() {
    round_trips(Message::Snapshot {
        channel: Channel(0),
        seq: 1,
        state: (0..4096).map(|i| i as u8).collect(),
    });
}

#[test]
fn a_truncated_snapshot_is_an_error() {
    let bytes = encode_message(&Message::Snapshot {
        channel: Channel(0),
        seq: 9,
        state: vec![7, 7, 7],
    });
    assert_eq!(
        decode_message(&bytes[..bytes.len() - 1]),
        Err(ProtocolError::UnexpectedEof)
    );
}

#[test]
fn trailing_bytes_after_a_snapshot_are_an_error() {
    let mut bytes = encode_message(&Message::Snapshot {
        channel: Channel(0),
        seq: 9,
        state: vec![7],
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn error_round_trips() {
    round_trips(Message::Error {
        code: ErrorCode::UnsupportedVersion,
        message: "upgrade required".to_string(),
    });
}

#[test]
fn every_error_code_round_trips() {
    for code in [
        ErrorCode::ProtocolViolation,
        ErrorCode::UnsupportedVersion,
        ErrorCode::AuthFailed,
        ErrorCode::UnknownRoom,
        ErrorCode::Internal,
    ] {
        round_trips(Message::Error {
            code,
            message: String::new(),
        });
    }
}

// --- messages reject malformed input ---

#[test]
fn unknown_message_tag_is_an_error() {
    assert_eq!(
        decode_message(&[0xFF]),
        Err(ProtocolError::BadTag {
            what: "message",
            tag: 0xFF,
        })
    );
}

#[test]
fn empty_input_is_an_error() {
    assert_eq!(decode_message(&[]), Err(ProtocolError::UnexpectedEof));
}

#[test]
fn a_truncated_message_is_an_error() {
    let bytes = encode_message(&Message::Hello { client: cid(9) });
    assert_eq!(
        decode_message(&bytes[..bytes.len() - 1]),
        Err(ProtocolError::UnexpectedEof)
    );
}

#[test]
fn trailing_bytes_after_a_fixed_message_are_an_error() {
    let mut bytes = encode_message(&Message::Hello { client: cid(9) });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn an_unknown_error_code_is_an_error() {
    // Error tag, then a code tag naming no variant.
    let mut bytes = encode_message(&Message::Error {
        code: ErrorCode::Internal,
        message: String::new(),
    });
    // The code is the two bytes right after the message tag.
    bytes[1] = 0xEE;
    bytes[2] = 0x00;
    assert_eq!(
        decode_message(&bytes),
        Err(ProtocolError::BadTag {
            what: "error code",
            tag: 0xEE,
        })
    );
}

#[test]
fn a_non_utf8_error_message_is_an_error() {
    let mut bytes = encode_message(&Message::Error {
        code: ErrorCode::Internal,
        message: "x".to_string(),
    });
    // Replace the single message byte with an invalid UTF-8 lead byte.
    let last = bytes.len() - 1;
    bytes[last] = 0xFF;
    assert_eq!(decode_message(&bytes), Err(ProtocolError::BadUtf8));
}

#[test]
fn a_corrupt_op_batch_is_an_error() {
    let bytes = encode_message(&Message::Ops {
        channel: Channel(0),
        ops: sample_ops(),
    });
    // Truncate inside the batch payload; the framed codec must reject it.
    assert!(matches!(
        decode_message(&bytes[..bytes.len() - 1]),
        Err(ProtocolError::Op(_))
    ));
}
