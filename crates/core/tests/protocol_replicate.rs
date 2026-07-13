//! The `Replicate` and `ReplicaAck` wire frames — a room's leader fanning its
//! committed ops to a follower replica, and the follower's watermark reply.
//!
//! `Replicate { room, branch, ops, base_seq }` carries a length-framed room and
//! branch, the leader's compaction floor, then the op batch, which reuses the op
//! codec and consumes the frame's remainder. `ReplicaAck { room, through_seq }`
//! reports the server sequence the follower has reached. Both are node-to-node,
//! never client frames. Decoding is total — a truncation or trailing byte is a
//! `ProtocolError`, never a panic.

use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};
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

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

// --- Replicate ---

#[test]
fn replicate_round_trips() {
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 9,
    });
}

#[test]
fn replicate_round_trips_an_empty_room() {
    round_trips(Message::Replicate {
        room: Vec::new(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 0,
    });
}

#[test]
fn replicate_round_trips_an_empty_branch() {
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: Vec::new(),
        ops: sample_ops(),
        base_seq: 3,
    });
}

#[test]
fn replicate_round_trips_an_empty_batch() {
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: b"main".to_vec(),
        ops: Vec::new(),
        base_seq: 0,
    });
}

#[test]
fn replicate_round_trips_a_binary_room_and_branch() {
    round_trips(Message::Replicate {
        room: vec![0, 1, 2, 255],
        branch: vec![0xFF, 0x00, 0x80, 0x7F],
        ops: sample_ops(),
        base_seq: u64::MAX,
    });
}

#[test]
fn a_truncated_replicate_header_is_an_error_not_a_panic() {
    // The fixed leading region (room, branch, base_seq) before the op batch:
    // truncating anywhere inside it must error, never panic. The batch itself
    // consumes the frame's remainder (like `Ops`), so a shorter batch decodes as
    // valid — the corrupt-batch case is covered separately. An empty-batch frame
    // is exactly that leading region, so its length is where the batch begins.
    let header_len = encode_message(&Message::Replicate {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        ops: Vec::new(),
        base_seq: 5,
    })
    .len();
    let bytes = encode_message(&Message::Replicate {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 5,
    });
    for cut in 0..header_len {
        assert!(
            decode_message(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error, not panic",
        );
    }
}

#[test]
fn a_corrupt_op_batch_in_a_replicate_is_an_error() {
    let bytes = encode_message(&Message::Replicate {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 5,
    });
    // Truncate inside the batch payload; the framed op codec must reject it.
    assert!(matches!(
        decode_message(&bytes[..bytes.len() - 1]),
        Err(ProtocolError::Op(_))
    ));
}

// --- ReplicaAck ---

#[test]
fn replica_ack_round_trips() {
    round_trips(Message::ReplicaAck {
        room: b"room-42".to_vec(),
        through_seq: 17,
    });
}

#[test]
fn replica_ack_round_trips_an_empty_room() {
    round_trips(Message::ReplicaAck {
        room: Vec::new(),
        through_seq: 0,
    });
}

#[test]
fn replica_ack_round_trips_a_binary_room() {
    round_trips(Message::ReplicaAck {
        room: vec![0, 1, 2, 255],
        through_seq: u64::MAX,
    });
}

#[test]
fn a_truncated_replica_ack_is_an_error_not_a_panic() {
    let bytes = encode_message(&Message::ReplicaAck {
        room: b"room".to_vec(),
        through_seq: 5,
    });
    for cut in 0..bytes.len() {
        assert!(
            decode_message(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error, not panic",
        );
    }
}

#[test]
fn trailing_bytes_after_a_replica_ack_are_an_error() {
    let mut bytes = encode_message(&Message::ReplicaAck {
        room: b"room".to_vec(),
        through_seq: 5,
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}
