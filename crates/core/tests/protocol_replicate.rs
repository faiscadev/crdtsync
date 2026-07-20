//! The `Replicate` and `ReplicaAck` wire frames — a room's leader fanning its
//! committed ops to a follower replica, and the follower's watermark reply.
//!
//! `Replicate { room, branch, ops, base_seq, epoch }` carries a length-framed
//! room and branch, the leader's compaction floor, its leadership epoch, then the
//! op batch, which reuses the op codec and consumes the frame's remainder.
//! `ReplicaAck { room, through_seq }`
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
        epoch: 4,
    });
}

#[test]
fn replicate_round_trips_the_epoch() {
    // The leadership epoch is carried verbatim across the wire, at its extremes.
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 1,
        epoch: 0,
    });
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 1,
        epoch: u64::MAX,
    });
}

#[test]
fn replicate_round_trips_an_empty_room() {
    round_trips(Message::Replicate {
        room: Vec::new(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 0,
        epoch: 1,
    });
}

#[test]
fn replicate_round_trips_an_empty_branch() {
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: Vec::new(),
        ops: sample_ops(),
        base_seq: 3,
        epoch: 7,
    });
}

#[test]
fn replicate_round_trips_an_empty_batch() {
    round_trips(Message::Replicate {
        room: b"room-42".to_vec(),
        branch: b"main".to_vec(),
        ops: Vec::new(),
        base_seq: 0,
        epoch: 2,
    });
}

#[test]
fn replicate_round_trips_a_binary_room_and_branch() {
    round_trips(Message::Replicate {
        room: vec![0, 1, 2, 255],
        branch: vec![0xFF, 0x00, 0x80, 0x7F],
        ops: sample_ops(),
        base_seq: u64::MAX,
        epoch: u64::MAX,
    });
}

#[test]
fn a_truncated_replicate_header_is_an_error_not_a_panic() {
    // The fixed leading region (room, branch, base_seq, epoch) before the op
    // batch: truncating anywhere inside it must error, never panic. The batch
    // itself consumes the frame's remainder (like `Ops`), so a shorter batch
    // decodes as valid — the corrupt-batch case is covered separately. An empty-
    // batch frame is exactly that leading region, so its length is where the batch
    // begins.
    let header_len = encode_message(&Message::Replicate {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        ops: Vec::new(),
        base_seq: 5,
        epoch: 3,
    })
    .len();
    let bytes = encode_message(&Message::Replicate {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        ops: sample_ops(),
        base_seq: 5,
        epoch: 3,
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
        epoch: 1,
    });
    // Truncate inside the batch payload; the framed op codec must reject it.
    assert!(matches!(
        decode_message(&bytes[..bytes.len() - 1]),
        Err(ProtocolError::Op(_))
    ));
}

// --- ReplicateSnapshot ---

#[test]
fn replicate_snapshot_round_trips() {
    round_trips(Message::ReplicateSnapshot {
        room: b"room-42".to_vec(),
        branch: b"main".to_vec(),
        seq: 12,
        state: vec![1, 2, 3, 4, 5],
        epoch: 4,
    });
}

#[test]
fn replicate_snapshot_round_trips_the_extremes() {
    // The sequence and epoch are carried verbatim at their extremes, and an empty
    // state (a snapshot of an empty room) round-trips.
    round_trips(Message::ReplicateSnapshot {
        room: Vec::new(),
        branch: Vec::new(),
        seq: 0,
        state: Vec::new(),
        epoch: 0,
    });
    round_trips(Message::ReplicateSnapshot {
        room: vec![0, 1, 2, 255],
        branch: vec![0xFF, 0x00, 0x80, 0x7F],
        seq: u64::MAX,
        state: vec![0xAB; 300],
        epoch: u64::MAX,
    });
}

#[test]
fn a_truncated_replicate_snapshot_is_an_error_not_a_panic() {
    let bytes = encode_message(&Message::ReplicateSnapshot {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        seq: 9,
        state: vec![7, 8, 9],
        epoch: 3,
    });
    for cut in 0..bytes.len() {
        assert!(
            decode_message(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error, not panic",
        );
    }
}

#[test]
fn trailing_bytes_after_a_replicate_snapshot_are_an_error() {
    let mut bytes = encode_message(&Message::ReplicateSnapshot {
        room: b"room".to_vec(),
        branch: b"main".to_vec(),
        seq: 9,
        state: vec![7, 8, 9],
        epoch: 3,
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

// --- FollowerHeads ---

#[test]
fn follower_heads_round_trips() {
    round_trips(Message::FollowerHeads {
        reporter: b"10.0.0.2:9000".to_vec(),
        heads: vec![(b"room-a".to_vec(), 5), (b"room-b".to_vec(), 0)],
    });
}

#[test]
fn follower_heads_round_trips_an_empty_manifest() {
    // A fully-wiped node reports no held rooms — the leader fail-closes every led
    // room to head 0. An empty reporter (defensive) round-trips too.
    round_trips(Message::FollowerHeads {
        reporter: b"node".to_vec(),
        heads: Vec::new(),
    });
    round_trips(Message::FollowerHeads {
        reporter: Vec::new(),
        heads: vec![(Vec::new(), u64::MAX)],
    });
}

#[test]
fn follower_heads_round_trips_a_binary_room() {
    round_trips(Message::FollowerHeads {
        reporter: vec![0, 1, 2, 255],
        heads: vec![(vec![0xFF, 0x00, 0x80], u64::MAX)],
    });
}

#[test]
fn a_truncated_follower_heads_is_an_error_not_a_panic() {
    let bytes = encode_message(&Message::FollowerHeads {
        reporter: b"node".to_vec(),
        heads: vec![(b"room".to_vec(), 3)],
    });
    for cut in 0..bytes.len() {
        assert!(
            decode_message(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error, not panic",
        );
    }
}

#[test]
fn trailing_bytes_after_follower_heads_are_an_error() {
    let mut bytes = encode_message(&Message::FollowerHeads {
        reporter: b"node".to_vec(),
        heads: vec![(b"room".to_vec(), 3)],
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
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
