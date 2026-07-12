//! Wire protocol — logical channels multiplexed over one connection.
//!
//! A connection carries several room subscriptions at once, each on its own
//! [`Channel`] — a compact, connection-local handle the client assigns at
//! Subscribe. Every op batch, snapshot, and unsubscribe names the channel it
//! belongs to, so the receiver routes it to the right room without binding the
//! whole connection to one room. The channel handle is what stays stable as a
//! subscription later grows to `(room, branch, zone)`. Decoding stays total —
//! malformed bytes yield a [`ProtocolError`], never a panic.

use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::protocol::{decode_message, encode_message, Channel, Message, ProtocolError};
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

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

// --- a channel travels with every routed message ---

#[test]
fn subscribe_carries_its_channel() {
    round_trip(Message::Subscribe {
        channel: Channel(2),
        room: b"room-1".to_vec(),
        branch: Vec::new(),
        last_seen_seq: 40,
    });
}

#[test]
fn subscribe_carries_its_branch() {
    round_trip(Message::Subscribe {
        channel: Channel(2),
        room: b"room-1".to_vec(),
        branch: b"release-2".to_vec(),
        last_seen_seq: 40,
    });
}

#[test]
fn ops_carry_their_channel() {
    round_trip(Message::Ops {
        channel: Channel(7),
        ops: sample_ops(),
    });
}

#[test]
fn an_empty_ops_batch_still_carries_its_channel() {
    round_trip(Message::Ops {
        channel: Channel(0),
        ops: Vec::new(),
    });
}

#[test]
fn a_snapshot_carries_its_channel() {
    round_trip(Message::Snapshot {
        channel: Channel(3),
        seq: 99,
        state: vec![1, 2, 3, 0, 255],
    });
}

#[test]
fn unsubscribe_carries_its_channel() {
    round_trip(Message::Unsubscribe {
        channel: Channel(5),
    });
}

#[test]
fn channels_are_distinguished_on_the_wire() {
    let a = encode_message(&Message::Ops {
        channel: Channel(1),
        ops: sample_ops(),
    });
    let b = encode_message(&Message::Ops {
        channel: Channel(2),
        ops: sample_ops(),
    });
    assert_ne!(a, b);
}

// --- decoding stays total over the channel-bearing messages ---

// The fixed-layout, length-prefixed messages must reject every truncation. Ops
// is excluded: its batch consumes the frame's remainder, so a frame cut down to
// just its tag and channel is an empty batch, not a corruption — corruption
// *inside* an Ops batch is covered in the protocol suite.
#[test]
fn a_truncated_channel_message_is_an_error_not_a_panic() {
    for m in [
        Message::Subscribe {
            channel: Channel(2),
            room: b"r".to_vec(),
            branch: b"b".to_vec(),
            last_seen_seq: 1,
        },
        Message::Snapshot {
            channel: Channel(3),
            seq: 9,
            state: vec![1, 2, 3],
        },
        Message::Unsubscribe {
            channel: Channel(5),
        },
    ] {
        let bytes = encode_message(&m);
        for cut in 0..bytes.len() {
            assert_eq!(
                decode_message(&bytes[..cut]),
                Err(ProtocolError::UnexpectedEof),
                "truncating to {cut} bytes must error",
            );
        }
    }
}

#[test]
fn trailing_bytes_after_an_unsubscribe_are_rejected() {
    let mut bytes = encode_message(&Message::Unsubscribe {
        channel: Channel(5),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}
