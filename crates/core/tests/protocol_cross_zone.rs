//! Wire protocol — the cross-zone capability-token messages.
//!
//! A client requests a cross-zone-move token with [`Message::CrossZoneToken`]
//! (naming the room, the element, and the destination zone); the server replies with
//! an opaque [`Message::CrossZoneTokenGrant`]; the client redeems it by attaching it
//! to a [`Message::CrossZoneOps`] batch. The op batch is length-framed and consumes
//! the frame remainder, exactly like a plain `Ops`. Decoding stays total.

use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::protocol::{decode_message, encode_message, Channel, Message, ProtocolError};
use crdtsync_core::{ClientId, ElementId, Scalar, Stamp};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn an_op() -> Op {
    Op::new(
        OpId {
            client: cid(1),
            seq: 3,
        },
        Stamp {
            lamport: 3,
            client: cid(1),
            offset: 0,
        },
        ElementId::from_bytes([1u8; 16]),
        OpKind::MapSet {
            key: b"k".to_vec(),
            value: Scalar::Int(1),
        },
    )
}

#[test]
fn cross_zone_messages_round_trip() {
    round_trip(Message::CrossZoneToken {
        room: b"room-1".to_vec(),
        element: ElementId::from_bytes([9u8; 16]),
        dst_zone: b"zb".to_vec(),
    });
    round_trip(Message::CrossZoneTokenGrant {
        room: b"room-1".to_vec(),
        token: vec![0, 1, 2, 3, 250, 251],
    });
    round_trip(Message::CrossZoneOps {
        channel: Channel(7),
        ops: vec![an_op()],
        token: vec![9, 8, 7],
    });
}

#[test]
fn empty_and_rootward_fields_round_trip() {
    // An empty dst zone is the unzoned root partition; an empty op batch and a
    // multi-byte token both survive the round trip.
    round_trip(Message::CrossZoneToken {
        room: Vec::new(),
        element: ElementId::from_bytes([0u8; 16]),
        dst_zone: Vec::new(),
    });
    round_trip(Message::CrossZoneOps {
        channel: Channel(0),
        ops: Vec::new(),
        token: Vec::new(),
    });
}

#[test]
fn a_truncated_fixed_cross_zone_message_is_an_error_not_a_panic() {
    // The two fixed-shape messages: every truncation short of the whole message must
    // error rather than silently decode or panic.
    for m in [
        Message::CrossZoneToken {
            room: b"room-1".to_vec(),
            element: ElementId::from_bytes([9u8; 16]),
            dst_zone: b"zb".to_vec(),
        },
        Message::CrossZoneTokenGrant {
            room: b"room-1".to_vec(),
            token: vec![1, 2, 3],
        },
    ] {
        let bytes = encode_message(&m);
        for cut in 0..bytes.len() {
            assert!(
                decode_message(&bytes[..cut]).is_err(),
                "truncating to {cut} bytes must error, not panic",
            );
        }
    }
}

#[test]
fn a_truncated_cross_zone_ops_never_panics() {
    // Like plain `Ops`, the op batch is a length-framed tail that legitimately
    // decodes at an op boundary — so a cut yields either a valid (shorter) batch or
    // an error, never a panic.
    let bytes = encode_message(&Message::CrossZoneOps {
        channel: Channel(7),
        ops: vec![an_op(), an_op()],
        token: vec![9, 8, 7],
    });
    for cut in 0..bytes.len() {
        let _ = decode_message(&bytes[..cut]);
    }
}

#[test]
fn cross_zone_tags_are_distinct_from_ops_and_each_other() {
    let tags: Vec<u8> = [
        Message::Ops {
            channel: Channel(0),
            ops: Vec::new(),
        },
        Message::CrossZoneToken {
            room: Vec::new(),
            element: ElementId::from_bytes([0u8; 16]),
            dst_zone: Vec::new(),
        },
        Message::CrossZoneTokenGrant {
            room: Vec::new(),
            token: Vec::new(),
        },
        Message::CrossZoneOps {
            channel: Channel(0),
            ops: Vec::new(),
            token: Vec::new(),
        },
    ]
    .iter()
    .map(|m| encode_message(m)[0])
    .collect();
    let mut sorted = tags.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), tags.len(), "cross-zone tags must be distinct");
}

#[test]
fn trailing_bytes_after_a_grant_are_rejected() {
    let mut bytes = encode_message(&Message::CrossZoneTokenGrant {
        room: b"r".to_vec(),
        token: vec![1],
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}
