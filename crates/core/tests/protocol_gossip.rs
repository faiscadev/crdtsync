//! The `Gossip` wire frame — a node advertising the cluster members it knows for
//! anti-entropy membership discovery.
//!
//! `Gossip { members }` carries a length-prefixed list of `(node_id,
//! advertise_addr)` pairs, each a length-framed byte string. A receiver unions
//! the pairs into its own membership, so the member set converges cluster-wide.
//! Node-to-node, never a client frame. Decoding is total — a truncation or a
//! trailing byte is a `ProtocolError`, never a panic.

use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

#[test]
fn gossip_round_trips_no_members() {
    round_trips(Message::Gossip {
        members: Vec::new(),
    });
}

#[test]
fn gossip_round_trips_one_member() {
    round_trips(Message::Gossip {
        members: vec![(b"node-a".to_vec(), b"10.0.0.1:9000".to_vec())],
    });
}

#[test]
fn gossip_round_trips_many_members() {
    round_trips(Message::Gossip {
        members: vec![
            (b"node-a".to_vec(), b"10.0.0.1:9000".to_vec()),
            (b"node-b".to_vec(), b"10.0.0.2:9000".to_vec()),
            (b"node-c".to_vec(), b"10.0.0.3:9000".to_vec()),
            (b"node-d".to_vec(), b"10.0.0.4:9000".to_vec()),
        ],
    });
}

#[test]
fn gossip_round_trips_binary_and_empty_fields() {
    // A node id or address is opaque bytes: empty, and arbitrary non-UTF-8, both
    // survive the round trip.
    round_trips(Message::Gossip {
        members: vec![
            (Vec::new(), Vec::new()),
            (vec![0, 1, 2, 255], vec![0xFF, 0x00, 0x80, 0x7F]),
        ],
    });
}

#[test]
fn a_truncated_gossip_is_an_error_not_a_panic() {
    // Every prefix of a well-formed multi-member frame must error, never panic:
    // truncating the count, a pair's node length, its bytes, or its addr each
    // lands inside a field the cursor is mid-read on.
    let bytes = encode_message(&Message::Gossip {
        members: vec![
            (b"node-a".to_vec(), b"10.0.0.1:9000".to_vec()),
            (b"node-b".to_vec(), b"10.0.0.2:9000".to_vec()),
        ],
    });
    for cut in 0..bytes.len() {
        assert!(
            decode_message(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error, not panic",
        );
    }
}

#[test]
fn trailing_bytes_after_a_gossip_are_an_error() {
    let mut bytes = encode_message(&Message::Gossip {
        members: vec![(b"node-a".to_vec(), b"10.0.0.1:9000".to_vec())],
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn a_gossip_count_past_the_payload_is_an_error() {
    // A count larger than the pairs that follow must fail on the missing bytes,
    // not over-reserve or panic.
    let mut bytes = encode_message(&Message::Gossip {
        members: vec![(b"node-a".to_vec(), b"10.0.0.1:9000".to_vec())],
    });
    // The count is the four bytes after the tag; inflate it well past reality.
    bytes[1..5].copy_from_slice(&9999u32.to_le_bytes());
    assert!(matches!(
        decode_message(&bytes),
        Err(ProtocolError::UnexpectedEof)
    ));
}

#[test]
fn the_gossip_tag_is_distinct() {
    // Tag 26 is the gossip frame; no other frame decodes from it, and a gossip
    // frame decodes to nothing else.
    let bytes = encode_message(&Message::Gossip {
        members: vec![(b"node-a".to_vec(), b"10.0.0.1:9000".to_vec())],
    });
    assert_eq!(bytes[0], 26);
}
