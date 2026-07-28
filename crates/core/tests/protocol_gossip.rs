//! The `Gossip` wire frame — a node advertising the cluster members it knows,
//! each with its liveness, for anti-entropy failure detection.
//!
//! `Gossip { members }` carries a length-prefixed list of `(node_id,
//! advertise_addr, incarnation, state, verified)` tuples: the id a peer places
//! with, the address it dials, a monotonic per-node refutation counter, the
//! member's SWIM state (`Alive` / `Suspect` / `Dead`), and whether the sender has
//! itself completed an identity-checked peer link to that member. A receiver
//! merges each tuple into its own liveness view — a higher incarnation wins, and at
//! equal incarnation a more-suspicious state wins — so both the member set and its
//! liveness converge cluster-wide; the verification flag is recorded against the
//! sender, and enough of them place the member in the ring. Node-to-node, never a
//! client frame. Decoding is total — a truncation, a trailing byte, or a bad state
//! tag is a `ProtocolError`, never a panic.

use crdtsync_core::protocol::{
    decode_message, encode_message, MemberState, Message, ProtocolError,
};

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

fn member(
    node: &[u8],
    addr: &[u8],
    incarnation: u64,
    state: MemberState,
) -> (Vec<u8>, Vec<u8>, u64, MemberState, bool) {
    (node.to_vec(), addr.to_vec(), incarnation, state, false)
}

/// The same advertisement, carrying the sender's claim to have verified the member.
fn verified_member(
    node: &[u8],
    addr: &[u8],
    incarnation: u64,
    state: MemberState,
) -> (Vec<u8>, Vec<u8>, u64, MemberState, bool) {
    (node.to_vec(), addr.to_vec(), incarnation, state, true)
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
        members: vec![member(b"node-a", b"10.0.0.1:9000", 0, MemberState::Alive)],
    });
}

#[test]
fn gossip_round_trips_every_state() {
    // Each liveness state survives the round trip, at a non-trivial incarnation.
    round_trips(Message::Gossip {
        members: vec![
            member(b"node-a", b"10.0.0.1:9000", 0, MemberState::Alive),
            member(b"node-b", b"10.0.0.2:9000", 7, MemberState::Suspect),
            member(b"node-c", b"10.0.0.3:9000", 42, MemberState::Dead),
        ],
    });
}

#[test]
fn gossip_round_trips_many_members() {
    round_trips(Message::Gossip {
        members: vec![
            member(b"node-a", b"10.0.0.1:9000", 1, MemberState::Alive),
            member(b"node-b", b"10.0.0.2:9000", 2, MemberState::Suspect),
            member(b"node-c", b"10.0.0.3:9000", 3, MemberState::Dead),
            member(b"node-d", b"10.0.0.4:9000", u64::MAX, MemberState::Alive),
        ],
    });
}

#[test]
fn gossip_round_trips_binary_and_empty_fields() {
    // A node id or address is opaque bytes: empty, and arbitrary non-UTF-8, both
    // survive the round trip alongside the liveness fields.
    round_trips(Message::Gossip {
        members: vec![
            member(&[], &[], 0, MemberState::Alive),
            (
                vec![0, 1, 2, 255],
                vec![0xFF, 0x00, 0x80, 0x7F],
                999,
                MemberState::Dead,
                false,
            ),
        ],
    });
}

#[test]
fn a_truncated_gossip_is_an_error_not_a_panic() {
    // Every prefix of a well-formed multi-member frame must error, never panic:
    // truncating the count, a pair's lengths or bytes, the incarnation, or the
    // state tag each lands inside a field the cursor is mid-read on.
    let bytes = encode_message(&Message::Gossip {
        members: vec![
            member(b"node-a", b"10.0.0.1:9000", 3, MemberState::Suspect),
            member(b"node-b", b"10.0.0.2:9000", 4, MemberState::Dead),
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
        members: vec![member(b"node-a", b"10.0.0.1:9000", 0, MemberState::Alive)],
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn a_gossip_count_past_the_payload_is_an_error() {
    // A count larger than the tuples that follow must fail on the missing bytes,
    // not over-reserve or panic.
    let mut bytes = encode_message(&Message::Gossip {
        members: vec![member(b"node-a", b"10.0.0.1:9000", 0, MemberState::Alive)],
    });
    // The count is the four bytes after the tag; inflate it well past reality.
    bytes[1..5].copy_from_slice(&9999u32.to_le_bytes());
    assert!(matches!(
        decode_message(&bytes),
        Err(ProtocolError::UnexpectedEof)
    ));
}

#[test]
fn a_bad_state_tag_is_an_error_not_a_panic() {
    // The state is a single tag byte; an unknown value is a decode error, never a
    // silent misread or a panic. Build a one-member frame and clobber its state byte
    // — the last but one, the verification flag trailing it — with an out-of-range
    // tag.
    let mut bytes = encode_message(&Message::Gossip {
        members: vec![member(b"node-a", b"10.0.0.1:9000", 0, MemberState::Alive)],
    });
    let state_byte = bytes.len() - 2;
    bytes[state_byte] = 200;
    assert!(matches!(
        decode_message(&bytes),
        Err(ProtocolError::BadTag { .. })
    ));
}

#[test]
fn each_state_tag_is_distinct() {
    // The three states encode to three distinct tag bytes.
    let tag_of = |state| {
        let bytes = encode_message(&Message::Gossip {
            members: vec![member(b"n", b"a", 0, state)],
        });
        bytes[bytes.len() - 2]
    };
    let alive = tag_of(MemberState::Alive);
    let suspect = tag_of(MemberState::Suspect);
    let dead = tag_of(MemberState::Dead);
    assert_ne!(alive, suspect);
    assert_ne!(suspect, dead);
    assert_ne!(alive, dead);
}

#[test]
fn gossip_round_trips_a_verification_claim() {
    // The sender's claim to have verified a member rides per tuple and survives the
    // round trip independently of the liveness beside it — a frame mixing verified
    // and unverified members comes back as it went.
    round_trips(Message::Gossip {
        members: vec![
            verified_member(b"node-a", b"10.0.0.1:9000", 4, MemberState::Alive),
            member(b"node-b", b"10.0.0.2:9000", 4, MemberState::Suspect),
        ],
    });
}

#[test]
fn a_verification_flag_is_a_distinct_trailing_byte() {
    // Verified and unverified encode differently, in the byte after the state tag —
    // so a decoder cannot read the claim off the state, nor drop it silently.
    let flag_of = |m| {
        let bytes = encode_message(&Message::Gossip { members: vec![m] });
        *bytes.last().unwrap()
    };
    assert_ne!(
        flag_of(member(b"n", b"a", 0, MemberState::Alive)),
        flag_of(verified_member(b"n", b"a", 0, MemberState::Alive)),
    );
}

#[test]
fn a_truncated_verification_flag_is_an_error_not_a_panic() {
    // The flag is the last byte of a tuple, so a frame cut just before it is a
    // truncation inside a field — an error, never a silently unverified member.
    let bytes = encode_message(&Message::Gossip {
        members: vec![verified_member(
            b"node-a",
            b"10.0.0.1:9000",
            0,
            MemberState::Alive,
        )],
    });
    assert!(decode_message(&bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn the_gossip_tag_is_distinct() {
    // Tag 26 is the gossip frame; the frame's first byte is that tag.
    let bytes = encode_message(&Message::Gossip {
        members: vec![member(b"node-a", b"10.0.0.1:9000", 0, MemberState::Alive)],
    });
    assert_eq!(bytes[0], 26);
}

#[test]
fn member_state_orders_by_suspicion() {
    // Dead is the most-suspicious, Alive the least — the ordering the anti-entropy
    // merge uses to let a failure win at equal incarnation.
    assert!(MemberState::Dead > MemberState::Suspect);
    assert!(MemberState::Suspect > MemberState::Alive);
}
