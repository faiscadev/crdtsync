//! Wire protocol — the clone-room messages.
//!
//! Cloning duplicates a room's live state into a fresh room id. The client sends
//! a room-keyed [`Message::CloneRoom`] naming source and destination; the server
//! replies with a [`Message::CloneRoomResult`] carrying the destination and
//! whether it was created (a no-op when the source is unknown or the destination
//! already exists). Rooms are opaque byte strings the core does not parse.
//! Decoding stays total.

use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

#[test]
fn clone_messages_round_trip() {
    round_trip(Message::CloneRoom {
        src: b"template".to_vec(),
        dst: b"copy".to_vec(),
    });
    round_trip(Message::CloneRoomResult {
        dst: b"copy".to_vec(),
        created: true,
    });
    round_trip(Message::CloneRoomResult {
        dst: b"copy".to_vec(),
        created: false,
    });
}

#[test]
fn empty_room_ids_round_trip() {
    round_trip(Message::CloneRoom {
        src: Vec::new(),
        dst: Vec::new(),
    });
    round_trip(Message::CloneRoomResult {
        dst: Vec::new(),
        created: false,
    });
}

// --- decoding stays total ---

#[test]
fn a_truncated_clone_message_is_an_error_not_a_panic() {
    for m in [
        Message::CloneRoom {
            src: b"template".to_vec(),
            dst: b"copy".to_vec(),
        },
        Message::CloneRoomResult {
            dst: b"copy".to_vec(),
            created: true,
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
fn trailing_bytes_after_a_clone_message_are_rejected() {
    let mut bytes = encode_message(&Message::CloneRoom {
        src: b"template".to_vec(),
        dst: b"copy".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn clone_tags_are_distinct_from_each_other_and_the_branch_block() {
    let tags: Vec<u8> = [
        Message::CloneRoom {
            src: Vec::new(),
            dst: Vec::new(),
        },
        Message::CloneRoomResult {
            dst: Vec::new(),
            created: false,
        },
        Message::BranchList { room: Vec::new() },
        Message::BranchFork {
            room: Vec::new(),
            name: Vec::new(),
            from_branch: Vec::new(),
        },
        Message::DiffResult {
            room: Vec::new(),
            changes: Vec::new(),
        },
    ]
    .iter()
    .map(|m| encode_message(m)[0])
    .collect();
    let mut sorted = tags.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), tags.len(), "clone tags must be distinct");
}
