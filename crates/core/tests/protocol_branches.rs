//! Wire protocol — the branch-management messages.
//!
//! Branch management is a room-keyed request/response sub-protocol: the client
//! asks to list, fork, fork-from-version, restore, publish, or delete a branch of
//! a room; the server replies with the room's current branch set (the
//! authoritative post-state of any mutation, and the answer to a list request).
//! Names, versions, and rooms are opaque byte strings the core does not parse.
//! Decoding stays total.

use crdtsync_core::protocol::{decode_message, encode_message, BranchInfo, Message, ProtocolError};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

#[test]
fn branch_requests_round_trip() {
    round_trip(Message::BranchList {
        room: b"room".to_vec(),
    });
    round_trip(Message::BranchFork {
        room: b"room".to_vec(),
        name: b"feature".to_vec(),
        from_branch: b"main".to_vec(),
    });
    round_trip(Message::BranchForkFromVersion {
        room: b"room".to_vec(),
        name: b"feature".to_vec(),
        version: b"v1".to_vec(),
    });
    round_trip(Message::BranchRestore {
        room: b"room".to_vec(),
        name: b"restored".to_vec(),
        version: b"v1".to_vec(),
    });
    round_trip(Message::BranchPublish {
        room: b"room".to_vec(),
        published: b"live".to_vec(),
    });
    round_trip(Message::BranchDelete {
        room: b"room".to_vec(),
        name: b"feature".to_vec(),
    });
}

#[test]
fn branch_responses_round_trip() {
    round_trip(Message::Branches {
        room: b"room".to_vec(),
        branches: vec![
            BranchInfo {
                name: b"main".to_vec(),
                fork_point: 0,
                head: 12,
                published: false,
            },
            BranchInfo {
                name: b"live".to_vec(),
                fork_point: 7,
                head: 7,
                published: true,
            },
        ],
    });
}

#[test]
fn an_empty_branch_list_round_trips() {
    round_trip(Message::Branches {
        room: Vec::new(),
        branches: Vec::new(),
    });
}

#[test]
fn empty_names_round_trip() {
    round_trip(Message::BranchFork {
        room: Vec::new(),
        name: Vec::new(),
        from_branch: Vec::new(),
    });
    round_trip(Message::Branches {
        room: Vec::new(),
        branches: vec![BranchInfo {
            name: Vec::new(),
            fork_point: 0,
            head: 0,
            published: false,
        }],
    });
}

// --- decoding stays total ---

#[test]
fn a_truncated_branch_message_is_an_error_not_a_panic() {
    for m in [
        Message::BranchFork {
            room: b"room".to_vec(),
            name: b"feature".to_vec(),
            from_branch: b"main".to_vec(),
        },
        Message::BranchRestore {
            room: b"room".to_vec(),
            name: b"restored".to_vec(),
            version: b"v1".to_vec(),
        },
        Message::Branches {
            room: b"room".to_vec(),
            branches: vec![BranchInfo {
                name: b"live".to_vec(),
                fork_point: 3,
                head: 9,
                published: true,
            }],
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
fn trailing_bytes_after_a_branch_message_are_rejected() {
    let mut bytes = encode_message(&Message::BranchList {
        room: b"room".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn a_branch_list_with_a_bogus_count_errors_not_panics() {
    // A count claiming more branches than the bytes hold must fail cleanly, never
    // over-allocate or panic.
    let mut bytes = encode_message(&Message::Branches {
        room: b"r".to_vec(),
        branches: vec![BranchInfo {
            name: b"main".to_vec(),
            fork_point: 0,
            head: 1,
            published: false,
        }],
    });
    // The count u32 sits right after the 1-byte tag and the length-framed room
    // (4-byte length + 1 byte "r").
    bytes[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_message(&bytes), Err(ProtocolError::UnexpectedEof));
}

#[test]
fn branch_tags_are_distinct() {
    let tags: Vec<u8> = [
        Message::BranchList { room: Vec::new() },
        Message::Branches {
            room: Vec::new(),
            branches: Vec::new(),
        },
        Message::BranchFork {
            room: Vec::new(),
            name: Vec::new(),
            from_branch: Vec::new(),
        },
        Message::BranchForkFromVersion {
            room: Vec::new(),
            name: Vec::new(),
            version: Vec::new(),
        },
        Message::BranchRestore {
            room: Vec::new(),
            name: Vec::new(),
            version: Vec::new(),
        },
        Message::BranchPublish {
            room: Vec::new(),
            published: Vec::new(),
        },
        Message::BranchDelete {
            room: Vec::new(),
            name: Vec::new(),
        },
    ]
    .iter()
    .map(|m| encode_message(m)[0])
    .collect();
    let mut sorted = tags.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), tags.len(), "branch tags must be distinct");
}
