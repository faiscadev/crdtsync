//! Wire protocol — the diff-query request/response frames.
//!
//! A client asks for the structural diff between two of a room's saved versions
//! or two of its branches; the server replies with the encoded change list. The
//! query is room-keyed like branch management — runnable before any subscription
//! — and the change payload rides the same `encode_changes` codec the diff SDK
//! bindings decode. Rooms, versions, and branch names are opaque byte strings the
//! core does not parse. Decoding stays total.

use crdtsync_core::diff::{encode_changes, Change};
use crdtsync_core::element::ElementKind;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::{decode_message, encode_message, DiffKind, Message, ProtocolError};
use crdtsync_core::Scalar;

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

fn sample_changes() -> Vec<u8> {
    encode_changes(&[
        Change::Value {
            path: encode_path(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(40),
        },
        Change::Added {
            path: encode_path(&[b"meta"]),
            kind: ElementKind::Map,
        },
    ])
}

#[test]
fn diff_query_frames_round_trip() {
    round_trip(Message::DiffQuery {
        room: b"room".to_vec(),
        kind: DiffKind::Versions,
        a: b"v1".to_vec(),
        b: b"v2".to_vec(),
    });
    round_trip(Message::DiffQuery {
        room: b"room".to_vec(),
        kind: DiffKind::Branches,
        a: b"main".to_vec(),
        b: b"draft".to_vec(),
    });
}

#[test]
fn diff_result_round_trips_an_empty_and_a_many_change_diff() {
    // An empty diff is a valid, distinct result — an empty change list, not an
    // error.
    round_trip(Message::DiffResult {
        room: b"room".to_vec(),
        changes: encode_changes(&[]),
    });
    round_trip(Message::DiffResult {
        room: b"room".to_vec(),
        changes: sample_changes(),
    });
}

#[test]
fn empty_names_round_trip() {
    round_trip(Message::DiffQuery {
        room: Vec::new(),
        kind: DiffKind::Versions,
        a: Vec::new(),
        b: Vec::new(),
    });
    round_trip(Message::DiffResult {
        room: Vec::new(),
        changes: Vec::new(),
    });
}

// --- decoding stays total ---

#[test]
fn a_truncated_diff_message_is_an_error_not_a_panic() {
    for m in [
        Message::DiffQuery {
            room: b"room".to_vec(),
            kind: DiffKind::Branches,
            a: b"main".to_vec(),
            b: b"draft".to_vec(),
        },
        Message::DiffResult {
            room: b"room".to_vec(),
            changes: sample_changes(),
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
fn a_bad_diff_kind_tag_is_an_error_not_a_panic() {
    let mut bytes = encode_message(&Message::DiffQuery {
        room: b"r".to_vec(),
        kind: DiffKind::Versions,
        a: b"a".to_vec(),
        b: b"b".to_vec(),
    });
    // The kind tag sits right after the 1-byte message tag and the length-framed
    // room (4-byte length + 1 byte "r").
    bytes[6] = 9;
    assert!(matches!(
        decode_message(&bytes),
        Err(ProtocolError::BadTag {
            what: "diff kind",
            ..
        })
    ));
}

#[test]
fn trailing_bytes_after_a_diff_message_are_rejected() {
    let mut bytes = encode_message(&Message::DiffQuery {
        room: b"room".to_vec(),
        kind: DiffKind::Versions,
        a: b"v1".to_vec(),
        b: b"v2".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn diff_tags_are_distinct_from_each_other_and_the_branch_frames() {
    let tags: Vec<u8> = [
        Message::DiffQuery {
            room: Vec::new(),
            kind: DiffKind::Versions,
            a: Vec::new(),
            b: Vec::new(),
        },
        Message::DiffResult {
            room: Vec::new(),
            changes: Vec::new(),
        },
        // A branch frame from the sibling region, to prove no collision.
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
    assert_eq!(
        sorted.len(),
        tags.len(),
        "diff/branch tags must be distinct"
    );
}
