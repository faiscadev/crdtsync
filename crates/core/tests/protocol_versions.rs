//! Wire protocol — the named-version messages.
//!
//! Versioning is a request/response sub-protocol over a subscribed channel: the
//! client asks to create, rename, delete, list, or fetch a version of the
//! channel's room; the server replies with the current name list (the
//! authoritative post-state of any mutation, and the answer to a list request)
//! or, for a fetch that hits, the version's captured state. Names and state are
//! opaque byte strings the core does not parse. Decoding stays total.

use crdtsync_core::protocol::{decode_message, encode_message, Channel, Message, ProtocolError};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

#[test]
fn version_requests_round_trip() {
    round_trip(Message::VersionCreate {
        channel: Channel(1),
        name: b"v1".to_vec(),
    });
    round_trip(Message::VersionRename {
        channel: Channel(2),
        from: b"draft".to_vec(),
        to: b"final".to_vec(),
    });
    round_trip(Message::VersionDelete {
        channel: Channel(3),
        name: b"old".to_vec(),
    });
    round_trip(Message::VersionList {
        channel: Channel(4),
    });
    round_trip(Message::VersionFetch {
        channel: Channel(5),
        name: b"v1".to_vec(),
    });
}

#[test]
fn version_responses_round_trip() {
    round_trip(Message::Versions {
        channel: Channel(1),
        names: vec![b"v1".to_vec(), b"v2".to_vec(), b"final".to_vec()],
    });
    round_trip(Message::VersionState {
        channel: Channel(2),
        name: b"v1".to_vec(),
        seq: 42,
        state: vec![0x00, 0x10, 0xFF],
    });
}

#[test]
fn an_empty_version_list_round_trips() {
    round_trip(Message::Versions {
        channel: Channel(0),
        names: Vec::new(),
    });
}

#[test]
fn empty_names_and_state_round_trip() {
    round_trip(Message::VersionCreate {
        channel: Channel(0),
        name: Vec::new(),
    });
    round_trip(Message::VersionState {
        channel: Channel(0),
        name: Vec::new(),
        seq: 0,
        state: Vec::new(),
    });
    round_trip(Message::Versions {
        channel: Channel(0),
        names: vec![Vec::new(), Vec::new()],
    });
}

// --- decoding stays total ---

#[test]
fn a_truncated_version_message_is_an_error_not_a_panic() {
    for m in [
        Message::VersionRename {
            channel: Channel(2),
            from: b"draft".to_vec(),
            to: b"final".to_vec(),
        },
        Message::Versions {
            channel: Channel(2),
            names: vec![b"v1".to_vec(), b"v2".to_vec()],
        },
        Message::VersionState {
            channel: Channel(2),
            name: b"v1".to_vec(),
            seq: 7,
            state: b"xy".to_vec(),
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
fn trailing_bytes_after_a_version_message_are_rejected() {
    let mut bytes = encode_message(&Message::VersionCreate {
        channel: Channel(0),
        name: b"v1".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn a_version_list_with_a_bogus_count_errors_not_panics() {
    // A count claiming more names than the bytes hold must fail cleanly, never
    // over-allocate or panic.
    let mut bytes = encode_message(&Message::Versions {
        channel: Channel(1),
        names: vec![b"v1".to_vec()],
    });
    // The count u32 sits right after the 1-byte tag and the 4-byte channel.
    bytes[5..9].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_message(&bytes), Err(ProtocolError::UnexpectedEof));
}
