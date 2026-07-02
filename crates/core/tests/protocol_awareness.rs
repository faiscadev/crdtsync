//! Wire protocol — the awareness (ephemeral presence) messages.
//!
//! Awareness is transient per-client state — cursors, selections, typing — that
//! rides the connection but never enters the op log or a snapshot. A client
//! publishes an entry with [`Message::AwarenessSet`] on a subscribed channel;
//! the server fans it out to the room's other subscribers as a
//! [`Message::AwarenessUpdate`] tagged with the publisher's actor, so receivers
//! know which human it belongs to. Entries are last-writer-wins per client, not
//! CRDT-merged. Keys, values, and actors are opaque byte strings the core does
//! not parse. Decoding stays total.

use crdtsync_core::protocol::{decode_message, encode_message, Channel, Message, ProtocolError};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

#[test]
fn awareness_set_round_trips() {
    round_trip(Message::AwarenessSet {
        channel: Channel(2),
        key: b"cursor".to_vec(),
        value: vec![0x00, 0x10, 0xFF],
    });
}

#[test]
fn awareness_update_round_trips() {
    round_trip(Message::AwarenessUpdate {
        channel: Channel(4),
        actor: b"alice".to_vec(),
        key: b"cursor".to_vec(),
        value: vec![1, 2, 3, 0, 255],
    });
}

#[test]
fn empty_key_and_value_round_trip() {
    round_trip(Message::AwarenessSet {
        channel: Channel(0),
        key: Vec::new(),
        value: Vec::new(),
    });
    round_trip(Message::AwarenessUpdate {
        channel: Channel(0),
        actor: Vec::new(),
        key: Vec::new(),
        value: Vec::new(),
    });
}

#[test]
fn distinct_entries_encode_differently() {
    let a = encode_message(&Message::AwarenessSet {
        channel: Channel(1),
        key: b"cursor".to_vec(),
        value: b"a".to_vec(),
    });
    let b = encode_message(&Message::AwarenessSet {
        channel: Channel(1),
        key: b"cursor".to_vec(),
        value: b"b".to_vec(),
    });
    assert_ne!(a, b);
}

// --- decoding stays total ---

#[test]
fn a_truncated_awareness_message_is_an_error_not_a_panic() {
    for m in [
        Message::AwarenessSet {
            channel: Channel(2),
            key: b"cursor".to_vec(),
            value: b"xy".to_vec(),
        },
        Message::AwarenessUpdate {
            channel: Channel(2),
            actor: b"alice".to_vec(),
            key: b"cursor".to_vec(),
            value: b"xy".to_vec(),
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
fn trailing_bytes_after_awareness_are_rejected() {
    let mut bytes = encode_message(&Message::AwarenessSet {
        channel: Channel(0),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}
