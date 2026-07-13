//! The `Redirect` wire frame — a node telling a client a room's leader is
//! elsewhere.
//!
//! A node that does not lead a room answers a subscribe (or a stray write) with
//! `Redirect { room, leader_addr }` instead of serving it: the client reconnects
//! to `leader_addr`. The codec length-frames both the room and the address, so
//! either may be empty; decoding is total — a truncation or trailing byte is a
//! `ProtocolError`, never a panic.

use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

#[test]
fn redirect_round_trips() {
    round_trips(Message::Redirect {
        room: b"room-42".to_vec(),
        leader_addr: b"10.0.0.7:9000".to_vec(),
    });
}

#[test]
fn redirect_round_trips_an_empty_room() {
    round_trips(Message::Redirect {
        room: Vec::new(),
        leader_addr: b"10.0.0.7:9000".to_vec(),
    });
}

#[test]
fn redirect_round_trips_an_empty_leader_addr() {
    round_trips(Message::Redirect {
        room: b"room-42".to_vec(),
        leader_addr: Vec::new(),
    });
}

#[test]
fn redirect_round_trips_both_empty() {
    round_trips(Message::Redirect {
        room: Vec::new(),
        leader_addr: Vec::new(),
    });
}

#[test]
fn redirect_round_trips_binary_addr() {
    // The address is an opaque node id — bytes, not necessarily UTF-8.
    round_trips(Message::Redirect {
        room: vec![0, 1, 2, 255],
        leader_addr: vec![0xFF, 0x00, 0x80, 0x7F],
    });
}

#[test]
fn a_truncated_redirect_is_an_error_not_a_panic() {
    let bytes = encode_message(&Message::Redirect {
        room: b"room".to_vec(),
        leader_addr: b"addr".to_vec(),
    });
    for cut in 0..bytes.len() {
        assert!(
            decode_message(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error, not panic",
        );
    }
}

#[test]
fn trailing_bytes_after_a_redirect_are_an_error() {
    let mut bytes = encode_message(&Message::Redirect {
        room: b"room".to_vec(),
        leader_addr: b"addr".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}
