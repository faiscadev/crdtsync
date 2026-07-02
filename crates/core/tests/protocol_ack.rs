//! Wire protocol — the op-acknowledgement messages.
//!
//! Two frames carry a per-channel commit frontier, one per direction. The
//! server replies [`Message::Accepted`] to an author after durably logging its
//! batch, naming the highest per-client op sequence (`OpId.seq`) it has
//! committed — the client drains its outbox up to it. A client sends
//! [`Message::Ack`] to report the server sequence it has applied — the server
//! feeds it into the tombstone-GC watermark. Both are opaque `u64` frontiers the
//! codec round-trips exactly; decoding stays total.

use crdtsync_core::protocol::{decode_message, encode_message, Channel, Message, ProtocolError};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

#[test]
fn accepted_round_trips() {
    round_trip(Message::Accepted {
        channel: Channel(2),
        through: 42,
    });
}

#[test]
fn ack_round_trips() {
    round_trip(Message::Ack {
        channel: Channel(4),
        seq: 1_000_000,
    });
}

#[test]
fn zero_frontier_round_trips() {
    round_trip(Message::Accepted {
        channel: Channel(0),
        through: 0,
    });
    round_trip(Message::Ack {
        channel: Channel(0),
        seq: 0,
    });
}

#[test]
fn max_frontier_round_trips() {
    round_trip(Message::Accepted {
        channel: Channel(7),
        through: u64::MAX,
    });
    round_trip(Message::Ack {
        channel: Channel(7),
        seq: u64::MAX,
    });
}

#[test]
fn accepted_and_ack_encode_distinctly() {
    // Same channel + frontier, opposite direction: the tag must separate them,
    // so a client never mistakes its own Ack shape for a server Accepted.
    let accepted = encode_message(&Message::Accepted {
        channel: Channel(1),
        through: 5,
    });
    let ack = encode_message(&Message::Ack {
        channel: Channel(1),
        seq: 5,
    });
    assert_ne!(accepted, ack);
}

// --- decoding stays total ---

#[test]
fn a_truncated_ack_message_is_an_error_not_a_panic() {
    for m in [
        Message::Accepted {
            channel: Channel(2),
            through: 9,
        },
        Message::Ack {
            channel: Channel(2),
            seq: 9,
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
fn trailing_bytes_after_ack_are_rejected() {
    for m in [
        Message::Accepted {
            channel: Channel(0),
            through: 1,
        },
        Message::Ack {
            channel: Channel(0),
            seq: 1,
        },
    ] {
        let mut bytes = encode_message(&m);
        bytes.push(0);
        assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
    }
}
