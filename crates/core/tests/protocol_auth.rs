//! Wire protocol — the authentication phase.
//!
//! Between Hello and Subscribe the client presents an opaque credential in an
//! [`Message::Auth`]; the server hands it to a deployment-configured verifier
//! and, on success, replies [`Message::AuthOk`] carrying the server-derived
//! `actor` id (the client never asserts it). A rejected credential is an
//! `AuthFailed` error. The credential and actor are opaque byte strings — the
//! core neither parses nor interprets them. Decoding stays total: malformed
//! bytes yield a [`ProtocolError`], never a panic.

use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};

fn round_trip(m: Message) {
    assert_eq!(decode_message(&encode_message(&m)).expect("decodes"), m);
}

#[test]
fn auth_carries_an_opaque_credential() {
    round_trip(Message::Auth {
        credential: vec![0x00, 0x01, 0xFF, 0x7F, 0x00],
    });
}

#[test]
fn an_empty_credential_round_trips() {
    round_trip(Message::Auth {
        credential: Vec::new(),
    });
}

#[test]
fn auth_ok_carries_the_server_derived_actor() {
    round_trip(Message::AuthOk {
        actor: b"anon:7f3a".to_vec(),
    });
}

#[test]
fn an_empty_actor_round_trips() {
    round_trip(Message::AuthOk { actor: Vec::new() });
}

#[test]
fn distinct_credentials_encode_differently() {
    let a = encode_message(&Message::Auth {
        credential: b"token-a".to_vec(),
    });
    let b = encode_message(&Message::Auth {
        credential: b"token-b".to_vec(),
    });
    assert_ne!(a, b);
}

// --- decoding stays total ---

#[test]
fn a_truncated_auth_message_is_an_error_not_a_panic() {
    for m in [
        Message::Auth {
            credential: b"credential-bytes".to_vec(),
        },
        Message::AuthOk {
            actor: b"actor-id".to_vec(),
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
fn trailing_bytes_after_an_auth_message_are_rejected() {
    let mut bytes = encode_message(&Message::Auth {
        credential: b"x".to_vec(),
    });
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}
