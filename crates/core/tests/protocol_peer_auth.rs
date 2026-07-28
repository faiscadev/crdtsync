//! The peer-plane admission frame — `PeerAuth`.
//!
//! A node opening a link to another member presents the deployment's cluster
//! secret once, after the link's `Hello`, alongside its own node id; the accepting
//! node honors the node-to-node frames only on a connection that presented the
//! secret, and decides each of them against the member the link named. Node-to-node,
//! never a client frame. Decoding is total — a truncation, a trailing byte, or a bad
//! tag is a `ProtocolError`, never a panic.

use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

#[test]
fn peer_auth_round_trips() {
    round_trips(Message::PeerAuth {
        node: b"wss://node-a.internal:9000".to_vec(),
        secret: b"a-cluster-secret-of-thirty-two-b".to_vec(),
    });
}

/// Both fields are opaque bytes, not text — neither a non-UTF-8 secret nor a
/// non-UTF-8 node id is altered on the wire. What either one is *worth* is the
/// acceptor's decision, and it must see them as sent to make it.
#[test]
fn peer_auth_round_trips_arbitrary_bytes() {
    round_trips(Message::PeerAuth {
        node: (0u8..=255).rev().collect(),
        secret: (0u8..=255).collect(),
    });
}

#[test]
fn peer_auth_round_trips_empty_fields() {
    round_trips(Message::PeerAuth {
        node: Vec::new(),
        secret: Vec::new(),
    });
}

/// The two fields are distinct on the wire: a frame naming a node and carrying a
/// secret never decodes as the transposition of them.
#[test]
fn peer_auth_keeps_its_node_and_secret_apart() {
    let bytes = encode_message(&Message::PeerAuth {
        node: b"10.0.0.1:9000".to_vec(),
        secret: b"a-cluster-secret-of-thirty-two-b".to_vec(),
    });
    assert_eq!(
        decode_message(&bytes),
        Ok(Message::PeerAuth {
            node: b"10.0.0.1:9000".to_vec(),
            secret: b"a-cluster-secret-of-thirty-two-b".to_vec(),
        }),
    );
}

/// The tag is part of the wire contract: pin the byte, so reassigning it to another
/// frame is a failing test rather than a silently incompatible build.
#[test]
fn peer_auth_carries_wire_tag_52() {
    let bytes = encode_message(&Message::PeerAuth {
        node: Vec::new(),
        secret: Vec::new(),
    });
    assert_eq!(bytes[0], 52);
}

#[test]
fn peer_auth_has_a_tag_of_its_own() {
    let peer_auth = encode_message(&Message::PeerAuth {
        node: Vec::new(),
        secret: Vec::new(),
    });
    for other in [
        Message::PingReq { target: Vec::new() },
        Message::PingAck { reachable: false },
        Message::Gossip {
            members: Vec::new(),
        },
        Message::FollowerHeads {
            reporter: Vec::new(),
            heads: Vec::new(),
        },
    ] {
        assert_ne!(peer_auth[0], encode_message(&other)[0]);
    }
}

#[test]
fn peer_auth_rejects_trailing_bytes() {
    let mut bytes = encode_message(&Message::PeerAuth {
        node: b"10.0.0.1:9000".to_vec(),
        secret: b"secret".to_vec(),
    });
    bytes.push(0xFF);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn peer_auth_rejects_truncation() {
    let bytes = encode_message(&Message::PeerAuth {
        node: b"10.0.0.1:9000".to_vec(),
        secret: b"secret".to_vec(),
    });
    // Drop the last length-framed byte — the secret's declared length now overruns.
    let truncated = &bytes[..bytes.len() - 1];
    assert!(decode_message(truncated).is_err());
}
