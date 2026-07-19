//! The SWIM indirect-probe wire frames — `PingReq` and `PingAck`.
//!
//! When a node's direct gossip probe to a member times out, it asks up to k other
//! members to probe that member on its behalf: a `PingReq { target }` carries the
//! target's advertise address, and the relay answers a `PingAck { reachable }`
//! naming whether its own fresh probe reached the target. The requester disregards
//! its direct-probe failure when any relay reports the target reachable. Both are
//! node-to-node, never client frames. Decoding is total — a truncation, a trailing
//! byte, or a bad tag is a `ProtocolError`, never a panic.

use crdtsync_core::protocol::{decode_message, encode_message, Message, ProtocolError};

#[track_caller]
fn round_trips(m: Message) {
    let bytes = encode_message(&m);
    assert_eq!(decode_message(&bytes), Ok(m));
}

#[test]
fn ping_req_round_trips() {
    round_trips(Message::PingReq {
        target: b"10.0.0.7:9000".to_vec(),
    });
}

#[test]
fn ping_req_round_trips_empty_target() {
    round_trips(Message::PingReq { target: Vec::new() });
}

#[test]
fn ping_ack_round_trips_reachable() {
    round_trips(Message::PingAck { reachable: true });
}

#[test]
fn ping_ack_round_trips_unreachable() {
    round_trips(Message::PingAck { reachable: false });
}

#[test]
fn ping_req_and_ack_have_distinct_tags() {
    let req = encode_message(&Message::PingReq { target: Vec::new() });
    let ack = encode_message(&Message::PingAck { reachable: false });
    assert_ne!(req[0], ack[0]);
}

#[test]
fn ping_ack_rejects_trailing_bytes() {
    let mut bytes = encode_message(&Message::PingAck { reachable: true });
    bytes.push(0xFF);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn ping_req_rejects_truncation() {
    let bytes = encode_message(&Message::PingReq {
        target: b"addr".to_vec(),
    });
    // Drop the last length-framed byte — the target's declared length now overruns.
    let truncated = &bytes[..bytes.len() - 1];
    assert!(decode_message(truncated).is_err());
}

/// Any non-zero byte decodes as `reachable = true`, so a re-serialized ack is
/// still canonical (`1`).
#[test]
fn ping_ack_reachable_byte_is_canonical() {
    let canonical = encode_message(&Message::PingAck { reachable: true });
    assert_eq!(canonical.last(), Some(&1u8));
}
