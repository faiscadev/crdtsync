//! State serialization — a value CRDT's full state to bytes and back.
//!
//! A snapshot persists a replica's merged state so a fresh replica can resume
//! from it instead of replaying the whole op log. Each value type serializes
//! its own state losslessly and canonically: `decode_state(encode_state(x))`
//! reads back the same observable value and re-encodes to the identical bytes,
//! so equal states have equal encodings regardless of internal map order. The
//! leaf values — Scalar, Register, Counter — are the foundation the composite
//! and Document codecs build on.

use crdtsync_core::counter::Counter;
use crdtsync_core::register::Register;
use crdtsync_core::{DecodeError, Scalar};

mod common;
use common::{cid, eid, stmp};

// --- Scalar ---

fn scalar_round_trips(s: Scalar) {
    let bytes = s.encode_state();
    let back = Scalar::decode_state(&bytes).unwrap();
    assert_eq!(back, s, "decoded scalar differs");
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn scalar_variants_round_trip() {
    scalar_round_trips(Scalar::Bool(true));
    scalar_round_trips(Scalar::Bool(false));
    scalar_round_trips(Scalar::Int(0));
    scalar_round_trips(Scalar::Int(-1));
    scalar_round_trips(Scalar::Int(i64::MIN));
    scalar_round_trips(Scalar::Int(i64::MAX));
    scalar_round_trips(Scalar::Bytes(vec![]));
    scalar_round_trips(Scalar::Bytes(vec![0, 1, 255, 0, 128]));
}

#[test]
fn a_truncated_scalar_is_an_error() {
    let bytes = Scalar::Int(42).encode_state();
    assert!(matches!(
        Scalar::decode_state(&bytes[..bytes.len() - 1]),
        Err(DecodeError::UnexpectedEof)
    ));
}

#[test]
fn an_unknown_scalar_tag_is_an_error() {
    assert!(matches!(
        Scalar::decode_state(&[0xff]),
        Err(DecodeError::BadTag { .. })
    ));
}

// --- Register ---

#[test]
fn register_round_trips() {
    let r = Register::new(eid(1, 2), Scalar::Int(30), stmp(5, 1));
    let bytes = r.encode_state();
    let back = Register::decode_state(&bytes).unwrap();
    assert_eq!(back.id(), r.id());
    assert_eq!(back.read(), r.read());
    // The stamp is carried too: re-encoding is byte-for-byte identical.
    assert_eq!(back.encode_state(), bytes);
}

#[test]
fn a_decoded_register_keeps_its_stamp_for_lww() {
    // A register decoded from a snapshot must still lose to a strictly-later
    // write and win over an earlier one — the stamp survived.
    let r = Register::new(eid(1, 2), Scalar::Int(1), stmp(5, 1));
    let mut back = Register::decode_state(&r.encode_state()).unwrap();

    let mut earlier = back.deep_clone();
    earlier.set(Scalar::Int(9), stmp(4, 1));
    assert_eq!(
        earlier.read(),
        &Scalar::Int(1),
        "an earlier write must lose"
    );

    back.set(Scalar::Int(7), stmp(6, 1));
    assert_eq!(back.read(), &Scalar::Int(7), "a later write must win");
}

#[test]
fn a_truncated_register_is_an_error() {
    let bytes = Register::new(eid(1, 2), Scalar::Int(1), stmp(5, 1)).encode_state();
    assert!(Register::decode_state(&bytes[..bytes.len() - 1]).is_err());
}

// --- Counter ---

#[test]
fn counter_round_trips_per_client_tallies() {
    let mut c = Counter::new(eid(7, 7));
    c.inc(cid(1), 5);
    c.dec(cid(2), 3);
    c.inc(cid(1), 2);
    c.inc(cid(3), 10);

    let bytes = c.encode_state();
    let back = Counter::decode_state(&bytes).unwrap();
    assert_eq!(back.id(), c.id());
    assert_eq!(back.read(), c.read());
    // Canonical: the per-client tallies came back in full, not just the sum.
    assert_eq!(back.encode_state(), bytes);
}

#[test]
fn a_decoded_counter_merges_idempotently_with_its_source() {
    // If the tallies survived intact, merging the decode back over the original
    // (per-client max in each direction) changes nothing.
    let mut c = Counter::new(eid(7, 7));
    c.inc(cid(1), 5);
    c.dec(cid(2), 3);
    let mut back = Counter::decode_state(&c.encode_state()).unwrap();
    back.merge(&c);
    assert_eq!(back.read(), c.read());
    c.merge(&back);
    assert_eq!(c.read(), back.read());
}

#[test]
fn an_empty_counter_round_trips() {
    let c = Counter::new(eid(7, 7));
    let back = Counter::decode_state(&c.encode_state()).unwrap();
    assert_eq!(back.read(), 0);
    assert_eq!(back.id(), c.id());
}

#[test]
fn a_truncated_counter_is_an_error() {
    let mut c = Counter::new(eid(7, 7));
    c.inc(cid(1), 5);
    let bytes = c.encode_state();
    assert!(Counter::decode_state(&bytes[..bytes.len() - 1]).is_err());
}
