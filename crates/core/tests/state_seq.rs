//! State serialization for the sequence CRDTs — List and Text.
//!
//! A snapshot must preserve the whole Fugue structure, tombstones included: a
//! deleted node stays as an anchor so a later concurrent insert still places
//! against it. So `decode_state(encode_state(x))` reads back the same live
//! sequence, re-encodes to identical bytes, and still merges to convergence
//! with a concurrent replica. Text is the same sequence over codepoints.

use crdtsync_core::{DecodeError, Element, List, Scalar, Text};

mod common;
use common::{eid, stmp};

fn int(n: i64) -> Element {
    Element::Scalar(Scalar::Int(n))
}

/// The live values of a list as integers.
fn ints(l: &List) -> Vec<i64> {
    l.values()
        .iter()
        .map(|e| match e {
            Element::Scalar(Scalar::Int(n)) => *n,
            _ => panic!("expected an Int scalar"),
        })
        .collect()
}

// --- List ---

#[test]
fn a_list_round_trips_its_live_values() {
    let mut l = List::new(eid(1, 1));
    l.insert(0, int(10), stmp(1, 1));
    l.insert(1, int(20), stmp(2, 1));
    l.insert(2, int(30), stmp(3, 1));

    let bytes = l.encode_state();
    let back = List::decode_state(&bytes).unwrap();
    assert_eq!(ints(&back), ints(&l));
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn a_list_snapshot_keeps_tombstones() {
    // A deleted node must survive the round-trip as a tombstone — the encoding
    // carries it, and re-encoding is stable.
    let mut l = List::new(eid(1, 1));
    l.insert(0, int(1), stmp(1, 1));
    l.insert(1, int(2), stmp(2, 1));
    l.insert(2, int(3), stmp(3, 1));
    l.delete(1); // tombstone the middle node

    let bytes = l.encode_state();
    let back = List::decode_state(&bytes).unwrap();
    assert_eq!(ints(&back), vec![1, 3]);
    assert_eq!(back.encode_state(), bytes);
}

#[test]
fn a_decoded_list_still_converges_with_a_concurrent_replica() {
    // Two replicas share history, then edit concurrently; reloading one from a
    // snapshot before merging must not change where the merges land.
    let mut a = List::new(eid(1, 1));
    a.insert(0, int(1), stmp(1, 1));
    a.insert(1, int(2), stmp(2, 1));
    let mut b = a.deep_clone();

    // Concurrent inserts at the tail from two clients.
    a.insert(2, int(3), stmp(3, 1));
    b.insert(2, int(4), stmp(3, 2));

    // Reload `a` from a snapshot, then merge both ways.
    let mut a = List::decode_state(&a.encode_state()).unwrap();
    a.merge(&b);
    b.merge(&a);
    assert_eq!(ints(&a), ints(&b), "replicas must converge after a reload");
}

#[test]
fn a_truncated_list_is_an_error() {
    let mut l = List::new(eid(1, 1));
    l.insert(0, int(1), stmp(1, 1));
    let bytes = l.encode_state();
    assert!(List::decode_state(&bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn an_empty_list_round_trips() {
    let l = List::new(eid(9, 9));
    let back = List::decode_state(&l.encode_state()).unwrap();
    assert!(back.is_empty());
    assert_eq!(back.id(), l.id());
}

// --- Text ---

#[test]
fn text_round_trips_its_string() {
    let mut t = Text::new(eid(2, 2));
    t.insert(0, "héllo", stmp(1, 1));
    let bytes = t.encode_state();
    let back = Text::decode_state(&bytes).unwrap();
    assert_eq!(back.as_string(), "héllo");
    assert_eq!(back.encode_state(), bytes);
}

#[test]
fn text_snapshot_keeps_deletes() {
    let mut t = Text::new(eid(2, 2));
    t.insert(0, "héllo", stmp(1, 1));
    t.delete(1, 3); // remove "éll" -> "ho"
    let back = Text::decode_state(&t.encode_state()).unwrap();
    assert_eq!(back.as_string(), "ho");
}

#[test]
fn a_decoded_text_still_converges_with_a_concurrent_replica() {
    let mut a = Text::new(eid(2, 2));
    a.insert(0, "ab", stmp(1, 1));
    let mut b = a.deep_clone();
    a.insert(2, "x", stmp(2, 1));
    b.insert(2, "y", stmp(2, 2));

    let mut a = Text::decode_state(&a.encode_state()).unwrap();
    a.merge(&b);
    b.merge(&a);
    assert_eq!(
        a.as_string(),
        b.as_string(),
        "text must converge after reload"
    );
}

#[test]
fn a_text_snapshot_with_an_invalid_codepoint_is_rejected() {
    // A List can hold any scalar, but Text nodes must be valid codepoints.
    // Decoding a list whose node is out of Unicode range as Text must error,
    // not decode-then-panic on read. 0x11FFFF exceeds the max scalar value.
    let mut l = List::new(eid(2, 2));
    l.insert(0, int(0x11_FFFF), stmp(1, 1));
    assert!(matches!(
        Text::decode_state(&l.encode_state()),
        Err(DecodeError::BadTag { .. })
    ));

    // A surrogate is a valid u32 but not a scalar value — also rejected.
    let mut s = List::new(eid(2, 2));
    s.insert(0, int(0xD800), stmp(1, 1));
    assert!(Text::decode_state(&s.encode_state()).is_err());
}

#[test]
fn a_truncated_text_is_an_error() {
    let mut t = Text::new(eid(2, 2));
    t.insert(0, "z", stmp(1, 1));
    let bytes = t.encode_state();
    assert!(matches!(
        Text::decode_state(&bytes[..bytes.len() - 1]),
        Err(DecodeError::UnexpectedEof)
    ));
}
