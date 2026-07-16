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

// --- tombstone compression ---

#[test]
fn a_deleted_run_compresses_far_below_its_length() {
    // A run inserted together then deleted collapses to one range record, so the
    // encoding is bounded by the run count, not the number of deleted items.
    let mut t = Text::new(eid(2, 2));
    t.insert(0, &"x".repeat(1000), stmp(1, 1));
    t.delete(0, 1000); // tombstone the whole run

    let bytes = t.encode_state();
    // A per-node encoding would be ~1000 * (stamp + value + anchor) bytes; the
    // compressed one is a single small run record plus the id header.
    assert!(
        bytes.len() < 200,
        "1000 deleted codepoints encoded to {} bytes",
        bytes.len()
    );
    // And it still reads back as empty live text.
    let back = Text::decode_state(&bytes).unwrap();
    assert_eq!(back.as_string(), "");
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn compressed_tombstones_still_anchor_a_concurrent_insert() {
    // The whole point of keeping tombstones is positioning. After a run is
    // deleted and the replica is reloaded from its compressed snapshot, a
    // concurrent insert that anchored inside the deleted run must still land in
    // the same place on both replicas.
    let mut a = Text::new(eid(2, 2));
    a.insert(0, "abcde", stmp(1, 1));
    let mut b = a.deep_clone();

    // b deletes the middle; a concurrently inserts after 'c' (char_id 3,1).
    b.delete(1, 3); // remove "bcd" -> "ae"
    let anchor = a.place(3); // after 'c'
    a.insert_run(stmp(10, 2), "Z", anchor);

    // Reload b from its compressed snapshot, then converge.
    let mut b = Text::decode_state(&b.encode_state()).unwrap();
    b.merge(&a);
    a.merge(&b);
    assert_eq!(a.as_string(), b.as_string(), "must converge after a reload");
}

#[test]
fn scattered_tombstones_and_live_nodes_round_trip() {
    // Interleaved live and deleted nodes: deletes that do not form one chain
    // stay as separate (length-1) runs, and live values survive untouched.
    let mut l = List::new(eid(1, 1));
    for i in 0..6 {
        l.insert(i, int(i as i64), stmp(i as u64 + 1, 1));
    }
    l.delete(1); // tombstone value 1
    l.delete(2); // tombstone value 3 (indices shift as live set shrinks)

    let bytes = l.encode_state();
    let back = List::decode_state(&bytes).unwrap();
    assert_eq!(ints(&back), ints(&l));
    assert_eq!(back.encode_state(), bytes);
}

#[test]
fn a_bogus_run_length_is_rejected_not_expanded() {
    // A crafted record claiming an enormous run must error at the cap, not try
    // to reconstruct billions of nodes. Build an honest single-tombstone list
    // (0 live nodes, 1 run of length 1), then overwrite the run-length field
    // with a huge value. Layout: id, u32 live_count(=0), u32 run_count(=1),
    // stamp start, u32 length, anchor. The length is the u32 sitting `4 +
    // anchor_len` before the end — compute anchor_len from a zero-tombstone
    // list, whose encoding is `id, u32 live_count(=0), u32 run_count(=0)`.
    let empty = List::new(eid(1, 1)).encode_state();
    let header = empty.len(); // id + live_count(0) + run_count(0)

    let mut l = List::new(eid(1, 1));
    l.insert(0, int(7), stmp(1, 1));
    l.delete(0);
    let mut bytes = l.encode_state();
    // Everything after `header - 4` (re-counting run_count as 1) is: stamp,
    // length, anchor. anchor_len = total - (header + stamp_len + 4).
    let stamp_len = 25; // 8-byte lamport + 16-byte client id + 1-byte offset flag
    let anchor_len = bytes.len() - (header + stamp_len + 4);
    let len_off = bytes.len() - anchor_len - 4;
    bytes[len_off..len_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(
        List::decode_state(&bytes).is_err(),
        "an oversized run length must be rejected, not expanded"
    );
}

#[test]
fn an_empty_run_is_rejected() {
    // The encoder never emits a zero-length run; accepting one would admit
    // non-canonical encodings (padding that decodes to the same state). Craft
    // an honest single-tombstone record and zero its length field.
    let empty = List::new(eid(1, 1)).encode_state();
    let header = empty.len();

    let mut l = List::new(eid(1, 1));
    l.insert(0, int(7), stmp(1, 1));
    l.delete(0);
    let mut bytes = l.encode_state();
    let stamp_len = 25;
    let anchor_len = bytes.len() - (header + stamp_len + 4);
    let len_off = bytes.len() - anchor_len - 4;
    bytes[len_off..len_off + 4].copy_from_slice(&0u32.to_le_bytes());
    assert!(
        List::decode_state(&bytes).is_err(),
        "a zero-length run must be rejected as malformed"
    );
}

#[test]
fn many_capped_runs_exceed_the_global_decode_budget() {
    // The per-record cap bounds one run but not their sum: a small stream of
    // many records, each at the per-record cap, could still claim an enormous
    // node count. Assemble such a stream directly and confirm decode rejects it
    // on the declared total — quickly, without materialising the nodes.
    let empty = List::new(eid(1, 1)).encode_state();
    let header = empty.len(); // id + live_count(0) + run_count(0)

    let mut l = List::new(eid(1, 1));
    l.insert(0, int(7), stmp(1, 1));
    l.delete(0);
    let one_run = l.encode_state();
    // One record = everything past the header (stamp, length, anchor), with its
    // length field set to the per-record cap (1 << 20).
    let mut record = one_run[header..].to_vec();
    let stamp_len = 24;
    let len_off = stamp_len; // length sits right after the start stamp
    record[len_off..len_off + 4].copy_from_slice(&(1u32 << 20).to_le_bytes());

    // Many records at the per-record cap (1<<20 each) sum far past the global
    // budget, so decode must reject on the declared total.
    let runs = 17u32;
    let mut bytes = one_run[..header].to_vec();
    bytes[header - 4..].copy_from_slice(&runs.to_le_bytes()); // run_count
    for _ in 0..runs {
        bytes.extend_from_slice(&record);
    }
    assert!(
        List::decode_state(&bytes).is_err(),
        "the summed run lengths exceed the decode budget and must be rejected"
    );
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
