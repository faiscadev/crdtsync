//! Text — a collaborative character sequence. CRDT identity is the codepoint
//! (Unicode scalar value): every codepoint gets a stable char_id and is one
//! node in the same Fugue sequence that backs List, so concurrent edits
//! converge and never interleave. A run inserted together takes consecutive
//! char_ids from its base stamp. The core is Unicode-neutral beyond codepoint
//! identity — no normalization, no grapheme segmentation (that is an SDK
//! helper); indices here are codepoint indices.

use crdtsync_core::text::Text;
use crdtsync_core::{Anchor, Stamp};

mod common;
use common::{default_id, eid, stmp};

/// One captured text-insert op: the run's base stamp, its string, and the
/// placement of its first codepoint.
type TextOp = (Stamp, String, Anchor);

fn text() -> Text {
    Text::new(default_id())
}

/// Insert `s` at codepoint `index`, its run based at stamp `(lamport, client)`.
fn ins(t: &mut Text, index: usize, s: &str, lamport: u64, client: u8) {
    t.insert(index, s, stmp(lamport, client));
}

// --- construction / read ---

#[test]
fn new_is_empty() {
    let t = text();
    assert_eq!(t.len(), 0);
    assert!(t.is_empty());
    assert_eq!(t.as_string(), "");
}

#[test]
fn new_stores_id() {
    let t = Text::new(eid(7, 42));
    assert_eq!(t.id(), eid(7, 42));
}

// --- insert ---

#[test]
fn insert_builds_the_string() {
    let mut t = text();
    ins(&mut t, 0, "hello", 1, 1);
    assert_eq!(t.as_string(), "hello");
    assert_eq!(t.len(), 5);
}

#[test]
fn insert_in_the_middle() {
    let mut t = text();
    ins(&mut t, 0, "ad", 1, 1);
    ins(&mut t, 1, "bc", 3, 1);
    assert_eq!(t.as_string(), "abcd");
}

#[test]
fn insert_at_front_prepends() {
    let mut t = text();
    ins(&mut t, 0, "world", 1, 1);
    ins(&mut t, 0, "hello ", 10, 1);
    assert_eq!(t.as_string(), "hello world");
}

// --- Unicode: codepoint identity ---

#[test]
fn len_counts_codepoints_not_bytes() {
    let mut t = text();
    ins(&mut t, 0, "héllo", 1, 1); // 'é' is one codepoint, two UTF-8 bytes
    assert_eq!(t.len(), 5);
    assert_eq!(t.as_string(), "héllo");
}

#[test]
fn a_multibyte_codepoint_is_one_indivisible_node() {
    let mut t = text();
    ins(&mut t, 0, "a😀b", 1, 1); // emoji is one codepoint (U+1F600)
    assert_eq!(t.len(), 3);
    t.delete(1, 1); // remove the emoji
    assert_eq!(t.as_string(), "ab");
}

#[test]
fn combining_marks_are_separate_codepoints() {
    let mut t = text();
    ins(&mut t, 0, "e\u{0301}", 1, 1); // 'e' + combining acute = 2 codepoints
    assert_eq!(t.len(), 2);
    // No normalization: the sequence stays decomposed, not folded to U+00E9.
    assert_eq!(t.as_string(), "e\u{0301}");
    t.delete(1, 1); // drop the combining mark
    assert_eq!(t.as_string(), "e");
}

#[test]
fn insert_between_codepoints_of_a_multibyte_string() {
    let mut t = text();
    ins(&mut t, 0, "😀😀", 1, 1);
    ins(&mut t, 1, "x", 5, 1);
    assert_eq!(t.as_string(), "😀x😀");
}

// --- delete ---

#[test]
fn delete_removes_a_range() {
    let mut t = text();
    ins(&mut t, 0, "abcdef", 1, 1);
    t.delete(1, 3); // drop "bcd"
    assert_eq!(t.as_string(), "aef");
    assert_eq!(t.len(), 3);
}

#[test]
fn delete_then_insert_positions_correctly() {
    let mut t = text();
    ins(&mut t, 0, "ac", 1, 1);
    t.delete(1, 1); // tombstone 'c' -> "a"
    ins(&mut t, 1, "b", 5, 1);
    assert_eq!(t.as_string(), "ab");
}

// --- merge laws ---

#[test]
fn merge_is_idempotent() {
    let mut t = text();
    ins(&mut t, 0, "abc", 1, 1);
    let twin = t.deep_clone();
    t.merge(&twin);
    assert_eq!(t.as_string(), "abc");
}

#[test]
fn merge_is_commutative() {
    let mut base = text();
    ins(&mut base, 0, "mid", 1, 1);
    let mut a = base.deep_clone();
    let mut b = base.deep_clone();
    ins(&mut a, 0, "L", 10, 1);
    ins(&mut b, 3, "R", 10, 2);

    let mut ab = a.deep_clone();
    ab.merge(&b);
    let mut ba = b.deep_clone();
    ba.merge(&a);
    assert_eq!(ab.as_string(), ba.as_string());
}

#[test]
fn merge_carries_tombstones() {
    let mut base = text();
    ins(&mut base, 0, "abc", 1, 1);
    let mut a = base.deep_clone();
    let b = base.deep_clone();
    a.delete(1, 1); // "ac"
    a.merge(&b);
    assert_eq!(a.as_string(), "ac");
}

// --- Fugue: concurrent editing ---

#[test]
fn concurrent_runs_at_the_same_gap_do_not_interleave() {
    let mut a = text();
    let mut b = text();
    ins(&mut a, 0, "ABC", 1, 1);
    ins(&mut b, 0, "XYZ", 1, 2);
    a.merge(&b);
    b.merge(&a);
    assert_eq!(a.as_string(), b.as_string(), "replicas diverged");
    let s = a.as_string();
    assert!(s == "ABCXYZ" || s == "XYZABC", "runs interleaved: {s}");
}

#[test]
fn concurrent_edits_converge() {
    let mut base = text();
    ins(&mut base, 0, "the fox", 1, 1);
    let mut a = base.deep_clone();
    let mut b = base.deep_clone();
    ins(&mut a, 4, "quick ", 10, 1); // "the quick fox"
    ins(&mut b, 7, " jumps", 10, 2); // "the fox jumps"
    a.merge(&b);
    b.merge(&a);
    assert_eq!(a.as_string(), b.as_string());
}

// --- lifecycle ---

#[test]
fn deep_clone_is_independent() {
    let mut t = text();
    ins(&mut t, 0, "ab", 1, 1);
    let mut c = t.deep_clone();
    ins(&mut c, 2, "c", 5, 1);
    assert_eq!(t.as_string(), "ab");
    assert_eq!(c.as_string(), "abc");
}

#[test]
fn displace_flags_the_handle() {
    let t = text();
    assert!(!t.is_displaced());
    t.displace();
    assert!(t.is_displaced());
}

// --- op-oriented placement (a text-insert op carries its run's placement, so
//     it applies identically on every replica regardless of local index) ---

/// Insert `s` at codepoint `index`, capturing it as a replica-independent op.
fn capture(t: &mut Text, index: usize, s: &str, lamport: u64, client: u8) -> TextOp {
    let anchor = t.place(index);
    let base = stmp(lamport, client);
    t.insert_run(base, s, anchor);
    (base, s.to_string(), anchor)
}

fn apply_ops(t: &mut Text, ops: &[TextOp]) {
    for (base, s, anchor) in ops {
        t.insert_run(*base, s, *anchor);
    }
}

#[test]
fn place_then_insert_run_matches_insert() {
    let mut t = text();
    capture(&mut t, 0, "hello", 1, 1);
    assert_eq!(t.as_string(), "hello");
}

#[test]
fn captured_text_op_replays_on_a_fresh_replica() {
    let mut a = text();
    let op = capture(&mut a, 0, "hi 😀", 1, 1);
    let mut b = text();
    apply_ops(&mut b, &[op]);
    assert_eq!(b.as_string(), "hi 😀");
}

#[test]
fn concurrent_text_runs_converge_without_interleaving() {
    let mut a = text();
    let mut b = text();
    let oa = capture(&mut a, 0, "ABC", 1, 1);
    let ob = capture(&mut b, 0, "XYZ", 1, 2);
    apply_ops(&mut a, &[ob]);
    apply_ops(&mut b, &[oa]);
    assert_eq!(a.as_string(), b.as_string());
    let s = a.as_string();
    assert!(s == "ABCXYZ" || s == "XYZABC", "interleaved: {s}");
}

#[test]
fn insert_run_is_idempotent() {
    let mut t = text();
    let op = capture(&mut t, 0, "abc", 1, 1);
    apply_ops(&mut t, &[op]); // replay the same run
    assert_eq!(t.as_string(), "abc");
    assert_eq!(t.len(), 3);
}

#[test]
fn node_ids_and_delete_ids_remove_a_range() {
    let mut t = text();
    ins(&mut t, 0, "abcde", 1, 1);
    let ids = t.node_ids(1, 3); // "bcd"
    assert_eq!(ids.len(), 3);
    t.delete_ids(&ids);
    assert_eq!(t.as_string(), "ae");
}

#[test]
fn delete_ids_is_idempotent() {
    let mut t = text();
    ins(&mut t, 0, "ab", 1, 1);
    let ids = t.node_ids(0, 1); // "a"
    t.delete_ids(&ids);
    t.delete_ids(&ids); // repeat
    assert_eq!(t.as_string(), "b");
}

#[test]
fn node_ids_clamps_past_the_end() {
    let mut t = text();
    ins(&mut t, 0, "ab", 1, 1);
    assert_eq!(t.node_ids(1, 10).len(), 1); // only "b" remains
}

#[test]
fn an_insert_run_based_at_the_lamport_ceiling_keeps_every_codepoint_distinct() {
    // `base` is an op's wire-derived stamp, so an adversarial op can set it to
    // the lamport ceiling. Deriving one char_id per codepoint must neither panic
    // nor collapse: the lamport cannot advance past `u64::MAX`, so the surplus
    // carries into the stamp offset and every codepoint keeps a distinct id.
    let mut t = text();
    let anchor = Anchor {
        parent: None,
        side: crdtsync_core::Side::Right,
    };
    let base = Stamp {
        lamport: u64::MAX,
        client: crdtsync_core::ClientId::from_bytes([7u8; 16]),
        offset: 0,
    };
    t.insert_run(base, "ab", anchor);

    // No collapse: both codepoints are present, in order, with distinct ids.
    assert_eq!(t.as_string(), "ab");
    assert_eq!(t.len(), 2);
    let ids = t.node_ids(0, 2);
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    // Both pin to the ceiling lamport; the offset is what separates them.
    assert_eq!(ids[0].lamport, u64::MAX);
    assert_eq!(ids[1].lamport, u64::MAX);
    assert_ne!(ids[0].offset, ids[1].offset);

    // The distinct ids survive a snapshot round-trip.
    let restored = Text::decode_state(&t.encode_state()).unwrap();
    assert_eq!(restored.as_string(), "ab");
    assert_eq!(restored.node_ids(0, 2), ids);

    // Two replicas deriving the same ceiling run converge on the same ids.
    let mut other = text();
    other.insert_run(base, "ab", anchor);
    assert_eq!(other.node_ids(0, 2), ids);
    t.merge(&other);
    assert_eq!(t.as_string(), "ab");
    assert_eq!(t.len(), 2);
}

#[test]
fn an_insert_run_based_at_the_offset_ceiling_stays_total_and_convergent() {
    // A wire op can decode any offset, so an adversary can base a run at the
    // offset ceiling too. Deriving char_ids must not panic; the offset carry
    // saturates, so a crafted run at `(MAX, MAX)` collapses convergently rather
    // than crashing. No legitimately minted op reaches here — a real op always
    // bases a run at offset 0 — so no real insert loses a codepoint.
    let anchor = Anchor {
        parent: None,
        side: crdtsync_core::Side::Right,
    };
    let base = Stamp {
        lamport: u64::MAX,
        client: crdtsync_core::ClientId::from_bytes([7u8; 16]),
        offset: u64::MAX,
    };
    let mut a = text();
    a.insert_run(base, "ab", anchor); // must not panic
    let mut b = text();
    b.insert_run(base, "ab", anchor);
    // Deterministic across replicas: same ids, same projection, converges.
    assert_eq!(a.node_ids(0, a.len()), b.node_ids(0, b.len()));
    a.merge(&b);
    assert_eq!(a.as_string(), b.as_string());
}
