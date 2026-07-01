//! Text — a collaborative character sequence. CRDT identity is the codepoint
//! (Unicode scalar value): every codepoint gets a stable char_id and is one
//! node in the same Fugue sequence that backs List, so concurrent edits
//! converge and never interleave. A run inserted together takes consecutive
//! char_ids from its base stamp. The core is Unicode-neutral beyond codepoint
//! identity — no normalization, no grapheme segmentation (that is an SDK
//! helper); indices here are codepoint indices.

use crdtsync_core::text::Text;

mod common;
use common::{default_id, eid, stmp};

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
