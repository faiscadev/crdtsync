//! RelativePosition — a stable cursor/selection position in a sequence.
//!
//! A position binds to a CRDT item id, not an integer offset, so it survives
//! concurrent inserts and deletes without drifting: an insert before it shifts
//! its resolved index by the inserted count, an insert at it lands per gravity,
//! and deleting the item it binds to resolves to the nearest live neighbour on
//! the gravity side. Boundary positions (`Start` / `End`) resolve to `0` / `len`
//! and stay stable. The same type works for List and Text (codepoint-indexed).

use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::list::List;
use crdtsync_core::{DecodeError, Element, Scalar, Side, Stamp, Text};

mod common;
use common::{default_id, stmp};

fn ch(c: u8) -> Element {
    Element::Scalar(Scalar::Bytes(vec![c]))
}

fn list() -> List {
    List::new(default_id())
}

/// Insert byte `c` at `index`, tagged `(lamport, client)`.
fn ins(l: &mut List, index: usize, c: u8, lamport: u64, client: u8) {
    l.insert(index, ch(c), stmp(lamport, client));
}

/// A list holding the bytes of `s`, each from one client at ascending lamports.
fn list_of(s: &str) -> List {
    let mut l = list();
    for (i, c) in s.bytes().enumerate() {
        ins(&mut l, i, c, i as u64 + 1, 1);
    }
    l
}

fn text_of(s: &str) -> Text {
    let mut t = Text::new(default_id());
    t.insert(0, s, stmp(1, 1));
    t
}

// --- capture then resolve is the identity on an unchanged replica ---

#[test]
fn resolve_returns_the_captured_index_on_a_list() {
    let l = list_of("ABCDE");
    for index in 0..=l.len() {
        for side in [Side::Left, Side::Right] {
            let pos = l.relative_position(index, side);
            assert_eq!(
                l.resolve_position(&pos),
                index,
                "index {index} side {side:?} round-trips"
            );
        }
    }
}

#[test]
fn resolve_returns_the_captured_index_on_text() {
    let t = text_of("hello");
    for index in 0..=t.len() {
        for side in [Side::Left, Side::Right] {
            let pos = t.relative_position(index, side);
            assert_eq!(
                t.resolve_position(&pos),
                index,
                "index {index} side {side:?}"
            );
        }
    }
}

// --- an insert before the anchor shifts it; the anchor tracks its item ---

#[test]
fn an_insert_before_the_anchor_shifts_the_resolved_index() {
    let mut l = list_of("AB");
    let pos = l.relative_position(1, Side::Right); // binds to B
    ins(&mut l, 0, b'X', 10, 2); // "XAB"
    assert_eq!(
        l.resolve_position(&pos),
        2,
        "the anchor followed B past the insert"
    );
}

#[test]
fn an_insert_after_the_anchor_does_not_move_it() {
    let mut l = list_of("AB");
    let pos = l.relative_position(1, Side::Right); // binds to B, index 1
    ins(&mut l, 2, b'X', 10, 2); // "ABX"
    assert_eq!(
        l.resolve_position(&pos),
        1,
        "an insert after the anchor is irrelevant"
    );
}

// --- gravity: an insert exactly at the captured gap lands per side ---

#[test]
fn gravity_places_a_concurrent_insert_on_the_expected_side() {
    let mut l = list_of("AB");
    let left = l.relative_position(1, Side::Left); // sticks to A (its right edge)
    let right = l.relative_position(1, Side::Right); // sticks to B (its left edge)
    ins(&mut l, 1, b'X', 10, 2); // "AXB"
    assert_eq!(l.resolve_position(&left), 1, "left gravity stays left of X");
    assert_eq!(
        l.resolve_position(&right),
        2,
        "right gravity stays right of X"
    );
}

// --- a deleted anchor resolves to the nearest live neighbour on its side ---

#[test]
fn a_deleted_right_anchor_resolves_to_the_next_live_item() {
    let mut l = list_of("ABC");
    let pos = l.relative_position(1, Side::Right); // binds to B (before it)
    l.delete(1); // tombstone B -> "AC"
    assert_eq!(l.resolve_position(&pos), 1, "walks right to C");
}

#[test]
fn a_deleted_left_anchor_resolves_after_the_previous_live_item() {
    let mut l = list_of("ABC");
    let pos = l.relative_position(2, Side::Left); // binds after B
    l.delete(1); // tombstone B -> "AC"
    assert_eq!(l.resolve_position(&pos), 1, "walks left to just after A");
}

#[test]
fn a_deleted_anchor_walks_across_a_run_of_tombstones() {
    let mut l = list_of("ABCDE");
    let pos = l.relative_position(2, Side::Right); // binds to C (before it)
                                                   // Delete the run B, C, D by id, leaving tombstones between A and E.
    for id in l.node_ids(1, 3) {
        l.delete_id(id);
    }
    // "AE": walking right from C crosses D's tombstone to reach E.
    assert_eq!(
        l.resolve_position(&pos),
        1,
        "nearest live to the right is E"
    );
}

#[test]
fn a_deleted_anchor_with_no_live_neighbour_clamps_to_the_end() {
    let mut l = list_of("AB");
    let right = l.relative_position(1, Side::Right); // before B
    let left = l.relative_position(1, Side::Left); // after A
    l.delete(1); // delete B
    l.delete(0); // delete A -> empty
    assert_eq!(
        l.resolve_position(&right),
        0,
        "no live to the right -> clamp to len 0"
    );
    assert_eq!(
        l.resolve_position(&left),
        0,
        "no live to the left -> clamp to 0"
    );
}

#[test]
fn deleted_anchor_resolution_holds_on_text() {
    let mut t = text_of("abcde");
    let pos = t.relative_position(2, Side::Right); // before 'c'
                                                   // delete the "bcd" run by codepoint ids
    let ids = t.node_ids(1, 3);
    t.delete_ids(&ids);
    assert_eq!(
        t.resolve_position(&pos),
        1,
        "nearest live to the right is 'e'"
    );
}

// --- Start / End boundaries resolve to 0 / len and stay put ---

#[test]
fn start_and_end_track_the_boundaries_across_edits() {
    let mut l = list_of("AB");
    let start = l.relative_position(0, Side::Left);
    let end = l.relative_position(2, Side::Right);
    assert!(matches!(start, RelativePosition::Start));
    assert!(matches!(end, RelativePosition::End));
    assert_eq!(l.resolve_position(&start), 0);
    assert_eq!(l.resolve_position(&end), 2);

    ins(&mut l, 0, b'X', 10, 2); // prepend
    ins(&mut l, 3, b'Y', 11, 2); // append -> "XABY"
    assert_eq!(l.resolve_position(&start), 0, "Start is always 0");
    assert_eq!(l.resolve_position(&end), l.len(), "End is always len");
    assert_eq!(l.resolve_position(&end), 4);
}

#[test]
fn an_out_of_bounds_index_pins_to_the_end_on_either_side() {
    let l = list_of("AB"); // len 2
    for side in [Side::Left, Side::Right] {
        let pos = l.relative_position(99, side);
        assert_eq!(l.resolve_position(&pos), 2, "a stale index clamps to len");
    }
}

// --- convergence: the same position resolves identically on converged replicas ---

#[test]
fn a_position_resolves_identically_on_converged_replicas() {
    // Two replicas receive the same three inserts in different orders, then merge.
    let a1 = stmp(1, 1);
    let b1 = stmp(2, 2);
    let c1 = stmp(3, 3);
    let mut r1 = list();
    r1.insert(0, ch(b'A'), a1);
    r1.insert(1, ch(b'B'), b1);
    r1.insert(2, ch(b'C'), c1);

    let mut r2 = list();
    r2.insert(0, ch(b'C'), c1);
    r2.insert(0, ch(b'B'), b1);
    r2.insert(0, ch(b'A'), a1);
    r1.merge(&r2);
    r2.merge(&r1);

    // Bind to B on r1, resolve on both — the converged order agrees.
    let pos = r1.relative_position(r1.live_index(b1).unwrap(), Side::Right);
    assert_eq!(r1.resolve_position(&pos), r2.resolve_position(&pos));
    assert_eq!(r2.resolve_position(&pos), r2.live_index(b1).unwrap());
}

// --- codec: every variant round-trips; malformed bytes are an error, not a panic ---

#[test]
fn every_variant_round_trips_through_the_codec() {
    let variants = [
        RelativePosition::Start,
        RelativePosition::End,
        RelativePosition::Before(stmp(7, 9)),
        RelativePosition::After(stmp(42, 3)),
    ];
    for pos in variants {
        let bytes = pos.encode();
        assert_eq!(
            RelativePosition::decode(&bytes),
            Ok(pos),
            "{pos:?} round-trips"
        );
    }
}

#[test]
fn malformed_position_bytes_decode_to_an_error() {
    assert_eq!(
        RelativePosition::decode(&[]),
        Err(DecodeError::UnexpectedEof)
    );
    assert!(matches!(
        RelativePosition::decode(&[99]),
        Err(DecodeError::BadTag { .. })
    ));
    // A Before tag with no stamp following.
    assert_eq!(
        RelativePosition::decode(&[2]),
        Err(DecodeError::UnexpectedEof)
    );
    // A Start tag with trailing bytes.
    assert_eq!(
        RelativePosition::decode(&[0, 0, 0]),
        Err(DecodeError::TrailingBytes)
    );
}

/// A helper the tests lean on: bind by id directly.
#[allow(dead_code)]
fn before(id: Stamp) -> RelativePosition {
    RelativePosition::Before(id)
}
