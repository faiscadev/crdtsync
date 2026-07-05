//! Compaction — bounding a room's in-memory op log with a state snapshot.
//!
//! A room's log grows without bound as ops arrive. `compact` folds everything
//! logged so far into the merged replica and drops those ops, keeping only the
//! high-water sequence and the dedup set. Catch-up stays correct across the
//! boundary: a subscriber still at or above the compaction floor gets the ops
//! it missed as a delta; one that fell below the floor gets a whole-replica
//! snapshot of the current state (via `Document::encode_state`) tagged with the
//! sequence it lands at, then folds any later ops on top. Dedup survives, so a
//! replayed compacted op is still rejected.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar};
use crdtsync_server::{Catchup, Hub};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn ingest(h: &mut Hub, room: &[u8], ops: Vec<Op>) -> Vec<Op> {
    h.ingest(room, ops, None).unwrap()
}

fn int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

fn counter(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a Counter"),
    }
}

/// Unwrap a catch-up that must be a plain op delta.
fn ops(c: Catchup) -> Vec<Op> {
    match c {
        Catchup::Ops(v) => v,
        Catchup::Snapshot { .. } => panic!("expected an op delta, got a snapshot"),
    }
}

/// Unwrap a catch-up that must be a snapshot, decoding it to a document.
fn snapshot(c: Catchup) -> (u64, Document) {
    match c {
        Catchup::Snapshot { seq, state } => (seq, Document::decode_state(&state).unwrap()),
        Catchup::Ops(_) => panic!("expected a snapshot, got an op delta"),
    }
}

const ROOM: &[u8] = b"room-1";

/// Ingest two registers and return the room's high-water sequence.
fn populate_two(h: &mut Hub) -> u64 {
    let mut a = doc(1);
    ingest(h, ROOM, a.transact(|tx| tx.register(b"a", Scalar::Int(1))));
    ingest(h, ROOM, a.transact(|tx| tx.register(b"b", Scalar::Int(2))));
    h.seq(ROOM)
}

// --- log truncation ---

#[test]
fn compaction_preserves_the_high_water_sequence() {
    let mut h = hub();
    let seq = populate_two(&mut h);
    h.compact(ROOM).unwrap();
    // The log is gone, but no op's sequence changes: the head stays put.
    assert_eq!(h.seq(ROOM), seq);
}

#[test]
fn compaction_keeps_the_merged_state() {
    let mut h = hub();
    populate_two(&mut h);
    h.compact(ROOM).unwrap();
    assert_eq!(int(h.get(ROOM, b"a")), 1);
    assert_eq!(int(h.get(ROOM, b"b")), 2);
}

#[test]
fn a_compacted_room_serves_a_below_floor_subscriber_a_snapshot() {
    let mut h = hub();
    let seq = populate_two(&mut h);
    h.compact(ROOM).unwrap();
    // A subscriber that saw nothing is below the floor: it gets the whole
    // current state as a snapshot, tagged with the head sequence, not ops.
    let (snap_seq, restored) = snapshot(h.catch_up(ROOM, 0));
    assert_eq!(snap_seq, seq);
    assert_eq!(int(restored.get(b"a")), 1);
    assert_eq!(int(restored.get(b"b")), 2);
}

#[test]
fn a_subscriber_at_the_floor_gets_no_ops() {
    let mut h = hub();
    let seq = populate_two(&mut h);
    h.compact(ROOM).unwrap();
    // Exactly at the head: nothing to send.
    assert!(ops(h.catch_up(ROOM, seq)).is_empty());
}

// --- deltas after compaction ---

#[test]
fn ops_after_compaction_are_a_delta_for_a_current_subscriber() {
    let mut h = hub();
    let floor = populate_two(&mut h);
    h.compact(ROOM).unwrap();

    let mut c = doc(3);
    let later = ingest(
        &mut h,
        ROOM,
        c.transact(|tx| tx.register(b"c", Scalar::Int(3))),
    );
    assert_eq!(h.seq(ROOM), floor + 1);

    // A subscriber caught up to the floor only needs the op past it.
    assert_eq!(ops(h.catch_up(ROOM, floor)), later);
}

#[test]
fn a_below_floor_snapshot_includes_ops_applied_after_compaction() {
    let mut h = hub();
    populate_two(&mut h);
    h.compact(ROOM).unwrap();
    let mut c = doc(3);
    ingest(
        &mut h,
        ROOM,
        c.transact(|tx| tx.register(b"c", Scalar::Int(3))),
    );

    // The snapshot reflects the live replica, so it carries the post-compaction
    // write too, and its sequence is the new head.
    let (snap_seq, restored) = snapshot(h.catch_up(ROOM, 0));
    assert_eq!(snap_seq, h.seq(ROOM));
    assert_eq!(int(restored.get(b"a")), 1);
    assert_eq!(int(restored.get(b"c")), 3);
}

// --- dedup across the boundary ---

#[test]
fn dedup_survives_compaction() {
    let mut h = hub();
    let mut a = doc(1);
    let first = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    ingest(&mut h, ROOM, first.clone());
    let seq = h.seq(ROOM);
    h.compact(ROOM).unwrap();
    // Replaying a compacted op must not re-apply or grow the log.
    assert!(ingest(&mut h, ROOM, first).is_empty());
    assert_eq!(h.seq(ROOM), seq);
    assert_eq!(int(h.get(ROOM, b"a")), 1);
}

// --- idempotence + edges ---

#[test]
fn compacting_twice_is_stable() {
    let mut h = hub();
    let seq = populate_two(&mut h);
    h.compact(ROOM).unwrap();
    h.compact(ROOM).unwrap();
    assert_eq!(h.seq(ROOM), seq);
    let (snap_seq, restored) = snapshot(h.catch_up(ROOM, 0));
    assert_eq!(snap_seq, seq);
    assert_eq!(int(restored.get(b"b")), 2);
}

#[test]
fn compacting_an_unknown_room_is_a_no_op() {
    let mut h = hub();
    h.compact(b"nope").unwrap();
    assert_eq!(h.seq(b"nope"), 0);
}

#[test]
fn an_uncompacted_room_still_serves_ops_from_zero() {
    let mut h = hub();
    populate_two(&mut h);
    // No compaction: a fresh subscriber replays the whole log as ops.
    assert_eq!(ops(h.catch_up(ROOM, 0)).len(), 2);
}

// --- automatic compaction policy ---

/// Ingest `count` single-op counter increments.
fn ingest_incs(h: &mut Hub, count: usize) {
    let mut a = doc(1);
    for _ in 0..count {
        ingest(h, ROOM, a.transact(|tx| tx.inc(b"n", 1)));
    }
}

#[test]
fn without_a_threshold_a_room_is_never_compacted() {
    let mut h = hub();
    ingest_incs(&mut h, 6);
    // No policy: a from-zero subscriber still replays the whole log as ops.
    assert_eq!(ops(h.catch_up(ROOM, 0)).len(), 6);
}

#[test]
fn crossing_the_threshold_auto_compacts() {
    let mut h = hub();
    h.set_compaction_threshold(3);
    ingest_incs(&mut h, 3);
    // The retained log hit the threshold and folded into a snapshot: a from-zero
    // subscriber gets a snapshot, and the head sequence is preserved.
    let (snap_seq, restored) = snapshot(h.catch_up(ROOM, 0));
    assert_eq!(snap_seq, 3);
    assert_eq!(h.seq(ROOM), 3);
    assert_eq!(counter(restored.get(b"n")), 3);
}

#[test]
fn auto_compaction_resets_its_window() {
    let mut h = hub();
    h.set_compaction_threshold(3);
    ingest_incs(&mut h, 3); // compacts at the third op

    // The window resets after compaction: one more op does not re-compact, so a
    // subscriber at the floor gets it as a delta rather than a fresh snapshot.
    let mut a = doc(2);
    let later = ingest(&mut h, ROOM, a.transact(|tx| tx.inc(b"n", 1)));
    assert_eq!(ops(h.catch_up(ROOM, 3)), later);
    assert_eq!(h.seq(ROOM), 4);
}

#[test]
fn a_batch_that_overshoots_the_threshold_still_compacts() {
    let mut h = hub();
    h.set_compaction_threshold(2);
    let mut a = doc(1);
    let mut batch = a.transact(|tx| tx.inc(b"n", 1));
    batch.extend(a.transact(|tx| tx.inc(b"n", 1)));
    batch.extend(a.transact(|tx| tx.inc(b"n", 1)));
    ingest(&mut h, ROOM, batch); // 3 ops in one batch, threshold 2
    let (snap_seq, _) = snapshot(h.catch_up(ROOM, 0));
    assert_eq!(snap_seq, 3);
}

#[test]
fn auto_compaction_preserves_dedup() {
    let mut h = hub();
    h.set_compaction_threshold(1);
    let mut a = doc(1);
    let op = a.transact(|tx| tx.register(b"k", Scalar::Int(9)));
    ingest(&mut h, ROOM, op.clone()); // threshold 1: compacts at once
                                      // The compacted op is still deduped on a resend.
    assert!(ingest(&mut h, ROOM, op).is_empty());
    assert_eq!(h.seq(ROOM), 1);
    assert_eq!(int(h.get(ROOM, b"k")), 9);
}

// --- nested state through the snapshot ---

#[test]
fn a_snapshot_round_trips_a_nested_document() {
    let mut h = hub();
    let mut a = doc(1);
    ingest(
        &mut h,
        ROOM,
        a.transact(|tx| {
            tx.register(b"reg", Scalar::Int(7));
            tx.inc(b"cnt", 5);
            let mut m = tx.map(b"m");
            m.register(b"inner", Scalar::Int(9));
            tx.list(b"lst").insert(0, Scalar::Int(1));
            tx.text(b"txt").insert(0, "hi");
        }),
    );
    h.compact(ROOM).unwrap();

    let (_, restored) = snapshot(h.catch_up(ROOM, 0));
    assert_eq!(int(restored.get(b"reg")), 7);
    assert_eq!(counter(restored.get(b"cnt")), 5);
    match restored.get(b"m") {
        Some(Element::Map(m)) => assert_eq!(int(m.borrow().get(b"inner")), 9),
        _ => panic!("expected a nested map"),
    }
    match restored.get(b"lst") {
        Some(Element::List(l)) => match l.borrow().get(0) {
            Some(Element::Scalar(Scalar::Int(n))) => assert_eq!(n, 1),
            _ => panic!("expected an int at the head of the list"),
        },
        _ => panic!("expected a list"),
    }
    match restored.get(b"txt") {
        Some(Element::Text(t)) => assert_eq!(t.borrow().as_string(), "hi"),
        _ => panic!("expected a text"),
    }
}
