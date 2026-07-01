//! Hub — the single-node server core: one authoritative replica per room plus
//! that room's append-only op log.
//!
//! Clients ingest ops; the hub deduplicates by op id, folds each new op into
//! the room's replica, and assigns it a monotonic server sequence. A
//! subscriber names the last sequence it saw and the hub replays everything
//! past it — the log a fresh replica replays back to the same state. Ingest is
//! idempotent (reconnects, retries, duplicates), rooms are isolated, and the
//! merged state converges regardless of the order ops arrive.
//!
//! This hub is in-memory (no store attached), so every ingest is infallible.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar};
use crdtsync_server::{Catchup, Hub};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A hub with an arbitrary server-side client for its replicas.
fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// Ingest into an in-memory hub, where persistence can never fail.
fn ingest(h: &mut Hub, room: &[u8], ops: Vec<Op>) -> Vec<Op> {
    h.ingest(room, ops).unwrap()
}

/// Unwrap a catch-up that must be a plain op delta — every room here is
/// uncompacted, so catch-up never returns a snapshot.
fn ops(c: Catchup) -> Vec<Op> {
    match c {
        Catchup::Ops(v) => v,
        Catchup::Snapshot { .. } => panic!("expected an op delta, got a snapshot"),
    }
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

const ROOM: &[u8] = b"room-1";

// --- empty ---

#[test]
fn a_fresh_room_is_empty_at_seq_zero() {
    let h = hub();
    assert_eq!(h.seq(ROOM), 0);
    assert!(h.get(ROOM, b"age").is_none());
}

#[test]
fn catch_up_on_an_unknown_room_is_empty() {
    let mut h = hub();
    assert!(ops(h.catch_up(ROOM, 0)).is_empty());
}

#[test]
fn ingesting_an_empty_batch_is_a_no_op() {
    let mut h = hub();
    assert!(ingest(&mut h, ROOM, Vec::new()).is_empty());
    assert_eq!(h.seq(ROOM), 0);
}

// --- ingest ---

#[test]
fn ingest_applies_and_reads_back() {
    let mut h = hub();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    ingest(&mut h, ROOM, ops);
    assert_eq!(int(h.get(ROOM, b"age")), 30);
}

#[test]
fn ingest_assigns_a_monotonic_sequence() {
    let mut h = hub();
    let mut a = doc(1);
    ingest(
        &mut h,
        ROOM,
        a.transact(|tx| tx.register(b"a", Scalar::Int(1))),
    );
    assert_eq!(h.seq(ROOM), 1);
    ingest(
        &mut h,
        ROOM,
        a.transact(|tx| tx.register(b"b", Scalar::Int(2))),
    );
    assert_eq!(h.seq(ROOM), 2);
}

#[test]
fn ingest_returns_the_newly_applied_ops() {
    let mut h = hub();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(ingest(&mut h, ROOM, ops.clone()), ops);
}

#[test]
fn re_ingesting_the_same_ops_is_idempotent() {
    let mut h = hub();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    ingest(&mut h, ROOM, ops.clone());
    // A reconnect resends the same ops: no new application, no log growth.
    assert!(ingest(&mut h, ROOM, ops).is_empty());
    assert_eq!(h.seq(ROOM), 1);
    assert_eq!(int(h.get(ROOM, b"age")), 30);
}

#[test]
fn a_partial_resend_applies_only_the_new_ops() {
    let mut h = hub();
    let mut a = doc(1);
    let first = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = a.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    ingest(&mut h, ROOM, first.clone());
    let mut resend = first;
    resend.extend(second.clone());
    assert_eq!(ingest(&mut h, ROOM, resend), second);
    assert_eq!(h.seq(ROOM), 2);
}

// --- catch-up ---

#[test]
fn catch_up_from_zero_replays_the_whole_log() {
    let mut h = hub();
    let mut a = doc(1);
    ingest(
        &mut h,
        ROOM,
        a.transact(|tx| tx.register(b"a", Scalar::Int(1))),
    );
    ingest(
        &mut h,
        ROOM,
        a.transact(|tx| tx.register(b"b", Scalar::Int(2))),
    );

    let log = ops(h.catch_up(ROOM, 0));
    assert_eq!(log.len(), 2);

    // A fresh replica replaying the catch-up reconstructs the room's state.
    let mut fresh = doc(9);
    for op in &log {
        fresh.apply(op);
    }
    assert_eq!(int(fresh.get(b"a")), 1);
    assert_eq!(int(fresh.get(b"b")), 2);
}

#[test]
fn catch_up_returns_only_ops_past_the_last_seen_seq() {
    let mut h = hub();
    let mut a = doc(1);
    let first = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = a.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    ingest(&mut h, ROOM, first);
    ingest(&mut h, ROOM, second.clone());
    assert_eq!(ops(h.catch_up(ROOM, 1)), second);
}

#[test]
fn catch_up_at_or_past_head_is_empty() {
    let mut h = hub();
    let mut a = doc(1);
    ingest(
        &mut h,
        ROOM,
        a.transact(|tx| tx.register(b"a", Scalar::Int(1))),
    );
    assert!(ops(h.catch_up(ROOM, 1)).is_empty());
    assert!(ops(h.catch_up(ROOM, 99)).is_empty());
}

// --- isolation + convergence ---

#[test]
fn rooms_are_isolated() {
    let mut h = hub();
    let mut a = doc(1);
    ingest(
        &mut h,
        b"room-a",
        a.transact(|tx| tx.register(b"k", Scalar::Int(1))),
    );
    assert!(h.get(b"room-b", b"k").is_none());
    assert_eq!(h.seq(b"room-b"), 0);
}

#[test]
fn a_counter_converges_regardless_of_ingest_order() {
    let inc_a = doc(1).transact(|tx| tx.inc(b"n", 3));
    let inc_b = doc(2).transact(|tx| tx.inc(b"n", 4));

    let mut forward = hub();
    ingest(&mut forward, ROOM, inc_a.clone());
    ingest(&mut forward, ROOM, inc_b.clone());

    let mut backward = hub();
    ingest(&mut backward, ROOM, inc_b);
    ingest(&mut backward, ROOM, inc_a);

    assert_eq!(counter(forward.get(ROOM, b"n")), 7);
    assert_eq!(counter(backward.get(ROOM, b"n")), 7);
    assert_eq!(forward.seq(ROOM), backward.seq(ROOM));
}

#[test]
fn concurrent_edits_from_two_clients_all_land() {
    let edit_a = doc(1).transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let edit_b = doc(2).transact(|tx| tx.register(b"b", Scalar::Int(2)));
    let mut h = hub();
    ingest(&mut h, ROOM, edit_a);
    ingest(&mut h, ROOM, edit_b);
    assert_eq!(int(h.get(ROOM, b"a")), 1);
    assert_eq!(int(h.get(ROOM, b"b")), 2);
    assert_eq!(h.seq(ROOM), 2);
}
