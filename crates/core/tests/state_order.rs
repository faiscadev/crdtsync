//! `encode_state` is order-canonical: two replicas that folded the same ops
//! encode the same bytes, whatever order those ops arrived in.
//!
//! Byte identity is what the snapshot/compaction arc treats as replica identity —
//! a snapshot-vs-op convergence check and every digest comparison rest on it — so
//! agreeing reads are not enough. Two things a displacement-heavy pool used to
//! break: an op whose target was displaced when it arrived was held forever, while
//! the same op arriving a moment earlier applied; and the held ops were encoded in
//! arrival order. A displaced container is *retained*, so a write addressed to one
//! lands in it hidden rather than waiting on a re-install that may never come, and
//! what is genuinely still waiting — an op whose target this replica has never seen
//! created — is held in op-id order.

use crdtsync_core::doc::Document;
use crdtsync_core::{Element, Op, Scalar};

mod common;
use common::cid;

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// Fold `pool` into a fresh replica in the given order.
fn fold(pool: &[&Op]) -> Document {
    let mut d = doc(9);
    for op in pool {
        d.apply(op);
    }
    d
}

/// A rotation of `pool` by `n` — every op still lands, in a different order.
fn rotated<'a>(pool: &[&'a Op], n: usize) -> Vec<&'a Op> {
    let mut out = pool.to_vec();
    out.rotate_left(n % pool.len());
    out
}

/// The document's slots, as a stable rendering.
fn render(d: &Document, keys: &[&[u8]]) -> String {
    keys.iter()
        .map(|k| match d.get(k) {
            None => "_".to_string(),
            Some(Element::Scalar(Scalar::Int(n))) => format!("S{n}"),
            Some(Element::Scalar(_)) => "S?".to_string(),
            Some(Element::Map(m)) => format!("M{}", m.borrow().keys().len()),
            Some(Element::List(l)) => format!("L{}", l.borrow().len()),
            Some(Element::Text(t)) => format!("T{:?}", t.borrow().as_string()),
            Some(_) => "?".to_string(),
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// Whether a snapshot's trailing framed op buffer holds exactly `ops`, in order.
/// `encode_state` ends with a `u32` length and that many framed bytes, so the
/// buffer a snapshot carries is a suffix of it.
fn holds_buffer(snapshot: &[u8], ops: &[Op]) -> bool {
    let framed = crdtsync_core::encode_ops(ops);
    let mut tail = (framed.len() as u32).to_le_bytes().to_vec();
    tail.extend_from_slice(&framed);
    snapshot.ends_with(&tail)
}

/// A pool in which three authors take the same keys from one another: every
/// container create is outranked by a later scalar at its key, so a fold that sees
/// a create's children after the displacement addresses a container that is
/// retained and never installed again.
fn displacement_heavy_pool() -> Vec<Op> {
    let mut a = doc(1);
    let creates = a.transact(|tx| {
        let mut m = tx.map(b"m");
        m.register(b"r", Scalar::Int(7));
        m.map(b"n").register(b"q", Scalar::Int(8));
        m.list(b"l").insert(0, Scalar::Int(1));
        m.text(b"t").insert(0, "hi");
        m.inc(b"c", 3);
    });

    // A peer holding those creates takes the nested key back with a higher-stamped
    // scalar, displacing the inner map.
    let mut b = doc(2);
    for op in &creates {
        b.apply(op);
    }
    let inner = b.transact(|tx| tx.map(b"m").set(b"n", Scalar::Int(99)));

    // A third takes the outer key, displacing the whole subtree.
    let mut c = doc(3);
    for op in creates.iter().chain(&inner) {
        c.apply(op);
    }
    let outer = c.transact(|tx| tx.set(b"m", Scalar::Int(42)));

    creates.into_iter().chain(inner).chain(outer).collect()
}

const KEYS: &[&[u8]] = &[b"m"];

#[test]
fn a_displacement_heavy_pool_encodes_the_same_bytes_in_every_arrival_order() {
    let pool = displacement_heavy_pool();
    let refs: Vec<&Op> = pool.iter().collect();
    let first = fold(&refs);
    let bytes = first.encode_state();
    for n in 1..pool.len() {
        let other = fold(&rotated(&refs, n));
        assert_eq!(
            render(&other, KEYS),
            render(&first, KEYS),
            "rotation {n} reads differently"
        );
        assert_eq!(
            other.encode_state(),
            bytes,
            "rotation {n} encodes different bytes"
        );
    }
}

#[test]
fn a_reversed_displacement_heavy_pool_encodes_the_same_bytes() {
    // A rotation preserves each op's neighbours; a reversal preserves none, so
    // every create arrives after the displacement that outranks it.
    let pool = displacement_heavy_pool();
    let forward: Vec<&Op> = pool.iter().collect();
    let mut backward = forward.clone();
    backward.reverse();
    assert_eq!(
        fold(&backward).encode_state(),
        fold(&forward).encode_state()
    );
}

#[test]
fn a_write_under_a_displaced_container_applies_rather_than_waiting() {
    // The create loses its slot, so the write that follows addresses a retained,
    // never-installed container. It applies there: `apply` reports it, and the
    // snapshot carries an empty buffer.
    let mut a = doc(1);
    let creates = a.transact(|tx| {
        tx.map(b"m").register(b"r", Scalar::Int(7));
    });
    let mut b = doc(2);
    for op in &creates {
        b.apply(op);
    }
    let displace = b.transact(|tx| tx.set(b"m", Scalar::Int(42)));

    let mut d = doc(3);
    assert!(d.apply(&creates[0]), "the create applies at the root");
    for op in &displace {
        assert!(d.apply(op), "the displacing scalar applies");
    }
    assert!(
        d.apply(&creates[1]),
        "a write under a displaced container applies into it"
    );
    assert!(
        holds_buffer(&d.encode_state(), &[]),
        "nothing is left waiting"
    );
}

#[test]
fn a_write_under_a_displaced_container_shows_when_the_slot_returns() {
    // Holding the write until the container came back was what made it visible on a
    // re-install; landing it hidden has to keep that.
    let mut a = doc(1);
    let creates = a.transact(|tx| {
        tx.map(b"m").register(b"r", Scalar::Int(7));
    });
    let mut b = doc(2);
    for op in &creates {
        b.apply(op);
    }
    let displace = b.transact(|tx| tx.set(b"m", Scalar::Int(42)));
    // The map is re-created above the scalar and takes its key back.
    for op in &displace {
        a.apply(op);
    }
    let revive = a.transact(|tx| {
        tx.map(b"m").register(b"s", Scalar::Int(1));
    });

    let mut late = doc(3);
    for op in creates
        .iter()
        .take(1)
        .chain(&displace)
        .chain(creates.iter().skip(1))
        .chain(&revive)
    {
        late.apply(op);
    }
    let mut early = doc(3);
    for op in creates.iter().chain(&displace).chain(&revive) {
        early.apply(op);
    }
    let m = match late.get(b"m") {
        Some(Element::Map(m)) => m,
        _ => panic!("expected the revived map"),
    };
    assert!(
        m.borrow().get(b"r").is_some(),
        "the write made under the displacement is visible once the slot returns"
    );
    assert_eq!(late.encode_state(), early.encode_state());
}

/// Two writes into a map whose create is never delivered, so both wait, returned
/// in the order they are to be delivered. Their authors' ids sort the opposite way
/// from their seqs, so an order that read the seq alone is as wrong as no order at
/// all — and the delivery order is that wrong one.
fn two_held_writes() -> (Op, Op) {
    let mut a = doc(5);
    let a_ops = a.transact(|tx| {
        tx.map(b"m").register(b"r", Scalar::Int(7));
    });
    let mut b = doc(2);
    for op in &a_ops {
        b.apply(op);
    }
    // Pad `b`'s sequence past `a`'s, so seq order and author order disagree.
    b.transact(|tx| tx.set(b"pad", Scalar::Int(0)));
    b.transact(|tx| tx.set(b"pad2", Scalar::Int(0)));
    let b_ops = b.transact(|tx| tx.map(b"m").register(b"s", Scalar::Int(8)));
    assert_eq!(b_ops.len(), 1, "the map already exists at `b`");
    let (early, late) = (a_ops[1].clone(), b_ops[0].clone());
    assert!(early.id.client.as_bytes() > late.id.client.as_bytes());
    assert!(early.id.seq < late.id.seq);
    (early, late)
}

#[test]
fn a_held_buffer_is_encoded_in_op_id_order() {
    let (early, late) = two_held_writes();
    let mut d = doc(9);
    assert!(!d.apply(&early), "held: the map's create is unseen");
    assert!(!d.apply(&late), "held: the map's create is unseen");
    assert!(d.get(b"m").is_none());
    assert!(
        holds_buffer(&d.encode_state(), &[late.clone(), early.clone()]),
        "the buffer is not encoded in op-id order"
    );
    assert!(
        !holds_buffer(&d.encode_state(), &[early, late]),
        "the buffer is encoded in arrival order"
    );
}

#[test]
fn a_snapshot_presenting_a_disordered_buffer_re_encodes_in_op_id_order() {
    // The order is an invariant of the buffer, not a courtesy of the encoder: a
    // snapshot that arrives holding its ops the other way round is read into the
    // same state as one holding them in order, and re-encodes identically.
    let (early, late) = two_held_writes();
    let empty = doc(9).encode_state();
    let tail = 4 + crdtsync_core::encode_ops(&[]).len();
    let disordered = {
        let framed = crdtsync_core::encode_ops(&[early.clone(), late.clone()]);
        let mut out = empty[..empty.len() - tail].to_vec();
        out.extend_from_slice(&(framed.len() as u32).to_le_bytes());
        out.extend_from_slice(&framed);
        out
    };
    let back = Document::decode_state(&disordered).expect("decode");
    assert!(
        holds_buffer(&back.encode_state(), &[late, early]),
        "a disordered buffer is not normalised on decode"
    );
}
