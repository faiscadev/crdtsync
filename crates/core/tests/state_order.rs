//! `encode_state` is order-canonical over a **displacement-heavy** pool: two
//! replicas that folded the same ops encode the same bytes, whatever order those
//! ops arrived in.
//!
//! Byte identity is what the snapshot/compaction arc treats as replica identity —
//! a snapshot-vs-op convergence check and every digest comparison rest on it — so
//! agreeing reads are not enough. Two things a displacement-heavy pool broke: an
//! op whose target was displaced when it arrived was held forever, while the same
//! op arriving a moment earlier applied; and the held ops were encoded in arrival
//! order. A displaced container is *retained*, so a write addressed to one lands
//! in it hidden rather than waiting on a re-install that may never come, and what
//! is genuinely still waiting is written in op-id order.
//!
//! Displacement is not the only way one op set can reach two encodings: a
//! container create racing a *delete* of its key turns on what a slot records
//! rather than on what a replica holds. What a key retains is the greatest
//! container create it has ever seen there, recorded whether that create wins the
//! slot, loses it to the delete, or is later shadowed by a leaf — a running
//! maximum, which no arrival order can reorder.

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

/// Fold `pool` in the given order, requiring every op to land — the pools whose
/// ops all target the root map, where a held op would mean the buffer is under
/// test rather than the slots.
fn fold_applied(pool: &[&Op]) -> Document {
    let mut d = doc(9);
    for op in pool {
        assert!(d.apply(op), "every op in this pool targets the root map");
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
/// buffer a snapshot carries is a suffix of it. Says nothing about an *empty*
/// buffer — that suffix is four zero bytes, which a populated one ends in too —
/// so "nothing is waiting" is asked of the causal frontier instead.
fn holds_buffer(snapshot: &[u8], ops: &[Op]) -> bool {
    assert!(
        !ops.is_empty(),
        "an empty buffer is not identified by its suffix"
    );
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
fn a_displacement_heavy_pool_encodes_the_same_bytes_under_a_permutation_sweep() {
    // Rotations and the reversal are structured orders; these are arbitrary ones,
    // from a fixed seed so a failure is a case anyone can rerun.
    let pool = displacement_heavy_pool();
    let refs: Vec<&Op> = pool.iter().collect();
    let bytes = fold(&refs).encode_state();
    let mut seed = 0x5eed_c58u64;
    for round in 0..64 {
        let mut order = refs.clone();
        // Fisher-Yates over a xorshift, so the sweep is deterministic.
        for i in (1..order.len()).rev() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            order.swap(i, (seed % (i as u64 + 1)) as usize);
        }
        assert_eq!(
            fold(&order).encode_state(),
            bytes,
            "permutation {round} encodes different bytes"
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

/// Every ordering of `pool`, as index permutations — a small pool's whole order
/// space, where the sweeps above sample a large one's.
fn orders(pool: &[Op]) -> Vec<Vec<&Op>> {
    fn walk<'a>(rest: &[&'a Op], taken: &mut Vec<&'a Op>, out: &mut Vec<Vec<&'a Op>>) {
        if rest.is_empty() {
            out.push(taken.clone());
            return;
        }
        for (i, op) in rest.iter().enumerate() {
            let mut without = rest.to_vec();
            without.remove(i);
            taken.push(op);
            walk(&without, taken, out);
            taken.pop();
        }
    }
    let mut out = Vec::new();
    walk(&pool.iter().collect::<Vec<_>>(), &mut Vec::new(), &mut out);
    out
}

/// Assert every arrival order of `pool` folds to the same bytes, naming the order
/// that broke it by its op kinds.
fn every_order_encodes_alike(pool: &[Op], keys: &[&[u8]]) {
    let first = fold(&pool.iter().collect::<Vec<_>>());
    let bytes = first.encode_state();
    for order in orders(pool) {
        let other = fold(&order);
        let names: Vec<String> = order.iter().map(|op| format!("{:?}", op.kind)).collect();
        assert_eq!(
            render(&other, keys),
            render(&first, keys),
            "order {names:?} reads differently"
        );
        assert_eq!(
            other.encode_state(),
            bytes,
            "order {names:?} encodes different bytes"
        );
    }
}

/// A container create and a delete of its key, from one author. The create is
/// outranked either way round: seen first it is installed and then tombstoned,
/// seen second it never installs at all.
fn create_and_delete() -> Vec<Op> {
    let mut a = doc(1);
    let create = a.transact(|tx| {
        tx.map(b"k");
    });
    let delete = a.transact(|tx| tx.delete(b"k"));
    create.into_iter().chain(delete).collect()
}

/// Two creates of *different kinds* at one key, both outranked by one delete.
fn two_kinds_and_a_delete() -> Vec<Op> {
    let mut a = doc(1);
    let map = a.transact(|tx| {
        tx.map(b"k");
    });
    let list = a.transact(|tx| {
        tx.list(b"k");
    });
    let delete = a.transact(|tx| tx.delete(b"k"));
    map.into_iter().chain(list).chain(delete).collect()
}

/// A create, a scalar that outranks it at the same key, and a delete over both —
/// the container is never live when the delete lands, in any order.
fn create_shadowed_then_deleted() -> Vec<Op> {
    let mut a = doc(1);
    let create = a.transact(|tx| {
        tx.map(b"k");
    });
    let shadow = a.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let delete = a.transact(|tx| tx.delete(b"k"));
    create.into_iter().chain(shadow).chain(delete).collect()
}

/// A pool of creates, deletes and leaf writes over three keys by three authors —
/// the shapes a create/delete race is drawn from, generated rather than hand-cut,
/// so the hand-cut pools below are not the only thing between this rule and a
/// regression. Every op targets the root map, so none is ever held.
fn generated_pool(seed: u64) -> Vec<Op> {
    let mut state = seed | 1;
    let mut roll = |n: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % n
    };
    const KEYS: [&[u8]; 3] = [b"a", b"b", b"c"];
    let mut authors: Vec<Document> = (1..=3).map(doc).collect();
    let mut pool = Vec::new();
    for _ in 0..12 {
        let author = roll(3) as usize;
        let key = KEYS[roll(3) as usize];
        let choice = roll(6);
        let written = authors[author].transact(|tx| match choice {
            0 => {
                tx.map(key);
            }
            1 => {
                tx.list(key);
            }
            2 => {
                tx.text(key);
            }
            3 => tx.delete(key),
            4 => tx.set(key, Scalar::Int(1)),
            _ => tx.inc(key, 1),
        });
        // The other two see the write, so later ops race it rather than stacking
        // on one author's clock.
        for (i, other) in authors.iter_mut().enumerate() {
            if i != author {
                for op in &written {
                    other.apply(op);
                }
            }
        }
        pool.extend(written);
    }
    pool
}

#[test]
fn generated_create_and_delete_pools_encode_alike_under_a_shuffle_sweep() {
    // Whole-order sweeps are affordable on a two- or three-op pool; over a dozen
    // ops they are not, so these are sampled — deterministically, so a failure
    // names a pool anyone can rerun.
    let seeds = if cfg!(miri) { 3 } else { 64 };
    let shuffles = if cfg!(miri) { 2 } else { 4 };
    for seed in 1..seeds {
        let pool = generated_pool(seed);
        let refs: Vec<&Op> = pool.iter().collect();
        let bytes = fold_applied(&refs).encode_state();
        let mut shuffle = seed.wrapping_mul(0x9e37_79b9) | 1;
        for round in 0..shuffles {
            let mut order = refs.clone();
            for i in (1..order.len()).rev() {
                shuffle ^= shuffle << 13;
                shuffle ^= shuffle >> 7;
                shuffle ^= shuffle << 17;
                order.swap(i, (shuffle % (i as u64 + 1)) as usize);
            }
            assert_eq!(
                fold_applied(&order).encode_state(),
                bytes,
                "pool {seed} shuffle {round} encodes different bytes"
            );
        }
    }
}

#[test]
fn a_create_racing_a_delete_of_its_key_encodes_the_same_bytes_in_either_order() {
    // The delete-first order records the container whose create it outranks, so
    // both orders leave the same deleted-container tombstone. Nothing about the
    // reading distinguishes them, which is why this is pinned on the bytes.
    every_order_encodes_alike(&create_and_delete(), &[b"k"]);
}

#[test]
fn two_creates_of_different_kinds_under_one_delete_encode_the_same_bytes_in_every_order() {
    // Which of the two the tombstone retains is settled by the creates' stamps,
    // not by which of them the fold happened to see first.
    every_order_encodes_alike(&two_kinds_and_a_delete(), &[b"k"]);
}

/// A map create, an XML create that takes the key from it, and a delete over
/// both — the XML create outranks a key-derived one it is not interchangeable
/// with, so the rank has to see it.
fn an_xml_create_over_a_map_create() -> Vec<Op> {
    let mut a = doc(1);
    let map = a.transact(|tx| {
        tx.map(b"k");
    });
    let xml = a.transact(|tx| {
        tx.xml_element(b"k", b"p");
    });
    let delete = a.transact(|tx| tx.delete(b"k"));
    map.into_iter().chain(xml).chain(delete).collect()
}

#[test]
fn an_xml_create_over_a_map_create_encodes_the_same_bytes_in_every_order() {
    every_order_encodes_alike(&an_xml_create_over_a_map_create(), &[b"k"]);
}

#[test]
fn a_create_shadowed_by_a_leaf_and_then_deleted_encodes_the_same_bytes_in_every_order() {
    // The key retains the create across the scalar that outranks it, so a fold
    // that never had the container live at the delete records it all the same.
    every_order_encodes_alike(&create_shadowed_then_deleted(), &[b"k"]);
}

#[test]
fn a_create_shadowed_by_a_leaf_round_trips_its_retained_create() {
    // With no delete in the pool the retained create rides on a *live* leaf slot,
    // which is the shape that has to survive an encode/decode round trip. Both
    // orders reach it, and the round trip is what pins that the slot spells the
    // create out at all.
    let mut a = doc(1);
    let create = a.transact(|tx| {
        tx.map(b"k");
    });
    let shadow = a.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let pool: Vec<Op> = create.into_iter().chain(shadow).collect();
    every_order_encodes_alike(&pool, &[b"k"]);
    let bytes = fold(&pool.iter().collect::<Vec<_>>()).encode_state();
    let back = Document::decode_state(&bytes).expect("decode");
    assert_eq!(
        back.encode_state(),
        bytes,
        "the retained create round-trips"
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
        d.seen().any(|id| id == creates[1].id),
        "applied, not held: a held op is out of the causal frontier"
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
    // A snapshot handed over holding its ops the other way round re-encodes into
    // the canonical order, so the bytes a replica serves are a function of the ops
    // it holds and not of the bytes it was handed.
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
        "a disordered buffer was re-served in the order it arrived"
    );
}
