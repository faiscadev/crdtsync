//! Per-zone lamport clocks — the causally-independent partitions each zone
//! replicates as.
//!
//! A document's lamport allocation is partitioned by zone: an op is stamped from
//! the clock of the zone its target resolves to, so an edit in one zone never
//! advances another's. Two zones' ops are therefore concurrent by construction —
//! no false causal edge crosses the boundary — while causal order within a zone
//! is intact. With no schema, or a schema with no zones, every op is in the one
//! root partition, exactly as before zones. Convergence holds over both the
//! observable state and the per-zone clocks, and a snapshot round-trips them.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::schema::Schema;
use crdtsync_core::{path, ClientId, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A schema declaring `zones` (name → root path), permissive on structure so any
/// path is writable — only the zone block matters to partitioning.
fn schema_with_zones(zones: &str) -> Schema {
    let src = format!(
        r#"{{ "schema": "s", "version": 1, "root": "R",
             "types": {{ "R": {{ "kind": "map" }} }},
             "zones": {zones} }}"#
    );
    Schema::parse(&src).expect("schema parses")
}

fn p(segs: &[&[u8]]) -> Vec<u8> {
    path::encode_path(segs)
}

/// The lamports of the ops in emission order.
fn lamports(ops: &[Op]) -> Vec<u64> {
    ops.iter().map(|o| o.stamp.lamport).collect()
}

#[test]
fn an_op_targeting_a_zoned_subtree_stamps_from_that_zones_clock() {
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zones(r#"{ "board": "/board" }"#));

    // First write: the create of the `board` map targets the root map (partition
    // None), the set inside it targets `board` (zone 0). Under one global clock
    // the set would be lamport 2; per-zone, zone 0 is fresh so it is lamport 1.
    let ops = path::register_int(&mut doc, &p(&[b"board", b"x"]), 1);
    assert_eq!(ops.len(), 2, "a create then a set");
    assert_eq!(ops[0].zone, None, "the create targets the root map");
    assert_eq!(ops[0].stamp.lamport, 1);
    assert_eq!(ops[1].zone, Some(0), "the set targets the zoned subtree");
    assert_eq!(ops[1].stamp.lamport, 1, "stamped from zone 0's own clock");

    assert_eq!(doc.zone_clock(None), 1);
    assert_eq!(doc.zone_clock(Some(0)), 1);
}

#[test]
fn an_op_in_one_zone_does_not_advance_another_zones_clock() {
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zones(r#"{ "a": "/a", "b": "/b" }"#));

    // Materialise both zone roots (these creates ride the root partition).
    path::register_int(&mut doc, &p(&[b"a", b"seed"]), 0);
    path::register_int(&mut doc, &p(&[b"b", b"seed"]), 0);

    // Five edits inside zone a.
    for i in 0..5 {
        path::register_int(&mut doc, &p(&[b"a", b"x"]), i);
    }
    assert_eq!(doc.zone_clock(Some(0)), 6, "seed + five edits");

    // One edit inside zone b — its stamp reflects zone b's own (near-fresh) clock,
    // never the six ops zone a has accrued. (The create-through of `b` rides the
    // root partition; the set inside it is the zone b op.)
    let ops = path::register_int(&mut doc, &p(&[b"b", b"x"]), 9);
    let set = ops.iter().find(|o| o.zone == Some(1)).expect("a zone b op");
    assert_eq!(set.stamp.lamport, 2, "zone b: seed=1, this=2");
    assert_eq!(doc.zone_clock(Some(1)), 2);
    assert_eq!(
        doc.zone_clock(Some(0)),
        6,
        "zone a untouched by a zone b edit"
    );
}

#[test]
fn two_zones_ops_are_concurrent_by_construction() {
    // Two replicas, same schema. Each edits a different zone without ever seeing
    // the other's op. Neither op's clock reflects the other — the counters are
    // independent, so there is no happens-before either direction.
    let schema = schema_with_zones(r#"{ "a": "/a", "b": "/b" }"#);
    let mut left = Document::new(cid(1));
    left.set_schema(schema.clone());
    let mut right = Document::new(cid(2));
    right.set_schema(schema);

    let a = left.transact(|tx| tx.child(b"a").register(b"x", Scalar::Int(1)));
    let b = right.transact(|tx| tx.child(b"b").register(b"y", Scalar::Int(2)));

    // The zone-carrying (non-create) op of each side.
    let a_set = a.iter().find(|o| o.zone == Some(0)).expect("zone a set");
    let b_set = b.iter().find(|o| o.zone == Some(1)).expect("zone b set");
    // Both minted lamport 1 from their own fresh zone clock — same number, disjoint
    // partitions, no ordering implied.
    assert_eq!(a_set.stamp.lamport, 1);
    assert_eq!(b_set.stamp.lamport, 1);

    // Cross-apply: each honors the other's zone from the envelope, advancing only
    // that partition.
    for o in &b {
        left.apply(o);
    }
    assert_eq!(left.zone_clock(Some(1)), 1, "zone b advanced by the merge");
    assert_eq!(
        left.zone_clock(Some(0)),
        1,
        "zone a untouched by a zone b op"
    );
}

#[test]
fn causal_order_within_a_zone_is_intact() {
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zones(r#"{ "z": "/z" }"#));
    path::register_int(&mut doc, &p(&[b"z", b"seed"]), 0);

    // Successive edits in the same zone strictly increase its lamport — a happens
    // before b in zone z ⇒ b's z-lamport > a's. (The create-through rides the root
    // partition; the set inside the zone is the zone z op.)
    let mut last = doc.zone_clock(Some(0));
    for i in 0..8 {
        let ops = path::register_int(&mut doc, &p(&[b"z", b"x"]), i);
        let l = ops
            .iter()
            .find(|o| o.zone == Some(0))
            .expect("a zone z op")
            .stamp
            .lamport;
        assert!(l > last, "within-zone lamport advances: {l} > {last}");
        last = l;
    }
}

#[test]
fn an_unzoned_target_rides_the_root_partition() {
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zones(r#"{ "z": "/z" }"#));

    // A target outside every zone root carries no zone and stamps from the root
    // clock — indistinguishable from a document with no zones.
    let ops = path::register_int(&mut doc, &p(&[b"loose"]), 7);
    assert_eq!(ops[0].zone, None);
    assert_eq!(doc.zone_clock(None), 1);
    assert_eq!(doc.zone_clock(Some(0)), 0, "the zone was never touched");
}

#[test]
fn merging_a_remote_op_advances_only_its_zone_clock() {
    let schema = schema_with_zones(r#"{ "a": "/a", "b": "/b" }"#);
    let mut author = Document::new(cid(1));
    author.set_schema(schema.clone());
    // Build up zone a so its clock is well past 1.
    path::register_int(&mut author, &p(&[b"a", b"seed"]), 0);
    for i in 0..4 {
        path::register_int(&mut author, &p(&[b"a", b"x"]), i);
    }
    // One more zone a edit, to be merged into a peer that already has the history.
    let pool: Vec<Op> = author.transact(|tx| tx.child(b"a").register(b"x", Scalar::Int(99)));

    // The peer learns the earlier ops via a snapshot, then binds the schema, so the
    // merge's target is reachable.
    let snapshot = author.encode_state();
    let mut peer = Document::decode_state_as(cid(2), 0, &snapshot).expect("decodes");
    peer.set_schema(schema);
    let before_root = peer.zone_clock(None);
    let before_a = peer.zone_clock(Some(0));
    for o in &pool {
        peer.apply(o);
    }
    assert!(
        peer.zone_clock(Some(0)) >= before_a,
        "zone a advanced or held"
    );
    assert_eq!(
        peer.zone_clock(None),
        before_root,
        "the root partition is untouched by a zone a op"
    );
}

#[test]
fn zoned_xml_and_list_edits_resolve_through_cursor_paths_without_panicking() {
    // Zone resolution walks the live tree (`element_paths`) inside `emit`, taking a
    // shared borrow of every container. An emit through an XML / list cursor must
    // therefore have dropped its own container borrow before emitting — this
    // exercises those paths with a zone bound (the plain xml/list suites bind no
    // schema, so they never reach the walk). A panic here would be a double borrow.
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zones(r#"{ "z": "/z" }"#));

    // An XML element inside the zone, then element and text children into it.
    path::xml_element(&mut doc, &p(&[b"z", b"root"]), b"div");
    let e = path::xml_insert_element(&mut doc, &p(&[b"z", b"root"]), 0, b"span");
    let t = path::xml_insert_text(&mut doc, &p(&[b"z", b"root"]), 1, "hi");
    // A list inside the zone, then an item.
    let li = path::list_insert(&mut doc, &p(&[b"z", b"items"]), 0, b"first");

    // Every edit whose target lives inside the zone carries zone 0.
    for ops in [&e, &t, &li] {
        assert!(
            ops.iter().any(|o| o.zone == Some(0)),
            "a zoned edit carries zone 0"
        );
    }
    assert_eq!(path::xml_children_len(&doc, &p(&[b"z", b"root"])), Some(2));
}

#[test]
fn replicas_converge_on_state_and_per_zone_clocks() {
    // A randomized multi-zone op set applied in two different orders converges to
    // identical per-zone clocks and identical observable state.
    let schema = schema_with_zones(r#"{ "a": "/a", "b": "/b", "c": "/c" }"#);

    let mut src = Document::new(cid(1));
    src.set_schema(schema.clone());
    let mut pool: Vec<Op> = Vec::new();
    let zones: [&[u8]; 3] = [b"a", b"b", b"c"];
    // A tiny deterministic sequence of edits spread across the three zones and the
    // unzoned region.
    let mut n: u64 = 0;
    for round in 0..12u64 {
        let z = zones[(round as usize) % 3];
        pool.extend(path::register_int(&mut src, &p(&[z, b"x"]), round as i64));
        pool.extend(path::register_int(&mut src, &p(&[b"loose"]), round as i64));
        n += 2;
    }
    assert!(pool.len() as u64 >= n);

    let replay = |order: &[usize]| -> Document {
        let mut d = Document::new(cid(9));
        d.set_schema(schema.clone());
        // Repeat to a fixpoint so buffered (out-of-order) ops all land.
        loop {
            let mut progressed = false;
            for &i in order {
                if d.apply(&pool[i]) {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        d
    };

    let forward: Vec<usize> = (0..pool.len()).collect();
    let reverse: Vec<usize> = (0..pool.len()).rev().collect();
    let a = replay(&forward);
    let b = replay(&reverse);

    for zone in [None, Some(0), Some(1), Some(2)] {
        assert_eq!(
            a.zone_clock(zone),
            b.zone_clock(zone),
            "clocks converge for zone {zone:?}"
        );
        // And they match the source that emitted the ops.
        assert_eq!(a.zone_clock(zone), src.zone_clock(zone));
    }
    // Observable state converges too.
    for z in [b"a".as_slice(), b"b", b"c"] {
        assert_eq!(
            path::get_int(&a, &p(&[z, b"x"])),
            path::get_int(&b, &p(&[z, b"x"]))
        );
    }
    assert_eq!(
        path::get_int(&a, &p(&[b"loose"])),
        path::get_int(&b, &p(&[b"loose"]))
    );
}

#[test]
fn a_snapshot_round_trips_the_per_zone_clocks() {
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zones(r#"{ "a": "/a", "b": "/b" }"#));
    path::register_int(&mut doc, &p(&[b"a", b"x"]), 1);
    path::register_int(&mut doc, &p(&[b"a", b"y"]), 2);
    path::register_int(&mut doc, &p(&[b"b", b"x"]), 3);
    path::register_int(&mut doc, &p(&[b"loose"]), 4);

    let bytes = doc.encode_state();
    let back = Document::decode_state(&bytes).expect("decodes");
    assert_eq!(back.zone_clock(None), doc.zone_clock(None));
    assert_eq!(back.zone_clock(Some(0)), doc.zone_clock(Some(0)));
    assert_eq!(back.zone_clock(Some(1)), doc.zone_clock(Some(1)));
    // Canonical: a re-encode of the decoded snapshot is byte-stable.
    assert_eq!(back.encode_state(), bytes);
}

#[test]
fn a_document_with_no_zones_behaves_as_a_single_global_clock() {
    // No schema bound: every op is in the root partition, the lamport advancing as
    // one global clock exactly as before zones existed.
    let mut doc = Document::new(cid(1));
    let mut all: Vec<Op> = Vec::new();
    all.extend(path::register_int(&mut doc, &p(&[b"a", b"x"]), 1));
    all.extend(path::register_int(&mut doc, &p(&[b"b", b"y"]), 2));
    all.extend(path::register_int(&mut doc, &p(&[b"a", b"x"]), 3));
    for o in &all {
        assert_eq!(o.zone, None, "no schema ⇒ every op is unzoned");
    }
    // The lamports are a single strictly-increasing global sequence.
    let ls = lamports(&all);
    for w in ls.windows(2) {
        assert!(w[1] > w[0], "one global clock: {ls:?}");
    }
    assert_eq!(doc.zone_clock(None), *ls.last().unwrap());

    // A bound schema that declares no zones is the same: still one partition.
    let mut zoned = Document::new(cid(2));
    zoned.set_schema(schema_with_zones(r#"{}"#));
    let ops = path::register_int(&mut zoned, &p(&[b"a", b"x"]), 1);
    assert!(ops.iter().all(|o| o.zone.is_none()));
}
