//! RangedElement — the document-level annotation set (XmlElement Unit 3a).
//!
//! A `RangedElement` is a generic ranged annotation held in a document-level CRDT
//! set keyed by its own id: two anchors `(seq, RelativePosition)` — which may name
//! different sequences, so a range can span elements — plus an LWW `Scalar`
//! payload. The set is the source of truth; "the ranges on this Text" is a query
//! over it. Semantics: concurrent creates union to distinct ids, payload is
//! LWW-by-stamp, delete wins over a concurrent payload change.

use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::{ClientId, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

/// The id a Text under root slot `key` derives to — the sequence an anchor names.
fn text_id(d: &Document, key: &[u8]) -> ElementId {
    ElementId::derive(d.root_id(), key, ElementKind::Text)
}

/// Build a Text at root slot `key` holding `s`.
fn build_text(d: &mut Document, key: &[u8], s: &str) -> Vec<Op> {
    d.transact(|tx| {
        let mut t = tx.text(key);
        t.insert(0, s);
    })
}

fn at(seq: ElementId, pos: RelativePosition) -> RangeAnchor {
    RangeAnchor { seq, pos }
}

#[test]
fn a_ranged_element_records_its_endpoints_and_payload() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "hello");
    let seq = text_id(&d, b"t");

    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Bool(true),
        );
    });

    let r = d.ranged_element(rid).expect("ranged element present");
    assert_eq!(r.id, rid);
    assert_eq!(r.start, at(seq, RelativePosition::Start));
    assert_eq!(r.end, at(seq, RelativePosition::End));
    assert_eq!(r.scalar(), Some(&Scalar::Bool(true)));
    assert_eq!(d.ranged_elements().len(), 1);
}

#[test]
fn a_range_may_span_two_elements() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"a", "one");
    build_text(&mut d, b"b", "two");
    let a = text_id(&d, b"a");
    let b = text_id(&d, b"b");

    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create(
            at(a, RelativePosition::Start),
            at(b, RelativePosition::End),
            Scalar::Null,
        );
    });

    let r = d.ranged_element(rid).unwrap();
    assert_ne!(r.start.seq, r.end.seq, "the range spans two sequences");
    assert_eq!(r.start.seq, a);
    assert_eq!(r.end.seq, b);
}

#[test]
fn a_payload_change_is_last_writer_wins() {
    let mut base = Document::new(cid(1));
    let build = build_text(&mut base, b"t", "x");
    let seq = text_id(&base, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = base.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(0),
        );
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    for r in [&mut r1, &mut r2] {
        apply_all(r, &build);
        apply_all(r, &create);
    }

    // Concurrent payload writes off identical history: equal lamports, so the
    // higher client id (r2 = cid 3) wins the LWW tiebreak.
    let p1 = r1.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(1)));
    let p2 = r2.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(2)));
    apply_all(&mut r1, &p2);
    apply_all(&mut r2, &p1);

    assert_eq!(
        r1.ranged_element(rid).unwrap().payload,
        r2.ranged_element(rid).unwrap().payload,
        "replicas converge on one payload",
    );
    assert_eq!(
        r1.ranged_element(rid).unwrap().scalar(),
        Some(&Scalar::Int(2))
    );
}

#[test]
fn concurrent_creates_union_to_distinct_ids() {
    let mut base = Document::new(cid(1));
    let build = build_text(&mut base, b"t", "x");
    let seq = text_id(&base, b"t");

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    apply_all(&mut r1, &build);
    apply_all(&mut r2, &build);

    let mut a = ElementId::from_bytes([0u8; 16]);
    let mut b = ElementId::from_bytes([0u8; 16]);
    let c1 = r1.transact(|tx| {
        a = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(1),
        );
    });
    let c2 = r2.transact(|tx| {
        b = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(2),
        );
    });
    apply_all(&mut r1, &c2);
    apply_all(&mut r2, &c1);

    assert_ne!(a, b, "concurrent creates get distinct ids");
    assert_eq!(r1.ranged_elements().len(), 2);
    assert_eq!(r2.ranged_elements().len(), 2);
    assert!(r1.ranged_element(a).is_some() && r1.ranged_element(b).is_some());
    assert!(r2.ranged_element(a).is_some() && r2.ranged_element(b).is_some());
}

#[test]
fn a_delete_wins_over_a_concurrent_payload_change() {
    let mut base = Document::new(cid(1));
    let build = build_text(&mut base, b"t", "x");
    let seq = text_id(&base, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = base.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(0),
        );
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    for r in [&mut r1, &mut r2] {
        apply_all(r, &build);
        apply_all(r, &create);
    }

    let del = r1.transact(|tx| tx.ranged().delete(rid));
    let setp = r2.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(9)));
    apply_all(&mut r1, &setp);
    apply_all(&mut r2, &del);

    assert!(r1.ranged_element(rid).is_none(), "deleted on r1");
    assert!(r2.ranged_element(rid).is_none(), "delete wins on r2");
    assert_eq!(r1.ranged_elements().len(), 0);
    assert_eq!(r2.ranged_elements().len(), 0);
}

#[test]
fn a_ranged_delete_applied_twice_is_idempotent() {
    // OpId dedup makes a re-delivered delete a no-op, but pin it per-op: applying
    // the same RangedDelete twice to one replica leaves the set exactly as one
    // delete did — no resurrection, no second tombstone effect.
    let mut src = Document::new(cid(1));
    let build = build_text(&mut src, b"t", "x");
    let seq = text_id(&src, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = src.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(0),
        );
    });
    let del = src.transact(|tx| tx.ranged().delete(rid));

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &create);
    apply_all(&mut dst, &del);
    let after_one = dst.encode_state();
    assert!(dst.ranged_element(rid).is_none(), "deleted");

    apply_all(&mut dst, &del); // re-deliver
    assert!(dst.ranged_element(rid).is_none(), "still deleted");
    assert_eq!(dst.ranged_elements().len(), 0);
    assert_eq!(dst.encode_state(), after_one, "re-delivery changed nothing");
}

#[test]
fn a_payload_change_waits_for_its_create() {
    // A payload change (or delete) that arrives before the create it depends on
    // must buffer until the create lands — applied against a missing entry it
    // would be silently lost, diverging from a replica that saw them in order.
    let mut src = Document::new(cid(1));
    let build = build_text(&mut src, b"t", "x");
    let seq = text_id(&src, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = src.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(1),
        );
    });
    let setp = src.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(2)));

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &setp); // payload change first — the ranged element is absent
    assert!(
        dst.ranged_element(rid).is_none(),
        "buffered until the create arrives"
    );
    apply_all(&mut dst, &create); // create lands; the buffered change replays after it
    assert_eq!(
        dst.ranged_element(rid).unwrap().scalar(),
        Some(&Scalar::Int(2)),
        "the buffered payload change is not lost",
    );
}

#[test]
fn a_local_change_to_an_unseen_range_emits_nothing() {
    // A payload change or delete for an id whose create this replica has not yet
    // applied must emit no op: a local apply would no-op (nothing to mutate) while
    // still broadcasting, so the author would keep the old reading while a peer
    // that applied the change against the present entry moves on — a divergence
    // that never heals.
    let mut src = Document::new(cid(1));
    let build = build_text(&mut src, b"t", "x");
    let seq = text_id(&src, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = src.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(1),
        );
    });

    // A second replica has never seen the create; a set/delete for that id
    // (obtained out of band) must produce no ops.
    let mut other = Document::new(cid(2));
    apply_all(&mut other, &build);
    let setp = other.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(2)));
    let del = other.transact(|tx| tx.ranged().delete(rid));
    assert!(setp.is_empty(), "set on an unseen range emits nothing");
    assert!(del.is_empty(), "delete on an unseen range emits nothing");

    // Once the create is applied, the change is a real op again and converges.
    apply_all(&mut other, &create);
    let setp2 = other.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(9)));
    assert!(!setp2.is_empty(), "set on a materialised range emits");
    let mut peer = Document::new(cid(3));
    apply_all(&mut peer, &build);
    apply_all(&mut peer, &create);
    apply_all(&mut peer, &setp2);
    assert_eq!(
        peer.ranged_element(rid).unwrap().payload,
        other.ranged_element(rid).unwrap().payload,
    );
}

#[test]
fn a_snapshot_round_trips_the_annotation_set() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "hello");
    let seq = text_id(&d, b"t");

    let mut keep = ElementId::from_bytes([0u8; 16]);
    let mut gone = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        keep = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(7),
        );
        gone = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(8),
        );
    });
    d.transact(|tx| tx.ranged().delete(gone));

    let bytes = d.encode_state();
    let restored = Document::decode_state(&bytes).unwrap();
    assert_eq!(restored.ranged_element(keep), d.ranged_element(keep));
    assert!(
        restored.ranged_element(gone).is_none(),
        "deleted stays deleted"
    );
    assert_eq!(restored.ranged_elements().len(), 1);
    assert_eq!(restored.encode_state(), bytes, "re-encode diverged");
}

#[test]
fn ranged_on_filters_by_sequence() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"a", "one");
    build_text(&mut d, b"b", "two");
    let a = text_id(&d, b"a");
    let b = text_id(&d, b"b");

    d.transact(|tx| {
        tx.ranged().create(
            at(a, RelativePosition::Start),
            at(a, RelativePosition::End),
            Scalar::Int(1),
        );
        // A cross-element range touches both a and b.
        tx.ranged().create(
            at(a, RelativePosition::Start),
            at(b, RelativePosition::End),
            Scalar::Int(2),
        );
        tx.ranged().create(
            at(b, RelativePosition::Start),
            at(b, RelativePosition::End),
            Scalar::Int(3),
        );
    });

    assert_eq!(d.ranged_on(a).len(), 2, "two ranges touch a");
    assert_eq!(d.ranged_on(b).len(), 2, "two ranges touch b");
}

#[test]
fn an_atomic_payload_change_waits_for_an_external_create() {
    // A payload change shipped as its own atomic transaction must not commit
    // before the create it depends on: the group readiness gate mirrors the
    // single-op one, or the group commits and the change is lost.
    let mut src = Document::new(cid(1));
    let build = build_text(&mut src, b"t", "x");
    let seq = text_id(&src, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = src.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(1),
        );
    });
    let setp = src.atomic_transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(2)));
    assert!(!setp.is_empty(), "the payload change should emit an op");

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &setp); // atomic change first — the create is external
    assert!(
        dst.ranged_element(rid).is_none(),
        "group holds until the create"
    );
    apply_all(&mut dst, &create); // create arrives; the buffered group commits
    assert_eq!(
        dst.ranged_element(rid).unwrap().scalar(),
        Some(&Scalar::Int(2)),
        "the atomic payload change is not lost",
    );
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 17
    }
}

#[test]
fn random_orderings_converge() {
    // A fixed script of ranged ops authored across three replicas, delivered in a
    // shuffled order to a fourth: every ordering converges to the same set.
    let mut author = Document::new(cid(1));
    let build = build_text(&mut author, b"t", "abcdef");
    let seq = text_id(&author, b"t");

    let mut r = [
        Document::new(cid(2)),
        Document::new(cid(3)),
        Document::new(cid(4)),
    ];
    for d in r.iter_mut() {
        apply_all(d, &build);
    }

    // Each replica creates a ranged element, then some mutate/delete.
    let mut ids = [ElementId::from_bytes([0u8; 16]); 3];
    let mut ops: Vec<Op> = Vec::new();
    for (i, d) in r.iter_mut().enumerate() {
        ops.extend(d.transact(|tx| {
            ids[i] = tx.ranged().create(
                at(seq, RelativePosition::Start),
                at(seq, RelativePosition::End),
                Scalar::Int(i as i64),
            );
        }));
    }
    // Cross-mutations: replica 0 repays id1, replica 1 deletes id2, replica 2
    // repays id0. Authored after each replica has only its own create, so they
    // buffer against the missing ranged ids until those creates arrive.
    ops.extend(r[0].transact(|tx| tx.ranged().set_payload(ids[1], Scalar::Int(100))));
    ops.extend(r[1].transact(|tx| tx.ranged().delete(ids[2])));
    ops.extend(r[2].transact(|tx| tx.ranged().set_payload(ids[0], Scalar::Int(200))));

    // A reference replica gets them in author order.
    let mut reference = Document::new(cid(9));
    apply_all(&mut reference, &build);
    apply_all(&mut reference, &ops);
    let expect: Vec<_> = reference.ranged_elements();

    let seeds: u64 = if cfg!(miri) { 8 } else { 64 };
    for seed in 0..seeds {
        let mut shuffled = ops.clone();
        let mut rng = Rng::new(seed);
        for i in (1..shuffled.len()).rev() {
            let j = (rng.next() as usize) % (i + 1);
            shuffled.swap(i, j);
        }
        let mut d = Document::new(cid(10));
        apply_all(&mut d, &build);
        apply_all(&mut d, &shuffled);
        assert_eq!(d.ranged_elements(), expect, "seed {seed} diverged");
    }
}
