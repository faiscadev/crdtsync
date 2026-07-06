//! RangedElement composite payload (XmlElement Unit 3b).
//!
//! A `RangedElement`'s payload generalizes from a leaf `Scalar` (3a) to a nested
//! container — a Map / List / Text — installed at an id derived from the range id
//! and edited through the ordinary container ops. A structured comment body
//! `{author, text}` or an object-flavored mark value is a first-class CRDT. The
//! payload composite is reachable through its range: a delete hides it (delete
//! wins), and an op targeting it buffers until the range's create materialises it.

use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::{ClientId, Element, Op, RangedPayload, Scalar};

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

fn text_id(d: &Document, key: &[u8]) -> ElementId {
    ElementId::derive(d.root_id(), key, ElementKind::Text)
}

fn build_text(d: &mut Document, key: &[u8], s: &str) -> Vec<Op> {
    d.transact(|tx| {
        let mut t = tx.text(key);
        t.insert(0, s);
    })
}

fn at(seq: ElementId, pos: RelativePosition) -> RangeAnchor {
    RangeAnchor { seq, pos }
}

/// The id a composite Map payload derives to under a RangedElement id.
fn map_payload_id(ranged: ElementId) -> ElementId {
    ElementId::derive(ranged, b"payload", ElementKind::Map)
}

/// Read a scalar slot from a RangedElement's Map payload — `Element` is neither
/// `Eq` nor `Debug`, so read through to the comparable `Scalar`.
fn payload_scalar(d: &Document, ranged: ElementId, key: &[u8]) -> Option<Scalar> {
    match d.ranged_payload(ranged) {
        Some(Element::Map(m)) => match m.borrow().get(key) {
            Some(Element::Scalar(s)) => Some(s),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn a_map_payload_records_and_reads_back() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "hello");
    let seq = text_id(&d, b"t");

    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        let mut rc = tx.ranged();
        rid = rc.create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        let mut m = rc.payload_map(rid).expect("live map payload");
        m.set(b"author", Scalar::Int(7));
        m.set(b"pinned", Scalar::Bool(true));
    });

    // The view reports a composite payload at the derived id; no scalar reading.
    let r = d.ranged_element(rid).expect("live range");
    assert_eq!(
        r.payload,
        RangedPayload::Composite {
            id: map_payload_id(rid),
            kind: ElementKind::Map,
        }
    );
    assert!(r.scalar().is_none(), "a composite payload has no scalar");

    // The payload's slots read back through the composite handle.
    assert_eq!(payload_scalar(&d, rid, b"author"), Some(Scalar::Int(7)));
    assert_eq!(payload_scalar(&d, rid, b"pinned"), Some(Scalar::Bool(true)));
}

#[test]
fn a_payload_composite_has_a_stable_derived_id_across_replicas() {
    // Two replicas that see the same composite create derive the same payload id
    // and materialise the same payload container.
    let mut base = Document::new(cid(1));
    let build = build_text(&mut base, b"t", "x");
    let seq = text_id(&base, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = base.transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
    });

    let mut peer = Document::new(cid(2));
    apply_all(&mut peer, &build);
    apply_all(&mut peer, &create);

    let RangedPayload::Composite { id, kind } = peer.ranged_element(rid).unwrap().payload else {
        panic!("payload is composite");
    };
    assert_eq!(id, map_payload_id(rid), "payload id derives identically");
    assert_eq!(kind, ElementKind::Map);
    assert!(
        peer.ranged_payload(rid).is_some(),
        "payload container materialised"
    );
}

#[test]
fn concurrent_edits_to_a_map_payload_merge() {
    let mut base = Document::new(cid(1));
    let build = build_text(&mut base, b"t", "x");
    let seq = text_id(&base, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = base.transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    for r in [&mut r1, &mut r2] {
        apply_all(r, &build);
        apply_all(r, &create);
    }

    // Different keys — union — and a shared key — LWW by stamp (r2 = cid 3 wins).
    let p1 = r1.transact(|tx| {
        let mut rc = tx.ranged();
        let mut m = rc.payload_map(rid).unwrap();
        m.set(b"a", Scalar::Int(1));
        m.set(b"shared", Scalar::Int(10));
    });
    let p2 = r2.transact(|tx| {
        let mut rc = tx.ranged();
        let mut m = rc.payload_map(rid).unwrap();
        m.set(b"b", Scalar::Int(2));
        m.set(b"shared", Scalar::Int(20));
    });
    apply_all(&mut r1, &p2);
    apply_all(&mut r2, &p1);

    for r in [&r1, &r2] {
        assert_eq!(payload_scalar(r, rid, b"a"), Some(Scalar::Int(1)));
        assert_eq!(payload_scalar(r, rid, b"b"), Some(Scalar::Int(2)));
        assert_eq!(
            payload_scalar(r, rid, b"shared"),
            Some(Scalar::Int(20)),
            "LWW tiebreak by client id",
        );
    }
}

#[test]
fn a_scalar_payload_is_unaffected() {
    // The 3a leaf-payload path is intact: a scalar create has no composite handle,
    // and its payload reads as a scalar.
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "x");
    let seq = text_id(&d, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(5),
        );
    });

    assert_eq!(
        d.ranged_element(rid).unwrap().scalar(),
        Some(&Scalar::Int(5))
    );
    assert!(
        d.ranged_payload(rid).is_none(),
        "a scalar payload has no composite handle"
    );
    // A Map cursor over a scalar-payload range is refused.
    d.transact(|tx| assert!(tx.ranged().payload_map(rid).is_none()));
}

#[test]
fn a_list_and_a_text_payload_work() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "x");
    let seq = text_id(&d, b"t");
    let (mut lid, mut tid) = (
        ElementId::from_bytes([0u8; 16]),
        ElementId::from_bytes([0u8; 16]),
    );
    d.transact(|tx| {
        lid = tx.ranged().create_list(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        tx.ranged()
            .payload_list(lid)
            .unwrap()
            .insert(0, Scalar::Int(9));

        tid = tx.ranged().create_text(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        tx.ranged().payload_text(tid).unwrap().insert(0, "hi");
    });

    match d.ranged_payload(lid) {
        Some(Element::List(l)) => {
            assert!(matches!(
                l.borrow().get(0),
                Some(Element::Scalar(Scalar::Int(9)))
            ))
        }
        _ => panic!("expected a list payload"),
    }
    match d.ranged_payload(tid) {
        Some(Element::Text(t)) => assert_eq!(t.borrow().as_string(), "hi"),
        _ => panic!("expected a text payload"),
    }
}

#[test]
fn a_delete_hides_a_composite_payload() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "x");
    let seq = text_id(&d, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        tx.ranged()
            .payload_map(rid)
            .unwrap()
            .set(b"a", Scalar::Int(1));
    });
    d.transact(|tx| tx.ranged().delete(rid));

    assert!(d.ranged_element(rid).is_none(), "range hidden");
    assert!(
        d.ranged_payload(rid).is_none(),
        "payload hidden with its range"
    );
    // A payload cursor over a deleted range is refused.
    d.transact(|tx| assert!(tx.ranged().payload_map(rid).is_none()));
}

#[test]
fn a_delete_wins_over_a_concurrent_payload_edit() {
    let mut base = Document::new(cid(1));
    let build = build_text(&mut base, b"t", "x");
    let seq = text_id(&base, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = base.transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    for r in [&mut r1, &mut r2] {
        apply_all(r, &build);
        apply_all(r, &create);
    }

    let del = r1.transact(|tx| tx.ranged().delete(rid));
    let edit = r2.transact(|tx| {
        tx.ranged()
            .payload_map(rid)
            .unwrap()
            .set(b"a", Scalar::Int(1))
    });
    // r1 receives the edit after deleting the range: it must apply to the retained
    // (now hidden) payload, NOT buffer forever — a permanent buffer would leak and
    // desync r1's snapshot from r2's. `apply` returns true only when applied now.
    for op in &edit {
        assert!(
            r1.apply(op),
            "a payload edit racing a delete applies to the hidden payload, not buffered",
        );
    }
    apply_all(&mut r2, &del);

    // Observable state converges: both hide the range and its payload.
    for r in [&r1, &r2] {
        assert!(r.ranged_element(rid).is_none(), "delete wins");
        assert!(r.ranged_payload(rid).is_none());
        assert_eq!(r.ranged_elements().len(), 0);
    }
}

#[test]
fn set_payload_on_a_composite_range_emits_nothing() {
    // set_payload targets a scalar payload; a composite is edited through its
    // container. A set against a composite range must emit no op — an emitted
    // RangedSetPayload would be silently inert on every replica (a lost write).
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "x");
    let seq = text_id(&d, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
    });

    let ops = d.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(1)));
    assert!(
        ops.is_empty(),
        "set_payload on a composite range emits nothing"
    );
    // The composite payload is untouched and still addressable.
    assert!(d.ranged_payload(rid).is_some());
}

#[test]
fn a_payload_edit_buffers_until_its_create() {
    // A container op targeting the payload arriving before the composite create
    // must buffer until the create materialises the payload, then apply.
    let mut src = Document::new(cid(1));
    let build = build_text(&mut src, b"t", "x");
    let seq = text_id(&src, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let create = src.transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
    });
    let edit = src.transact(|tx| {
        tx.ranged()
            .payload_map(rid)
            .unwrap()
            .set(b"a", Scalar::Int(1))
    });

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &edit); // edit first — the payload container is absent
    assert!(
        dst.ranged_payload(rid).is_none(),
        "buffered until the create arrives"
    );
    apply_all(&mut dst, &create); // create lands; the buffered edit replays after it
    assert_eq!(
        payload_scalar(&dst, rid, b"a"),
        Some(Scalar::Int(1)),
        "the buffered payload edit is not lost",
    );
}

#[test]
fn an_atomic_create_and_edit_commit_together() {
    // A composite create and an edit of its payload shipped as one atomic
    // transaction commit together on a replica that receives them out of order.
    let mut src = Document::new(cid(1));
    let build = build_text(&mut src, b"t", "x");
    let seq = text_id(&src, b"t");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    let group = src.atomic_transact(|tx| {
        rid = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        tx.ranged()
            .payload_map(rid)
            .unwrap()
            .set(b"a", Scalar::Int(1));
    });

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    // Deliver the group in reverse: the edit before its create. The group holds
    // until every member is present, then commits atomically.
    let reversed: Vec<Op> = group.iter().rev().cloned().collect();
    apply_all(&mut dst, &reversed);
    assert_eq!(
        payload_scalar(&dst, rid, b"a"),
        Some(Scalar::Int(1)),
        "the atomic composite payload commits whole",
    );
}

#[test]
fn a_snapshot_round_trips_a_composite_payload() {
    let mut d = Document::new(cid(1));
    build_text(&mut d, b"t", "hello");
    let seq = text_id(&d, b"t");

    let (mut keep, mut gone) = (
        ElementId::from_bytes([0u8; 16]),
        ElementId::from_bytes([0u8; 16]),
    );
    d.transact(|tx| {
        {
            let mut rc = tx.ranged();
            keep = rc.create_map(
                at(seq, RelativePosition::Start),
                at(seq, RelativePosition::End),
            );
            let mut m = rc.payload_map(keep).unwrap();
            m.set(b"author", Scalar::Int(7));
            m.map(b"meta").set(b"pinned", Scalar::Bool(true));
        }
        // A composite-payload range that is then deleted — its payload must decode
        // hidden, not resurface.
        {
            let mut rc = tx.ranged();
            gone = rc.create_map(
                at(seq, RelativePosition::Start),
                at(seq, RelativePosition::End),
            );
            rc.payload_map(gone).unwrap().set(b"x", Scalar::Int(9));
        }
    });
    d.transact(|tx| tx.ranged().delete(gone));

    let bytes = d.encode_state();
    let restored = Document::decode_state(&bytes).unwrap();

    assert_eq!(restored.ranged_element(keep), d.ranged_element(keep));
    assert_eq!(
        payload_scalar(&restored, keep, b"author"),
        Some(Scalar::Int(7)),
        "composite payload survives the reload",
    );
    // The nested Map inside the payload survives too.
    match restored.ranged_payload(keep) {
        Some(Element::Map(m)) => match m.borrow().get(b"meta") {
            Some(Element::Map(meta)) => {
                assert!(matches!(
                    meta.borrow().get(b"pinned"),
                    Some(Element::Scalar(Scalar::Bool(true)))
                ))
            }
            _ => panic!("nested map lost"),
        },
        _ => panic!("payload lost"),
    }
    assert!(
        restored.ranged_element(gone).is_none(),
        "deleted stays deleted"
    );
    assert!(restored.ranged_payload(gone).is_none());
    assert_eq!(restored.ranged_elements().len(), 1);
    assert_eq!(restored.encode_state(), bytes, "re-encode diverged");
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

/// A deterministic reading of the annotation set and every Map payload's slots —
/// the convergence oracle.
fn fingerprint(d: &Document) -> String {
    d.ranged_elements()
        .iter()
        .map(|r| {
            let payload = match &r.payload {
                RangedPayload::Scalar(s) => format!("S{s:?}"),
                RangedPayload::Composite { .. } => {
                    let mut slots: Vec<String> = [b"a".as_ref(), b"b", b"c"]
                        .iter()
                        .filter_map(|k| {
                            payload_scalar(d, r.id, k)
                                .map(|s| format!("{}={s:?}", String::from_utf8_lossy(k)))
                        })
                        .collect();
                    slots.sort();
                    format!("M[{}]", slots.join(","))
                }
            };
            format!("{:?}:{}", r.id.as_bytes(), payload)
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[test]
fn random_orderings_converge_with_composite_payloads() {
    // A mixed script — scalar and composite creates, payload edits, and a delete —
    // authored across three replicas and delivered shuffled to a fourth. Every
    // ordering lands on the same annotation set and payload contents.
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

    let mut ids = [ElementId::from_bytes([0u8; 16]); 3];
    // Phase 1 — each replica creates one range: r0 a Map payload, r1 a scalar, r2 a
    // Map payload. Then every replica sees every create, so a cross-mutation below
    // is a real op (a local change to a range this replica has not seen emits
    // nothing — the divergence guard).
    let mut creates: Vec<Op> = Vec::new();
    creates.extend(r[0].transact(|tx| {
        ids[0] = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        tx.ranged()
            .payload_map(ids[0])
            .unwrap()
            .set(b"a", Scalar::Int(1));
    }));
    creates.extend(r[1].transact(|tx| {
        ids[1] = tx.ranged().create(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
            Scalar::Int(0),
        );
    }));
    creates.extend(r[2].transact(|tx| {
        ids[2] = tx.ranged().create_map(
            at(seq, RelativePosition::Start),
            at(seq, RelativePosition::End),
        );
        tx.ranged()
            .payload_map(ids[2])
            .unwrap()
            .set(b"b", Scalar::Int(2));
    }));
    for d in r.iter_mut() {
        apply_all(d, &creates); // idempotent on a replica's own create
    }

    // Phase 2 — cross-mutations: r0 edits id2's payload, r1 deletes id0, r2 repays
    // the scalar id1. Bundled after the creates; a shuffled delivery may land one
    // before its create, so it buffers until the create arrives.
    let mut muts: Vec<Op> = Vec::new();
    muts.extend(r[0].transact(|tx| {
        tx.ranged()
            .payload_map(ids[2])
            .unwrap()
            .set(b"c", Scalar::Int(3))
    }));
    muts.extend(r[1].transact(|tx| tx.ranged().delete(ids[0])));
    muts.extend(r[2].transact(|tx| tx.ranged().set_payload(ids[1], Scalar::Int(9))));

    let ops: Vec<Op> = creates.iter().chain(muts.iter()).cloned().collect();

    let mut reference = Document::new(cid(9));
    apply_all(&mut reference, &build);
    apply_all(&mut reference, &ops);
    let expect = fingerprint(&reference);

    for seed in 0..64u64 {
        let mut shuffled = ops.clone();
        let mut rng = Rng::new(seed);
        for i in (1..shuffled.len()).rev() {
            let j = (rng.next() as usize) % (i + 1);
            shuffled.swap(i, j);
        }
        let mut d = Document::new(cid(10));
        apply_all(&mut d, &build);
        apply_all(&mut d, &shuffled);
        assert_eq!(fingerprint(&d), expect, "seed {seed} diverged");
    }
}
