//! State serialization for the whole replica — `Document::encode_state` /
//! `decode_state`.
//!
//! A snapshot captures a replica's entire merged state — every container in the
//! flat id registries, the LWW stamps, the dedup set, and any buffered
//! out-of-order ops — so a decoded document reads back the same observable
//! state AND keeps operating correctly: it dedups a replayed op, resolves LWW
//! against later writes, drains a buffered op once its parent lands, and
//! converges with a concurrent replica. The encoding is canonical.

use crdtsync_core::doc::Document;
use crdtsync_core::{DecodeError, Element, Op, OpKind, Scalar};

mod common;
use common::cid;

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const KEYS: &[&[u8]] = &[b"reg", b"cnt", b"m", b"lst", b"txt", b"body", b"frag"];
const SUBKEYS: &[&[u8]] = &[b"r", b"c"];

fn fp_element(e: &Element) -> String {
    match e {
        Element::Scalar(s) => format!("S{s:?}"),
        Element::Register(r) => format!("R{:?}", r.borrow().read()),
        Element::Counter(c) => format!("C{}", c.borrow().read()),
        Element::Map(m) => {
            let m = m.borrow();
            let parts: Vec<String> = SUBKEYS
                .iter()
                .filter_map(|sk| {
                    m.get(sk)
                        .map(|v| format!("{}={}", String::from_utf8_lossy(sk), fp_element(&v)))
                })
                .collect();
            format!("M[{}]", parts.join(","))
        }
        Element::List(l) => {
            let l = l.borrow();
            let parts: Vec<String> = (0..l.len())
                .filter_map(|i| l.get(i).map(|v| fp_element(&v)))
                .collect();
            format!("L[{}]", parts.join(","))
        }
        Element::Text(t) => format!("T{:?}", t.borrow().as_string()),
        Element::XmlElement(x) => {
            let x = x.borrow();
            format!(
                "X{:?}{{{}}}[{}]",
                x.tag(),
                fp_attrs(&x.attrs()),
                fp_seq(&x.children())
            )
        }
        Element::XmlFragment(f) => format!("F[{}]", fp_seq(&f.borrow().children())),
    }
}

fn fp_seq(children: &std::rc::Rc<std::cell::RefCell<crdtsync_core::list::List>>) -> String {
    children
        .borrow()
        .values()
        .iter()
        .map(fp_element)
        .collect::<Vec<_>>()
        .join(",")
}

fn fp_attrs(attrs: &std::rc::Rc<std::cell::RefCell<crdtsync_core::map::Map>>) -> String {
    let a = attrs.borrow();
    let mut keys = a.keys();
    keys.sort();
    keys.iter()
        .filter_map(|k| {
            a.get(k)
                .map(|v| format!("{}={}", String::from_utf8_lossy(k), fp_element(&v)))
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// A stable rendering of a document's observable state over a fixed vocabulary.
fn fingerprint(d: &Document) -> String {
    KEYS.iter()
        .map(|k| {
            let slot = d
                .get(k)
                .as_ref()
                .map_or_else(|| "_".to_string(), fp_element);
            format!("{}={}", String::from_utf8_lossy(k), slot)
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// A document exercising every element kind, nesting, and a displacement.
fn populated() -> Document {
    let mut d = doc(1);
    d.transact(|tx| {
        tx.register(b"reg", Scalar::Int(30));
        tx.inc(b"cnt", 5);
        tx.dec(b"cnt", 2);
        let mut m = tx.map(b"m");
        m.register(b"r", Scalar::Int(7));
        m.inc(b"c", 9);
        let mut l = tx.list(b"lst");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        tx.text(b"txt").insert(0, "hi");
        // An XML tree: an element with an attr and two children (a nested
        // element carrying its own text run, and a top-level text run), plus a
        // bare fragment — so the round-trip covers map-slot XML refs, sequence
        // composite nodes, attrs, and nested text.
        let mut body = tx.xml_element(b"body", b"div");
        body.attrs().register(b"class", Scalar::Int(1));
        let mut kids = body.children();
        let mut h1 = kids.insert_element(0, b"h1");
        h1.children().insert_text(0).insert(0, "Title");
        kids.insert_text(1).insert(0, "tail");
        tx.xml_fragment(b"frag");
    });
    // Displace the counter with a scalar, then re-win it — the counter's tally
    // must be retained through the snapshot.
    d.transact(|tx| tx.set(b"cnt", Scalar::Int(0)));
    d.transact(|tx| tx.inc(b"cnt", 1));
    d
}

#[test]
fn a_document_round_trips_its_observable_state() {
    let d = populated();
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(fingerprint(&back), fingerprint(&d));
    assert_eq!(back.client(), d.client());
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn a_decoded_document_dedups_a_replayed_op() {
    let mut d = doc(1);
    let ops = d.transact(|tx| tx.register(b"reg", Scalar::Int(1)));
    let mut back = Document::decode_state(&d.encode_state()).unwrap();
    // The op is already in the restored dedup set: replaying it changes nothing.
    assert!(!back.apply(&ops[0]));
    assert_eq!(fingerprint(&back), fingerprint(&d));
}

fn reg_int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            _ => panic!("expected an Int register"),
        },
        _ => panic!("expected a register"),
    }
}

#[test]
fn a_decoded_document_resolves_lww_against_a_later_write() {
    let mut a = doc(1);
    let a_ops = a.transact(|tx| tx.register(b"reg", Scalar::Int(1)));
    let mut back = Document::decode_state(&a.encode_state()).unwrap();

    // A peer that saw the create, then wrote later, must win after reload.
    let mut b = doc(2);
    for op in &a_ops {
        b.apply(op);
    }
    let later = b.transact(|tx| tx.register(b"reg", Scalar::Int(9)));
    for op in &later {
        back.apply(op);
    }
    assert_eq!(reg_int(back.get(b"reg")), 9);
}

#[test]
fn a_decoded_document_converges_with_a_concurrent_replica() {
    let mut a = doc(1);
    let a_ops = a.transact(|tx| {
        tx.register(b"reg", Scalar::Int(1));
        tx.inc(b"cnt", 3);
    });
    let mut b = doc(2);
    let b_ops = b.transact(|tx| {
        tx.register(b"reg", Scalar::Int(2));
        tx.inc(b"cnt", 4);
    });

    // Reload `a` from a snapshot, then exchange ops both ways.
    let mut a = Document::decode_state(&a.encode_state()).unwrap();
    for op in &b_ops {
        a.apply(op);
    }
    for op in &a_ops {
        b.apply(op);
    }
    assert_eq!(
        fingerprint(&a),
        fingerprint(&b),
        "must converge after reload"
    );
}

#[test]
fn a_snapshot_carries_buffered_ops() {
    // An op buffered against an unseen parent must survive the round-trip and
    // still drain once the parent arrives.
    let mut src = doc(1);
    let ops = src.transact(|tx| {
        tx.map(b"m").register(b"r", Scalar::Int(7));
    });
    // ops: [MapCreate "m", RegisterSet "r" in "m"]

    let mut d = doc(2);
    d.apply(&ops[1]); // child op first — buffered, parent unseen
    assert!(d.get(b"m").is_none());

    let mut back = Document::decode_state(&d.encode_state()).unwrap();
    back.apply(&ops[0]); // parent create unblocks the buffered child
    let m = match back.get(b"m") {
        Some(Element::Map(m)) => m,
        _ => panic!("expected map"),
    };
    assert_eq!(reg_int(m.borrow().get(b"r")), 7);
}

#[test]
fn a_displaced_counter_survives_a_snapshot() {
    // `populated` displaced then re-won the counter; its tally (5 - 2 + 1 = 4)
    // must come back, proving the counter registry round-trips.
    let d = populated();
    let back = Document::decode_state(&d.encode_state()).unwrap();
    assert_eq!(fp_element(&back.get(b"cnt").unwrap()), "C4");
}

#[test]
fn a_displaced_xml_node_survives_a_snapshot() {
    // An XML element displaced by a later scalar is retained in the registry; the
    // snapshot must carry it (and its attrs) so re-creating it restores the tree.
    let mut d = doc(1);
    d.transact(|tx| {
        tx.xml_element(b"body", b"div")
            .attrs()
            .register(b"class", Scalar::Int(7));
    });
    d.transact(|tx| tx.set(b"body", Scalar::Int(9)));

    let mut back = Document::decode_state(&d.encode_state()).unwrap();
    // The slot reads the scalar; the XML element is displaced but retained.
    assert!(matches!(
        back.get(b"body"),
        Some(Element::Scalar(Scalar::Int(9)))
    ));
    // Re-creating the same element re-installs it with its attr intact.
    back.transact(|tx| {
        tx.xml_element(b"body", b"div");
    });
    match back.get(b"body") {
        Some(Element::XmlElement(x)) => {
            let x = x.borrow();
            assert_eq!(x.tag(), b"div");
            match x.attrs().borrow().get(b"class") {
                Some(Element::Register(r)) => assert_eq!(r.borrow().read().clone(), Scalar::Int(7)),
                _ => panic!("attr class lost across the snapshot"),
            }
        }
        _ => panic!("body did not restore to an element"),
    }
}

/// The int at `a.b.x`, or `None` if that path isn't a live register.
fn nested_x(d: &Document) -> Option<i64> {
    let a = match d.get(b"a") {
        Some(Element::Map(a)) => a,
        _ => return None,
    };
    let b = match a.borrow().get(b"b") {
        Some(Element::Map(b)) => b,
        _ => return None,
    };
    let x = b.borrow().get(b"x");
    match x {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn a_nested_container_under_a_displaced_one_reconverges_after_a_snapshot() {
    // A container displaced only because an ancestor lost its slot keeps its own
    // flag clear, so re-winning the ancestor restores the whole subtree. A
    // snapshot must preserve that: displacement is per-slot, not inherited.
    let mut author = doc(1);
    let mut log: Vec<Op> = Vec::new();
    // root.a = map A, A.b = map B, B.x = 7.
    log.extend(author.transact(|tx| {
        tx.map(b"a").map(b"b").register(b"x", Scalar::Int(7));
    }));
    // Overwrite slot "a" with a scalar: A is displaced, B is hidden under it but
    // never lost its own slot in A.
    log.extend(author.transact(|tx| tx.set(b"a", Scalar::Int(0))));

    // Snapshot with A displaced.
    let mut back = Document::decode_state(&author.encode_state()).unwrap();

    // Re-win A (a bare MapCreate, not re-creating B), then a bare register on B
    // whose create is not re-carried — the delta-sync shape that exposes a
    // wrongly-inherited displaced flag.
    let rewin = author.transact(|tx| {
        tx.map(b"a");
    });
    log.extend(rewin.iter().cloned());
    let full = author.transact(|tx| {
        tx.map(b"a").map(b"b").register(b"x", Scalar::Int(8));
    });
    let reg_op = full
        .iter()
        .find(|o| matches!(o.kind, OpKind::RegisterSet { .. }))
        .cloned()
        .expect("a register op on B");
    log.push(reg_op.clone());

    // A reference replica that never snapshotted, from the same op stream.
    let mut reference = doc(2);
    for op in &log {
        reference.apply(op);
    }
    assert_eq!(nested_x(&reference), Some(8));

    // The reloaded replica applies only the post-snapshot delta.
    for op in &rewin {
        back.apply(op);
    }
    back.apply(&reg_op);
    assert_eq!(
        nested_x(&back),
        Some(8),
        "a nested container was wrongly displaced by the snapshot"
    );
}

#[test]
fn a_truncated_document_is_an_error() {
    let bytes = populated().encode_state();
    assert!(Document::decode_state(&bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn a_snapshot_with_an_unknown_version_is_rejected() {
    // The leading version byte gates the format; an unrecognized one must be
    // refused rather than misread against a future layout.
    let mut bytes = populated().encode_state();
    bytes[0] = 0xFF;
    assert!(matches!(
        Document::decode_state(&bytes),
        Err(DecodeError::BadTag { .. })
    ));
}

#[test]
fn an_empty_document_round_trips() {
    let d = doc(1);
    let back = Document::decode_state(&d.encode_state()).unwrap();
    assert_eq!(back.client(), d.client());
    assert!(back.get(b"nope").is_none());
}
