//! XmlElement / XmlFragment through the document op layer — creation in a map
//! slot, attrs edited through the reused Map ops, and convergence of the two.
//!
//! This is the op-driven counterpart to `tests/xml.rs` (which drives the bare
//! in-memory types). It covers only creation + attrs; the children sequence and
//! the state codec are later slices.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// The tag of the XmlElement in slot `key`, or `None` if the slot holds anything
/// else (or nothing).
fn tag_at(d: &Document, key: &[u8]) -> Option<Vec<u8>> {
    match d.get(key) {
        Some(Element::XmlElement(x)) => Some(x.borrow().tag().to_vec()),
        _ => None,
    }
}

/// A named attr's scalar reading on the XmlElement in slot `key`.
fn attr_at(d: &Document, key: &[u8], attr: &[u8]) -> Option<Scalar> {
    match d.get(key) {
        Some(Element::XmlElement(x)) => {
            let got = x.borrow().attrs().borrow().get(attr);
            match got {
                Some(Element::Register(r)) => Some(r.borrow().read().clone()),
                Some(Element::Scalar(s)) => Some(s),
                _ => None,
            }
        }
        _ => None,
    }
}

fn is_fragment(d: &Document, key: &[u8]) -> bool {
    matches!(d.get(key), Some(Element::XmlFragment(_)))
}

#[test]
fn creates_an_xml_element_in_a_slot() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"div");
    });
    assert_eq!(tag_at(&d, b"body"), Some(b"div".to_vec()));
    // A fresh element has no attrs.
    assert_eq!(attr_at(&d, b"body", b"class"), None);
}

#[test]
fn creates_an_xml_fragment_in_a_slot() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_fragment(b"body");
    });
    assert!(is_fragment(&d, b"body"));
    assert_eq!(tag_at(&d, b"body"), None);
}

#[test]
fn sets_and_reads_an_attr() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"div")
            .attrs()
            .register(b"class", Scalar::Int(7));
    });
    assert_eq!(attr_at(&d, b"body", b"class"), Some(Scalar::Int(7)));
}

#[test]
fn an_attr_is_last_writer_wins_across_replicas() {
    let mut a = Document::new(cid(1));
    let mut b = Document::new(cid(2));

    // Both create the same element, then set the same attr concurrently.
    let a_create = a.transact(|tx| {
        tx.xml_element(b"body", b"div")
            .attrs()
            .register(b"class", Scalar::Int(1));
    });
    let b_create = b.transact(|tx| {
        tx.xml_element(b"body", b"div")
            .attrs()
            .register(b"class", Scalar::Int(2));
    });

    for op in &b_create {
        a.apply(op);
    }
    for op in &a_create {
        b.apply(op);
    }

    // Both replicas converge on one reading of the attr.
    assert_eq!(
        attr_at(&a, b"body", b"class"),
        attr_at(&b, b"body", b"class")
    );
    // And on the tag.
    assert_eq!(tag_at(&a, b"body"), tag_at(&b, b"body"));
}

#[test]
fn a_concurrent_different_tag_create_converges() {
    let mut a = Document::new(cid(1));
    let mut b = Document::new(cid(2));

    let a_ops = a.transact(|tx| {
        tx.xml_element(b"body", b"div");
    });
    let b_ops = b.transact(|tx| {
        tx.xml_element(b"body", b"span");
    });

    for op in &b_ops {
        a.apply(op);
    }
    for op in &a_ops {
        b.apply(op);
    }

    // The slot's LWW picks one tag; both replicas agree on which.
    assert_eq!(tag_at(&a, b"body"), tag_at(&b, b"body"));
    let winner = tag_at(&a, b"body").unwrap();
    assert!(winner == b"div".to_vec() || winner == b"span".to_vec());
}

#[test]
fn a_later_scalar_displaces_the_element() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"div")
            .attrs()
            .register(b"class", Scalar::Int(1));
    });
    // A scalar written to the same slot afterwards wins the slot.
    d.transact(|tx| tx.set(b"body", Scalar::Int(9)));
    assert_eq!(
        d.get(b"body").and_then(|e| scalar(&e)),
        Some(Scalar::Int(9))
    );
    assert_eq!(tag_at(&d, b"body"), None);
}

#[test]
fn a_re_create_after_displacement_restores_the_attrs() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"div")
            .attrs()
            .register(b"class", Scalar::Int(1));
    });
    d.transact(|tx| tx.set(b"body", Scalar::Int(9)));
    // Re-creating the same element (same key + tag) re-installs the retained
    // handle, attrs intact.
    d.transact(|tx| {
        tx.xml_element(b"body", b"div");
    });
    assert_eq!(tag_at(&d, b"body"), Some(b"div".to_vec()));
    assert_eq!(attr_at(&d, b"body", b"class"), Some(Scalar::Int(1)));
}

#[test]
fn a_fresh_replica_converges_from_ops() {
    let mut src = Document::new(cid(1));
    let ops = src.transact(|tx| {
        let mut x = tx.xml_element(b"body", b"section");
        x.attrs().register(b"id", Scalar::Int(42));
        x.attrs().register(b"open", Scalar::Bool(true));
    });

    let mut dst = Document::new(cid(2));
    for op in &ops {
        dst.apply(op);
    }

    assert_eq!(tag_at(&dst, b"body"), Some(b"section".to_vec()));
    assert_eq!(attr_at(&dst, b"body", b"id"), Some(Scalar::Int(42)));
    assert_eq!(attr_at(&dst, b"body", b"open"), Some(Scalar::Bool(true)));
}

fn scalar(e: &Element) -> Option<Scalar> {
    match e {
        Element::Scalar(s) => Some(s.clone()),
        _ => None,
    }
}
