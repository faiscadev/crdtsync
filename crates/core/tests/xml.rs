//! XmlElement / XmlFragment — the tree primitives (Unit 1a: in-memory structure).
//!
//! An XmlElement is a tagged node pairing an `attrs` Map with a `children`
//! sequence of nested XmlElements and Text runs; an XmlFragment is the same
//! children sequence with no tag or attrs. Both reuse the built composites, so
//! their attrs LWW and children Fugue ordering/tombstones are the Map's and
//! List's — these tests fix the pairing: id derivation, the tag as identity, and
//! that merge / deep-clone / displacement forward into both halves. The op,
//! codec, and document-registry wiring are Units 1b/1c; here the primitives are
//! driven directly, as `tests/list.rs` drives a bare List.

use std::cell::RefCell;
use std::rc::Rc;

use crdtsync_core::text::Text;
use crdtsync_core::xml::{XmlElement, XmlFragment};
use crdtsync_core::{Element, ElementId, Scalar};

mod common;
use common::{eid, stmp};

fn xml(tag: &str) -> XmlElement {
    XmlElement::new(eid(1, 1), tag.as_bytes().to_vec())
}

/// An XmlElement child wrapped as an Element, with an explicit id.
fn child(id: ElementId, tag: &str) -> Element {
    Element::XmlElement(Rc::new(RefCell::new(XmlElement::new(
        id,
        tag.as_bytes().to_vec(),
    ))))
}

/// A Text run child wrapped as an Element.
fn text_child(id: ElementId, s: &str) -> Element {
    let mut t = Text::new(id);
    t.insert(0, s, stmp(1, 9));
    Element::Text(Rc::new(RefCell::new(t)))
}

/// The tags of an element's live children, in order.
fn child_tags(x: &XmlElement) -> Vec<Vec<u8>> {
    let children = x.children();
    let children = children.borrow();
    children
        .values()
        .iter()
        .map(|e| match e {
            Element::XmlElement(c) => c.borrow().tag().to_vec(),
            Element::Text(_) => b"#text".to_vec(),
            _ => panic!("a child is an XmlElement or Text"),
        })
        .collect()
}

/// Insert `value` at `index` in `x`'s children, tagged `(lamport, client)`.
fn insert_child(x: &XmlElement, index: usize, value: Element, lamport: u64, client: u8) {
    x.children()
        .borrow_mut()
        .insert(index, value, stmp(lamport, client));
}

// --- construction / read ---

#[test]
fn new_stores_id_and_tag() {
    let x = XmlElement::new(eid(7, 42), b"section".to_vec());
    assert_eq!(x.id(), eid(7, 42));
    assert_eq!(x.tag(), b"section");
}

#[test]
fn new_has_empty_attrs_and_children() {
    let x = xml("p");
    assert_eq!(x.attrs().borrow().size(), 0);
    assert_eq!(x.children().borrow().len(), 0);
}

#[test]
fn attrs_and_children_ids_derive_from_the_element() {
    // Two elements built at the same id agree on their attrs and children ids —
    // the convergence guarantee the sub-composites inherit from ElementId.
    let a = XmlElement::new(eid(3, 9), b"div".to_vec());
    let b = XmlElement::new(eid(3, 9), b"div".to_vec());
    assert_eq!(a.attrs().borrow().id(), b.attrs().borrow().id());
    assert_eq!(a.children().borrow().id(), b.children().borrow().id());
    // Attrs and children of the same element are distinct ids.
    assert_ne!(a.attrs().borrow().id(), a.children().borrow().id());
    // A different element derives different sub-ids.
    let c = XmlElement::new(eid(3, 10), b"div".to_vec());
    assert_ne!(a.children().borrow().id(), c.children().borrow().id());
}

// --- children (Fugue sequence) ---

#[test]
fn children_insert_in_order() {
    let x = xml("ul");
    insert_child(&x, 0, child(eid(2, 1), "li"), 1, 1);
    insert_child(&x, 1, text_child(eid(2, 2), "hi"), 2, 1);
    insert_child(&x, 2, child(eid(2, 3), "li"), 3, 1);
    assert_eq!(
        child_tags(&x),
        vec![b"li".to_vec(), b"#text".to_vec(), b"li".to_vec()],
    );
}

#[test]
fn deleting_a_child_tombstones_it() {
    let x = xml("ul");
    insert_child(&x, 0, child(eid(2, 1), "a"), 1, 1);
    insert_child(&x, 1, child(eid(2, 2), "b"), 2, 1);
    x.children().borrow_mut().delete(0);
    assert_eq!(child_tags(&x), vec![b"b".to_vec()]);
    assert_eq!(x.children().borrow().len(), 1);
}

// --- attrs (Map, LWW) ---

#[test]
fn set_attr_reads_back() {
    let x = xml("a");
    x.attrs().borrow_mut().set(
        b"href",
        Element::Scalar(Scalar::Bytes(b"/home".to_vec())),
        stmp(1, 1),
    );
    match x.attrs().borrow().get(b"href") {
        Some(Element::Scalar(Scalar::Bytes(v))) => assert_eq!(v, b"/home"),
        _ => panic!("expected the href attr"),
    }
}

#[test]
fn attr_is_last_writer_wins() {
    let x = xml("a");
    let attrs = x.attrs();
    attrs
        .borrow_mut()
        .set(b"class", Element::Scalar(Scalar::Int(1)), stmp(1, 1));
    // A later stamp wins; an earlier one is ignored.
    attrs
        .borrow_mut()
        .set(b"class", Element::Scalar(Scalar::Int(2)), stmp(3, 1));
    attrs
        .borrow_mut()
        .set(b"class", Element::Scalar(Scalar::Int(9)), stmp(2, 1));
    let got = attrs.borrow().get(b"class");
    match got {
        Some(Element::Scalar(Scalar::Int(n))) => assert_eq!(n, 2),
        _ => panic!("expected class=2"),
    }
}

// --- merge ---

/// Two independent replicas of the same element (same id + tag).
fn twins(tag: &str) -> (XmlElement, XmlElement) {
    (
        XmlElement::new(eid(5, 5), tag.as_bytes().to_vec()),
        XmlElement::new(eid(5, 5), tag.as_bytes().to_vec()),
    )
}

#[test]
fn merge_is_idempotent() {
    let x = xml("ul");
    insert_child(&x, 0, child(eid(2, 1), "li"), 1, 1);
    let twin = x.deep_clone();
    x.merge(&twin);
    assert_eq!(child_tags(&x), vec![b"li".to_vec()]);
}

#[test]
fn merge_absorbs_disjoint_children() {
    let (a, b) = twins("ul");
    insert_child(&a, 0, child(eid(2, 1), "a"), 1, 1);
    insert_child(&b, 0, child(eid(2, 2), "b"), 1, 2);
    a.merge(&b);
    b.merge(&a);
    // Both replicas hold both children in the one converged order.
    assert_eq!(child_tags(&a), child_tags(&b));
    assert_eq!(a.children().borrow().len(), 2);
}

#[test]
fn merge_is_commutative() {
    let (a, b) = twins("ul");
    insert_child(&a, 0, child(eid(2, 1), "a"), 1, 1);
    insert_child(&b, 0, child(eid(2, 2), "b"), 1, 2);

    let (ab, _) = twins("ul");
    ab.merge(&a);
    ab.merge(&b);
    let (ba, _) = twins("ul");
    ba.merge(&b);
    ba.merge(&a);
    assert_eq!(child_tags(&ab), child_tags(&ba));
}

#[test]
fn merge_carries_child_tombstones() {
    let (a, b) = twins("ul");
    insert_child(&a, 0, child(eid(2, 1), "gone"), 1, 1);
    // b learns of the child, then a deletes it; the tombstone must ride the merge.
    b.merge(&a);
    a.children().borrow_mut().delete(0);
    b.merge(&a);
    assert!(child_tags(&b).is_empty());
}

#[test]
fn merge_reconciles_attrs_by_lww() {
    let (a, b) = twins("a");
    a.attrs()
        .borrow_mut()
        .set(b"k", Element::Scalar(Scalar::Int(1)), stmp(1, 1));
    b.attrs()
        .borrow_mut()
        .set(b"k", Element::Scalar(Scalar::Int(2)), stmp(2, 1));
    a.merge(&b);
    match a.attrs().borrow().get(b"k") {
        Some(Element::Scalar(Scalar::Int(n))) => assert_eq!(n, 2, "the later attr write wins"),
        _ => panic!("expected k=2"),
    }
}

#[test]
fn merge_recurses_into_a_shared_child() {
    // The same child element edited on both sides folds together, not replaced.
    let (a, b) = twins("ul");
    let child_id = eid(2, 7);
    insert_child(&a, 0, child(child_id, "li"), 1, 1);
    b.merge(&a); // b now holds the same child (same id)

    // Each side sets a distinct attr on its copy of the child.
    if let Some(Element::XmlElement(ca)) = a.children().borrow().get(0) {
        ca.borrow()
            .attrs()
            .borrow_mut()
            .set(b"x", Element::Scalar(Scalar::Int(1)), stmp(5, 1));
    }
    if let Some(Element::XmlElement(cb)) = b.children().borrow().get(0) {
        cb.borrow()
            .attrs()
            .borrow_mut()
            .set(b"y", Element::Scalar(Scalar::Int(2)), stmp(5, 2));
    }
    a.merge(&b);

    let merged = a.children().borrow().get(0).unwrap();
    let Element::XmlElement(c) = merged else {
        panic!("child is an XmlElement");
    };
    let c = c.borrow();
    let attrs = c.attrs();
    let attrs = attrs.borrow();
    assert!(attrs.get(b"x").is_some(), "the shared child kept a's attr");
    assert!(
        attrs.get(b"y").is_some(),
        "and absorbed b's attr — a fold, not a replace"
    );
}

// --- deep clone ---

#[test]
fn deep_clone_is_independent() {
    let x = xml("ul");
    insert_child(&x, 0, child(eid(2, 1), "a"), 1, 1);
    let clone = x.deep_clone();
    // Editing the clone leaves the original alone.
    insert_child(&clone, 1, child(eid(2, 2), "b"), 2, 1);
    assert_eq!(child_tags(&x), vec![b"a".to_vec()]);
    assert_eq!(child_tags(&clone), vec![b"a".to_vec(), b"b".to_vec()]);
    // Clone shares the id/tag but not the handles.
    assert_eq!(clone.id(), x.id());
    assert_eq!(clone.tag(), x.tag());
    assert!(!Rc::ptr_eq(&x.attrs(), &clone.attrs()));
}

// --- displacement ---

#[test]
fn displace_and_reinstate_toggle_the_flag() {
    let x = xml("p");
    assert!(!x.is_displaced());
    x.displace();
    assert!(x.is_displaced());
    x.reinstate();
    assert!(!x.is_displaced());
}

// --- XmlFragment ---

#[test]
fn fragment_holds_a_children_sequence() {
    let f = XmlFragment::new(eid(8, 1));
    assert_eq!(f.id(), eid(8, 1));
    assert_eq!(f.children().borrow().len(), 0);
    f.children()
        .borrow_mut()
        .insert(0, child(eid(8, 2), "p"), stmp(1, 1));
    assert_eq!(f.children().borrow().len(), 1);
}

#[test]
fn fragment_merges_disjoint_children() {
    let a = XmlFragment::new(eid(8, 1));
    let b = XmlFragment::new(eid(8, 1));
    a.children()
        .borrow_mut()
        .insert(0, child(eid(8, 2), "a"), stmp(1, 1));
    b.children()
        .borrow_mut()
        .insert(0, child(eid(8, 3), "b"), stmp(1, 2));
    a.merge(&b);
    assert_eq!(a.children().borrow().len(), 2);
}

// --- Element integration ---

#[test]
fn element_wraps_an_xml_element() {
    let e = child(eid(4, 4), "div");
    assert_eq!(e.kind(), crdtsync_core::ElementKind::XmlElement);
    assert!(e.is_container());
    assert_eq!(e.id(), eid(4, 4));

    // deep_clone yields a fresh, independent handle of the same kind.
    let cloned = e.deep_clone();
    assert_eq!(cloned.kind(), crdtsync_core::ElementKind::XmlElement);
    match (&e, &cloned) {
        (Element::XmlElement(a), Element::XmlElement(b)) => assert!(!Rc::ptr_eq(a, b)),
        _ => panic!("both are XmlElement"),
    }
}

#[test]
fn element_merge_forwards_to_xml() {
    let a = child(eid(4, 4), "div");
    let b = a.deep_clone();
    if let Element::XmlElement(bx) = &b {
        bx.borrow()
            .attrs()
            .borrow_mut()
            .set(b"k", Element::Scalar(Scalar::Int(1)), stmp(1, 1));
    }
    a.merge(&b);
    let Element::XmlElement(ax) = &a else {
        panic!("a is XmlElement");
    };
    assert!(ax.borrow().attrs().borrow().get(b"k").is_some());
}
