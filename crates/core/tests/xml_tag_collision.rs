//! Two claims naming one XML node with two different tags.
//!
//! `xml_child_id` mixes the children list, the stamp and the *kind* into the
//! derivation, but never the tag — so `XmlInsertChild { tag: Some(b"div") }` and
//! `XmlInsertChild { tag: Some(b"span") }` at one stamp into one list derive the
//! **same** `XmlElement` id. Both ops pass every gate: dedup is on `OpId`, and
//! the id-space record only bounds an *honest* mint. C24 makes the two a join
//! rather than a contest — correctly, since they name one node — and settles the
//! position at the meet of the two anchors; the *tag* the node ends at is what is
//! left, and it has to be a function of the ops alone.
//!
//! The tag rides `encode_state` (the XML registry writes each node as its id plus
//! its tag), so a replica that took the first arrival's tag encodes different
//! bytes from one that saw the other order. Every shape that reaches the question
//! is folded here in both orders and compared byte-for-byte.

use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::list::{Anchor, List, Side};
use crdtsync_core::op::{Op, OpKind};
use crdtsync_core::xml::XmlElement;
use crdtsync_core::Element;
use std::cell::RefCell;
use std::rc::Rc;

mod common;
use common::{cid, stmp};

/// A parenthesised rendering of the fragment in slot `doc` — an element as
/// `tag(children)`, a text run quoted.
fn tree(d: &Document) -> String {
    match d.get(b"doc") {
        Some(Element::XmlFragment(f)) => {
            let kids: Vec<String> = f
                .borrow()
                .children()
                .borrow()
                .values()
                .iter()
                .map(render)
                .collect();
            format!("frag({})", kids.join(","))
        }
        _ => "∅".to_string(),
    }
}

fn render(e: &Element) -> String {
    match e {
        Element::XmlElement(x) => {
            let x = x.borrow();
            let kids: Vec<String> = x.children().borrow().values().iter().map(render).collect();
            format!("{}({})", String::from_utf8_lossy(x.tag()), kids.join(","))
        }
        Element::Text(t) => format!("{:?}", t.borrow().as_string()),
        other => format!("?{}", other.kind() as u8),
    }
}

/// Build `doc` = frag(a()); return the ops.
fn frag_with_a(d: &mut Document) -> Vec<Op> {
    d.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(0, b"a");
    })
}

/// The lone `XmlInsertChild` in a batch.
fn only_insert(batch: Vec<Op>) -> Op {
    batch
        .into_iter()
        .find(|op| matches!(op.kind, OpKind::XmlInsertChild { .. }))
        .expect("the child insert")
}

/// A twin of `op` carrying the identical stamp under a distinct `OpId`, with its
/// insert tag replaced.
fn twin_tagged(op: &Op, seq: u64, tag: &[u8]) -> Op {
    let mut twin = op.clone();
    twin.id.seq = seq;
    if let OpKind::XmlInsertChild { tag: t, .. } = &mut twin.kind {
        *t = Some(tag.to_vec());
    }
    assert_eq!(twin.stamp, op.stamp, "the twin must carry one stamp");
    twin
}

/// Fold `build` then `ops` into a fresh replica and return `(rendered tree,
/// snapshot bytes)`. The replica identity is fixed so the orders differ in
/// nothing but arrival.
fn fold(build: &[Op], ops: &[&Op]) -> (String, Vec<u8>) {
    let mut d = Document::new(cid(9));
    for op in build.iter().chain(ops.iter().copied()) {
        d.apply(op);
    }
    (tree(&d), d.encode_state())
}

/// The tag of the sole `XmlElement` child of the `doc` fragment other than `a`.
fn contested_tag(d: &Document) -> Option<Vec<u8>> {
    contested(d).map(|x| x.borrow().tag().to_vec())
}

/// The id of that child.
fn contested_id(d: &Document) -> Option<ElementId> {
    contested(d).map(|x| x.borrow().id())
}

fn contested(d: &Document) -> Option<Rc<RefCell<XmlElement>>> {
    let Some(Element::XmlFragment(f)) = d.get(b"doc") else {
        return None;
    };
    let kids = f.borrow().children();
    let values = kids.borrow().values();
    values.iter().find_map(|value| match value {
        Element::XmlElement(x) if x.borrow().tag() != b"a" => Some(Rc::clone(x)),
        _ => None,
    })
}

#[test]
fn two_tags_at_one_stamp_converge_in_either_order() {
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);

    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let div = twin_tagged(&base, 9_000, b"div");
    let span = twin_tagged(&base, 9_001, b"span");

    let (tree_a, bytes_a) = fold(&build, &[&div, &span]);
    let (tree_b, bytes_b) = fold(&build, &[&span, &div]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
}

#[test]
fn the_smaller_tag_bytes_take_the_node() {
    // The rank is an intrinsic total order over the two ops, so the answer is
    // stated, not merely agreed on: `div` < `span`.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let div = twin_tagged(&base, 9_000, b"div");
    let span = twin_tagged(&base, 9_001, b"span");

    for order in [[&div, &span], [&span, &div]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        assert_eq!(
            contested_tag(&d).as_deref(),
            Some(&b"div"[..]),
            "the smaller tag bytes must take the node"
        );
    }
}

#[test]
fn a_restated_tag_survives_a_snapshot_round_trip() {
    // The tag rides the snapshot, so the rule has to be recoverable on reload
    // with no new persisted state.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let div = twin_tagged(&base, 9_000, b"div");
    let span = twin_tagged(&base, 9_001, b"span");

    for order in [[&span, &div], [&div, &span]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        let bytes = d.encode_state();
        let back =
            Document::decode_state(&bytes).expect("a replica could not load its own snapshot");
        assert_eq!(back.encode_state(), bytes, "the re-encode is not canonical");
        assert_eq!(
            contested_tag(&back).as_deref(),
            Some(&b"div"[..]),
            "the reload forgot which tag won"
        );
    }
}

#[test]
fn three_tags_at_one_stamp_converge_in_every_order() {
    // The rank is a meet over the tags, so it is idempotent and associative as
    // well as commutative: any number of claims, in any order, land on the least.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let p = twin_tagged(&base, 9_000, b"p");
    let div = twin_tagged(&base, 9_001, b"div");
    let span = twin_tagged(&base, 9_002, b"span");

    let orders: [[&Op; 3]; 6] = [
        [&p, &div, &span],
        [&p, &span, &div],
        [&div, &p, &span],
        [&div, &span, &p],
        [&span, &p, &div],
        [&span, &div, &p],
    ];
    let expect = fold(&build, &orders[0]);
    for (i, order) in orders.iter().enumerate().skip(1) {
        assert_eq!(fold(&build, order), expect, "order {i} diverged");
    }
    assert!(
        expect.0.contains("div()"),
        "the least tag must win: {}",
        expect.0
    );
}

#[test]
fn a_delete_between_the_two_claims_does_not_decide_the_tag() {
    // A delete is terminal for the sequence slot and takes the loser's or the
    // winner's position depending on where it lands — the shape C40 measured. The
    // tag lives in the node registry, which no delete tombstones, so the rank has
    // to answer the same with a delete in the middle as without one.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let div = twin_tagged(&base, 9_000, b"div");
    let span = twin_tagged(&base, 9_001, b"span");
    let mut delete = base.clone();
    delete.id.seq = 9_100;
    delete.stamp.lamport = base.stamp.lamport + 1;
    delete.kind = OpKind::ListDelete { id: base.stamp };

    let orders: [[&Op; 3]; 4] = [
        [&div, &delete, &span],
        [&span, &delete, &div],
        [&div, &span, &delete],
        [&span, &div, &delete],
    ];
    let expect = fold(&build, &orders[0]);
    for (i, order) in orders.iter().enumerate().skip(1) {
        assert_eq!(fold(&build, order), expect, "order {i} diverged");
    }
}

#[test]
fn a_reveal_and_a_birth_naming_one_node_converge_in_either_order() {
    // An `XmlReveal` names an arbitrary element id with an arbitrary tag, so it
    // can name exactly the node a birth derives. A reveal that met a materialised
    // node and returned without claiming would settle the two tags by which
    // arrived first — the same bug in a second seat.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let birth = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));

    // The node the birth derives, read off a replica that folded it alone.
    let node = {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([&birth]) {
            d.apply(op);
        }
        contested_id(&d).expect("the born child")
    };

    // The reveal carries the *smaller* tag, so it must win from either side —
    // a reveal that only ever seated an unheld id would win from one.
    let mut reveal = birth.clone();
    reveal.id.seq = 9_000;
    reveal.kind = OpKind::XmlReveal {
        node,
        tag: Some(b"aa".to_vec()),
    };

    let (tree_a, bytes_a) = fold(&build, &[&birth, &reveal]);
    let (tree_b, bytes_b) = fold(&build, &[&reveal, &birth]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    assert!(
        tree_a.contains("aa()"),
        "the smaller tag must win: {tree_a}"
    );
}

#[test]
fn a_live_handle_observes_the_restated_tag() {
    // A handle is a view onto convergent state, not a snapshot: the app holds the
    // node's `Rc` across the second claim's arrival and reads the tag the rank
    // settled on, rather than a stale one or an invalidated handle.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc")
            .children()
            .insert_element(1, b"span");
    }));
    let span = twin_tagged(&base, 9_000, b"span");
    let div = twin_tagged(&base, 9_001, b"div");

    let mut d = Document::new(cid(9));
    for op in build.iter().chain([&span]) {
        d.apply(op);
    }
    let held = match d.get(b"doc") {
        Some(Element::XmlFragment(f)) => {
            let kids = f.borrow().children();
            let values = kids.borrow().values();
            values
                .iter()
                .find_map(|v| match v {
                    Element::XmlElement(x) if x.borrow().tag() == b"span" => Some(Rc::clone(x)),
                    _ => None,
                })
                .expect("the born child")
        }
        _ => panic!("doc is not a fragment"),
    };
    assert_eq!(held.borrow().tag(), b"span");

    d.apply(&div);
    assert_eq!(
        held.borrow().tag(),
        b"div",
        "the handle the app already held did not observe the restated tag"
    );
}

/// Two independent replicas of one node under two tags.
fn twin_nodes(id: ElementId) -> (XmlElement, XmlElement) {
    (
        XmlElement::new(id, b"div".to_vec()),
        XmlElement::new(id, b"span".to_vec()),
    )
}

#[test]
fn an_element_merge_ranks_the_tag_rather_than_taking_the_receiver_s() {
    // `XmlElement::merge` folds two replicas of one node. Leaving the tag alone
    // resolved it by which side received — the seam the op fold does not reach,
    // and the one C40's sweep found a layer down in `List::merge`.
    let id = ElementId::from_bytes([7u8; 16]);
    let (mut a, b) = twin_nodes(id);
    a.merge(&b);
    let (mut b, a2) = twin_nodes(id);
    b.merge(&a2);
    assert_eq!(a.tag(), b"div");
    assert_eq!(b.tag(), b"div", "the merge took the receiver's tag");
}

#[test]
fn a_list_merge_ranks_a_child_s_tag() {
    // The sequence rank is blind to a tag — `put_node_value` writes a composite as
    // its kind and id — so two same-id nodes always rank equal and fold through
    // `Element::merge`. That fold is where the tag is settled.
    let id = ElementId::from_bytes([7u8; 16]);
    let stamp = stmp(1, 1);
    let anchor = Anchor {
        parent: None,
        side: Side::Right,
    };
    let seat = |tag: &[u8]| {
        let mut l = List::new(ElementId::from_bytes([1u8; 16]));
        l.insert_at(
            stamp,
            Element::XmlElement(Rc::new(RefCell::new(XmlElement::new(id, tag.to_vec())))),
            anchor,
        );
        l
    };
    let tag_of = |l: &List| match l.get(0) {
        Some(Element::XmlElement(x)) => x.borrow().tag().to_vec(),
        _ => panic!("a child element"),
    };

    let (mut a, b) = (seat(b"div"), seat(b"span"));
    a.merge(&b);
    let (mut b, a2) = (seat(b"span"), seat(b"div"));
    b.merge(&a2);
    assert_eq!(tag_of(&a), b"div");
    assert_eq!(tag_of(&b), b"div", "the merge took the receiver's tag");
}
