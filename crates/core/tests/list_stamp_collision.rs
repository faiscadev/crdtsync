//! Two ops carrying one stamp into one plain sequence.
//!
//! `List::insert_at` is idempotent on the id, so two `ListInsert` ops under one
//! `ClientId` at the identical `Stamp` used to leave whichever landed first
//! holding the slot, with its value *and* its Fugue anchor — one op set, two
//! states, two snapshots. Both are admissible for the reason C24 gives: dedup is
//! on `OpId`, and the id-space record only bounds an *honest* mint.
//!
//! Which claim holds a sequence id therefore has to be a function of the ops
//! alone. The order is `(kind tag, encoded value)`: two scalars separate on their
//! bytes, a scalar and a composite on the tag, and two composites are already
//! ranked by the document's `(list, stamp)` rank (C24) before the sequence is
//! touched at all. Every shape is folded here in every delivery order and
//! compared byte-for-byte.

use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::list::{Anchor, List, Side};
use crdtsync_core::op::{Op, OpKind};
use crdtsync_core::xml::XmlFragment;
use crdtsync_core::{Element, Scalar};

mod common;
use common::{cid, eid, stmp};

/// A rendering of the live sequence in slot `l`: scalars as their debug form, a
/// composite as its kind tag and id, so a divergence in *what* holds a slot
/// shows up as plainly as a divergence in order.
fn seq(d: &Document) -> String {
    match d.get(b"l") {
        Some(Element::List(l)) => render_all(&l.borrow().values()),
        _ => "∅".to_string(),
    }
}

/// The same rendering for the `doc` fragment's children sequence.
fn kids(d: &Document) -> String {
    match d.get(b"doc") {
        Some(Element::XmlFragment(f)) => render_all(&f.borrow().children().borrow().values()),
        _ => "∅".to_string(),
    }
}

fn render_all(values: &[Element]) -> String {
    let parts: Vec<String> = values.iter().map(render).collect();
    format!("[{}]", parts.join(","))
}

fn render(e: &Element) -> String {
    match e {
        Element::Scalar(s) => format!("{s:?}"),
        Element::XmlElement(x) => format!("elem({})", String::from_utf8_lossy(x.borrow().tag())),
        Element::Text(t) => format!("text({:?})", t.borrow().as_string()),
        other => format!("?{}", other.kind() as u8),
    }
}

/// The lone op of a given shape in a batch.
fn only_kind(batch: Vec<Op>, is: impl Fn(&OpKind) -> bool) -> Op {
    batch
        .into_iter()
        .find(|op| is(&op.kind))
        .expect("the op of that shape")
}

fn only_insert(batch: Vec<Op>) -> Op {
    only_kind(batch, |k| matches!(k, OpKind::ListInsert { .. }))
}

/// Fold `build` then `ops` into a fresh replica and return `(rendered sequence,
/// snapshot bytes)`. The replica identity is fixed so two orders differ in
/// nothing but arrival.
fn fold(build: &[Op], ops: &[&Op], render: fn(&Document) -> String) -> (String, Vec<u8>) {
    let mut d = Document::new(cid(9));
    for op in build.iter().chain(ops.iter().copied()) {
        d.apply(op);
    }
    (render(&d), d.encode_state())
}

/// Fold every delivery order of `ops` (after `build`) and assert they agree on
/// the rendered state and byte-for-byte on the snapshot. Returns the one state.
#[track_caller]
fn converges(build: &[Op], ops: &[&Op], render: fn(&Document) -> String) -> (String, Vec<u8>) {
    let mut folded: Vec<(Vec<usize>, String, Vec<u8>)> = Vec::new();
    for order in permutations(ops.len()) {
        let picked: Vec<&Op> = order.iter().map(|i| ops[*i]).collect();
        let (state, bytes) = fold(build, &picked, render);
        folded.push((order, state, bytes));
    }
    let (first_order, first_state, first_bytes) = folded[0].clone();
    for (order, state, bytes) in folded.iter().skip(1) {
        assert_eq!(
            *state, first_state,
            "order {order:?} folded differently from {first_order:?}"
        );
        assert_eq!(
            *bytes, first_bytes,
            "order {order:?} encoded a different snapshot from {first_order:?}"
        );
    }
    (first_state, first_bytes)
}

/// Every permutation of `0..n` (Heap's algorithm, iterative on a small n).
fn permutations(n: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut items: Vec<usize> = (0..n).collect();
    permute(&mut items, 0, &mut out);
    out
}

fn permute(items: &mut Vec<usize>, at: usize, out: &mut Vec<Vec<usize>>) {
    if at == items.len() {
        out.push(items.clone());
        return;
    }
    for i in at..items.len() {
        items.swap(at, i);
        permute(items, at + 1, out);
        items.swap(at, i);
    }
}

/// A list `l` holding two items, so two colliding inserts can name different
/// Fugue anchors.
fn list_with_two(d: &mut Document) -> Vec<Op> {
    d.transact(|tx| {
        let mut l = tx.list(b"l");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(7));
    })
}

/// The `doc` fragment with two children, so a colliding claim into its children
/// list has somewhere to differ.
fn fragment_with_two(d: &mut Document) -> Vec<Op> {
    d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.insert_element(0, b"p");
        kids.insert_element(1, b"q");
    })
}

#[test]
fn two_inserts_at_one_stamp_into_one_list_converge_in_either_order() {
    // The measured bug: the encoded node carried `2` on one replica and `99` on
    // the other, because `insert_at` is idempotent on the id and the first to
    // land kept it.
    let mut author = Document::new(cid(1));
    let build = list_with_two(&mut author);

    let low = only_insert(author.transact(|tx| tx.list(b"l").insert(0, Scalar::Int(2))));
    let mut high = only_insert(author.transact(|tx| tx.list(b"l").insert(3, Scalar::Int(99))));
    high.stamp = low.stamp;
    assert_ne!(
        low.kind, high.kind,
        "the twins must differ in value and anchor"
    );

    let (state, _) = converges(&build, &[&low, &high], seq);
    // Exactly one of the two values holds the slot — a rule that dropped both
    // would converge while losing an admissible op.
    assert!(
        state.contains("Int(2)") ^ state.contains("Int(99)"),
        "exactly one value must hold the slot: {state}"
    );
}

#[test]
fn the_winners_anchor_travels_with_its_value() {
    // Anchor and value are one unit. A rule that seated the winner's value at the
    // incumbent's position would render the same sequence only until a delete
    // froze the position into a tombstone.
    let mut author = Document::new(cid(1));
    let build = list_with_two(&mut author);

    let front = only_insert(author.transact(|tx| tx.list(b"l").insert(0, Scalar::Int(2))));
    let mut back = only_insert(author.transact(|tx| tx.list(b"l").insert(3, Scalar::Int(99))));
    back.stamp = front.stamp;

    let (state, _) = converges(&build, &[&front, &back], seq);
    // `Int(2)` wins on encoded bytes, and it was placed at the front — so the
    // winner's own anchor is what the sequence must show.
    assert_eq!(
        state, "[Int(2),Int(1),Int(7)]",
        "the winner sits at the loser's position"
    );
}

#[test]
fn a_delete_between_the_two_colliding_inserts_converges_in_every_order() {
    // A tombstone keeps the position of whatever it took out, so the two claims
    // must agree on *where* the id sits and not only on what holds it: a delete
    // landing between them would otherwise freeze the loser's anchor into the dead
    // run on one replica and the winner's on the other, and the two encode
    // different bytes while rendering the same sequence.
    let mut author = Document::new(cid(1));
    let build = list_with_two(&mut author);

    let front = only_insert(author.transact(|tx| tx.list(b"l").insert(0, Scalar::Int(2))));
    let mut back = only_insert(author.transact(|tx| tx.list(b"l").insert(3, Scalar::Int(99))));
    back.stamp = front.stamp;

    let mut delete = front.clone();
    delete.id.seq = 9_100;
    delete.stamp.lamport = front.stamp.lamport + 1;
    delete.kind = OpKind::ListDelete { id: front.stamp };

    // The delete lands after at least one insert in every order tried: it is
    // inert against an id no insert has installed, which is a delete-of-an-unseen
    // -id question, not a collision one.
    let orders: [[&Op; 3]; 4] = [
        [&front, &delete, &back],
        [&back, &delete, &front],
        [&front, &back, &delete],
        [&back, &front, &delete],
    ];
    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in orders {
        folded.push(fold(&build, &order, seq));
    }
    for (i, got) in folded.iter().enumerate().skip(1) {
        assert_eq!(
            got.0, folded[0].0,
            "order {i} folded to a different sequence"
        );
        assert_eq!(got.1, folded[0].1, "order {i} encoded a different snapshot");
    }
    assert_eq!(folded[0].0, "[Int(1),Int(7)]", "the delete must win");
}

#[test]
fn two_text_runs_at_one_stamp_converge_in_either_order() {
    // Text runs ride the same seam: `insert_run` derives each codepoint's id from
    // one base stamp, so two runs at one stamp collide id-for-id.
    let mut author = Document::new(cid(1));
    let build = author.transact(|tx| {
        let mut t = tx.text(b"t");
        t.insert(0, "ab");
    });

    let first = only_kind(author.transact(|tx| tx.text(b"t").insert(0, "xy")), |k| {
        matches!(k, OpKind::TextInsert { .. })
    });
    let mut second = only_kind(author.transact(|tx| tx.text(b"t").insert(4, "PQ")), |k| {
        matches!(k, OpKind::TextInsert { .. })
    });
    second.stamp = first.stamp;

    fn text(d: &Document) -> String {
        match d.get(b"t") {
            Some(Element::Text(t)) => t.borrow().as_string(),
            _ => "∅".to_string(),
        }
    }
    // A run is one contest per codepoint, not one for the run: `x` takes the base
    // id on its anchor, and `Q` takes the next — where both runs chain to the right
    // of the base id, so the anchors tie and the encoded codepoints separate them.
    let (state, _) = converges(&build, &[&first, &second], text);
    assert_eq!(state, "xQab", "the codepoint ids resolved somewhere else");
}

/// A `ListInsert` addressed straight at the `doc` fragment's children list — a
/// scalar claim on a key an `XmlInsertChild` also derives a child for. It carries
/// the template's stamp, target and anchor, so the two meet at the sequence id.
fn scalar_into_children(template: &Op, value: Scalar) -> Op {
    let mut op = template.clone();
    op.id.seq = 9_500;
    let OpKind::XmlInsertChild { anchor, .. } = template.kind else {
        panic!("the template must be a child insert")
    };
    op.kind = OpKind::ListInsert { value, anchor };
    op
}

#[test]
fn a_scalar_and_a_child_insert_at_one_stamp_converge_in_either_order() {
    // A plain `ListInsert` reaches a children list without passing through the
    // placement index at all, so the two claims meet only at the sequence id. The
    // kind tag is the first key of the one order, so `Scalar` (tag 0) takes it.
    let mut author = Document::new(cid(1));
    let build = fragment_with_two(&mut author);

    let child = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let scalar = scalar_into_children(&child, Scalar::Int(5));

    let (state, _) = converges(&build, &[&child, &scalar], kids);
    assert_eq!(
        state, "[elem(p),elem(q),Int(5)]",
        "the scalar's kind tag orders first, so it holds the slot: {state}"
    );
}

#[test]
fn a_scalar_and_both_child_inserts_at_one_stamp_converge_in_every_order() {
    // The shape #371 sharpened rather than closed: `[ListInsert, tagged,
    // tagless]` replaced the scalar (an eviction re-seats over it) while
    // `[ListInsert, tagless, tagged]` kept it (a refusal never reaches the
    // sequence) — one op set and two trees. Every one of the six orders is folded.
    let mut author = Document::new(cid(1));
    let build = fragment_with_two(&mut author);

    let tagged = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_400;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }
    let scalar = scalar_into_children(&tagged, Scalar::Int(5));
    assert_eq!(tagless.stamp, tagged.stamp);
    assert_eq!(scalar.stamp, tagged.stamp);

    let (state, bytes) = converges(&build, &[&scalar, &tagged, &tagless], kids);
    assert_eq!(
        state, "[elem(p),elem(q),Int(5)]",
        "the scalar's kind tag orders first, so it holds the slot: {state}"
    );

    // And the replica still encodes a snapshot its own decoder accepts — the
    // losing children are materialised, parented and placeless.
    let back = Document::decode_state(&bytes).expect("a replica could not load its own snapshot");
    assert_eq!(back.encode_state(), bytes, "the re-encode is not canonical");
}

#[test]
fn a_composite_claim_does_not_take_an_id_a_scalar_holds_through_a_move() {
    // The other way a composite reaches an id a scalar holds: an `XmlMove` whose
    // stamp a `ListInsert` also carries. A move that lost the sequence id still
    // holds its `(list, stamp)` placement — the two are separate contests — so the
    // snapshot has to load back.
    let mut author = Document::new(cid(1));
    let build = author.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.insert_element(0, b"p");
        kids.insert_element(1, b"q");
    });
    let node = match author.get(b"doc") {
        Some(Element::XmlFragment(f)) => {
            let kids = f.borrow().children();
            let first = kids.borrow().get(0).expect("a first child");
            first.id()
        }
        _ => panic!("doc is not a fragment"),
    };
    let frag_id = XmlFragment::node_id(author.root_id(), b"doc");
    let children = XmlFragment::children_id(frag_id);

    let mv = only_kind(author.transact(|tx| tx.move_xml(node, frag_id, 2)), |k| {
        matches!(k, OpKind::XmlMove { .. })
    });
    let OpKind::XmlMove { anchor, .. } = mv.kind else {
        panic!("the move op")
    };
    let mut scalar = mv.clone();
    scalar.id.seq = 9_600;
    scalar.target = children;
    scalar.kind = OpKind::ListInsert {
        value: Scalar::Int(5),
        anchor,
    };

    let (state, bytes) = converges(&build, &[&mv, &scalar], kids);
    assert!(
        state.contains("Int(5)"),
        "the scalar must hold the sequence id: {state}"
    );
    let back = Document::decode_state(&bytes).expect("a replica could not load its own snapshot");
    assert_eq!(back.encode_state(), bytes, "the re-encode is not canonical");
}

// --- the seam itself ---

#[test]
fn merging_two_sequences_that_disagree_at_one_id_converges_either_way() {
    // A merge is a seat path like any other: two replicas that folded one of the
    // colliding ops each must not converge on whichever list was the receiver.
    let id = eid(1, 1);
    let anchor_a = Anchor {
        parent: None,
        side: Side::Right,
    };
    let anchor_b = Anchor {
        parent: None,
        side: Side::Left,
    };
    let mk = |value: i64, anchor: Anchor| {
        let mut l = List::new(id);
        l.insert_at(stmp(5, 1), Element::Scalar(Scalar::Int(value)), anchor);
        l
    };
    let mut ab = mk(2, anchor_a);
    ab.merge(&mk(99, anchor_b));
    let mut ba = mk(99, anchor_b);
    ba.merge(&mk(2, anchor_a));
    assert_eq!(
        ab.encode_state(),
        ba.encode_state(),
        "a merge resolved by which side received"
    );
}

#[test]
fn a_claim_that_ties_takes_the_meet_of_the_two_positions() {
    // Two claims that carry the same value are not a contest — nothing separates
    // them — so the position is the meet, which is the same whichever arrived
    // first. This is the scalar image of `rejoin`.
    let id = eid(1, 1);
    let low = Anchor {
        parent: None,
        side: Side::Left,
    };
    let high = Anchor {
        parent: None,
        side: Side::Right,
    };
    let mk = |first: Anchor, second: Anchor| {
        let mut l = List::new(id);
        l.insert_at(stmp(5, 1), Element::Scalar(Scalar::Int(4)), first);
        l.insert_at(stmp(5, 1), Element::Scalar(Scalar::Int(4)), second);
        l
    };
    assert_eq!(
        mk(low, high).encode_state(),
        mk(high, low).encode_state(),
        "two equal claims resolved by arrival order"
    );
}

#[test]
fn a_replayed_insert_is_still_inert() {
    // The idempotence `insert_at` had on the id is preserved where it is a
    // replay: the same op twice must leave one node at one position.
    let mut l = List::new(eid(1, 1));
    let anchor = Anchor {
        parent: None,
        side: Side::Right,
    };
    l.insert_at(stmp(5, 1), Element::Scalar(Scalar::Int(4)), anchor);
    let once = l.encode_state();
    l.insert_at(stmp(5, 1), Element::Scalar(Scalar::Int(4)), anchor);
    assert_eq!(l.encode_state(), once, "a replay changed the sequence");
    assert_eq!(l.len(), 1);
}

#[test]
fn the_order_ranks_the_kind_tag_before_the_value() {
    // A composite's encoded value is its element id, which is uncorrelated with
    // any scalar's bytes — so the tag has to be read first or the two would
    // interleave by content.
    let anchor = Anchor {
        parent: None,
        side: Side::Right,
    };
    let composite = Element::List(std::rc::Rc::new(std::cell::RefCell::new(List::new(
        ElementId::from_bytes([0u8; 16]),
    ))));
    assert_eq!(composite.kind(), ElementKind::List);
    // The all-zero element id is the smallest possible composite payload; a
    // scalar still outranks it, because tag 0 is read before either payload.
    let mut l = List::new(eid(1, 1));
    l.insert_at(stmp(5, 1), composite.deep_clone(), anchor);
    l.insert_at(
        stmp(5, 1),
        Element::Scalar(Scalar::Bytes(vec![0xff; 8])),
        anchor,
    );
    assert!(
        matches!(l.get(0), Some(Element::Scalar(_))),
        "the composite kept an id the scalar's tag outranks"
    );
}
