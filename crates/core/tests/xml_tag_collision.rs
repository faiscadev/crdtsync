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
//!
//! The rank is a **meet** over the tags, and the three properties that buys are
//! each pinned rather than argued: it is commutative (two claims, both orders),
//! associative and idempotent (three claims, all six orders), and it needs no
//! state a snapshot does not carry — a claim arriving at a *reloaded* replica is
//! ranked against a decoded tag and lands where no restart does. The last shape
//! is the randomized one: pools mixing several tag claims at two stamps with a
//! reveal and a delete, shuffled, where an interaction the reasoning missed shows
//! up and an isolated shape would not reach it.

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
fn a_reload_between_the_two_claims_lands_where_no_restart_does() {
    // The sharper form of the round-trip: the *second* claim arrives at a replica
    // that has restarted, so it is ranked against a **decoded** tag rather than a
    // live one. Both directions matter — the reload can carry the winner (the
    // later claim must lose to it) or the loser (the later claim must take it) —
    // and a rule needing state the snapshot does not carry would break on one of
    // them.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let div = twin_tagged(&base, 9_000, b"div");
    let span = twin_tagged(&base, 9_001, b"span");

    for order in [[&div, &span], [&span, &div]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([order[0]]) {
            d.apply(op);
        }
        let mut back = Document::decode_state(&d.encode_state()).expect("a reloadable snapshot");
        back.apply(order[1]);

        // The replica that never restarted, fed the same two ops.
        let straight = fold(&build, &order);
        assert_eq!(
            (tree(&back), back.encode_state()),
            straight,
            "a reload between the two claims landed somewhere a restart-free replica does not"
        );
        assert_eq!(contested_tag(&back).as_deref(), Some(&b"div"[..]));
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

#[test]
fn an_element_merge_ranks_the_tag_rather_than_taking_the_receiver_s() {
    // `XmlElement::merge` folds two replicas of one node. Leaving the tag alone
    // resolved it by which side received — the seam the op fold does not reach,
    // and the one C40's sweep found a layer down in `List::merge`.
    //
    // Both directions have to run, and they have to differ: a pair where the
    // receiver is `div` in *both* merges tests one direction twice and stays green
    // with the rank removed.
    let id = ElementId::from_bytes([7u8; 16]);
    let node = |tag: &[u8]| XmlElement::new(id, tag.to_vec());

    let mut receives_larger = node(b"div");
    receives_larger.merge(&node(b"span"));
    let mut receives_smaller = node(b"span");
    receives_smaller.merge(&node(b"div"));

    assert_eq!(receives_larger.tag(), b"div");
    assert_eq!(
        receives_smaller.tag(),
        b"div",
        "the merge took the receiver's tag"
    );
}

#[test]
fn the_empty_tag_is_the_rank_s_bottom() {
    // `Some(vec![])` survives the wire — no op-level validation bounds a tag's
    // bytes — so it is an admissible claim and, being the least byte string, it
    // takes every node it names. A rank that special-cased it away would decide
    // an empty-vs-nonempty pair by arrival order, which is the whole bug.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let empty = twin_tagged(&base, 9_000, b"");
    let div = twin_tagged(&base, 9_001, b"div");

    let (tree_a, bytes_a) = fold(&build, &[&empty, &div]);
    let (tree_b, bytes_b) = fold(&build, &[&div, &empty]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    for order in [[&empty, &div], [&div, &empty]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        assert_eq!(
            contested_tag(&d).as_deref(),
            Some(&b""[..]),
            "the empty tag is the least byte string and must take the node"
        );
    }
}

#[test]
fn a_non_utf8_tag_ranks_on_its_bytes() {
    // Nothing constrains a tag to UTF-8 either, and the rank is over bytes, so a
    // tag that is no text at all still orders totally against one that is.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let base = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let raw = twin_tagged(&base, 9_000, &[0x00, 0xff, 0xfe]);
    let div = twin_tagged(&base, 9_001, b"div");

    assert_eq!(fold(&build, &[&raw, &div]), fold(&build, &[&div, &raw]));
    for order in [[&raw, &div], [&div, &raw]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        assert_eq!(
            contested_tag(&d).as_deref(),
            Some(&[0x00u8, 0xff, 0xfe][..]),
            "a leading 0x00 byte is below every printable tag"
        );
    }
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
/// A small linear-congruential PRNG — deterministic, seedable, reproducible.
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
    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

const TAGS: &[&[u8]] = &[b"p", b"div", b"span", b"b", b"section", b"a"];

#[test]
fn a_shuffled_pool_of_tag_claims_converges_on_every_permutation() {
    // The deterministic tests fold the shapes the rank was reasoned about. This
    // pools them — several inserts at one stamp under different tags, a reveal
    // naming the node they derive, a delete of the contested slot, and a second
    // stamp's claims — and folds every pool in many permutations, comparing
    // snapshot bytes. An interaction the reasoning missed shows up here, where an
    // isolated shape would not reach it.
    for seed in 0..40u64 {
        let mut rng = Rng::new(seed);
        let mut author = Document::new(cid(1));
        let build = frag_with_a(&mut author);
        let base = only_insert(author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(1, b"z");
        }));
        let other = only_insert(author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"z");
        }));

        // The node the first stamp's tagged child derives, read off a replica
        // that folded one claim alone.
        let node = {
            let mut d = Document::new(cid(9));
            for op in build.iter().chain([&base]) {
                d.apply(op);
            }
            contested_id(&d).expect("the born child")
        };

        let mut pool: Vec<Op> = Vec::new();
        let mut seq = 9_000u64;
        for _ in 0..3 {
            pool.push(twin_tagged(&base, seq, TAGS[rng.below(TAGS.len())]));
            seq += 1;
        }
        for _ in 0..2 {
            pool.push(twin_tagged(&other, seq, TAGS[rng.below(TAGS.len())]));
            seq += 1;
        }
        if rng.below(2) == 0 {
            let mut reveal = base.clone();
            reveal.id.seq = seq;
            seq += 1;
            reveal.kind = OpKind::XmlReveal {
                node,
                tag: Some(TAGS[rng.below(TAGS.len())].to_vec()),
            };
            pool.push(reveal);
        }
        if rng.below(2) == 0 {
            let mut delete = base.clone();
            delete.id.seq = seq;
            delete.stamp.lamport = base.stamp.lamport + 2;
            delete.kind = OpKind::ListDelete { id: base.stamp };
            pool.push(delete);
        }

        let expect = {
            let refs: Vec<&Op> = pool.iter().collect();
            fold(&build, &refs)
        };
        for round in 0..12 {
            let mut shuffled = pool.clone();
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, rng.below(i + 1));
            }
            let refs: Vec<&Op> = shuffled.iter().collect();
            assert_eq!(
                fold(&build, &refs),
                expect,
                "seed {seed} round {round} diverged"
            );
        }

        // And the pool's verdict survives a reload.
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(pool.iter()) {
            d.apply(op);
        }
        let bytes = d.encode_state();
        let back = Document::decode_state(&bytes).expect("a replica could not load its snapshot");
        assert_eq!(
            back.encode_state(),
            bytes,
            "seed {seed}: the re-encode is not canonical"
        );
    }
}

#[test]
fn the_empty_tag_is_the_rank_s_bottom_at_the_reveal_seat_too() {
    // The empty tag has to be admissible at *every* seat, not just the birth. A
    // guard exempting it at the reveal seat alone left the whole workspace green
    // while an `XmlReveal` and a birth naming one node resolved by arrival.
    let mut author = Document::new(cid(1));
    let build = frag_with_a(&mut author);
    let birth = only_insert(author.transact(|tx| {
        tx.xml_fragment(b"doc").children().insert_element(1, b"div");
    }));
    let node = {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([&birth]) {
            d.apply(op);
        }
        contested_id(&d).expect("the born child")
    };
    let mut reveal = birth.clone();
    reveal.id.seq = 9_000;
    reveal.kind = OpKind::XmlReveal {
        node,
        tag: Some(Vec::new()),
    };

    let (tree_a, bytes_a) = fold(&build, &[&birth, &reveal]);
    let (tree_b, bytes_b) = fold(&build, &[&reveal, &birth]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    for order in [[&birth, &reveal], [&reveal, &birth]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        assert_eq!(
            contested_tag(&d).as_deref(),
            Some(&b""[..]),
            "the empty tag must take the node at the reveal seat"
        );
    }
}

#[test]
fn the_empty_tag_is_the_rank_s_bottom_at_the_merge_seat_too() {
    // And at the third seat. Both directions, since a merge that exempted the
    // empty tag would answer by which side received.
    let id = ElementId::from_bytes([7u8; 16]);
    let node = |tag: &[u8]| XmlElement::new(id, tag.to_vec());

    let mut receives_empty = node(b"div");
    receives_empty.merge(&node(b""));
    let mut receives_named = node(b"");
    receives_named.merge(&node(b"div"));

    assert_eq!(receives_empty.tag(), b"");
    assert_eq!(
        receives_named.tag(),
        b"",
        "the merge took the receiver's tag"
    );
}
