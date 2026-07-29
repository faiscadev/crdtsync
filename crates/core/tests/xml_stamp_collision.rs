//! Two ops carrying one stamp into one children list.
//!
//! A children-list placement is keyed `(list, stamp)` and a document holds at
//! most one — `read_state` refuses a duplicate, so a writer that stored two
//! would encode a snapshot no restart could load. Two ops can nevertheless
//! carry one stamp and both pass every gate: dedup is on `OpId`, and the
//! id-space record only bounds an *honest* mint. Which of them takes the
//! placement therefore has to be a function of the ops alone — a replica that
//! resolved it by arrival order folds one op set into two states, and two
//! replicas holding the same ops encode different bytes.
//!
//! Every shape that reaches the collision is folded here in both orders and
//! compared byte-for-byte: two moves naming different nodes; a tagged and a
//! tagless insert (whose child ids differ, since `xml_child_id` mixes the kind
//! into the derivation, so they collide on the placement without colliding on
//! the element); a birth against a move; a delete landing between the two, which
//! puts the loser's position into a tombstone; a reveal shell whose only
//! placement is taken; and a born node two shells try to strip of both of its.

use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::op::{Op, OpKind};
use crdtsync_core::stamp::Stamp;
use crdtsync_core::xml::XmlElement;
use crdtsync_core::{ClientId, Element};

mod common;
use common::cid;

/// A parenthesised rendering of the fragment in slot `doc`: an element as
/// `tag(children)`, a text run quoted — the live sequence order, so a node
/// appears under exactly one parent.
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

/// Build `doc` = frag(a(x,y), b()); return the ops plus the ids of b, x, y.
fn frag_with_x_y_and_b(d: &mut Document) -> (Vec<Op>, ElementId, ElementId, ElementId) {
    let zero = ElementId::from_bytes([0u8; 16]);
    let (mut b_id, mut x_id, mut y_id) = (zero, zero, zero);
    let ops = d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        {
            let mut a = kids.insert_element(0, b"a");
            let mut ac = a.children();
            x_id = ac.insert_element(0, b"x").id();
            y_id = ac.insert_element(1, b"y").id();
        }
        b_id = kids.insert_element(1, b"b").id();
    });
    (ops, b_id, x_id, y_id)
}

/// The same tree with two children already under `b`, so two colliding moves
/// into `b` can carry different Fugue anchors.
fn frag_with_x_y_and_filled_b(d: &mut Document) -> (Vec<Op>, ElementId, ElementId, ElementId) {
    let zero = ElementId::from_bytes([0u8; 16]);
    let (mut b_id, mut x_id, mut y_id) = (zero, zero, zero);
    let ops = d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        {
            let mut a = kids.insert_element(0, b"a");
            let mut ac = a.children();
            x_id = ac.insert_element(0, b"x").id();
            y_id = ac.insert_element(1, b"y").id();
        }
        let mut b = kids.insert_element(1, b"b");
        b_id = b.id();
        let mut bc = b.children();
        bc.insert_element(0, b"p");
        bc.insert_element(1, b"q");
    });
    (ops, b_id, x_id, y_id)
}

/// Fold `build` then `ops` into a fresh replica and return `(rendered tree,
/// snapshot bytes)`. The replica identity is fixed so the two orders differ in
/// nothing but arrival.
fn fold(build: &[Op], ops: [&Op; 2]) -> (String, Vec<u8>) {
    let mut d = Document::new(cid(9));
    for op in build.iter().chain(ops) {
        d.apply(op);
    }
    (tree(&d), d.encode_state())
}

/// The lone op of a given shape in a batch.
fn only_kind(batch: Vec<Op>, is: impl Fn(&OpKind) -> bool) -> Op {
    batch
        .into_iter()
        .find(|op| is(&op.kind))
        .expect("the op of that shape")
}

fn only_move(batch: Vec<Op>) -> Op {
    only_kind(batch, |k| matches!(k, OpKind::XmlMove { .. }))
}

/// The element id of the first child of `parent`, itself a direct child of the
/// `doc` fragment.
fn first_child_of(d: &Document, parent: ElementId) -> ElementId {
    let Some(Element::XmlFragment(frag)) = d.get(b"doc") else {
        panic!("doc is not a fragment")
    };
    let kids = frag.borrow().children();
    let values = kids.borrow().values();
    for value in values.iter() {
        let Element::XmlElement(x) = value else {
            continue;
        };
        if x.borrow().id() != parent {
            continue;
        }
        let inner = x.borrow().children();
        let first = inner.borrow().get(0).expect("a first child");
        return first.id();
    }
    panic!("no such parent under the fragment")
}

#[test]
fn two_moves_at_one_stamp_into_one_list_converge_in_either_order() {
    // Two `XmlMove`s under one `ClientId` carrying the identical `Stamp` into one
    // destination list, naming different nodes. Only one can hold `(list, stamp)`;
    // *which* one must come off the ops, not off delivery order.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_b(&mut author);

    let first = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut second = first.clone();
    second.id.seq = 9_000;
    if let OpKind::XmlMove { node, .. } = &mut second.kind {
        *node = y_id;
    }
    assert_eq!(second.stamp, first.stamp, "the twin carries one stamp");

    let (tree_a, bytes_a) = fold(&build, [&first, &second]);
    let (tree_b, bytes_b) = fold(&build, [&second, &first]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );

    // Exactly one of the two moved — a rule that refused both would converge
    // while silently dropping an admissible op.
    let moved_x = tree_a.contains("b(x())");
    let moved_y = tree_a.contains("b(y())");
    assert!(moved_x ^ moved_y, "exactly one node must move: {tree_a}");
}

#[test]
fn a_tagged_and_a_tagless_insert_at_one_stamp_converge_in_either_order() {
    // `xml_child_id` mixes the kind into the derivation, so a tagged and a
    // tagless insert at one stamp materialise *different* children — they
    // collide on the placement and on the list slot without colliding on the
    // element id.
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);

    let tagged = author
        .transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        })
        .into_iter()
        .find(|op| matches!(op.kind, OpKind::XmlInsertChild { .. }))
        .expect("the child insert");
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_000;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }
    assert_eq!(tagless.stamp, tagged.stamp, "the twin carries one stamp");

    let (tree_a, bytes_a) = fold(&build, [&tagged, &tagless]);
    let (tree_b, bytes_b) = fold(&build, [&tagless, &tagged]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );

    // One of the two children holds the slot, never both and never neither.
    let took_tagged = tree_a.contains("c()");
    let took_tagless = tree_a.contains("\"\"");
    assert!(
        took_tagged ^ took_tagless,
        "exactly one child must hold the slot: {tree_a}"
    );
}

#[test]
fn a_birth_and_a_move_at_one_stamp_converge_in_either_order() {
    // The mixed shape: a birth holds `(list, stamp)` and a move arrives at that
    // same stamp into that list. A birth writes no move-log entry, so nothing
    // dedups the move — resolving this by arrival order both diverges and, in
    // one order, leaves the moved node with an effective parent it holds no
    // placement in, which suppresses every placement it has anywhere.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);

    let mv = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut birth = mv.clone();
    birth.id.seq = 9_000;
    let OpKind::XmlMove { anchor, .. } = mv.kind else {
        panic!("the move op")
    };
    birth.kind = OpKind::XmlInsertChild {
        tag: Some(b"c".to_vec()),
        anchor,
    };
    assert_eq!(birth.stamp, mv.stamp, "the twin carries one stamp");

    let (tree_a, bytes_a) = fold(&build, [&mv, &birth]);
    let (tree_b, bytes_b) = fold(&build, [&birth, &mv]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );

    // Whichever wins, `x` renders under exactly one parent — never nowhere.
    assert!(
        tree_a.contains("x()"),
        "the moved node vanished from the tree: {tree_a}"
    );
}

/// A replica that folded both colliding ops must still encode a snapshot its own
/// decoder accepts — the duplicate-placement refusal `read_state` performs.
#[test]
fn a_collision_still_leaves_a_loadable_snapshot() {
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_b(&mut author);
    let first = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut second = first.clone();
    second.id.seq = 9_000;
    if let OpKind::XmlMove { node, .. } = &mut second.kind {
        *node = y_id;
    }

    // The insert collision as well: the losing child is materialised but holds
    // no position, so the snapshot carries a container no list reaches — and the
    // reload has to rebuild exactly that, not invent a birth placement for it.
    let tagged = author
        .transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        })
        .into_iter()
        .find(|op| matches!(op.kind, OpKind::XmlInsertChild { .. }))
        .expect("the child insert");
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_001;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }

    let mut d = Document::new(cid(9));
    for op in build.iter().chain([&first, &second, &tagged, &tagless]) {
        d.apply(op);
    }
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).expect("a replica could not load its own snapshot");
    assert_eq!(back.encode_state(), bytes, "the re-encode is not canonical");
}

#[test]
fn a_delete_between_the_two_colliding_moves_converges_in_every_order() {
    // The slot the loser installed is what a delete tombstones, and a tombstone
    // keeps the position of whatever it took out. So the two colliding ops must
    // not only agree on who owns the placement but on *where* the id sits: a
    // delete landing between them would otherwise freeze the loser's anchor into
    // the dead run on one replica and the winner's on the other, and the two
    // encode different bytes while rendering the same tree.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_filled_b(&mut author);

    let first = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut second = only_move(author.transact(|tx| tx.move_xml(y_id, b_id, 3)));
    second.stamp = first.stamp;
    assert_ne!(
        second.kind, first.kind,
        "the two moves must differ in anchor"
    );

    let mut delete = first.clone();
    delete.id.seq = 9_100;
    delete.stamp.lamport = first.stamp.lamport + 1;
    delete.kind = OpKind::ListDelete { id: first.stamp };

    let orders: [[&Op; 3]; 4] = [
        [&first, &delete, &second],
        [&second, &delete, &first],
        [&first, &second, &delete],
        [&second, &first, &delete],
    ];
    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in orders {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    for (i, got) in folded.iter().enumerate().skip(1) {
        assert_eq!(got.0, folded[0].0, "order {i} folded to a different tree");
        assert_eq!(got.1, folded[0].1, "order {i} encoded a different snapshot");
    }
    assert!(
        !folded[0].0.contains("b(p(),x(),q())") && !folded[0].0.contains("b(p(),q(),y())"),
        "the delete must win over the move: {}",
        folded[0].0
    );
}

#[test]
fn a_reveal_shell_that_loses_the_placement_stays_movable() {
    // A move is gated on the node holding a placement or being a reveal shell
    // awaiting its first. A shell that takes `(list, stamp)` and then loses it to
    // a smaller node id must go back to awaiting one — left holding neither, every
    // later move of it would be refused forever, and the shell would be stranded
    // outside the tree on one replica while living in it on the other.
    let shell = ElementId::from_bytes([0xffu8; 16]);
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);

    let mut reveal = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let taking = {
        let mut op = reveal.clone();
        op.id.seq = 9_000;
        op
    };
    reveal.kind = OpKind::XmlReveal {
        node: shell,
        tag: Some(b"shell".to_vec()),
    };
    reveal.id.seq = 9_100;

    // The shell's move carries the same stamp as `x`'s, so the two contend; the
    // shell's id is maximal, so it always loses.
    let mut shell_move = taking.clone();
    shell_move.id.seq = 9_200;
    if let OpKind::XmlMove { node, .. } = &mut shell_move.kind {
        *node = shell;
    }

    // A later move of the shell, at its own stamp — this is what a stranded shell
    // would refuse.
    let mut rescue = shell_move.clone();
    rescue.id.seq = 9_300;
    rescue.stamp.lamport = shell_move.stamp.lamport + 1;

    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in [[&taking, &shell_move], [&shell_move, &taking]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([&reveal]).chain(order).chain([&rescue]) {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    assert_eq!(
        folded[0].0, folded[1].0,
        "the two orders folded differently"
    );
    assert_eq!(
        folded[0].1, folded[1].1,
        "the two orders encode differently"
    );
    assert!(
        folded[0].0.contains("shell()"),
        "the shell was stranded outside the tree: {}",
        folded[0].0
    );
}

#[test]
fn a_born_node_stripped_of_both_its_keys_does_not_become_a_shell() {
    // A born node's *birth* key is where its element id comes from, so it outranks
    // any move naming that key however small the mover's id. Ranking a birth by id
    // alone let two shells strip a born node of both its placements, and the
    // emptied record then read as "a shell awaiting its first placement" — putting
    // a node back on the movable side that the opposite order leaves permanently
    // unplaced, and the two orders rendered different trees.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);

    // A move into b's children, kept as a template only: it names the destination
    // list and carries an anchor into it.
    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let at = |seq: u64, bump: u64, kind: OpKind| {
        let mut op = seed.clone();
        op.id.seq = seq;
        op.stamp.lamport = seed.stamp.lamport + bump;
        op.kind = kind;
        op
    };

    let born = at(
        8_000,
        1,
        OpKind::XmlInsertChild {
            tag: Some(b"m".to_vec()),
            anchor,
        },
    );
    let m_id = {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([&born]) {
            probe.apply(op);
        }
        first_child_of(&probe, b_id)
    };
    // `m` moves on to a second key, so it holds a birth placement and a move one.
    let onward = at(8_001, 2, OpKind::XmlMove { node: m_id, anchor });

    // Two shells, both ranking below every real element id, contend for the two
    // keys `m` holds.
    let low = ElementId::from_bytes([0u8; 16]);
    let high = {
        let mut b = [0u8; 16];
        b[15] = 1;
        ElementId::from_bytes(b)
    };
    let mut shells = Vec::new();
    let mut claims = Vec::new();
    for (i, (shell, victim)) in [(low, &born), (high, &onward)].into_iter().enumerate() {
        let i = i as u64;
        shells.push(at(
            9_000 + i,
            3 + i,
            OpKind::XmlReveal {
                node: shell,
                tag: Some(b"shell".to_vec()),
            },
        ));
        let mut claim = at(
            9_100 + i,
            0,
            OpKind::XmlMove {
                node: shell,
                anchor,
            },
        );
        claim.stamp = victim.stamp;
        claims.push(claim);
    }

    // A later move of `m` — what the two orders must agree to accept or refuse.
    let rescue = at(9_300, 9, OpKind::XmlMove { node: m_id, anchor });

    let mine: Vec<&Op> = vec![&born, &onward];
    let theirs: Vec<&Op> = claims.iter().collect();
    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for (a, b) in [(&mine, &theirs), (&theirs, &mine)] {
        let mut d = Document::new(cid(9));
        for op in build
            .iter()
            .chain(shells.iter())
            .chain(a.iter().copied())
            .chain(b.iter().copied())
            .chain([&rescue])
        {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    assert_eq!(
        folded[0].0, folded[1].0,
        "the two orders folded differently"
    );
    assert_eq!(
        folded[0].1, folded[1].1,
        "the two orders encode differently"
    );
    assert!(
        folded[0].0.contains("m()"),
        "the born node lost its birth key: {}",
        folded[0].0
    );
}

/// The whole point of a total order over the ops: the winner cannot depend on
/// which replica folded them, nor on the identity of the replica.
#[test]
fn the_winner_is_the_same_on_a_replica_that_never_authored_either_op() {
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_b(&mut author);
    let first = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut second = first.clone();
    second.id.seq = 9_000;
    if let OpKind::XmlMove { node, .. } = &mut second.kind {
        *node = y_id;
    }

    let observers: Vec<ClientId> = vec![cid(2), cid(3), cid(4)];
    let (expected, _) = fold(&build, [&first, &second]);
    for who in observers {
        let mut d = Document::new(who);
        for op in build.iter().chain([&second, &first]) {
            d.apply(op);
        }
        assert_eq!(tree(&d), expected, "the winner moved with the observer");
    }
}

#[test]
fn a_birth_that_loses_the_key_keeps_no_move_it_only_got_by_arriving_first() {
    // A birth can lose its key only to another birth — a tagged and a tagless
    // insert at one stamp derive different children and contend. Whichever loses
    // is left materialised and unplaced, which is a *movable* state: the winning
    // order refuses the losing birth and a move naming its child then has to land
    // exactly as it does when the losing birth happened to arrive first and hold
    // the key for a while. Otherwise the move applies on one replica and waits on
    // the other, and the same op set folds two ways.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);

    let tagged = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_000;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }

    // The id each birth derives, read off a replica that saw only that one.
    let child_of = |birth: &Op| {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([birth]) {
            probe.apply(op);
        }
        let Some(Element::XmlFragment(frag)) = probe.get(b"doc") else {
            panic!("doc is not a fragment")
        };
        let kids = frag.borrow().children();
        let last = kids.borrow().values().last().expect("the new child").id();
        last
    };

    // A move of each child into b, so whichever birth loses, a move naming its
    // child is in the op set.
    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let moves: Vec<Op> = [&tagged, &tagless]
        .into_iter()
        .enumerate()
        .map(|(i, birth)| {
            let mut op = seed.clone();
            op.id.seq = 9_100 + i as u64;
            op.stamp.lamport = seed.stamp.lamport + 1 + i as u64;
            op.kind = OpKind::XmlMove {
                node: child_of(birth),
                anchor,
            };
            op
        })
        .collect();

    let pool: Vec<&Op> = vec![&tagged, &tagless, &moves[0], &moves[1]];
    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in permutations(pool.len()) {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order.iter().map(|&i| pool[i])) {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    for (i, got) in folded.iter().enumerate().skip(1) {
        assert_eq!(got.0, folded[0].0, "order {i} folded to a different tree");
        assert_eq!(got.1, folded[0].1, "order {i} encoded a different snapshot");
    }
    // Named, not merely agreed: a rule that refused both births, or both moves,
    // converges over all 24 just as well. Both children must render — one holding
    // the key it was born at, the other landed by the move that its refused birth
    // left it able to accept.
    assert!(
        folded[0].0.contains("c()") && folded[0].0.contains("\"\""),
        "one of the two children never rendered: {}",
        folded[0].0
    );
}

/// Every ordering of `n` items, as index vectors.
fn permutations(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for shorter in permutations(n - 1) {
        for at in 0..n {
            let mut one = shorter.clone();
            one.insert(at, n - 1);
            out.push(one);
        }
    }
    out
}

#[test]
fn an_eviction_inside_a_dead_run_keeps_the_run_the_delete_built() {
    // A contiguous delete welds its ids into one record, so evicting an id in the
    // *interior* of one has to split the record on both sides and let the
    // re-delete weld it back. The winner's position is what the tombstone then
    // holds, and the record has to come back exactly as the order that never
    // evicted leaves it — otherwise the two orders render the same tree and
    // encode different bytes.
    let mut author = Document::new(cid(1));
    let mut b_id = ElementId::from_bytes([0u8; 16]);
    let all = author.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let mut b = kids.insert_element(0, b"b");
        b_id = b.id();
        let mut bc = b.children();
        for i in 0..4u8 {
            bc.insert_element(i as usize, &[b'a' + i]);
        }
    });

    // The middle child's insert is pulled out of the build: it is the one the
    // twin contends for, and its id sits between two tombstones on each side.
    let b_children = XmlElement::children_id(b_id);
    let inserts: Vec<usize> = all
        .iter()
        .enumerate()
        .filter(|(_, op)| {
            matches!(op.kind, OpKind::XmlInsertChild { .. }) && op.target == b_children
        })
        .map(|(i, _)| i)
        .collect();
    let middle = inserts[inserts.len() - 2];
    let tagged = all[middle].clone();
    let build: Vec<Op> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != middle)
        .map(|(_, op)| op.clone())
        .collect();
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_000;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }

    // Every child deleted, so the four ids weld into one record.
    let live: Vec<ElementId> = children_ids(&author, b_id);
    let deletes: Vec<Op> = live
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let mut op = tagged.clone();
            op.id.seq = 9_100 + i as u64;
            op.stamp.lamport = tagged.stamp.lamport + 10 + i as u64;
            op.kind = OpKind::ListDelete {
                id: all[inserts[i]].stamp,
            };
            op
        })
        .collect();

    // Order the pair so the *loser* lands first and is evicted from inside the
    // run; the mirror order refuses it and never splits anything.
    let born_of = |op: &Op| {
        let mut probe = Document::new(cid(9));
        for o in build.iter().chain([op]) {
            probe.apply(o);
        }
        let before: Vec<ElementId> = {
            let mut plain = Document::new(cid(9));
            for o in build.iter() {
                plain.apply(o);
            }
            children_ids(&plain, b_id)
        };
        children_ids(&probe, b_id)
            .into_iter()
            .find(|id| !before.contains(id))
            .expect("the new child")
    };
    let (a, b) = if born_of(&tagged).as_bytes() < born_of(&tagless).as_bytes() {
        (&tagless, &tagged)
    } else {
        (&tagged, &tagless)
    };

    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in [[a, b], [b, a]] {
        let mut d = Document::new(cid(9));
        for op in build
            .iter()
            .chain([order[0]])
            .chain(deletes.iter())
            .chain([order[1]])
        {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    assert_eq!(
        folded[0].0, folded[1].0,
        "the two orders folded differently"
    );
    assert_eq!(
        folded[0].1, folded[1].1,
        "the two orders encode differently"
    );

    // And the snapshot still round-trips: the split-then-weld has to leave the
    // record canonical, not two adjacent pieces the encoder would write twice.
    let back = Document::decode_state(&folded[0].1).expect("the snapshot loads");
    assert_eq!(
        back.encode_state(),
        folded[0].1,
        "the re-encode is not canonical"
    );
}

/// The element ids of `parent`'s children, `parent` being a direct child of the
/// `doc` fragment.
fn children_ids(d: &Document, parent: ElementId) -> Vec<ElementId> {
    let Some(Element::XmlFragment(frag)) = d.get(b"doc") else {
        panic!("doc is not a fragment")
    };
    let kids = frag.borrow().children();
    let values = kids.borrow().values();
    for value in values.iter() {
        let Element::XmlElement(x) = value else {
            continue;
        };
        if x.borrow().id() != parent {
            continue;
        }
        let inner = x.borrow().children();
        let got = inner.borrow().values().iter().map(|c| c.id()).collect();
        return got;
    }
    panic!("no such parent under the fragment")
}

#[test]
fn a_birth_keeps_its_key_against_a_smaller_id_mover() {
    // The rank is what the outcome tests above cannot see: two orders agree under
    // "smallest id wins" too, they just agree on the wrong winner. Pin the rule
    // directly — a shell whose id is minimal still does not take a born child's
    // key, because that key is where the child's own id comes from.
    let shell = ElementId::from_bytes([0u8; 16]);
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);

    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let at = |seq: u64, bump: u64, kind: OpKind| {
        let mut op = seed.clone();
        op.id.seq = seq;
        op.stamp.lamport = seed.stamp.lamport + bump;
        op.kind = kind;
        op
    };
    let born = at(
        8_000,
        1,
        OpKind::XmlInsertChild {
            tag: Some(b"m".to_vec()),
            anchor,
        },
    );
    let reveal = at(
        8_001,
        2,
        OpKind::XmlReveal {
            node: shell,
            tag: Some(b"shell".to_vec()),
        },
    );
    let mut taking = at(
        8_002,
        0,
        OpKind::XmlMove {
            node: shell,
            anchor,
        },
    );
    taking.stamp = born.stamp;

    for order in [[&born, &taking], [&taking, &born]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([&reveal]).chain(order) {
            d.apply(op);
        }
        assert!(
            tree(&d).contains("b(m())"),
            "a move took a born child's key: {}",
            tree(&d)
        );
    }
}

#[test]
fn a_move_onto_the_key_its_own_node_was_born_at_keeps_the_edge_it_logs() {
    // One node asking twice is not a collision: a move can name exactly the child
    // a birth at that key derives. Resolving it as a contest recorded the move's
    // edge in the log when the move happened to arrive first, and none when the
    // birth did — the same op set, two move logs, two snapshots. The join records
    // it either way round: the move applied, and the edge it names is the parent
    // the node was created under, so logging it moves nothing.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);

    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let at = |seq: u64, bump: u64, kind: OpKind| {
        let mut op = seed.clone();
        op.id.seq = seq;
        op.stamp.lamport = seed.stamp.lamport + bump;
        op.kind = kind;
        op
    };
    let born = at(
        8_000,
        1,
        OpKind::XmlInsertChild {
            tag: Some(b"m".to_vec()),
            anchor,
        },
    );
    let m_id = {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([&born]) {
            probe.apply(op);
        }
        first_child_of(&probe, b_id)
    };
    // The node is materialised ahead of the birth so the move is admissible even
    // when it arrives first.
    let reveal = at(
        8_001,
        2,
        OpKind::XmlReveal {
            node: m_id,
            tag: Some(b"m".to_vec()),
        },
    );
    let mut onto_itself = at(8_002, 0, OpKind::XmlMove { node: m_id, anchor });
    onto_itself.stamp = born.stamp;

    // The move log dedups on the stamp, so whether the edge was recorded shows in
    // what a *later* move at the same stamp can still do. Both orders agree either
    // way round, so name the outcome rather than only compare them to each other.
    // Into a *different* list, so its own key is free and only the log's
    // stamp dedup decides whether the edge lands.
    let frag_id = match author.get(b"doc") {
        Some(Element::XmlFragment(f)) => f.borrow().id(),
        _ => panic!("doc is not a fragment"),
    };
    let mut probe = only_move(author.transact(|tx| tx.move_xml(x_id, frag_id, 0)));
    probe.id.seq = 9_400;
    probe.stamp = born.stamp;

    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in [[&born, &onto_itself], [&onto_itself, &born]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([&reveal]).chain(order) {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));

        d.apply(&probe);
        assert!(
            tree(&d).contains("a(x()"),
            "the edge was withdrawn, so a later move at that stamp landed: {}",
            tree(&d)
        );
    }
    assert_eq!(
        folded[0].0, folded[1].0,
        "the two orders folded differently"
    );
    assert_eq!(
        folded[0].1, folded[1].1,
        "the two orders encode differently"
    );
}

#[test]
fn one_nodes_moves_encode_the_same_whichever_order_they_arrived() {
    // No forgery: a node moved twice accumulates a placement record, and a record
    // grown in arrival order and written as stored makes two honest replicas that
    // saw the same two moves disagree byte for byte — the same law this whole
    // suite is about, one layer under the collision.
    let mut src = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut src);
    let first = only_move(src.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let second = only_move(src.transact(|tx| tx.move_xml(x_id, b_id, 0)));

    let (tree_a, bytes_a) = fold(&build, [&first, &second]);
    let (tree_b, bytes_b) = fold(&build, [&second, &first]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
}

#[test]
fn a_reload_keeps_a_losing_birth_movable() {
    // The loser is left materialised and unplaced, which is a movable state — and
    // a snapshot has to say so, or a replica that restarted between the collision
    // and the loser's next move buffers that move forever while a peer that stayed
    // up applies it.
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);
    let tagged = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_000;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }
    // A later move of whichever child lost — it names both in turn, so one of the
    // two is the loser's and the other is inert.
    let mut later = Vec::new();
    for (i, op) in [&tagged, &tagless].into_iter().enumerate() {
        let mut probe = Document::new(cid(9));
        for o in build.iter().chain([op]) {
            probe.apply(o);
        }
        let Some(Element::XmlFragment(frag)) = probe.get(b"doc") else {
            panic!("doc is not a fragment")
        };
        let kids = frag.borrow().children();
        let node = kids.borrow().values().last().expect("the new child").id();
        let mut mv = tagged.clone();
        mv.id.seq = 9_100 + i as u64;
        mv.stamp.lamport = tagged.stamp.lamport + 1 + i as u64;
        let OpKind::XmlInsertChild { anchor, .. } = tagged.kind else {
            panic!("the insert op")
        };
        mv.kind = OpKind::XmlMove { node, anchor };
        later.push(mv);
    }

    let mut live = Document::new(cid(9));
    for op in build.iter().chain([&tagged, &tagless]) {
        live.apply(op);
    }
    let mut reloaded = Document::decode_state(&live.encode_state()).expect("the snapshot loads");
    for op in &later {
        live.apply(op);
        reloaded.apply(op);
    }
    assert_eq!(
        tree(&live),
        tree(&reloaded),
        "a reload forgot the loser was movable"
    );
    assert_eq!(
        live.encode_state(),
        reloaded.encode_state(),
        "a reload encodes differently"
    );
}

#[test]
fn a_reveal_under_the_other_kind_does_not_hand_a_birth_key_to_a_move() {
    // The birth test has to be a pure function of the key. A stamp derives two
    // children — the tagged and the tagless — and an `XmlReveal` naming one of
    // them registers that id under whichever kind it likes. Deciding the test by
    // the registry then makes an honest birth answer "not born here" and hands
    // its key to any smaller-id mover, which is the rank inverted.
    let shell = ElementId::from_bytes([0u8; 16]);
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);

    let tagged = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_000;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }
    // The id the tagless birth derives, read off a replica that saw only it.
    let text_id = {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([&tagless]) {
            probe.apply(op);
        }
        let Some(Element::XmlFragment(frag)) = probe.get(b"doc") else {
            panic!("doc is not a fragment")
        };
        let kids = frag.borrow().children();
        let last = kids.borrow().values().last().expect("the run").id();
        last
    };

    let OpKind::XmlInsertChild { anchor, .. } = tagged.kind else {
        panic!("the insert op")
    };
    let at = |seq: u64, bump: u64, kind: OpKind| {
        let mut op = tagged.clone();
        op.id.seq = seq;
        op.stamp.lamport = tagged.stamp.lamport + bump;
        op.kind = kind;
        op
    };
    // Registers the tagless child's id as an element, so the registry disagrees
    // with the derivation.
    let ghost = at(
        8_000,
        1,
        OpKind::XmlReveal {
            node: text_id,
            tag: Some(b"ghost".to_vec()),
        },
    );
    let reveal = at(
        8_001,
        2,
        OpKind::XmlReveal {
            node: shell,
            tag: Some(b"shell".to_vec()),
        },
    );
    let mut taking = at(
        8_002,
        0,
        OpKind::XmlMove {
            node: shell,
            anchor,
        },
    );
    taking.stamp = tagless.stamp;

    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in [[&tagless, &taking], [&taking, &tagless]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain([&ghost, &reveal]).chain(order) {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    assert_eq!(
        folded[0].0, folded[1].0,
        "the two orders folded differently"
    );
    assert_eq!(
        folded[0].1, folded[1].1,
        "the two orders encode differently"
    );
    // Named, not merely agreed: a birth test that read the registry would answer
    // "born nowhere" for the honest run — the ghost registered its id as an element
    // — and hand the key to the shell in *both* orders, which agree just as well.
    assert!(
        folded[0].0.contains("\"\""),
        "the honest run lost its own birth key: {}",
        folded[0].0
    );
    assert!(
        !folded[0].0.contains("shell()"),
        "the shell took a key it does not derive: {}",
        folded[0].0
    );
}

#[test]
fn the_key_goes_to_the_smaller_id_by_name_not_merely_consistently() {
    // Every either-order case above asserts that two replicas *agree*. A rule that
    // agrees on the wrong winner satisfies all of them — inverting the comparison
    // leaves the whole suite green. So name the winner: of two moves contending for
    // one key, the smaller element id takes it, and the larger stays where it was.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_b(&mut author);

    let first = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut second = first.clone();
    second.id.seq = 9_000;
    if let OpKind::XmlMove { node, .. } = &mut second.kind {
        *node = y_id;
    }
    let (smaller, larger) = if x_id.as_bytes() < y_id.as_bytes() {
        ("x", "y")
    } else {
        ("y", "x")
    };

    for order in [[&first, &second], [&second, &first]] {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        let t = tree(&d);
        assert!(
            t.contains(&format!("b({smaller}())")),
            "the smaller id must hold the key, got {t}"
        );
        assert!(
            t.contains(&format!("a({larger}())")),
            "the larger id must stay where it was, got {t}"
        );
    }
}

#[test]
fn a_move_naming_a_derivable_id_is_ranked_as_the_child_that_key_derives() {
    // The rank asks of *both* sides "is this the child this key derives", never
    // "what kind of op is arriving". A move can name exactly the id a birth at the
    // key would derive; reading that node as a move on the way in but as a birth
    // once it holds the key makes the pair refuse in both directions, and the key
    // goes to whoever arrived first. Which of the two derivable ids sorts lower is
    // the hash's business, so both assignments are run — one of them is always the
    // case where the mover outranks the birth.
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);

    let tagged = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let mut tagless = tagged.clone();
    tagless.id.seq = 9_000;
    if let OpKind::XmlInsertChild { tag, .. } = &mut tagless.kind {
        *tag = None;
    }
    let id_of = |birth: &Op| {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([birth]) {
            probe.apply(op);
        }
        let Some(Element::XmlFragment(frag)) = probe.get(b"doc") else {
            panic!("doc is not a fragment")
        };
        let kids = frag.borrow().children();
        let last = kids.borrow().values().last().expect("the child").id();
        last
    };
    let tagged_id = id_of(&tagged);
    let text_id = id_of(&tagless);
    let OpKind::XmlInsertChild { anchor, .. } = tagged.kind.clone() else {
        panic!("the insert op")
    };
    // Rendered form of whichever id wins: the smaller one, either way round.
    let winner = if text_id.as_bytes() < tagged_id.as_bytes() {
        "\"\""
    } else {
        "c()"
    };

    // Assignment A: the birth is tagged, the move names the tagless derivation.
    // Assignment B: the birth is tagless, the move names the tagged derivation.
    for (birth, moved, tag) in [
        (&tagged, text_id, None),
        (&tagless, tagged_id, Some(b"c".to_vec())),
    ] {
        let at = |seq: u64, bump: u64, kind: OpKind| {
            let mut op = birth.clone();
            op.id.seq = seq;
            op.stamp.lamport = birth.stamp.lamport + bump;
            op.kind = kind;
            op
        };
        // Materialised elsewhere first, so the move is admissible.
        let reveal = at(
            8_000,
            1,
            OpKind::XmlReveal {
                node: moved,
                tag: tag.clone(),
            },
        );
        let mut moving = at(
            8_001,
            0,
            OpKind::XmlMove {
                node: moved,
                anchor,
            },
        );
        moving.stamp = birth.stamp;

        let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
        for order in [[birth, &moving], [&moving, birth]] {
            let mut d = Document::new(cid(9));
            for op in build.iter().chain([&reveal]).chain(order) {
                d.apply(op);
            }
            let t = tree(&d);
            assert!(
                t.contains(winner),
                "the smaller id must hold the key, got {t}"
            );
            // Both orders agreeing is not the property — they agree on the wrong
            // winner too when the birth test reads the registry. One of the two
            // derivable children holds the key; the reveal's shell never does.
            assert!(
                t.contains("c()") || t.contains("\"\""),
                "neither derivable child holds the key: {t}"
            );
            folded.push((t, d.encode_state()));
        }
        assert_eq!(
            folded[0].0, folded[1].0,
            "the two orders folded differently"
        );
        assert_eq!(
            folded[0].1, folded[1].1,
            "the two orders encode differently"
        );
    }
}

#[test]
fn a_move_under_a_node_inside_its_own_attrs_does_not_close_a_parents_cycle() {
    // The move log's cycle check walks the created-under relation, and a container
    // keyed into a map is part of it. Left out, a move under an element living in
    // the moved node's own attrs reads as acyclic, `parents` closes a loop, and the
    // replica holds a snapshot its own decoder refuses — a durable room state no
    // restart can load.
    let mut d = Document::new(cid(1));
    let mut outer = ElementId::from_bytes([0u8; 16]);
    let mut inner = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let mut o = kids.insert_element(0, b"o");
        outer = o.id();
        // An element in the outer node's attrs map, with children of its own.
        inner = o.attrs().xml_element(b"holder", b"h").id();
    });
    assert_ne!(outer, inner, "the attrs element is a distinct node");

    d.transact(|tx| tx.move_xml(outer, inner, 0));
    let bytes = d.encode_state();
    Document::decode_state(&bytes).unwrap_or_else(|e| {
        panic!("the replica cannot reload its own snapshot: {e:?}");
    });
}

#[test]
fn a_reload_does_not_make_a_map_slot_root_movable() {
    // A decode re-derives "materialised, unplaced, movable" from the parent link,
    // and the link is what separates a node that belongs in a children list from a
    // document root, which sits in a map slot. A root is keyed rather than
    // positioned, so a move of it is a no-op — and a reloaded replica has to agree
    // with the one that never restarted.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);
    let mut root_id = ElementId::from_bytes([0u8; 16]);
    let keyed = author.transact(|tx| {
        root_id = tx.xml_element(b"root", b"r").id();
    });

    // A move of the keyed root into b's children, forged off a real move's anchor.
    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let mut moving = seed.clone();
    moving.id.seq = 9_500;
    moving.stamp.lamport = seed.stamp.lamport + 1;
    moving.kind = OpKind::XmlMove {
        node: root_id,
        anchor,
    };

    let mut live = Document::new(cid(9));
    for op in build.iter().chain(keyed.iter()) {
        live.apply(op);
    }
    let mut reloaded = Document::decode_state(&live.encode_state()).expect("the snapshot loads");
    live.apply(&moving);
    reloaded.apply(&moving);

    assert_eq!(
        tree(&live),
        tree(&reloaded),
        "a reload made a keyed root movable"
    );
    assert_eq!(
        live.encode_state(),
        reloaded.encode_state(),
        "a reload encodes differently"
    );
    assert!(
        !tree(&live).contains("b(r())"),
        "a keyed root must not take a position: {}",
        tree(&live)
    );
}

#[test]
fn a_birth_that_loses_its_key_keeps_the_parent_it_was_created_under() {
    // A birth records the parent it was created under whether or not it takes the
    // `(list, stamp)` key — that edge is what the move log's cycle check walks. A
    // decode re-derives it from a birth *placement*, which a losing birth does not
    // have, so a reload that forgets it admits a move the live replica refuses as a
    // cycle, and the two replicas fold one op set into two trees.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);
    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let at = |seq: u64, bump: u64, kind: OpKind| {
        let mut op = seed.clone();
        op.id.seq = seq;
        op.stamp.lamport = seed.stamp.lamport + bump;
        op.kind = kind;
        op
    };

    // Two births at one key under b's children. At this stamp the tagless child's
    // id is the smaller of the two the key derives, so the tagged one loses and is
    // left materialised, parented under b's list, holding no position.
    let born = at(
        8_000,
        2,
        OpKind::XmlInsertChild {
            tag: Some(b"m".to_vec()),
            anchor,
        },
    );
    let twin = at(8_001, 2, OpKind::XmlInsertChild { tag: None, anchor });

    let mut probe = Document::new(cid(9));
    for op in build.iter().chain([&born]) {
        probe.apply(op);
    }
    let m_id = first_child_of(&probe, b_id);
    // A move of `b` under the loser closes a loop — `m` was created under `b`.
    let cycle = only_move(probe.transact(|tx| tx.move_xml(b_id, m_id, 0)));

    let mut d = Document::new(cid(9));
    for op in build.iter().chain([&born, &twin, &cycle]) {
        d.apply(op);
    }
    let live = tree(&d);
    assert!(
        !live.contains("m("),
        "the tagged child was meant to lose the key: {live}"
    );
    assert!(
        live.contains("b("),
        "the live replica admitted the cycle: {live}"
    );

    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).expect("a replica could not load its own snapshot");
    assert_eq!(
        tree(&back),
        live,
        "the reload admitted a move the live replica refused"
    );
    assert_eq!(back.encode_state(), bytes, "the re-encode is not canonical");
}

#[test]
fn two_moves_of_one_node_at_one_stamp_converge_in_either_order() {
    // The rank orders the *nodes* two claims name. When both name the same node it
    // decides nothing, and the two ops still differ — in the Fugue anchor they
    // carry — so the position the node ends up at has to come off the ops too.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_filled_b(&mut author);

    let first = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut second = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 2)));
    second.stamp = first.stamp;
    assert_ne!(
        second.kind, first.kind,
        "the two moves must differ in anchor"
    );

    let (tree_a, bytes_a) = fold(&build, [&first, &second]);
    let (tree_b, bytes_b) = fold(&build, [&second, &first]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
}

#[test]
fn two_births_of_one_node_at_one_stamp_converge_in_either_order() {
    // The same shape on the birth side: two inserts carrying one tag at one stamp
    // derive one child id, so no rank over nodes separates them — and re-seating on
    // the second arrival made the *last* one's anchor win where the first one's did
    // before.
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);

    let insert = |d: &mut Document, at: usize| {
        only_kind(
            d.transact(|tx| {
                tx.xml_fragment(b"doc").children().insert_element(at, b"c");
            }),
            |k| matches!(k, OpKind::XmlInsertChild { .. }),
        )
    };
    let first = insert(&mut author, 0);
    let mut second = insert(&mut author, 2);
    second.stamp = first.stamp;
    assert_ne!(
        second.kind, first.kind,
        "the two inserts must differ in anchor"
    );

    let (tree_a, bytes_a) = fold(&build, [&first, &second]);
    let (tree_b, bytes_b) = fold(&build, [&second, &first]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    // Named, not merely agreed: the position is the meet of the two anchors, and
    // the first insert named the front where the second named the back.
    assert_eq!(tree_a, "frag(c(),a(x(),y()),b())");
}

/// Fold `build` then the first op, snapshot, reload, and fold the second into the
/// reloaded replica — the collision seen by a replica that restarted between the
/// two ops.
fn fold_across_a_reload(build: &[Op], ops: [&Op; 2]) -> (String, Vec<u8>) {
    let mut d = Document::new(cid(9));
    for op in build.iter().chain([ops[0]]) {
        d.apply(op);
    }
    let mut d = Document::decode_state(&d.encode_state()).expect("a snapshot of the first claim");
    d.apply(ops[1]);
    (tree(&d), d.encode_state())
}

#[test]
fn the_anchor_that_takes_a_shared_key_is_named_not_merely_agreed() {
    // Two moves of one node into one list at one stamp differ in nothing but the
    // position they name, so the position is what ranks them. The two orders agree
    // whichever way that comparison runs, so name the winner rather than only
    // compare the orders to each other. The direction itself carries no meaning —
    // the anchor order is arbitrary — but it is fixed, and a document that changed
    // it would move an already-placed node on every replica that reloads.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_filled_b(&mut author);
    let ahead = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut behind = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 2)));
    behind.stamp = ahead.stamp;

    for order in [[&ahead, &behind], [&behind, &ahead]] {
        let (rendered, _) = fold(&build, order);
        assert_eq!(rendered, "frag(a(y()),b(x(),p(),q()))");
    }
}

#[test]
fn a_reload_ranks_a_second_claim_on_one_node_as_a_replica_that_stayed_up_does() {
    // Nothing remembers what put a node at a key: the kind of op comes off the move
    // log (a move records its edge only while it holds the key) and the position off
    // the sequence. Both ride the snapshot, so a replica that restarted between the
    // two colliding ops has to rank the second exactly as one that never did — a
    // rank kept only in memory would resolve the pair one way live and the other
    // after a reload.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_filled_b(&mut author);
    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let at = |seq: u64, bump: u64, kind: OpKind| {
        let mut op = seed.clone();
        op.id.seq = seq;
        op.stamp.lamport = seed.stamp.lamport + bump;
        op.kind = kind;
        op
    };

    // Two moves of one node: the position half of the rank, read back off the
    // sequence the reload rebuilt.
    let ahead = seed.clone();
    let mut behind = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 2)));
    behind.stamp = ahead.stamp;

    // A birth and a move naming the child that birth derives: the op-kind half,
    // read back off the move log the reload replayed. The node is materialised
    // ahead of both so the move is admissible whichever arrives first.
    let born = at(
        8_000,
        1,
        OpKind::XmlInsertChild {
            tag: Some(b"m".to_vec()),
            anchor,
        },
    );
    let m_id = {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([&born]) {
            probe.apply(op);
        }
        first_child_of(&probe, b_id)
    };
    let reveal = at(
        8_001,
        2,
        OpKind::XmlReveal {
            node: m_id,
            tag: Some(b"m".to_vec()),
        },
    );
    let mut onto_itself = at(8_002, 0, OpKind::XmlMove { node: m_id, anchor });
    onto_itself.stamp = born.stamp;

    let with_reveal: Vec<Op> = build.iter().cloned().chain([reveal]).collect();
    for (build, pair) in [
        (&build, [&ahead, &behind]),
        (&build, [&behind, &ahead]),
        (&with_reveal, [&born, &onto_itself]),
        (&with_reveal, [&onto_itself, &born]),
    ] {
        assert_eq!(
            fold_across_a_reload(build, pair),
            fold(build, pair),
            "a reload between the two claims resolved them differently"
        );
    }
}

#[test]
fn a_move_under_a_node_two_maps_deep_does_not_close_a_parents_cycle() {
    // The created-under relation runs through the map half one hop at a time, so a
    // chain of maps is as long as the nesting. A single hop over the map — the
    // element that owns it, straight from the container keyed into it — spans only
    // the first, and everything below is a dead end the cycle check walks off: the
    // move reads as acyclic, `parents` closes a loop, and `resolvable` walks it
    // forever.
    let mut d = Document::new(cid(1));
    let mut outer = ElementId::from_bytes([0u8; 16]);
    let mut inner = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let mut o = kids.insert_element(0, b"o");
        outer = o.id();
        // One map deeper than the attrs map itself.
        inner = o.attrs().map(b"m").xml_element(b"holder", b"h").id();
    });
    assert_ne!(outer, inner, "the nested element is a distinct node");

    d.transact(|tx| tx.move_xml(outer, inner, 0));
    let bytes = d.encode_state();
    Document::decode_state(&bytes).unwrap_or_else(|e| {
        panic!("the replica cannot reload its own snapshot: {e:?}");
    });
}

#[test]
fn a_second_claim_on_one_node_is_ranked_without_reading_the_move_log() {
    // The move log dedups on the stamp alone, so a move can hold a `(list, stamp)`
    // key having recorded no edge at all — a move of some other node at that stamp
    // into some other list took the log slot first. A rank that asked the log what
    // put the incumbent at the key would read that move as a birth exactly when
    // that happened, and the pair of claims on one node would resolve by which of
    // the three ops arrived first.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_filled_b(&mut author);
    let ahead = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let mut behind = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 2)));
    behind.stamp = ahead.stamp;

    // A move of another node into another list, at that same stamp. It takes the
    // log's only slot for the stamp, so neither claim on `x` records an edge.
    let frag_id = match author.get(b"doc") {
        Some(Element::XmlFragment(f)) => f.borrow().id(),
        _ => panic!("doc is not a fragment"),
    };
    let mut elsewhere = only_move(author.transact(|tx| tx.move_xml(y_id, frag_id, 0)));
    elsewhere.id.seq = 9_600;
    elsewhere.stamp = ahead.stamp;

    // `elsewhere` leads in both, so which edge the log's stamp dedup keeps is held
    // fixed and the only thing varying is the order of the two claims on `x`.
    let first = fold(&build, [&elsewhere, &ahead]);
    let mut a = Document::new(cid(9));
    let mut b = Document::new(cid(9));
    for op in build.iter().chain([&elsewhere, &ahead, &behind]) {
        a.apply(op);
    }
    for op in build.iter().chain([&elsewhere, &behind, &ahead]) {
        b.apply(op);
    }
    assert_eq!(
        (tree(&a), a.encode_state()),
        (tree(&b), b.encode_state()),
        "the two claims on one node resolved by arrival order"
    );
    assert_eq!(
        tree(&a),
        first.0,
        "the pair settled somewhere neither of the two orders alone reaches"
    );
}

#[test]
fn the_winner_of_a_key_takes_its_own_position_even_when_it_lands_second() {
    // An eviction hands the key to a different node, so the loser's position is
    // nothing to the winner — a rule that met the two anchors would let the
    // loser's show through exactly when it arrived first. Delivered the other way
    // the winner never sees it, and the two replicas render the node at two
    // positions and encode two snapshots.
    //
    // The winner here carries the *later* position, which is what tells a meet
    // from an outright re-seat: `x` outranks `y` at this key, and moves to the end
    // of `b` where `y` moved to the front.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, y_id) = frag_with_x_y_and_filled_b(&mut author);
    let winner = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 2)));
    let mut loser = only_move(author.transact(|tx| tx.move_xml(y_id, b_id, 0)));
    loser.stamp = winner.stamp;

    let (tree_a, bytes_a) = fold(&build, [&winner, &loser]);
    let (tree_b, bytes_b) = fold(&build, [&loser, &winner]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    assert_eq!(
        tree_a, "frag(a(y()),b(p(),q(),x()))",
        "the winner did not take its own position"
    );

    // And with a delete landing between them, so the position the winner takes is
    // the one the tombstone keeps.
    let mut delete = winner.clone();
    delete.id.seq = 9_700;
    delete.stamp.lamport = winner.stamp.lamport + 1;
    delete.kind = OpKind::ListDelete { id: winner.stamp };
    let orders: [[&Op; 3]; 4] = [
        [&winner, &delete, &loser],
        [&loser, &delete, &winner],
        [&winner, &loser, &delete],
        [&loser, &winner, &delete],
    ];
    let mut folded: Vec<(String, Vec<u8>)> = Vec::new();
    for order in orders {
        let mut d = Document::new(cid(9));
        for op in build.iter().chain(order) {
            d.apply(op);
        }
        folded.push((tree(&d), d.encode_state()));
    }
    for (i, got) in folded.iter().enumerate().skip(1) {
        assert_eq!(got.0, folded[0].0, "order {i} folded to a different tree");
        assert_eq!(got.1, folded[0].1, "order {i} encoded a different snapshot");
    }
}

#[test]
fn a_birth_that_wins_a_key_takes_its_own_position_even_when_it_lands_second() {
    // The move side of an eviction is named elsewhere; this is the birth side. A
    // tagged and a tagless insert at one stamp derive different children and
    // contend for one slot, so the winner's position has to be the one it named —
    // meeting the two would let the loser's show through exactly when it arrived
    // first, and the loser here names the front where the winner names the back.
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);
    let insert = |d: &mut Document, at: usize| {
        only_kind(
            d.transact(|tx| {
                tx.xml_fragment(b"doc").children().insert_element(at, b"c");
            }),
            |k| matches!(k, OpKind::XmlInsertChild { .. }),
        )
    };
    let behind = insert(&mut author, 2);
    let mut ahead = insert(&mut author, 0);
    ahead.stamp = behind.stamp;
    if let OpKind::XmlInsertChild { tag, .. } = &mut ahead.kind {
        *tag = None;
    }
    assert_ne!(behind.kind, ahead.kind, "the two inserts must differ");

    let (tree_a, bytes_a) = fold(&build, [&ahead, &behind]);
    let (tree_b, bytes_b) = fold(&build, [&behind, &ahead]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    assert_eq!(
        tree_a, "frag(a(x(),y()),b(),c())",
        "the winner did not take its own position"
    );
}

#[test]
fn a_text_run_that_loses_its_key_keeps_the_parent_it_was_created_under() {
    // The birth placement of a node is the one whose `(list, stamp)` re-derives the
    // node's own id, and a stamp derives two children — the tagged and the tagless.
    // Reading the kind off the registry instead of trying both answers "born
    // nowhere" for one of the two, so its created-under edge is not re-seeded and a
    // reloaded replica leaves it belonging nowhere where the live one has it under
    // the list it was created in.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_b(&mut author);
    let seed = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    let OpKind::XmlMove { anchor, .. } = seed.kind else {
        panic!("the move op")
    };
    let at = |seq: u64, kind: OpKind| {
        let mut op = seed.clone();
        op.id.seq = seq;
        op.stamp.lamport = seed.stamp.lamport + 1;
        op.kind = kind;
        op
    };
    // At this stamp the tagged child's id is the smaller, so the tagless one — a
    // text run — is the loser, materialised under b's children with no position.
    let tagless = at(8_000, OpKind::XmlInsertChild { tag: None, anchor });
    let tagged = at(
        8_001,
        OpKind::XmlInsertChild {
            tag: Some(b"m".to_vec()),
            anchor,
        },
    );

    let mut live = Document::new(cid(9));
    for op in build.iter().chain([&tagless, &tagged]) {
        live.apply(op);
    }
    assert!(
        tree(&live).contains("b(m())"),
        "the tagless child was meant to lose the key: {}",
        tree(&live)
    );

    let mut reloaded = Document::new(cid(9));
    for op in build.iter().chain([&tagless]) {
        reloaded.apply(op);
    }
    let mut reloaded =
        Document::decode_state(&reloaded.encode_state()).expect("a snapshot of the text run");
    reloaded.apply(&tagged);

    assert_eq!(
        tree(&reloaded),
        tree(&live),
        "the reload folded the eviction differently"
    );
    assert_eq!(
        reloaded.encode_state(),
        live.encode_state(),
        "the reload lost the loser's created-under edge"
    );
}

/// Every occurrence of `find` in `bytes`, so a patch can assert it is rewriting
/// the one field it means to.
fn occurrences(bytes: &[u8], find: &[u8]) -> Vec<usize> {
    (0..bytes.len().saturating_sub(find.len()) + 1)
        .filter(|&i| &bytes[i..i + find.len()] == find)
        .collect()
}

/// The bytes a `(list, stamp)` placement record occupies: the list's element id,
/// then the stamp — a little-endian lamport, the author's id, and the absent-offset
/// flag.
fn placement_bytes(list: ElementId, stamp: Stamp) -> Vec<u8> {
    let mut out = list.as_bytes().to_vec();
    out.extend_from_slice(&stamp.lamport.to_le_bytes());
    out.extend_from_slice(&stamp.client.as_bytes());
    out.push(0);
    out
}

#[test]
fn a_snapshot_that_names_one_key_twice_still_reloads_into_a_loadable_document() {
    // A snapshot is bytes someone hands over. Only a node with more than one
    // placement (or a tombstoned one) is stored; the rest are rebuilt by scanning
    // the children lists — and a stored record may already name a key that a
    // scanned node sits at. Left to overwrite, the document comes back with two
    // records naming one key: this decode accepts it and the next one refuses it,
    // which is a durable room no restart can load.
    let zero = ElementId::from_bytes([0u8; 16]);
    let (mut a_id, mut b_id, mut x_id, mut c_id) = (zero, zero, zero, zero);
    let mut author = Document::new(cid(1));
    let build = author.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        {
            let mut a = kids.insert_element(0, b"a");
            a_id = a.id();
            let mut ac = a.children();
            x_id = ac.insert_element(0, b"x").id();
            c_id = ac.insert_element(1, b"c").id();
        }
        b_id = kids.insert_element(1, b"b").id();
    });
    // `c` never moves, so the reload rebuilds its placement by scanning — the seam
    // a stored record can collide with.
    let c_stamp = build
        .iter()
        .find(|op| {
            op.target == XmlElement::children_id(a_id)
                && matches!(&op.kind, OpKind::XmlInsertChild { tag: Some(t), .. } if t == b"c")
        })
        .expect("c's insert")
        .stamp;
    // `x` moves, so its placements are the ones the snapshot stores.
    let mv = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));

    let mut d = Document::new(cid(9));
    for op in build.iter().chain([&mv]) {
        d.apply(op);
    }
    assert!(
        tree(&d).contains("c()"),
        "c is not in the tree: {}",
        tree(&d)
    );
    let bytes = d.encode_state();

    // Point the stored record for `x`'s move at the key `c` sits at.
    let from = placement_bytes(XmlElement::children_id(b_id), mv.stamp);
    let to = placement_bytes(XmlElement::children_id(a_id), c_stamp);
    assert_eq!(
        occurrences(&bytes, &from).len(),
        1,
        "the move placement record is not uniquely locatable"
    );
    assert_eq!(from.len(), to.len(), "a placement record is fixed width");
    let at = occurrences(&bytes, &from)[0];
    let mut forged = bytes.clone();
    forged[at..at + to.len()].copy_from_slice(&to);

    let mut back = Document::decode_state(&forged).expect("the forged snapshot decodes");

    // A move of `c` gives it a second placement, which is what makes its record
    // storable — and so what puts a record it should never have been given into the
    // bytes beside the stored one that already names the key.
    let mut moving = mv.clone();
    moving.id.seq = 9_800;
    moving.stamp.lamport = mv.stamp.lamport + 5;
    moving.kind = match mv.kind {
        OpKind::XmlMove { anchor, .. } => OpKind::XmlMove { node: c_id, anchor },
        _ => panic!("the move op"),
    };
    back.apply(&moving);

    let again = back.encode_state();
    Document::decode_state(&again)
        .expect("a replica re-encoded a snapshot its own decoder refuses");
}

#[test]
fn a_birth_takes_a_move_key_at_its_own_position_not_the_movers() {
    // The birth-against-move eviction, with the two ops naming different positions
    // — the case every other birth/move fixture leaves out by carrying one anchor.
    // A birth outranks a move at the key it derives, and it takes that key at the
    // position it named: meeting the two would let the mover's show through exactly
    // when the mover arrived first.
    let mut author = Document::new(cid(1));
    let (build, b_id, x_id, _y) = frag_with_x_y_and_filled_b(&mut author);

    // A move of `x` into b's children at the front.
    let mover = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 0)));
    // A birth at that same key, at the back. Its child derives from the key, so it
    // outranks the move whatever the anchors — and it must take the key at its own
    // position, which is the *later* of the two, so a meet would show.
    let back = only_move(author.transact(|tx| tx.move_xml(x_id, b_id, 2)));
    let OpKind::XmlMove { anchor, .. } = back.kind else {
        panic!("the move op")
    };
    let mut birth = mover.clone();
    birth.id.seq = 9_900;
    birth.kind = OpKind::XmlInsertChild {
        tag: Some(b"c".to_vec()),
        anchor,
    };
    assert_eq!(birth.stamp, mover.stamp, "the twin carries one stamp");

    let (tree_a, bytes_a) = fold(&build, [&mover, &birth]);
    let (tree_b, bytes_b) = fold(&build, [&birth, &mover]);
    assert_eq!(tree_a, tree_b, "the two orders folded to different trees");
    assert_eq!(
        bytes_a, bytes_b,
        "the two orders encode different snapshots"
    );
    assert_eq!(
        tree_a, "frag(a(x(),y()),b(p(),q(),c()))",
        "the birth did not take the key at its own position"
    );
}

#[test]
fn a_second_birth_at_a_key_its_node_has_moved_off_does_not_render_it_twice() {
    // A join re-seats the sequence node, and re-seating clears the suppression the
    // fold wrote. A node that has since moved away renders at its new parent *and*
    // back at its birth slot until the fold runs again — the duplication the
    // Kleppmann fold exists to forbid, and a divergence from the replica that saw
    // only the first birth.
    let mut author = Document::new(cid(1));
    let (build, b_id, _x, _y) = frag_with_x_y_and_b(&mut author);
    let born = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(2, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let c_id = {
        let mut probe = Document::new(cid(9));
        for op in build.iter().chain([&born]) {
            probe.apply(op);
        }
        let Some(Element::XmlFragment(f)) = probe.get(b"doc") else {
            panic!("doc is not a fragment")
        };
        let kids = f.borrow().children();
        let last = kids.borrow().values().last().expect("c").id();
        last
    };
    let mv = only_move(author.transact(|tx| tx.move_xml(c_id, b_id, 0)));

    // A second insert carrying that stamp and tag names the same child, at another
    // position — so the join has something to re-seat.
    let elsewhere = only_kind(
        author.transact(|tx| {
            tx.xml_fragment(b"doc").children().insert_element(0, b"c");
        }),
        |k| matches!(k, OpKind::XmlInsertChild { .. }),
    );
    let mut again = elsewhere.clone();
    again.id.seq = 9_950;
    again.stamp = born.stamp;

    let mut d = Document::new(cid(9));
    for op in build.iter().chain([&born, &mv, &again]) {
        d.apply(op);
    }
    let rendered = tree(&d);
    assert_eq!(
        rendered.matches("c()").count(),
        1,
        "the node renders under two parents: {rendered}"
    );
    assert_eq!(
        rendered,
        tree(&{
            let mut without = Document::new(cid(9));
            for op in build.iter().chain([&born, &mv]) {
                without.apply(op);
            }
            without
        }),
        "the second birth moved a node it only re-seated"
    );
}

#[test]
fn a_join_on_a_buried_id_reads_the_position_the_run_buried_it_at() {
    // A join meets the position the id already holds, and a tombstoned id holds one
    // as faithfully as a live one: it heads the run the delete built, or — welded in
    // — hangs to the right of its predecessor, which is the only way it welds at
    // all. Reading the run *head's* position for a welded-in id instead answers the
    // same thing for every claim, so a join that should leave the run alone and one
    // that should move the id become indistinguishable.
    let mut author = Document::new(cid(1));
    let (build, _b, _x, _y) = frag_with_x_y_and_b(&mut author);
    // Two consecutive inserts: `q` takes the next lamport and hangs to `p`'s right,
    // which is exactly what lets the two tombstones weld into one run.
    let pair = author.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.insert_element(2, b"p");
        kids.insert_element(3, b"q");
    });
    let inserts: Vec<Op> = pair
        .into_iter()
        .filter(|op| matches!(op.kind, OpKind::XmlInsertChild { .. }))
        .collect();
    assert_eq!(inserts.len(), 2, "two children were inserted");
    let (born_p, born_q) = (inserts[0].clone(), inserts[1].clone());
    assert_eq!(
        born_q.stamp.lamport,
        born_p.stamp.lamport + 1,
        "the two ids must be adjacent for the runs to weld"
    );

    let delete = |seq: u64, bump: u64, id: Stamp| {
        let mut op = born_q.clone();
        op.id.seq = seq;
        op.stamp.lamport = born_q.stamp.lamport + bump;
        op.kind = OpKind::ListDelete { id };
        op
    };
    let kill_p = delete(9_960, 1, born_p.stamp);
    let kill_q = delete(9_961, 2, born_q.stamp);

    // Three claims on `q`'s own key, one op id between them so the only thing that
    // can move is the position: the one the run buried it at, one after it, and one
    // before it.
    let seq = 9_970;
    let mut named = |at: usize| {
        let mut op = only_kind(
            author.transact(|tx| {
                tx.xml_fragment(b"doc").children().insert_element(at, b"q");
            }),
            |k| matches!(k, OpKind::XmlInsertChild { .. }),
        );
        op.id.seq = seq;
        op.stamp = born_q.stamp;
        op
    };
    let buried = {
        let mut op = born_q.clone();
        op.id.seq = seq;
        op
    };
    let after = named(4);
    let before = named(2);

    let fold_with = |extra: &Op| {
        let mut d = Document::new(cid(9));
        for op in build
            .iter()
            .chain([&born_p, &born_q, &kill_p, &kill_q, extra])
        {
            d.apply(op);
        }
        d.encode_state()
    };
    assert_eq!(
        fold_with(&after),
        fold_with(&buried),
        "a join naming a later position moved the id off where the delete left it"
    );
    assert_ne!(
        fold_with(&before),
        fold_with(&buried),
        "a join naming an earlier position left the id where the delete did"
    );
}
