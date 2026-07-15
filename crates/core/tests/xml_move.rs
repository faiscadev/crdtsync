//! Tree move through the op layer — relocating an `XmlElement`/`Text` child to a
//! new parent while preserving its identity and subtree, and converging under
//! concurrency (Kleppmann 2021). Builds on 1c (children) + 2a (the move log).
//!
//! A move is a single `XmlMove` op: the moved node keeps its element id, so its
//! attrs and descendants ride along; only which children sequence renders it
//! changes. Concurrency guarantees: one parent per node, no cycle, no
//! duplication, order-independent convergence.

use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::{ClientId, Element, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn zero_id() -> ElementId {
    ElementId::from_bytes([0u8; 16])
}

/// A parenthesised rendering of the tree under slot `key`: an element as
/// `tag(children)`, a text run quoted. Order is the live sequence order, so a
/// moved child appears under exactly one parent.
fn tree(d: &Document, key: &[u8]) -> String {
    match d.get(key) {
        Some(e @ Element::XmlElement(_)) => render(&e),
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
        Element::Scalar(s) => format!("S{s:?}"),
        other => format!("?{}", other.kind() as u8),
    }
}

/// Build a fragment `doc` = frag(a(x), b()); return the ops plus the ids of a, b,
/// x. `x` carries an `id` attr and a grandchild so the move can be checked to
/// preserve the subtree.
fn frag_with_a_x_b(d: &mut Document) -> (Vec<Op>, ElementId, ElementId, ElementId) {
    let mut a_id = zero_id();
    let mut b_id = zero_id();
    let mut x_id = zero_id();
    let ops = d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        {
            let mut a = kids.insert_element(0, b"a");
            a_id = a.id();
            let mut ac = a.children();
            let mut x = ac.insert_element(0, b"x");
            x_id = x.id();
            x.attrs().register(b"id", Scalar::Int(7));
            x.children().insert_element(0, b"grand");
        }
        {
            let b = kids.insert_element(1, b"b");
            b_id = b.id();
        }
    });
    (ops, a_id, b_id, x_id)
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

#[test]
fn a_child_moves_to_a_new_parent() {
    let mut d = Document::new(cid(1));
    let (_ops, _a, b_id, x_id) = frag_with_a_x_b(&mut d);
    assert_eq!(tree(&d, b"doc"), "frag(a(x(grand())),b())");

    d.transact(|tx| tx.move_xml(x_id, b_id, 0));
    assert_eq!(tree(&d, b"doc"), "frag(a(),b(x(grand())))");
}

#[test]
fn a_moved_node_keeps_its_identity_and_subtree() {
    let mut d = Document::new(cid(1));
    let (_ops, _a, b_id, x_id) = frag_with_a_x_b(&mut d);
    d.transact(|tx| tx.move_xml(x_id, b_id, 0));

    // The subtree rode along.
    assert_eq!(tree(&d, b"doc"), "frag(a(),b(x(grand())))");

    // An edit addressed to x's stable id still lands after the move: reach x via
    // b, read its preserved attr.
    let doc = d.get(b"doc").unwrap();
    let Element::XmlFragment(frag) = doc else {
        panic!("root not a fragment")
    };
    let b = frag.borrow().children().borrow().get(1).unwrap();
    let Element::XmlElement(b) = b else {
        panic!("b not an element")
    };
    let x = b.borrow().children().borrow().get(0).unwrap();
    let Element::XmlElement(x) = x else {
        panic!("x not under b")
    };
    assert_eq!(x.borrow().id(), x_id, "x kept its identity across the move");
    let attrs = x.borrow().attrs();
    let got = attrs.borrow().get(b"id");
    match got {
        Some(Element::Register(r)) => assert_eq!(r.borrow().read().clone(), Scalar::Int(7)),
        _ => panic!("x lost its attr"),
    }
}

#[test]
fn a_fresh_replica_converges_on_the_moved_tree() {
    let mut src = Document::new(cid(1));
    let (build, _a, b_id, x_id) = frag_with_a_x_b(&mut src);
    let mv = src.transact(|tx| tx.move_xml(x_id, b_id, 0));

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &mv);
    assert_eq!(tree(&dst, b"doc"), tree(&src, b"doc"));
    assert_eq!(tree(&dst, b"doc"), "frag(a(),b(x(grand())))");
}

#[test]
fn the_move_op_can_arrive_before_the_subtree_it_moves() {
    // A replica that receives the move first must buffer it until the create of
    // its node arrives, then converge — the readiness gate holds it.
    let mut src = Document::new(cid(1));
    let (build, _a, b_id, x_id) = frag_with_a_x_b(&mut src);
    let mv = src.transact(|tx| tx.move_xml(x_id, b_id, 0));

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &mv); // move first — nothing to move yet
    apply_all(&mut dst, &build); // the subtree arrives; the move replays
    assert_eq!(tree(&dst, b"doc"), "frag(a(),b(x(grand())))");
}

#[test]
fn an_atomic_move_waits_for_its_node_to_materialise() {
    // A move shipped as an atomic transaction must not commit before the node it
    // relocates exists — the transaction readiness gate must mirror the single-op
    // one, or the group commits and the move is dropped against a missing node.
    let mut src = Document::new(cid(1));
    let (build, _a, b_id, x_id) = frag_with_a_x_b(&mut src);
    let mv = src.atomic_transact(|tx| tx.move_xml(x_id, b_id, 0));
    assert!(!mv.is_empty(), "the move should emit an op");

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &mv); // atomic move first — x does not exist yet
    apply_all(&mut dst, &build); // x arrives; the buffered move must commit
    assert_eq!(tree(&dst, b"doc"), tree(&src, b"doc"));
    assert_eq!(tree(&dst, b"doc"), "frag(a(),b(x(grand())))");
}

#[test]
fn concurrent_moves_of_the_same_node_converge_to_one_parent() {
    // Two replicas move x to different parents concurrently. The move log picks
    // one winner by stamp; both converge, x has exactly one parent.
    let mut base = Document::new(cid(1));
    let (build, a_id, b_id, x_id) = frag_with_a_x_b(&mut base);

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    apply_all(&mut r1, &build);
    apply_all(&mut r2, &build);

    // r1 moves x under b; r2 moves x back under a (a different placement).
    let m1 = r1.transact(|tx| tx.move_xml(x_id, b_id, 0));
    let m2 = r2.transact(|tx| tx.move_xml(x_id, a_id, 0));

    apply_all(&mut r1, &m2);
    apply_all(&mut r2, &m1);

    assert_eq!(tree(&r1, b"doc"), tree(&r2, b"doc"), "replicas diverged");
    // x appears under exactly one of a / b, never both, never neither.
    let t = tree(&r1, b"doc");
    let under_a = t.contains("a(x(");
    let under_b = t.contains("b(x(");
    assert!(under_a ^ under_b, "x must have exactly one parent: {t}");
}

#[test]
fn a_same_parent_reorder_lands_at_the_requested_index() {
    // Reordering a child within its own parent must not be off by one: the node's
    // own slot is discounted when reading the target index.
    let mut src = Document::new(cid(1));
    let mut ids = [zero_id(); 3];
    let build = src.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        for (i, slot) in ids.iter_mut().enumerate() {
            let e = kids.insert_element(i, &[b'a' + i as u8]);
            *slot = e.id();
        }
    });
    let frag_id = frag_node_id(&src);
    assert_eq!(tree(&src, b"doc"), "frag(a(),b(),c())");

    // Move a (index 0) to index 1 → expect b, a, c.
    let mv = src.transact(|tx| tx.move_xml(ids[0], frag_id, 1));
    assert_eq!(tree(&src, b"doc"), "frag(b(),a(),c())");

    // Converges on a fresh replica.
    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &mv);
    assert_eq!(tree(&dst, b"doc"), "frag(b(),a(),c())");

    // Move a to the end (index 2) → b, c, a.
    src.transact(|tx| tx.move_xml(ids[0], frag_id, 2));
    assert_eq!(tree(&src, b"doc"), "frag(b(),c(),a())");
}

/// The fragment's node id for slot `doc` — the owner used as a move destination.
fn frag_node_id(d: &Document) -> ElementId {
    match d.get(b"doc") {
        Some(Element::XmlFragment(f)) => f.borrow().id(),
        _ => panic!("doc is not a fragment"),
    }
}

#[test]
fn a_move_that_would_create_a_cycle_is_rejected() {
    // Build frag(p(c())): c under p. Concurrently move p under c (a cycle) while
    // the other replica does the reverse-safe move; the cycle move must be
    // dropped so the tree stays acyclic and both replicas agree.
    let mut d = Document::new(cid(1));
    let mut p_id = zero_id();
    let mut c_id = zero_id();
    let build = d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let mut p = kids.insert_element(0, b"p");
        p_id = p.id();
        let mut pc = p.children();
        let c = pc.insert_element(0, b"c");
        c_id = c.id();
    });
    assert_eq!(tree(&d, b"doc"), "frag(p(c()))");

    // Move p under c — p is c's ancestor, so this is a cycle: it must no-op.
    d.transact(|tx| tx.move_xml(p_id, c_id, 0));
    assert_eq!(
        tree(&d, b"doc"),
        "frag(p(c()))",
        "cycle move changed the tree"
    );

    // A fresh replica applying the same ops reaches the same acyclic tree.
    let mv = {
        let mut s = Document::new(cid(9));
        apply_all(&mut s, &build);
        s.transact(|tx| tx.move_xml(p_id, c_id, 0))
    };
    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &mv);
    assert_eq!(tree(&dst, b"doc"), "frag(p(c()))");
}

#[test]
fn moving_a_map_slot_root_is_a_no_op() {
    // A node created straight into a map slot (a document root) has no children
    // placement, so a move of it does nothing — the same on every replica, not a
    // local-only duplication that diverges a peer.
    let mut src = Document::new(cid(1));
    let mut root_id = zero_id();
    let mut host_id = zero_id();
    let build = src.transact(|tx| {
        let e = tx.xml_element(b"root", b"div");
        root_id = e.id();
        let host = tx.xml_element(b"host", b"section");
        host_id = host.id();
    });
    // Attempt to move the map-slot root under host: no-op, no op emitted.
    let mv = src.transact(|tx| tx.move_xml(root_id, host_id, 0));
    assert!(mv.is_empty(), "a map-root move must emit no op");
    assert_eq!(tree(&src, b"root"), "div()");
    assert_eq!(tree(&src, b"host"), "section()");

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &build);
    apply_all(&mut dst, &mv);
    assert_eq!(tree(&dst, b"root"), tree(&src, b"root"));
    assert_eq!(tree(&dst, b"host"), tree(&src, b"host"));
}

#[test]
fn a_concurrent_delete_wins_over_a_move() {
    // One replica deletes a child; another concurrently moves it. The delete must
    // win — the node stays gone, not resurrected under the new parent — and both
    // replicas converge. Fixture: frag(x, b) with x a direct fragment child so it
    // can be deleted by index.
    let mut src = Document::new(cid(1));
    let mut x_id = zero_id();
    let mut b_id = zero_id();
    let build = src.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let x = kids.insert_element(0, b"x");
        x_id = x.id();
        let b = kids.insert_element(1, b"b");
        b_id = b.id();
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    apply_all(&mut r1, &build);
    apply_all(&mut r2, &build);

    // r1 deletes x (fragment child 0); r2 concurrently moves x under b.
    let del = r1.transact(|tx| tx.xml_fragment(b"doc").children().delete(0));
    let mv = r2.transact(|tx| tx.move_xml(x_id, b_id, 0));

    apply_all(&mut r1, &mv);
    apply_all(&mut r2, &del);

    assert_eq!(tree(&r1, b"doc"), tree(&r2, b"doc"), "replicas diverged");
    let t = tree(&r1, b"doc");
    assert!(
        !t.contains("x("),
        "deleted x resurrected under the move: {t}"
    );
    assert_eq!(t, "frag(b())", "only b should remain");
}

#[test]
fn a_moved_tree_survives_a_snapshot() {
    // The move log persists: a replica rebuilt from a snapshot renders the moved
    // tree, not the pre-move one, and a further move still converges.
    let mut src = Document::new(cid(1));
    let (_build, _a, b_id, x_id) = frag_with_a_x_b(&mut src);
    src.transact(|tx| tx.move_xml(x_id, b_id, 0));
    assert_eq!(tree(&src, b"doc"), "frag(a(),b(x(grand())))");

    let bytes = src.encode_state();
    let restored = Document::decode_state(&bytes).unwrap();
    assert_eq!(tree(&restored, b"doc"), "frag(a(),b(x(grand())))");

    // The reloaded replica encodes identically (settled state round-trips).
    assert_eq!(restored.encode_state(), bytes, "re-encode diverged");
}

#[test]
fn a_snapshot_of_a_deleted_moved_node_stays_deleted() {
    // A node moved on one replica and deleted on another converges to gone; a
    // snapshot of that state must stay gone — the deleted node's tombstoned
    // placement (stamp retained, value dropped) is stored explicitly, so the fold
    // still hides every placement after a reload.
    let mut base = Document::new(cid(1));
    let mut x_id = zero_id();
    let mut b_id = zero_id();
    let build = base.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let x = kids.insert_element(0, b"x");
        x_id = x.id();
        let b = kids.insert_element(1, b"b");
        b_id = b.id();
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    apply_all(&mut r1, &build);
    apply_all(&mut r2, &build);
    let del = r1.transact(|tx| tx.xml_fragment(b"doc").children().delete(0)); // delete x
    let mv = r2.transact(|tx| tx.move_xml(x_id, b_id, 0)); // move x under b
    apply_all(&mut r1, &mv);
    apply_all(&mut r2, &del);
    assert_eq!(tree(&r1, b"doc"), "frag(b())");

    let bytes = r1.encode_state();
    let restored = Document::decode_state(&bytes).unwrap();
    assert_eq!(
        tree(&restored, b"doc"),
        "frag(b())",
        "deleted move resurrected"
    );
    assert_eq!(restored.encode_state(), bytes, "re-encode diverged");
}

#[test]
fn a_deleted_never_moved_node_keeps_its_placement_across_a_snapshot() {
    // A node created then deleted but never moved has a single tombstoned
    // placement — unrecoverable from the list on decode (its value is dropped by
    // tombstone compression), so the snapshot must store it. A concurrent move of
    // that node, atomic with a sibling edit, must become ready on a reloaded
    // replica; if the placement is lost the whole group stalls and the sibling
    // edit is dropped, diverging from a replica that applied the ops directly.
    let mut author = Document::new(cid(1));
    let mut x_id = zero_id();
    let mut b_id = zero_id();
    let build = author.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        let x = kids.insert_element(0, b"x");
        x_id = x.id();
        let b = kids.insert_element(1, b"b");
        b_id = b.id();
    });
    let del = author.transact(|tx| tx.xml_fragment(b"doc").children().delete(0)); // delete x

    // A second replica that saw the build but not the delete authors an atomic
    // group: move x (still live to it) under b, plus insert a sibling c.
    let mut mover = Document::new(cid(2));
    apply_all(&mut mover, &build);
    let grp = mover.atomic_transact(|tx| {
        tx.move_xml(x_id, b_id, 0);
        tx.xml_fragment(b"doc").children().insert_element(2, b"c");
    });

    // Direct replica: build, delete, then the group — x is gone (delete wins), c
    // lands, so the group's sibling edit is visible.
    let mut direct = Document::new(cid(3));
    apply_all(&mut direct, &build);
    apply_all(&mut direct, &del);
    apply_all(&mut direct, &grp);
    assert_eq!(tree(&direct, b"doc"), "frag(b(),c())");

    // Reloaded replica: build, delete, snapshot, reload, then the group. The
    // deleted node's placement must survive the reload or the group never commits.
    let mut reload = Document::new(cid(4));
    apply_all(&mut reload, &build);
    apply_all(&mut reload, &del);
    let bytes = reload.encode_state();
    let mut reload = Document::decode_state(&bytes).unwrap();
    apply_all(&mut reload, &grp);

    assert_eq!(
        tree(&reload, b"doc"),
        tree(&direct, b"doc"),
        "reload stalled the group after losing the deleted node's placement"
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
    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

#[test]
fn random_moves_converge_across_orderings() {
    let seeds: u64 = if cfg!(miri) { 4 } else { 80 };
    for seed in 0..seeds {
        let mut rng = Rng::new(seed);
        // Author on one replica: a fragment with four elements, then a batch of
        // random moves relocating them under one another.
        let mut src = Document::new(cid(1));
        let mut ids = [zero_id(); 4];
        let mut log: Vec<Op> = src.transact(|tx| {
            let mut frag = tx.xml_fragment(b"doc");
            let mut kids = frag.children();
            for (i, slot) in ids.iter_mut().enumerate() {
                let e = kids.insert_element(i, &[b'a' + i as u8]);
                *slot = e.id();
            }
        });

        for _ in 0..10 {
            let node = ids[rng.below(4)];
            let parent = ids[rng.below(4)];
            let idx = rng.below(2);
            let mut mv = src.transact(|tx| tx.move_xml(node, parent, idx));
            log.append(&mut mv);
        }

        // Replica two applies the identical op set in a shuffled order.
        let mut shuffled = log.clone();
        for i in (1..shuffled.len()).rev() {
            let j = rng.below(i + 1);
            shuffled.swap(i, j);
        }
        let mut dst = Document::new(cid(2));
        apply_all(&mut dst, &shuffled);

        assert_eq!(
            tree(&dst, b"doc"),
            tree(&src, b"doc"),
            "seed {seed}: orderings diverged"
        );
    }
}
