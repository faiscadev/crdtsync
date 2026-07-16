//! Doc-ACL snapshot projection — `Document::project_read_paths`.
//!
//! The state half of the per-path read redaction: a compacted room's cold-start
//! snapshot narrowed to a partial reader's readable paths, so a subtree-scoped reader
//! is served (like a zone-limited subscriber) rather than refused. The server passes in
//! the composed doc-ACL read verdict at each `core::path` — the same authority the per-op
//! fan-out gates each op on — and this projection retains the readable elements and drops
//! the rest, the doc-ACL analogue of `project_zones`. An unreadable container drops its
//! whole subtree (every prefix of a path must read for the element to survive); a
//! leaf-level deny drops just that slot; a root the reader cannot read whole loses its
//! own leaf slots and its ACL grants.

use crdtsync_core::acl::{AclGrant, AclScope, AclSubject, Capability};
use crdtsync_core::path::{
    encode_path, parse_path, xml_children_len, xml_fragment, xml_insert_element, xml_move_child,
};
use crdtsync_core::{
    zone, AclEffect, Document, Element, ElementId, Op, OpKind, RangeAnchor, Scalar, Schema, Side,
};

mod common;
use common::cid;

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) plus unzoned root-partition
/// slots — the fixture the doc-ACL × zones composition test stacks both projections on.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "secret": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

fn doc() -> Document {
    Document::new(cid(1))
}

/// A read predicate admitting a path iff its first key is in `keys` (so a whole subtree
/// is admitted), with the document root (empty path) admitted per `root`. The common
/// shape: a reader granted read on a set of top-level subtrees, optionally on the root.
fn reads_top(root: bool, keys: &'static [&'static [u8]]) -> impl Fn(&[Vec<u8>]) -> bool {
    move |path: &[Vec<u8>]| match path.first() {
        None => root,
        Some(k) => keys.contains(&k.as_slice()),
    }
}

/// The Int behind `outer.inner`, or `None` when either level is absent.
fn nested_reg(d: &Document, outer: &[u8], inner: &[u8]) -> Option<i64> {
    let m = match d.get(outer) {
        Some(Element::Map(m)) => m,
        _ => return None,
    };
    let child = m.borrow().get(inner);
    match child {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => panic!("expected an Int register"),
        },
        None => None,
        _ => panic!("expected a register or nothing"),
    }
}

fn counter(d: &Document, key: &[u8]) -> Option<i64> {
    match d.get(key) {
        Some(Element::Counter(c)) => Some(c.borrow().read()),
        None => None,
        _ => panic!("expected a counter or nothing"),
    }
}

/// Grant read on `path` to a fixed subject, authored by a fixed grantor — returns the
/// emitted ops so an op-served replica can selectively apply them.
fn grant_read(d: &mut Document, path: &[u8]) -> Vec<Op> {
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(9)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            path.to_vec(),
            cid(1),
        );
    })
}

/// The governing paths of a document's live ACL tuples, sorted.
fn acl_paths(d: &Document) -> Vec<Vec<u8>> {
    let mut p: Vec<Vec<u8>> = d
        .acl_tuples()
        .into_iter()
        .map(|t| match t.scope {
            AclScope::Path(p) => p,
            AclScope::Element(_) => unreachable!("fixtures grant only path scopes"),
        })
        .collect();
    p.sort();
    p
}

#[test]
fn acl_tuples_are_kept_on_readable_paths_and_dropped_on_unreadable_ones() {
    let mut d = doc();
    grant_read(&mut d, &encode_path(&[b"a"]));
    grant_read(&mut d, &encode_path(&[b"b"]));
    // Reads /a (and root), not /b.
    d.project_read_paths(reads_top(true, &[b"a"]));
    assert_eq!(
        acl_paths(&d),
        vec![encode_path(&[b"a"])],
        "the /a tuple is kept, the /b tuple dropped — ACL state redacted by governing path",
    );
}

#[test]
fn a_whole_document_reader_keeps_every_acl_tuple() {
    let mut d = doc();
    grant_read(&mut d, &encode_path(&[b"a"]));
    grant_read(&mut d, &encode_path(&[b"b"]));
    grant_read(&mut d, &encode_path(&[]));
    let before = acl_paths(&d);
    d.project_read_paths(|_| true);
    assert_eq!(
        acl_paths(&d),
        before,
        "an identity projection keeps every ACL tuple",
    );
}

#[test]
fn a_subtree_readers_acl_tuple_survives_even_when_the_root_is_unreadable() {
    // The refinement over the old root-gated rule: a reader denied the root still keeps
    // the ACL tuples on subtrees it may read — and NONE on subtrees (or the root) it
    // cannot. No subject/effect/path of an unreadable tuple leaks.
    let mut d = doc();
    grant_read(&mut d, &encode_path(&[b"a"])); // on readable /a
    grant_read(&mut d, &encode_path(&[b"b"])); // on unreadable /b
    grant_read(&mut d, &encode_path(&[])); // on the unreadable root
    d.project_read_paths(reads_top(false, &[b"a"]));
    assert_eq!(
        acl_paths(&d),
        vec![encode_path(&[b"a"])],
        "only the /a tuple survives; the /b and root tuples are dropped",
    );
}

#[test]
fn op_join_and_snapshot_join_materialize_the_same_acl_subset() {
    // Convergence: an op-served reader applies only the AclGrant ops on its readable
    // paths; a snapshot-served reader is projected. Same path authority ⇒ same ACL set.
    let reads = reads_top(false, &[b"a"]);
    let grant_paths = [encode_path(&[b"a"]), encode_path(&[b"b"]), encode_path(&[])];

    // snapshot-join: full document, then projected.
    let mut snap = doc();
    for p in &grant_paths {
        grant_read(&mut snap, p);
    }
    snap.project_read_paths(&reads);

    // op-join: apply an authoring replica's grant ops only where the path reads.
    let mut authoring = doc();
    let mut ops_replica = doc();
    for p in &grant_paths {
        let ops = grant_read(&mut authoring, p);
        if reads(&parse_path(p).unwrap()) {
            for op in ops {
                ops_replica.apply(&op);
            }
        }
    }

    assert_eq!(
        acl_paths(&snap),
        acl_paths(&ops_replica),
        "the snapshot-served and op-served readers materialize the same ACL subset",
    );
    assert_eq!(
        acl_paths(&snap),
        vec![encode_path(&[b"a"])],
        "both hold exactly the /a tuple",
    );
}

#[test]
fn an_authorized_subtree_is_retained_and_an_unauthorized_one_dropped() {
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"a").register(b"v", Scalar::Int(1));
        tx.map(b"b").register(b"v", Scalar::Int(2));
    });
    d.project_read_paths(reads_top(true, &[b"a"]));
    assert_eq!(
        nested_reg(&d, b"a", b"v"),
        Some(1),
        "authorized /a is retained"
    );
    assert!(d.get(b"b").is_none(), "unauthorized /b is dropped");
}

#[test]
fn an_unauthorized_container_drops_its_whole_subtree() {
    let mut d = doc();
    d.transact(|tx| {
        let mut a = tx.map(b"a");
        a.register(b"v", Scalar::Int(1));
        a.map(b"inner").register(b"deep", Scalar::Int(9));
    });
    // Authorize only the root: /a and every descendant it holds are dropped.
    d.project_read_paths(reads_top(true, &[]));
    assert!(d.get(b"a").is_none(), "the unauthorized container is gone");
    // No dangling descendant is left behind — a re-encode round-trips canonically.
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert!(back.get(b"a").is_none());
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn an_all_deny_predicate_yields_an_empty_document() {
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"a").register(b"v", Scalar::Int(1));
        tx.register(b"note", Scalar::Int(7));
    });
    d.project_read_paths(|_| false);
    assert!(d.get(b"a").is_none(), "the subtree is dropped");
    assert!(
        d.get(b"note").is_none(),
        "a root leaf is dropped when the root is unreadable"
    );
    // An emptied projection still round-trips through the codec.
    let back = Document::decode_state(&d.encode_state()).unwrap();
    assert!(back.get(b"a").is_none());
    assert!(back.get(b"note").is_none());
}

#[test]
fn an_all_admit_predicate_is_an_identity_projection() {
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"a").register(b"v", Scalar::Int(1));
        tx.map(b"b").register(b"v", Scalar::Int(2));
        tx.register(b"note", Scalar::Int(7));
    });
    let before = d.encode_state();
    d.project_read_paths(|_| true);
    assert_eq!(
        d.encode_state(),
        before,
        "a whole-document projection is byte-identical"
    );
}

#[test]
fn a_projected_document_round_trips_through_the_state_codec() {
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"a").register(b"v", Scalar::Int(1));
        tx.map(b"b").register(b"v", Scalar::Int(2));
    });
    d.project_read_paths(reads_top(true, &[b"a"]));
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(nested_reg(&back, b"a", b"v"), Some(1));
    assert!(back.get(b"b").is_none());
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn root_leaves_survive_only_when_the_root_is_readable() {
    let build = || {
        let mut d = doc();
        d.transact(|tx| {
            tx.map(b"a").register(b"v", Scalar::Int(1));
            tx.register(b"note", Scalar::Int(7));
            tx.inc(b"tally", 5);
        });
        d
    };

    // Root unreadable (reads /a only): /a survives (subtree grant), but the root's own
    // leaf slots are cut — and the counter's registry entry is pruned, so no phantom
    // tally resurfaces when the slot is re-won.
    let mut d = build();
    d.project_read_paths(reads_top(false, &[b"a"]));
    assert_eq!(nested_reg(&d, b"a", b"v"), Some(1), "the subtree survives");
    assert!(d.get(b"note").is_none(), "the root register is cut");
    assert_eq!(counter(&d, b"tally"), None, "the root counter is cut");
    d.transact(|tx| tx.inc(b"tally", 3));
    assert_eq!(
        counter(&d, b"tally"),
        Some(3),
        "no phantom tally is re-adopted at the cut counter's id"
    );

    // Root readable but /a denied: the root's leaves survive, /a is still dropped.
    let mut d = build();
    d.project_read_paths(|path: &[Vec<u8>]| path.first().is_none_or(|k| k != b"a"));
    assert!(
        d.get(b"note").is_some(),
        "a readable root register survives"
    );
    assert_eq!(
        counter(&d, b"tally"),
        Some(5),
        "a readable root counter survives"
    );
    assert!(
        d.get(b"a").is_none(),
        "the denied /a subtree is still dropped"
    );
}

#[test]
fn a_leaf_level_deny_drops_the_slot_inside_a_readable_container() {
    // A reader granted read on /a but denied the single leaf /a/x. The container stays,
    // but the denied leaf is cut — matching the per-op redaction, which withholds the
    // keyed op at path /a/x while delivering /a/y.
    let mut d = doc();
    d.transact(|tx| {
        let mut a = tx.map(b"a");
        a.register(b"x", Scalar::Int(1));
        a.register(b"y", Scalar::Int(2));
    });
    d.project_read_paths(|path: &[Vec<u8>]| path != [b"a".to_vec(), b"x".to_vec()]);
    assert_eq!(nested_reg(&d, b"a", b"x"), None, "the denied leaf is cut");
    assert_eq!(
        nested_reg(&d, b"a", b"y"),
        Some(2),
        "the readable leaf survives"
    );
    // The cut survives a round-trip through the codec.
    let back = Document::decode_state(&d.encode_state()).unwrap();
    assert_eq!(nested_reg(&back, b"a", b"x"), None);
    assert_eq!(nested_reg(&back, b"a", b"y"), Some(2));
}

#[test]
fn a_grant_below_a_denied_ancestor_leaves_no_orphan_in_the_snapshot() {
    // A grant on the nested /a/b whose ancestor /a is denied. Op catch-up withholds the
    // /a create, so /a/b's create can never apply on an op-served joiner — it materializes
    // neither. The projection must drop /a/b too (every prefix of a path must read), so no
    // orphaned subtree is left in the registries for `encode_state` to emit.
    let marker = b"ORPHAN_MARKER".to_vec();
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"a")
            .map(b"b")
            .register(b"deep", Scalar::Bytes(marker.clone()));
    });
    // /a/b reads, but its ancestor /a does not.
    d.project_read_paths(|path: &[Vec<u8>]| path != [b"a".to_vec()]);
    assert!(d.get(b"a").is_none(), "the denied ancestor is dropped");
    let bytes = d.encode_state();
    assert!(
        !bytes.windows(marker.len()).any(|w| w == marker.as_slice()),
        "the orphaned /a/b subtree leaves no trace in the snapshot",
    );
    // And it round-trips clean.
    let back = Document::decode_state(&bytes).unwrap();
    assert!(back.get(b"a").is_none());
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn a_node_moved_into_a_denied_subtree_is_kept_at_its_readable_origin() {
    // A child born under the readable /a fragment then moved under the denied /b
    // fragment. Op catch-up delivers the create (its birth path /a reads) but withholds
    // the move (an XmlMove's read path is the denied destination /b), so an op-served
    // reader keeps the child in /a, never learning it left. The projection must too:
    // dropping it by its current /b position would diverge from the op stream and leave
    // the child's birth slot dangling in the retained /a children list.
    let mut d = doc();
    xml_fragment(&mut d, &encode_path(&[b"a"]));
    xml_fragment(&mut d, &encode_path(&[b"b"]));
    xml_insert_element(&mut d, &encode_path(&[b"a"]), 0, b"card");
    xml_move_child(&mut d, &encode_path(&[b"a"]), 0, &encode_path(&[b"b"]), 0);
    assert_eq!(
        xml_children_len(&d, &encode_path(&[b"b"])),
        Some(1),
        "the live tree renders the moved child under /b",
    );

    d.project_read_paths(reads_top(false, &[b"a"]));
    assert!(d.get(b"b").is_none(), "the denied /b fragment is dropped");

    // The projection filters the move state only in the persisted log (like
    // `project_zones`), so it is sound solely as the final transform before a re-encode:
    // a decoded joiner is what must converge. Decoding replays the filtered log — the
    // move into /b is gone — and re-folds the child back under its readable origin /a,
    // with no dangling reference to the dropped /b subtree, so the re-encode is canonical.
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).expect("the projected snapshot decodes");
    assert_eq!(
        xml_children_len(&back, &encode_path(&[b"a"])),
        Some(1),
        "a snapshot joiner re-folds the moved-into-denied child at its readable origin /a",
    );
    assert!(back.get(b"b").is_none());
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

/// The live-children count of the first XML child under the fragment at raw map-slot key
/// `key` — the grandchildren of a `frag(card(...))` shape — or `None` if the shape differs.
fn first_child_grandchildren(d: &Document, key: &[u8]) -> Option<usize> {
    let f = match d.get(key) {
        Some(Element::XmlFragment(f)) => f,
        _ => return None,
    };
    let children = f.borrow().children();
    let vals = children.borrow().values();
    match vals.first() {
        Some(Element::XmlElement(card)) => Some(card.borrow().children().borrow().len()),
        _ => None,
    }
}

#[test]
fn a_node_kept_at_its_origin_drops_the_subtree_it_grew_in_the_denied_position() {
    // A `card` element born under readable /a with a `gc` grandchild, then moved into
    // denied /b. Op catch-up keeps the card at /a (its create read, its move withheld) but
    // never delivers the grandchild — its create's read path is the card's denied /b
    // position — so the op joiner holds the card with an EMPTY subtree. The projection must
    // keep the card at /a and CLEAR the subtree it grew in the denied position: no dangling
    // reference to the dropped grandchild, and the same empty card the op joiner folds.
    let mut d = doc();
    d.transact(|tx| {
        tx.xml_fragment(b"b");
        let mut fa = tx.xml_fragment(b"a");
        let mut kids = fa.children();
        let mut card = kids.insert_element(0, b"card");
        card.children().insert_element(0, b"gc");
    });
    xml_move_child(&mut d, &encode_path(&[b"a"]), 0, &encode_path(&[b"b"]), 0);
    assert_eq!(
        first_child_grandchildren(&d, b"b"),
        Some(1),
        "the live tree renders the card (with its grandchild) under /b",
    );

    d.project_read_paths(reads_top(false, &[b"a"]));
    assert!(d.get(b"b").is_none(), "the denied /b fragment is dropped");

    // A decoded joiner re-folds the card at /a with no grandchild and no dangling ref.
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).expect("the projected snapshot decodes");
    assert_eq!(
        xml_children_len(&back, &encode_path(&[b"a"])),
        Some(1),
        "the card is kept at its readable origin /a",
    );
    assert_eq!(
        first_child_grandchildren(&back, b"a"),
        Some(0),
        "the grandchild grown in the denied /b is dropped — the card's subtree is empty",
    );
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn reveal_ops_reveals_a_born_denied_node_and_converges_with_the_projection() {
    // The mirror of `a_node_moved_into_a_denied_subtree_is_kept_at_its_readable_origin`:
    // a `card` born under the DENIED /b fragment (its create's read path is /b, withheld)
    // then moved into readable /a. Op catch-up withholds the create, so the reader's
    // readable move op cannot materialize the card — `reveal_ops` supplies a shell that
    // does. The projection keeps the card at /a; an op joiner given the shell + the
    // readable move converges with it, learning nothing of the /b origin.
    let reads = reads_top(false, &[b"a"]);

    // Author the scenario, capturing the ops per step so the readable subset can be
    // replayed without the denied ones.
    let mut author = doc();
    let a = xml_fragment(&mut author, &encode_path(&[b"a"]));
    let b = xml_fragment(&mut author, &encode_path(&[b"b"]));
    let birth = xml_insert_element(&mut author, &encode_path(&[b"b"]), 0, b"card");
    let mv = xml_move_child(
        &mut author,
        &encode_path(&[b"b"]),
        0,
        &encode_path(&[b"a"]),
        0,
    );

    // The projection keeps the card at its readable current position /a.
    let mut snap = doc();
    for op in a.iter().chain(&b).chain(&birth).chain(&mv) {
        snap.apply(op);
    }
    snap.project_read_paths(&reads);
    let back = Document::decode_state(&snap.encode_state()).expect("projected snapshot decodes");
    assert_eq!(xml_children_len(&back, &encode_path(&[b"a"])), Some(1));
    assert!(back.get(b"b").is_none());

    // `reveal_ops` yields exactly one shell — the born-denied, now-readable card — and it
    // is an `XmlReveal` carrying only the node's identity and tag (no /b origin op).
    let shells = author.reveal_ops(&reads);
    assert_eq!(
        shells.len(),
        1,
        "one shell: the born-denied, now-readable card"
    );
    assert!(
        matches!(&shells[0].kind, OpKind::XmlReveal { tag, .. } if tag.as_deref() == Some(b"card")),
        "the shell reveals the card's current tag",
    );

    // A born-READABLE node is delivered by its ordinary create — no shell — and a
    // whole-document reader has nothing denied, so it too gets none.
    let mut fwd = doc();
    for op in a.iter().chain(&b) {
        fwd.apply(op);
    }
    for op in &xml_insert_element(&mut fwd, &encode_path(&[b"a"]), 0, b"card") {
        fwd.apply(op);
    }
    for op in &xml_move_child(&mut fwd, &encode_path(&[b"a"]), 0, &encode_path(&[b"b"]), 0) {
        fwd.apply(op);
    }
    assert!(
        fwd.reveal_ops(&reads).is_empty(),
        "a node born readable then moved into denied is not revealed (it is kept at origin)",
    );
    assert!(
        author.reveal_ops(|_| true).is_empty(),
        "a whole-document reader is revealed nothing",
    );

    // Op-join: a fresh reader applies only its readable ops — the /a create, the shell,
    // and the readable move — never the /b creates or the card's denied birth. It
    // converges with the projection on the materialized tree and re-encodes canonically.
    let mut op_join = doc();
    for op in a.iter() {
        op_join.apply(op);
    }
    for op in shells.iter().chain(&mv) {
        op_join.apply(op);
    }
    assert_eq!(
        xml_children_len(&op_join, &encode_path(&[b"a"])),
        Some(1),
        "the op joiner is revealed the card at /a",
    );
    assert!(
        op_join.get(b"b").is_none(),
        "the op joiner never learns of /b"
    );
    assert_eq!(
        xml_children_len(&op_join, &encode_path(&[b"a"])),
        xml_children_len(&back, &encode_path(&[b"a"])),
        "op-join and snapshot-join converge on the card at /a",
    );
    let bytes = op_join.encode_state();
    assert_eq!(
        Document::decode_state(&bytes).unwrap().encode_state(),
        bytes,
        "the op-joined replica re-encodes canonically",
    );
}

#[test]
fn a_reader_limited_by_both_doc_acl_and_zones_gets_the_intersection() {
    // The server stacks the two projections in sequence — doc-ACL read paths, then
    // zones. A reader limited by both is served their intersection, and neither
    // transform disturbs the other.
    let schema = Schema::parse(ZONED).expect("zoned schema parses");
    let mut d = doc();
    d.set_schema(schema.clone());
    d.transact(|tx| {
        tx.map(b"board").register(b"v", Scalar::Int(1));
        tx.map(b"notes").register(b"v", Scalar::Int(2));
        tx.map(b"secret").register(b"v", Scalar::Int(3));
        tx.map(b"loose").register(b"v", Scalar::Int(4));
    });

    // doc-ACL: readable on the root, /board and /loose only — /secret and /notes denied.
    d.project_read_paths(reads_top(true, &[b"board", b"loose"]));
    // zones: scoped to za (= /board); the unzoned root partition is always carried.
    let za = zone::zone_id_of(&schema, &[b"board".to_vec()]).expect("za resolves");
    d.project_zones(&schema, &std::collections::HashSet::from([za]));

    // Intersection: /board survives both; /notes is dropped by both; /secret is dropped
    // by doc-ACL though zones would keep the root partition; /loose (readable, root
    // partition) survives both.
    assert!(d.get(b"board").is_some(), "za ∩ readable keeps /board");
    assert!(
        d.get(b"loose").is_some(),
        "the readable root partition survives"
    );
    assert!(
        d.get(b"notes").is_none(),
        "/notes is dropped by both projections"
    );
    assert!(
        d.get(b"secret").is_none(),
        "/secret is dropped by doc-ACL even though zones keep the root partition"
    );
    // The stacked projection round-trips through the codec.
    let back = Document::decode_state(&d.encode_state()).unwrap();
    assert!(back.get(b"board").is_some() && back.get(b"secret").is_none());
}

/// Create a top-level List holding `items` bytes items, returning the emitted ops so
/// an op-served replica can selectively apply them.
fn list_ops(d: &mut Document, key: &[u8], items: usize) -> Vec<Op> {
    let mut ops = Vec::new();
    for i in 0..items {
        ops.extend(crdtsync_core::path::list_insert(
            d,
            &encode_path(&[key]),
            i,
            b"x",
        ));
    }
    ops
}

/// A range endpoint at `index` in the top-level List `key`, gravity left.
fn anchor(d: &Document, key: &[u8], index: usize) -> RangeAnchor {
    let seq = match d.get(key) {
        Some(Element::List(l)) => l.borrow().id(),
        _ => panic!("expected a live list at {key:?}"),
    };
    RangeAnchor {
        seq,
        pos: crdtsync_core::path::relative_position(d, &encode_path(&[key]), index, Side::Left)
            .expect("a live sequence yields a position"),
    }
}

/// A RangedElement spanning `[start_key[0], end_key[1])`; the two keys may name
/// different sequences (a cross-element range). Returns the emitted ops and its id.
fn make_range(d: &mut Document, start_key: &[u8], end_key: &[u8]) -> (Vec<Op>, ElementId) {
    let start = anchor(d, start_key, 0);
    let end = anchor(d, end_key, 1);
    let mut id = None;
    let ops = d.transact(|tx| {
        id = Some(tx.ranged().create(start, end, Scalar::Bool(true)));
    });
    (ops, id.expect("a create emits a range id"))
}

/// The ids of a document's live RangedElements, sorted.
fn ranged_ids(d: &Document) -> Vec<ElementId> {
    let mut v: Vec<ElementId> = d.ranged_elements().into_iter().map(|r| r.id).collect();
    v.sort_by_key(|id| id.as_bytes());
    v
}

#[test]
fn a_single_sequence_mark_is_kept_only_where_its_anchor_seq_reads() {
    let mut d = doc();
    list_ops(&mut d, b"a", 2);
    list_ops(&mut d, b"b", 2);
    let (_, mark_a) = make_range(&mut d, b"a", b"a"); // both endpoints in /a
    let (_, _mark_b) = make_range(&mut d, b"b", b"b"); // both endpoints in /b
                                                       // Reads /a (and root), not /b.
    d.project_read_paths(reads_top(true, &[b"a"]));
    assert_eq!(
        ranged_ids(&d),
        vec![mark_a],
        "the /a mark is kept, the /b mark dropped — a range rides its anchor seq path",
    );
}

#[test]
fn a_cross_element_range_needs_read_on_both_anchor_seqs() {
    // start anchored in /a, end anchored in /b — two governing paths, require-all.
    let build = || {
        let mut d = doc();
        list_ops(&mut d, b"a", 2);
        list_ops(&mut d, b"b", 2);
        let (_, id) = make_range(&mut d, b"a", b"b");
        (d, id)
    };

    // A reader of both anchor seqs keeps it.
    let (mut both, id) = build();
    both.project_read_paths(reads_top(true, &[b"a", b"b"]));
    assert_eq!(
        ranged_ids(&both),
        vec![id],
        "a reader of both /a and /b keeps the cross-element range",
    );

    // A reader of only /a drops it — the /b endpoint is unreadable (require-all).
    let (mut only_a, _) = build();
    only_a.project_read_paths(reads_top(true, &[b"a"]));
    assert!(
        ranged_ids(&only_a).is_empty(),
        "a reader of only /a drops a range spanning into unreadable /b",
    );

    // A whole-document reader keeps it (identity projection).
    let (mut whole, id2) = build();
    whole.project_read_paths(|_| true);
    assert_eq!(
        ranged_ids(&whole),
        vec![id2],
        "a whole-document reader keeps the cross-element range",
    );
}

#[test]
fn op_join_and_snapshot_join_materialize_the_same_ranged_subset() {
    // Convergence: an op-served reader receives a RangedCreate only where it reads all
    // of the op's anchor seq paths (the server's require-all fan-out); a snapshot-served
    // reader is projected by the same rule. Same path authority ⇒ same ranged subset.
    let reads = reads_top(false, &[b"a"]); // reads /a only, not the root or /b

    // authoring: lists /a and /b, a mark wholly in /a, a cross range /a→/b.
    let mut authoring = doc();
    let list_a = list_ops(&mut authoring, b"a", 2);
    let list_b = list_ops(&mut authoring, b"b", 2);
    let (mark_ops, mark_a) = make_range(&mut authoring, b"a", b"a");
    let (cross_ops, _cross) = make_range(&mut authoring, b"a", b"b");

    // snapshot-join: the full authoring document, projected.
    let mut snap = Document::decode_state(&authoring.encode_state()).unwrap();
    snap.project_read_paths(&reads);

    // op-join: apply only the ops on readable paths — the /a sequence and the /a mark
    // (both endpoints readable). Same op objects as authoring, so the ids match. The /b
    // sequence and the cross range (its /b endpoint unread) are withheld.
    let mut ops_replica = doc();
    for op in list_a.iter().chain(&mark_ops) {
        ops_replica.apply(op);
    }
    let _ = (list_b, cross_ops);

    assert_eq!(
        ranged_ids(&snap),
        ranged_ids(&ops_replica),
        "the snapshot-served and op-served readers materialize the same ranged subset",
    );
    assert_eq!(
        ranged_ids(&snap),
        vec![mark_a],
        "both hold exactly the /a mark",
    );
}

#[test]
fn a_range_whose_anchor_seq_is_deleted_falls_back_to_root_gating() {
    // Once a range's anchor sequence leaves the tree (deleted or re-parented away), its
    // anchor no longer resolves, so it is gated by root — fail-closed. A partial reader
    // drops the orphaned range; a whole-document reader keeps it. This is the same
    // fallback op_read_paths applies on the op seam, so a fresh op-served and a fresh
    // snapshot-served reader converge on dropping it.
    let build = || {
        let mut d = doc();
        list_ops(&mut d, b"a", 2);
        let (_, id) = make_range(&mut d, b"a", b"a");
        crdtsync_core::path::delete(&mut d, &encode_path(&[b"a"])); // orphan the range
        (d, id)
    };

    let (mut partial, _) = build();
    partial.project_read_paths(reads_top(false, &[b"a"]));
    assert!(
        ranged_ids(&partial).is_empty(),
        "a partial reader drops a range whose deleted anchor falls back to root",
    );

    let (mut whole, id) = build();
    whole.project_read_paths(|_| true);
    assert_eq!(
        ranged_ids(&whole),
        vec![id],
        "a whole-document reader keeps the orphaned range",
    );
}
