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

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::{encode_path, parse_path};
use crdtsync_core::{zone, AclEffect, Document, Element, Op, Scalar, Schema};

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
    let mut p: Vec<Vec<u8>> = d.acl_tuples().into_iter().map(|t| t.path).collect();
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
