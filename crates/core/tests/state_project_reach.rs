//! What a snapshot projection does with state the live walk does not reach.
//!
//! Both projections build their purge set by walking the live tree from the root, so an
//! element the walk does not reach is in no zone and at no path, and every retain that
//! asks "is this purged" keeps it. That is the right answer for a *container*: a deleted
//! or displaced container's registry entry is what displace-then-recreate identity
//! retains, and dropping it would lose content a subscriber entitled to the partition
//! gets back when the slot is re-won (C67).
//!
//! It is the wrong answer for the id-keyed registries beside it. An annotation is
//! tombstoned only by an explicit delete, so deleting the container that held the
//! annotated sequence leaves the mark with an anchor resolving to nothing; an
//! element-scoped ACL tuple outlives its target the same way. Neither names a path or a
//! partition any more, so no verdict places it — and the fallbacks that stood in (the
//! root read verdict, "no zone means the root partition") are wider than the reader
//! entitled to it: a root grant a subtree deny carves passes the root query, and every
//! zone-scoped subscriber holds the root partition. So a reader was served a mark's
//! name, its payload and its anchor's element id, or a tuple's subject and effect, out of
//! a region it may not read.
//!
//! The zone half of that is unrecoverable rather than merely unresolved: the key a
//! container was derived under is one-way, so a sequence the walk does not reach cannot be
//! re-attributed to a partition at all.
//!
//! The rule both projections take instead: drop it. Neither transform ever runs except
//! to narrow — the server declines to project for a reader denied nothing and for a
//! subscriber holding every declared zone — so the drop needs no whole-view test of its
//! own here, and deliberately does not re-derive one: whether a reader is denied
//! anywhere is authority state, not document state, and a projection that re-derived it
//! from the tuples it happens to carry would answer differently at every seam that
//! serves an *archived* state (a version, a branch base, a diff side), whose tuple set
//! is not the live one. `op_read_gate` admits the matching ops to exactly the readers the
//! caller declines to project for, so op-join and snapshot-join still materialize the
//! same subset; that half is pinned in `crates/server/tests/acl_redaction.rs`, where both
//! seams are real.

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::{delete, encode_path, relative_position};
use crdtsync_core::{
    zone, AclEffect, Document, Element, ElementId, RangeAnchor, Scalar, Schema, Side,
};
use std::collections::HashSet;

mod common;
use common::cid;

/// A schema declaring `za` over `/board` and `zb` over `/notes`, permissive on structure
/// so any path is writable — only the zone block matters to partitioning.
const ZONED: &str = r#"{ "schema": "z", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "zones": { "za": "/board", "zb": "/notes" } }"#;

fn doc() -> Document {
    Document::new(cid(1))
}

fn zoned() -> Schema {
    Schema::parse(ZONED).expect("the zoned schema parses")
}

/// A read predicate admitting a path iff its first key is in `keys` (so a whole subtree
/// is admitted), with the document root (empty path) admitted per `root`.
fn reads_top(root: bool, keys: &'static [&'static [u8]]) -> impl Fn(&[Vec<u8>]) -> bool {
    move |path: &[Vec<u8>]| match path.first() {
        None => root,
        Some(k) => keys.contains(&k.as_slice()),
    }
}

/// A List of two items at `path`, created on the way.
fn seq(d: &mut Document, path: &[&[u8]]) {
    for i in 0..2 {
        crdtsync_core::path::list_insert(d, &encode_path(path), i, b"x");
    }
}

/// The id of the List at `path` — one or two key segments deep.
fn list_id(d: &Document, path: &[&[u8]]) -> ElementId {
    let elem = match path {
        [key] => d.get(key),
        [outer, key] => match d.get(outer) {
            Some(Element::Map(m)) => m.borrow().get(key),
            _ => panic!("expected a live map at {outer:?}"),
        },
        _ => panic!("list_id takes one or two segments"),
    };
    match elem {
        Some(Element::List(l)) => {
            let id = l.borrow().id();
            id
        }
        _ => panic!("expected a live list at {path:?}"),
    }
}

/// A mark spanning `[0, 1)` of the List at `path` whose payload is a composite Map
/// holding `secret` at key `k`, returning its RangedElement id and the payload's id.
fn composite_mark(d: &mut Document, path: &[&[u8]], secret: &[u8]) -> (ElementId, ElementId) {
    let list = list_id(d, path);
    let at = |index: usize| RangeAnchor {
        seq: list,
        pos: relative_position(d, &encode_path(path), index, Side::Left)
            .expect("a live sequence yields a position"),
    };
    let (start, end) = (at(0), at(1));
    let mut id = None;
    d.transact(|tx| id = Some(tx.ranged().create_map(start, end)));
    let id = id.expect("a create emits a range id");
    d.transact(|tx| {
        tx.ranged()
            .payload_map(id)
            .expect("the composite payload is a map")
            .set(b"k", Scalar::Bytes(secret.to_vec()))
    });
    let payload = match d.ranged_element(id).expect("the mark is live").payload {
        crdtsync_core::RangedPayload::Composite { id, .. } => id,
        _ => panic!("expected a composite payload"),
    };
    (id, payload)
}

/// A mark spanning `[0, 1)` of the List at `path`, returning its RangedElement id.
fn mark(d: &mut Document, path: &[&[u8]]) -> ElementId {
    let list = list_id(d, path);
    let at = |index: usize, side: Side| RangeAnchor {
        seq: list,
        pos: relative_position(d, &encode_path(path), index, side)
            .expect("a live sequence yields a position"),
    };
    let (start, end) = (at(0, Side::Left), at(1, Side::Left));
    let mut id = None;
    d.transact(|tx| id = Some(tx.ranged().create(start, end, Scalar::Bool(true))));
    id.expect("a create emits a range id")
}

/// Grant read on `path`, which makes `path` a *governing* path of the document — the
/// shape a reader can be denied at, whether or not anything lives there.
fn govern(d: &mut Document, path: &[&[u8]]) {
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(9)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            encode_path(path),
            cid(1),
        );
    });
}

/// Grant read on the element `target`, returning the tuple's id.
fn govern_element(d: &mut Document, target: ElementId) -> ElementId {
    let mut id = None;
    d.transact(|tx| {
        id = Some(tx.acl().grant_element(
            AclSubject::Actor(cid(9)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            target,
            cid(1),
        ));
    });
    id.expect("a grant emits a tuple id")
}

/// The ids of a document's live RangedElements, sorted.
fn marks(d: &Document) -> Vec<ElementId> {
    let mut v: Vec<ElementId> = d.ranged_elements().into_iter().map(|r| r.id).collect();
    v.sort_by_key(|id| id.as_bytes());
    v
}

/// A document with `/a` and `/b` sequences, `/a` a governing path, a mark anchored in
/// `/a`, and `/a` then deleted so the mark's anchor resolves to nothing.
fn orphaned_mark() -> (Document, ElementId) {
    let mut d = doc();
    seq(&mut d, &[b"a"]);
    seq(&mut d, &[b"b"]);
    govern(&mut d, &[b"a"]);
    let id = mark(&mut d, &[b"a"]);
    delete(&mut d, &encode_path(&[b"a"]));
    (d, id)
}

#[test]
fn an_orphaned_mark_is_withheld_from_a_reader_a_governing_deny_carves() {
    // The reader reads the root and /b but is denied at /a — a governing path, so it is
    // denied somewhere and reads no document whole. The mark's payload was authored in
    // /a, and its anchor no longer resolves to say so, which is exactly why the root
    // verdict must not stand in for the region's.
    let (mut carved, _) = orphaned_mark();
    carved.project_read_paths(reads_top(true, &[b"b"]), None);
    assert!(
        marks(&carved).is_empty(),
        "a reader carved out at a governing path is not served an orphaned mark",
    );
}

#[test]
fn an_orphaned_mark_is_dropped_whatever_the_read_predicate_admits() {
    // The drop is unconditional, because the transform is: a reader entitled to the whole
    // document is served by the caller declining to project at all, the same rule
    // `project_zones` has always stated for a whole-zone subscriber. So an identity
    // predicate is not an identity projection here, and must not be read as the
    // whole-document case — that one is pinned on the server, where the decline is.
    for (label, reads) in [
        (
            "an identity predicate",
            &(|_: &[Vec<u8>]| true) as &dyn Fn(&[Vec<u8>]) -> bool,
        ),
        ("a reader denied at root", &|path: &[Vec<u8>]| {
            !path.is_empty()
        }),
    ] {
        let (mut d, _) = orphaned_mark();
        d.project_read_paths(reads, None);
        assert!(
            marks(&d).is_empty(),
            "{label}: the projection drops the orphaned mark",
        );
    }
}

#[test]
fn an_unresolvable_element_tuple_is_withheld_from_a_reader_a_governing_deny_carves() {
    // The element-scoped tuple beside the mark takes the same rule: once its target
    // leaves the tree it names no path, and keeping it reveals its subject and effect.
    let build = || {
        let mut d = doc();
        seq(&mut d, &[b"a"]);
        let list = list_id(&d, &[b"a"]);
        govern(&mut d, &[b"a"]);
        let tuple = govern_element(&mut d, list);
        delete(&mut d, &encode_path(&[b"a"]));
        (d, tuple)
    };

    let (mut carved, tuple) = build();
    carved.project_read_paths(reads_top(true, &[b"b"]), None);
    assert!(
        carved.acl_tuple(tuple).is_none(),
        "a reader carved out at a governing path is not served an unresolvable-element tuple",
    );

    // Unconditional, for the same reason the mark's drop is.
    let (mut identity, tuple) = build();
    identity.project_read_paths(|_| true, None);
    assert!(
        identity.acl_tuple(tuple).is_none(),
        "an identity predicate is a narrowing projection too, so the tuple still goes",
    );
}

/// A zoned document whose `/notes` (zone `zb`) subtree held the annotated sequence, with
/// `/notes` then deleted so the sequence is retained but unreachable.
fn orphaned_mark_in_zb() -> (Document, ElementId) {
    let mut d = doc();
    d.set_schema(zoned());
    seq(&mut d, &[b"board", b"seq"]);
    seq(&mut d, &[b"notes", b"seq"]);
    let id = mark(&mut d, &[b"notes", b"seq"]);
    delete(&mut d, &encode_path(&[b"notes"]));
    (d, id)
}

fn zone_id(name: &[u8]) -> u32 {
    zone::zone_id_of(&zoned(), &[name.to_vec()]).expect("the zone resolves")
}

#[test]
fn an_orphaned_mark_is_withheld_from_a_zone_scoped_subscriber() {
    // Every zone-scoped subscriber holds the root partition, so "this names no zone"
    // reached all of them — including the za-only subscriber here, which was served a
    // mark authored wholly inside zb.
    let (mut scoped, _) = orphaned_mark_in_zb();
    scoped.project_zones(&zoned(), &HashSet::from([zone_id(b"board")]), None);
    assert!(
        marks(&scoped).is_empty(),
        "a za-only subscriber is not served a mark whose anchor left the tree",
    );
}

#[test]
fn an_orphaned_mark_is_dropped_whatever_zones_are_authorized() {
    // Unconditional, as on the read seam: a subscriber holding every declared zone is
    // served by the caller declining to project, which `project_zones`' own contract has
    // always said. The op stream does *not* attribute such a mark — a Ranged op is emitted
    // at the document root and so rides the root partition every zone subscriber holds
    // (C74) — which makes this seam the stricter of the two rather than the looser.
    let (mut whole, _) = orphaned_mark_in_zb();
    whole.project_zones(
        &zoned(),
        &HashSet::from([zone_id(b"board"), zone_id(b"notes")]),
        None,
    );
    assert!(
        marks(&whole).is_empty(),
        "the zone projection drops the orphaned mark whatever set it narrows to",
    );
}

#[test]
fn an_unresolvable_element_tuple_is_withheld_from_a_zone_scoped_subscriber() {
    let build = || {
        let mut d = doc();
        d.set_schema(zoned());
        seq(&mut d, &[b"notes", b"seq"]);
        let list = list_id(&d, &[b"notes", b"seq"]);
        let tuple = govern_element(&mut d, list);
        delete(&mut d, &encode_path(&[b"notes"]));
        (d, tuple)
    };

    let (mut scoped, tuple) = build();
    scoped.project_zones(&zoned(), &HashSet::from([zone_id(b"board")]), None);
    assert!(
        scoped.acl_tuple(tuple).is_none(),
        "a za-only subscriber is not served a tuple scoped to an element that left the tree",
    );

    let (mut whole, tuple) = build();
    whole.project_zones(
        &zoned(),
        &HashSet::from([zone_id(b"board"), zone_id(b"notes")]),
        None,
    );
    assert!(
        whole.acl_tuple(tuple).is_none(),
        "the drop is unconditional, as the orphaned mark's is",
    );
}

#[test]
fn a_container_the_walk_does_not_reach_is_still_served() {
    // The boundary this unit stops at. A displaced or deleted container stays in the
    // registries by design — that retention is displace-then-recreate identity — and both
    // projections still re-encode it whatever partition or path it fell in. Narrowing
    // that is C67's ruling, because dropping it loses content a subscriber entitled to
    // the partition gets back when the slot is re-won. Pinned so the boundary moves
    // deliberately rather than by drift.
    let build = || {
        let mut d = doc();
        d.set_schema(zoned());
        seq(&mut d, &[b"board", b"seq"]);
        seq(&mut d, &[b"notes", b"seq"]);
        let inner = list_id(&d, &[b"notes", b"seq"]);
        delete(&mut d, &encode_path(&[b"notes"]));
        (d, inner)
    };
    let names = |state: &[u8], id: ElementId| state.windows(16).any(|w| w == id.as_bytes());
    // A control the byte scan cannot pass by accident: the same document without the
    // `/notes/seq` list ever existing must not name the id the real one is asserted to.
    let mut control = doc();
    control.set_schema(zoned());
    seq(&mut control, &[b"board", b"seq"]);
    let (_, absent) = build();
    assert!(
        !names(&control.encode_state(), absent),
        "the byte scan does not report an id the document never held",
    );

    let (mut scoped, inner) = build();
    scoped.project_zones(&zoned(), &HashSet::from([zone_id(b"board")]), None);
    assert!(
        names(&scoped.encode_state(), inner),
        "the retained container survives the zone projection (C67, not this unit)",
    );

    let (mut carved, inner) = build();
    carved.project_read_paths(reads_top(true, &[b"board"]), None);
    assert!(
        names(&carved.encode_state(), inner),
        "the retained container survives the read projection (C67, not this unit)",
    );
}

#[test]
fn a_dropped_marks_composite_payload_goes_with_it() {
    // A mark's payload can be a container, registered under the mark's own id and linked
    // to it rather than held in any map slot — so the root walk never reaches it, it has
    // no path and falls in no zone, and no verdict either projection computes would ever
    // cut it. Dropping the `ranged` entry alone would withhold the mark's name and anchor
    // while re-encoding the container holding its content, which is the content the drop
    // exists to withhold. Pinned on the byte stream, since the container is unreachable
    // by every read the projected document offers.
    const SECRET: &[u8] = b"payload-content-that-must-not-ride";
    let carries = |state: &[u8]| state.windows(SECRET.len()).any(|w| w == SECRET);

    // Read seam: the mark's region is /a, which the reader may not read.
    let mut d = doc();
    seq(&mut d, &[b"a"]);
    seq(&mut d, &[b"b"]);
    let (_, payload) = composite_mark(&mut d, &[b"a"], SECRET);
    assert!(carries(&d.encode_state()), "the fixture really holds it");
    let mut cut = Document::decode_state(&d.encode_state()).expect("round-trip");
    cut.project_read_paths(reads_top(true, &[b"b"]), None);
    let state = cut.encode_state();
    assert!(marks(&cut).is_empty(), "the mark itself is dropped");
    assert!(
        !carries(&state),
        "the payload container does not ride the projection the mark was cut from",
    );
    assert!(
        d.encode_state()
            .windows(16)
            .any(|w| w == payload.as_bytes()),
        "control: the unprojected state does name the payload id",
    );
    assert!(
        !state.windows(16).any(|w| w == payload.as_bytes()),
        "nor does its id",
    );

    // The same for an orphaned anchor, and for the zone seam.
    let mut orphaned = Document::decode_state(&d.encode_state()).expect("round-trip");
    delete(&mut orphaned, &encode_path(&[b"a"]));
    assert!(
        carries(&orphaned.encode_state()),
        "orphaning the anchor does not remove the payload on its own",
    );
    let mut cut = Document::decode_state(&orphaned.encode_state()).expect("round-trip");
    cut.project_read_paths(reads_top(true, &[b"b"]), None);
    assert!(
        !carries(&cut.encode_state()),
        "an orphaned mark's payload goes with it too",
    );

    let mut z = doc();
    z.set_schema(zoned());
    seq(&mut z, &[b"board", b"seq"]);
    seq(&mut z, &[b"notes", b"seq"]);
    composite_mark(&mut z, &[b"notes", b"seq"], SECRET);
    assert!(
        carries(&z.encode_state()),
        "the zone fixture really holds it"
    );
    let mut scoped = Document::decode_state(&z.encode_state()).expect("round-trip");
    scoped.project_zones(&zoned(), &HashSet::from([zone_id(b"board")]), None);
    assert!(
        !carries(&scoped.encode_state()),
        "a za-only subscriber is served no part of a zb mark, payload included",
    );
}

#[test]
fn a_mark_under_a_denied_ancestor_goes_even_where_a_deeper_grant_reopens_its_own_path() {
    // The anchor takes the prefix rule the containers take, not its own path's verdict. A
    // sequence under an unreadable ancestor is purged even where a more specific grant
    // re-opens its own path — so reading that path alone would keep a mark whose sequence
    // the same projection just cut, leaving the mark's payload with an anchor pointing at
    // nothing the reader holds.
    let mut d = doc();
    seq(&mut d, &[b"outer", b"inner"]);
    let id = mark(&mut d, &[b"outer", b"inner"]);
    let inner = list_id(&d, &[b"outer", b"inner"]);

    // Denies /outer, re-grants /outer/inner — the shape the container rule already cuts.
    let reads = |path: &[Vec<u8>]| match path {
        [] => true,
        [a] => a.as_slice() != b"outer",
        _ => true,
    };
    let mut cut = Document::decode_state(&d.encode_state()).expect("round-trip");
    cut.project_read_paths(reads, None);
    let state = cut.encode_state();
    assert!(
        marks(&cut).is_empty(),
        "the mark goes with the sequence its denied ancestor cut",
    );
    assert!(
        !state
            .windows(16)
            .any(|w| w == inner.as_bytes() || w == id.as_bytes()),
        "and neither id rides the re-encode",
    );
}

#[test]
fn a_nested_container_under_a_dropped_marks_payload_goes_too() {
    // A composite payload is an ordinary container, so an op stream nests containers and
    // counters inside it — each registered under a derived id the walk reaches no more than
    // it reaches the payload. Purging the payload alone would leave the nested content in
    // the registries, which `encode_state` emits whole.
    const SECRET: &[u8] = b"nested-content-that-must-not-ride";
    let mut d = doc();
    seq(&mut d, &[b"a"]);
    seq(&mut d, &[b"b"]);
    let (rid, _) = composite_mark(&mut d, &[b"a"], b"outer-value");
    d.transact(|tx| {
        let mut ranged = tx.ranged();
        let mut payload = ranged.payload_map(rid).expect("a map payload");
        payload
            .map(b"inner")
            .set(b"k", Scalar::Bytes(SECRET.to_vec()));
        payload.inc(b"votes", 7);
    });
    let carries = |state: &[u8]| state.windows(SECRET.len()).any(|w| w == SECRET);
    assert!(carries(&d.encode_state()), "the fixture really holds it");

    // The counter beneath the payload has a registry entry of its own and no parent
    // edge, so it is reached by the live-handle half of the walk alone.
    let votes = match d.ranged_element(rid).expect("the mark is live").payload {
        crdtsync_core::RangedPayload::Composite { id, .. } => {
            ElementId::derive(id, b"votes", crdtsync_core::ElementKind::Counter)
        }
        _ => panic!("expected a composite payload"),
    };
    assert!(
        d.encode_state().windows(16).any(|w| w == votes.as_bytes()),
        "the fixture really registers the counter",
    );

    let mut cut = Document::decode_state(&d.encode_state()).expect("round-trip");
    cut.project_read_paths(reads_top(true, &[b"b"]), None);
    let state = cut.encode_state();
    assert!(
        !carries(&state),
        "the nested container under the dropped mark's payload does not ride",
    );
    assert!(
        !state.windows(16).any(|w| w == votes.as_bytes()),
        "nor does the counter beneath it",
    );

    // A counter whose own slot was deleted keeps its registry entry with no live slot and
    // no parent edge — reached only by sweeping the holding map's tombstoned slot keys.
    let mut gone = Document::decode_state(&d.encode_state()).expect("round-trip");
    gone.transact(|tx| {
        let mut ranged = tx.ranged();
        ranged
            .payload_map(rid)
            .expect("a map payload")
            .delete(b"votes");
    });
    let names_votes = |st: &[u8]| st.windows(16).any(|w| w == votes.as_bytes());
    assert!(
        names_votes(&gone.encode_state()),
        "deleting the counter's slot retains its registry entry",
    );
    let mut cut = Document::decode_state(&gone.encode_state()).expect("round-trip");
    cut.project_read_paths(reads_top(true, &[b"b"]), None);
    assert!(
        !names_votes(&cut.encode_state()),
        "and it is still purged with the mark it sat under",
    );

    // A nested container whose own slot was then deleted is retained in the registries
    // and reached through the `parents` relation rather than the live handles.
    let mut displaced = Document::decode_state(&d.encode_state()).expect("round-trip");
    displaced.transact(|tx| {
        let mut ranged = tx.ranged();
        ranged
            .payload_map(rid)
            .expect("a map payload")
            .delete(b"inner");
    });
    assert!(
        carries(&displaced.encode_state()),
        "deleting the slot retains the nested container",
    );
    let mut cut = Document::decode_state(&displaced.encode_state()).expect("round-trip");
    cut.project_read_paths(reads_top(true, &[b"b"]), None);
    assert!(
        !carries(&cut.encode_state()),
        "and it is still purged with the mark",
    );

    let mut z = doc();
    z.set_schema(zoned());
    seq(&mut z, &[b"board", b"seq"]);
    seq(&mut z, &[b"notes", b"seq"]);
    let (zid, _) = composite_mark(&mut z, &[b"notes", b"seq"], b"outer-value");
    z.transact(|tx| {
        let mut ranged = tx.ranged();
        let mut payload = ranged.payload_map(zid).expect("a map payload");
        payload
            .map(b"inner")
            .set(b"k", Scalar::Bytes(SECRET.to_vec()));
    });
    assert!(
        carries(&z.encode_state()),
        "the zone fixture really holds it"
    );
    let mut scoped = Document::decode_state(&z.encode_state()).expect("round-trip");
    scoped.project_zones(&zoned(), &HashSet::from([zone_id(b"board")]), None);
    assert!(
        !carries(&scoped.encode_state()),
        "nor for a subscriber scoped away from the mark's partition",
    );
}
