//! The partition an op belongs to is the region it **governs**, not the position it
//! is emitted at (C74).
//!
//! An annotation and an ACL tuple are doc-level state: both are emitted at the
//! document root, so the op's target names no region and no partition can be read off
//! it. What each one carries is content of the region it names — a mark's name, its
//! payload and its anchor's element id; a grant's subject, effect and scope — so the
//! partition it belongs to is that region's, and a subscriber the region's zone does
//! not admit receives none of it.
//!
//! So a `RangedElement` op belongs to the partition of the sequences its endpoints
//! anchor (require-agreement — endpoints in two zones are a `CrossZoneAnchor`
//! violation the read repairs away), an op editing a mark's composite payload rides
//! the mark, and an ACL op belongs to the partition its scope resolves into. Those
//! are the same regions the snapshot projection keeps such state by, so the op seam
//! and the state seam withhold from the same subscribers — and the per-zone clocks
//! stay consistent with the projections, since an op stamped in a zone advances that
//! zone's clock and a projection that drops the zone drops its clock with it.
//!
//! A governing region that resolves to no path names no partition. The root is the
//! only partition an envelope can express, so such an op keeps it while the snapshot
//! projection drops the state form (C52) — pinned below as the residual it is (C82).

use std::collections::HashSet;

use crdtsync_core::acl::{AclEffect, AclGrant, AclScope, AclSubject, Capability};
use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::list::Side;
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::{path, zone, Document, Op, Scalar, Schema};

mod common;
use common::cid;

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) plus an unzoned
/// root-partition slot (`/loose`), each holding a text sequence a mark can span.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map", "children": { "seq": "Body" } },
        "Body": { "kind": "text" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

fn zoned() -> Schema {
    Schema::parse(ZONED).expect("zoned schema parses")
}

/// The compact id of the zone rooted at `/key`.
fn zone_of(key: &[u8]) -> u32 {
    zone::zone_id_of(&zoned(), &[key.to_vec()]).expect("the zone resolves")
}

fn p(segs: &[&[u8]]) -> Vec<u8> {
    path::encode_path(segs)
}

/// A replica bound to the zoned schema, with a text sequence seeded under each of
/// the three sections — so a mark or a grant has a live region to name in the root
/// partition and in both zones.
fn seeded(client: u8) -> Document {
    let mut d = Document::new(cid(client));
    d.set_schema(zoned());
    for sect in [&b"board"[..], b"notes", b"loose"] {
        path::text_insert(&mut d, &p(&[sect, b"seq"]), 0, "hello world");
    }
    d
}

/// Author a `bold` mark over `[0, 5)` of the sequence under `sect`, returning the
/// ops and the mark handle.
fn mark_in(d: &mut Document, sect: &[u8]) -> (Vec<Op>, Vec<u8>) {
    let (ops, id) = path::mark(
        d,
        &p(&[sect, b"seq"]),
        0,
        Side::Right,
        5,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    (ops, id.expect("the mark is authored over a live sequence"))
}

/// The single zone every op in `ops` carries, or the assertion fails — a batch is
/// only meaningful here when it is wholly in one partition.
fn one_zone(ops: &[Op]) -> Option<u32> {
    assert!(!ops.is_empty(), "the batch emitted nothing");
    let zone = ops[0].zone;
    assert!(
        ops.iter().all(|o| o.zone == zone),
        "the batch straddles partitions: {:?}",
        ops.iter().map(|o| o.zone).collect::<Vec<_>>()
    );
    zone
}

fn grant_path(d: &mut Document, path: Vec<u8>) -> (Vec<Op>, ElementId) {
    let mut id = None;
    let ops = d.transact(|tx| {
        id = Some(tx.acl().grant(
            AclSubject::Actor(cid(9)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            path,
            cid(1),
        ));
    });
    (ops, id.expect("the grant returns its handle"))
}

fn grant_element(d: &mut Document, element: ElementId) -> (Vec<Op>, ElementId) {
    let mut id = None;
    let ops = d.transact(|tx| {
        id = Some(tx.acl().grant_element(
            AclSubject::Actor(cid(9)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            element,
            cid(1),
        ));
    });
    (ops, id.expect("the grant returns its handle"))
}

fn text_id(d: &Document, sect: &[u8]) -> ElementId {
    let map = ElementId::derive(d.root_id(), sect, ElementKind::Map);
    ElementId::derive(map, b"seq", ElementKind::Text)
}

fn map_id(d: &Document, sect: &[u8]) -> ElementId {
    ElementId::derive(d.root_id(), sect, ElementKind::Map)
}

// --- an annotation rides its anchors' partition ---

#[test]
fn a_mark_over_a_zoned_sequence_is_stamped_in_that_zone() {
    let mut d = seeded(1);
    let zb = zone_of(b"notes");
    let before_root = d.zone_clock(None);
    let before_zb = d.zone_clock(Some(zb));

    let (ops, _) = mark_in(&mut d, b"notes");
    assert_eq!(
        one_zone(&ops),
        Some(zb),
        "the mark belongs to the partition its anchors fall in, not the root it is \
         emitted at"
    );
    // The stamp comes off that zone's clock, so the partition the op is withheld
    // from is the partition whose clock moved.
    assert!(d.zone_clock(Some(zb)) > before_zb, "zb's clock advanced");
    assert_eq!(
        d.zone_clock(None),
        before_root,
        "the root partition is untouched"
    );
}

#[test]
fn a_marks_payload_change_and_delete_ride_the_marks_zone() {
    let mut d = seeded(1);
    let zb = zone_of(b"notes");
    let (_, handle) = mark_in(&mut d, b"notes");

    let set = path::mark_set_value(&mut d, &handle, Scalar::Int(3));
    assert_eq!(one_zone(&set), Some(zb), "a payload change rides the mark");
    let del = path::mark_delete(&mut d, &handle);
    assert_eq!(one_zone(&del), Some(zb), "a delete rides the mark");
}

#[test]
fn an_op_editing_a_marks_composite_payload_rides_the_mark() {
    // A composite payload hangs off the range rather than a map slot, so the tree
    // walk gives it no path of its own and its edits would fall to the root — while
    // the projection cuts the payload with the mark it belongs to.
    let mut d = seeded(1);
    let zb = zone_of(b"notes");
    let seq = text_id(&d, b"notes");
    let anchor = |pos| RangeAnchor { seq, pos };

    let mut rid = None;
    let create = d.transact(|tx| {
        let mut rc = tx.ranged();
        rid = Some(rc.create_map(
            anchor(RelativePosition::Start),
            anchor(RelativePosition::End),
        ));
    });
    assert_eq!(one_zone(&create), Some(zb));
    let rid = rid.expect("the range is created");

    let edit = d.transact(|tx| {
        let mut rc = tx.ranged();
        let mut m = rc.payload_map(rid).expect("live map payload");
        m.set(b"author", Scalar::Int(7));
        m.map(b"nested").set(b"deep", Scalar::Int(8));
    });
    assert_eq!(
        one_zone(&edit),
        Some(zb),
        "the payload container and everything registered beneath it ride the mark"
    );
}

#[test]
fn a_mark_over_an_unzoned_sequence_stays_in_the_root_partition() {
    let mut d = seeded(1);
    let (ops, _) = mark_in(&mut d, b"loose");
    assert_eq!(
        one_zone(&ops),
        None,
        "an unzoned region is the root partition, which every subscriber holds"
    );
}

// --- an ACL tuple rides its scope's partition ---

#[test]
fn a_path_scoped_grant_naming_a_zoned_path_is_stamped_in_that_zone() {
    // The sharpest form: the scope is perfectly resolvable and names the zone root
    // outright, so nothing about it is a pathless-scope corner.
    let mut d = seeded(1);
    let zb = zone_of(b"notes");
    let before_root = d.zone_clock(None);

    let (ops, _) = grant_path(&mut d, p(&[b"notes"]));
    assert_eq!(
        one_zone(&ops),
        Some(zb),
        "a grant rides its scope's partition"
    );
    let (deep, _) = grant_path(&mut d, p(&[b"notes", b"seq"]));
    assert_eq!(
        one_zone(&deep),
        Some(zb),
        "and so does one scoped inside it"
    );
    assert_eq!(
        d.zone_clock(None),
        before_root,
        "the root partition is untouched"
    );
}

#[test]
fn an_element_scoped_grant_rides_the_elements_partition() {
    let mut d = seeded(1);
    let zb = zone_of(b"notes");
    let notes = map_id(&d, b"notes");
    let (ops, tuple) = grant_element(&mut d, notes);
    assert_eq!(one_zone(&ops), Some(zb));

    let revoke = d.transact(|tx| tx.acl().revoke(tuple));
    assert_eq!(
        one_zone(&revoke),
        Some(zb),
        "a revoke rides the partition its tuple's scope resolves into, so a scope that \
         has not moved reaches exactly the subscribers the grant did"
    );
}

#[test]
fn a_grant_scoped_outside_every_zone_stays_in_the_root_partition() {
    let mut d = seeded(1);
    let (ops, _) = grant_path(&mut d, p(&[b"loose"]));
    assert_eq!(
        one_zone(&ops),
        None,
        "an unzoned scope is the root partition"
    );
    let (root, _) = grant_path(&mut d, p(&[]));
    assert_eq!(
        one_zone(&root),
        None,
        "a document-wide grant governs every partition, so it rides the root"
    );
}

// --- the op seam and the snapshot projection withhold the same state ---

/// Whether `d` holds a mark named `bold` anchored in the sequence under `sect`.
fn holds_mark(d: &Document, sect: &[u8]) -> bool {
    let seq = text_id(d, sect);
    d.ranged_elements()
        .iter()
        .any(|r| r.name.as_deref() == Some(&b"bold"[..]) && r.start.seq == seq)
}

fn holds_scope(d: &Document, scope: &AclScope) -> bool {
    d.acl_tuples().iter().any(|t| &t.scope == scope)
}

#[test]
fn a_zone_projection_and_the_op_filter_withhold_the_same_mark_and_grant() {
    let za = zone_of(b"board");
    let zb = zone_of(b"notes");
    let mut author = seeded(1);
    let (mark_ops, _) = mark_in(&mut author, b"notes");
    let scope = AclScope::Path(p(&[b"notes"]));
    let (grant_ops, _) = grant_path(&mut author, p(&[b"notes"]));

    // Op seam: a subscriber scoped to za takes only the root partition and za, so
    // neither op reaches it.
    let admits = |ops: &[Op], zones: &HashSet<u32>| {
        ops.iter()
            .all(|o| o.zone.is_none_or(|z| zones.contains(&z)))
    };
    let za_only = HashSet::from([za]);
    let zb_only = HashSet::from([zb]);
    assert!(!admits(&mark_ops, &za_only), "the mark is withheld from za");
    assert!(
        !admits(&grant_ops, &za_only),
        "the grant is withheld from za"
    );
    assert!(admits(&mark_ops, &zb_only), "and served to zb");
    assert!(admits(&grant_ops, &zb_only), "and served to zb");

    // Snapshot seam: the same subscriber's cold-start projection drops the same two.
    let mut projected = seeded(2);
    for op in mark_ops.iter().chain(&grant_ops) {
        projected.apply(op);
    }
    assert!(holds_mark(&projected, b"notes"));
    assert!(holds_scope(&projected, &scope));
    projected.project_zones(&zoned(), &za_only, None);
    assert!(
        !holds_mark(&projected, b"notes"),
        "the projection drops the mark za may not read"
    );
    assert!(
        !holds_scope(&projected, &scope),
        "and the grant naming the zone"
    );
}

#[test]
fn a_zone_projection_drops_the_clock_of_the_partition_it_drops_the_mark_and_grant_from() {
    // The invariant the two seams share: an op stamped in `zb` advances `zb`'s clock,
    // and a projection that withholds `zb` drops that clock — so a recipient never
    // observes the partition it may not read, neither as state nor as a clock jump.
    // The root clock is the one every projection keeps whole, which is why an
    // annotation or an ACL op must not move it on a zoned region's behalf.
    let za = zone_of(b"board");
    let zb = zone_of(b"notes");
    let mut author = seeded(1);
    let root_before = author.zone_clock(None);
    let (mark_ops, handle) = mark_in(&mut author, b"notes");
    let (grant_ops, _) = grant_path(&mut author, p(&[b"notes"]));
    let payload_ops = path::mark_set_value(&mut author, &handle, Scalar::Int(1));

    assert_eq!(
        author.zone_clock(None),
        root_before,
        "no annotation or ACL op touched the root clock"
    );
    let zb_clock = author.zone_clock(Some(zb));
    let highest = mark_ops
        .iter()
        .chain(&grant_ops)
        .chain(&payload_ops)
        .map(|o| o.stamp.lamport)
        .max()
        .expect("ops were emitted");
    assert!(
        zb_clock >= highest,
        "zb's clock covers every op stamped in it: {zb_clock} < {highest}"
    );

    let mut projected = author;
    projected.project_zones(&zoned(), &HashSet::from([za]), None);
    assert_eq!(
        projected.zone_clock(Some(zb)),
        0,
        "the withheld partition's clock goes with its state"
    );
    assert_eq!(
        projected.zone_clock(None),
        root_before,
        "and the root clock still reads what the root partition alone put there"
    );
}

#[test]
fn an_op_served_replica_converges_with_a_snapshot_served_one_over_a_mark_and_a_grant() {
    // The two seams are only equal if they agree on the *whole* state: the annotations
    // and the ACL tuples a za-scoped joiner materializes from the op stream are exactly
    // the ones a projected snapshot serves it.
    let za = zone_of(b"board");
    let za_only = HashSet::from([za]);
    let mut author = seeded(1);
    let mut pool: Vec<Op> = Vec::new();
    let (ops, handle) = mark_in(&mut author, b"board");
    pool.extend(ops);
    let (ops, _) = grant_path(&mut author, p(&[b"board"]));
    pool.extend(ops);
    let (ops, _) = mark_in(&mut author, b"notes");
    pool.extend(ops);
    let (ops, _) = grant_path(&mut author, p(&[b"notes"]));
    pool.extend(ops);
    pool.extend(path::mark_set_value(&mut author, &handle, Scalar::Int(4)));

    // The op-served joiner: seeded the same way, then fed the ops its zone filter
    // admits.
    let mut op_served = seeded(2);
    for op in &pool {
        if op.zone.is_none_or(|z| za_only.contains(&z)) {
            op_served.apply(op);
        }
    }

    // The snapshot-served joiner: the author's replica projected to the same zones.
    let mut snapshot_served = author;
    snapshot_served.project_zones(&zoned(), &za_only, None);

    assert!(holds_mark(&op_served, b"board"), "za's own mark survives");
    assert!(!holds_mark(&op_served, b"notes"), "zb's does not");
    assert!(
        holds_scope(&op_served, &AclScope::Path(p(&[b"board"]))),
        "za's own grant survives"
    );
    assert!(
        !holds_scope(&op_served, &AclScope::Path(p(&[b"notes"]))),
        "zb's does not"
    );
    // The projected snapshot keeps exactly what the op filter served, named rather
    // than counted — equal counts alone would pass on two disjoint sets.
    assert!(holds_mark(&snapshot_served, b"board"));
    assert!(!holds_mark(&snapshot_served, b"notes"));
    assert!(holds_scope(
        &snapshot_served,
        &AclScope::Path(p(&[b"board"]))
    ));
    assert!(!holds_scope(
        &snapshot_served,
        &AclScope::Path(p(&[b"notes"]))
    ));
    let ids = |d: &Document| {
        let mut ids: Vec<_> = d
            .ranged_elements()
            .iter()
            .map(|r| r.id.as_bytes())
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(
        ids(&op_served),
        ids(&snapshot_served),
        "the two seams keep the same annotations"
    );
    let scopes = |d: &Document| {
        let mut s: Vec<_> = d.acl_tuples().into_iter().map(|t| t.scope).collect();
        s.sort_by_key(|sc| format!("{sc:?}"));
        s
    };
    assert_eq!(
        scopes(&op_served),
        scopes(&snapshot_served),
        "and the same ACL tuples"
    );
}

// --- the residual: a governing region that resolves to no path (C82) ---

#[test]
fn a_scope_that_resolves_to_no_path_keeps_the_root_partition() {
    // `Path` scope bytes are raw and nothing validates them on the way in, so a scope
    // that is not a `core::path` names a position no partition can be resolved at.
    // The root is the only partition an envelope expresses, so the op keeps it —
    // while the projection drops the tuple, which is the one place the two seams
    // still part company (C82).
    let mut d = seeded(1);
    let scope = AclScope::Path(vec![0xff, 0xff, 0xff]);
    let (ops, _) = grant_path(&mut d, vec![0xff, 0xff, 0xff]);
    assert_eq!(one_zone(&ops), None);
    assert!(holds_scope(&d, &scope));

    d.project_zones(&zoned(), &HashSet::from([zone_of(b"board")]), None);
    assert!(
        !holds_scope(&d, &scope),
        "the projection drops a tuple that names no partition"
    );
}

#[test]
fn a_mark_whose_endpoints_straddle_two_zones_keeps_the_root_partition() {
    // Endpoints in two zones are a `CrossZoneAnchor` violation the read repairs away
    // by dropping the range, so the two anchors must *agree* for the mark to name a
    // partition. Disagreement resolves to no partition, and the op keeps the root
    // (C82) rather than picking one endpoint's zone over the other's.
    let mut d = seeded(1);
    let board = text_id(&d, b"board");
    let notes = text_id(&d, b"notes");
    let ops = d.transact(|tx| {
        tx.ranged().mark(
            b"bold",
            RangeAnchor {
                seq: board,
                pos: RelativePosition::Start,
            },
            RangeAnchor {
                seq: notes,
                pos: RelativePosition::End,
            },
            Scalar::Bool(true),
        );
    });
    assert_eq!(one_zone(&ops), None);

    // The positive control: two endpoints in one zone do resolve, so what the case
    // above pins is the disagreement and not a resolution that never happens.
    let agreeing = d.transact(|tx| {
        tx.ranged().mark(
            b"bold",
            RangeAnchor {
                seq: board,
                pos: RelativePosition::Start,
            },
            RangeAnchor {
                seq: board,
                pos: RelativePosition::End,
            },
            Scalar::Bool(true),
        );
    });
    assert_eq!(one_zone(&agreeing), Some(zone_of(b"board")));
}
