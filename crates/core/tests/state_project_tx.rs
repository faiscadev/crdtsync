//! What a zone projection does to an atomic group its buffer straddles.
//!
//! `project_zones` filters the buffer to the authorized partitions, which is a per-op
//! filter like any other: a group with members on both sides of the cut loses some,
//! and the survivors carry a `count` the recipient's bucket can never reach. Left
//! tagged they would be buffered forever in the decoded replica — invisible, and
//! counted among the ids it holds. So the survivors of a straddling group ride
//! untagged, the same rule the delivery seams apply; a group wholly inside the
//! authorized partitions keeps its tag and its all-or-nothing commit.
//!
//! (`project_read_paths`, the doc-ACL analogue, clears the buffer whole as soon as it
//! drops anything, so nothing survives there to strand.)

use std::collections::HashSet;

use crdtsync_core::{zone, ClientId, Document, Element, Op, Scalar, Schema};

mod common;
use common::cid;

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) plus an unzoned root-
/// partition slot (`/loose`).
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
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

/// The zone set a za-only subscriber is served.
fn za_only() -> HashSet<u32> {
    HashSet::from([zone_of(b"board")])
}

/// A replica of `client` bound to the zoned schema, so its ops carry their zone.
fn replica(client: ClientId) -> Document {
    let mut d = Document::new(client);
    d.set_schema(zoned());
    d
}

/// The author, and the create batch seeding the three zone containers.
fn seeded() -> (Document, Vec<Op>) {
    let mut author = replica(cid(1));
    let setup = author.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(0));
        tx.map(b"notes").register(b"nseed", Scalar::Int(0));
        tx.map(b"loose").register(b"lseed", Scalar::Int(0));
    });
    (author, setup)
}

/// A room replica — a server's, which merges under its own identity and never mints
/// — holding `setup` then `ops`.
fn room(setup: &[Op], ops: &[Op]) -> Document {
    let mut d = replica(cid(9));
    for op in setup.iter().chain(ops) {
        d.apply(op);
    }
    d
}

/// The `Int` at `/<container>/<key>`.
fn zoned_int(d: &Document, container: &[u8], key: &[u8]) -> Option<i64> {
    let map = match d.get(container)? {
        Element::Map(m) => m,
        _ => return None,
    };
    let inner = map.borrow().get(key)?;
    match inner {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(i) => Some(*i),
            _ => None,
        },
        _ => None,
    }
}

/// A projected snapshot's round trip — what the recipient actually reconstructs.
fn round_trip(d: &Document) -> Document {
    Document::decode_state(&d.encode_state()).expect("the projected snapshot decodes")
}

/// A three-member group straddling za and zb, of which the room has folded the first
/// two — so both sit in its buffer, waiting on the third, and the projection is what
/// cuts the group rather than the room's own arrival order.
fn straddling(author: &mut Document) -> Vec<Op> {
    author.atomic_transact(|tx| {
        tx.map(b"board").register(b"bk", Scalar::Int(1));
        tx.map(b"notes").register(b"nk", Scalar::Int(2));
        tx.map(b"board").register(b"bk2", Scalar::Int(3));
    })
}

#[test]
fn a_group_the_zone_cut_straddles_lands_its_survivors() {
    let (mut author, setup) = seeded();
    let group = straddling(&mut author);
    let mut projected = room(&setup, &group[..2]);
    projected.project_zones(&zoned(), &za_only(), None);

    let restored = round_trip(&projected);
    assert_eq!(
        zoned_int(&restored, b"board", b"bk"),
        Some(1),
        "the za survivor stranded in the projected buffer"
    );
    assert!(
        restored.get(b"notes").is_none(),
        "the withheld partition is absent"
    );
}

#[test]
fn a_group_inside_the_authorized_zones_keeps_its_tag() {
    let (mut author, setup) = seeded();
    // Three members, all in za, of which the room has folded only two — a group
    // legitimately in flight, which the cut does not touch.
    let group = author.atomic_transact(|tx| {
        tx.map(b"board").register(b"one", Scalar::Int(1));
        tx.map(b"board").register(b"two", Scalar::Int(2));
        tx.map(b"board").register(b"three", Scalar::Int(3));
    });
    let mut projected = room(&setup, &group[..2]);
    projected.project_zones(&zoned(), &za_only(), None);

    let mut restored = round_trip(&projected);
    assert_eq!(
        zoned_int(&restored, b"board", b"one"),
        None,
        "an uncut group is still all-or-nothing"
    );
    restored.apply(&group[2]);
    assert_eq!(zoned_int(&restored, b"board", b"one"), Some(1));
    assert_eq!(zoned_int(&restored, b"board", b"two"), Some(2));
    assert_eq!(zoned_int(&restored, b"board", b"three"), Some(3));
}

#[test]
fn an_unzoned_member_of_a_straddling_group_lands_too() {
    // The root partition is always authorized, so an unzoned member survives every
    // zone cut — and must not be held back by a withheld zone's member.
    let (mut author, setup) = seeded();
    let group = author.atomic_transact(|tx| {
        tx.map(b"loose").register(b"lk", Scalar::Int(4));
        tx.map(b"notes").register(b"nk", Scalar::Int(5));
        tx.map(b"loose").register(b"lk2", Scalar::Int(6));
    });
    let mut projected = room(&setup, &group[..2]);
    projected.project_zones(&zoned(), &za_only(), None);

    let restored = round_trip(&projected);
    assert_eq!(zoned_int(&restored, b"loose", b"lk"), Some(4));
}

#[test]
fn a_projection_that_cuts_nothing_leaves_every_tag_alone() {
    // The same straddling group, projected to a subscriber admitted to both zones:
    // nothing is withheld, so the group is still whole and still atomic.
    let (mut author, setup) = seeded();
    let group = straddling(&mut author);
    let mut projected = room(&setup, &group[..2]);
    let both = HashSet::from([zone_of(b"board"), zone_of(b"notes")]);
    projected.project_zones(&zoned(), &both, None);

    let mut restored = round_trip(&projected);
    assert_eq!(
        zoned_int(&restored, b"board", b"bk"),
        None,
        "an uncut group is still all-or-nothing"
    );
    restored.apply(&group[2]);
    assert_eq!(zoned_int(&restored, b"board", b"bk"), Some(1));
    assert_eq!(zoned_int(&restored, b"notes", b"nk"), Some(2));
}

/// The ops of `group` a decoded projection holds without having applied them.
fn unapplied(d: &Document, group: &[Op]) -> usize {
    let seen: HashSet<_> = d.seen().collect();
    group.iter().filter(|op| !seen.contains(&op.id)).count()
}

#[test]
fn a_straddling_groups_survivor_is_applied_not_held() {
    // C6/C9's accounting reads the ids a replica holds, buffered ones included. A
    // destranded survivor has to land in the applied set, not linger in the buffer.
    let (mut author, setup) = seeded();
    let group = straddling(&mut author);
    let mut projected = room(&setup, &group[..2]);
    projected.project_zones(&zoned(), &za_only(), None);
    let restored = round_trip(&projected);
    assert_eq!(
        unapplied(&restored, &group[..1]),
        0,
        "the survivor is still waiting on a member that will never arrive"
    );
}

/// A group of `n` two-member atomic transactions inside the withheld zone, folded by
/// a room replica so each one commits and spends its bucket key there.
fn zb_groups(author: &mut Document, n: usize) -> Vec<Op> {
    (0..n)
        .flat_map(|i| {
            author.atomic_transact(|tx| {
                tx.map(b"notes")
                    .register(format!("n{i}a").as_bytes(), Scalar::Int(1));
                tx.map(b"notes")
                    .register(format!("n{i}b").as_bytes(), Scalar::Int(2));
            })
        })
        .collect()
}

#[test]
fn a_zone_projection_serves_no_record_of_the_withheld_partitions_groups() {
    // A resolved-group key names an author and a group, never a partition, so a key
    // kept through the cut would tell a za-only subscriber that the author resolved a
    // group it cannot see. The record goes whole, and the recipient buckets a member
    // arriving under one of those ids as a group it has never seen resolve.
    let (mut author, setup) = seeded();
    let group = zb_groups(&mut author, 1);
    let stray = author.transact(|tx| {
        tx.map(b"loose").register(b"lk", Scalar::Int(9));
    });
    // Re-tagged into the withheld zone's group, but targeting the root partition, so
    // the cut leaves it applicable at the recipient — only the record decides.
    let mut forged = stray[0].clone();
    forged.tx = group[0].tx;
    // The group's other member, on its own key, so the reading proves both landed.
    let mut partner = author.transact(|tx| {
        tx.map(b"loose").register(b"lk2", Scalar::Int(8));
    })[0]
        .clone();
    partner.tx = group[0].tx;

    let mut whole = room(&setup, &group);
    assert!(
        whole.apply(&forged),
        "the unprojected room holds the record and merges the stray"
    );

    let mut projected = room(&setup, &group);
    projected.project_zones(&zoned(), &za_only(), None);
    let mut restored = round_trip(&projected);
    assert!(
        !restored.apply(&forged),
        "the projection served its record of the withheld zone's groups"
    );
    assert_eq!(
        zoned_int(&restored, b"loose", b"lk"),
        None,
        "the stray is held, not applied"
    );
    // Held as a *bucket* rather than refused: a second member under that id commits
    // the pair, which is what tells the two apart.
    assert!(
        restored.apply(&partner),
        "the bucket completed on its second member"
    );
    assert_eq!(zoned_int(&restored, b"loose", b"lk"), Some(9));
    assert_eq!(
        zoned_int(&restored, b"loose", b"lk2"),
        Some(8),
        "only one of the pair landed"
    );
}

#[test]
fn a_zone_projection_does_not_grow_with_the_withheld_partitions_group_count() {
    // The size of what is served must not count the groups behind the cut.
    let served = |n: usize| {
        let (mut author, setup) = seeded();
        let groups = zb_groups(&mut author, n);
        let mut projected = room(&setup, &groups);
        projected.project_zones(&zoned(), &za_only(), None);
        projected.encode_state().len()
    };
    assert_eq!(
        served(1),
        served(9),
        "the snapshot counts the groups the withheld zone resolved"
    );
}
