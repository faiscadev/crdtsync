//! An atomic transaction is emitted one group per partition (C2).
//!
//! ARCHITECTURE §Scope Constraints says a Tx must stay within one zone, and
//! §Not Shipped lists cross-zone transactions among the things the engine does not
//! offer. A commit closes one group per partition its ops fall in, which is what
//! makes that true at the seam it is decided: a commit wholly inside one partition —
//! the ordinary case, and every case in a document declaring no zones — is a single
//! group, and one that straddles is a group per zone, each id derived from its own
//! members' sequences.
//!
//! The reason is that only a subscriber admitted to every partition a group spans can
//! receive it whole. A zone-scoped subscription is served a subset of the room's
//! partitions, so its filter withholds the other zone's members and destrands (C11)
//! what is left; a group straddling the cut therefore loses its atomic view at exactly
//! the recipients zones exist to serve, while a full-doc subscriber keeps it. Cut to
//! partitions, every recipient whose subscription cuts on zone holds a group whole or
//! not at all.
//!
//! What a straddling commit gives up is atomicity *across* the zones, which
//! §Not Shipped never offered; what it keeps is every edit and per-zone atomicity.
//! The rejected alternative was refusing the straddle at emit — see DECISIONS.md.
//!
//! Destranding is the wire-level floor underneath this, not a thing it replaces: the
//! doc-ACL read filter cuts on a dimension no scope constraint aligns groups to, so a
//! group it splits still ships untagged, and the wire admits a straddling group from a
//! peer whose emitter does not cut.

use std::collections::BTreeMap;

use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::{path, zone, Document, Op, Scalar, Schema, Tx, TxId};

mod common;
use common::cid;

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) beside an unzoned
/// root-partition one (`/loose`), each holding a text sequence to edit.
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

fn seq_of(sect: &[u8]) -> Vec<u8> {
    p(&[sect, b"seq"])
}

/// Insert `s` at the head of `sect`'s sequence — ops wholly in `sect`'s partition.
fn edit(d: &mut Document, sect: &[u8], s: &str) -> Vec<Op> {
    path::text_insert(d, &seq_of(sect), 0, s)
}

/// A replica bound to the zoned schema with a live text sequence under each of the
/// three sections, beside the ops that seeded them — a peer replays those first, or
/// an edit anchored in one of the runs resolves nowhere there.
fn seeded(client: u8) -> (Document, Vec<Op>) {
    let mut d = Document::new(cid(client));
    d.set_schema(zoned());
    let mut seed = Vec::new();
    for sect in [&b"board"[..], b"notes", b"loose"] {
        seed.extend(edit(&mut d, sect, "hello"));
    }
    (d, seed)
}

/// A second replica of the same document, caught up on `seed`.
fn peer(client: u8, seed: &[Op]) -> Document {
    let mut d = Document::new(cid(client));
    d.set_schema(zoned());
    for op in seed {
        d.apply(op);
    }
    d
}

fn text_id(d: &Document, sect: &[u8]) -> ElementId {
    let map = ElementId::derive(d.root_id(), sect, ElementKind::Map);
    ElementId::derive(map, b"seq", ElementKind::Text)
}

/// The transaction tag the ops of each partition carry, keyed by partition. Fails
/// if a partition's ops disagree over their tag, since a partition is exactly what
/// a group is cut to.
fn groups(ops: &[Op]) -> BTreeMap<Option<u32>, Option<Tx>> {
    let mut out: BTreeMap<Option<u32>, Option<Tx>> = BTreeMap::new();
    for op in ops {
        let seen = out.entry(op.zone).or_insert(op.tx);
        assert_eq!(
            *seen, op.tx,
            "the partition {:?} carries two different tags",
            op.zone
        );
    }
    out
}

/// How many of `ops` were stamped in `zone`.
fn members(ops: &[Op], zone: Option<u32>) -> u32 {
    u32::try_from(ops.iter().filter(|op| op.zone == zone).count()).expect("a small group")
}

/// The tag every op in `ops` must carry, or the assertion fails.
fn one_group(ops: &[Op]) -> Tx {
    let groups = groups(ops);
    assert_eq!(groups.len(), 1, "the commit emitted more than one group");
    groups
        .into_values()
        .next()
        .flatten()
        .expect("the group is tagged")
}

/// A copy of `op` re-tagged into `id` at `count` — what a relay forges when it
/// pushes a stray into a group it saw go by.
fn retagged(op: &Op, id: TxId, count: u32) -> Op {
    Op {
        tx: Some(Tx { id, count }),
        ..op.clone()
    }
}

// --- one partition, one group ---

#[test]
fn a_commit_wholly_inside_one_zone_is_one_atomic_group() {
    let (mut d, _) = seeded(1);
    d.begin_atomic();
    edit(&mut d, b"board", "x");
    edit(&mut d, b"board", "y");
    let ops = d.commit_atomic();

    let count = members(&ops, Some(zone_of(b"board")));
    assert_eq!(count, u32::try_from(ops.len()).unwrap());
    assert!(count > 1, "the group has members to hold together");
    assert_eq!(one_group(&ops).count, count, "every member names one group");
}

#[test]
fn a_commit_in_an_unpartitioned_document_is_one_atomic_group() {
    // No schema bound means no zones, so every op takes the root partition and the
    // commit has exactly one partition to cut to.
    let mut d = Document::new(cid(1));
    let ops = d.atomic_transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
    });
    assert!(ops.iter().all(|op| op.zone.is_none()));
    assert_eq!(one_group(&ops).count, 2);
}

// --- a straddle is one group per partition ---

#[test]
fn a_commit_straddling_two_zones_emits_one_group_per_zone() {
    let (mut d, _) = seeded(1);
    d.begin_atomic();
    edit(&mut d, b"board", "x");
    edit(&mut d, b"notes", "y");
    edit(&mut d, b"board", "z");
    let ops = d.commit_atomic();

    let groups = groups(&ops);
    assert_eq!(groups.len(), 2, "the commit closed two groups, not one");
    let board = groups[&Some(zone_of(b"board"))].expect("the za members are tagged");
    let notes = groups[&Some(zone_of(b"notes"))].expect("the zb members are tagged");
    assert_eq!(
        (board.count, notes.count),
        (
            members(&ops, Some(zone_of(b"board"))),
            members(&ops, Some(zone_of(b"notes")))
        ),
        "each group counts only its own partition's members"
    );
    assert_ne!(
        board.id, notes.id,
        "each group's id is derived from its own members' sequences"
    );
}

#[test]
fn the_root_partition_takes_a_group_of_its_own_beside_a_zones() {
    // An unzoned location is the root partition, which every zone-scoped subscriber
    // holds — a partition like any other here, not an exemption from the split.
    let (mut d, _) = seeded(1);
    d.begin_atomic();
    edit(&mut d, b"loose", "x");
    edit(&mut d, b"board", "y");
    let ops = d.commit_atomic();

    let groups = groups(&ops);
    assert_eq!(groups.len(), 2);
    let root = groups[&None].expect("the root-partition members are tagged");
    let board = groups[&Some(zone_of(b"board"))].expect("the za members are tagged");
    assert_eq!(
        (root.count, board.count),
        (members(&ops, None), members(&ops, Some(zone_of(b"board"))))
    );
    assert_ne!(root.id, board.id);
}

#[test]
fn a_straddling_commit_still_emits_every_edit_and_converges() {
    // The split gives up atomicity across the zones, never an edit: a peer that
    // receives both groups holds exactly the state the author does.
    let (mut a, seed) = seeded(1);
    a.begin_atomic();
    edit(&mut a, b"board", "x");
    edit(&mut a, b"notes", "y");
    let ops = a.commit_atomic();

    let mut b = peer(2, &seed);
    for op in &ops {
        b.apply(op);
    }
    for sect in [&b"board"[..], b"notes"] {
        assert_eq!(
            path::text_get(&b, &seq_of(sect)),
            path::text_get(&a, &seq_of(sect)),
            "the {} sequence converged",
            String::from_utf8_lossy(sect)
        );
    }
}

// --- the atomic view survives the zone filter ---

#[test]
fn a_zone_scoped_subscriber_receives_its_partitions_group_whole() {
    // The payoff. A subscription admitting only zb withholds the za members, and
    // those name a group this subscriber holds no member of — so the cut runs
    // between groups rather than through one, and nothing it receives is destranded.
    // A group spanning both partitions would instead lose its tags here, leaving the
    // zb edits to merge one at a time.
    let (mut a, seed) = seeded(1);
    a.begin_atomic();
    edit(&mut a, b"board", "x");
    edit(&mut a, b"notes", "y");
    let ops = a.commit_atomic();

    let admits = |op: &Op| op.zone != Some(zone_of(b"board"));
    let (kept, dropped): (Vec<Op>, Vec<Op>) = ops.into_iter().partition(admits);
    let split = crdtsync_core::split_groups(&dropped);
    let held = crdtsync_core::split_groups(&kept);
    assert!(
        !split.is_empty(),
        "the filter did withhold a group's members"
    );
    assert!(
        split.is_disjoint(&held),
        "the cut runs between groups, never through one"
    );

    let mut delivered = kept;
    crdtsync_core::destrand_split(delivered.iter_mut(), &split);
    assert!(
        delivered.iter().all(|op| op.tx.is_some()),
        "the zb group reaches the subscriber tagged"
    );

    // And it is a genuine atomic view: every member but the last is held, and they
    // land together on the arrival that completes the group.
    let mut b = peer(
        2,
        &seed
            .iter()
            .filter(|op| admits(op))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let (last, held) = delivered.split_last().expect("the group has members");
    for op in held {
        assert!(!b.apply(op), "an incomplete group applies nothing");
    }
    assert_eq!(
        path::text_get(&b, &seq_of(b"notes")).as_deref(),
        Some("hello"),
        "the partial group is invisible"
    );
    assert!(b.apply(last), "the last member commits the group");
    assert_eq!(
        path::text_get(&b, &seq_of(b"notes")),
        path::text_get(&a, &seq_of(b"notes"))
    );
}

// --- a partition the governing region does not resolve ---

#[test]
fn an_op_whose_governing_region_is_ambiguous_joins_the_root_partitions_group() {
    // C74 makes an op's partition the region it *governs*, and a mark's two anchors
    // must agree on one — endpoints in two zones are a `CrossZoneAnchor` violation
    // the read repairs away, so a straddling mark names no partition and keeps the
    // root (C82). That is the case where "which zone is this op in" has no simple
    // answer, and the split reads exactly the answer the envelope carries: the mark
    // groups with the root partition, not with the board edit beside it.
    let (mut d, _) = seeded(1);
    let board = text_id(&d, b"board");
    let notes = text_id(&d, b"notes");
    d.begin_atomic();
    edit(&mut d, b"board", "x");
    d.transact(|tx| {
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
    let ops = d.commit_atomic();

    let groups = groups(&ops);
    assert_eq!(groups.len(), 2, "the ambiguous mark joined the za group");
    let root = groups[&None].expect("the mark is tagged in the root partition");
    let board = groups[&Some(zone_of(b"board"))].expect("the za members are tagged");
    assert_eq!(root.count, members(&ops, None));
    assert_eq!(board.count, members(&ops, Some(zone_of(b"board"))));
}

#[test]
fn a_mark_that_agrees_with_the_edits_beside_it_shares_their_group() {
    // The positive control for the case above: anchors that agree resolve to the
    // zone they anchor in, so the mark is a member of that zone's group rather than
    // a group of its own — what pins the case above as the disagreement and not a
    // resolution that never happens.
    let (mut d, _) = seeded(1);
    let board = text_id(&d, b"board");
    d.begin_atomic();
    edit(&mut d, b"board", "x");
    d.transact(|tx| {
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
    let ops = d.commit_atomic();

    assert!(ops.iter().all(|op| op.zone == Some(zone_of(b"board"))));
    assert_eq!(one_group(&ops).count, u32::try_from(ops.len()).unwrap());
}

// --- the author spends every key it mints ---

#[test]
fn a_stray_of_either_split_group_lands_at_the_author() {
    // Tagging spends a group's bucket key at the author, which buckets nothing of
    // its own — so a split commit has to spend *every* key it minted. Miss one and a
    // stray under it waits forever at the author while every receiver merges it: one
    // op set, two states.
    let (mut a, seed) = seeded(1);
    a.begin_atomic();
    edit(&mut a, b"board", "x");
    edit(&mut a, b"notes", "y");
    let ops = a.commit_atomic();
    let ids: Vec<TxId> = groups(&ops)
        .values()
        .map(|tx| tx.expect("every partition is tagged").id)
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1], "the two partitions minted two keys");

    // Ops of the author's own id space it has not published — what a relay forges
    // from. The relay replays the author's history first, so its next sequences are
    // the ones the author would mint next and its edits anchor in the same runs.
    let (mut relay, _) = seeded(1);
    relay.begin_atomic();
    edit(&mut relay, b"board", "x");
    edit(&mut relay, b"notes", "y");
    let _ = relay.commit_atomic();
    let strays: Vec<Op> = ids
        .iter()
        .map(|id| {
            let source = edit(&mut relay, b"loose", "q");
            retagged(source.last().expect("the edit emitted an op"), *id, 2)
        })
        .collect();

    let mut b = peer(9, &seed);
    for op in ops.iter().chain(&strays) {
        b.apply(op);
    }
    for stray in &strays {
        assert!(
            a.apply(stray),
            "the author merged a stray of its own spent group"
        );
    }
    assert_eq!(
        path::text_get(&a, &seq_of(b"loose")),
        path::text_get(&b, &seq_of(b"loose")),
        "the author and the receiver merged the same strays"
    );
}

// --- the cap bounds a partition, not the commit ---

#[test]
#[cfg_attr(miri, ignore = "thousand-member groups are slow under Miri")]
fn the_member_cap_bounds_a_partition_rather_than_the_commit() {
    // A partition is a group, and the cap is a per-group bound — so an oversized
    // partition streams untagged while an in-range one beside it is still tagged,
    // where an uncut commit would have streamed both. What bounds the members a
    // commit asks a recipient to hold is therefore the cap times the partitions it
    // spans, at most one per declared zone plus the root.
    let (mut d, _) = seeded(1);
    d.begin_atomic();
    d.transact(|tx| {
        for i in 0..=crdtsync_core::MAX_TX_MEMBERS {
            tx.map(b"board")
                .register(format!("k{i}").as_bytes(), Scalar::Int(i64::from(i)));
        }
        tx.map(b"notes").register(b"n1", Scalar::Int(1));
        tx.map(b"notes").register(b"n2", Scalar::Int(2));
    });
    let ops = d.commit_atomic();

    let board = Some(zone_of(b"board"));
    let notes = Some(zone_of(b"notes"));
    assert!(
        members(&ops, board) > crdtsync_core::MAX_TX_MEMBERS,
        "the za partition is past the cap"
    );
    assert!(
        ops.iter()
            .filter(|op| op.zone == board)
            .all(|op| op.tx.is_none()),
        "a partition no receiver may accept is streamed rather than tagged"
    );
    let tagged = groups(&ops)[&notes].expect("the in-range partition is tagged");
    assert_eq!(tagged.count, members(&ops, notes));
}

// --- undo replays through the same cut ---

#[test]
fn undoing_a_cross_zone_atomic_intention_replays_one_group_per_zone() {
    // An intention is undone by emitting its inverses through the same commit seam,
    // so the undo is cut exactly as the forward edits were: a peer scoped to one
    // zone sees the revert of its own partition all-or-nothing, and never a group
    // half of which it can never receive.
    let (mut d, _) = seeded(1);
    let undo = crdtsync_core::UndoManager::new();
    let forward = undo.atomic_group(&mut d, |doc| {
        edit(doc, b"board", "x");
        edit(doc, b"notes", "y");
    });
    assert_eq!(
        groups(&forward).len(),
        2,
        "the forward edits were cut in two"
    );

    let inverses = undo.undo(&mut d).expect("the intention undoes");
    let groups = groups(&inverses);
    assert_eq!(groups.len(), 2, "the undo is cut the same way");
    for zone in [Some(zone_of(b"board")), Some(zone_of(b"notes"))] {
        let tx = groups[&zone].expect("each partition's inverses are tagged");
        assert_eq!(tx.count, members(&inverses, zone));
    }
    assert_eq!(
        path::text_get(&d, &seq_of(b"board")).as_deref(),
        Some("hello"),
        "the board edit is reverted"
    );
    assert_eq!(
        path::text_get(&d, &seq_of(b"notes")).as_deref(),
        Some("hello")
    );
}
