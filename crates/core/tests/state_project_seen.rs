//! What a projected snapshot keeps of its recipient's own op ids.
//!
//! `project_zones` and `project_read_paths` withhold a partition, so they cannot
//! serve that partition's causal frontier: the ids of ops the recipient may not read
//! would name their existence and their count. Scrubbing it whole, though, also
//! throws away the recipient's *own* ids — and minting walks the ids a replica holds,
//! so a recipient that persists its `ClientId`, restarts, and adopts a projected
//! snapshot mints straight into ids the room's log already holds. An `OpId` is the
//! dedup key, so every one of those writes is dropped at ingest, silently.
//!
//! Nor is a fresh replica the worst of it. The op-seq position is the first sequence
//! the replica has not published — a *hole*, not a high-water — so a replica caught
//! up by a delta that withheld a member it authored (the same per-op filter the
//! fan-out applies) reports the hole, adopts a projected snapshot at it, and re-mints
//! every id above the hole as well. Any hole in the run is enough.
//!
//! So the frontier a projection serves names exactly one replica: the one adopting
//! the snapshot. Its own authorship is the one thing a scrub can hand back to it
//! without telling it anything about the withheld partition — the recipient
//! originated those ops. Every *other* replica's ids go, so the existence and count
//! of their ops in the withheld partition stay absent, which is what the scrub is
//! for. What a recipient learns of its own run is bounded by who may present its
//! identity, which is the transport's credential check rather than this seam's.

use std::collections::HashSet;

use crdtsync_core::{zone, ClientId, Document, Element, Op, OpId, Scalar, Schema};

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

/// The replica adopting the projected snapshot — a reader admitted to za and the
/// root partition, never to zb.
fn reader() -> ClientId {
    cid(1)
}

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

/// A read predicate admitting the root, `/board` and `/loose` — the doc-ACL analogue
/// of the za-only zone scope, denying `/notes`.
fn reads_board(path: &[Vec<u8>]) -> bool {
    match path.first() {
        None => true,
        Some(k) => k.as_slice() != b"notes",
    }
}

/// A replica of `client` bound to the zoned schema, so its ops carry their zone.
fn replica(client: ClientId) -> Document {
    let mut d = Document::new(client);
    d.set_schema(zoned());
    d
}

/// The room's replica — a server's, which merges under its own identity and never
/// mints — holding `ops`.
fn room(ops: &[Op]) -> Document {
    let mut d = replica(cid(9));
    for op in ops {
        d.apply(op);
    }
    d
}

/// The reader's durable run: a create batch seeding all three subtrees, then one
/// further write into each partition — the root (`/loose`), the withheld zone zb
/// (`/notes`), then the readable zone za (`/board`). zb's ops sit in the middle of
/// the run, so a replica missing them has a hole with published ids above it.
fn reader_run() -> (Document, Vec<Op>) {
    let mut d = replica(reader());
    let mut ops = d.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(0));
        tx.map(b"notes").register(b"nseed", Scalar::Int(0));
        tx.map(b"loose").register(b"lseed", Scalar::Int(0));
    });
    ops.extend(d.transact(|tx| {
        tx.map(b"loose").register(b"l", Scalar::Int(1));
    }));
    ops.extend(d.transact(|tx| {
        tx.map(b"notes").register(b"n", Scalar::Int(1));
    }));
    ops.extend(d.transact(|tx| {
        tx.map(b"board").register(b"b", Scalar::Int(1));
    }));
    (d, ops)
}

/// The seqs `client` holds in a replica's dedup set, ascending.
fn seqs_of(d: &Document, client: ClientId) -> Vec<u64> {
    let mut seqs: Vec<u64> = d
        .seen()
        .filter(|id| id.client == client)
        .map(|id| id.seq)
        .collect();
    seqs.sort_unstable();
    seqs
}

/// The distinct replicas a dedup set names.
fn authors(d: &Document) -> HashSet<ClientId> {
    d.seen().map(|id| id.client).collect()
}

/// The seqs of a batch of ops.
fn seqs(ops: &[Op]) -> Vec<u64> {
    ops.iter().map(|op| op.id.seq).collect()
}

/// The Int behind `outer.inner`, or `None` when either level is absent.
fn nested(d: &Document, outer: &[u8], inner: &[u8]) -> Option<i64> {
    let Some(Element::Map(m)) = d.get(outer) else {
        return None;
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

/// The zone-projected snapshot of `ops` as served to `recipient`.
fn zone_projected(ops: &[Op], recipient: Option<ClientId>) -> Document {
    let mut d = room(ops);
    d.project_zones(&zoned(), &za_only(), recipient);
    d
}

/// The read-path-projected snapshot of `ops` as served to `recipient`.
fn read_projected(ops: &[Op], recipient: Option<ClientId>) -> Document {
    let mut d = room(ops);
    d.project_read_paths(reads_board, recipient);
    d
}

/// A replica restored from `projected` under the reader's identity, authoring from
/// `next_seq` — what `ClientSession` does with an adopted snapshot.
fn restored(projected: &Document, next_seq: u64) -> Document {
    Document::decode_state_as(reader(), next_seq, &projected.encode_state()).expect("decodes")
}

// --- the frontier a projection serves ---

#[test]
fn a_zone_projection_keeps_every_id_the_recipient_published() {
    let (_, ops) = reader_run();
    let projected = zone_projected(&ops, Some(reader()));
    assert_eq!(
        seqs_of(&projected, reader()),
        seqs(&ops),
        "the recipient's own run, its withheld-zone ids included, survives the scrub",
    );
}

#[test]
fn a_read_path_projection_keeps_every_id_the_recipient_published() {
    let (_, ops) = reader_run();
    let projected = read_projected(&ops, Some(reader()));
    assert_eq!(seqs_of(&projected, reader()), seqs(&ops));
}

#[test]
fn a_projection_naming_no_recipient_scrubs_the_frontier_whole() {
    // A snapshot no replica will author from carries no frontier at all — the
    // recipient is what makes keeping any of it justified.
    let (_, ops) = reader_run();
    assert!(authors(&zone_projected(&ops, None)).is_empty());
    assert!(authors(&read_projected(&ops, None)).is_empty());
}

#[test]
fn a_zone_projection_authorized_for_every_zone_still_scrubs() {
    // Projecting is not the identity even when nothing is withheld: the pass scrubs
    // whatever set it is handed, so a whole-zone subscriber is served the room's
    // replica by its caller declining to project at all — not by this pass turning
    // into a no-op. `project_read_paths` carries that guard itself (a whole-document
    // reader drops nothing, so it scrubs nothing); zones does not.
    let (_, ops) = reader_run();
    let mut other = replica(cid(3));
    for op in &ops {
        other.apply(op);
    }
    let foreign = other.transact(|tx| {
        tx.map(b"loose").register(b"o", Scalar::Int(7));
    });
    let mut all = room(&ops);
    for op in &foreign {
        all.apply(op);
    }
    assert_eq!(authors(&all), HashSet::from([reader(), cid(3)]));

    all.project_zones(
        &zoned(),
        &HashSet::from([zone_of(b"board"), zone_of(b"notes")]),
        Some(reader()),
    );
    assert_eq!(authors(&all), HashSet::from([reader()]));
    assert!(all.get(b"notes").is_some(), "no zone was withheld");
}

// --- the restart: a fresh replica adopting a projected snapshot ---

#[test]
fn a_restarted_zone_reader_mints_past_its_durable_run() {
    // The persisted thing is the `ClientId`; the replica is rebuilt from the room,
    // so it reports a position of 0 and the adopted snapshot is its only evidence.
    let (_, ops) = reader_run();
    let projected = zone_projected(&ops, Some(reader()));
    let mut restarted = restored(&projected, 0);

    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });
    assert_eq!(
        seqs(&fresh),
        vec![ops.len() as u64],
        "re-minted an id the room's log already holds",
    );
}

#[test]
fn a_restarted_read_path_reader_mints_past_its_durable_run() {
    let (_, ops) = reader_run();
    let projected = read_projected(&ops, Some(reader()));
    let mut restarted = restored(&projected, 0);

    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });
    assert_eq!(seqs(&fresh), vec![ops.len() as u64]);
}

#[test]
fn the_write_after_a_projected_zone_catch_up_reaches_the_room() {
    // The observable failure is not divergence, it is a *disappearance*: the room
    // already holds every id of the reader's durable run, so a re-minted one is
    // dropped at its dedup set and the edit is simply gone.
    let (_, ops) = reader_run();
    let projected = zone_projected(&ops, Some(reader()));
    let mut restarted = restored(&projected, 0);
    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });

    let mut room = room(&ops);
    for op in &fresh {
        room.apply(op);
    }
    assert_eq!(
        nested(&room, b"board", b"after"),
        Some(9),
        "the post-restart write was deduped away",
    );
    assert_eq!(nested(&room, b"board", b"b"), Some(1), "the run survives");
}

#[test]
fn the_write_after_a_projected_read_catch_up_reaches_the_room() {
    let (_, ops) = reader_run();
    let projected = read_projected(&ops, Some(reader()));
    let mut restarted = restored(&projected, 0);
    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });

    let mut room = room(&ops);
    for op in &fresh {
        room.apply(op);
    }
    assert_eq!(nested(&room, b"board", b"after"), Some(9));
    assert_eq!(nested(&room, b"board", b"b"), Some(1));
}

// --- the wider case: a hole in the run, not just an empty one ---

/// The reader's replica of its own run with zb's ops withheld — what the per-op
/// filter delivers a za-scoped catch-up. Its position is the hole zb left, and it has
/// published ids above the hole.
fn live_with_zb_withheld(ops: &[Op]) -> Document {
    let mut d = Document::new(reader());
    let zb = zone_of(b"notes");
    for op in ops.iter().filter(|op| op.zone != Some(zb)) {
        d.apply(op);
    }
    d
}

#[test]
fn a_hole_left_by_a_withheld_member_is_not_the_position_to_mint_from() {
    let (_, ops) = reader_run();
    let live = live_with_zb_withheld(&ops);
    let hole = live.next_seq();
    assert!(
        hole < ops.len() as u64 - 1,
        "the fixture needs published ids above the hole, got {hole} of {}",
        ops.len(),
    );

    // It adopts the projected snapshot at the hole it reports. Every id above the
    // hole is still the room's, and the hole itself is an id the room holds — zb's
    // op — so neither is free.
    let projected = zone_projected(&ops, Some(reader()));
    let mut adopted = restored(&projected, hole);
    let fresh = adopted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });
    assert_eq!(
        seqs(&fresh),
        vec![ops.len() as u64],
        "re-minted an id the withheld member already holds",
    );

    let mut room = room(&ops);
    for op in &fresh {
        room.apply(op);
    }
    assert_eq!(
        nested(&room, b"board", b"after"),
        Some(9),
        "the write above the hole was deduped away",
    );
}

#[test]
fn a_hole_left_by_a_withheld_read_path_is_not_the_position_to_mint_from() {
    let (_, ops) = reader_run();
    let hole = live_with_zb_withheld(&ops).next_seq();
    let projected = read_projected(&ops, Some(reader()));
    let mut adopted = restored(&projected, hole);
    let fresh = adopted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });
    assert_eq!(seqs(&fresh), vec![ops.len() as u64]);
}

#[test]
fn an_id_the_room_holds_only_in_its_buffer_is_not_re_minted() {
    // Held, not merely applied: an op waiting on its transaction group sits in the
    // room's buffer with its id out of the dedup set, and the log holds it all the
    // same. Re-minting it leaves two different ops under one identity once the
    // buffer drains — divergence, not a dropped write.
    let (mut d, ops) = reader_run();
    let group = d.atomic_transact(|tx| {
        tx.map(b"loose").register(b"g1", Scalar::Int(1));
        tx.map(b"loose").register(b"g2", Scalar::Int(2));
    });
    // The room holding the run plus the group's first member, which waits on the
    // partner that never arrives.
    let held = || {
        let mut d = room(&ops);
        d.apply(&group[0]);
        d
    };

    let mut by_zone = held();
    by_zone.project_zones(&zoned(), &za_only(), Some(reader()));
    let mut by_read = held();
    by_read.project_read_paths(reads_board, Some(reader()));

    for (label, projected) in [("zones", by_zone), ("read paths", by_read)] {
        assert_eq!(
            restored(&projected, 0).next_seq(),
            group[0].id.seq + 1,
            "{label}: the buffered id was reported free",
        );
    }
}

#[test]
fn an_own_id_the_withheld_partition_buffered_survives_the_projection() {
    // The frontier is read before the buffer is filtered, so an own op waiting in the
    // partition the projection drops is as published as one waiting in a partition it
    // keeps: the buffered zb member goes with its zone, its id does not.
    let (mut d, ops) = reader_run();
    let group = d.atomic_transact(|tx| {
        tx.map(b"notes").register(b"g1", Scalar::Int(1));
        tx.map(b"notes").register(b"g2", Scalar::Int(2));
    });
    let mut held = room(&ops);
    held.apply(&group[0]);
    held.project_zones(&zoned(), &za_only(), Some(reader()));

    assert!(
        held.seen().any(|id| id == group[0].id),
        "the withheld partition's buffered own id was dropped with its op",
    );
    assert_eq!(restored(&held, 0).next_seq(), group[0].id.seq + 1);
}

#[test]
fn a_projected_frontier_names_no_id_the_buffer_still_holds() {
    // The dedup set is what a replica applied and the buffer what it holds unapplied;
    // an id in both is a state no decode accepts. A frontier cut back to the
    // recipient's own ids has to stay clear of what the buffer still carries.
    let (mut d, ops) = reader_run();
    let group = d.atomic_transact(|tx| {
        tx.map(b"loose").register(b"g1", Scalar::Int(1));
        tx.map(b"loose").register(b"g2", Scalar::Int(2));
    });
    let mut held = room(&ops);
    held.apply(&group[0]);
    held.project_zones(&zoned(), &za_only(), Some(reader()));

    assert!(
        !held.seen().any(|id| id == group[0].id),
        "an id the retained buffer holds was also named in the frontier",
    );
    Document::decode_state(&held.encode_state()).expect("the projected snapshot decodes");
}

// --- the privacy property the scrub exists for ---

#[test]
fn a_zone_projection_carries_no_id_of_the_withheld_partitions_other_authors() {
    let (_, ops) = reader_run();
    let mut other = replica(cid(3));
    other.apply(&ops[0]);
    let hidden = other.transact(|tx| {
        tx.map(b"notes").register(b"x", Scalar::Int(1));
    });

    let mut room = room(&ops);
    for op in &hidden {
        room.apply(op);
    }
    room.project_zones(&zoned(), &za_only(), Some(reader()));

    assert_eq!(
        authors(&room),
        HashSet::from([reader()]),
        "the frontier names the recipient and nobody else",
    );
    assert!(room.get(b"notes").is_none(), "the partition itself is gone");
}

#[test]
fn a_zone_projection_does_not_vary_with_the_withheld_partitions_op_count() {
    // The sharper statement: the served bytes are a function of the authorized
    // partitions and the recipient's own run alone. A hidden zone busy with another
    // replica's ops and one that is idle project to the same snapshot, so the
    // recipient cannot count what it cannot read.
    let (_, ops) = reader_run();
    let busy = {
        let mut other = replica(cid(3));
        other.apply(&ops[0]);
        let mut room = room(&ops);
        for i in 0..5 {
            let key = format!("x{i}").into_bytes();
            for op in &other.transact(|tx| {
                tx.map(b"notes").register(&key, Scalar::Int(i));
            }) {
                room.apply(op);
            }
        }
        room.project_zones(&zoned(), &za_only(), Some(reader()));
        room.encode_state()
    };
    let idle = zone_projected(&ops, Some(reader())).encode_state();
    assert_eq!(busy, idle, "the withheld zone's activity is observable");
}

#[test]
fn a_read_path_projection_carries_no_id_of_the_denied_subtrees_other_authors() {
    let (_, ops) = reader_run();
    let mut other = Document::new(cid(3));
    other.apply(&ops[0]);
    let hidden = other.transact(|tx| {
        tx.map(b"notes").register(b"x", Scalar::Int(1));
    });

    let mut room = room(&ops);
    for op in &hidden {
        room.apply(op);
    }
    room.project_read_paths(reads_board, Some(reader()));

    assert_eq!(authors(&room), HashSet::from([reader()]));
    assert!(room.get(b"notes").is_none());
}

#[test]
fn the_recipients_own_ids_reveal_nothing_of_the_partition_they_fell_in() {
    // What the recipient gets back is a set of *its own* sequences, carrying no zone,
    // no path, and no payload — so it can no more locate its withheld ops than count
    // anyone else's. The projected frontier is the reader's run and nothing else.
    let (_, ops) = reader_run();
    let projected = zone_projected(&ops, Some(reader()));
    let served: HashSet<OpId> = projected.seen().collect();
    let own: HashSet<OpId> = ops.iter().map(|op| op.id).collect();
    assert_eq!(served, own);
}

// --- convergence across the fix ---

#[test]
fn a_reader_restored_from_a_projected_snapshot_converges_with_the_room() {
    // The reader restarts, adopts the projected snapshot, writes, and its write
    // reaches the room; the room's fan-out of the authorized partitions then leaves
    // the two agreeing on everything the reader may read.
    let (_, ops) = reader_run();
    let projected = zone_projected(&ops, Some(reader()));
    let mut restarted = restored(&projected, 0);
    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
        tx.map(b"loose").register(b"also", Scalar::Int(8));
    });

    let mut room = room(&ops);
    for op in &fresh {
        room.apply(op);
    }
    for (container, key) in [
        (b"board".as_slice(), b"bseed".as_slice()),
        (b"board", b"b"),
        (b"board", b"after"),
        (b"loose", b"lseed"),
        (b"loose", b"l"),
        (b"loose", b"also"),
    ] {
        assert_eq!(
            nested(&room, container, key),
            nested(&restarted, container, key),
            "{container:?}/{key:?} diverged",
        );
        assert!(
            nested(&restarted, container, key).is_some(),
            "{container:?}/{key:?} missing from the reader",
        );
    }
    assert!(
        restarted.get(b"notes").is_none(),
        "the withheld partition stays withheld across the write",
    );
}

// --- what a projection must NOT carry: the projector's own reservations ---

#[test]
fn a_projection_carries_none_of_the_projectors_reservations() {
    // A reservation (C14) is an id the replica published and does not hold, and the
    // state encoding writes it as a bare **sequence** — correct only while every
    // entry belongs to the document's own client. A projection is served to another
    // replica, which reads those sequences back under *its* identity, so one that
    // rode across would reserve sequences in the recipient's space that nothing
    // published — the mirror of the frontier scrub, on the set the mint reads.
    //
    // No in-tree caller reaches this: a reservation is installed only by a client
    // session's inbound frame and every projection today runs on a server-side
    // document. Both projections are public API, though, so the rule is enforced
    // rather than assumed.
    // Asserted on the projected *bytes*, which is what a projection controls.
    // Reading them back with `decode_state_as` would prove nothing: `adopt_as`
    // clears reservations as it takes a snapshot over, so it masks this rule
    // entirely — measured, and the reason the earlier shape of this test was
    // vacuous. Served for a recipient that published nothing, so the frontier the
    // projection keeps is empty and a surviving reservation is the only thing that
    // could move the decoded replica's mint.
    let (_, ops) = reader_run();
    let mut d = room(&ops);
    d.note_published(&[0, 1, 2], 0);
    d.project_zones(&zoned(), &za_only(), Some(cid(7)));

    let served = Document::decode_state(&d.encode_state()).expect("the projection decodes");
    assert_eq!(
        served.next_seq(),
        0,
        "the projected snapshot carries the projector's reservations",
    );
}

#[test]
fn a_read_path_projection_carries_none_of_the_projectors_reservations() {
    let (_, ops) = reader_run();
    let mut d = room(&ops);
    d.note_published(&[0, 1, 2], 0);
    d.project_read_paths(reads_board, Some(cid(7)));

    let served = Document::decode_state(&d.encode_state()).expect("the projection decodes");
    assert_eq!(
        served.next_seq(),
        0,
        "the projected snapshot carries the projector's reservations",
    );
}
