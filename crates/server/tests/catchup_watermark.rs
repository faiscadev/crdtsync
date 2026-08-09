//! The catch-up watermark a zone-limited reader is told (C31).
//!
//! `Message::Snapshot` and `Message::VersionState` each carry a `seq` — the room's
//! whole-log server head. Their *content* is narrowed to the reader's partitions and
//! their causal frontier is scrubbed to the recipient's own ids, but the scalar was
//! not, so the difference between two readings counted the ops written into
//! partitions the reader is never served. ARCHITECTURE §Zones promises a hidden
//! partition leaks "not the ops, snapshot, structure, existence, or size, and cannot
//! infer activity from clock jumps".
//!
//! The channel was enumerable rather than theoretical: version *names* are a room-read
//! fact — the `Versions` reply hands them all over — and `autoVersion` schedule triggers mint
//! timestamped ones, so a reader fetches each capture in turn and charts a hidden
//! partition's write volume over time from the scalars alone.
//!
//! What a narrowed reader is told instead is the last sequence in the stream its own
//! zone scope admits — a real room sequence, so it stays a resume floor the client
//! sends back as `last_seen_seq`, `Hub::catch_up` indexes the log with, and the
//! follower-read gate compares against a node's committed watermark. The tests below
//! pin the inference closed across these two frames (within a compaction epoch, a
//! window holding only hidden writes reads like an idle one; across one the catch-up
//! scalar can fall to `0`, which is C119's residue, while the version scalar refuses
//! the field outright and so cannot move at all), the watermark still live (a *visible* write moves it, and both readings
//! are a live sequence rather than a flat zero), resume still working across a
//! reconnect, a whole-room reader unchanged, and both branch stream shapes read in the
//! branch's own sequence space.
//!
//! **What this suite does not pin.** These are the two scalar seams, not §Zones'
//! promise. The room's sequence still reaches a zone-limited reader by other routes,
//! each filed and each measured: a version *name* minted by a publish or a restore
//! (C116), `Message::Branches` reporting `main`'s head unnarrowed (C118), and the yes/no answers
//! of the read-routing gate and of `catch_up`'s snapshot-vs-delta branch, each of
//! which binary-searches a room-wide sequence (C119). A test here that claimed the
//! inference closed *in general* would be asserting something false.
//!
//! Everything runs in-process through the [`Registry`] (no socket, no fs), so the
//! suite runs under Miri.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Message, Op, OpKind, Scalar, Schema};
use crdtsync_server::acl::actor_key;
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const ROOM: &[u8] = b"room-w";
const APP: &[u8] = b"z";
const CH: Channel = Channel(0);

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) and one unzoned slot
/// (`/loose`, the root partition).
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

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn zoned_schema() -> Schema {
    Schema::parse(ZONED).expect("zoned schema parses")
}

/// Every actor may read the room; only the author writes. `za` is admitted to zone
/// za alone — `zb` stays hidden from it, which is the partition every test below
/// writes into to test the inference.
fn authorizer(id: &Identity, action: Action, res: &Resource) -> bool {
    let actor = id.actor();
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            matches!(actor, b"author" | b"full") || (actor == b"za" && zone == b"za")
        }
        _ => matches!(action, Action::Read) || actor == b"author",
    }
}

fn tokens() -> StaticTokens {
    let mut t = StaticTokens::new();
    for (cred, actor) in [
        ("c-author", "author"),
        ("c-za", "za"),
        ("c-za2", "za"),
        ("c-za3", "za"),
        ("c-full", "full"),
    ] {
        t.insert(cred.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

fn registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello (enforcing `{APP, v1}`) + Auth as `cred`, without subscribing.
fn auth(r: &mut Registry, client: u8, cred: &str) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: cred.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// Subscribe on `channel` to the `(ROOM, branch)` stream with zone selector `zone`
/// (empty is the whole room), resuming from `last_seen_seq`, returning the raw reply
/// frames.
fn subscribe_stream(
    r: &mut Registry,
    id: ConnId,
    channel: Channel,
    branch: &[u8],
    zone: &[u8],
    last_seen_seq: u64,
) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel,
            room: ROOM.to_vec(),
            branch: branch.to_vec(),
            zone: zone.to_vec(),
            last_seen_seq,
        },
    ));
    r.take_outbox(id)
}

fn subscribe_from(r: &mut Registry, id: ConnId, zone: &[u8], last_seen_seq: u64) -> Vec<Message> {
    subscribe_stream(r, id, CH, b"", zone, last_seen_seq)
}

fn subscribe(r: &mut Registry, id: ConnId, zone: &[u8]) -> Vec<Message> {
    subscribe_from(r, id, zone, 0)
}

fn write_on(r: &mut Registry, id: ConnId, channel: Channel, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel, ops }));
    r.take_outbox(id);
}

fn write(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    write_on(r, id, CH, ops);
}

/// Take a named version of the channel's room, through the wire sub-protocol.
fn create_version(r: &mut Registry, id: ConnId, name: &[u8]) {
    assert!(r.deliver(
        id,
        Message::VersionCreate {
            channel: CH,
            name: name.to_vec(),
        }
    ));
    r.take_outbox(id);
}

/// The watermark `id` is served for the named version.
fn version_seq(r: &mut Registry, id: ConnId, name: &[u8]) -> u64 {
    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel: CH,
            name: name.to_vec(),
        }
    ));
    r.take_outbox(id)
        .into_iter()
        .find_map(|m| match m {
            Message::VersionState { seq, .. } => Some(seq),
            _ => None,
        })
        .expect("the fetch replies with the version's state")
}

/// The watermark a cold join on `zone` is served, which a below-floor join always
/// takes as a snapshot.
fn cold_snapshot_seq(r: &mut Registry, client: u8, cred: &str, zone: &[u8]) -> u64 {
    let id = auth(r, client, cred);
    subscribe(r, id, zone)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { seq, .. } => Some(seq),
            _ => None,
        })
        .expect("a below-floor join is served a snapshot")
}

/// Whether `msgs` carry a `RegisterSet` of `key`.
fn has_key(msgs: &[Message], key: &[u8]) -> bool {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .flatten()
        .any(|op| matches!(&op.kind, OpKind::RegisterSet { key: k, .. } if k == key))
}

/// An author connection that has bootstrapped the room and created the three zone
/// containers, each seeded with one register.
fn seeded() -> (Registry, Document, ConnId) {
    let mut r = registry();
    let author = auth(&mut r, 1, "c-author");
    subscribe(&mut r, author, b"");
    r.take_outbox(author);

    let mut doc = Document::new(cid(1));
    doc.set_schema(zoned_schema());
    let setup = doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(1));
        tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        tx.map(b"loose").register(b"lseed", Scalar::Int(1));
    });
    write(&mut r, author, setup);
    (r, doc, author)
}

/// A content write into `/board` — a pure zoned (za) `RegisterSet`, visible to the
/// za reader.
fn board_write(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(b"board").register(key, Scalar::Int(v));
    })
}

/// A content write into `/notes` — a pure zoned (zb) `RegisterSet`, hidden from the
/// za reader.
fn notes_write(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(b"notes").register(key, Scalar::Int(v));
    })
}

/// What a za-limited reader observes across a window into which the author wrote
/// `hidden` ops of the zone it cannot read: the watermark on either side of the
/// window, and how far the room's own head moved across it.
fn across_a_window(hidden: usize) -> (u64, u64, u64) {
    let (mut r, mut doc, author) = seeded();
    // Compact so every later join falls below the floor and is served a snapshot —
    // the frame that carries the watermark under test — then leave one op this
    // reader *may* see above the floor, so both readings are a live sequence rather
    // than the empty answer, and an implementation that simply returned nothing
    // would not pass for the same reason.
    r.hub_mut().compact(ROOM).expect("compact");
    write(&mut r, author, board_write(&mut doc, b"bk", 1));

    let before = cold_snapshot_seq(&mut r, 2, "c-za", b"za");
    let head_before = r.hub().seq(ROOM);
    for i in 0..hidden {
        write(&mut r, author, notes_write(&mut doc, b"nk", i as i64));
    }
    let after = cold_snapshot_seq(&mut r, 3, "c-za2", b"za");
    (before, after, r.hub().seq(ROOM) - head_before)
}

/// The inference, closed. A window into which only a hidden partition was written is
/// indistinguishable — in the watermark the reader is handed on either side of it —
/// from a window in which nothing was written at all.
#[test]
fn a_hidden_write_window_reads_like_an_idle_one() {
    let (idle_before, idle_after, idle_moved) = across_a_window(0);
    let (busy_before, busy_after, busy_moved) = across_a_window(5);

    assert_eq!(idle_moved, 0, "the idle window holds no write");
    assert_eq!(busy_moved, 5, "the busy window holds five hidden writes");
    assert_ne!(
        busy_before, 0,
        "the reader is reading a live watermark, not the empty answer"
    );
    assert_eq!(
        (busy_before, busy_after),
        (idle_before, idle_after),
        "a za reader reads the same pair of watermarks either way"
    );
    assert_eq!(
        busy_before, busy_after,
        "the watermark does not move across a hidden write"
    );
}

/// The watermark is narrowed, not frozen: a write into the partition this reader
/// *does* read moves it, and moves it to that op's own room sequence.
#[test]
fn a_visible_write_moves_a_zone_limited_watermark() {
    let (mut r, mut doc, author) = seeded();
    r.hub_mut().compact(ROOM).expect("compact");
    let floor = r.hub().seq(ROOM);

    let before = cold_snapshot_seq(&mut r, 2, "c-za", b"za");
    assert_eq!(
        before, 0,
        "nothing this reader may see is retained above the floor"
    );

    write(&mut r, author, board_write(&mut doc, b"bk", 1));
    let after = cold_snapshot_seq(&mut r, 3, "c-za2", b"za");
    assert_eq!(
        after,
        floor + 1,
        "the watermark is the visible op's own room sequence"
    );
    assert_eq!(after, r.hub().seq(ROOM), "which is the room's head here");
}

/// A whole-room reader is not narrowed even where the narrowing would answer `0`: a
/// room compacted to an empty retained log holds nothing for `partition_head` to find,
/// which is every room between a compaction and the next write. Such a reader must
/// still be told the head — told `0` it would re-take the whole state on each
/// reconnect and its follower-read floor would go vacuous.
#[test]
fn a_whole_room_watermark_survives_an_empty_retained_log() {
    let (mut r, _doc, _author) = seeded();
    r.hub_mut().compact(ROOM).expect("compact");
    let head = r.hub().seq(ROOM);
    assert_eq!(cold_snapshot_seq(&mut r, 2, "c-author", b""), head);
}

/// A whole-room reader's watermark is the room's head, unchanged — the narrowing
/// tracks the zone scope, and a reader admitted to every zone is not narrowed.
#[test]
fn a_whole_room_watermark_is_the_rooms_head() {
    let (mut r, mut doc, author) = seeded();
    write(&mut r, author, notes_write(&mut doc, b"nk", 1));
    r.hub_mut().compact(ROOM).expect("compact");
    write(&mut r, author, notes_write(&mut doc, b"nk2", 2));

    let whole = cold_snapshot_seq(&mut r, 2, "c-author", b"");
    assert_eq!(whole, r.hub().seq(ROOM));
}

/// Resume still works for a narrowed reader: the watermark it was handed is a real
/// room sequence, so sending it back as `last_seen_seq` catches it up with an op
/// delta — carrying the write into its own partition made since, and still none of
/// the hidden one's.
#[test]
fn a_narrowed_watermark_resumes_a_reconnect() {
    let (mut r, mut doc, author) = seeded();
    r.hub_mut().compact(ROOM).expect("compact");
    write(&mut r, author, board_write(&mut doc, b"bk1", 1));

    let za = auth(&mut r, 2, "c-za");
    let resume_from = subscribe(&mut r, za, b"za")
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { seq, .. } => Some(seq),
            _ => None,
        })
        .expect("a below-floor join is served a snapshot");

    write(&mut r, author, notes_write(&mut doc, b"nk", 1));
    write(&mut r, author, board_write(&mut doc, b"bk2", 2));

    // The same reader reconnecting, resuming from the watermark it holds.
    let again = auth(&mut r, 3, "c-za2");
    let caught_up = subscribe_from(&mut r, again, b"za", resume_from);
    assert!(
        !caught_up
            .iter()
            .any(|m| matches!(m, Message::Snapshot { .. })),
        "the floor is above the compaction floor, so the resume is a delta"
    );
    assert!(
        has_key(&caught_up, b"bk2"),
        "the resume carries the write into this reader's own partition"
    );
    assert!(
        !has_key(&caught_up, b"nk"),
        "and none of the hidden partition's"
    );
    assert!(
        !has_key(&caught_up, b"bk1"),
        "nor re-sends what the watermark already covered"
    );
}

/// The watermark never goes backwards. A reader already holding a floor is handed at
/// least that floor, even where nothing it may read is retained above the compaction
/// floor — a watermark that regressed would let a lagging replica serve it a state
/// older than the one it has.
#[test]
fn a_narrowed_watermark_never_regresses_below_the_readers_floor() {
    let (mut r, mut doc, author) = seeded();
    write(&mut r, author, notes_write(&mut doc, b"nk", 1));
    r.hub_mut().compact(ROOM).expect("compact");
    let floor = r.hub().seq(ROOM);
    write(&mut r, author, notes_write(&mut doc, b"nk2", 2));

    let za = auth(&mut r, 2, "c-za");
    let served = subscribe_from(&mut r, za, b"za", floor - 1)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { seq, .. } => Some(seq),
            _ => None,
        })
        .expect("a below-floor join is served a snapshot");
    assert_eq!(
        served,
        floor - 1,
        "the reader keeps the floor it arrived on"
    );
}

/// What a za-limited reader reads off two captures taken either side of a window
/// into which the author wrote `hidden` ops of the zone it cannot read.
fn versions_across_a_window(hidden: usize) -> (u64, u64) {
    let (mut r, mut doc, author) = seeded();
    write(&mut r, author, board_write(&mut doc, b"bk", 1));
    create_version(&mut r, author, b"v1");
    for i in 0..hidden {
        write(&mut r, author, notes_write(&mut doc, b"nk", i as i64));
    }
    create_version(&mut r, author, b"v2");

    let za = auth(&mut r, 2, "c-za");
    subscribe(&mut r, za, b"za");
    (
        version_seq(&mut r, za, b"v1"),
        version_seq(&mut r, za, b"v2"),
    )
}

/// The enumerable form of the same inference: a reader walks the version names — a
/// room-read fact — and fetches each capture. Two captures spanning a hidden-only
/// window read alike, exactly as two spanning an idle one do.
#[test]
fn two_captures_spanning_a_hidden_write_carry_one_watermark() {
    let idle = versions_across_a_window(0);
    let busy = versions_across_a_window(5);

    assert_eq!(busy, idle, "a za reader charts the same pair either way");
    assert_eq!(busy.0, busy.1, "with no gap to read a write volume off");
}

/// The captures' own points do differ — the room head moved across the window — so
/// the reader is being told less than the room knows, not the same thing twice.
#[test]
fn a_whole_room_reader_reads_each_captures_true_point() {
    let (mut r, mut doc, author) = seeded();
    create_version(&mut r, author, b"v1");
    let at_v1 = r.hub().seq(ROOM);
    for i in 0..5 {
        write(&mut r, author, notes_write(&mut doc, b"nk", i));
    }
    create_version(&mut r, author, b"v2");
    let at_v2 = r.hub().seq(ROOM);

    assert_ne!(at_v1, at_v2, "the window holds five writes");
    assert_eq!(version_seq(&mut r, author, b"v1"), at_v1);
    assert_eq!(version_seq(&mut r, author, b"v2"), at_v2);
}

/// A narrowed reader is told nothing about a capture's point — the same `0` whatever
/// the room did between two captures, including a write into its *own* partition. The
/// catch-up seam answers with a live sequence because its scalar is a resume cursor;
/// this one feeds no cursor, so it can refuse outright, and refusing is what makes the
/// answer independent of the room's volume.
#[test]
fn a_narrowed_reader_is_told_nothing_of_a_captures_point() {
    let (mut r, mut doc, author) = seeded();
    create_version(&mut r, author, b"v1");
    write(&mut r, author, board_write(&mut doc, b"bk", 1));
    create_version(&mut r, author, b"v2");

    let za = auth(&mut r, 2, "c-za");
    subscribe(&mut r, za, b"za");
    assert_eq!(version_seq(&mut r, za, b"v1"), 0);
    assert_eq!(version_seq(&mut r, za, b"v2"), 0);
    // The room does know the difference, and says so to a reader of the whole room.
    assert_ne!(
        version_seq(&mut r, author, b"v1"),
        version_seq(&mut r, author, b"v2")
    );
}

/// The regression the refusal exists for: re-reading **one fixed capture** must not
/// change its answer because the room got busier somewhere this reader cannot see.
/// A sequence read out of the *retained* log would — a compaction the hidden writes
/// alone triggered drops this reader's last visible op below the floor, and the answer
/// collapses. Measured on `main` this pair is `(7, 7)`; a retained-log answer makes it
/// `(7, 0)`, which is a one-bit report that the room's total volume crossed a
/// threshold, on a seam that leaked nothing before.
#[test]
fn one_captures_answer_does_not_move_when_a_hidden_partition_gets_busy() {
    let (mut r, mut doc, author) = seeded();
    write(&mut r, author, board_write(&mut doc, b"bk", 1));
    create_version(&mut r, author, b"v1");

    let za = auth(&mut r, 2, "c-za");
    subscribe(&mut r, za, b"za");
    let before = version_seq(&mut r, za, b"v1");

    // Auto-compaction, so the floor moves because the *room* got busy rather than
    // because the test said so — the threshold counts the retained log's length, and
    // this window's writes are all in a partition this reader cannot see.
    let retained = r.hub().seq(ROOM) - r.hub().base_seq(ROOM);
    r.hub_mut().set_compaction_threshold(retained + 4);
    for i in 0..8 {
        write(&mut r, author, notes_write(&mut doc, b"nk", i));
    }
    assert!(
        r.hub().base_seq(ROOM) > before,
        "the hidden writes alone carried the floor past this reader's last visible op"
    );

    assert_eq!(
        version_seq(&mut r, za, b"v1"),
        before,
        "one capture reads the same however busy a hidden partition got"
    );
}

/// A branch is its own sequence space, and the watermark is read in it. A
/// snapshot-forked branch owns its base — its stream is that capture plus the
/// divergent tail past the fork point, never `main`'s log — so a za reader joining
/// it below the fork point is told the last sequence of *that* stream it was served.
#[test]
fn a_branch_watermark_is_read_in_the_branchs_own_sequence_space() {
    const RESTORED: &[u8] = b"restored";
    let (mut r, _doc, _author) = seeded();
    r.hub_mut().create_version(ROOM, b"v1").expect("capture v1");
    let fork_point = r.hub().seq(ROOM);
    assert!(r
        .hub_mut()
        .fork_branch_from_version(ROOM, RESTORED, b"v1")
        .expect("fork from the capture"));

    // An editor joining the branch and authoring on top of the state it is served —
    // two writes on the branch's tail, the hidden one last, so a watermark taken
    // from the stream's head rather than from what a za reader was served would land
    // on it.
    let editor = auth(&mut r, 4, "c-author");
    let base = subscribe_stream(&mut r, editor, CH, RESTORED, b"", 0)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { state, .. } => Some(state),
            _ => None,
        })
        .expect("a below-fork-point join to an owned-base branch is served a snapshot");
    let mut editing = Document::decode_state(&base).expect("the branch base decodes");
    editing.adopt_as(cid(4), 0);
    editing.set_schema(zoned_schema());
    write(&mut r, editor, board_write(&mut editing, b"bk", 1));
    write(&mut r, editor, notes_write(&mut editing, b"nk", 1));

    let za = auth(&mut r, 5, "c-za");
    let served = subscribe_stream(&mut r, za, CH, RESTORED, b"za", 0)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { seq, .. } => Some(seq),
            _ => None,
        })
        .expect("a below-fork-point join to an owned-base branch is served a snapshot");
    assert_eq!(
        served,
        fork_point + 1,
        "the branch's own first tail sequence, which is the visible write"
    );
    // A snapshot fork owns its base, so `main`'s log is not part of its stream: asked
    // below the branch's first tail sequence it names nothing, rather than reaching
    // back for a sequence that belongs to another stream — `main` holds visible ops
    // below the fork point, and they are not this branch's to report.
    assert_eq!(
        r.hub()
            .partition_head(ROOM, RESTORED, fork_point, |zone| zone != Some(1)),
        0,
        "main's log never answers for a branch that owns its base"
    );

    // A whole-room reader of the same branch still reads its head — both tail ops —
    // so what narrowed is this reader, not the stream.
    let whole = auth(&mut r, 6, "c-author");
    let head = subscribe_stream(&mut r, whole, CH, RESTORED, b"", 0)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { seq, .. } => Some(seq),
            _ => None,
        })
        .expect("a below-fork-point join to an owned-base branch is served a snapshot");
    assert_eq!(head, fork_point + 2);
}

/// A live-log fork's stream is `main`'s log up to the fork point followed by the
/// branch's own tail, and the watermark is read across both — the shared base
/// bounded at the fork point, so `main`'s later writes never answer for the branch.
/// Read at the hub seam: the wire serves this branch kind an op delta rather than a
/// snapshot, so no frame carries the scalar, while [`Hub::partition_head`] answers
/// for the stream whichever frame asks.
#[test]
fn a_live_log_forks_watermark_spans_its_base_and_its_tail() {
    const DEV: &[u8] = b"dev";
    let (mut r, mut doc, author) = seeded();
    write(&mut r, author, board_write(&mut doc, b"bk", 1));
    let fork_point = r.hub().seq(ROOM);
    let visible_in_base = fork_point;
    r.hub_mut()
        .fork_branch(ROOM, DEV, b"main", u64::MAX)
        .expect("fork at main's head");

    // `main` moves on in a partition the reader may see, past the fork point; the
    // branch's own tail takes one hidden write and then one visible one.
    write(&mut r, author, board_write(&mut doc, b"bk-main", 2));
    let hidden = doc.transact(|tx| {
        tx.map(b"notes").register(b"nk-dev", Scalar::Int(1));
    });
    let visible = doc.transact(|tx| {
        tx.map(b"board").register(b"bk-dev", Scalar::Int(1));
    });
    let hub = r.hub_mut();
    hub.ingest_branch(ROOM, DEV, hidden, Some(1)).expect("tail");
    hub.ingest_branch(ROOM, DEV, visible, Some(1))
        .expect("tail");

    let za_only = |zone: Option<u32>| zone != Some(1);
    let head = hub.seq(ROOM).max(fork_point + 2);
    assert_eq!(
        hub.partition_head(ROOM, DEV, head, za_only),
        fork_point + 2,
        "the branch's own later visible tail op"
    );
    // With the tail's visible op gone from range, the answer falls back through the
    // shared base — and stops at the fork point, never reaching main's later write.
    assert_eq!(
        hub.partition_head(ROOM, DEV, fork_point + 1, za_only),
        visible_in_base,
        "the base's last visible op, main's post-fork write excluded"
    );
    assert_eq!(
        hub.partition_head(ROOM, DEV, head, |_| false),
        0,
        "a scope admitting nothing names no sequence"
    );
    assert_eq!(
        hub.partition_head(ROOM, b"no-such-branch", head, za_only),
        0,
        "a branch the room does not hold is not answered out of another stream"
    );
}

/// The ruling is zone-scoped, and this is what that means at the wire: a reader the
/// **doc-ACL** narrows — read on the room, a deny carving a subtree out — is told the
/// room's head, exactly as before. §Wire-Level Redaction says outright that such a
/// reader still observes the room's activity; §Zones is the promise that forbids it,
/// and only the zone scope tracks that promise.
#[test]
fn a_doc_acl_narrowed_reader_is_still_told_the_rooms_head() {
    let (mut r, mut doc, author) = seeded();
    // The room's creator grants `full` read on the room and denies it `/notes`, so it
    // is a partial reader on the doc-ACL axis while admitted to every zone.
    for path in [encode_path(&[]), encode_path(&[b"notes"])] {
        let effect = if path == encode_path(&[]) {
            AclEffect::Allow
        } else {
            AclEffect::Deny
        };
        let ops = doc.transact(|tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"full")),
                AclGrant::Capability(Capability::Read),
                effect,
                path.clone(),
                actor_key(b"author"),
            );
        });
        write(&mut r, author, ops);
    }
    r.hub_mut().compact(ROOM).expect("compact");
    write(&mut r, author, notes_write(&mut doc, b"nk", 1));

    assert_eq!(
        cold_snapshot_seq(&mut r, 7, "c-full", b""),
        r.hub().seq(ROOM),
        "a doc-ACL-narrowed reader's watermark is untouched"
    );
    // The control on the other axis, over the same room and the same write.
    assert_ne!(
        cold_snapshot_seq(&mut r, 8, "c-za", b"za"),
        r.hub().seq(ROOM),
        "while the zone-limited reader's is narrowed"
    );
}

/// The refusal does not depend on where the capture sits: below the compaction floor
/// it reads `0` like any other, while a whole-room reader still reads its true point.
#[test]
fn a_capture_below_the_compaction_floor_reads_empty_for_a_narrowed_reader() {
    let (mut r, mut doc, author) = seeded();
    write(&mut r, author, board_write(&mut doc, b"bk", 1));
    create_version(&mut r, author, b"v1");
    let captured_at = r.hub().seq(ROOM);
    r.hub_mut().compact(ROOM).expect("compact");

    let za = auth(&mut r, 9, "c-za");
    subscribe(&mut r, za, b"za");
    assert_eq!(
        version_seq(&mut r, za, b"v1"),
        0,
        "no sequence this reader may see survives the floor"
    );
    assert_eq!(
        version_seq(&mut r, author, b"v1"),
        captured_at,
        "a whole-room reader still reads the capture's true point"
    );
}
