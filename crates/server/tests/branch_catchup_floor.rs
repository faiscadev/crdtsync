//! A live-log fork's catch-up against `main`'s compaction floor.
//!
//! A live-log fork owns nothing: its base is `main`'s **retained** log below its
//! fork point, and the floor only rises. Once it has passed any part of that
//! window the records the branch is made of are gone — folded into `main`'s
//! replica, where the branch's stream cannot reach them.
//!
//! The rule this file pins: a `(room, branch)` catch-up either serves a stream
//! carrying the branch's whole history, or refuses. What it must never do is
//! answer `Catchup::Ops` with the retained remainder — a fresh subscriber folds
//! that into a document missing everything below the floor, is told it is at the
//! head, and edits from there (C53).
//!
//! The refusal takes **both** ends of that window, and standing above either end
//! is enough to be served. A subscriber at or above the floor is owed only records
//! `main` still retains — the whole retained remainder of its base below the fork
//! point, its divergent tail alone above it. A subscriber at or above its own base
//! window (`fork_point` clamped to `main`'s head) is owed none of the base at all,
//! however far the floor has since run past it. So the refusal reaches only a
//! subscriber below both. `main` and a snapshot fork are untouched — the first *is*
//! the replica the log folded into, the second owns its base.
//!
//! What the position is, is what the subscriber *claims*: the cursor is a wire
//! field and nothing verifies it. The refusal covers a client catching up honestly,
//! not the branch.

use crdtsync_core::doc::Document;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Element, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{Catchup, ConnId, DiffError, Hub, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
const DRAFT: &[u8] = b"draft";
const MAIN: &[u8] = b"main";
const SERVER: u8 = 0xFF;

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// A register write of `value` under `key` from `d`.
fn reg(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
}

/// The `key` register's int value in a document, or `None` where the document does
/// not hold that key at all — which is what a stream folded over a dropped base
/// looks like.
fn int(d: &Document, key: &[u8]) -> Option<i64> {
    match d.get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            other => panic!("expected an int, got {other:?}"),
        },
        _ => None,
    }
}

/// The document a **fresh** subscriber to `(room, branch)` is caught up to, or
/// `None` where the catch-up refused to serve one.
///
/// This is the oracle every case below runs, and it is deliberately indifferent to
/// *how* the stream is served: a delta folds from the empty document exactly as a
/// subscriber does, a snapshot decodes. So a case asserting "the pre-floor content
/// is there, or the catch-up said no" holds whether the fix preserves the base or
/// refuses over it.
fn caught_up(hub: &mut Hub, room: &[u8], branch: &[u8]) -> Option<Document> {
    match hub.catch_up_branch(room, branch, 0) {
        Catchup::Ops(records) => {
            let mut d = Document::new(cid(SERVER));
            for rec in &records {
                d.apply(&rec.op);
            }
            Some(d)
        }
        Catchup::Snapshot { state, .. } => Some(Document::decode_state(&state).unwrap()),
        Catchup::Unavailable => None,
    }
}

/// A room holding `key = 1` at sequence 1 and `key2 = 2` at sequence 2, with the
/// author that wrote them — the shared history every case below forks over.
fn room_with_two_ops(hub: &mut Hub) -> Document {
    let mut author = doc(1);
    hub.ingest(ROOM, reg(&mut author, b"one", 1), None).unwrap();
    hub.ingest(ROOM, reg(&mut author, b"two", 2), None).unwrap();
    author
}

// --- the shape the entry was filed with: a fork above a floor that has risen ---

#[test]
fn a_fork_whose_shared_base_straddles_the_floor_is_not_served_short() {
    // The filing's literal shape: forked at 4 in a room compacted to 2. The
    // subscriber's base window is `(0, 4]`; `main` retains `(2, 4]`, so ops 1 and 2
    // exist in no stream this branch can read.
    let mut hub = Hub::new(cid(SERVER));
    let mut author = room_with_two_ops(&mut hub);
    hub.compact(ROOM).unwrap();
    assert_eq!(
        hub.base_seq(ROOM),
        2,
        "the floor rose over the first two ops"
    );

    hub.ingest(ROOM, reg(&mut author, b"three", 3), None)
        .unwrap();
    hub.ingest(ROOM, reg(&mut author, b"four", 4), None)
        .unwrap();
    let fork = hub.seq(ROOM);
    assert_eq!(fork, 4);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();

    match caught_up(&mut hub, ROOM, DRAFT) {
        None => {}
        Some(served) => panic!(
            "a fresh subscriber was served a stream missing its pre-floor base: \
             one={:?} two={:?} three={:?} four={:?} tail={:?}",
            int(&served, b"one"),
            int(&served, b"two"),
            int(&served, b"three"),
            int(&served, b"four"),
            int(&served, b"tail"),
        ),
    }
}

#[test]
fn a_floor_raised_to_the_fork_point_is_not_served_short() {
    // The plain shape, and the one an inline `set_compaction_threshold` reaches with
    // no operator action: the branch forks at the head and the very next compaction
    // takes the whole window.
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    assert_eq!(
        int(
            &caught_up(&mut hub, ROOM, DRAFT).expect("servable before the floor moves"),
            b"one"
        ),
        Some(1),
        "the control: the same fork serves its whole base while the log is retained"
    );

    hub.compact(ROOM).unwrap();
    assert_eq!(hub.base_seq(ROOM), fork);
    assert!(
        caught_up(&mut hub, ROOM, DRAFT).is_none(),
        "a fresh subscriber was caught up to a stream with no base at all"
    );
}

#[test]
fn a_floor_past_the_fork_point_is_not_served_from_the_tail_alone() {
    // `base_seq >= fork_point`: the base window sits wholly below the floor, so the
    // retained-log slice is empty and the branch materializes from its own tail
    // alone — the shape that reaches *nothing* of the shared history rather than a
    // suffix of it.
    let mut hub = Hub::new(cid(SERVER));
    let mut author = room_with_two_ops(&mut hub);
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    hub.ingest(ROOM, reg(&mut author, b"later", 5), None)
        .unwrap();
    hub.compact(ROOM).unwrap();
    assert!(
        hub.base_seq(ROOM) > fork,
        "the floor is past the fork point, not merely at it"
    );

    match caught_up(&mut hub, ROOM, DRAFT) {
        None => {}
        Some(served) => panic!(
            "the branch materialized from its divergent tail alone: one={:?} two={:?} tail={:?}",
            int(&served, b"one"),
            int(&served, b"two"),
            int(&served, b"tail"),
        ),
    }
    // The other end of the window, and the case that keeps the refusal from
    // collapsing to "below the floor": the floor now sits *above* the fork point, so
    // this subscriber stands below the floor and is still owed nothing of the base —
    // it is already at its own base window's end.
    assert!(
        matches!(hub.catch_up_branch(ROOM, DRAFT, fork), Catchup::Ops(records) if records.len() == 1),
        "a subscriber below the floor but at its own fork point was refused its tail"
    );
}

#[test]
fn a_fork_clipped_by_a_state_transfer_is_refused_the_same_way() {
    // The reach that needs no compaction threshold configured anywhere: an ordinary
    // below-floor follower catch-up installs a whole replica with an *empty* log at
    // a raised floor, which drops every live-log fork's base on that node. The
    // refusal belongs to the read seam, not to `compact`.
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    let state = hub.export_room(ROOM).unwrap();

    hub.install_snapshot(ROOM, &state, fork, None).unwrap();
    assert!(
        caught_up(&mut hub, ROOM, DRAFT).is_none(),
        "a fork clipped by a state transfer was still served short"
    );
}

#[test]
fn a_fork_taken_on_an_already_compacted_room_is_unservable_from_birth() {
    // The reach is not an aging one, and this is where it bites hardest: a fork
    // created *after* the floor rose is refused from the moment it exists, since its
    // shared base is the log that is already gone. The wire's own fork clamps to
    // `main`'s head, so on a room with `set_compaction_threshold` configured this is
    // what every new branch does. No seam repairs it — the branch owns no base and
    // nothing here gives it one — so this is the cost of the refusal, stated as a
    // case rather than left to be discovered (C88 owns the repair).
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    hub.compact(ROOM).unwrap();

    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, u64::MAX).unwrap());
    assert_eq!(
        hub.branch(ROOM, DRAFT).unwrap().fork_point,
        fork,
        "a fork past the head clamps to it, which is the only point the wire forks at"
    );
    assert!(
        caught_up(&mut hub, ROOM, DRAFT).is_none(),
        "a branch born over a dropped base was served as if it had one"
    );

    // And deleting it does not clear the condition — the name re-forks to the same
    // point over the same missing log.
    assert!(hub.delete_branch(ROOM, DRAFT).unwrap());
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, u64::MAX).unwrap());
    assert!(
        caught_up(&mut hub, ROOM, DRAFT).is_none(),
        "re-forking the name recovered a base the room no longer holds"
    );
}

// --- what the refusal must not swallow ---

#[test]
fn a_partly_retained_base_is_served_from_the_floor_it_survives_at() {
    // The arm where the slice arithmetic still does real work: the guard passes and
    // the window is a genuine *partial* of `main`'s log. Forked at 4 over a floor of
    // 2, a subscriber standing exactly at the floor is owed the retained remainder of
    // its base — records 3 and 4 — and one record lower is owed what is gone. This is
    // the `>` boundary itself, and the only case in this file where the served base
    // is neither whole nor absent.
    let mut hub = Hub::new(cid(SERVER));
    let mut author = room_with_two_ops(&mut hub);
    hub.compact(ROOM).unwrap();
    let three = hub
        .ingest(ROOM, reg(&mut author, b"three", 3), None)
        .unwrap();
    let four = hub
        .ingest(ROOM, reg(&mut author, b"four", 4), None)
        .unwrap();
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    let tail = hub
        .ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();

    let mut owed = three;
    owed.extend(four);
    owed.extend(tail);
    match hub.catch_up_branch(ROOM, DRAFT, 2) {
        // By identity, so the case pins which records and in what order —
        // base before tail — rather than how many of them there are.
        Catchup::Ops(records) => assert_eq!(
            records.into_iter().map(|rec| rec.op).collect::<Vec<Op>>(),
            owed,
            "a subscriber at the floor is owed its base's retained remainder plus the tail"
        ),
        _ => panic!("a subscriber standing at the floor was refused a base the room retains"),
    }
    assert!(
        matches!(hub.catch_up_branch(ROOM, DRAFT, 1), Catchup::Unavailable),
        "a subscriber one record below the floor is owed a record the room dropped"
    );
}

#[test]
fn a_fork_on_a_cloned_room_is_unservable_from_birth_with_no_compaction() {
    // The reach is not compaction's either. A whole-replica install lands an empty
    // log at a floor equal to the state's op count, so a cloned or imported room is
    // born at `floor == head` — and `clone_room` is the "duplicate this doc as a
    // template" primitive the per-user-fork workflow is built on. So every per-user
    // live-log fork of a cloned template is refused from birth, on a node that has
    // compacted nothing, and later `main` writes never rescue it: the fork point
    // stays pinned at the old head, which is the floor.
    const CLONE: &[u8] = b"room-clone";
    let mut hub = Hub::new(cid(SERVER));
    let mut author = room_with_two_ops(&mut hub);
    assert!(hub.clone_room(ROOM, CLONE).unwrap());
    assert_eq!(
        hub.base_seq(CLONE),
        2,
        "a clone's floor is the op count its state carries, which is where a wire fork clamps"
    );

    let fork = hub.seq(CLONE);
    assert!(hub.fork_branch(CLONE, DRAFT, MAIN, fork).unwrap());
    hub.ingest_branch(CLONE, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    assert!(
        caught_up(&mut hub, CLONE, DRAFT).is_none(),
        "a fork on a cloned template was served as if it shared a log the clone has none of"
    );

    hub.ingest(CLONE, reg(&mut author, b"later", 5), None)
        .unwrap();
    assert!(
        caught_up(&mut hub, CLONE, DRAFT).is_none(),
        "a later write on the clone's main rescued a fork point that sits at the floor"
    );
}

#[test]
fn a_subscriber_past_the_fork_point_still_gets_its_tail() {
    // A subscriber above the fork point holds the shared base already, so nothing it
    // needs is below the floor and its divergent tail is still its whole delta. The
    // refusal is about the base a catch-up must *serve*, not about the branch.
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    let tail = hub
        .ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    hub.compact(ROOM).unwrap();

    match hub.catch_up_branch(ROOM, DRAFT, fork) {
        Catchup::Ops(records) => assert_eq!(
            records.into_iter().map(|rec| rec.op).collect::<Vec<Op>>(),
            tail,
            "a subscriber at the fork point is owed exactly the divergent tail"
        ),
        _ => panic!("a subscriber holding the shared base was not served its tail delta"),
    }
    assert!(
        matches!(hub.catch_up_branch(ROOM, DRAFT, fork + 1), Catchup::Ops(records) if records.is_empty()),
        "a subscriber at the branch head is owed nothing"
    );
}

#[test]
fn an_uncompacted_fork_is_served_whole() {
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();

    let served = caught_up(&mut hub, ROOM, DRAFT).expect("a retained base serves");
    assert_eq!(int(&served, b"one"), Some(1));
    assert_eq!(int(&served, b"two"), Some(2));
    assert_eq!(int(&served, b"tail"), Some(9));
}

#[test]
fn main_and_a_snapshot_fork_are_untouched_by_the_floor() {
    // The two streams a raised floor does not harm, so the refusal cannot be reached
    // by widening it to "the room compacted". `main`'s replica *is* what the log
    // folded into, and a snapshot fork owns its base.
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    let fork = hub.seq(ROOM);
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    assert!(hub.fork_branch_from_version(ROOM, DRAFT, b"v1").unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    hub.compact(ROOM).unwrap();
    assert_eq!(hub.base_seq(ROOM), fork);

    let on_main = caught_up(&mut hub, ROOM, MAIN).expect("main serves its replica");
    assert_eq!(int(&on_main, b"one"), Some(1));
    assert_eq!(int(&on_main, b"two"), Some(2));

    let on_fork = caught_up(&mut hub, ROOM, DRAFT).expect("a snapshot fork owns its base");
    assert_eq!(int(&on_fork, b"one"), Some(1));
    assert_eq!(int(&on_fork, b"two"), Some(2));
    assert_eq!(int(&on_fork, b"tail"), Some(9));
}

#[test]
fn a_floor_of_zero_and_a_fork_at_zero_are_never_refused() {
    // A floor of 0 has passed nothing, and a fork at sequence 0 needs no base — so
    // neither is reachable by the refusal however far `main` runs on.
    let mut hub = Hub::new(cid(SERVER));
    room_with_two_ops(&mut hub);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, 0).unwrap());
    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    assert!(
        caught_up(&mut hub, ROOM, DRAFT).is_some(),
        "an uncompacted room refuses nothing"
    );

    hub.compact(ROOM).unwrap();
    let served = caught_up(&mut hub, ROOM, DRAFT).expect("a fork at zero owes no shared base");
    assert_eq!(int(&served, b"tail"), Some(9));
    assert_eq!(
        int(&served, b"one"),
        None,
        "a fork at zero shares no history, which is its stream and not a loss"
    );
}

#[test]
fn the_serve_seam_and_the_diff_seam_agree_on_which_forks_are_readable() {
    // The property the refusal is shaped to hold, measured across the grid rather
    // than argued from the two conditions looking alike: for a fold from sequence 0,
    // "this catch-up cannot serve the stream" and "this branch has no state to diff"
    // are the same fact. Drift between them is what put a redaction index and a
    // served stream out of step to begin with (C60).
    //
    // It reaches one direction, and only that one. `stream_doc` decides the clipped
    // case on its own check and answers before it folds, so a catch-up that serves
    // where the diff refuses is caught here. The other way round it cannot be: past
    // that check the diff seam *is* this catch-up (`fold_stream` calls it), so an
    // over-refusal propagates into both answers and they agree while both are wrong.
    // That direction is the explicit controls' — the fork at zero, the uncompacted
    // fork, the partly-retained base, and the two subscribers past the fork point.
    // `pre` ops, then optionally a compaction, then `post` ops, then a fork at every
    // sequence from 0 to one past the head — so the grid covers a fork below the
    // floor, at it, between it and the head, at the head, and above it (which
    // `fork_branch` clamps to the source's head) — and finally a compaction where
    // one has not already happened.
    let mut disagreements = Vec::new();
    for pre in 0..3u64 {
        for post in 0..3u64 {
            for fork in 0..=(pre + post + 1) {
                for compact_first in [false, true] {
                    let mut hub = Hub::new(cid(SERVER));
                    let mut author = doc(1);
                    for i in 0..pre {
                        hub.ingest(ROOM, reg(&mut author, b"pre", i as i64), None)
                            .unwrap();
                    }
                    if compact_first {
                        hub.compact(ROOM).unwrap();
                    }
                    for i in 0..post {
                        hub.ingest(ROOM, reg(&mut author, b"post", i as i64), None)
                            .unwrap();
                    }
                    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
                    hub.ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
                        .unwrap();
                    if !compact_first {
                        hub.compact(ROOM).unwrap();
                    }

                    let refused =
                        matches!(hub.catch_up_branch(ROOM, DRAFT, 0), Catchup::Unavailable);
                    let unreadable = matches!(
                        hub.diff_branches(ROOM, MAIN, DRAFT, |s| s),
                        Err(DiffError::UnreadableBranch(_))
                    );
                    if refused != unreadable {
                        disagreements.push(format!(
                            "pre={pre} post={post} fork={fork} compact_first={compact_first}: \
                             catch-up refused={refused}, diff unreadable={unreadable}"
                        ));
                    }
                }
            }
        }
    }
    assert!(
        disagreements.is_empty(),
        "the serve seam and the diff seam describe different branches:\n{}",
        disagreements.join("\n")
    );
}

// --- the same refusal at the wire ---

const CH: Channel = Channel(0);

fn registry() -> Registry {
    let mut r = Registry::new(cid(SERVER));
    r.set_clock(std::sync::Arc::new(crdtsync_server::ManualClock::new(0)));
    r
}

fn auth(r: &mut Registry, id: ConnId, client: u8) {
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
}

fn subscribe(r: &mut Registry, client: u8, branch: &[u8], last_seen_seq: u64) -> Vec<Message> {
    let id = r.connect();
    auth(r, id, client);
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: branch.to_vec(),
            zone: Vec::new(),
            last_seen_seq,
        }
    ));
    r.take_outbox(id)
}

/// A room with two ops and a `draft` forked at its head, compacted so the fork's
/// shared base is gone. No doc-ACL tuples, so nothing here rides the redaction
/// seam's own refusal — this is the plain stream.
fn clipped_at_the_wire() -> Registry {
    let mut r = registry();
    room_with_two_ops(r.hub_mut());
    let fork = r.hub().seq(ROOM);
    assert!(r.hub_mut().fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    r.hub_mut()
        .ingest_branch(ROOM, DRAFT, reg(&mut doc(2), b"tail", 9), None)
        .unwrap();
    r.hub_mut().compact(ROOM).unwrap();
    assert!(
        r.hub().acl_records(ROOM).is_empty(),
        "a tuple here would route the subscribe to C60's own refusal, which answers with the identical frame"
    );
    r
}

#[test]
fn a_fresh_subscriber_to_a_clipped_branch_is_told_rather_than_caught_up() {
    let mut r = clipped_at_the_wire();
    match subscribe(&mut r, 3, DRAFT, 0).as_slice() {
        [Message::Error { code, .. }] => assert_eq!(
            *code,
            ErrorCode::Internal,
            "a branch whose base this node cannot serve is a fault, not an absence"
        ),
        other => panic!("a fresh subscriber was caught up to a clipped branch: {other:?}"),
    }
}

#[test]
fn a_subscriber_claiming_the_fork_point_is_served_its_tail_at_the_wire() {
    // The bound is what the subscriber *says* it has seen. Nothing verifies the
    // claim — this connection is a fresh one asserting the number — so the refusal
    // covers a client catching up honestly and not a client at all.
    let mut r = clipped_at_the_wire();
    let fork = r.hub().branch(ROOM, DRAFT).unwrap().fork_point;
    match subscribe(&mut r, 3, DRAFT, fork).as_slice() {
        [Message::Ops { ops, .. }] => assert_eq!(ops.len(), 1, "the divergent tail"),
        other => panic!("a subscriber holding the base was refused its tail: {other:?}"),
    }
}

#[test]
fn a_clipped_branch_does_not_refuse_the_room_s_main_stream() {
    let mut r = clipped_at_the_wire();
    match subscribe(&mut r, 3, MAIN, 0).as_slice() {
        [Message::Snapshot { state, .. }] => {
            let served = Document::decode_state(state).unwrap();
            assert_eq!(int(&served, b"one"), Some(1));
            assert_eq!(int(&served, b"two"), Some(2));
        }
        other => panic!("main was not served its compacted replica: {other:?}"),
    }
}
