//! Client session — one replica identity per channel.
//!
//! A session holds a replica per subscribed [`Channel`], and two of its channels
//! can be bound to the *same* room: a whole-room subscription beside a
//! zone-scoped one, two zones of one room, a branch alongside the default. Those
//! replicas are independent authors — each mints op ids from its own counter and
//! stamps from its own lamport clock — so they cannot share an identity. Under a
//! shared one they mint identical `OpId`s and identical [`Stamp`]s for unrelated
//! edits, and the first consumer to see them — a replica's `OpId` dedup set —
//! drops one channel's op as an already-applied duplicate of the other's.
//!
//! Repairing the *seq* alone would not have been enough, and the counterfactual
//! is pinned below: with the identity still shared, two channels' inserts carry
//! one stamp, and a sequence node's id is its stamp.
//!
//! So each channel authors under [`ClientId::for_channel`] of the session's id:
//! derived, deterministic (the server re-derives it from the Hello id and the
//! channel an op batch names), and stable across a reconnect and a snapshot
//! catch-up.
//!
//! The identity a session is *given* still names one replica: handing the same
//! `ClientId` to two separately-constructed replicas — two sessions, or a session
//! beside a bare [`Document`] — collides under any derivation scheme, and is the
//! embedder's contract to keep.

use crdtsync_core::client::ClientSession;
use crdtsync_core::op::TxId;
use crdtsync_core::{Channel, ClientId, Document, Element, Message, Op, Scalar};

mod common;
use common::cid;

const ROOM: &[u8] = b"room-a";

fn ops_of(m: Message) -> Vec<Op> {
    match m {
        Message::Ops { ops, .. } => ops,
        other => panic!("expected Ops, got {other:?}"),
    }
}

/// A peer subscribed to the whole room, folding in what both channels author.
fn peer() -> Document {
    Document::new(cid(0xFF))
}

fn int(doc: &Document, key: &[u8]) -> i64 {
    match doc.get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register at {key:?}"),
    }
}

fn counter(doc: &Document, key: &[u8]) -> i64 {
    match doc.get(key) {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a Counter at {key:?}"),
    }
}

fn list_len(doc: &Document, key: &[u8]) -> usize {
    match doc.get(key) {
        Some(Element::List(l)) => l.borrow().len(),
        _ => panic!("expected a List at {key:?}"),
    }
}

/// Nodes the sequence actually stores, live or tombstoned — what tells "the two
/// inserts became one node" apart from "one of two nodes is hidden".
fn list_stored(doc: &Document, key: &[u8]) -> usize {
    match doc.get(key) {
        Some(Element::List(l)) => l.borrow().stored_records(),
        _ => panic!("expected a List at {key:?}"),
    }
}

/// A session holding two channels on `ROOM` — the shape this file is about. The
/// subscription flavour does not bear on identity (every constructor routes
/// through one `subscribe_inner`, and the room/branch/zone selectors ride the
/// Subscribe frame, never an op), so the flavours are covered once by
/// [`every_subscribe_flavour_takes_a_fresh_identity`] and the behaviour below
/// uses a single pair.
fn two_channels() -> (ClientSession, Channel, Channel) {
    let mut session = ClientSession::new(cid(1));
    let a = session.subscribe(ROOM).0;
    let b = session.subscribe_zone(ROOM, b"z").0;
    (session, a, b)
}

// --- the identity each channel authors under ---

#[test]
fn each_channel_authors_under_its_derived_identity() {
    let (session, a, b) = two_channels();

    assert_eq!(session.client(), cid(1), "Hello keeps the session's own id");
    assert_eq!(session.channel_client(a), Some(cid(1).for_channel(a.0)));
    assert_eq!(session.channel_client(b), Some(cid(1).for_channel(b.0)));
    assert_eq!(
        session.document(a).unwrap().client(),
        cid(1).for_channel(a.0)
    );
    assert_eq!(
        session.document(b).unwrap().client(),
        cid(1).for_channel(b.0)
    );
    assert_ne!(session.channel_client(a), session.channel_client(b));
}

/// Every way to open a subscription — the whole room, a named branch, a named
/// zone — takes the next channel and with it a fresh identity, covering the three
/// same-room pairings that make this unit necessary.
#[test]
fn every_subscribe_flavour_takes_a_fresh_identity() {
    let mut session = ClientSession::new(cid(1));
    let whole = session.subscribe(ROOM).0;
    let branch = session.subscribe_branch(ROOM, b"feature").0;
    let zone_a = session.subscribe_zone(ROOM, b"z1").0;
    let zone_b = session.subscribe_zone(ROOM, b"z2").0;

    let channels = [whole, branch, zone_a, zone_b];
    for (i, x) in channels.iter().enumerate() {
        assert_eq!(session.channel_client(*x), Some(cid(1).for_channel(x.0)));
        for y in &channels[i + 1..] {
            assert_ne!(
                session.channel_client(*x),
                session.channel_client(*y),
                "channels {x:?} and {y:?} share an identity"
            );
        }
    }
}

#[test]
fn an_unheld_channel_names_no_identity() {
    let session = ClientSession::new(cid(1));
    assert_eq!(session.channel_client(Channel(3)), None);
}

// --- op ids and stamps ---

#[test]
fn two_channels_on_one_room_mint_disjoint_op_ids() {
    let (mut session, a, b) = two_channels();
    // Both start from a fresh replica, so both mint seq 0 — only the client half
    // of the id can keep them apart.
    let from_a = ops_of(
        session
            .edit(a, |tx| tx.register(b"a", Scalar::Int(1)))
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .edit(b, |tx| tx.register(b"b", Scalar::Int(2)))
            .unwrap(),
    );
    assert_eq!(from_a[0].id.seq, from_b[0].id.seq, "both mint seq 0");
    assert_ne!(from_a[0].id, from_b[0].id);
}

#[test]
fn two_channels_on_one_room_mint_disjoint_stamps() {
    let (mut session, a, b) = two_channels();
    let from_a = ops_of(
        session
            .edit(a, |tx| tx.register(b"a", Scalar::Int(1)))
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .edit(b, |tx| tx.register(b"b", Scalar::Int(2)))
            .unwrap(),
    );
    assert_eq!(
        from_a[0].stamp.lamport, from_b[0].stamp.lamport,
        "both clocks start at the same lamport"
    );
    assert_ne!(from_a[0].stamp, from_b[0].stamp);
}

// --- what a whole-room subscriber ends up with ---

#[test]
fn a_whole_room_subscriber_keeps_both_channels_edits() {
    let (mut session, a, b) = two_channels();
    let from_a = ops_of(
        session
            .edit(a, |tx| tx.register(b"a", Scalar::Int(1)))
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .edit(b, |tx| tx.register(b"b", Scalar::Int(2)))
            .unwrap(),
    );

    let mut peer = peer();
    for op in from_a.iter().chain(from_b.iter()) {
        peer.apply(op);
    }
    assert_eq!(int(&peer, b"a"), 1, "lost the first channel's edit");
    assert_eq!(int(&peer, b"b"), 2, "lost the second channel's edit");
}

#[test]
fn two_channels_inserting_into_one_list_keep_both_items() {
    let (mut session, a, b) = two_channels();
    let from_a = ops_of(
        session
            .edit(a, |tx| tx.list(b"xs").insert(0, Scalar::Int(1)))
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .edit(b, |tx| tx.list(b"xs").insert(0, Scalar::Int(2)))
            .unwrap(),
    );

    let mut peer = peer();
    for op in from_a.iter().chain(from_b.iter()) {
        peer.apply(op);
    }
    assert_eq!(list_len(&peer, b"xs"), 2);
}

#[test]
fn two_channels_incrementing_one_counter_both_count() {
    let (mut session, a, b) = two_channels();
    let from_a = ops_of(session.edit(a, |tx| tx.inc(b"hits", 3)).unwrap());
    let from_b = ops_of(session.edit(b, |tx| tx.inc(b"hits", 4)).unwrap());

    let mut peer = peer();
    for op in from_a.iter().chain(from_b.iter()) {
        peer.apply(op);
    }
    assert_eq!(counter(&peer, b"hits"), 7);
}

// --- why the identity had to change, not just the seq ---

/// The counterfactual behind the design decision. Give the two ops *distinct* op
/// ids — what a channel-scoped seq in `OpId` would have produced — but leave the
/// replica identity shared, so the stamps still collide. Dedup no longer drops
/// anything, and the loss moves one layer down: a sequence node's id is its
/// stamp, so the two inserts become one node and an item vanishes anyway.
#[test]
fn distinct_op_ids_alone_do_not_save_two_inserts_that_share_a_stamp() {
    // Two replicas under one identity, as the defect had them. The list is
    // created elsewhere and folded into both, so neither spends a seq on it and
    // both stand at seq 0 and the same lamport — exactly the position two
    // freshly-subscribed channels of one session are in.
    let mut origin = Document::new(cid(0xEE));
    let create = origin.transact(|tx| {
        tx.list(b"xs");
    });
    let mut left = Document::new(cid(1));
    let mut right = Document::new(cid(1));
    for op in &create {
        left.apply(op);
        right.apply(op);
    }
    assert_eq!(left.next_seq(), 0);
    assert_eq!(right.next_seq(), 0);

    // `tx.list` re-emits the container create, so each batch is [create, insert]
    // and the insert — the op whose node id is at stake — is [1].
    let from_left = left.transact(|tx| tx.list(b"xs").insert(0, Scalar::Int(1)));
    let mut from_right = right.transact(|tx| tx.list(b"xs").insert(0, Scalar::Int(2)));
    assert_eq!(from_left.len(), 2);
    assert_eq!(from_right.len(), 2);
    assert_eq!(
        from_left[1].id, from_right[1].id,
        "one identity at one seq is one op id"
    );
    assert_eq!(
        from_left[1].stamp, from_right[1].stamp,
        "one identity at one lamport is one stamp"
    );

    // Repair the envelope across the whole batch, as a channel-scoped seq would:
    // these assignments are the entirety of the hypothetical fix.
    for (i, op) in from_right.iter_mut().enumerate() {
        op.id.seq = 90 + i as u64;
    }
    assert_ne!(from_left[1].id, from_right[1].id);

    let mut peer = peer();
    for op in create.iter().chain(&from_left) {
        peer.apply(op);
    }
    // Every one of the right-hand ops is genuinely folded in — nothing is dropped
    // as a duplicate. Without this the conclusion below could not tell the stamp
    // collision apart from the `OpId` dedup the repair was meant to remove.
    assert!(
        from_right.iter().all(|op| peer.apply(op)),
        "an op was dedup-dropped, so this no longer isolates the stamp collision"
    );
    assert_eq!(
        list_len(&peer, b"xs"),
        1,
        "a shared stamp collapses the two inserts onto one node — which is why \
         the replica identity, not the seq space, is what had to change"
    );
    assert_eq!(
        list_stored(&peer, b"xs"),
        1,
        "one node stored, not two with one hidden"
    );
}

// --- atomic transaction groups ---

#[test]
fn atomic_groups_on_two_channels_land_in_distinct_buckets() {
    let (mut session, a, b) = two_channels();
    let from_a = ops_of(
        session
            .atomic_edit(a, |tx| {
                tx.register(b"a1", Scalar::Int(1));
                tx.register(b"a2", Scalar::Int(1));
            })
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .atomic_edit(b, |tx| {
                tx.register(b"b1", Scalar::Int(2));
                tx.register(b"b2", Scalar::Int(2));
            })
            .unwrap(),
    );

    // The group id is derived from its members' sequences, so two channels that
    // both start at seq 0 name the same TxId — only the author client keeps the
    // receiver's `(client, tx id)` buckets apart.
    let group = |ops: &[Op]| -> (ClientId, TxId) {
        let tx = ops[0].tx.expect("an atomic member carries its group");
        assert!(
            ops.iter().all(|op| op.tx.map(|t| t.id) == Some(tx.id)),
            "a group's members disagree on their id"
        );
        (ops[0].id.client, tx.id)
    };
    let (client_a, tx_a) = group(&from_a);
    let (client_b, tx_b) = group(&from_b);
    assert_eq!(tx_a, tx_b, "both groups mint TxId(0)");
    assert_ne!((client_a, tx_a), (client_b, tx_b), "both share one bucket");
}

#[test]
fn interleaved_atomic_groups_from_two_channels_both_commit() {
    let (mut session, a, b) = two_channels();
    let from_a = ops_of(
        session
            .atomic_edit(a, |tx| {
                tx.register(b"a1", Scalar::Int(1));
                tx.register(b"a2", Scalar::Int(1));
            })
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .atomic_edit(b, |tx| {
                tx.register(b"b1", Scalar::Int(2));
                tx.register(b"b2", Scalar::Int(2));
            })
            .unwrap(),
    );

    // Interleaved arrival is what a shared bucket cannot survive: the size gate
    // sees two members, commits a mixed set, and strands the remainder for good.
    let mut peer = peer();
    peer.apply(&from_a[0]);
    peer.apply(&from_b[0]);
    peer.apply(&from_a[1]);
    peer.apply(&from_b[1]);

    assert_eq!(int(&peer, b"a1"), 1);
    assert_eq!(int(&peer, b"a2"), 1);
    assert_eq!(int(&peer, b"b1"), 2);
    assert_eq!(int(&peer, b"b2"), 2);
}

/// The build-a-group-across-several-calls route, which reaches the outbox
/// separately from `atomic_edit`.
#[test]
fn begin_commit_groups_on_two_channels_land_in_distinct_buckets() {
    let (mut session, a, b) = two_channels();
    session.begin_atomic(a).unwrap();
    session.edit(a, |tx| tx.register(b"a1", Scalar::Int(1)));
    session.edit(a, |tx| tx.register(b"a2", Scalar::Int(1)));
    let from_a = ops_of(session.commit_atomic(a).unwrap());

    session.begin_atomic(b).unwrap();
    session.edit(b, |tx| tx.register(b"b1", Scalar::Int(2)));
    session.edit(b, |tx| tx.register(b"b2", Scalar::Int(2)));
    let from_b = ops_of(session.commit_atomic(b).unwrap());

    let key = |ops: &[Op]| (ops[0].id.client, ops[0].tx.expect("tagged").id);
    assert_ne!(key(&from_a), key(&from_b));

    let mut peer = peer();
    peer.apply(&from_a[0]);
    peer.apply(&from_b[0]);
    peer.apply(&from_a[1]);
    peer.apply(&from_b[1]);
    assert_eq!(int(&peer, b"a2"), 1);
    assert_eq!(int(&peer, b"b2"), 2);
}

// --- convergence across the two channels ---

#[test]
fn peers_converge_on_both_channels_work_whatever_the_order() {
    let (mut session, a, b) = two_channels();

    let mut all = Vec::new();
    for round in 0..4i64 {
        // `contended` is written by both channels, so the two authors' stamps
        // decide the slot — an LWW race a shared identity could not resolve.
        all.extend(ops_of(
            session
                .edit(a, |tx| {
                    tx.register(b"contended", Scalar::Int(round));
                    tx.inc(b"hits", 1);
                    tx.list(b"xs").insert(0, Scalar::Int(round));
                })
                .unwrap(),
        ));
        all.extend(ops_of(
            session
                .edit(b, |tx| {
                    tx.register(b"contended", Scalar::Int(100 + round));
                    tx.inc(b"hits", 1);
                    tx.list(b"xs").insert(0, Scalar::Int(100 + round));
                })
                .unwrap(),
        ));
    }

    // Three independent peers, each fed a different order, must agree byte for
    // byte.
    let fold = |ops: &[&Op]| {
        let mut doc = peer();
        for op in ops {
            doc.apply(op);
        }
        doc
    };
    let forward: Vec<&Op> = all.iter().collect();
    let reverse: Vec<&Op> = all.iter().rev().collect();
    let interleaved: Vec<&Op> = all
        .iter()
        .step_by(2)
        .chain(all.iter().skip(1).step_by(2))
        .collect();

    let first = fold(&forward);
    let expected = first.encode_state();
    assert_eq!(fold(&reverse).encode_state(), expected, "reverse diverges");
    assert_eq!(
        fold(&interleaved).encode_state(),
        expected,
        "interleaved diverges"
    );

    // Converging on a state that dropped half the writes would still converge, so
    // pin the totals: 4 rounds x 2 channels of increments and inserts.
    assert_eq!(counter(&first, b"hits"), 8);
    assert_eq!(list_len(&first, b"xs"), 8);
}

// --- the other authoring routes ---

#[test]
fn edits_through_the_replica_handle_carry_the_channels_identity() {
    let (mut session, a, b) = two_channels();

    // The path-façade route: author on the channel's replica directly, then hand
    // the ops back through `enqueue_ops`. It has to mint under the same identity
    // as `edit`, since the server binds an op to the channel its batch names.
    let from_a = session
        .document_mut(a)
        .unwrap()
        .transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let from_b = session
        .document_mut(b)
        .unwrap()
        .transact(|tx| tx.register(b"b", Scalar::Int(2)));

    assert_eq!(from_a[0].id.client, cid(1).for_channel(a.0));
    assert_eq!(from_b[0].id.client, cid(1).for_channel(b.0));
    assert_ne!(from_a[0].id, from_b[0].id);
}

#[test]
fn a_resent_batch_still_carries_its_channels_identity() {
    let (mut session, a, b) = two_channels();
    session.edit(a, |tx| tx.register(b"a", Scalar::Int(1)));
    session.edit(b, |tx| tx.register(b"b", Scalar::Int(2)));

    // A reconnect resumes each channel by its number and replays its outbox, so
    // the replayed ops must still carry the identity that channel authors under
    // — the server re-derives it from the Hello id and the channel.
    let replayed_a = ops_of(session.resend(a).expect("channel a has an outstanding op"));
    let replayed_b = ops_of(session.resend(b).expect("channel b has an outstanding op"));
    assert_eq!(replayed_a[0].id.client, cid(1).for_channel(a.0));
    assert_eq!(replayed_b[0].id.client, cid(1).for_channel(b.0));
    assert_ne!(replayed_a[0].id, replayed_b[0].id);
}

/// A pre-existing invariant this unit leans on rather than one it introduces:
/// the outbox and its `Accepted` prune are keyed by channel, never by author. Two
/// channels minting from seq 0 would otherwise make an ack frontier ambiguous
/// across the session, so pin it here beside the identity split that relies on
/// it. (Unchanged by this unit — it passes on `main` too.)
#[test]
fn an_ack_on_one_channel_leaves_the_others_outbox_queued() {
    let (mut session, a, b) = two_channels();
    session.edit(a, |tx| tx.register(b"a", Scalar::Int(1)));
    session.edit(b, |tx| tx.register(b"b", Scalar::Int(2)));
    assert_eq!(session.outbox_len(a), 1);
    assert_eq!(session.outbox_len(b), 1);

    session
        .receive(Message::Accepted {
            channel: a,
            through: 0,
        })
        .unwrap();
    assert_eq!(session.outbox_len(a), 0);
    assert_eq!(
        session.outbox_len(b),
        1,
        "the ack crossed to another channel"
    );
}

// --- the identity outlives a catch-up ---

#[test]
fn a_channels_identity_survives_a_snapshot_catch_up() {
    let mut server = Document::new(cid(0xEE));
    server.transact(|tx| tx.register(b"seed", Scalar::Int(0)));

    let (mut session, a, b) = two_channels();
    session
        .receive(Message::Snapshot {
            channel: b,
            seq: 1,
            state: server.encode_state(),
        })
        .unwrap();
    assert_eq!(
        session.channel_client(b),
        Some(cid(1).for_channel(b.0)),
        "adopting a snapshot took an identity other than the channel's"
    );

    // And the channels still author disjointly afterwards. The catch-up lifted
    // this channel's seq high-water past the other's, so compare the client half
    // — the part that has to keep the two apart at any seq.
    let from_a = ops_of(
        session
            .edit(a, |tx| tx.register(b"a", Scalar::Int(1)))
            .unwrap(),
    );
    let from_b = ops_of(
        session
            .edit(b, |tx| tx.register(b"b", Scalar::Int(2)))
            .unwrap(),
    );
    assert_ne!(from_a[0].id.client, from_b[0].id.client);
    assert_ne!(from_a[0].stamp.client, from_b[0].stamp.client);
}

/// A channel number is never recycled, so a fresh subscription — which mints from
/// seq 0 — can never take the identity a retired channel's in-flight ops still
/// carry.
#[test]
fn a_resubscribed_channel_authors_under_a_fresh_identity() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let retired = ops_of(
        session
            .edit(a, |tx| tx.register(b"a", Scalar::Int(1)))
            .unwrap(),
    );
    session.unsubscribe(a).expect("the channel is held");

    let (b, _) = session.subscribe(ROOM);
    assert_ne!(a, b, "a freed channel number is not reused");
    let fresh = ops_of(
        session
            .edit(b, |tx| tx.register(b"b", Scalar::Int(2)))
            .unwrap(),
    );

    assert_eq!(fresh[0].id.seq, 0, "the fresh replica mints from zero");
    assert_eq!(retired[0].id.seq, fresh[0].id.seq);
    assert_ne!(
        retired[0].id, fresh[0].id,
        "the fresh subscription re-minted seq 0 under the retired identity"
    );
    assert_ne!(retired[0].stamp, fresh[0].stamp);
}
