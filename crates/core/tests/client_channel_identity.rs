//! Client session — one replica identity per channel.
//!
//! A session holds a replica per subscribed [`Channel`], and two of its channels
//! can be bound to the *same* room: a whole-room subscription beside a
//! zone-scoped one, two zones of one room, a branch alongside the default. Those
//! replicas are independent authors — each mints op ids from its own counter and
//! stamps from its own lamport clock — so they cannot share an identity. Under a
//! shared one they mint identical `OpId`s and identical [`Stamp`]s for unrelated
//! edits, and every consumer keyed on those (a peer's dedup set, the server's
//! per-room log, the `(client, TxId)` atomic-group bucket, a counter's per-client
//! tally) folds one channel's work into the other's and drops it.
//!
//! So each channel authors under [`ClientId::for_channel`] of the session's id:
//! derived, deterministic (the server re-derives it from the Hello id and the
//! channel an op batch names), and stable across a reconnect and a snapshot
//! catch-up.

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

/// Two channels of one session, both bound to `ROOM`, paired the three ways a
/// session can double up on one room.
fn same_room_pairs() -> Vec<(&'static str, ClientSession, Channel, Channel)> {
    let mut whole_and_zone = ClientSession::new(cid(1));
    let a = whole_and_zone.subscribe(ROOM).0;
    let b = whole_and_zone.subscribe_zone(ROOM, b"z").0;

    let mut two_zones = ClientSession::new(cid(1));
    let c = two_zones.subscribe_zone(ROOM, b"z1").0;
    let d = two_zones.subscribe_zone(ROOM, b"z2").0;

    let mut whole_and_branch = ClientSession::new(cid(1));
    let e = whole_and_branch.subscribe(ROOM).0;
    let f = whole_and_branch.subscribe_branch(ROOM, b"").0;

    vec![
        ("subscribe + subscribe_zone", whole_and_zone, a, b),
        ("two subscribe_zones", two_zones, c, d),
        ("subscribe + subscribe_branch", whole_and_branch, e, f),
    ]
}

// --- the identity each channel authors under ---

#[test]
fn each_channel_authors_under_its_derived_identity() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let (b, _) = session.subscribe_zone(ROOM, b"z");

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
}

#[test]
fn an_unheld_channel_names_no_identity() {
    let session = ClientSession::new(cid(1));
    assert_eq!(session.channel_client(Channel(3)), None);
}

#[test]
fn two_channels_on_one_room_hold_distinct_identities() {
    for (label, session, a, b) in same_room_pairs() {
        assert_ne!(
            session.channel_client(a),
            session.channel_client(b),
            "{label}: both channels author under one identity"
        );
    }
}

// --- op ids ---

#[test]
fn two_channels_on_one_room_mint_disjoint_op_ids() {
    for (label, mut session, a, b) in same_room_pairs() {
        // Both start from a fresh replica, so both mint seq 0 — only the client
        // half of the id can keep them apart.
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
            from_a[0].id.seq, from_b[0].id.seq,
            "{label}: both mint seq 0"
        );
        for x in &from_a {
            for y in &from_b {
                assert_ne!(x.id, y.id, "{label}: colliding op id");
            }
        }
    }
}

#[test]
fn two_channels_on_one_room_mint_disjoint_stamps() {
    for (label, mut session, a, b) in same_room_pairs() {
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
            "{label}: both clocks start at the same lamport"
        );
        for x in &from_a {
            for y in &from_b {
                assert_ne!(x.stamp, y.stamp, "{label}: colliding stamp");
            }
        }
    }
}

// --- what a whole-room subscriber ends up with ---

#[test]
fn a_whole_room_subscriber_keeps_both_channels_edits() {
    for (label, mut session, a, b) in same_room_pairs() {
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
        assert_eq!(
            int(&peer, b"a"),
            1,
            "{label}: lost the first channel's edit"
        );
        assert_eq!(
            int(&peer, b"b"),
            2,
            "{label}: lost the second channel's edit"
        );
    }
}

#[test]
fn two_channels_inserting_into_one_list_keep_both_items() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let (b, _) = session.subscribe_zone(ROOM, b"z");

    // Both channels create the list at the same key, so the two inserts land in
    // the same sequence at a whole-room subscriber — a shared stamp would give
    // the two nodes one id.
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
    assert_eq!(
        list_len(&peer, b"xs"),
        2,
        "one insert collapsed onto the other"
    );
}

#[test]
fn two_channels_incrementing_one_counter_both_count() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let (b, _) = session.subscribe_zone(ROOM, b"z");

    // A PN-counter tallies per authoring client and merges by per-client max, so
    // two channels sharing an identity merge into one tally instead of summing.
    let from_a = ops_of(session.edit(a, |tx| tx.inc(b"hits", 3)).unwrap());
    let from_b = ops_of(session.edit(b, |tx| tx.inc(b"hits", 4)).unwrap());

    let mut peer = peer();
    for op in from_a.iter().chain(from_b.iter()) {
        peer.apply(op);
    }
    assert_eq!(counter(&peer, b"hits"), 7);
}

// --- atomic transaction groups ---

#[test]
fn atomic_groups_on_two_channels_land_in_distinct_buckets() {
    for (label, mut session, a, b) in same_room_pairs() {
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

        // The group id is the group's lowest member seq, so two channels that
        // both start at seq 0 name the same TxId — only the author client keeps
        // the receiver's `(client, tx id)` buckets apart.
        let group = |ops: &[Op]| -> (ClientId, TxId) {
            let tx = ops[0].tx.expect("an atomic member carries its group");
            assert!(
                ops.iter().all(|op| op.tx.map(|t| t.id) == Some(tx.id)),
                "{label}: a group's members disagree on their id"
            );
            (ops[0].id.client, tx.id)
        };
        let (client_a, tx_a) = group(&from_a);
        let (client_b, tx_b) = group(&from_b);
        assert_eq!(tx_a, tx_b, "{label}: both groups mint TxId(0)");
        assert_ne!(
            (client_a, tx_a),
            (client_b, tx_b),
            "{label}: both groups share one bucket"
        );
    }
}

#[test]
fn interleaved_atomic_groups_from_two_channels_both_commit() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let (b, _) = session.subscribe_zone(ROOM, b"z");

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

// --- convergence across the two channels ---

#[test]
fn two_channels_converge_at_every_peer_whatever_the_order() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let (b, _) = session.subscribe_zone(ROOM, b"z");

    let mut all = Vec::new();
    for round in 0..4u8 {
        all.extend(ops_of(
            session
                .edit(a, |tx| {
                    tx.register(b"shared", Scalar::Int(round as i64));
                    tx.inc(b"hits", 1);
                    tx.list(b"xs").insert(0, Scalar::Int(round as i64));
                })
                .unwrap(),
        ));
        all.extend(ops_of(
            session
                .edit(b, |tx| {
                    tx.register(b"other", Scalar::Int(round as i64));
                    tx.inc(b"hits", 1);
                    tx.list(b"xs").insert(0, Scalar::Int(100 + round as i64));
                })
                .unwrap(),
        ));
    }

    let fold = |ops: &[&Op]| {
        let mut doc = peer();
        for op in ops {
            doc.apply(op);
        }
        doc.encode_state()
    };
    let forward: Vec<&Op> = all.iter().collect();
    let reverse: Vec<&Op> = all.iter().rev().collect();
    let interleaved: Vec<&Op> = all
        .iter()
        .step_by(2)
        .chain(all.iter().skip(1).step_by(2))
        .collect();

    let expected = fold(&forward);
    assert_eq!(fold(&reverse), expected, "reverse order diverges");
    assert_eq!(fold(&interleaved), expected, "interleaved order diverges");

    // Converging on a state that dropped half the writes would still converge, so
    // pin the totals too: 4 rounds x 2 channels of increments and inserts.
    let mut doc = peer();
    for op in &all {
        doc.apply(op);
    }
    assert_eq!(counter(&doc, b"hits"), 8);
    assert_eq!(list_len(&doc, b"xs"), 8);
}

// --- the identity outlives a catch-up ---

#[test]
fn a_channels_identity_survives_a_snapshot_catch_up() {
    let mut server = Document::new(cid(0xEE));
    server.transact(|tx| tx.register(b"seed", Scalar::Int(0)));

    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let (b, _) = session.subscribe_zone(ROOM, b"z");

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

#[test]
fn a_resubscribed_channel_authors_under_a_fresh_identity() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM);
    let first = session.channel_client(a);
    session.unsubscribe(a).expect("the channel is held");

    let (b, _) = session.subscribe(ROOM);
    assert_ne!(a, b, "a freed channel number is not reused");
    assert_ne!(
        session.channel_client(b),
        first,
        "the fresh subscription re-mints seq 0 under the retired identity"
    );
}
