//! The op-seq high-water mark — a replica never re-mints an id it published.
//!
//! An `OpId` is `(client, seq)` and it is the dedup key: the first holder of an
//! id keeps it and every later arrival of that id is dropped as already-applied.
//! So a replica that mints an id it already published loses the write
//! *silently* — no divergence, no error, nothing downstream can detect it.
//!
//! A replica that persists its `ClientId` and rebuilds a fresh [`Document`] hits
//! exactly that: its durable ops come back on the catch-up, and a counter still
//! sitting at 0 mints straight into them. Both catch-up shapes reach it — the op
//! delta and the whole-replica snapshot (whose encoded counter belongs to the
//! *server*, which never mints, so it is 0 and lifts nothing).
//!
//! The counter therefore advances past what the replica has already published,
//! read off the dedup set it already holds — never assigned from a number on the
//! wire. That distinction is the whole design. A lift of the form
//! `seq = seq.max(wire_seq + 1)` repairs the restore case and hands any frame a
//! write primitive on the counter: one op carrying this replica's own id at
//! `u64::MAX` pins it at the ceiling, and the next ordinary local edit overflows
//! — a panic in debug, a wrap into already-published ids in release. Walking the
//! dedup set instead steps one id at a time over evidence already in memory, so
//! a forged frame contributes one entry the walk will never reach, and driving
//! the counter to the ceiling would take 2^64 folded ops.
//!
//! The same property is what keeps a *server's* replica clean. Its doc merges
//! ops under a node identity, and its `encode_state` rides every catch-up
//! snapshot it serves; a counter it could be made to inherit from a frame would
//! reach every client that later adopts one. Its counter moves only when the
//! server itself mints.

use crdtsync_core::client::ClientSession;
use crdtsync_core::{ClientId, Document, Element, Message, Op, OpId, Scalar};

mod common;
use common::cid;

const ROOM: &[u8] = b"room-a";

/// A one-op batch authored by `client`, re-labelled to carry `seq`. The stamp is
/// left as minted, so the op is otherwise an ordinary, applicable op — only its
/// identity is forged.
fn op_at_seq(client: ClientId, key: &[u8], seq: u64) -> Op {
    let mut op = Document::new(client)
        .transact(|tx| tx.set(key, Scalar::Bytes(key.to_vec())))
        .remove(0);
    op.id = OpId { client, seq };
    op
}

/// The seqs of the ops a batch minted.
fn seqs(ops: &[Op]) -> Vec<u64> {
    ops.iter().map(|op| op.id.seq).collect()
}

/// A root slot's scalar value. Two replicas carry their own identity and counter
/// in a snapshot, so their encodings are never byte-equal — the live values are
/// the equality oracle here.
fn scalar(d: &Document, key: &[u8]) -> Option<Scalar> {
    match d.get(key) {
        Some(Element::Scalar(s)) => Some(s),
        _ => None,
    }
}

/// A replica of `client` that authored `n` single-op edits, and those ops.
fn authored(client: ClientId, n: u64) -> (Document, Vec<Op>) {
    let mut doc = Document::new(client);
    let mut ops = Vec::new();
    for i in 0..n {
        let key = format!("k{i}").into_bytes();
        ops.extend(doc.transact(|tx| tx.set(&key, Scalar::Int(i as i64))));
    }
    (doc, ops)
}

// --- the restore: a catch-up hands back what this replica already published ---

#[test]
fn a_replica_caught_up_by_an_op_delta_mints_past_its_durable_run() {
    let (_, durable) = authored(cid(1), 3);
    assert_eq!(seqs(&durable), vec![0, 1, 2]);

    // The restart: same persisted identity, a fresh replica, caught up by the
    // room's op delta rather than a snapshot.
    let mut restored = Document::new(cid(1));
    for op in &durable {
        restored.apply(op);
    }

    let fresh = restored.transact(|tx| tx.set(b"after", Scalar::Int(9)));
    assert_eq!(
        seqs(&fresh),
        vec![3],
        "re-minted an id the room's log already holds"
    );
}

#[test]
fn a_replica_caught_up_by_a_snapshot_mints_past_its_durable_run() {
    let (_, durable) = authored(cid(1), 3);

    // The server's own replica merges but never mints, so the counter its
    // snapshot carries is 0 — the snapshot lifts nothing by itself.
    let mut server = Document::new(cid(9));
    for op in &durable {
        server.apply(op);
    }
    assert_eq!(server.next_seq(), 0);

    let mut restored =
        Document::decode_state_as(cid(1), 0, &server.encode_state()).expect("decodes");
    let fresh = restored.transact(|tx| tx.set(b"after", Scalar::Int(9)));
    assert_eq!(
        seqs(&fresh),
        vec![3],
        "re-minted an id the room's log already holds"
    );
}

#[test]
fn the_writes_after_an_op_delta_catch_up_reach_a_peer() {
    // The observable failure the counter exists to prevent: not divergence, a
    // *disappearance*. The peer already holds the restored replica's durable
    // ops, so a re-minted id is dropped at its dedup set and the edit is gone.
    let (_, durable) = authored(cid(1), 2);

    let mut peer = Document::new(cid(2));
    for op in &durable {
        peer.apply(op);
    }

    let mut restored = Document::new(cid(1));
    for op in &durable {
        restored.apply(op);
    }
    let fresh = restored.transact(|tx| tx.set(b"after", Scalar::Bytes(b"kept".to_vec())));
    for op in &fresh {
        peer.apply(op);
    }

    assert_eq!(
        scalar(&peer, b"after"),
        Some(Scalar::Bytes(b"kept".to_vec())),
        "the post-restore write was deduped away"
    );
    assert_eq!(scalar(&peer, b"k0"), scalar(&restored, b"k0"));
    assert_eq!(scalar(&peer, b"after"), scalar(&restored, b"after"));
}

#[test]
fn a_gap_in_the_durable_run_is_reused_and_the_published_ids_are_skipped() {
    // The counter is not "the highest id seen plus one" — it is the next id this
    // replica has not published. A hole left by an op the room never kept is a
    // free id, and the ids past it are still taken.
    let mut restored = Document::new(cid(1));
    restored.apply(&op_at_seq(cid(1), b"a", 0));
    restored.apply(&op_at_seq(cid(1), b"b", 1));
    restored.apply(&op_at_seq(cid(1), b"c", 5));

    let mut minted = Vec::new();
    for i in 0..5 {
        let key = format!("n{i}").into_bytes();
        minted.extend(restored.transact(|tx| tx.set(&key, Scalar::Int(i))));
    }
    assert_eq!(seqs(&minted), vec![2, 3, 4, 6, 7]);
}

#[test]
fn a_delta_holding_no_op_of_this_replica_leaves_the_counter_at_zero() {
    let (_, foreign) = authored(cid(2), 4);
    let mut doc = Document::new(cid(1));
    for op in &foreign {
        doc.apply(op);
    }
    assert_eq!(doc.next_seq(), 0);
    assert_eq!(
        seqs(&doc.transact(|tx| tx.set(b"x", Scalar::Int(1)))),
        vec![0]
    );
}

#[test]
fn a_peers_high_op_seq_never_moves_this_replicas_counter() {
    // Another replica's counter is its own; a batch of theirs at seq 5000 says
    // nothing about which ids this replica has published.
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_seq(cid(2), b"a", 5_000));
    assert_eq!(doc.next_seq(), 0);
    assert_eq!(
        seqs(&doc.transact(|tx| tx.set(b"x", Scalar::Int(1)))),
        vec![0]
    );
}

// --- the forged frame: no number on the wire reaches the counter ---

#[test]
fn a_frame_carrying_this_replicas_id_at_the_ceiling_does_not_move_the_counter() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_seq(cid(1), b"forged", u64::MAX));

    assert_eq!(
        doc.next_seq(),
        0,
        "the wire seq was adopted as a high-water"
    );
    // The next ordinary local edit: no overflow panic, no wrap into a published
    // id, and the id it mints is the one a replica with no durable run mints.
    assert_eq!(
        seqs(&doc.transact(|tx| tx.set(b"x", Scalar::Int(1)))),
        vec![0]
    );
}

#[test]
fn a_frame_one_below_the_ceiling_does_not_move_the_counter_either() {
    // `max(seq + 1)` at `u64::MAX - 1` lands the counter exactly on `u64::MAX`,
    // where the *following* increment overflows — the same primitive one step
    // further out, so the ceiling is not a special case to guard.
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_seq(cid(1), b"forged", u64::MAX - 1));

    assert_eq!(doc.next_seq(), 0);
    let first = doc.transact(|tx| tx.set(b"x", Scalar::Int(1)));
    let second = doc.transact(|tx| tx.set(b"y", Scalar::Int(2)));
    assert_eq!(seqs(&first), vec![0]);
    assert_eq!(seqs(&second), vec![1]);
}

#[test]
fn a_forged_ceiling_frame_leaves_the_replica_converging() {
    // The forged op is still an ordinary op — it applies and merges. What it
    // must not do is disturb what the replica mints next.
    let mut doc = Document::new(cid(1));
    let forged = op_at_seq(cid(1), b"forged", u64::MAX);
    doc.apply(&forged);

    let mut peer = Document::new(cid(2));
    peer.apply(&forged);
    for op in &doc.transact(|tx| tx.set(b"x", Scalar::Int(1))) {
        peer.apply(op);
    }

    assert_eq!(scalar(&peer, b"x"), Some(Scalar::Int(1)));
    assert_eq!(scalar(&peer, b"forged"), scalar(&doc, b"forged"));
}

#[test]
fn a_ceiling_frame_survives_a_snapshot_round_trip_without_moving_the_counter() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_seq(cid(1), b"forged", u64::MAX));
    let back = Document::decode_state(&doc.encode_state()).expect("decodes");
    assert_eq!(back.next_seq(), 0);
}

// --- the server's replica, and the snapshot it serves ---

#[test]
fn a_ceiling_frame_under_the_nodes_identity_does_not_poison_its_snapshot() {
    // A node's room replica authors under a fixed identity, and its
    // `encode_state` rides every catch-up snapshot it serves. A counter a frame
    // could push to the ceiling here would reach every client that later adopts
    // one — so the frame must move nothing.
    let node = cid(0);
    let mut server = Document::new(node);
    server.apply(&op_at_seq(node, b"forged", u64::MAX));
    assert_eq!(server.next_seq(), 0, "a frame moved the node's counter");

    let mut joiner = Document::decode_state_as(cid(1), 0, &server.encode_state()).expect("decodes");
    assert_eq!(joiner.next_seq(), 0, "the joiner inherited a ceiling");
    assert_eq!(
        seqs(&joiner.transact(|tx| tx.set(b"x", Scalar::Int(1)))),
        vec![0]
    );
}

#[test]
fn an_adopted_snapshot_does_not_import_the_snapshot_authors_counter() {
    // The counter encoded in a snapshot belongs to whoever authored it. An
    // adopting replica keeps its own — it has published no id under the author's
    // count, and a foreign number is not its floor.
    let (author, _) = authored(cid(9), 6);
    assert_eq!(author.next_seq(), 6);

    let adopter = Document::decode_state_as(cid(1), 2, &author.encode_state()).expect("decodes");
    assert_eq!(adopter.next_seq(), 2);
}

#[test]
fn an_adopting_replica_keeps_the_high_water_it_was_handed() {
    // The live session's counter is the caller's contribution: a projected
    // snapshot arrives with its dedup set scrubbed, so the handed high-water is
    // the only evidence of what this replica already published.
    let (author, durable) = authored(cid(1), 3);
    let mut server = Document::new(cid(9));
    for op in &durable {
        server.apply(op);
    }

    let mut adopter = Document::decode_state_as(cid(1), author.next_seq(), &server.encode_state())
        .expect("decodes");
    assert_eq!(adopter.next_seq(), 3);
    assert_eq!(
        seqs(&adopter.transact(|tx| tx.set(b"x", Scalar::Int(1)))),
        vec![3]
    );
}

// --- through the session, which is where a real restart lands ---

#[test]
fn a_restarted_session_caught_up_by_an_op_delta_mints_past_its_durable_run() {
    // The persisted thing is the `ClientId`; the replica is rebuilt from the
    // room. Channel 0 authors under the declared id unchanged, so the ops the
    // delta carries are this session's own.
    let (_, durable) = authored(cid(1), 3);

    let mut session = ClientSession::new(cid(1));
    let (channel, _) = session.subscribe(ROOM);
    session
        .receive(Message::Ops {
            channel,
            ops: durable.clone(),
        })
        .expect("catch-up applies");

    let sent = session
        .edit(channel, |tx| tx.set(b"after", Scalar::Int(9)))
        .expect("channel held");
    let Message::Ops { ops, .. } = sent else {
        panic!("expected an Ops frame");
    };
    assert_eq!(
        seqs(&ops),
        vec![3],
        "the session re-minted an id the room's log already holds"
    );
}

#[test]
fn a_restarted_session_caught_up_by_a_snapshot_mints_past_its_durable_run() {
    let (_, durable) = authored(cid(1), 3);
    let mut server = Document::new(cid(9));
    for op in &durable {
        server.apply(op);
    }

    let mut session = ClientSession::new(cid(1));
    let (channel, _) = session.subscribe(ROOM);
    session
        .receive(Message::Snapshot {
            channel,
            seq: 3,
            state: server.encode_state(),
        })
        .expect("snapshot adopts");

    let sent = session
        .edit(channel, |tx| tx.set(b"after", Scalar::Int(9)))
        .expect("channel held");
    let Message::Ops { ops, .. } = sent else {
        panic!("expected an Ops frame");
    };
    assert_eq!(seqs(&ops), vec![3]);
}

#[test]
fn a_forged_catch_up_frame_leaves_a_session_minting_ordinarily() {
    let mut session = ClientSession::new(cid(1));
    let (channel, _) = session.subscribe(ROOM);
    session
        .receive(Message::Ops {
            channel,
            ops: vec![op_at_seq(cid(1), b"forged", u64::MAX)],
        })
        .expect("applies");

    let sent = session
        .edit(channel, |tx| tx.set(b"x", Scalar::Int(1)))
        .expect("channel held");
    let Message::Ops { ops, .. } = sent else {
        panic!("expected an Ops frame");
    };
    assert_eq!(seqs(&ops), vec![0]);
}
