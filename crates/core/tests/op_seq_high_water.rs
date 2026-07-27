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
//! So minting searches for a sequence the replica does not hold, rather than
//! tracking a frontier. That distinction is the whole design. A lift of the form
//! `seq = seq.max(wire_seq + 1)` repairs the restore case and hands any frame a
//! write primitive on the counter: one op carrying this replica's own id at
//! `u64::MAX` pins it at the ceiling, and the next ordinary local edit overflows
//! — a panic in debug, a wrap into already-published ids in release. A search
//! over the ids already in memory takes nothing off the wire: a forged frame
//! contributes one held id, which costs one step.
//!
//! Held, not merely applied — an op waiting on its transaction group sits in the
//! buffer with its id out of the dedup set, and the room's log holds it all the
//! same. And the space is a finite *set*, not a ladder: the search wraps at its
//! end, so a position no minting could reach is a few wrapped steps rather than a
//! replica re-issuing one id forever.
//!
//! The same property is what keeps a *server's* replica clean. Its doc merges
//! ops under a node identity, and its `encode_state` rides every catch-up
//! snapshot it serves; a counter it could be made to inherit from a frame would
//! reach every client that later adopts one. Its counter moves only when the
//! server itself mints.

use std::collections::HashSet;

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
fn the_reported_next_seq_is_the_id_a_catch_up_left_free() {
    // `next_seq` is what a session hands `decode_state_as` when it adopts a
    // snapshot, so it has to answer for the durable run the catch-up delivered —
    // not for the position the counter happens to sit at.
    let (_, durable) = authored(cid(1), 3);
    let mut restored = Document::new(cid(1));
    assert_eq!(restored.next_seq(), 0);
    for op in &durable {
        restored.apply(op);
    }
    assert_eq!(restored.next_seq(), 3);
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
fn an_op_of_this_replica_still_held_in_the_buffer_is_not_a_free_id() {
    // An op waiting on its transaction group — or on a target no create has made
    // reachable — is buffered with its id out of the dedup set, and it is as
    // published as any other: the room's log holds it. A catch-up delta split
    // across frames delivers exactly this state.
    let mut author = Document::new(cid(1));
    let plain = author.transact(|tx| tx.set(b"k", Scalar::Int(0)));
    let group = author.atomic_transact(|tx| {
        tx.set(b"x", Scalar::Int(1));
        tx.set(b"y", Scalar::Int(2));
    });
    assert_eq!(seqs(&plain), vec![0]);
    assert_eq!(seqs(&group), vec![1, 2]);

    // The group's second member has not landed, so the first waits in the buffer
    // with its id out of the dedup set. Minting it back would leave the replica
    // holding two different ops under one identity once the buffer drains —
    // divergence, not a dropped write, since each peer keeps whichever it saw
    // first.
    let mut restored = Document::new(cid(1));
    restored.apply(&plain[0]);
    restored.apply(&group[0]);
    assert_eq!(
        restored.next_seq(),
        2,
        "an id held for an incomplete group was reported free"
    );
    let local = restored.transact(|tx| tx.set(b"after", Scalar::Int(9)));
    assert!(
        local[0].id != group[0].id,
        "minted an id the buffer was already holding"
    );
}

#[test]
fn a_completed_group_leaves_the_counter_past_every_member() {
    // The catch-up shape: the whole delta arrives, the group completes, and the
    // counter clears every member it carried — a buffered member counts while it
    // waits and is still counted once it lands.
    let mut author = Document::new(cid(1));
    let plain = author.transact(|tx| tx.set(b"k", Scalar::Int(0)));
    let group = author.atomic_transact(|tx| {
        tx.set(b"x", Scalar::Int(1));
        tx.set(b"y", Scalar::Int(2));
    });

    let mut restored = Document::new(cid(1));
    for op in plain.iter().chain(group.iter()) {
        restored.apply(op);
    }
    assert_eq!(restored.next_seq(), 3);
    assert_eq!(
        seqs(&restored.transact(|tx| tx.set(b"after", Scalar::Int(9)))),
        vec![3]
    );
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

/// `bytes` with the encoded op-seq position replaced by `seq`. The position
/// follows the version byte, the client id, and the lamport clock, little-endian
/// like every other integer in the state codec.
fn with_seq(mut bytes: Vec<u8>, seq: u64) -> Vec<u8> {
    let at = 1 + 16 + 8;
    bytes[at..at + 8].copy_from_slice(&seq.to_le_bytes());
    bytes
}

#[test]
fn a_counter_decoded_at_the_end_of_the_space_keeps_minting_distinct_ids() {
    // The position is a search hint, not a frontier, so the end of the space is
    // not the end of the ids: the search wraps into the sequences the replica has
    // not published. A replica restored onto a position no minting could reach
    // keeps authoring distinct ids rather than re-issuing one forever.
    let state = with_seq(Document::new(cid(1)).encode_state(), u64::MAX);
    let mut doc = Document::decode_state(&state).expect("decodes");

    let mut minted = Vec::new();
    for i in 0..4 {
        let key = format!("k{i}").into_bytes();
        minted.extend(doc.transact(|tx| tx.set(&key, Scalar::Int(i))));
    }
    let ids: HashSet<OpId> = minted.iter().map(|op| op.id).collect();
    assert_eq!(ids.len(), minted.len(), "re-issued an id it had published");
}

#[test]
fn a_counter_decoded_short_of_the_end_wraps_past_it_without_repeating() {
    // The same, one step out: a position at `MAX - 2` buys three mints before the
    // wrap, which is exactly where a bound that only refused `u64::MAX` would let
    // the duplicate through.
    let state = with_seq(Document::new(cid(1)).encode_state(), u64::MAX - 2);
    let mut doc = Document::decode_state(&state).expect("decodes");

    let mut minted = Vec::new();
    for i in 0..6 {
        let key = format!("k{i}").into_bytes();
        minted.extend(doc.transact(|tx| tx.set(&key, Scalar::Int(i))));
    }
    let ids: HashSet<OpId> = minted.iter().map(|op| op.id).collect();
    assert_eq!(ids.len(), minted.len(), "re-issued an id it had published");
    assert_eq!(
        seqs(&minted),
        vec![u64::MAX - 2, u64::MAX - 1, u64::MAX, 0, 1, 2]
    );
}

#[test]
fn a_replica_handed_the_end_of_the_space_still_mints_distinct_ids() {
    // `decode_state_as` takes the position from its caller, so the same property
    // has to hold when the caller names one no minting could reach.
    let state = Document::new(cid(1)).encode_state();
    let mut doc = Document::decode_state_as(cid(1), u64::MAX, &state).expect("decodes");

    let first = doc.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    let second = doc.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    assert_ne!(first[0].id, second[0].id);
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
    // the only evidence of what this replica already published. It answers for a
    // session that stayed up across the catch-up; a session rebuilt from nothing
    // reports 0 and has no other evidence to offer.
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
