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
//! over the ids already in memory takes no *number* off the wire: what a frame
//! contributes is ids, one search step each, never a position the counter adopts.
//! A forged op contributes one. The frame naming what a redacted delta withheld
//! (`Message::Frontier`) contributes one per sequence it names — so it buys held
//! ids at a higher density per wire byte than ops do, bounded by the frame itself
//! and by nothing else, and that cost is stated rather than claimed away.
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
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Element, Message, Op, OpId, Scalar};

mod common;
use common::{cid, with_seq};

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
    let (channel, _) = session.subscribe(ROOM).unwrap();
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
    let (channel, _) = session.subscribe(ROOM).unwrap();
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
    let (channel, _) = session.subscribe(ROOM).unwrap();
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

// --- the redacted delta: the ids a frame withholds, named by the frame ahead of it ---

#[test]
fn a_noted_sequence_is_not_a_free_id() {
    // The redaction's shape: the room's log holds seqs 0 and 1 on a path this
    // replica may no longer read, so the delta carries neither. Named, they are as
    // taken as an applied id.
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_seq(cid(1), b"shown", 2));
    assert_eq!(doc.next_seq(), 0, "the hole is where the redaction left it");

    doc.note_published(&[0, 1], 0);
    assert_eq!(doc.next_seq(), 3);
    assert_eq!(
        seqs(&doc.transact(|tx| tx.set(b"after", Scalar::Int(9)))),
        vec![3],
        "minted into an id the room's log already holds"
    );
}

#[test]
fn a_noted_sequence_leaves_the_gaps_around_it_free() {
    // Naming ids is not a high-water either: a sequence the room never kept stays
    // mintable whichever side of a named one it falls.
    let mut doc = Document::new(cid(1));
    doc.note_published(&[1, 4], 0);
    let mut minted = Vec::new();
    for i in 0..4 {
        let key = format!("k{i}").into_bytes();
        minted.extend(doc.transact(|tx| tx.set(&key, Scalar::Int(i))));
    }
    assert_eq!(seqs(&minted), vec![0, 2, 3, 5]);
}

#[test]
fn a_noted_ceiling_sequence_does_not_move_the_counter() {
    // The carrier takes sequences off the wire, so it has to obey what the whole
    // position obeys: one named sequence is one held id and one step of the
    // search, never a number the counter adopts. A frame naming the ceiling costs
    // a replica with no durable run nothing.
    let mut doc = Document::new(cid(1));
    doc.note_published(&[u64::MAX], 0);
    assert_eq!(
        doc.next_seq(),
        0,
        "the named seq was adopted as a high-water"
    );
    assert_eq!(
        seqs(&doc.transact(|tx| tx.set(b"x", Scalar::Int(1)))),
        vec![0]
    );
}

#[test]
fn a_noted_sequence_the_buffer_is_holding_leaves_the_state_encodable() {
    // The dedup set and the buffer are disjoint by construction — the state
    // encoding refuses a document whose id appears in both — and a delta split
    // across frames can name a sequence whose op is already waiting on its group.
    let mut author = Document::new(cid(1));
    let group = author.atomic_transact(|tx| {
        tx.set(b"x", Scalar::Int(1));
        tx.set(b"y", Scalar::Int(2));
    });
    let mut doc = Document::new(cid(1));
    doc.apply(&group[0]);

    assert_eq!(
        doc.next_seq(),
        1,
        "the buffered id blocks the mint on its own"
    );
    doc.note_published(&[group[0].id.seq, group[1].id.seq], 0);
    assert_eq!(
        doc.next_seq(),
        2,
        "naming a run the buffer half-holds left a hole in it"
    );
    Document::decode_state(&doc.encode_state())
        .expect("a noted id the buffer holds left the state undecodable");
}

#[test]
fn noting_a_sequence_reaches_no_other_replicas_run() {
    // The frame carries sequences, not ids, so it resolves in exactly one id
    // space: the replica's own. A peer's counter is untouched by anything this
    // replica is told.
    let mut doc = Document::new(cid(1));
    doc.note_published(&[0, 1, 2], 0);

    let mut peer = Document::new(cid(2));
    peer.note_published(&[0, 1, 2], 0);
    assert_eq!(doc.next_seq(), 3);
    assert_eq!(peer.next_seq(), 3);

    // What `doc` was told about its own run says nothing about what `peer` may
    // mint, and the ids stay distinct across the two.
    let a = doc.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    let b = peer.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    assert_ne!(a[0].id, b[0].id);
}

#[test]
fn a_noted_run_survives_a_snapshot_round_trip() {
    // A replica that persists its own state must not lose the hole's repair with
    // it: the named ids ride the dedup set, which the encoding carries.
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_seq(cid(1), b"shown", 2));
    doc.note_published(&[0, 1], 0);

    let back = Document::decode_state(&doc.encode_state()).expect("decodes");
    assert_eq!(back.next_seq(), 3);
}

#[test]
fn a_session_told_what_its_delta_withheld_mints_past_it() {
    // The wire shape end to end: the delta carries the one op on a readable path,
    // the frame ahead of it names the two it withheld, and the session's next edit
    // clears all three.
    let mut session = ClientSession::new(cid(1));
    let (channel, _) = session.subscribe(ROOM).unwrap();
    session
        .receive(Message::Frontier {
            channel,
            seqs: vec![0, 1],
            reach: 0,
        })
        .expect("the frontier applies");
    session
        .receive(Message::Ops {
            channel,
            ops: vec![op_at_seq(cid(1), b"shown", 2)],
        })
        .expect("the delta applies");

    let sent = session
        .edit(channel, |tx| tx.set(b"after", Scalar::Int(9)))
        .expect("channel held");
    let Message::Ops { ops, .. } = sent else {
        panic!("expected an Ops frame");
    };
    assert_eq!(
        seqs(&ops),
        vec![3],
        "the session re-minted an id the redaction withheld"
    );
}

#[test]
fn a_frontier_for_an_unheld_channel_is_refused() {
    let mut session = ClientSession::new(cid(1));
    let channel = Channel(7);
    assert!(session
        .receive(Message::Frontier {
            channel,
            seqs: vec![0],
            reach: 0,
        })
        .is_err());
}

#[test]
fn a_frontier_resolves_against_the_channels_own_identity() {
    // A session past its first subscription authors under a derived identity, so
    // the sequences it is told resolve in *that* replica's space — the connection's
    // own id answers only for channel 0.
    let mut session = ClientSession::new(cid(1));
    let (first, _) = session.subscribe(ROOM).unwrap();
    let (second, _) = session.subscribe(ROOM).unwrap();
    assert_ne!(
        session.document(second).expect("held").client(),
        session.document(first).expect("held").client(),
    );

    session
        .receive(Message::Frontier {
            channel: second,
            seqs: vec![0, 1],
            reach: 0,
        })
        .expect("the frontier applies");
    assert_eq!(session.document(second).expect("held").next_seq(), 2);
    assert_eq!(
        session.document(first).expect("held").next_seq(),
        0,
        "one channel's frontier moved another channel's replica",
    );
}

// --- the id-space half: a mint reads two records, and a redaction holes both ---

/// A schema with a text body, so a range can be anchored over it.
const MARK_SCHEMA: &str = r#"{
    "schema": "doc", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map", "children": { "body": "Body" } }, "Body": { "kind": "text" } },
    "marks": { "bold": { "flavor": "boolean" } }
}"#;

fn doc_with_body(client: ClientId) -> (Document, Vec<Op>) {
    let mut d = Document::new(client);
    d.set_schema(crdtsync_core::schema::Schema::parse(MARK_SCHEMA).unwrap());
    let ops = d.transact(|tx| {
        tx.text(b"body").insert(0, "hello");
    });
    (d, ops)
}

/// A restarted replica of `client`: the schema bound, and only the ops a redacted
/// delta carried folded in.
fn body_replica(client: ClientId, delivered: &[Op]) -> Document {
    let mut d = Document::new(client);
    d.set_schema(crdtsync_core::schema::Schema::parse(MARK_SCHEMA).unwrap());
    for op in delivered {
        d.apply(op);
    }
    d
}

/// A mark named `bold` over the whole body — an element whose id derives from the
/// op's stamp alone.
fn mark_body(d: &mut Document) -> Vec<Op> {
    let text = match d.get(b"body") {
        Some(Element::Text(t)) => t,
        _ => panic!("no body text"),
    };
    let seq = text.borrow().id();
    let start = crdtsync_core::ranged::RangeAnchor {
        seq,
        pos: text
            .borrow()
            .relative_position(0, crdtsync_core::list::Side::Right),
    };
    let end = crdtsync_core::ranged::RangeAnchor {
        seq,
        pos: text
            .borrow()
            .relative_position(5, crdtsync_core::list::Side::Left),
    };
    d.transact(|tx| {
        tx.ranged().mark(b"bold", start, end, Scalar::Bool(true));
    })
}

#[test]
fn a_noted_reach_moves_the_id_space_floor_like_the_op_it_stands_in_for() {
    // The frame's `reach` is the same primitive folding the op is: it moves this
    // replica's own entry in the id-space record and nothing else, so a replica
    // told the reach and one handed the ops mint from the same position.
    let (_, durable) = authored(cid(1), 3);
    let reach = durable
        .iter()
        .map(|op| op.reservation_end())
        .max()
        .expect("a run");

    let mut folded = Document::new(cid(1));
    for op in &durable {
        folded.apply(op);
    }
    let mut told = Document::new(cid(1));
    told.note_published(&seqs(&durable), reach);

    assert_eq!(told.next_seq(), folded.next_seq());
    assert_eq!(
        told.transact(|tx| tx.set(b"x", Scalar::Int(1)))[0]
            .stamp
            .lamport,
        folded.transact(|tx| tx.set(b"x", Scalar::Int(1)))[0]
            .stamp
            .lamport,
        "a told reach and a folded op left different mint positions",
    );
}

#[test]
fn a_noted_reach_is_held_to_the_ceiling_a_folded_stamp_is() {
    // A frame naming a position past the id space cannot install one the encoding
    // would refuse to carry — the same clamp `record_stamp` applies on the way in.
    let mut doc = Document::new(cid(1));
    doc.note_published(&[], u64::MAX);
    Document::decode_state(&doc.encode_state())
        .expect("a noted reach past the ceiling left the state undecodable");
}

#[test]
fn a_withheld_stamp_named_only_by_its_sequence_re_derives_its_element_id() {
    // The consequence the sequence half does not reach. A mark, an ACL tuple and an
    // XML sequence child all take their id from the stamp alone, so a replica
    // restored onto a position a withheld op already occupies derives an id the room
    // already binds — and the room drops the second one, silently.
    let (mut author, text) = doc_with_body(cid(1));
    let withheld = mark_body(&mut author);

    let mut room = Document::new(cid(9));
    for op in text.iter().chain(withheld.iter()) {
        room.apply(op);
    }
    assert_eq!(room.ranged_elements().len(), 1);

    // The restart: the mark is on a path this reader may no longer read, so its
    // delta carries the body and not the mark. Told only the sequences, it mints the
    // next mark onto the position the withheld op holds.
    let mut restored = body_replica(cid(1), &text);
    restored.note_published(&seqs(&withheld), 0);
    assert_eq!(
        mark_body(&mut restored)[0].stamp.lamport,
        withheld[0].stamp.lamport,
        "the fixture no longer re-derives, so it measures nothing",
    );

    // Told both, the mark takes a position the withheld op does not hold, and lands.
    let mut fixed = body_replica(cid(1), &text);
    let reach = withheld
        .iter()
        .map(|op| op.reservation_end())
        .max()
        .expect("a withheld run");
    fixed.note_published(&seqs(&withheld), reach);
    for op in &mark_body(&mut fixed) {
        room.apply(op);
    }
    assert_eq!(
        room.ranged_elements().len(),
        2,
        "the post-restart mark was swallowed at ingest",
    );
}

// --- a named sequence is not the op's grave: a later delivery still folds ---

#[test]
fn an_op_a_frontier_named_still_applies_when_it_is_delivered() {
    // Naming the run must not put the ops in the dedup set. The room's log holds
    // them, so a widened read grant or a resumed subscription can still deliver
    // them — and the recipient would be the one author whose content it never got
    // back if the name doubled as a refusal.
    let (_, durable) = authored(cid(1), 3);
    let mut doc = Document::new(cid(1));
    doc.note_published(&seqs(&durable), 0);
    assert_eq!(doc.next_seq(), 3);
    assert_eq!(scalar(&doc, b"k0"), None);

    for op in &durable {
        assert!(
            doc.apply(op),
            "an op named as withheld was dropped as a replay"
        );
    }
    assert_eq!(scalar(&doc, b"k0"), Some(Scalar::Int(0)));
    assert_eq!(
        doc.next_seq(),
        3,
        "the run is still closed once the ops land"
    );
}

#[test]
fn a_named_run_survives_a_state_round_trip_and_still_admits_its_ops() {
    // Both properties have to ride the encoding: a replica that persists its state
    // and reloads keeps the repair, and keeps the ops behind it applicable.
    let (_, durable) = authored(cid(1), 3);
    let mut doc = Document::new(cid(1));
    doc.note_published(&seqs(&durable), 0);

    let mut back = Document::decode_state(&doc.encode_state()).expect("decodes");
    assert_eq!(back.next_seq(), 3);
    for op in &durable {
        assert!(back.apply(op), "a reloaded reservation refused its own op");
    }
    assert_eq!(scalar(&back, b"k2"), Some(Scalar::Int(2)));
}

#[test]
fn a_sequence_the_same_delta_delivered_is_never_reserved_at_all() {
    // The three sets stay disjoint, which the state encoding requires: a sequence
    // the replica already holds is not reserved, whichever order the frames arrive
    // in.
    let (_, durable) = authored(cid(1), 2);
    let mut doc = Document::new(cid(1));
    for op in &durable {
        doc.apply(op);
    }
    doc.note_published(&seqs(&durable), 0);
    Document::decode_state(&doc.encode_state()).expect("a reservation overlapped the dedup set");
    assert_eq!(doc.next_seq(), 2);
}

#[test]
fn an_adopted_snapshot_carries_none_of_the_encoders_reservations() {
    // A reservation is an id under the *encoding* replica's identity, so an adopter
    // taking the snapshot over must not inherit it — its own run is what the
    // snapshot's frontier carries.
    let (_, durable) = authored(cid(1), 3);
    let mut author = Document::new(cid(1));
    author.note_published(&seqs(&durable), 0);
    assert_eq!(author.next_seq(), 3);

    let adopter = Document::decode_state_as(cid(2), 0, &author.encode_state()).expect("decodes");
    assert_eq!(
        adopter.next_seq(),
        0,
        "the adopter inherited reservations in an id space it never published in",
    );
}

#[test]
fn a_state_naming_one_id_as_both_reserved_and_buffered_is_refused() {
    // The three sets are disjoint by construction at runtime — `apply` clears the
    // reservation before the op is held — so a snapshot claiming otherwise did not
    // come from a replica. Decoding it would strand the reservation for the life of
    // the replica: a held id short-circuits `apply` before the clear, so the
    // sequence would never come back.
    let mut author = Document::new(cid(1));
    let group = author.atomic_transact(|tx| {
        tx.set(b"x", Scalar::Int(1));
        tx.set(b"y", Scalar::Int(2));
    });
    let mut doc = Document::new(cid(1));
    doc.apply(&group[0]);
    // A distinctive reservation, so the one occurrence in the encoding is the one
    // the patch below re-points.
    const MARKER: u64 = 0xDEAD_BEEF_0BAD_F00D;
    doc.note_published(&[MARKER], 0);
    let bytes = doc.encode_state();
    assert!(
        Document::decode_state(&bytes).is_ok(),
        "the honest document must decode",
    );

    // The reserved section is a `u32` count then one `u64` per sequence, following
    // the dedup set. Re-point the one reservation at the buffered member's sequence.
    let at = bytes
        .windows(8)
        .position(|w| w == MARKER.to_le_bytes())
        .expect("the reserved sequence is in the encoding");
    let mut forged = bytes.clone();
    forged[at..at + 8].copy_from_slice(&group[0].id.seq.to_le_bytes());
    assert!(
        Document::decode_state(&forged).is_err(),
        "a reservation over a buffered id decoded",
    );
}

// --- the three sets stay disjoint through every transition, because the encoding
//     writes a reservation as a bare sequence and reads it back under whatever
//     client the document then names ---

#[test]
fn an_op_delivered_after_its_reservation_leaves_the_state_encodable() {
    // `apply` clears the reservation as the op lands, and it has to: the id would
    // otherwise sit in the dedup set *and* the reserved set, which `read_state`
    // refuses — so a replica that folded a frontier, was later delivered the ops,
    // and persisted itself would write a snapshot it cannot read back.
    let (_, durable) = authored(cid(1), 3);
    let mut doc = Document::new(cid(1));
    doc.note_published(&seqs(&durable), 0);
    for op in &durable {
        doc.apply(op);
    }
    assert_eq!(doc.next_seq(), 3);
    Document::decode_state(&doc.encode_state())
        .expect("a delivered op left its reservation standing");
}

#[test]
fn an_adopting_replica_does_not_re_encode_the_encoders_reservations_as_its_own() {
    // The encoding writes a reservation as a bare sequence, which is right only
    // while every entry belongs to the document's own client — so the one place
    // that changes the client has to clear them. Immediately after `adopt_as` a
    // stale entry is inert (it is keyed on the *encoder's* id, which the mint no
    // longer asks about); it becomes real on the next round trip, where the
    // sequences are read back under the adopter's own identity.
    let (_, durable) = authored(cid(1), 3);
    let mut author = Document::new(cid(1));
    author.note_published(&seqs(&durable), 0);

    let adopter = Document::decode_state_as(cid(2), 0, &author.encode_state()).expect("decodes");
    assert_eq!(adopter.next_seq(), 0);
    let back = Document::decode_state(&adopter.encode_state()).expect("decodes");
    assert_eq!(
        back.next_seq(),
        0,
        "the adopter re-encoded the encoder's run as its own",
    );
}

#[test]
fn a_state_naming_one_id_as_both_reserved_and_applied_is_refused() {
    // The dedup-set half of the same disjointness `read_state` enforces for the
    // buffer: a reservation over an applied id is one `apply` can never clear,
    // since the dedup check short-circuits ahead of it.
    let (_, durable) = authored(cid(1), 1);
    let mut doc = Document::new(cid(1));
    doc.apply(&durable[0]);
    const MARKER: u64 = 0xDEAD_BEEF_0BAD_F00D;
    doc.note_published(&[MARKER], 0);
    let bytes = doc.encode_state();
    assert!(
        Document::decode_state(&bytes).is_ok(),
        "the honest document must decode",
    );

    let at = bytes
        .windows(8)
        .position(|w| w == MARKER.to_le_bytes())
        .expect("the reserved sequence is in the encoding");
    let mut forged = bytes.clone();
    forged[at..at + 8].copy_from_slice(&durable[0].id.seq.to_le_bytes());
    assert!(
        Document::decode_state(&forged).is_err(),
        "a reservation over an applied id decoded",
    );
}
