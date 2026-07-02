//! Client offline op queue — the outbox that survives a disconnect.
//!
//! A [`ClientSession`] retains every op it authors per channel until the server
//! confirms it. `edit` / `atomic_edit` / `commit_atomic` enqueue their ops; an
//! inbound [`Message::Accepted`] `{ through }` prunes every op whose per-client
//! sequence (`OpId.seq`) is at or below `through`; `resend` re-emits the
//! unpruned tail so a reconnect replays exactly the ops the server has not yet
//! acknowledged. The sender is never echoed its own ops, so `Accepted` is the
//! only thing that drains the outbox.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::{Channel, ClientId, Document, Message, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn ops_of(m: Message) -> Vec<Op> {
    match m {
        Message::Ops { ops, .. } => ops,
        other => panic!("expected Ops, got {other:?}"),
    }
}

/// The highest per-client op sequence in a batch — the frontier a server would
/// acknowledge after committing it.
fn max_seq(ops: &[Op]) -> u64 {
    ops.iter().map(|o| o.id.seq).max().expect("non-empty batch")
}

const ROOM_A: &[u8] = b"room-a";
const ROOM_B: &[u8] = b"room-b";

// --- enqueue on author ---

#[test]
fn an_edit_enqueues_its_ops() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let ops = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    assert_eq!(s.outbox_len(ch), ops.len());
    assert!(ops.len() >= 1);
}

#[test]
fn edits_accumulate_in_the_outbox() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let first = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    let second = ops_of(s.edit(ch, |c| c.register(b"b", Scalar::Int(2))).unwrap());
    assert_eq!(s.outbox_len(ch), first.len() + second.len());
}

#[test]
fn an_atomic_edit_enqueues_the_whole_group() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let ops = ops_of(
        s.atomic_edit(ch, |c| {
            c.register(b"x", Scalar::Int(1));
            c.register(b"y", Scalar::Int(2));
        })
        .unwrap(),
    );
    assert_eq!(ops.len(), 2);
    assert_eq!(s.outbox_len(ch), 2);
}

#[test]
fn a_recorded_group_enqueues_only_on_commit() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    s.begin_atomic(ch).unwrap();
    // Edits during recording emit empty batches — nothing to enqueue yet.
    assert!(ops_of(s.edit(ch, |c| c.register(b"x", Scalar::Int(1))).unwrap()).is_empty());
    assert!(ops_of(s.edit(ch, |c| c.register(b"y", Scalar::Int(2))).unwrap()).is_empty());
    assert_eq!(s.outbox_len(ch), 0);
    let ops = ops_of(s.commit_atomic(ch).unwrap());
    assert_eq!(ops.len(), 2);
    assert_eq!(s.outbox_len(ch), 2);
}

// --- prune on Accepted ---

#[test]
fn accepted_prunes_ops_through_the_frontier() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let first = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    let second = ops_of(s.edit(ch, |c| c.register(b"b", Scalar::Int(2))).unwrap());

    // The server acknowledges only the first batch.
    s.receive(Message::Accepted {
        channel: ch,
        through: max_seq(&first),
    })
    .unwrap();

    // Exactly the second batch remains outstanding.
    assert_eq!(s.outbox_len(ch), second.len());
}

#[test]
fn accepted_at_the_last_seq_drains_the_outbox() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let a = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    let b = ops_of(s.edit(ch, |c| c.register(b"b", Scalar::Int(2))).unwrap());
    let last = max_seq(&b).max(max_seq(&a));

    s.receive(Message::Accepted {
        channel: ch,
        through: last,
    })
    .unwrap();
    assert_eq!(s.outbox_len(ch), 0);
}

#[test]
fn accepted_is_idempotent_and_a_stale_frontier_prunes_nothing_new() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let first = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    let second = ops_of(s.edit(ch, |c| c.register(b"b", Scalar::Int(2))).unwrap());
    let through = max_seq(&first);

    s.receive(Message::Accepted {
        channel: ch,
        through,
    })
    .unwrap();
    // Re-delivering the same (or a stale lower) frontier changes nothing.
    s.receive(Message::Accepted {
        channel: ch,
        through,
    })
    .unwrap();
    assert_eq!(s.outbox_len(ch), second.len());
}

#[test]
fn accepted_on_an_unheld_channel_is_rejected() {
    let mut s = ClientSession::new(cid(1));
    s.subscribe(ROOM_A);
    let err = s.receive(Message::Accepted {
        channel: Channel(9),
        through: 0,
    });
    assert!(matches!(err, Err(ClientError::UnknownChannel(_))));
}

// --- resend the tail ---

#[test]
fn resend_reemits_the_unacked_tail() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let first = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    let second = ops_of(s.edit(ch, |c| c.register(b"b", Scalar::Int(2))).unwrap());
    s.receive(Message::Accepted {
        channel: ch,
        through: max_seq(&first),
    })
    .unwrap();

    match s.resend(ch) {
        Some(Message::Ops { channel, ops }) => {
            assert_eq!(channel, ch);
            assert_eq!(ops, second);
        }
        other => panic!("expected the unacked tail as Ops, got {other:?}"),
    }
}

#[test]
fn a_resent_tail_still_applies_on_a_peer() {
    // The tail a reconnect replays is ordinary ops a peer folds in.
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    s.edit(ch, |c| c.register(b"a", Scalar::Int(7))).unwrap();

    let resent = s.resend(ch).expect("outstanding ops");
    let mut peer = Document::new(cid(2));
    for op in ops_of(resent) {
        peer.apply(&op);
    }
    match peer.get(b"a") {
        Some(crdtsync_core::Element::Register(r)) => {
            assert_eq!(r.borrow().read(), &Scalar::Int(7));
        }
        _ => panic!("peer did not fold in the resent op"),
    }
}

#[test]
fn resend_after_a_full_ack_is_none() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let ops = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    s.receive(Message::Accepted {
        channel: ch,
        through: max_seq(&ops),
    })
    .unwrap();
    assert!(s.resend(ch).is_none());
}

#[test]
fn resend_on_a_fresh_room_is_none() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    assert!(s.resend(ch).is_none());
}

#[test]
fn resend_on_an_unheld_channel_is_none() {
    let s = ClientSession::new(cid(1));
    assert!(s.resend(Channel(7)).is_none());
}

// --- isolation ---

#[test]
fn inbound_peer_ops_do_not_touch_the_outbox() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap();
    let before = s.outbox_len(ch);

    // A peer's op fanned in by the server is not this client's authored op.
    let mut peer = Document::new(cid(9));
    let peer_ops = peer.transact(|tx| tx.register(b"z", Scalar::Int(3)));
    s.receive(Message::Ops {
        channel: ch,
        ops: peer_ops,
    })
    .unwrap();

    assert_eq!(s.outbox_len(ch), before);
}

#[test]
fn two_channels_keep_separate_outboxes() {
    let mut s = ClientSession::new(cid(1));
    let (a, _) = s.subscribe(ROOM_A);
    let (b, _) = s.subscribe(ROOM_B);
    let a_ops = ops_of(s.edit(a, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    s.edit(b, |c| c.register(b"b", Scalar::Int(2))).unwrap();

    // Acking channel A leaves channel B's outbox untouched.
    s.receive(Message::Accepted {
        channel: a,
        through: max_seq(&a_ops),
    })
    .unwrap();
    assert_eq!(s.outbox_len(a), 0);
    assert!(s.outbox_len(b) >= 1);
}

#[test]
fn the_outbox_survives_a_reconnect_and_resends() {
    // Edit while the connection is down (no Accepted arrives), then reconnect:
    // resume re-subscribes from the last-seen sequence and resend replays the
    // unacknowledged ops.
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    let authored = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());

    match s.resume(ch) {
        Some(Message::Subscribe { last_seen_seq, .. }) => assert_eq!(last_seen_seq, 0),
        other => panic!("expected Subscribe, got {other:?}"),
    }
    match s.resend(ch) {
        Some(Message::Ops { ops, .. }) => assert_eq!(ops, authored),
        other => panic!("expected the outbox replay, got {other:?}"),
    }
}
