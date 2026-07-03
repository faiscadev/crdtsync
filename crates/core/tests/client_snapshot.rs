//! Snapshot adoption at the client seat.
//!
//! When a reconnecting client falls below the room's compaction floor the server
//! replies with a whole-replica [`Message::Snapshot`]. Adopting it must replace
//! the room's state **without rewinding the client's own op-sequence counter**:
//! a server snapshot authors nothing, so its encoded replica op-seq counter is
//! 0 (distinct from the `Message::Snapshot { seq }` server sequence), and if adoption
//! reset the adopting client to it the client would re-mint `OpId`s it already
//! made durable — the server dedups those away, so the edits would apply locally
//! but never reach a peer (silent divergence).

use crdtsync_core::client::ClientSession;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};

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

const ROOM_A: &[u8] = b"room-a";

/// Build the state a server would snapshot: a replica under a *different*
/// identity that has only `apply`d the given ops (never authored one), so its
/// encoded op-seq counter is 0.
fn server_snapshot(ops: &[Op]) -> Vec<u8> {
    let mut server = Document::new(cid(0xFF));
    for op in ops {
        server.apply(op);
    }
    server.encode_state()
}

#[test]
fn adopting_a_snapshot_does_not_rewind_the_op_seq() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);

    // Author ops — advances this client's op-seq counter past 0.
    let mut authored = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    authored.extend(ops_of(
        s.edit(ch, |c| c.register(b"b", Scalar::Int(2))).unwrap(),
    ));
    let high = authored.iter().map(|o| o.id.seq).max().unwrap();

    // A below-floor cold-start reply: adopt the server's snapshot.
    let state = server_snapshot(&authored);
    s.receive(Message::Snapshot {
        channel: ch,
        seq: 7,
        state,
    })
    .unwrap();

    // The next authored op must not reuse a sequence already made durable.
    let next = ops_of(s.edit(ch, |c| c.register(b"c", Scalar::Int(3))).unwrap());
    assert!(
        next.iter().all(|op| op.id.seq > high),
        "op-seq rewound after snapshot adoption: minted {:?}, must all exceed {high}",
        next.iter().map(|o| o.id.seq).collect::<Vec<_>>()
    );
}
