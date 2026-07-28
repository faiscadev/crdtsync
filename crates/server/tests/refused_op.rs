//! An op the replica refuses must not be logged, deduped, fanned out or acked.
//!
//! `Document::apply` answers `false` for three unrelated situations, and only two
//! of them mean "not yet": an op already applied or already held (a duplicate), an
//! op whose target is not reachable yet or whose transaction group is incomplete (a
//! wait, which a later arrival ends), and an op the replica **refuses forever** — a
//! stamp naming a client other than its author, a stamp outside the position an id
//! may occupy, or a transaction member declaring a group size no group can have.
//! The third set is a pure function of the op, so every replica refuses exactly the
//! same ops and the judgement is convergent rather than a divergence.
//!
//! The ingest seam collapsed the three: it appended to the store *before* applying,
//! discarded the bool, and entered the room's dedup set and retained log
//! unconditionally. A refused op therefore became durable, permanently deduped
//! against a corrected resend of the same `OpId`, broadcast to every peer, replayed
//! on every room reload, and — because `handle_ops` acks a `through` computed over
//! the whole submitted batch — positively acked while landing nowhere.
//!
//! The refusal is now a gate at ingress, beside the other author checks: the batch
//! comes back as a recoverable `OpsRejected`, nothing is persisted, and the author
//! keeps its ops. The waiting cases are untouched — a buffered op is still logged,
//! still fanned out, and still acked, because it is state a later op completes.

use std::sync::Mutex;

use crdtsync_core::doc::Document;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    ClientId, ErrorCode, Message, Op, OpId, OpKind, Scalar, Stamp, Tx, TxId, LAMPORT_STATE_CEILING,
};
use crdtsync_server::auth::AllowAll;
use crdtsync_server::{step, Hub, PermitAll, SchemaRegistry, Session};

const ROOM: &[u8] = b"room-1";
const CH: Channel = Channel(0);

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

// --- op shapes ---
//
// Every batch authors under `client`, because `handle_ops` independently requires
// each op's `id.client` to be the channel's authoring identity. The refusals below
// are therefore reached through a batch that clears every pre-existing gate.

/// A well-formed write the replica applies.
fn honest_op(client: ClientId) -> Op {
    let mut d = Document::new(client);
    d.transact(|tx| tx.register(b"title", Scalar::Int(1)))
        .pop()
        .expect("a register write emits one op")
}

/// An op whose stamp names a client other than its author — it would mint node ids
/// inside the victim's id space.
fn foreign_stamp_op(client: ClientId, victim: ClientId) -> Op {
    let mut op = honest_op(client);
    op.stamp.client = victim;
    op
}

/// An op stamped off the sub-lamport dimension. `stamp_key` omits the offset, so
/// two stamps differing only there derive the same node id.
fn offset_stamp_op(client: ClientId) -> Op {
    let mut op = honest_op(client);
    op.stamp.offset = 1;
    op
}

/// An op whose stamp reaches past the highest position an id may occupy.
fn over_ceiling_op(client: ClientId) -> Op {
    let mut op = honest_op(client);
    op.stamp.lamport = LAMPORT_STATE_CEILING + 1;
    op
}

/// A transaction member declaring a group size no group can have. The codec refuses
/// the same op at the wire boundary; this is the shape an in-process relay submits.
fn zero_count_tx_op(client: ClientId) -> Op {
    let mut op = honest_op(client);
    op.tx = Some(Tx {
        id: TxId(7),
        count: 0,
    });
    op
}

/// A write to a container no op in the batch creates — admissible, but not
/// applicable yet. It is buffered, and must be logged, fanned out and acked exactly
/// as before.
fn buffered_op(client: ClientId) -> Op {
    let mut d = Document::new(client);
    let mut ops = d.transact(|tx| tx.map(b"nested").register(b"k", Scalar::Int(1)));
    // Drop the create, keeping the write that depends on it.
    ops.pop().expect("the nested write is the last op")
}

// --- the wire path through the session ---

fn st(h: &mut Hub, s: &mut Session, msg: Message) -> crdtsync_server::Response {
    step(
        h,
        s,
        &AllowAll,
        &PermitAll,
        None,
        &Mutex::new(SchemaRegistry::new()),
        None,
        None,
        0,
        None,
        msg,
    )
}

fn handshake(h: &mut Hub, s: &mut Session, client: ClientId) {
    st(
        h,
        s,
        Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        },
    );
    st(
        h,
        s,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
    let r = st(
        h,
        s,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    );
    assert!(!r.close, "subscribe establishes the channel");
}

fn submit(h: &mut Hub, s: &mut Session, ops: Vec<Op>) -> crdtsync_server::Response {
    st(h, s, Message::Ops { channel: CH, ops })
}

fn is_accepted(r: &crdtsync_server::Response) -> bool {
    r.replies
        .iter()
        .any(|m| matches!(m, Message::Accepted { .. }))
}

fn rejected_seqs(r: &crdtsync_server::Response) -> Option<Vec<u64>> {
    r.replies.iter().find_map(|m| match m {
        Message::OpsRejected {
            seqs,
            reason: ErrorCode::MalformedOp,
            ..
        } => Some(seqs.clone()),
        _ => None,
    })
}

/// A hub and an authenticated, subscribed session authoring as `client`.
fn joined(client: ClientId) -> (Hub, Session) {
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, client);
    (h, s)
}

// --- a refused op is rejected at ingress ---

/// The whole shape of the defect in one case: refused, so nothing is acked, nothing
/// is logged, nothing is broadcast, and the room is left exactly as it was.
#[test]
fn a_refused_op_is_rejected_never_logged_acked_or_fanned_out() {
    let (mut h, mut s) = joined(cid(1));
    let before = h.seq(ROOM);

    let op = foreign_stamp_op(cid(1), cid(2));
    let r = submit(&mut h, &mut s, vec![op.clone()]);

    assert!(!is_accepted(&r), "a refused op is never acked as landed");
    assert_eq!(
        rejected_seqs(&r),
        Some(vec![op.id.seq]),
        "the author is told which ops it keeps"
    );
    assert!(!r.close, "a malformed op is recoverable, not a disconnect");
    assert!(r.broadcast.is_empty(), "nothing fans out to peers");
    assert_eq!(h.seq(ROOM), before, "the log does not grow");
    assert!(h.get(ROOM, b"title").is_none(), "no state lands");
}

/// Each permanent refusal is reached over the wire: none of the three is caught by
/// an earlier gate, so each would otherwise be logged and acked.
#[test]
fn every_permanent_refusal_is_rejected_at_ingress() {
    for (name, op) in [
        (
            "stamp names another client",
            foreign_stamp_op(cid(1), cid(2)),
        ),
        ("stamp off the sub-lamport axis", offset_stamp_op(cid(1))),
        ("stamp past the id-space ceiling", over_ceiling_op(cid(1))),
        (
            "transaction size no group can have",
            zero_count_tx_op(cid(1)),
        ),
    ] {
        let (mut h, mut s) = joined(cid(1));
        let r = submit(&mut h, &mut s, vec![op]);
        assert!(!is_accepted(&r), "{name}: acked");
        assert!(rejected_seqs(&r).is_some(), "{name}: not rejected");
        assert_eq!(h.seq(ROOM), 0, "{name}: logged");
    }
}

/// The gate covers the batch, not the op: `through` is a max over the whole
/// submitted batch, so accepting the honest half would ack the refused op's
/// sequence along with it.
#[test]
fn one_refused_op_rejects_the_whole_batch() {
    let (mut h, mut s) = joined(cid(1));
    let good = honest_op(cid(1));
    let bad = offset_stamp_op(cid(1));

    let r = submit(&mut h, &mut s, vec![good.clone(), bad.clone()]);

    assert!(!is_accepted(&r));
    assert_eq!(
        rejected_seqs(&r),
        Some(vec![good.id.seq, bad.id.seq]),
        "the author keeps the whole batch"
    );
    assert_eq!(h.seq(ROOM), 0, "neither op lands");
}

// --- the dedup set is not poisoned ---

/// The durable consequence: a refused op used to enter the room's seen set, so the
/// author's corrected resend under the same `OpId` was swallowed as a duplicate —
/// forever, and across a reload.
#[test]
fn a_refused_op_does_not_dedup_a_corrected_resend() {
    let (mut h, mut s) = joined(cid(1));
    let bad = offset_stamp_op(cid(1));
    submit(&mut h, &mut s, vec![bad.clone()]);

    // The same op id, correctly stamped — what a client fixing its bug resends.
    let fixed = honest_op(cid(1));
    assert_eq!(fixed.id, bad.id, "the resend reuses the refused op's id");

    let r = submit(&mut h, &mut s, vec![fixed]);
    assert!(is_accepted(&r), "the corrected resend is accepted");
    assert_eq!(h.seq(ROOM), 1, "and lands in the log");
    assert!(h.get(ROOM, b"title").is_some(), "and in the state");
}

// --- the waiting cases are untouched ---

/// A buffered op also answers `false` from `apply`, and must keep being logged,
/// broadcast and acked: it is state a later op completes, not a refusal.
#[test]
fn a_buffered_op_is_still_logged_broadcast_and_acked() {
    let (mut h, mut s) = joined(cid(1));
    let op = buffered_op(cid(1));

    let r = submit(&mut h, &mut s, vec![op.clone()]);

    assert!(is_accepted(&r), "a waiting op is acked");
    assert_eq!(r.broadcast, vec![op], "and fans out to peers");
    assert_eq!(h.seq(ROOM), 1, "and is retained for catch-up");
}

/// A duplicate also answers `false`, and is acked so the author can prune its
/// outbox — the case the `through`-over-the-whole-batch rule exists for.
#[test]
fn a_duplicate_is_still_acked() {
    let (mut h, mut s) = joined(cid(1));
    let op = honest_op(cid(1));
    submit(&mut h, &mut s, vec![op.clone()]);

    let r = submit(&mut h, &mut s, vec![op]);
    assert!(is_accepted(&r), "a resent op is acked so the outbox prunes");
    assert!(r.broadcast.is_empty(), "but nothing fans out twice");
    assert_eq!(h.seq(ROOM), 1, "and the log does not grow");
}

// --- the ingest seam itself, reached without a session ---

/// Node-to-node replication and store replay reach `Hub::ingest` directly, without
/// crossing the session's gates, so the seam holds the invariant on its own: the
/// retained log contains only ops the replica applied or is holding.
#[test]
fn the_ingest_seam_drops_a_refused_op() {
    let mut h = Hub::new(cid(0xFF));
    let good = honest_op(cid(1));
    // A distinct sequence, so the batch's own dedup is not what drops it.
    let mut bad = offset_stamp_op(cid(1));
    bad.id.seq = good.id.seq + 1;

    let applied = h
        .ingest(ROOM, vec![bad.clone(), good.clone()], None)
        .expect("a store-less ingest never fails");

    assert_eq!(
        applied,
        vec![good],
        "only the admissible op is committed and fanned out"
    );
    assert_eq!(h.seq(ROOM), 1, "the refused op is not retained");

    // And it left no dedup entry: the same id, correctly stamped, still lands.
    let mut resend = honest_op(cid(1));
    resend.id = bad.id;
    let applied = h
        .ingest(ROOM, vec![resend.clone()], None)
        .expect("a store-less ingest never fails");
    assert_eq!(
        applied,
        vec![resend],
        "the refused id was never entered in the dedup set"
    );
}

/// A refusal is a pure function of the op, so the predicate the server gates on is
/// the same judgement every replica reaches — the batch a peer refuses is the batch
/// this hub refuses, and state converges on its absence rather than diverging.
#[test]
fn the_refusal_is_a_pure_function_of_the_op() {
    let ops = [
        foreign_stamp_op(cid(1), cid(2)),
        offset_stamp_op(cid(1)),
        over_ceiling_op(cid(1)),
        zero_count_tx_op(cid(1)),
    ];
    for op in &ops {
        assert!(!op.is_admissible(), "refused: {:?}", op.id);
        // Two replicas at unrelated states reach the same verdict.
        let mut a = Document::new(cid(3));
        let mut b = Document::new(cid(4));
        b.transact(|tx| tx.register(b"seed", Scalar::Int(9)));
        assert!(!a.apply(op), "replica a applied a refused op");
        assert!(!b.apply(op), "replica b applied a refused op");
    }
    assert!(honest_op(cid(1)).is_admissible());
    assert!(
        buffered_op(cid(1)).is_admissible(),
        "a waiting op is admissible — it is held, not refused"
    );
}

/// `is_admissible` is a judgement on the op alone: an op already applied is still
/// admissible, so the predicate never confuses "already have it" with "never take
/// it".
#[test]
fn an_applied_op_stays_admissible() {
    let op = honest_op(cid(1));
    let mut d = Document::new(cid(2));
    assert!(d.apply(&op));
    assert!(!d.apply(&op), "the second fold is a duplicate");
    assert!(op.is_admissible(), "a duplicate is not a refusal");
}

/// A refused op is inadmissible whatever envelope carries it: `OpId` and `Tx` are
/// not part of the stamp judgement, so a peer cannot launder one through a
/// transaction.
#[test]
fn a_transaction_does_not_launder_a_refused_stamp() {
    let mut op = offset_stamp_op(cid(1));
    op.tx = Some(Tx {
        id: TxId(3),
        count: 2,
    });
    assert!(!op.is_admissible());

    let (mut h, mut s) = joined(cid(1));
    let r = submit(&mut h, &mut s, vec![op]);
    assert!(rejected_seqs(&r).is_some());
    assert_eq!(h.seq(ROOM), 0);
}

/// A refusal never depends on the op's payload size or kind: the same stamp defect
/// on a text run (whose reservation spans one id per codepoint) is refused for the
/// reservation's reach, not the base.
#[test]
fn a_run_is_judged_by_its_whole_reservation() {
    let client = cid(1);
    let mut d = Document::new(client);
    let mut ops = d.transact(|tx| tx.text(b"body").insert(0, "hello"));
    let mut run = ops.pop().expect("the insert is the last op");
    assert!(run.is_admissible());

    // The base fits, the reservation's last id does not.
    run.stamp.lamport = LAMPORT_STATE_CEILING;
    assert!(
        !run.is_admissible(),
        "a run reaching past the ceiling is refused"
    );
    assert!(matches!(run.kind, OpKind::TextInsert { .. }));

    // A one-id op based at exactly the ceiling still fits.
    let mut single = honest_op(client);
    single.stamp.lamport = LAMPORT_STATE_CEILING;
    assert!(single.is_admissible(), "the ceiling itself is a position");
}

/// The gate is not reachable only through a crafted `Op` struct: an op decoded off
/// the wire carries the same defect, so the codec is not a filter that makes this
/// unreachable.
#[test]
fn a_refused_op_survives_a_codec_round_trip() {
    use crdtsync_core::codec::{decode_ops, encode_ops};
    for op in [
        foreign_stamp_op(cid(1), cid(2)),
        offset_stamp_op(cid(1)),
        over_ceiling_op(cid(1)),
    ] {
        let decoded = decode_ops(&encode_ops(std::slice::from_ref(&op)))
            .expect("the codec admits it — only `apply` refuses it");
        assert_eq!(decoded, vec![op.clone()]);
        assert!(!decoded[0].is_admissible());
    }
}

/// An op id is not a stamp: an honest op keeps its author's sequence, and nothing
/// about `OpId` bears on admissibility.
#[test]
fn an_unrelated_sequence_is_admissible() {
    let mut op = honest_op(cid(1));
    op.id = OpId {
        client: op.stamp.client,
        seq: u64::MAX,
    };
    assert!(op.is_admissible());
    assert_eq!(
        op.stamp,
        Stamp {
            lamport: op.stamp.lamport,
            client: cid(1),
            offset: 0
        }
    );
}
