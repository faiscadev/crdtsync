//! An op no replica can hold is refused at ingress, never logged or acknowledged.
//!
//! `Document::apply` answers `false` for three unrelated situations, and only one
//! of them is permanent. An op already applied or already held is a **duplicate**.
//! An op whose target is not reachable yet, or whose atomic-transaction group is
//! incomplete, is **waiting** — a later arrival commits it. An op whose stamp names
//! a client other than its author, whose stamp sits outside the position an id may
//! occupy, or which declares a transaction size no group can have, is **refused
//! forever**: that set is [`Op::is_admissible`]'s complement, a pure function of the
//! op, so every replica refuses exactly the same ops and the room converges on their
//! absence rather than splitting over them.
//!
//! Only the permanent set may be dropped, and the distinction is what the ingest
//! path turns on. A refused op that reached the log would be durable, entered in the
//! room's dedup set — swallowing the author's corrected resend under the same
//! `OpId` forever — fanned out to every peer, replayed on each reload, and acked
//! `Accepted`, because the ack frontier is a max over the whole submitted batch. So
//! the session refuses the batch recoverably (`OpsRejected` / `MalformedOp`), the
//! author keeps its ops, and the ingest seams drop such a record before persisting
//! it — the seams matter on their own, since a peer's `Replicate` frame reaches them
//! without crossing the session.
//!
//! A waiting op is not refused: it is logged, fanned out and acked as it lands,
//! because it is state its group or its create completes.

use std::sync::Mutex;

use crdtsync_core::doc::Document;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    ClientId, ErrorCode, Message, Op, OpId, OpKind, Scalar, Stamp, Tx, TxId, LAMPORT_STATE_CEILING,
};
use crdtsync_server::auth::AllowAll;
use crdtsync_server::replay::{head_seq, reconstruct_at};
use crdtsync_server::store::{RoomLog, Store, StoredOp};
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

/// A container create and the write into it, as `(create, write)`. Submitted alone
/// the write is admissible but not applicable yet, so the replica holds it; the
/// create releases it.
fn buffered_pair(client: ClientId) -> (Op, Op) {
    let mut d = Document::new(client);
    let mut ops = d.transact(|tx| tx.map(b"nested").register(b"k", Scalar::Int(1)));
    let write = ops.pop().expect("the nested write is the last op");
    let create = ops.pop().expect("the container create is the first op");
    assert!(ops.is_empty(), "a nested register write is exactly two ops");
    (create, write)
}

/// The dependent half of [`buffered_pair`] — held when it arrives on its own.
fn buffered_op(client: ClientId) -> Op {
    buffered_pair(client).1
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
    accepted_through(r).is_some()
}

/// The frontier an `Accepted` acknowledges — the sequence the author prunes its
/// outbox through, and so the value that decides whether a refused op was reported
/// as landed.
fn accepted_through(r: &crdtsync_server::Response) -> Option<u64> {
    r.replies.iter().find_map(|m| match m {
        Message::Accepted { through, .. } => Some(*through),
        _ => None,
    })
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

/// The whole rule in one case: a refused op is acked by nothing, logged by nothing,
/// broadcast to nobody, and leaves the room exactly as it was.
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

/// Each permanent refusal is reachable over the wire — no earlier gate in
/// `handle_ops` catches any of them, so each depends on this one.
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
    // Distinct sequences, so the rejection names the admissible op's own frontier
    // rather than collapsing onto the refused one's.
    let mut bad = offset_stamp_op(cid(1));
    bad.id.seq = good.id.seq + 1;
    assert_ne!(good.id.seq, bad.id.seq);

    let r = submit(&mut h, &mut s, vec![good.clone(), bad.clone()]);

    assert!(!is_accepted(&r), "the admissible half is not acked either");
    assert_eq!(
        rejected_seqs(&r),
        Some(vec![good.id.seq, bad.id.seq]),
        "the author keeps the whole batch"
    );
    assert_eq!(h.seq(ROOM), 0, "neither op lands");
    assert!(h.get(ROOM, b"title").is_none(), "and no state does");

    // Resubmitted without the refused op, the same admissible op is accepted — the
    // rejection was the batch's company, not anything about this op.
    let r = submit(&mut h, &mut s, vec![good.clone()]);
    assert_eq!(accepted_through(&r), Some(good.id.seq));
    assert_eq!(h.seq(ROOM), 1);
}

// --- the dedup set is not poisoned ---

/// The durable half of the rule: a refused op leaves no entry in the room's seen
/// set, so the author's corrected resend under the same `OpId` still lands rather
/// than being swallowed as a duplicate — forever, and across a reload.
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

/// A buffered op also answers `false` from `apply`, and is logged, broadcast and
/// acked all the same: it is state a later op completes, not a refusal.
#[test]
fn a_buffered_op_is_still_logged_broadcast_and_acked() {
    let (mut h, mut s) = joined(cid(1));
    let (create, write) = buffered_pair(cid(1));

    // The dependent write alone: admissible, so logged, fanned out and acked — but
    // held rather than applied, since its container does not exist yet.
    let r = submit(&mut h, &mut s, vec![write.clone()]);
    assert_eq!(
        accepted_through(&r),
        Some(write.id.seq),
        "a waiting op is acked"
    );
    assert_eq!(r.broadcast, vec![write], "and fans out to peers");
    assert_eq!(h.seq(ROOM), 1, "and is retained for catch-up");
    assert!(
        h.get(ROOM, b"nested").is_none(),
        "but nothing of it has landed — it is waiting, not applied"
    );

    // The create it waits on releases it, which is what makes holding it correct.
    let r = submit(&mut h, &mut s, vec![create.clone()]);
    assert_eq!(accepted_through(&r), Some(create.id.seq));
    assert_eq!(h.seq(ROOM), 2);
    assert!(
        h.get(ROOM, b"nested").is_some(),
        "the held write applied once its container arrived"
    );
}

/// The read-only replay tooling numbers a room's sequences from the same tail the
/// hub commits, so it must apply the same filter. It reads bytes this node did not
/// necessarily write — a store handed over, or one a crash left mid-write — where a
/// record the ingest seam would have dropped can appear. Reading such a record as
/// sequence-advancing would slide every later op's sequence by one against the live
/// hub, and point-in-time reconstruction would answer for the wrong ops.
#[test]
fn replay_numbers_sequences_past_a_refused_record() {
    let good_one = honest_op(cid(1));
    let mut refused = offset_stamp_op(cid(1));
    refused.id.seq = 1;
    let mut good_two = honest_op(cid(1));
    good_two.id.seq = 2;
    good_two.stamp.lamport += 1;

    // The order matters: the refused record sits between the two admissible ones, so
    // an unfiltered tail would number `good_two` at 3 rather than 2.
    let log = RoomLog {
        ops: vec![&good_one, &refused, &good_two]
            .into_iter()
            .map(|op| StoredOp::new(op.clone(), None))
            .collect(),
        ..RoomLog::default()
    };
    assert_eq!(head_seq(&log), 2, "the refused record advances nothing");

    // And the sequences agree with a live hub fed the identical batch.
    let mut h = Hub::new(cid(0xFF));
    h.ingest(
        ROOM,
        vec![good_one.clone(), refused, good_two.clone()],
        None,
    )
    .expect("a store-less ingest never fails");
    assert_eq!(h.seq(ROOM), head_seq(&log));

    let at_head =
        reconstruct_at(&log, ROOM, head_seq(&log), cid(0xFF)).expect("the head is reconstructable");
    assert_eq!(
        at_head.state,
        h.export_room(ROOM).expect("the room exists"),
        "reconstruction matches the live replica byte for byte"
    );
}

/// The other waiting case: a transaction member whose group is incomplete. It is
/// held rather than applied, so `apply` answers `false` for it too — but the group
/// commits the moment its last member arrives, so every member must be logged and
/// acked as it lands. Refusing a member because it does not apply yet would strand
/// its whole group forever.
#[test]
fn an_incomplete_transaction_member_is_still_logged_broadcast_and_acked() {
    let (mut h, mut s) = joined(cid(1));
    let mut d = Document::new(cid(1));
    let mut members = d.atomic_transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
    });
    assert_eq!(members.len(), 2, "a two-member group");
    assert!(
        members.iter().all(|op| op.tx.is_some_and(|t| t.count == 2)),
        "both members declare the group's size"
    );

    let second = members.pop().expect("two members");
    let first = members.pop().expect("two members");

    // The first member alone: held, because its partner has not arrived.
    let r = submit(&mut h, &mut s, vec![first.clone()]);
    assert!(is_accepted(&r), "a held member is acked");
    assert_eq!(r.broadcast, vec![first], "and fans out to peers");
    assert_eq!(h.seq(ROOM), 1, "and is retained for catch-up");
    assert!(
        h.get(ROOM, b"a").is_none(),
        "but the group has not committed"
    );

    // The partner completes the group, and both writes land at once.
    let r = submit(&mut h, &mut s, vec![second]);
    assert!(is_accepted(&r));
    assert_eq!(h.seq(ROOM), 2);
    assert!(h.get(ROOM, b"a").is_some(), "the group committed");
    assert!(h.get(ROOM, b"b").is_some(), "both members, together");
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

/// A branch tail is folded into a document only when the branch is materialized, so
/// a refused op admitted there would sit durable and undetected until the fold and
/// then be dropped — the same land-nowhere write, deferred.
#[test]
fn a_branch_tail_never_holds_a_refused_op() {
    let mut h = Hub::new(cid(0xFF));
    h.ingest(ROOM, vec![honest_op(cid(1))], None)
        .expect("a store-less ingest never fails");
    assert!(h
        .fork_branch(ROOM, b"feature", b"main", 1)
        .expect("a store-less fork never fails"));

    let good = honest_op(cid(2));
    let mut bad = offset_stamp_op(cid(2));
    bad.id.seq = good.id.seq + 1;

    let applied = h
        .ingest_branch(ROOM, b"feature", vec![bad.clone(), good.clone()], None)
        .expect("a store-less ingest never fails");

    // The positive control: the admissible op in the same batch does append, so the
    // branch write path is exercised rather than merely absent.
    assert_eq!(applied, vec![good], "only the refused op is dropped");
    assert_eq!(
        h.branch(ROOM, b"feature").map(|b| b.head),
        Some(2),
        "the branch head advances by exactly the appended op"
    );

    // And the refused op left no entry in the tail's own dedup set.
    let mut resend = honest_op(cid(2));
    resend.id = bad.id;
    let applied = h
        .ingest_branch(ROOM, b"feature", vec![resend.clone()], None)
        .expect("a store-less ingest never fails");
    assert_eq!(applied, vec![resend], "the refused id was never deduped");
}

/// The durable half: a refused op never reaches the store, so a reload does not
/// replay it back into the log it was kept out of.
#[test]
#[cfg_attr(miri, ignore = "real filesystem I/O, which Miri does not model")]
fn a_refused_op_never_reaches_the_store() {
    let tmp = tempdir();
    let dir = tmp.0.as_path();

    let good = honest_op(cid(1));
    let mut bad = offset_stamp_op(cid(1));
    bad.id.seq = good.id.seq + 1;
    {
        let mut h = Hub::new(cid(0xFF));
        h.attach_store(Store::open(&dir).expect("the store opens"));
        h.ingest(ROOM, vec![bad.clone(), good.clone()], None)
            .expect("the ingest persists");
    }

    let h = Hub::from_rooms(
        cid(0xFF),
        Store::open(&dir).expect("the store opens").load().unwrap(),
    )
    .expect("the reload succeeds");
    assert_eq!(h.seq(ROOM), 1, "only the admissible op was persisted");
    assert!(h.get(ROOM, b"title").is_some(), "and it replays into state");
}

/// A scratch directory that removes itself, so a failing assertion leaves nothing
/// behind. The name carries the pid, and there is one per process.
struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    let dir = std::env::temp_dir().join(format!("crdtsync-refused-op-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the temp dir is creatable");
    TempDir(dir)
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
