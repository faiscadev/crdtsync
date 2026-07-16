//! Producer-side reject-before-send (Schema Unit 9c) — the enforcing server's
//! op-ingress refusal of the one schema violation with no read-time repair: a
//! runtime-kind mismatch at a declared slot.
//!
//! The schema's violation set is closed — every declarable dimension (a bound, a
//! sequence length, a disallowed/mistyped attr, a disallowed/excess xml child, an
//! orphan inline) has a convergent read-time repair, so those are never rejected;
//! invariant repair (Unit 2) folds them away at read and every replica reads the
//! same value. A **kind mismatch** — a declared slot holding the wrong element kind
//! — is the one violation repair cannot normalize (a counter cannot be read as the
//! register its slot declares), so an enforcing-tier server refuses at ingress the
//! op that would unilaterally introduce one. The op never enters the log
//! (`OpsRejected` / `SchemaViolation`), the author keeps its ops, and every replica
//! converges on its absence.
//!
//! An **undeclared map slot** is *not* rejected — a Map is an open container, so an
//! untyped extra slot is admissible, not a violation to enforce. The enforcing
//! server's ingress is the authoritative, mandatory reject boundary: a malicious or
//! buggy client bypasses any client-side check, so the reject MUST be server-side. A
//! relay-tier connection carries no schema and never validates — it passes the same
//! op through unvalidated.

use std::sync::Mutex;

use crdtsync_core::doc::Document;
use crdtsync_core::protocol::Channel;
use crdtsync_core::repair::{repairs, RepairKind};
use crdtsync_core::validate::{validate, Step, ViolationKind};
use crdtsync_core::{ClientId, Element, ErrorCode, Message, Op, Scalar, Schema};
use crdtsync_server::auth::AllowAll;
use crdtsync_server::{step, Hub, PermitAll, SchemaRegistry, Session, Store};

const ROOM: &[u8] = b"room-1";
const CH: Channel = Channel(0);

/// A map root declaring `title` (a bounded register) and `hits` (a bounded
/// counter). A register op on `hits` (or a counter op on `title`) is a runtime-kind
/// mismatch (rejected); a register value over `max` is out-of-range (repairable by
/// clamp — accepted); a write to any other slot is an undeclared slot (admissible —
/// a Map is an open container).
const SCHEMA: &str = r#"{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "title": "Title", "hits": "Hits" } },
        "Title": { "kind": "register", "min": 0, "max": 280 },
        "Hits":  { "kind": "counter", "min": 0, "max": 100 }
    }
}"#;

fn schema() -> Schema {
    Schema::parse(SCHEMA).expect("schema parses")
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

// --- op builders: each authors a batch as `client`, against a fresh document ---
//
// The author must match the submitting session's client, and two ops from
// distinct clients never collide on an op id — so a batch and any state it is
// validated against are authored by different clients where both must coexist.

/// A valid write: an in-bounds register value on the declared `title` slot.
fn valid_op(client: ClientId) -> Vec<Op> {
    let mut d = Document::new(client);
    d.transact(|tx| tx.register(b"title", Scalar::Int(42)))
}

/// An admissible write: a slot the root type does not declare. A Map is an open
/// container, so this is not a violation to reject.
fn undeclared_slot_op(client: ClientId) -> Vec<Op> {
    let mut d = Document::new(client);
    d.transact(|tx| tx.register(b"extra", Scalar::Int(1)))
}

/// A kind mismatch: a counter on `title`, where the schema declares a register.
fn mistyped_title_op(client: ClientId) -> Vec<Op> {
    let mut d = Document::new(client);
    d.transact(|tx| tx.inc(b"title", 1))
}

/// A kind mismatch on an independent slot: a register on `hits`, where the schema
/// declares a counter. Used where the room already holds `title`, so this collides
/// with nothing.
fn mistyped_hits_op(client: ClientId) -> Vec<Op> {
    let mut d = Document::new(client);
    d.transact(|tx| tx.register(b"hits", Scalar::Int(1)))
}

/// A repairable write: a register value above its declared `max`. Read-time repair
/// clamps it to the ceiling — it is accepted, not rejected.
fn over_max_op(client: ClientId) -> Vec<Op> {
    let mut d = Document::new(client);
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)))
}

/// One batch that *heals* a standing mismatch on `hits` (writing a counter, its
/// declared kind) while *planting* a fresh one on `title` (a counter where a
/// register is declared). The two cancel in a bare count, so this is the batch a
/// count-only gate would wrongly admit — it must be rejected on the planted one.
fn heal_hits_and_mistype_title_op(client: ClientId) -> Vec<Op> {
    let mut d = Document::new(client);
    d.transact(|tx| {
        tx.inc(b"hits", 1);
        tx.inc(b"title", 1);
    })
}

// --- the wire path through the session ---

/// Drive one message with the dev verifier, permit-all deployment authorizer, and
/// the given schema tier — `Some` is an enforcing connection, `None` a relay.
fn st(
    h: &mut Hub,
    s: &mut Session,
    schema: Option<&Schema>,
    msg: Message,
) -> crdtsync_server::Response {
    step(
        h,
        s,
        &AllowAll,
        &PermitAll,
        schema,
        &Mutex::new(SchemaRegistry::new()),
        None,
        None,
        0,
        None,
        msg,
    )
}

fn handshake(h: &mut Hub, s: &mut Session, schema: Option<&Schema>, client: ClientId) {
    st(
        h,
        s,
        schema,
        Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    st(
        h,
        s,
        schema,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
    let r = st(
        h,
        s,
        schema,
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

fn ops_msg(ops: Vec<Op>) -> Message {
    Message::Ops { channel: CH, ops }
}

/// Whether the response carries an `OpsRejected` naming a schema violation.
fn is_schema_rejected(r: &crdtsync_server::Response) -> bool {
    r.replies.iter().any(|m| {
        matches!(
            m,
            Message::OpsRejected {
                reason: ErrorCode::SchemaViolation,
                ..
            }
        )
    })
}

fn is_accepted(r: &crdtsync_server::Response) -> bool {
    r.replies
        .iter()
        .any(|m| matches!(m, Message::Accepted { .. }))
}

fn key(s: &str) -> Step {
    Step::Key(s.as_bytes().to_vec())
}

/// The merged value of the `title` register, or `None` if the slot is absent or
/// not a register.
fn title_value(d: &Document) -> Option<Scalar> {
    match d.get(b"title")? {
        Element::Register(r) => Some(r.borrow().read().clone()),
        _ => None,
    }
}

// --- rejection of a kind mismatch at the enforcing tier ---

#[test]
fn an_enforcing_server_rejects_a_kind_mismatch_and_the_op_never_logs() {
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, Some(&sch), cid(1));
    let before = h.seq(ROOM);

    let r = st(
        &mut h,
        &mut s,
        Some(&sch),
        ops_msg(mistyped_title_op(cid(1))),
    );
    assert!(
        is_schema_rejected(&r),
        "the wrong-kind write is refused SchemaViolation"
    );
    assert!(!is_accepted(&r), "no acknowledgement is sent");
    assert_eq!(h.seq(ROOM), before, "the op never entered the log");
    assert!(
        h.get(ROOM, b"title").is_none(),
        "the mistyped slot did not materialize"
    );
}

// --- an undeclared slot is admitted, not rejected (a Map is an open container) ---

#[test]
fn an_undeclared_slot_is_admitted_not_rejected() {
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, Some(&sch), cid(1));
    let before = h.seq(ROOM);

    let r = st(
        &mut h,
        &mut s,
        Some(&sch),
        ops_msg(undeclared_slot_op(cid(1))),
    );
    assert!(
        !is_schema_rejected(&r),
        "an undeclared slot is admissible, not a rejection"
    );
    assert!(is_accepted(&r), "the write is acknowledged");
    assert_eq!(h.seq(ROOM), before + 1, "the op entered the log");
    assert!(
        h.get(ROOM, b"extra").is_some(),
        "the undeclared slot is present in the merged state"
    );
}

// --- the producer sees the rejection (its ops are named back) ---

#[test]
fn the_rejection_names_the_authors_op_seqs() {
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, Some(&sch), cid(1));

    let ops = mistyped_title_op(cid(1));
    let seqs: Vec<u64> = ops.iter().map(|op| op.id.seq).collect();
    let r = st(&mut h, &mut s, Some(&sch), ops_msg(ops));
    let named = r.replies.iter().find_map(|m| match m {
        Message::OpsRejected {
            channel,
            seqs,
            reason: ErrorCode::SchemaViolation,
        } => Some((*channel, seqs.clone())),
        _ => None,
    });
    assert_eq!(
        named,
        Some((CH, seqs)),
        "the producer is handed back its own op seqs on the submitting channel"
    );
}

// --- regression: a repairable violation is NOT turned into a rejection ---

#[test]
fn a_repairable_over_max_write_is_accepted_and_repaired_at_read() {
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, Some(&sch), cid(1));
    let before = h.seq(ROOM);

    let r = st(&mut h, &mut s, Some(&sch), ops_msg(over_max_op(cid(1))));
    assert!(
        !is_schema_rejected(&r),
        "an over-max value is repairable — not rejected"
    );
    assert!(is_accepted(&r), "the write is acknowledged");
    assert_eq!(h.seq(ROOM), before + 1, "the op entered the log");

    // It genuinely IS a violation — just a repairable one, folded at read: the
    // stored value is 999, validate reports AboveMax, and repair clamps to 280.
    let doc =
        Document::decode_state(&h.export_room(ROOM).expect("room exists")).expect("state decodes");
    let violations = validate(&doc, &sch);
    assert!(
        violations
            .iter()
            .any(|v| v.path == [key("title")] && matches!(v.kind, ViolationKind::AboveMax { .. })),
        "the stored over-max value is a live AboveMax violation"
    );
    assert!(
        violations.iter().all(|v| !v.kind.rejects_at_ingress()),
        "no ingress-rejectable violation reached the committed log"
    );
    let repaired = repairs(&doc, &sch);
    assert!(
        repaired
            .iter()
            .any(|rep| rep.path == [key("title")]
                && rep.kind == RepairKind::Clamped { value: 280 }),
        "a conformant read clamps the value to the ceiling"
    );
}

#[test]
fn a_valid_write_is_unaffected() {
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, Some(&sch), cid(1));
    let before = h.seq(ROOM);

    let r = st(&mut h, &mut s, Some(&sch), ops_msg(valid_op(cid(1))));
    assert!(
        !is_schema_rejected(&r),
        "a conforming write is not rejected"
    );
    assert!(is_accepted(&r), "the write is acknowledged");
    assert_eq!(h.seq(ROOM), before + 1, "the op entered the log");
    let doc =
        Document::decode_state(&h.export_room(ROOM).expect("room exists")).expect("state decodes");
    assert!(validate(&doc, &sch).is_empty(), "the state is conforming");
}

// --- the relay tier passes the same op through unvalidated ---

#[test]
fn a_relay_tier_passes_a_mistyped_op_through_unvalidated() {
    // A relay connection carries no schema (`None`), so no validation runs — the
    // kind-mismatch op the enforcing tier refuses is accepted and logged here.
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, None, cid(1));
    let before = h.seq(ROOM);

    let r = st(&mut h, &mut s, None, ops_msg(mistyped_title_op(cid(1))));
    assert!(!is_schema_rejected(&r), "a relay never rejects on schema");
    assert!(is_accepted(&r), "the relay logs the op");
    assert_eq!(h.seq(ROOM), before + 1, "the op entered the relay's log");
    assert!(
        h.get(ROOM, b"title").is_some(),
        "the mistyped slot is present in the relay's state"
    );
}

// --- convergence: a rejected op never entered any replica's log ---

#[test]
fn after_a_reject_the_accepting_replicas_converge_without_the_rejected_op() {
    // Producer A submits a valid write (accepted, broadcast). Producer B submits a
    // kind-mismatch write on an independent slot (rejected, never broadcast). A
    // second replica that folds only what was broadcast converges with the
    // authoritative room on the valid op alone — the rejected op is absent
    // everywhere.
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));

    let mut a = Session::new();
    handshake(&mut h, &mut a, Some(&sch), cid(1));
    let mut b = Session::new();
    handshake(&mut h, &mut b, Some(&sch), cid(2));

    // A's valid write commits and fans out.
    let ra = st(&mut h, &mut a, Some(&sch), ops_msg(valid_op(cid(1))));
    assert!(is_accepted(&ra), "A's valid write is accepted");
    let broadcast = ra.broadcast.clone();
    assert!(!broadcast.is_empty(), "the valid write is broadcast");

    // B's kind-mismatch write — a register on the counter slot `hits`, independent
    // of A's `title` — is rejected and nothing is broadcast.
    let rb = st(
        &mut h,
        &mut b,
        Some(&sch),
        ops_msg(mistyped_hits_op(cid(2))),
    );
    assert!(is_schema_rejected(&rb), "B's write is rejected");
    assert!(
        rb.broadcast.is_empty(),
        "a rejected op is never broadcast to any peer"
    );

    // A downstream replica folds only what was broadcast.
    let mut replica = Document::new(cid(9));
    for op in &broadcast {
        replica.apply(op);
    }
    let authoritative =
        Document::decode_state(&h.export_room(ROOM).expect("room exists")).expect("state decodes");

    // Both hold the valid op and neither holds the rejected one: the merged content
    // converges (the raw snapshot embeds each replica's own id, so convergence is a
    // content equality, not a byte one).
    assert_eq!(
        title_value(&replica),
        Some(Scalar::Int(42)),
        "the downstream replica folded the valid op"
    );
    assert_eq!(
        title_value(&replica),
        title_value(&authoritative),
        "the accepting replicas converge on the valid op alone"
    );
    assert!(
        replica.get(b"hits").is_none() && authoritative.get(b"hits").is_none(),
        "the rejected op is absent from every replica"
    );
    assert!(
        validate(&authoritative, &sch).is_empty(),
        "the converged state is conforming — the rejected op never entered"
    );
}

// --- a pre-existing mismatch never wedges an unrelated write (count exemption) ---

#[test]
fn a_pre_existing_mismatch_does_not_wedge_an_unrelated_valid_write() {
    // A mismatch enters via an untagged (relay-like) ingest that bypasses the gate,
    // so the room stands with one committed mismatch. An unrelated valid write to a
    // different slot must not be refused on account of it — the gate refuses only a
    // batch that *raises* the mismatch count, never one that leaves it standing.
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    h.ingest(ROOM, mistyped_hits_op(cid(1)), None)
        .expect("seed a pre-existing mismatch");

    assert!(
        !h.batch_violates_schema(ROOM, &valid_op(cid(2)), &sch),
        "an unrelated valid write is not wedged by the pre-existing mismatch"
    );
}

#[test]
fn a_second_independent_mismatch_is_still_caught_when_one_already_stands() {
    // With one mismatch already standing, a batch that introduces a *second*,
    // independent one raises the count and is refused — the exemption is for the
    // pre-existing violation only, not a blanket pass once any mismatch exists.
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    h.ingest(ROOM, mistyped_hits_op(cid(1)), None)
        .expect("seed a pre-existing mismatch");

    assert!(
        h.batch_violates_schema(ROOM, &mistyped_title_op(cid(2)), &sch),
        "a fresh, independent mismatch is still refused"
    );
}

#[test]
fn healing_one_mismatch_does_not_pay_for_planting_another() {
    // With one mismatch standing on `hits`, a batch that heals it (writes the
    // declared counter kind) while planting a fresh mismatch on the clean `title`
    // slot leaves the total count unchanged — but a location diff still catches the
    // planted one. A bare-count gate would net 1→1 and admit it; the gate must not.
    let sch = schema();
    let mut h = Hub::new(cid(0xFF));
    h.ingest(ROOM, mistyped_hits_op(cid(1)), None)
        .expect("seed a pre-existing mismatch on hits");

    assert!(
        h.batch_violates_schema(ROOM, &heal_hits_and_mistype_title_op(cid(2)), &sch),
        "planting a fresh mismatch is refused even while healing a standing one"
    );
}

// --- durability: the refusal survives a store replay reopen ---

#[test]
#[cfg_attr(miri, ignore)] // touches the filesystem store
fn the_refusal_survives_a_store_replay_reopen() {
    let dir = std::env::temp_dir().join(format!("cs-schema-reject-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // Seed a valid write through a store-backed hub, then drop it.
    {
        let store = Store::open(&dir).expect("store opens");
        let mut h = Hub::from_rooms(cid(0xFF), Vec::new()).expect("empty hub");
        h.attach_store(store);
        h.ingest(ROOM, valid_op(cid(1)), None)
            .expect("in-memory ingest");
    }

    // Reopen from the store: the tail replays through the shared commit path, so
    // the room's document is rebuilt and enforcement stands against it — a
    // kind-mismatch op is still refused, a repairable over-max op is not.
    let store = Store::open(&dir).expect("store reopens");
    let rooms = store.load().expect("store loads");
    let h = Hub::from_rooms(cid(0xFF), rooms).expect("hub rebuilt");
    assert!(
        h.batch_violates_schema(ROOM, &mistyped_hits_op(cid(2)), &schema()),
        "the reopened room still refuses the kind-mismatch op"
    );
    assert!(
        !h.batch_violates_schema(ROOM, &over_max_op(cid(2)), &schema()),
        "a repairable op is still not refused after reopen"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
