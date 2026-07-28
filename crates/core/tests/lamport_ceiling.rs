//! The lamport ceiling — a foreign op cannot move a partition clock into the
//! range where minting the next local edit overflows.
//!
//! A partition clock is a Lamport clock: `apply` raises it to the last lamport a
//! folded op reserves, and `emit` stamps the next local edit one above it. The
//! op's lamport is a bare `u64` off the wire and nothing bounded it, so one op
//! carrying `lamport: u64::MAX` — from any peer, on the live path, with no id
//! guessing and no forged authorship — overflowed the very next ordinary local
//! edit: a panic in debug, a wrap to 0 in release.
//!
//! **Two different questions, two constants.** What a folded op may do to a
//! *clock* is bounded by [`LAMPORT_WIRE_CEILING`]; where a *stamp* may sit at all
//! is bounded by [`LAMPORT_STATE_CEILING`]. Both are constants, so each is a pure
//! function of the input and every replica decides identically — clocks stay
//! convergent, which a relative bound (`clock + k`) could not promise.
//!
//! * **The clock bound is a clamp, and the op is still applied.** Refusing an op
//!   for where it puts a clock buys nothing: a peer that wants to dominate LWW
//!   simply sits one below whatever the gate is, while a gate at the value the
//!   mint tops out at lets one admissible op park a replica's clock exactly where
//!   its next mint would be refused by everyone else — divergence traded for a
//!   panic. So a stamp anywhere in `(LAMPORT_WIRE_CEILING, LAMPORT_STATE_CEILING]`
//!   is folded, with the clock clamped under it, and the gap between the two
//!   constants is the runway a clock-parked replica keeps minting into.
//! * **The id-space bound is a refusal, and it is where a stamp may exist.** A
//!   replica stores the highest lamport each client's stamps reach, so a stamp is
//!   an id and the record of it has to stay inside what a decode admits; clamping
//!   the record instead hands live ids back to the mint. So a stamp whose
//!   *reservation* reaches past [`LAMPORT_STATE_CEILING`] names no id and is
//!   refused — on the wire, on the decode path, and at the local mint alike. One
//!   constant for all three is what keeps it convergent rather than divergent: a
//!   replica emits exactly the set its peers admit, so an honest mint is never
//!   refused by the network. That is a deliberate departure from applying every
//!   op regardless, and it is bounded to the region no honest replica reaches.
//! * **Exhaustion is a refused edit**, never a panic, a wrap, or a re-issued id.
//!   A snapshot may legally declare a clock at the very ceiling, and a replica
//!   that adopts one has no id left to mint: [`Document::can_mint`] answers false
//!   and the edit emits nothing.
//! * Neither bound sits at `u64::MAX`, and that is the whole point. Saturating at
//!   the top of the space is what makes a pinned clock re-mint one stamp forever —
//!   and a sequence node's id *is* its stamp, so that is C4's collision reached
//!   from the other side.
//!
//! The reservation, not just the base, is what both bounds read: a text run takes
//! one lamport per codepoint, so bounding `stamp.lamport` alone would let a long
//! run carry the clock past the ceiling anyway — a point check on a range problem.
//! And the clamp is per-partition, so a zone clock is bounded by the same constant
//! as the root's, through the same fold.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::schema::Schema;
use crdtsync_core::stamp::{LAMPORT_STATE_CEILING, LAMPORT_WIRE_CEILING};
use crdtsync_core::{ClientId, Element, OpKind, Scalar, Stamp};

mod common;
use common::{cid, with_only_zone_clock, with_root_clock};

/// A one-op batch authored by `client`, re-stamped to carry `lamport`. The op is
/// otherwise ordinary and applicable — only its causal position is forged.
fn op_at_lamport(client: ClientId, key: &[u8], lamport: u64) -> Op {
    let mut op = Document::new(client)
        .transact(|tx| tx.set(key, Scalar::Bytes(key.to_vec())))
        .remove(0);
    op.stamp.lamport = lamport;
    op
}

/// A text-insert op of `s` into a fresh `key` text, re-stamped at `lamport`. Two
/// ops: the create, then the run — the run is the one that reserves a span.
fn text_run_at_lamport(client: ClientId, key: &[u8], s: &str, lamport: u64) -> Vec<Op> {
    let mut ops = Document::new(client).transact(|tx| tx.text(key).insert(0, s));
    for op in ops.iter_mut() {
        if matches!(op.kind, OpKind::TextInsert { .. }) {
            op.stamp.lamport = lamport;
        }
    }
    ops
}

/// The lamport the next local edit on the root partition is stamped with. The
/// probe *mints* rather than reading `zone_clock`, because minting is where the
/// overflow was: `emit_stamped`'s `clock + 1` is the panic site.
fn next_root_lamport(doc: &mut Document) -> u64 {
    doc.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0]
        .stamp
        .lamport
}

/// The same probe inside zone 0.
fn next_zone_lamport(doc: &mut Document) -> u64 {
    doc.transact(|tx| {
        tx.map(b"board").set(b"probe", Scalar::Int(1));
    })
    .iter()
    .find(|op| op.zone == Some(0))
    .expect("a zoned op")
    .stamp
    .lamport
}

/// A hostile batch writing into zone 0, every op re-stamped at the highest
/// position a stamp may occupy — the zoned counterpart of [`op_at_lamport`].
fn zoned_ceiling_ops(client: ClientId, key: &[u8]) -> Vec<Op> {
    let mut peer = Document::new(client);
    peer.set_schema(schema_with_zone());
    let mut ops = peer.transact(|tx| {
        tx.map(b"board").set(key, Scalar::Int(1));
    });
    for op in ops.iter_mut() {
        assert_eq!(op.zone, Some(0), "the whole batch belongs to the zone");
        op.stamp.lamport = LAMPORT_STATE_CEILING;
    }
    ops
}

/// A replica bound to the zone schema, with `ops` folded in the given order.
fn zoned_doc(client: ClientId, ops: &[Op]) -> Document {
    let mut doc = Document::new(client);
    doc.set_schema(schema_with_zone());
    for op in ops {
        doc.apply(op);
    }
    doc
}

/// The node ids of a text's first `count` characters.
fn text_ids(doc: &Document, key: &[u8], count: usize) -> Vec<Stamp> {
    let Some(Element::Text(t)) = doc.get(key) else {
        panic!("text materialised");
    };
    let ids = t.borrow().node_ids(0, count);
    ids
}

/// A text's rendered content.
fn text_of(doc: &Document, key: &[u8]) -> String {
    let Some(Element::Text(t)) = doc.get(key) else {
        panic!("text materialised");
    };
    let s = t.borrow().as_string();
    s
}

/// A stable rendering of every key these tests write — the equality oracle for
/// convergence. Reads through `get`, so it reflects the materialised tree.
fn fingerprint(doc: &Document) -> String {
    [
        b"a".as_slice(),
        b"b",
        b"x",
        b"y",
        b"k",
        b"j",
        b"board",
        b"t",
    ]
    .iter()
    .map(|k| match doc.get(k) {
        None => "_".to_string(),
        Some(Element::Scalar(v)) => format!("S{v:?}"),
        Some(Element::Register(r)) => format!("R{:?}", r.borrow().read()),
        Some(Element::Text(t)) => format!("T{:?}", t.borrow().as_string()),
        Some(Element::Map(m)) => {
            let m = m.borrow();
            let mut keys: Vec<String> = m
                .keys()
                .iter()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect();
            keys.sort();
            format!("M{keys:?}")
        }
        Some(_) => "?".to_string(),
    })
    .collect::<Vec<_>>()
    .join(";")
}

fn schema_with_zone() -> Schema {
    Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
             "types": { "R": { "kind": "map" } },
             "zones": { "board": "/board" } }"#,
    )
    .expect("schema parses")
}

// ---------------------------------------------------------------------------
// The reported primitive.
// ---------------------------------------------------------------------------

#[test]
fn a_peer_op_at_the_top_of_the_space_does_not_overflow_the_next_local_edit() {
    // The reported primitive, reached from the highest position an op may still
    // occupy. Debug builds panicked on `clock + 1`; release wrapped to 0.
    let mut doc = Document::new(cid(1));
    assert!(doc.apply(&op_at_lamport(cid(2), b"k", LAMPORT_STATE_CEILING)));

    let lamport = next_root_lamport(&mut doc);
    assert_eq!(lamport, LAMPORT_WIRE_CEILING + 1);
}

#[test]
fn a_peer_op_at_the_top_of_the_space_does_not_wrap_the_next_local_edit() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_lamport(cid(2), b"k", LAMPORT_STATE_CEILING));
    assert_ne!(next_root_lamport(&mut doc), 0);
}

#[test]
fn an_op_stamped_past_the_id_space_is_refused_and_moves_nothing() {
    // Past the end of the id space there is nothing to record: the replica stores
    // the highest lamport each client's stamps reach, and a record above the
    // ceiling is one its own decoder would refuse. So the op is refused rather
    // than folded-and-clamped — the one place the fold's apply-anyway rule stops.
    let mut doc = Document::new(cid(1));
    assert!(!doc.apply(&op_at_lamport(cid(2), b"k", u64::MAX)));
    assert!(doc.get(b"k").is_none(), "nothing of it landed");
    assert_eq!(doc.zone_clock(None), 0, "it moved no clock either");
    assert_eq!(next_root_lamport(&mut doc), 1, "the mint is untouched");
}

#[test]
fn local_mints_after_a_ceiling_op_stay_strictly_increasing() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_lamport(cid(2), b"k", u64::MAX));

    // A clock saturated at the top of the space would re-issue one lamport
    // forever. The clamp sits below the top, so the mint still climbs.
    let mut seen = Vec::new();
    for _ in 0..8 {
        seen.push(next_root_lamport(&mut doc));
    }
    for pair in seen.windows(2) {
        assert!(pair[1] > pair[0], "mint did not advance: {seen:?}");
    }
}

#[test]
fn a_ceiling_op_does_not_make_the_replica_mint_a_colliding_node_id() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_lamport(cid(2), b"k", u64::MAX));

    // A sequence node's id *is* the stamp its op was minted at, so a pinned
    // clock shows up here as two characters sharing one id.
    doc.transact(|tx| tx.text(b"t").insert(0, "a"));
    for i in 1..32 {
        doc.transact(|tx| tx.text(b"t").insert(i, "a"));
    }
    let Some(Element::Text(t)) = doc.get(b"t") else {
        panic!("text materialised");
    };
    let ids = t.borrow().node_ids(0, 32);
    assert_eq!(ids.len(), 32);
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 32, "minted a duplicate node id: {ids:?}");
    assert_eq!(t.borrow().as_string().chars().count(), 32);
}

// ---------------------------------------------------------------------------
// Zone clocks — the same fold, per partition.
// ---------------------------------------------------------------------------

#[test]
fn a_ceiling_op_in_a_zone_does_not_overflow_the_next_local_edit_in_that_zone() {
    let mut doc = zoned_doc(cid(1), &zoned_ceiling_ops(cid(2), b"k"));
    assert_eq!(next_zone_lamport(&mut doc), LAMPORT_WIRE_CEILING + 1);
}

#[test]
fn a_ceiling_op_in_a_zone_leaves_the_root_clock_alone() {
    // The clamp is per-partition because the fold is: a zoned op never touches
    // the root clock, clamped or not.
    let mut doc = zoned_doc(cid(1), &zoned_ceiling_ops(cid(2), b"k"));
    assert_eq!(next_root_lamport(&mut doc), 1);
}

// ---------------------------------------------------------------------------
// Spans — the reservation is what is bounded, not the base.
// ---------------------------------------------------------------------------

#[test]
fn a_text_run_based_below_the_ceiling_cannot_carry_the_clock_over_it() {
    let mut doc = Document::new(cid(1));
    // A base a handful under the ceiling with a run long enough to cross it.
    let base = LAMPORT_WIRE_CEILING - 3;
    for op in &text_run_at_lamport(cid(2), b"t", "abcdefghij", base) {
        doc.apply(op);
    }
    // Bounding `stamp.lamport` alone would leave the clock at base + 9.
    assert_eq!(next_root_lamport(&mut doc), LAMPORT_WIRE_CEILING + 1);
}

#[test]
fn a_text_run_at_the_top_of_the_space_does_not_overflow_the_next_local_edit() {
    // Based so the tenth codepoint lands exactly on the ceiling — the furthest a
    // run can reach and still name ten ids that exist.
    let mut doc = Document::new(cid(1));
    for op in &text_run_at_lamport(cid(2), b"t", "abcdefghij", LAMPORT_STATE_CEILING - 9) {
        assert!(doc.apply(op));
    }
    assert_eq!(next_root_lamport(&mut doc), LAMPORT_WIRE_CEILING + 1);
}

#[test]
fn a_text_run_reaching_past_the_id_space_is_refused_though_its_base_fits() {
    // The gate reads the reservation, not the base — the same range-versus-point
    // distinction the clock clamp reads it on. A base one inside the ceiling with
    // ten codepoints names nine ids that do not exist, so the run is refused whole
    // rather than saturated onto the last one.
    let mut doc = Document::new(cid(1));
    let ops = text_run_at_lamport(cid(2), b"t", "abcdefghij", LAMPORT_STATE_CEILING - 8);
    for op in &ops {
        if matches!(op.kind, OpKind::TextInsert { .. }) {
            assert!(!doc.apply(op), "a run past the end of the space is refused");
        } else {
            assert!(doc.apply(op));
        }
    }
    let Some(Element::Text(t)) = doc.get(b"t") else {
        panic!("the create still landed");
    };
    assert_eq!(t.borrow().len(), 0, "no codepoint of the run landed");
}

#[test]
fn a_local_text_run_reserves_its_whole_span_after_a_ceiling_op() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_lamport(cid(2), b"k", u64::MAX));

    let ops = doc.transact(|tx| tx.text(b"t").insert(0, "hello"));
    let run = ops
        .iter()
        .find(|op| matches!(op.kind, OpKind::TextInsert { .. }))
        .expect("a run op");
    // Five codepoints take five lamports; the next mint clears all of them.
    assert_eq!(next_root_lamport(&mut doc), run.stamp.lamport + 5);
}

// ---------------------------------------------------------------------------
// A genuinely large lamport is still a legitimate lamport.
// ---------------------------------------------------------------------------

#[test]
fn a_large_but_legitimate_lamport_is_folded_unclamped() {
    for lamport in [
        1u64 << 20,
        1 << 40,
        LAMPORT_WIRE_CEILING - 1,
        LAMPORT_WIRE_CEILING,
    ] {
        let mut doc = Document::new(cid(1));
        assert!(doc.apply(&op_at_lamport(cid(2), b"k", lamport)));
        assert_eq!(next_root_lamport(&mut doc), lamport + 1);
    }
}

#[test]
fn an_over_ceiling_op_is_still_applied() {
    // Every position between the two constants keeps the original rule: the clock
    // is clamped, the op itself is folded. This is the range the gate deliberately
    // does not reach into, and it is where a peer that wants to dominate LWW sits.
    for lamport in [
        LAMPORT_WIRE_CEILING + 1,
        LAMPORT_WIRE_CEILING + (1 << 40),
        LAMPORT_STATE_CEILING,
    ] {
        let mut doc = Document::new(cid(1));
        let op = op_at_lamport(cid(2), b"k", lamport);
        assert!(
            doc.apply(&op),
            "the op is folded, not refused, at {lamport}"
        );
        assert!(matches!(doc.get(b"k"), Some(Element::Scalar(_))));
        assert_eq!(doc.zone_clock(None), LAMPORT_WIRE_CEILING, "clamped");
        // And it is deduped like any other op.
        assert!(!doc.apply(&op));
    }
}

#[test]
fn the_clamp_does_not_lower_a_clock_a_replica_already_reached() {
    let mut doc = Document::new(cid(1));
    doc.apply(&op_at_lamport(cid(2), b"k", LAMPORT_WIRE_CEILING));
    // A local mint climbs past the wire ceiling; a later over-ceiling op must
    // not drag the clock back down onto ids this replica already minted.
    let mine = next_root_lamport(&mut doc);
    assert!(doc.apply(&op_at_lamport(cid(3), b"j", LAMPORT_STATE_CEILING)));
    assert!(next_root_lamport(&mut doc) > mine);
}

// ---------------------------------------------------------------------------
// A stamp names its author, and the clamp is what makes that load-bearing.
// ---------------------------------------------------------------------------

/// `op` re-stamped at `lamport` under `client` — an op whose *author* stays
/// whoever minted its `OpId` while its stamp claims someone else's id space.
fn with_forged_stamp(mut op: Op, lamport: u64, client: ClientId) -> Op {
    op.stamp.lamport = lamport;
    op.stamp.client = client;
    op
}

#[test]
fn an_op_whose_stamp_names_another_client_is_refused() {
    let mut doc = Document::new(cid(1));
    let honest = op_at_lamport(cid(2), b"k", 5);
    let forged = with_forged_stamp(honest.clone(), 5, cid(1));

    assert!(!doc.apply(&forged), "a stamp may only name its own author");
    assert!(doc.get(b"k").is_none(), "nothing of it landed");
    // The refusal is categorical, so the same op with its own author's stamp is
    // an ordinary op.
    assert!(doc.apply(&honest));
}

#[test]
fn a_stamp_forged_under_a_different_author_is_refused_before_it_plants_anything() {
    // The **mismatched-author** shape only, and that is the whole reach of the
    // authorship check: both fields are attacker-supplied, so an attacker that
    // authors the batch under the victim's own `ClientId` sets them equal and the
    // check admits it. What actually keeps the victim's next mint off planted ids
    // is the id-space high-water — see `lamport_mint.rs`, which models that
    // attacker. This case is worth refusing anyway: it is malformed, no honest op
    // is anywhere near it, and refusing raises the bar to impersonating a
    // `ClientId` rather than merely naming one.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    // 1. Park every clock at the ceiling with an ordinary, admissible op.
    assert!(doc.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    // 2. Plant a run in the victim's stamp space just above it.
    let run = text_run_at_lamport(attacker, b"t", "MMMMMMMM", LAMPORT_WIRE_CEILING + 1);
    for op in &run {
        let forged = with_forged_stamp(op.clone(), op.stamp.lamport, victim);
        doc.apply(&forged);
    }
    // 3. The victim's own edits are unaffected: nothing was planted.
    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    doc.transact(|tx| tx.text(b"t").insert(1, "B"));
    doc.transact(|tx| tx.text(b"t").insert(2, "C"));
    assert_eq!(text_of(&doc, b"t"), "ABC");

    let ids = text_ids(&doc, b"t", 3);
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 3, "the victim re-minted a live id: {ids:?}");
}

// ---------------------------------------------------------------------------
// The snapshot carries the same clocks.
// ---------------------------------------------------------------------------

#[test]
fn a_snapshot_declaring_a_root_clock_above_the_ceiling_is_refused() {
    let mut src = Document::new(cid(2));
    src.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let bytes = with_root_clock(src.encode_state(), u64::MAX);
    assert!(Document::decode_state(&bytes).is_err());
    // The bound is a refusal rather than a clamp because a stored clock is a
    // high-water over ids already published, and lowering one re-issues them —
    // see `a_save_and_reload_never_lowers_a_replicas_clock`. Exactly at the
    // ceiling is still a legal declaration.
    let at = with_root_clock(src.encode_state(), LAMPORT_STATE_CEILING);
    let mut doc = Document::decode_state(&at).expect("a decodable snapshot");
    // Decodable, and spent: the declaration names the top of the id space, so
    // there is no position left to mint. The edit is refused whole rather than
    // stepping over the bound the same decoder enforces.
    assert!(!doc.can_mint(None), "a clock at the ceiling leaves no id");
    assert!(
        doc.transact(|tx| tx.set(b"probe", Scalar::Int(1)))
            .is_empty(),
        "an exhausted replica emits nothing"
    );
    assert!(Document::decode_state(&doc.encode_state()).is_ok());
}

#[test]
fn a_snapshot_declaring_a_zone_clock_above_the_ceiling_is_refused() {
    let mut src = Document::new(cid(2));
    src.set_schema(schema_with_zone());
    src.transact(|tx| {
        tx.map(b"board").set(b"k", Scalar::Int(1));
    });
    let bytes = with_only_zone_clock(src.encode_state(), u64::MAX);
    assert!(Document::decode_state(&bytes).is_err());

    let at = with_only_zone_clock(src.encode_state(), LAMPORT_STATE_CEILING);
    let mut doc = Document::decode_state(&at).expect("a decodable snapshot");
    doc.set_schema(schema_with_zone());
    // The *capacity* is per-partition, exactly as the clock is: zone 0 is spent and
    // the root is not.
    assert!(!doc.can_mint(Some(0)));
    assert!(doc.can_mint(None));

    // The *refusal* is not. A zoned edit is refused, and because the latch is
    // document-global it takes the rest of that transaction with it — the batch is
    // empty, not merely free of zoned ops.
    let zoned = doc.transact(|tx| {
        tx.map(b"board").set(b"probe", Scalar::Int(1));
        tx.set(b"root_probe", Scalar::Int(1));
    });
    assert!(
        zoned.is_empty(),
        "a refused zoned edit let the rest of its transaction through: {zoned:?}"
    );
    assert!(doc.get(b"root_probe").is_none());

    // The next transaction starts clean, so the root partition's room is real and
    // the latch did not outlive the intention that set it.
    let root = doc.transact(|tx| tx.set(b"root_probe", Scalar::Int(1)));
    assert_eq!(root.len(), 1, "the root partition had no room after all");
    assert_eq!(root[0].zone, None);
}

#[test]
fn an_honest_replicas_snapshot_round_trips_its_clock_untouched() {
    // The state clamp sits far above anything a folded op can reach, so it never
    // bites on a replica's own snapshot — including one that took the wire clamp
    // and then minted past it, which is the case a clamp on decode would break
    // by re-issuing ids the author already published.
    let mut src = Document::new(cid(1));
    assert!(src.apply(&op_at_lamport(cid(2), b"k", LAMPORT_STATE_CEILING)));
    let minted = next_root_lamport(&mut src);
    assert!(minted > LAMPORT_WIRE_CEILING);

    let bytes = src.encode_state();
    let mut back = Document::decode_state(&bytes).expect("round trips");
    assert_eq!(back.encode_state(), bytes);
    assert!(next_root_lamport(&mut back) > minted);
}

// ---------------------------------------------------------------------------
// Convergence across the fix.
// ---------------------------------------------------------------------------

#[test]
fn replicas_that_fold_a_ceiling_op_converge() {
    let hostile = op_at_lamport(cid(9), b"k", u64::MAX);

    let mut a = Document::new(cid(1));
    let mut b = Document::new(cid(2));

    // A folds the hostile op first, B folds an ordinary local edit first.
    a.apply(&hostile);
    let a_ops = a.transact(|tx| tx.set(b"a", Scalar::Int(1)));

    let b_ops = b.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    b.apply(&hostile);

    for op in &b_ops {
        a.apply(op);
    }
    for op in &a_ops {
        b.apply(op);
    }

    assert_eq!(fingerprint(&a), fingerprint(&b));
}

#[test]
fn replicas_that_fold_the_same_ops_below_the_ceiling_hold_identical_clocks() {
    // The clamp is inert below the ceiling, so the per-partition `max` merge is
    // still order-independent and every replica lands on the same clock — the
    // property every honest room keeps.
    let mut a = Document::new(cid(1));
    let mut b = Document::new(cid(2));
    let pool: Vec<Op> = (1..=4)
        .flat_map(|i| {
            let mut peer = Document::new(cid(10 + i as u8));
            peer.transact(|tx| tx.text(b"t").insert(0, "abcde"));
            peer.transact(|tx| tx.set(b"x", Scalar::Int(i)))
        })
        .collect();

    for op in &pool {
        a.apply(op);
    }
    for op in pool.iter().rev() {
        b.apply(op);
    }
    assert_eq!(next_root_lamport(&mut a), next_root_lamport(&mut b));
}

#[test]
fn a_ceiling_op_costs_the_partition_its_causal_order_but_never_an_id() {
    // Once a hostile lamport has parked every replica at the clamp, each mints
    // above it from its own count and clamps the others back — lamport ordering
    // in that partition is spent. That is the price of bounding the clock at
    // all, and it is what an *unbounded* clock pays too, on top of the overflow.
    // What survives is the part that matters: no replica ever re-issues a stamp.
    let hostile = op_at_lamport(cid(9), b"k", u64::MAX);
    let mut a = Document::new(cid(1));
    let mut b = Document::new(cid(2));
    a.apply(&hostile);
    b.apply(&hostile);

    let mut minted: Vec<Stamp> = Vec::new();
    for round in 0..16 {
        let a_ops = a.transact(|tx| tx.set(b"x", Scalar::Int(round)));
        let b_ops = b.transact(|tx| tx.set(b"y", Scalar::Int(round)));
        minted.extend(a_ops.iter().chain(b_ops.iter()).map(|op| op.stamp));
        for op in &b_ops {
            a.apply(op);
        }
        for op in &a_ops {
            b.apply(op);
        }
    }

    // Both replicas sit at the same lamport every round, so only the client
    // tiebreak separates the stamps — and it always does.
    let mut unique = minted.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), minted.len(), "re-issued a stamp: {minted:?}");
    assert_eq!(fingerprint(&a), fingerprint(&b));
}

#[test]
fn a_zoned_ceiling_op_converges_across_replicas() {
    let hostile = zoned_ceiling_ops(cid(9), b"k");
    let mut a = zoned_doc(cid(1), &hostile);
    let reversed: Vec<Op> = hostile.iter().rev().cloned().collect();
    let mut b = zoned_doc(cid(2), &reversed);

    let a_ops = a.transact(|tx| tx.map(b"board").set(b"x", Scalar::Int(1)));
    let b_ops = b.transact(|tx| tx.map(b"board").set(b"y", Scalar::Int(2)));
    for op in &b_ops {
        a.apply(op);
    }
    for op in &a_ops {
        b.apply(op);
    }

    assert_eq!(fingerprint(&a), fingerprint(&b));
    assert_eq!(next_zone_lamport(&mut a), next_zone_lamport(&mut b));
}

// ---------------------------------------------------------------------------
// The constants themselves.
// ---------------------------------------------------------------------------

#[test]
fn an_op_a_replica_minted_above_the_clamp_is_still_folded_by_its_peers() {
    // The rejected alternative — an acceptance gate that *refuses* an
    // over-ceiling op rather than clamping what it does to the clock — dies
    // here. Any gate is one op away from the clock it bounds, so a replica the
    // wire parked at the ceiling makes its very next ordinary mint above it, and
    // a gate would refuse that mint on every peer. The room would then diverge
    // on honest traffic, which is strictly worse than the panic being fixed.
    let mut a = Document::new(cid(1));
    assert!(a.apply(&op_at_lamport(cid(9), b"k", LAMPORT_STATE_CEILING)));
    let ops = a.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    assert!(ops[0].stamp.lamport > LAMPORT_WIRE_CEILING);

    let mut b = Document::new(cid(2));
    assert!(b.apply(&op_at_lamport(cid(9), b"k", LAMPORT_STATE_CEILING)));
    for op in &ops {
        assert!(b.apply(op), "a peer refused an honestly minted op");
    }
    assert_eq!(fingerprint(&a), fingerprint(&b));
}

#[test]
fn no_wire_input_can_put_a_clock_above_the_state_ceiling() {
    // The wire clamp, the decode refusal and the mint bound are the only writers
    // of a clock, and all three stop at or below the state ceiling — so nothing
    // any input can do puts a clock above it. That is what makes the emit site's
    // arithmetic total: every position it reads is in range, and the one position
    // past the end is refused rather than handled, because every other answer
    // there re-issues a stamp.
    let mut src = Document::new(cid(2));
    src.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let bytes = with_root_clock(src.encode_state(), LAMPORT_STATE_CEILING);

    let mut doc = Document::decode_state(&bytes).expect("a decodable snapshot");
    assert!(!doc.apply(&op_at_lamport(cid(3), b"j", u64::MAX)));
    for op in &text_run_at_lamport(cid(4), b"t", "abcdefghij", u64::MAX - 4) {
        doc.apply(op);
    }
    assert_eq!(doc.zone_clock(None), LAMPORT_STATE_CEILING);
    assert!(!doc.can_mint(None));
    assert!(doc
        .transact(|tx| tx.set(b"probe", Scalar::Int(1)))
        .is_empty());
    assert_eq!(
        doc.zone_clock(None),
        LAMPORT_STATE_CEILING,
        "a refused mint moved the clock"
    );
}

#[test]
fn a_save_and_reload_never_lowers_a_replicas_clock() {
    // The defect a *clamp* on decode would create, and the reason the state bound
    // is a refusal. A clock is its author's high-water over the ids it published;
    // a decode that lowered one would hand the replica live node ids back, and a
    // sequence drops a re-issued id as a replay — the write is lost on the author
    // and on every peer, silently. So a reload either carries the clock forward or
    // refuses the snapshot; it never comes back lower.
    // Every start that still leaves the five ids the two edits below reserve: the
    // create, the two codepoints of "AB", the idempotent re-create a second
    // `tx.text` emits, and "C". Where the space runs out first, the boundary is
    // its own case — `a_reload_at_the_top_of_the_space_refuses_rather_than_re_issuing`.
    for start in [0, 1 << 20, LAMPORT_WIRE_CEILING, LAMPORT_STATE_CEILING - 5] {
        let seed = with_root_clock(Document::new(cid(1)).encode_state(), start);
        let mut doc = Document::decode_state_as(cid(1), 0, &seed).expect("decodes");
        doc.transact(|tx| tx.text(b"t").insert(0, "AB"));
        let live = text_ids(&doc, b"t", 2);

        // Every start a snapshot may legally declare — the ceiling included —
        // reloads. `encode_state` never emits a clock its own decoder refuses, so
        // no iteration here is excused: excusing one is what hid the edit that
        // stepped the stored clock over the bound.
        let mut back = Document::decode_state(&doc.encode_state())
            .unwrap_or_else(|e| panic!("no reload of a replica's own snapshot at {start}: {e:?}"));
        back.transact(|tx| tx.text(b"t").insert(2, "C"));
        assert_eq!(
            text_of(&back, b"t"),
            "ABC",
            "reload lost a write at {start}"
        );
        let after = text_ids(&back, b"t", 3);
        assert_eq!(after[..2], live[..], "reload moved the ids it decoded");
        assert!(
            !live.contains(&after[2]),
            "re-issued a live stamp at {start}"
        );
    }
}

#[test]
fn a_reload_at_the_top_of_the_space_refuses_rather_than_re_issuing() {
    // Past the last id, the sweep above has no honest answer left, and the whole
    // point of the bound is which answer is taken there. A clamp would hand the
    // replica a stamp it already published; walking past the ceiling would leave a
    // snapshot its own decoder refuses. So the edit is refused: nothing is emitted,
    // no live id is re-issued, the clock is not lowered, and the snapshot still
    // reloads.
    for start in [LAMPORT_STATE_CEILING - 4, LAMPORT_STATE_CEILING] {
        let seed = with_root_clock(Document::new(cid(1)).encode_state(), start);
        let mut doc = Document::decode_state_as(cid(1), 0, &seed).expect("decodes");
        doc.transact(|tx| tx.text(b"t").insert(0, "AB"));
        let live: Vec<Stamp> = match doc.get(b"t") {
            Some(Element::Text(t)) => {
                let t = t.borrow();
                t.node_ids(0, t.len())
            }
            _ => Vec::new(),
        };
        let clock = doc.zone_clock(None);

        let bytes = doc.encode_state();
        let mut back = Document::decode_state(&bytes)
            .unwrap_or_else(|e| panic!("no reload of a replica's own snapshot at {start}: {e:?}"));
        assert!(
            back.zone_clock(None) >= clock,
            "the reload lowered the clock at {start}"
        );

        let emitted = back.transact(|tx| tx.text(b"t").insert(live.len(), "C"));
        let after: Vec<Stamp> = match back.get(b"t") {
            Some(Element::Text(t)) => {
                let t = t.borrow();
                t.node_ids(0, t.len())
            }
            _ => Vec::new(),
        };
        for stamp in &after {
            assert_eq!(
                after.iter().filter(|s| *s == stamp).count(),
                1,
                "re-issued a live stamp at {start}"
            );
        }
        assert!(
            after.len() >= live.len(),
            "the refusal dropped a codepoint at {start}"
        );
        // Whatever was emitted stayed inside the space every peer admits.
        for op in &emitted {
            assert!(
                op.stamp.lamport <= LAMPORT_STATE_CEILING,
                "minted past the id space at {start}"
            );
        }
        // The write lands exactly when the space allowed it — `can_mint` cannot be
        // used as the oracle here, since it folds in the refusal latch this very
        // transaction may have just set.
        assert_eq!(
            after.len() > live.len(),
            start <= LAMPORT_STATE_CEILING - 5,
            "the space ran out at a different start than {start}"
        );
        Document::decode_state(&back.encode_state()).expect("still reloads");
    }
}
