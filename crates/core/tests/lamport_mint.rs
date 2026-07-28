//! The lamport mint clears its own id space, and a replica can always reload the
//! snapshot it just wrote.
//!
//! Two halves of one rule. A partition clock is a running maximum that a *bound*
//! keeps off the top of the space — so the clock is no longer an upper bound over
//! the stamps the document holds, and a mint that reads the clock alone is minting
//! into a region another party may already occupy. The rule this suite pins:
//!
//! * **The mint clears the replica's own id space, not the clock.** A stamp names
//!   its author, so the only stamps a local mint can collide with are the ones
//!   already present under *this* replica's client id. The mint starts above both
//!   the partition clock and that high-water — the same discipline the op-seq
//!   position already uses, which reads the ids the replica holds rather than a
//!   counter off the wire. Trusting the clock alone is exploitable: a peer that
//!   authors under a victim's `ClientId` plants a run just above the clamp, the
//!   clamp stops the clock under it, and the victim's next edits land on ids the
//!   sequence already holds and are dropped as replays.
//! * **`encode_state` never emits bytes `decode_state` refuses.** A stored clock is
//!   bounded on the way in, so it has to be bounded on the way out too: the local
//!   mint's clock advance is clamped at the same [`LAMPORT_STATE_CEILING`] the
//!   decoder enforces. Clamping the *clock* is safe precisely because the mint no
//!   longer depends on it for id-freedom — the high-water above carries that — so
//!   the pair holds together where neither half does alone.
//!
//! The high-water is derived from the ids the replica holds, never stored beside
//! them: a decoded snapshot's stamps are read back as they decode, so a reload and
//! a snapshot adopted under a different client id both recover it exactly, and no
//! byte of the state encoding moves.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::schema::Schema;
use crdtsync_core::stamp::{LAMPORT_STATE_CEILING, LAMPORT_WIRE_CEILING};
use crdtsync_core::{ClientId, Element, OpKind, Scalar, Stamp};

mod common;
use common::{cid, with_only_zone_clock, with_root_clock};

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// The text a key holds.
fn text_of(doc: &Document, key: &[u8]) -> String {
    let Some(Element::Text(t)) = doc.get(key) else {
        panic!("text materialised");
    };
    let s = t.borrow().as_string();
    s
}

/// The node ids of a text's first `count` characters.
fn text_ids(doc: &Document, key: &[u8], count: usize) -> Vec<Stamp> {
    let Some(Element::Text(t)) = doc.get(key) else {
        panic!("text materialised");
    };
    let ids = t.borrow().node_ids(0, count);
    assert_eq!(ids.len(), count, "text holds fewer than {count} chars");
    ids
}

/// Every char id a text holds.
fn all_text_ids(doc: &Document, key: &[u8]) -> Vec<Stamp> {
    let Some(Element::Text(t)) = doc.get(key) else {
        panic!("text materialised");
    };
    let t = t.borrow();
    let ids = t.node_ids(0, t.len());
    ids
}

/// A batch authored by `client`, with every text-insert re-stamped at `lamport`.
/// The **whole op is authored under `client`** — id and stamp both — which is the
/// attacker a `stamp.client == id.client` check admits: the server checks an op's
/// author against the identity the client *declared* at Hello, and authenticating
/// that claim is the transport's job.
fn impersonated_run(victim: ClientId, key: &[u8], s: &str, lamport: u64) -> Vec<Op> {
    let mut ops = Document::new(victim).transact(|tx| tx.text(key).insert(0, s));
    for op in ops.iter_mut() {
        if matches!(op.kind, OpKind::TextInsert { .. }) {
            op.stamp.lamport = lamport;
        }
    }
    ops
}

/// A one-op batch authored by `client`, re-stamped to carry `lamport`.
fn op_at_lamport(client: ClientId, key: &[u8], lamport: u64) -> Op {
    let mut op = Document::new(client)
        .transact(|tx| tx.set(key, Scalar::Bytes(key.to_vec())))
        .remove(0);
    op.stamp.lamport = lamport;
    op
}

fn schema_with_zone() -> Schema {
    Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
             "types": { "R": { "kind": "map" } },
             "zones": { "board": "/board" } }"#,
    )
    .expect("schema parses")
}

/// Every stamp is distinct.
fn assert_all_distinct(ids: &[Stamp], what: &str) {
    let mut sorted = ids.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), ids.len(), "{what}: a stamp was re-issued");
}

// ---------------------------------------------------------------------------
// C17 — the mint clears its own id space.
// ---------------------------------------------------------------------------

#[test]
fn an_impersonating_peer_cannot_plant_a_victims_next_stamps() {
    // The whole plant is authored under the victim's own `ClientId`, so every
    // author check in the stack — including `stamp.client == id.client` — admits
    // it. The clamp then stops the clock *below* the plant, so a mint that reads
    // the clock alone lands exactly on the planted ids.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    // Park every clock at the clamp with an ordinary, admissible op.
    assert!(doc.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    // Plant a run in the victim's stamp space just above it.
    for op in &impersonated_run(victim, b"t", "MMMMMMMM", LAMPORT_WIRE_CEILING + 1) {
        doc.apply(op);
    }

    // The victim's own subsequent writes must land.
    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    doc.transact(|tx| tx.text(b"t").insert(1, "B"));
    doc.transact(|tx| tx.text(b"t").insert(2, "C"));
    assert_eq!(
        text_of(&doc, b"t"),
        "ABCMMMMMMMM",
        "the victim's writes were dropped as replays"
    );
    assert_all_distinct(&all_text_ids(&doc, b"t"), "planted text");
}

#[test]
fn a_planted_run_survives_a_reload_without_taking_the_next_mint() {
    // The high-water is derived from the ids the replica holds, so it has to come
    // back off a snapshot — otherwise the reload mints straight onto the plant
    // that the snapshot's own clamped clock does not cover.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    assert!(doc.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    for op in &impersonated_run(victim, b"t", "MMMM", LAMPORT_WIRE_CEILING + 1) {
        doc.apply(op);
    }

    let mut back = Document::decode_state(&doc.encode_state()).expect("its own snapshot decodes");
    back.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(text_of(&back, b"t"), "AMMMM", "the reload lost the write");
    assert_all_distinct(&all_text_ids(&back, b"t"), "reloaded text");
}

#[test]
fn a_snapshot_adopted_under_a_victims_id_clears_that_victims_planted_ids() {
    // The offline-first lifecycle: a *server* replica folds the plant, so its own
    // clock clamps it away, and the victim then adopts that server snapshot under
    // its own identity. The high-water has to be recovered for the adopting
    // client, not for whoever encoded the bytes.
    let victim = cid(1);
    let server = cid(7);
    let attacker = cid(9);

    let mut room = Document::new(server);
    assert!(room.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    for op in &impersonated_run(victim, b"t", "MMMM", LAMPORT_WIRE_CEILING + 1) {
        room.apply(op);
    }

    let mut doc =
        Document::decode_state_as(victim, 0, &room.encode_state()).expect("a decodable snapshot");
    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(
        text_of(&doc, b"t"),
        "AMMMM",
        "the adopting replica minted onto ids planted in its own space"
    );
    assert_all_distinct(&all_text_ids(&doc, b"t"), "adopted text");
}

#[test]
fn a_snapshot_declaring_a_clock_below_its_own_stamps_does_not_take_the_next_mint() {
    // The other way a clock stops covering the ids a document holds: not a clamp
    // stopping it short, but a snapshot simply declaring one below its own content.
    // The mint reads the high-water exactly when no clock covers it, so both shapes
    // land on the same rule.
    let mut src = Document::new(cid(1));
    src.transact(|tx| tx.text(b"t").insert(0, "MMMM"));
    let live = all_text_ids(&src, b"t");
    let lowered = with_root_clock(src.encode_state(), 0);

    let mut doc = Document::decode_state(&lowered).expect("a decodable snapshot");
    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(
        text_of(&doc, b"t"),
        "AMMMM",
        "the mint landed on an id the snapshot already held"
    );
    let after = all_text_ids(&doc, b"t");
    assert_all_distinct(&after, "lowered-clock text");
    assert!(
        !live.contains(&after[0]),
        "re-issued a stamp the snapshot carried"
    );
}

#[test]
fn another_partitions_clock_does_not_vouch_for_a_plant_above_the_clamp() {
    // A clock that climbed past the clamp got there by *local* minting in its own
    // partition, so it is no evidence about what a peer planted in another one. A
    // rule that read "some clock reaches at least this far" would be satisfied by
    // that unrelated clock and hand the plant back to the mint.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    doc.set_schema(schema_with_zone());

    // Park both partitions at the clamp, then run the zone's clock well past it
    // with the victim's own ordinary edits.
    let mut park_root = op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING);
    park_root.zone = None;
    assert!(doc.apply(&park_root));
    let mut park_zone = Document::new(cid(8)).transact(|tx| tx.set(b"seed", Scalar::Int(1)));
    park_zone[0].stamp.lamport = LAMPORT_WIRE_CEILING;
    park_zone[0].zone = Some(0);
    assert!(doc.apply(&park_zone[0]));
    for i in 0..20 {
        doc.transact(|tx| {
            tx.map(b"board").set(b"busy", Scalar::Int(i));
        });
    }
    assert!(doc.zone_clock(Some(0)) > LAMPORT_WIRE_CEILING + 8);

    // Now plant in the *root*, inside the window that zone clock reaches over. The
    // op ids are moved clear of the ones the victim has already published, which is
    // the seq the impersonator would pick anyway.
    for (i, op) in impersonated_run(victim, b"t", "MMMM", LAMPORT_WIRE_CEILING + 1)
        .iter_mut()
        .enumerate()
    {
        op.id.seq = 5_000 + i as u64;
        assert!(doc.apply(op), "the plant landed");
    }
    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(
        text_of(&doc, b"t"),
        "AMMMM",
        "a zone clock vouched for a plant in the root partition"
    );
    assert_all_distinct(&all_text_ids(&doc, b"t"), "root plant under a busy zone");
}

#[test]
fn an_op_tagged_into_another_partition_cannot_hide_a_plant() {
    // An op's envelope names its own partition and the fold honours it, but the ids
    // it plants land wherever its *target* lives. So a peer raises zone 0's clock
    // while planting in the root, and any rule that let one partition's clock speak
    // for another would hand the plant straight back to the mint — with no clamp and
    // no ceiling involved, on a brand-new document.
    let victim = cid(1);
    let mut doc = Document::new(victim);

    let mut plant = impersonated_run(victim, b"t", "MMMM", 1);
    for op in plant.iter_mut() {
        op.zone = Some(0);
        assert!(doc.apply(op), "the plant landed");
    }
    assert_eq!(doc.zone_clock(None), 0, "the root clock never moved");

    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    doc.transact(|tx| tx.text(b"t").insert(1, "B"));
    assert_eq!(
        text_of(&doc, b"t"),
        "ABMMMM",
        "a zone-tagged op hid a plant from the root partition's mint"
    );
    assert_all_distinct(&all_text_ids(&doc, b"t"), "cross-partition plant");
}

#[test]
fn a_second_partitions_clock_does_not_cover_a_lowered_root_clock() {
    // The snapshot half of the same shape: a declared root clock below the stamps
    // the state carries, with a zone clock high enough that "some clock reaches this
    // far" would be satisfied by it.
    let mut src = Document::new(cid(1));
    src.set_schema(schema_with_zone());
    src.transact(|tx| tx.text(b"t").insert(0, "MMMM"));
    for i in 0..30 {
        src.transact(|tx| {
            tx.map(b"board").set(b"busy", Scalar::Int(i));
        });
    }
    let live = all_text_ids(&src, b"t");
    let lowered = with_root_clock(src.encode_state(), 0);

    let mut doc = Document::decode_state(&lowered).expect("a decodable snapshot");
    doc.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(
        text_of(&doc, b"t"),
        "AMMMM",
        "a zone clock covered for a lowered root clock"
    );
    let after = all_text_ids(&doc, b"t");
    assert_all_distinct(&after, "lowered root clock beside a busy zone");
    assert!(
        !live.contains(&after[0]),
        "re-issued a stamp the state held"
    );
}

#[test]
fn a_plant_still_waiting_in_the_buffer_is_already_held() {
    // An op whose target has not arrived sits in the buffer with its ids as
    // published as any other — the room's log holds it and no peer resends it. If
    // the mint only cleared *applied* stamps, which replica re-mints onto them would
    // be a function of arrival order, and two replicas folding the same ops would
    // disagree rather than merely lose a write.
    let victim = cid(1);
    let mut plant = impersonated_run(victim, b"t", "MMMM", 1);
    for (i, op) in plant.iter_mut().enumerate() {
        op.id.seq = 5_000 + i as u64;
    }
    let (create, insert) = (plant[0].clone(), plant[1].clone());

    // X sees the run first: its container has not arrived, so it waits — and the
    // victim then writes into that same text itself.
    let mut x = Document::new(victim);
    assert!(!x.apply(&insert), "the run waits on its container");
    x.transact(|tx| tx.text(b"t").insert(0, "A"));
    x.apply(&create);

    // Y sees the whole batch before the victim writes.
    let mut y = Document::new(victim);
    y.apply(&create);
    y.apply(&insert);
    y.transact(|tx| tx.text(b"t").insert(0, "A"));

    // The two replicas author *different* "A" ops — an insert's anchor is whatever
    // its author saw — so their renderings need not match. What must match is that
    // neither lost a write and neither re-issued an id: with the buffer invisible to
    // the high-water, X mints straight onto the run waiting in it and drops as many
    // of its codepoints as it overlapped.
    for (name, doc) in [("x", &x), ("y", &y)] {
        assert_eq!(
            all_text_ids(doc, b"t").len(),
            5,
            "{name} lost a write: {:?}",
            text_of(doc, b"t")
        );
        assert!(
            text_of(doc, b"t").contains('A'),
            "{name} lost the victim's own"
        );
        assert_all_distinct(&all_text_ids(doc, b"t"), name);
    }
}

#[test]
fn a_stamp_at_the_end_of_the_space_does_not_take_the_next_mint_out() {
    // One op is enough to put a replica's own high-water at the very top, and the
    // mint has to stay total there: no panic, no wrap, and no id it already holds.
    let victim = cid(1);
    let mut doc = Document::new(victim);

    // The last position of all is refused: there is nothing past it to mint.
    let mut end = op_at_lamport(victim, b"k", u64::MAX);
    end.stamp.offset = u64::MAX;
    assert!(!doc.apply(&end), "an op with no successor is admissible");
    assert!(doc.get(b"k").is_none(), "nothing of it landed");

    // One below it is ordinary, and the mint counts past it rather than onto it.
    let mut top = op_at_lamport(victim, b"k", u64::MAX);
    top.stamp.offset = u64::MAX - 1;
    assert!(doc.apply(&top));
    let minted = doc.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0].stamp;
    assert!(
        (minted.lamport, minted.offset) > (top.stamp.lamport, top.stamp.offset),
        "the mint did not count past the stamp it holds"
    );
    assert!(Document::decode_state(&doc.encode_state()).is_ok());
}

#[test]
fn a_plant_in_a_zone_does_not_take_that_zones_next_mint() {
    // The mint reads its own partition's clock, so the plant has to be cleared in
    // whichever partition it lands in.
    let victim = cid(1);
    let attacker = cid(9);

    let mut peer = Document::new(victim);
    peer.set_schema(schema_with_zone());
    let mut plant = peer.transact(|tx| {
        tx.map(b"board").text(b"t").insert(0, "MMMM");
    });
    for op in plant.iter_mut() {
        assert_eq!(op.zone, Some(0), "the whole batch belongs to the zone");
        if matches!(op.kind, OpKind::TextInsert { .. }) {
            op.stamp.lamport = LAMPORT_WIRE_CEILING + 1;
        }
    }

    let mut doc = Document::new(victim);
    doc.set_schema(schema_with_zone());
    let mut park = Document::new(attacker);
    park.set_schema(schema_with_zone());
    for op in park
        .transact(|tx| {
            tx.map(b"board").set(b"k", Scalar::Int(1));
        })
        .iter_mut()
    {
        op.stamp.lamport = LAMPORT_WIRE_CEILING;
        doc.apply(op);
    }
    for op in &plant {
        doc.apply(op);
    }

    doc.transact(|tx| {
        tx.map(b"board").text(b"t").insert(0, "A");
    });
    let Some(Element::Map(board)) = doc.get(b"board") else {
        panic!("board materialised");
    };
    let Some(Element::Text(t)) = board.borrow().get(b"t") else {
        panic!("zoned text materialised");
    };
    let (s, ids) = {
        let t = t.borrow();
        (t.as_string(), t.node_ids(0, t.len()))
    };
    assert_eq!(s, "AMMMM", "the zoned write was dropped as a replay");
    assert_all_distinct(&ids, "zoned text");
}

// ---------------------------------------------------------------------------
// C18 — encode_state never emits bytes decode_state refuses.
// ---------------------------------------------------------------------------

#[test]
fn one_edit_on_a_ceiling_snapshot_still_re_decodes() {
    // The reported shape: a snapshot declaring exactly the ceiling is accepted, so
    // whatever the replica does next has to stay inside what the same decoder
    // admits. One ordinary edit was enough to leave the replica unable to reload
    // its own bytes.
    let mut src = Document::new(cid(1));
    src.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let at = with_root_clock(src.encode_state(), LAMPORT_STATE_CEILING);

    let mut doc = Document::decode_state(&at).expect("a decodable snapshot");
    doc.transact(|tx| tx.set(b"probe", Scalar::Int(1)));
    Document::decode_state(&doc.encode_state()).expect("a replica can reload its own snapshot");
}

#[test]
fn a_ceiling_zone_snapshot_still_re_decodes_after_an_edit() {
    let mut src = Document::new(cid(1));
    src.set_schema(schema_with_zone());
    src.transact(|tx| {
        tx.map(b"board").set(b"k", Scalar::Int(1));
    });
    let at = with_only_zone_clock(src.encode_state(), LAMPORT_STATE_CEILING);

    let mut doc = Document::decode_state(&at).expect("a decodable snapshot");
    doc.set_schema(schema_with_zone());
    doc.transact(|tx| {
        tx.map(b"board").set(b"probe", Scalar::Int(1));
    });
    Document::decode_state(&doc.encode_state()).expect("a replica can reload its own snapshot");
}

#[test]
fn a_text_run_at_the_ceiling_still_re_decodes() {
    // A run reserves one lamport per codepoint, so it is the shape that carries a
    // clock furthest past a bound in one op — a point check on a range problem.
    let mut src = Document::new(cid(1));
    src.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let at = with_root_clock(src.encode_state(), LAMPORT_STATE_CEILING - 2);

    let mut doc = Document::decode_state(&at).expect("a decodable snapshot");
    doc.transact(|tx| tx.text(b"t").insert(0, "ABCDEFGH"));
    Document::decode_state(&doc.encode_state()).expect("a replica can reload its own snapshot");
    assert_eq!(text_of(&doc, b"t"), "ABCDEFGH");
    assert_all_distinct(&all_text_ids(&doc, b"t"), "ceiling run");
}

#[test]
fn every_reachable_clock_round_trips_and_never_re_issues_a_stamp() {
    // The sweep the two halves have to hold together on: every start a snapshot
    // may legally declare, edited and reloaded, with no iteration excused. A
    // reload must decode, must not lose the write, and must not hand a live stamp
    // back to the mint.
    for start in [
        0,
        1 << 20,
        LAMPORT_WIRE_CEILING - 1,
        LAMPORT_WIRE_CEILING,
        LAMPORT_WIRE_CEILING + 1,
        LAMPORT_STATE_CEILING - 4,
        LAMPORT_STATE_CEILING - 1,
        LAMPORT_STATE_CEILING,
    ] {
        let seed = with_root_clock(Document::new(cid(1)).encode_state(), start);
        let mut doc = Document::decode_state_as(cid(1), 0, &seed).expect("decodes");
        doc.transact(|tx| tx.text(b"t").insert(0, "AB"));
        let live = text_ids(&doc, b"t", 2);

        let mut back = Document::decode_state(&doc.encode_state()).unwrap_or_else(|e| {
            panic!("a replica could not reload its own snapshot at {start}: {e:?}")
        });
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

// ---------------------------------------------------------------------------
// Large-but-legitimate traffic is untouched.
// ---------------------------------------------------------------------------

#[test]
fn an_honest_replica_mints_exactly_as_before() {
    // The high-water only ever sits above the clock when something planted stamps
    // the clock did not cover, so ordinary traffic mints the same lamports it
    // always did.
    let mut doc = Document::new(cid(1));
    let a = doc.transact(|tx| tx.set(b"k", Scalar::Int(1)))[0]
        .stamp
        .lamport;
    doc.apply(&op_at_lamport(cid(2), b"peer", 40));
    let b = doc.transact(|tx| tx.set(b"k", Scalar::Int(2)))[0]
        .stamp
        .lamport;
    assert_eq!((a, b), (1, 41));
}

#[test]
fn a_large_but_legitimate_clock_still_mints_and_replicates() {
    // A clock just under the clamp is ordinary traffic: the mint counts on from it,
    // the run reserves its whole span, and a peer folding the same ops converges on
    // the same text and the same clock.
    let base = op_at_lamport(cid(2), b"k", LAMPORT_WIRE_CEILING - 1);
    let mut doc = Document::new(cid(1));
    assert!(doc.apply(&base));

    let create_and_run = doc.transact(|tx| tx.text(b"t").insert(0, "hello"));
    let run = create_and_run
        .iter()
        .find(|op| matches!(op.kind, OpKind::TextInsert { .. }))
        .expect("the run op");
    assert_eq!(run.stamp.lamport, LAMPORT_WIRE_CEILING + 1);
    assert_eq!(text_of(&doc, b"t"), "hello");
    assert_all_distinct(&all_text_ids(&doc, b"t"), "large-clock run");
    assert_eq!(
        doc.zone_clock(None),
        LAMPORT_WIRE_CEILING + 5,
        "the run reserved one lamport per codepoint"
    );

    let tail = doc.transact(|tx| tx.text(b"t").insert(5, "!"));
    let doc_run_end = doc.zone_clock(None);
    let mut peer = Document::new(cid(3));
    assert!(peer.apply(&base));
    for op in create_and_run.iter().chain(tail.iter()) {
        assert!(peer.apply(op), "the peer folded {:?}", op.id);
    }
    assert_eq!(text_of(&peer, b"t"), "hello!");
    assert_all_distinct(&all_text_ids(&peer, b"t"), "peer's copy of the run");
    // The peer's clock stops at the clamp, where the author's went past it — that
    // asymmetry is C8's price for bounding the clock, and the author's own next
    // mint stays clear of the ids it published regardless, which is what matters.
    assert_eq!(peer.zone_clock(None), LAMPORT_WIRE_CEILING);
    assert!(
        doc.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0]
            .stamp
            .lamport
            > doc_run_end
    );
}

#[test]
fn a_zone_still_folds_independently_though_its_mint_counts_globally() {
    // A stamp is a document-global id, so the mint counts on from the replica's own
    // global position and a zone's lamports are no longer compact. What the per-zone
    // streams actually need is untouched: **folding** an op still advances that op's
    // partition alone, so two zones stay causally independent.
    let mut doc = Document::new(cid(1));
    doc.set_schema(schema_with_zone());
    doc.transact(|tx| tx.set(b"top", Scalar::Int(1)));
    doc.transact(|tx| tx.set(b"top", Scalar::Int(2)));
    let root_clock = doc.zone_clock(None);

    let zoned = doc.transact(|tx| {
        tx.map(b"board").set(b"k", Scalar::Int(1));
    });
    let zone_lamport = zoned
        .iter()
        .find(|op| op.zone == Some(0))
        .expect("a zoned op")
        .stamp
        .lamport;
    assert!(
        zone_lamport > root_clock,
        "the mint counted on from the replica's own stamp position"
    );

    // Folding a peer's zoned op leaves the root partition where it was.
    let mut peer = Document::new(cid(2));
    peer.set_schema(schema_with_zone());
    let before = doc.zone_clock(None);
    for op in &peer.transact(|tx| {
        tx.map(b"board").set(b"k", Scalar::Int(9));
    }) {
        doc.apply(op);
    }
    assert_eq!(
        doc.zone_clock(None),
        before,
        "a zoned fold advanced the root clock"
    );
}
