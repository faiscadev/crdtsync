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
//!   bounded on the way in, so it has to be bounded on the way out too. Nothing
//!   clamps at the emit site: the mint *refuses* when a reservation would pass
//!   [`LAMPORT_STATE_CEILING`], so the clock it advances is already under the bound
//!   the decoder enforces. The three writers of a clock — a fold (clamped at the
//!   wire ceiling), a mint (refused past the state ceiling) and a decode (refused
//!   above it) — are then all inside it, and the pair holds together where neither
//!   half does alone.
//!
//! * **The high-water is stored beside the clocks, and bounded like one.** It
//!   cannot be recovered from the content: a tombstoned sequence run persists as
//!   `(head, len)` and only the head is a stamp on the wire, a counter persists no
//!   stamp at all, and an ACL or ranged entry persists only the id *derived* from
//!   one — so deleting a plant would hide up to a chunk of ids from anything that
//!   read them back off a decode. Storing it also makes a projection safe by
//!   default (a projection mutates a live document and leaves the field alone,
//!   scrubbed to the recipient's own entry) and carries the record through
//!   `adopt_as`. Being stored, it has to stay decodable: a declared high-water
//!   above [`LAMPORT_STATE_CEILING`] is refused, never clamped, on the same
//!   reasoning a stored clock is.
//! * **The mint can refuse, and that is structural.** Bounding what the record may
//!   hold bounds where a stamp may sit, and at the top of that space no answer is
//!   total: clamping the record re-issues an id that is already live (measured —
//!   a replica at the ceiling mints `CEILING + 1`, records it clamped back to
//!   `CEILING`, and mints `CEILING + 1` again, forever), and the sub-lamport
//!   `offset` is no second dimension to escape into, because `stamp_key` is
//!   `lamport ++ client` and two stamps differing only in offset derive the *same*
//!   ACL, ranged and XML-child ids. So exhaustion is a refused edit
//!   ([`Document::can_mint`]) — and the wire gate stops at the same constant, so
//!   no replica ever emits what its peers reject.

use crdtsync_core::doc::Document;
use crdtsync_core::list::Side;
use crdtsync_core::op::Op;
use crdtsync_core::path;
use crdtsync_core::schema::Schema;
use crdtsync_core::stamp::{LAMPORT_STATE_CEILING, LAMPORT_WIRE_CEILING};
use crdtsync_core::{ClientId, Element, OpKind, Scalar, Stamp};

mod common;
use common::{cid, with_only_zone_clock, with_root_clock, with_stamp_high_water};

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

/// [`op_at_lamport`] under an op id `seq` the victim replica has not spent, so a
/// plant lands on a document that has already authored under the same client.
fn plant_at_lamport(client: ClientId, key: &[u8], lamport: u64, seq: u64) -> Op {
    let mut op = op_at_lamport(client, key, lamport);
    op.id.seq = seq;
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

/// Every char id a text holds, or none when the key holds no text — the shape a
/// refused create leaves.
fn text_ids_or_none(doc: &Document, key: &[u8]) -> Vec<Stamp> {
    match doc.get(key) {
        Some(Element::Text(t)) => {
            let t = t.borrow();
            t.node_ids(0, t.len())
        }
        _ => Vec::new(),
    }
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
fn a_deleted_plant_still_holds_its_ids_across_a_reload() {
    // A sequence encodes a dead run as `(head, len)`, so only the head is a stamp on
    // the wire and the ids behind it would be invisible to anything reading stamps
    // alone. The floor is told the run's whole *reach* for exactly this reason.
    // Deleting the junk a plant inserted is the obvious thing a user does, so this
    // is the mainline path, not a corner: the ids stay held, and the victim's next
    // write after the reload lands rather than being dropped as a replay.
    //
    // What this does *not* pin is that the record is stored — the reach alone
    // carries it here. `a_counter_run_leaves_no_stamp_behind_and_the_record_still_holds`
    // and `a_projection_keeps_the_recipients_own_high_water` are the two that fail
    // if storage goes, because neither leaves a stamp for a floor to read.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    assert!(doc.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    for op in &impersonated_run(victim, b"t", "MMMM", LAMPORT_WIRE_CEILING + 1) {
        doc.apply(op);
    }
    // The user deletes the whole plant, leaving a tombstoned run behind.
    doc.transact(|tx| tx.text(b"t").delete(0, 4));
    assert_eq!(text_of(&doc, b"t"), "");

    let mut back = Document::decode_state(&doc.encode_state()).expect("its own snapshot decodes");
    back.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(
        text_of(&back, b"t"),
        "A",
        "the write landed on a tombstoned planted id and was dropped as a replay"
    );
}

#[test]
fn a_counter_run_leaves_no_stamp_behind_and_the_record_still_holds() {
    // The same gap from the other direction: a counter tally persists no stamp at
    // all, so a record rebuilt from a snapshot's content would not see the ids a
    // counter op consumed. Stored, it does.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    assert!(doc.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    let mut plant = Document::new(victim).transact(|tx| {
        tx.inc(b"c", 1);
    });
    for op in plant.iter_mut() {
        op.stamp.lamport = LAMPORT_WIRE_CEILING + 1;
        doc.apply(op);
    }

    let back = Document::decode_state(&doc.encode_state()).expect("its own snapshot decodes");
    let mut back = back;
    let minted = back.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0].stamp;
    assert!(
        minted.lamport > LAMPORT_WIRE_CEILING + 1,
        "the reload minted onto an id a counter op already spent"
    );
}

#[test]
fn a_projection_keeps_the_recipients_own_high_water() {
    // A projection drops content wholesale, so a record derived from content could
    // not survive one. Stored on the document, it does — and the projection cuts it
    // to the recipient's own entry, on exactly the reasoning the causal frontier is
    // cut on (`state_project_seen` pins the leak this closes).
    let victim = cid(1);
    let server = cid(7);

    let mut room = Document::new(server);
    room.set_schema(schema_with_zone());
    // The plant sits above the wire ceiling, so the room's own clock is clamped
    // below it and cannot stand in for the record. And it is a **counter** tally,
    // which persists no stamp at all — so the projection's own re-floor cannot see
    // it either, and the recipient's stored entry is the only thing left that does.
    assert!(room.apply(&op_at_lamport(cid(3), b"k", LAMPORT_WIRE_CEILING)));
    let mut plant = Document::new(victim).transact(|tx| tx.inc(b"c", 1));
    for op in plant.iter_mut() {
        op.stamp.lamport = LAMPORT_WIRE_CEILING + 1;
        assert!(room.apply(op));
    }
    // Into a zone the recipient may not read, so the projection has content to drop.
    for op in &Document::new(cid(4)).transact(|tx| {
        tx.map(b"board").set(b"hidden", Scalar::Int(1));
    }) {
        room.apply(op);
    }

    room.project_zones(&schema_with_zone(), &Default::default(), Some(victim));
    assert!(
        room.get(b"board").is_none(),
        "the withheld partition survived the projection"
    );
    let mut adopted = Document::decode_state_as(victim, 0, &room.encode_state())
        .expect("a projected snapshot decodes");
    let minted = adopted.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0].stamp;
    assert!(
        minted.lamport > LAMPORT_WIRE_CEILING + 1,
        "the projection dropped the recipient's own record"
    );
}

#[test]
fn two_ops_at_one_stamp_into_one_list_still_leave_a_loadable_snapshot() {
    // `encode_state` must never emit bytes `decode_state` refuses — the invariant
    // C18 is about — and a placement was the last way it could. `List::insert_at` is
    // idempotent on the id, but the placement push was not: two ops carrying one
    // stamp into one children list stored the placement twice, and `read_state`
    // refuses a duplicate. Durable, because the room replica folds both, compaction
    // writes the snapshot, and no restart can load it.
    //
    // Two ops can carry one stamp and both pass every gate: dedup is on `OpId`, and
    // the id-space record only bounds an *honest* mint. They need not even name the
    // same child — `xml_child_id` mixes the kind in, so a tagged and a tagless insert
    // at one stamp derive different ids, which is why the tagless variant is run too.
    let victim = cid(1);
    for tagged in [true, false] {
        let mut doc = Document::new(victim);
        let batch = doc.transact(|tx| {
            tx.xml_fragment(b"f").children().insert_element(0, b"a");
        });
        let insert = batch
            .iter()
            .position(|op| matches!(op.kind, OpKind::XmlInsertChild { .. }))
            .expect("the child insert");
        let mut twin = batch[insert].clone();
        twin.id.seq = 9_000;
        if !tagged {
            if let OpKind::XmlInsertChild { tag, .. } = &mut twin.kind {
                *tag = None;
            }
        }
        assert_eq!(
            twin.stamp, batch[insert].stamp,
            "the twin carries one stamp"
        );
        assert!(
            doc.apply(&twin),
            "the twin was refused, so nothing is being pinned (tagged={tagged})"
        );

        // A placement is only *stored* for a node with more than one, or a
        // tombstoned one — so tombstoning every child is what makes both colliding
        // placements reach the encoding. Without that the tagless half writes no
        // placement at all and passes with the guard deleted: its two ops derive
        // different child ids, so each node holds a single live placement.
        doc.transact(|tx| {
            let mut frag = tx.xml_fragment(b"f");
            let mut children = frag.children();
            while !children.is_empty() {
                children.delete(0);
            }
        });

        Document::decode_state(&doc.encode_state()).unwrap_or_else(|e| {
            panic!("a replica could not load its own snapshot (tagged={tagged}): {e:?}")
        });
    }
}

#[test]
fn a_buffered_op_at_lamport_zero_leaves_the_snapshot_byte_stable() {
    // `record_stamp` stores no entry for a zero reach, so the decode-side buffer
    // floor must not create one either — otherwise a decoded replica declares a
    // record the encoder never wrote, and `encode_state`'s byte-stability contract
    // breaks on a re-encode. A projection's re-floor would then carry the invented
    // entry into a live document.
    let victim = cid(1);
    let mut plant = impersonated_run(victim, b"t", "M", 1);
    let run = plant.pop().expect("the run op");
    let mut zero = run;
    zero.stamp.lamport = 0;
    zero.id.seq = 7_000;

    let mut doc = Document::new(victim);
    assert!(!doc.apply(&zero), "the run waits on its container");
    let bytes = doc.encode_state();
    let back = Document::decode_state(&bytes).expect("its own snapshot decodes");
    assert_eq!(
        back.encode_state(),
        bytes,
        "the decode invented a zero record entry the encoder never wrote"
    );
}

#[test]
fn a_projection_withholds_every_other_clients_entry_and_re_floors() {
    // The two halves of the scrub, each pinned on its own. The *withholding* half is
    // a privacy rule: another client's entry counts what that replica minted inside
    // the partition this projection drops, so serving it lets a zone-scoped
    // subscriber read how busy a zone it cannot see is. The *re-floor* half is a
    // correctness rule: a projected document must still dominate the content that
    // survived, or its own snapshot decodes to something different from itself.
    let recipient = cid(1);
    let other = cid(2);

    let mut room = Document::new(cid(7));
    room.set_schema(schema_with_zone());
    // The other client's activity is a counter in the withheld zone — it persists no
    // stamp, so only its record entry could reveal it. Stamped far above anything the
    // surviving content reaches, so the probe below cannot be satisfied by a clock.
    let mut author = Document::new(other);
    author.set_schema(schema_with_zone());
    let mut hidden = author.transact(|tx| {
        tx.map(b"board").inc(b"hits", 1);
    });
    for op in hidden.iter_mut() {
        assert_eq!(op.zone, Some(0), "the batch belongs to the withheld zone");
        op.stamp.lamport = 500;
        room.apply(op);
    }
    // Content the recipient keeps, authored by a third client, so the re-floor has
    // something to recover that the scrub does not hand back.
    for op in &impersonated_run(cid(3), b"t", "MMMM", 90) {
        room.apply(op);
    }
    let kept = all_text_ids(&room, b"t");

    assert_eq!(
        room.zone_clock(None),
        93,
        "the zoned batch left the root clock alone"
    );
    room.project_zones(&schema_with_zone(), &Default::default(), Some(recipient));
    assert!(room.get(b"board").is_none(), "the withheld zone survived");
    let bytes = room.encode_state();

    // Withheld: nothing in the served bytes carries the other client's position.
    let served = Document::decode_state(&bytes).expect("a projected snapshot decodes");
    let mut probe = Document::decode_state_as(other, 0, &bytes).expect("decodes");
    let minted = probe.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0].stamp;
    assert!(
        minted.lamport < 500,
        "the projection served the withheld zone's author position: {}",
        minted.lamport
    );

    // Re-floored: the surviving content is still dominated, so the snapshot decodes
    // to itself and a re-encode is byte-identical.
    assert_eq!(
        served.encode_state(),
        bytes,
        "a projected snapshot does not decode to itself"
    );
    let mut adopted = Document::decode_state_as(cid(3), 0, &bytes).expect("decodes");
    let re = adopted.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0].stamp;
    assert!(
        !kept.contains(&re),
        "the adopting replica re-issued an id the projected content still holds"
    );
}

#[test]
fn a_snapshot_cannot_under_declare_a_tombstoned_runs_tail() {
    // The floor has to read a run's *reach*, not its head. A dead sequence run
    // encodes as `(head, length)` and only the head is a stamp on the wire, so a
    // snapshot that plants a run, lets it be deleted, and then declares a record at
    // the head under-declares by the whole length of its own tombstone — and the
    // length is right there in the record, three bytes along.
    //
    // Deleting a plant is the obvious thing a user does, so this is the mainline
    // path: the reload minted straight onto ids the bytes it had just decoded held,
    // and the victim's next write was swallowed as a replay.
    let victim = cid(1);
    let mut doc = Document::new(victim);
    for op in &impersonated_run(victim, b"t", "MMMMMMMM", 100) {
        assert!(doc.apply(op));
    }
    let run: Vec<Stamp> = all_text_ids(&doc, b"t");
    assert_eq!(run.len(), 8);
    doc.transact(|tx| tx.text(b"t").delete(0, 8));
    assert_eq!(text_of(&doc, b"t"), "", "the plant is a tombstone now");

    // Declare both the clock and the record at the run's *head*: values the decode's
    // own floor would produce from the head alone, so nothing is refused.
    let lowered = with_stamp_high_water(with_root_clock(doc.encode_state(), 100), &[(victim, 100)]);
    let mut back = Document::decode_state(&lowered).expect("a decodable snapshot");
    back.transact(|tx| tx.text(b"t").insert(0, "A"));
    assert_eq!(
        text_of(&back, b"t"),
        "A",
        "the write landed on a tombstoned id"
    );
    let minted = all_text_ids(&back, b"t")[0];
    assert!(
        !run.contains(&minted),
        "re-issued an id the tombstoned run still holds"
    );
}

#[test]
fn a_snapshots_waiting_buffer_is_counted_even_when_the_record_omits_it() {
    // An op waiting on its target is not in the content, so only the encoded buffer
    // carries its ids — and its reservation is as published as an applied op's. A
    // declaration that omits it must not let the mint re-issue it.
    let victim = cid(1);
    let mut plant = impersonated_run(victim, b"t", "MMMM", 60);
    for (i, op) in plant.iter_mut().enumerate() {
        op.id.seq = 5_000 + i as u64;
    }
    let run = plant.pop().expect("the run op");
    assert!(matches!(run.kind, OpKind::TextInsert { .. }));

    let mut doc = Document::new(victim);
    assert!(!doc.apply(&run), "the run waits on its container");
    let stripped = with_stamp_high_water(with_root_clock(doc.encode_state(), 0), &[]);

    let mut back = Document::decode_state(&stripped).expect("a decodable snapshot");
    let minted = back.transact(|tx| tx.set(b"probe", Scalar::Int(1)))[0].stamp;
    assert!(
        minted.lamport > 63,
        "the mint re-issued an id the waiting buffer holds"
    );
}

#[test]
fn a_snapshot_cannot_under_declare_the_ids_it_visibly_holds() {
    // The record is stored, and a stored figure is supplied by whoever hands the
    // bytes over — exactly the kind of input this whole unit says a mint must not
    // trust. So the declaration only ever *raises*: it is floored by every stamp
    // the decode reads, and under-declaring buys nothing.
    //
    // Without the floor, a 24-byte edit to an otherwise honest snapshot — dropping
    // the record to zero entries — hands the reload the planted ids back and the
    // victim's next write is dropped as a replay, on it and on every peer.
    let victim = cid(1);
    let attacker = cid(9);

    let mut doc = Document::new(victim);
    assert!(doc.apply(&op_at_lamport(attacker, b"k", LAMPORT_WIRE_CEILING)));
    for op in &impersonated_run(victim, b"t", "MMMM", LAMPORT_WIRE_CEILING + 1) {
        doc.apply(op);
    }
    let live = all_text_ids(&doc, b"t");

    for declared in [
        Vec::new(),
        vec![(victim, 0)],
        vec![(victim, LAMPORT_WIRE_CEILING)],
    ] {
        let lowered = with_stamp_high_water(doc.encode_state(), &declared);
        let mut back = Document::decode_state(&lowered).expect("a decodable snapshot");
        back.transact(|tx| tx.text(b"t").insert(0, "A"));
        assert_eq!(
            text_of(&back, b"t"),
            "AMMMM",
            "an under-declared record handed the mint a live id"
        );
        let after = all_text_ids(&back, b"t");
        assert_all_distinct(&after, "under-declared record");
        assert!(
            !live.contains(&after[0]),
            "re-issued a stamp the state carried"
        );
    }
}

#[test]
fn a_snapshot_declaring_a_record_past_the_id_space_is_refused() {
    // The declaration lands in the slot the next local mint reads, so it is bounded
    // on the same terms as a clock — refused above the ceiling, never clamped, since
    // lowering one hands the replica live ids back. And a repeated entry is refused
    // rather than resolved, so no decode has to pick a winner.
    let me = cid(1);
    let other = cid(2);
    let mut doc = Document::new(me);
    doc.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let bytes = doc.encode_state();

    for declared in [
        vec![(me, LAMPORT_STATE_CEILING + 1)],
        vec![(me, u64::MAX)],
        vec![(me, 5), (me, 6)],
    ] {
        assert!(
            Document::decode_state(&with_stamp_high_water(bytes.clone(), &declared)).is_err(),
            "a record at {declared:?} was accepted"
        );
    }
    // Exactly the ceiling is a legal declaration, and it spends the id space.
    let at = with_stamp_high_water(bytes.clone(), &[(me, LAMPORT_STATE_CEILING)]);
    let spent = Document::decode_state(&at).expect("a decodable snapshot");
    assert!(!spent.can_mint(None));
    // Another client's entry at the ceiling leaves this replica's own mint alone.
    let theirs = with_stamp_high_water(bytes, &[(other, LAMPORT_STATE_CEILING)]);
    let mine = Document::decode_state(&theirs).expect("a decodable snapshot");
    assert!(mine.can_mint(None));
}

#[test]
fn the_refusal_latch_spans_an_intention_and_no_further() {
    // The latch has to cover exactly one intention. An atomic group is several
    // `transact` calls and one all-or-nothing delivery, so clearing between them
    // would tear the group the latch exists to keep whole; and a *later* intention
    // has to get a fresh answer, or a run refused for its length would go on
    // refusing the single-id edits a subsequent undo is made of.
    let me = cid(1);

    // An atomic group whose first transact is refused **on span alone**, with room
    // left for the edits after it. That asymmetry is the whole test: at full
    // exhaustion every transact refuses on its own merits and the latch is never
    // consulted, so a group planted at the ceiling would pass with the latch gone.
    // Here the four-codepoint run does not fit and the single-id write after it
    // would — so only the latch keeps the group whole.
    let mut doc = Document::new(me);
    assert!(doc.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 2)));
    doc.begin_atomic();
    doc.transact(|tx| tx.text(b"t").insert(0, "abcd"));
    doc.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    let group = doc.commit_atomic();
    assert!(
        group
            .iter()
            .all(|op| !matches!(op.kind, OpKind::MapSet { .. })),
        "an edit after a refused one joined the group"
    );
    assert!(
        doc.get(b"b").is_none(),
        "a torn atomic group reached local state"
    );

    // Full exhaustion refuses the group outright, latch or no latch.
    let mut spent = Document::new(me);
    assert!(spent.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING)));
    spent.begin_atomic();
    spent.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    spent.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    assert!(
        spent.commit_atomic().is_empty(),
        "an exhausted atomic group reached the wire"
    );

    // An atomic group nested in an explicit intention joins it, so opening the
    // group must not hand the mint a fresh answer mid-intention.
    let mut nested = Document::new(me);
    assert!(nested.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 2)));
    nested.begin_intention();
    nested.transact(|tx| tx.text(b"t").insert(0, "abcd"));
    nested.begin_atomic();
    nested.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    let inner = nested.commit_atomic();
    nested.end_intention();
    assert!(
        inner.is_empty() && nested.get(b"b").is_none(),
        "opening a nested group cleared the latch mid-intention"
    );

    // And the latch does not outlive the intention. Here the text create fits and
    // the run behind it does not, so the batch is cut at the refusal — what the
    // latch guarantees is that nothing *after* it is emitted, not that the ops
    // before it are taken back (they are already applied locally, and dropping
    // them would diverge the author from its peers). One id is left, and the next
    // transaction gets it.
    let mut room = Document::new(me);
    assert!(room.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 2)));
    let cut = room.transact(|tx| tx.text(b"t").insert(0, "abcd"));
    assert!(
        cut.iter()
            .all(|op| !matches!(op.kind, OpKind::TextInsert { .. })),
        "a run reaching past the end of the space was emitted"
    );
    assert_eq!(text_of(&room, b"t"), "", "and none of it landed");
    let next = room.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    assert_eq!(
        next.len(),
        1,
        "the latch outlived the transaction that set it"
    );
    assert_eq!(next[0].stamp.lamport, LAMPORT_STATE_CEILING);
}

#[test]
fn a_refused_mint_takes_the_whole_transaction() {
    // A transact is one intention, and its later edits address what its earlier ones
    // created. If a refused create still let the writes into it through, they would
    // address a container no replica can ever hold — and a peer buffers such an op
    // forever, waiting on an arrival that cannot come. So the refusal takes the rest
    // of the transaction, and the next one starts clean.
    let me = cid(1);
    let mut doc = Document::new(me);
    doc.set_schema(schema_with_zone());
    assert!(doc.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING)));

    let ops = doc.transact(|tx| {
        tx.map(b"board").set(b"a", Scalar::Int(1));
        tx.set(b"b", Scalar::Int(2));
        tx.text(b"t").insert(0, "x");
    });
    assert!(
        ops.is_empty(),
        "a torn transaction emitted {} ops",
        ops.len()
    );
    assert!(doc.get(b"b").is_none() && doc.get(b"t").is_none());
    assert!(Document::decode_state(&doc.encode_state()).is_ok());
}

#[test]
fn a_planted_run_survives_a_reload_without_taking_the_next_mint() {
    // The record is stored beside the clocks, so it comes back off a snapshot —
    // otherwise the reload mints straight onto the plant that the snapshot's own
    // clamped clock does not cover.
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
    // One op under the victim's own id is enough to try to put its high-water at
    // the top of the space, and the replica has to stay total there: no panic, no
    // wrap, and no id it already holds. The whole region past
    // `LAMPORT_STATE_CEILING` is refused, so nothing of any of these lands and the
    // mint is untouched.
    //
    // A bound placed only on the last position of all — `(u64::MAX, u64::MAX)` —
    // is not enough, and this pins why: the position one below it was admitted,
    // parked the victim's high-water there, and made the *second* local edit after
    // it panic. Both edits run here.
    let victim = cid(1);
    for offset in [u64::MAX, u64::MAX - 1, 0] {
        let mut doc = Document::new(victim);
        let mut end = op_at_lamport(victim, b"k", u64::MAX);
        end.stamp.offset = offset;
        assert!(
            !doc.apply(&end),
            "a stamp past the id space is admissible at offset {offset}"
        );
        assert!(doc.get(b"k").is_none(), "nothing of it landed");

        let first = doc.transact(|tx| tx.set(b"a", Scalar::Int(1)))[0].stamp;
        let second = doc.transact(|tx| tx.set(b"b", Scalar::Int(2)))[0].stamp;
        assert_all_distinct(&[first, second], "mints after a refused top-of-space op");
        assert!(Document::decode_state(&doc.encode_state()).is_ok());
    }
}

#[test]
fn a_stamp_off_the_lamport_axis_is_refused_because_the_derived_ids_ignore_it() {
    // The sub-lamport `offset` is a tiebreak inside one run, not a second dimension
    // an id may live in: `stamp_key` — the derived-id input for an ACL tuple, a
    // ranged element and an XML child — is `lamport ++ client` and omits it. Two
    // stamps differing only there would derive one id and the second create would
    // be dropped. No honest stamp carries one, so the position is refused, which is
    // also what makes the lamport-only high-water a complete record.
    let attacker = cid(9);
    let mut doc = Document::new(cid(1));
    let mut off_axis = op_at_lamport(attacker, b"k", 4);
    off_axis.stamp.offset = 1;
    assert!(
        !doc.apply(&off_axis),
        "a stamp off the lamport axis is refused"
    );
    assert!(doc.get(b"k").is_none(), "nothing of it landed");

    // The same position on the axis is ordinary.
    let on_axis = op_at_lamport(attacker, b"k", 4);
    assert!(doc.apply(&on_axis));
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
    // Nine ids left: the text create, then one per codepoint, the last landing
    // exactly on the ceiling.
    let at = with_root_clock(src.encode_state(), LAMPORT_STATE_CEILING - 9);

    let mut doc = Document::decode_state(&at).expect("a decodable snapshot");
    doc.transact(|tx| tx.text(b"t").insert(0, "ABCDEFGH"));
    assert!(!doc.can_mint(None), "the run took the space to its end");
    Document::decode_state(&doc.encode_state()).expect("a replica can reload its own snapshot");
    assert_eq!(text_of(&doc, b"t"), "ABCDEFGH");
    assert_all_distinct(&all_text_ids(&doc, b"t"), "ceiling run");
}

#[test]
fn every_reachable_clock_round_trips_and_never_re_issues_a_stamp() {
    // The sweep the two halves have to hold together on: every start a snapshot
    // may legally declare, edited and reloaded, with **no iteration excused** —
    // including the two at the very top, where the space runs out mid-sweep and
    // the answer is a refusal rather than a lost write.
    //
    // The rule each iteration is held to, whether or not there was room: the
    // reload decodes, the ids it decoded are exactly where they were, no stamp is
    // re-issued, and a write lands *precisely* when an op was emitted for it — so
    // a refusal can never present as a silent drop.
    for start in [
        0,
        1 << 20,
        LAMPORT_WIRE_CEILING - 1,
        LAMPORT_WIRE_CEILING,
        LAMPORT_WIRE_CEILING + 1,
        LAMPORT_STATE_CEILING - 5,
        LAMPORT_STATE_CEILING - 4,
        LAMPORT_STATE_CEILING - 1,
        LAMPORT_STATE_CEILING,
    ] {
        let seed = with_root_clock(Document::new(cid(1)).encode_state(), start);
        let mut doc = Document::decode_state_as(cid(1), 0, &seed).expect("decodes");
        doc.transact(|tx| tx.text(b"t").insert(0, "AB"));
        let live = text_ids_or_none(&doc, b"t");

        let mut back = Document::decode_state(&doc.encode_state()).unwrap_or_else(|e| {
            panic!("a replica could not reload its own snapshot at {start}: {e:?}")
        });
        let emitted = back.transact(|tx| tx.text(b"t").insert(live.len(), "C"));
        let after = text_ids_or_none(&back, b"t");

        assert_eq!(
            after[..live.len()],
            live[..],
            "reload moved the ids it decoded at {start}"
        );
        assert_all_distinct(&after, &format!("reload at {start}"));
        let inserted = emitted
            .iter()
            .any(|op| matches!(op.kind, OpKind::TextInsert { .. }));
        assert_eq!(
            after.len() > live.len(),
            inserted,
            "a write landed without an op, or an op landed nowhere, at {start}"
        );
        assert_eq!(
            inserted,
            start <= LAMPORT_STATE_CEILING - 5,
            "the space ran out at a different start than {start}"
        );
        for op in &emitted {
            assert!(
                op.stamp.lamport <= LAMPORT_STATE_CEILING,
                "minted past the id space at {start}"
            );
        }
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

#[test]
fn a_saturated_high_water_refuses_rather_than_re_issuing_one_stamp() {
    // The measured boundary that forces the mint to be refusable at all. A stamp
    // on the last id of the space is a legal position, so it is admitted and it
    // saturates its author's high-water. From there:
    //
    // * clamping the record re-issues: the mint would take `CEILING + 1`, record it
    //   back down to `CEILING`, and take `CEILING + 1` again on the next edit —
    //   one id for every later edit, and one derived `acl_id`/`ranged_id`/XML-child
    //   id too, since those read the lamport and the client alone;
    // * not clamping it leaves the replica unable to reload its own snapshot.
    //
    // So the mint refuses, visibly, and nothing is re-issued.
    let me = cid(1);
    let mut doc = Document::new(me);
    assert!(
        doc.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING)),
        "the last id of the space is a legal position"
    );

    assert!(!doc.can_mint(None), "the id space is spent");
    let first = doc.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    let second = doc.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    assert!(
        first.is_empty() && second.is_empty(),
        "a refused edit emits nothing"
    );
    assert!(doc.get(b"a").is_none(), "and changes no state");
    assert!(
        Document::decode_state(&doc.encode_state()).is_ok(),
        "the replica can still reload its own snapshot"
    );

    // One id below it, the same edits mint and stay distinct.
    let mut room = Document::new(me);
    assert!(room.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 1)));
    let minted = room.transact(|tx| tx.set(b"a", Scalar::Int(1)))[0].stamp;
    assert_eq!(minted.lamport, LAMPORT_STATE_CEILING);
    assert!(!room.can_mint(None), "and that was the last one");
}

#[test]
fn a_refused_edit_is_reported_where_an_empty_batch_says_nothing() {
    // A refused edit and an inert one both return no ops, so the batch is not the
    // signal. `mint_refused` is, and it has to survive the close of the intention
    // that set it or the caller that ran the transact has nothing to read.
    let me = cid(1);
    let mut doc = Document::new(me);
    assert!(!doc.mint_refused(), "a fresh replica reported a refusal");

    // An inert edit — the key already holds this value — emits nothing and was not
    // refused. This is the case the empty batch cannot tell from a refusal.
    doc.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let inert = doc.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    assert!(!doc.mint_refused(), "an ordinary edit reported a refusal");
    let _ = inert;

    assert!(doc.apply(&plant_at_lamport(me, b"planted", LAMPORT_STATE_CEILING, 99)));
    let refused = doc.transact(|tx| tx.set(b"a", Scalar::Int(2)));
    assert!(refused.is_empty());
    assert!(doc.mint_refused(), "the refusal was not reported");

    // The report is about the intention most recently opened, so a replica that
    // recovers room answers for its next edit rather than forever.
    let mut room = Document::new(me);
    assert!(room.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 2)));
    room.transact(|tx| tx.text(b"t").insert(0, "abcd"));
    assert!(
        room.mint_refused(),
        "the run did not fit and was not reported"
    );
    room.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    assert!(
        !room.mint_refused(),
        "the next intention inherited the refusal"
    );
}

#[test]
fn capacity_and_refusal_are_separate_questions() {
    // `can_mint` reports capacity between operations; `mint_refused` reports what
    // the last intention did. They part company exactly where a run is refused for
    // its length: a single-id edit still fits, so capacity is intact, and the edit
    // that was attempted was refused. One predicate answering both questions can
    // only be wrong about one of them.
    let me = cid(1);
    let mut doc = Document::new(me);
    assert!(doc.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 2)));

    doc.begin_atomic();
    doc.transact(|tx| tx.text(b"t").insert(0, "abcd"));
    assert!(
        doc.can_mint(None),
        "capacity for one id was reported as none"
    );
    assert!(doc.mint_refused(), "the refusal inside the group is unread");
    // The latch still governs the rest of the group: capacity says nothing about
    // whether the next edit in this intention will be taken.
    doc.transact(|tx| tx.set(b"b", Scalar::Int(2)));
    assert!(doc.get(b"b").is_none(), "a torn group reached local state");
    let group = doc.commit_atomic();
    assert!(
        group
            .iter()
            .all(|op| !matches!(op.kind, OpKind::MapSet { .. })),
        "an edit after the refused one joined the group"
    );
    assert!(
        doc.mint_refused(),
        "the group's refusal is unread at commit"
    );

    // Real exhaustion answers false on both.
    let mut spent = Document::new(me);
    assert!(spent.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING)));
    assert!(!spent.can_mint(None));
    spent.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    assert!(spent.mint_refused());
}

#[test]
fn an_undo_replay_reports_its_own_refusal() {
    // A replay is a fresh intention, so it clears the report on the way in and
    // leaves its own answer behind — an undo that could not mint its inverse is as
    // silent as any other refused edit without it.
    let me = cid(1);
    let mut doc = Document::new(me);
    doc.set_undo_origin(b"seat");
    doc.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    assert!(!doc.mint_refused());

    assert!(doc.apply(&plant_at_lamport(me, b"planted", LAMPORT_STATE_CEILING, 99)));
    assert!(doc.undo(b"seat").is_some_and(|ops| ops.is_empty()));
    assert!(doc.mint_refused(), "a refused undo reported nothing");
}

#[test]
fn the_latch_spans_an_intention_a_nested_group_only_joins() {
    // An atomic group nested in an explicit intention joins that intention, so
    // neither end of the group is an intention boundary: opening it must not hand
    // the mint a fresh answer, and closing it must not either. The latch therefore
    // covers every edit up to `end_intention`, and `mint_refused` still reports it
    // after the group has been committed.
    let me = cid(1);
    let mut doc = Document::new(me);
    assert!(doc.apply(&op_at_lamport(me, b"planted", LAMPORT_STATE_CEILING - 6)));

    doc.begin_intention();
    doc.begin_atomic();
    // A single-id edit fits and lands, so the group is genuinely *torn* rather than
    // empty: what came before the refusal is emitted and applied, and only what
    // follows it is cut.
    doc.transact(|tx| tx.set(b"before", Scalar::Int(1)));
    // Ten codepoints do not fit in the ids that are left; a single-id edit does.
    doc.transact(|tx| tx.text(b"t").insert(0, "abcdefghij"));
    let group = doc.commit_atomic();
    assert!(
        group
            .iter()
            .any(|op| matches!(op.kind, OpKind::MapSet { .. })),
        "the edit before the refusal was taken back out of the group"
    );
    assert!(
        group
            .iter()
            .all(|op| !matches!(op.kind, OpKind::TextInsert { .. })),
        "a run reaching past the end of the space was emitted"
    );
    assert!(
        doc.get(b"before").is_some(),
        "the landed edit left no state"
    );
    assert!(
        doc.mint_refused(),
        "the group's refusal is unread at commit"
    );

    doc.transact(|tx| tx.set(b"after", Scalar::Int(1)));
    assert!(
        doc.get(b"after").is_none(),
        "an edit after the refusal reached state inside the same intention"
    );
    doc.end_intention();

    // The intention is over, so the next one gets a fresh answer — and the space
    // that was there all along.
    assert!(doc.can_mint(None));
    let next = doc.transact(|tx| tx.set(b"after", Scalar::Int(1)));
    assert_eq!(next.len(), 1, "the latch outlived its intention");
    assert!(!doc.mint_refused());
}

#[test]
fn an_edit_that_resolves_to_nothing_is_not_a_refusal() {
    // An inert edit and a refused one both emit nothing, which is the whole reason
    // `mint_refused` exists — so a mutator that resolves to nothing must answer for
    // *itself* rather than leave the previous edit's refusal standing. Several path
    // mutators return early without reaching a cursor (a delete naming no live item,
    // an XML insert on a path that is not an XML node), and those are exactly the
    // ones that would report a stale answer.
    let me = cid(1);
    let mut doc = Document::new(me);
    doc.transact(|tx| {
        tx.text(b"t").insert(0, "ab");
    });
    assert!(doc.apply(&plant_at_lamport(
        me,
        b"planted",
        LAMPORT_STATE_CEILING - 6,
        99
    )));

    let text = path::encode_path(&[b"t"]);
    let nowhere = path::encode_path(&[b"nope"]);
    // A run longer than the space that is left refuses; capacity survives it, so
    // each probe below starts from a raised report on a replica that can still mint.
    let refuse = |doc: &mut Document| {
        path::text_insert(doc, &text, 0, "abcdefghij");
        assert!(doc.mint_refused(), "the run was not refused");
        assert!(doc.can_mint(None), "the single-id space was spent");
    };

    // Each of these resolves to nothing, and each must answer for itself rather
    // than inherit the refusal standing when it was called.
    refuse(&mut doc);
    assert!(path::xml_insert_element(&mut doc, &nowhere, 0, b"p").is_empty());
    assert!(
        !doc.mint_refused(),
        "an XML insert on an absent node reported the previous refusal"
    );

    refuse(&mut doc);
    assert!(path::list_delete(&mut doc, &nowhere, 3).is_empty());
    assert!(
        !doc.mint_refused(),
        "a delete naming no live item reported the previous refusal"
    );

    refuse(&mut doc);
    let (ops, id) = path::mark(
        &mut doc,
        &nowhere,
        0,
        Side::Left,
        1,
        Side::Right,
        b"b",
        Scalar::Bool(true),
    );
    assert!(ops.is_empty() && id.is_none());
    assert!(
        !doc.mint_refused(),
        "a mark over an absent sequence reported the previous refusal"
    );

    // And the replica really did still have room throughout.
    assert!(!path::text_insert(&mut doc, &text, 0, "z").is_empty());
    assert!(!doc.mint_refused());
}
