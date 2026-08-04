//! Atomic transactions — opt-in all-or-nothing visibility.
//!
//! A plain `transact` streams its ops: each merges independently on arrival, so
//! a peer can observe a partial group. An `atomic_transact` instead tags its ops
//! as one transaction; a receiver holds the members until the whole group is
//! present, then applies them together, so no peer ever sees a partial
//! transaction. Atomicity is a *view* guarantee — the same ops still merge, so an
//! atomic author and a non-atomic peer converge on identical state.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Tx;
use crdtsync_core::stamp::LAMPORT_STATE_CEILING;
use crdtsync_core::{ClientId, Element, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn reg(d: &Document, key: &[u8]) -> Option<Scalar> {
    match d.get(key) {
        Some(Element::Register(r)) => Some(r.borrow().read().clone()),
        _ => None,
    }
}

#[test]
fn atomic_transact_tags_every_member_with_one_tx() {
    let mut d = doc(1);
    let ops = d.atomic_transact(|tx| {
        tx.register(b"first", Scalar::Int(1));
        tx.register(b"last", Scalar::Int(2));
    });
    assert_eq!(ops.len(), 2);
    let txs: Vec<Tx> = ops.iter().map(|o| o.tx.clone().expect("tagged")).collect();
    assert_eq!(txs[0].id, txs[1].id, "members share one tx id");
    assert!(
        txs.iter().all(|t| t.count == 2),
        "each member knows the group size"
    );
}

#[test]
fn two_atomic_transactions_get_distinct_ids() {
    let mut d = doc(1);
    let a = d.atomic_transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
    });
    let b = d.atomic_transact(|tx| {
        tx.register(b"b", Scalar::Int(1));
    });
    assert_ne!(a[0].tx.as_ref().unwrap().id, b[0].tx.as_ref().unwrap().id);
}

#[test]
fn a_partial_atomic_transaction_is_invisible_until_it_commits() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });

    // The first member alone shows nothing: a partial tx is held.
    assert!(!b.apply(&ops[0]), "an incomplete tx member is buffered");
    assert_eq!(reg(&b, b"x"), None);
    assert_eq!(reg(&b, b"y"), None);

    // The last member commits the whole group at once.
    assert!(b.apply(&ops[1]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn an_atomic_transaction_commits_regardless_of_delivery_order() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });

    // Deliver the members in reverse; the group still stays hidden until whole.
    assert!(!b.apply(&ops[1]));
    assert_eq!(reg(&b, b"y"), None);
    assert!(b.apply(&ops[0]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn an_atomic_author_and_a_nonatomic_peer_converge() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });
    for op in &ops {
        b.apply(op);
    }
    assert_eq!(reg(&b, b"x"), reg(&a, b"x"));
    assert_eq!(reg(&b, b"y"), reg(&a, b"y"));
}

#[test]
fn an_atomic_transaction_spanning_a_nested_create_is_atomic() {
    let mut a = doc(1);
    let mut b = doc(2);
    // Create a nested map and set a slot inside it as one gesture.
    let ops = a.atomic_transact(|tx| {
        tx.map(b"profile").register(b"name", Scalar::Int(7));
    });
    assert!(ops.len() >= 2, "a create plus a set");

    // Deliver all but the last: nothing is visible, even the container.
    for op in &ops[..ops.len() - 1] {
        assert!(!b.apply(op));
    }
    assert!(b.get(b"profile").is_none());

    // The final member commits the whole tx.
    assert!(b.apply(&ops[ops.len() - 1]));
    let child = match b.get(b"profile") {
        Some(Element::Map(m)) => m,
        _ => panic!("nested map missing after commit"),
    };
    let slot = child.borrow().get(b"name");
    match slot {
        Some(Element::Register(r)) => assert_eq!(r.borrow().read().clone(), Scalar::Int(7)),
        _ => panic!("nested slot missing after commit"),
    }
}

#[test]
fn replaying_a_committed_member_is_a_no_op() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });
    for op in &ops {
        b.apply(op);
    }
    // A resend of any member after commit changes nothing.
    assert!(!b.apply(&ops[0]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
}

#[test]
fn a_single_op_atomic_transaction_applies_immediately() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"solo", Scalar::Int(9));
    });
    assert_eq!(ops.len(), 1);
    assert!(b.apply(&ops[0]), "a complete one-member tx applies at once");
    assert_eq!(reg(&b, b"solo"), Some(Scalar::Int(9)));
}

#[test]
fn a_buffered_partial_tx_survives_a_snapshot_round_trip() {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });

    let mut b = doc(2);
    b.apply(&ops[0]); // partial: buffered, invisible

    // A snapshot taken mid-tx preserves the held member, and the decoded replica
    // still commits when the rest arrives.
    let snap = b.encode_state();
    let mut restored = Document::decode_state(&snap).expect("decode");
    assert_eq!(reg(&restored, b"x"), None);
    restored.apply(&ops[1]);
    assert_eq!(reg(&restored, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&restored, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn begin_and_commit_group_separate_edits_into_one_tx() {
    let mut d = doc(1);
    d.begin_atomic();
    assert!(d.is_atomic());
    // Each edit accumulates and returns nothing of its own while recording.
    assert!(d.transact(|c| c.register(b"x", Scalar::Int(1))).is_empty());
    assert!(d.transact(|c| c.register(b"y", Scalar::Int(2))).is_empty());
    let ops = d.commit_atomic();
    assert!(!d.is_atomic());
    assert_eq!(ops.len(), 2);
    let id = ops[0].tx.clone().expect("tagged").id;
    assert!(ops.iter().all(|o| o.tx.as_ref().unwrap().id == id));
    assert!(ops.iter().all(|o| o.tx.as_ref().unwrap().count == 2));
    // The author sees its own edits immediately.
    assert_eq!(reg(&d, b"x"), Some(Scalar::Int(1)));
}

#[test]
fn committing_with_no_recorded_edits_yields_nothing() {
    let mut d = doc(1);
    d.begin_atomic();
    assert!(d.commit_atomic().is_empty());
}

#[test]
fn a_begin_commit_group_commits_atomically_on_a_peer() {
    let mut a = doc(1);
    let mut b = doc(2);
    a.begin_atomic();
    a.transact(|c| c.register(b"x", Scalar::Int(1)));
    a.transact(|c| c.register(b"y", Scalar::Int(2)));
    let ops = a.commit_atomic();

    assert!(!b.apply(&ops[0]));
    assert_eq!(reg(&b, b"x"), None);
    assert!(b.apply(&ops[1]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn ops_from_a_plain_transact_carry_no_tx() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.register(b"k", Scalar::Int(1));
    });
    assert!(ops.iter().all(|o: &Op| o.tx.is_none()));
}

#[test]
fn a_tx_textinsert_at_the_lamport_ceiling_keeps_every_codepoint() {
    // A complete one-member atomic tx runs the readiness check, whose TextInsert
    // arm derives a char_id stamp per codepoint from the op's wire-derived
    // lamport. At the very top of the id space that derivation must neither
    // overflow-panic nor collapse two codepoints onto one saturated stamp: every
    // codepoint survives with a distinct id, through the public apply() boundary.
    //
    // The run is based so its *last* codepoint lands exactly on
    // `LAMPORT_STATE_CEILING`, the highest id any stamp may occupy — a run
    // reaching past it reserves ids that do not exist and is refused whole
    // (`a_tx_textinsert_past_the_id_space_is_refused_whole`), so this is the
    // furthest a surviving run can reach.
    let mut d = doc(1);
    d.transact(|tx| {
        tx.text(b"body");
    });
    let text_id = match d.get(b"body") {
        Some(Element::Text(t)) => t.borrow().id(),
        _ => panic!("text not created"),
    };

    let attacker = cid(9);
    let op = Op {
        id: crdtsync_core::OpId {
            client: attacker,
            seq: 0,
        },
        stamp: crdtsync_core::Stamp {
            lamport: LAMPORT_STATE_CEILING - 1,
            client: attacker,
            offset: 0,
        },
        target: text_id,
        kind: crdtsync_core::OpKind::TextInsert {
            s: "ab".to_string(),
            anchor: crdtsync_core::Anchor {
                parent: None,
                side: crdtsync_core::Side::Right,
            },
        },
        tx: Some(Tx {
            id: crdtsync_core::TxId(0),
            count: 1,
        }),
        zone: None,
    };
    assert!(d.apply(&op));

    let text = match d.get(b"body") {
        Some(Element::Text(t)) => t,
        _ => panic!("text missing"),
    };
    let text = text.borrow();
    assert_eq!(text.as_string(), "ab");
    assert_eq!(text.len(), 2);
    let ids = text.node_ids(0, 2);
    assert_eq!(ids.len(), 2);
    assert_ne!(
        ids[0], ids[1],
        "codepoints must not collapse to one char_id"
    );
}

#[test]
fn a_tx_textinsert_past_the_id_space_is_refused_whole() {
    // The other side of the same boundary. A run reserves one id per codepoint, so
    // a base inside the space can still reach past it — and the ids past the end do
    // not exist, so the op is refused rather than saturated onto the last one. The
    // refusal is on the reservation, not the base, and it holds through the atomic
    // path exactly as it does through the plain one: a member the readiness check
    // would fold must not reach state by the group seam instead.
    let mut d = doc(1);
    d.transact(|tx| {
        tx.text(b"body");
    });
    let text_id = match d.get(b"body") {
        Some(Element::Text(t)) => t.borrow().id(),
        _ => panic!("text not created"),
    };

    let attacker = cid(9);
    let op = Op {
        id: crdtsync_core::OpId {
            client: attacker,
            seq: 0,
        },
        stamp: crdtsync_core::Stamp {
            // One past the base the sibling test uses: the second codepoint would
            // land on `LAMPORT_STATE_CEILING + 1`.
            lamport: LAMPORT_STATE_CEILING,
            client: attacker,
            offset: 0,
        },
        target: text_id,
        kind: crdtsync_core::OpKind::TextInsert {
            s: "ab".to_string(),
            anchor: crdtsync_core::Anchor {
                parent: None,
                side: crdtsync_core::Side::Right,
            },
        },
        tx: Some(Tx {
            id: crdtsync_core::TxId(1),
            count: 1,
        }),
        zone: None,
    };
    assert!(!d.apply(&op), "a run reaching past the id space is refused");

    let text = match d.get(b"body") {
        Some(Element::Text(t)) => t,
        _ => panic!("text missing"),
    };
    assert_eq!(text.borrow().len(), 0, "no codepoint of it landed");
}

/// The live length of the List in a top-level slot.
fn list_len(d: &Document, key: &[u8]) -> usize {
    match d.get(key) {
        Some(Element::List(l)) => l.borrow().len(),
        _ => panic!("slot holds no list"),
    }
}

/// The text in a top-level slot.
fn text(d: &Document, key: &[u8]) -> String {
    match d.get(key) {
        Some(Element::Text(t)) => t.borrow().as_string(),
        _ => panic!("slot holds no text"),
    }
}

/// A slot inside the nested map at `key`.
fn nested(d: &Document, key: &[u8], slot: &[u8]) -> Option<Scalar> {
    match d.get(key) {
        Some(Element::Map(m)) => match m.borrow().get(slot) {
            Some(Element::Register(r)) => Some(r.borrow().read().clone()),
            _ => None,
        },
        _ => None,
    }
}

/// The op set that displaces a group's own container: `a` opens a sequence at a
/// slot `b`'s concurrent register then wins, and `a` later re-creates the
/// sequence at a higher stamp, taking the slot back. Returns `(b`'s register,
/// the losing group, the winning group)`.
fn displaced_group<F>(edit: F) -> (Document, Document, Vec<Op>, Vec<Op>, Vec<Op>)
where
    F: Fn(&mut Document, u8) -> Vec<Op>,
{
    let mut a = doc(1);
    let mut b = doc(2);
    // Same lamport, higher client: b's register beats a's first create.
    let reg = b.atomic_transact(|tx| {
        tx.register(b"k", Scalar::Int(7));
    });
    let losing = edit(&mut a, 1);
    for op in &reg {
        a.apply(op);
    }
    // Now stamped above the register, so this create takes the slot back.
    let winning = edit(&mut a, 2);
    (a, b, reg, losing, winning)
}

#[test]
fn a_group_member_survives_its_own_create_losing_the_slot() {
    let (a, mut b, _reg, losing, winning) = displaced_group(|d, n| {
        d.atomic_transact(|tx| {
            tx.list(b"k").insert(0, Scalar::Int(n as i64));
        })
    });
    for op in losing.iter().chain(winning.iter()) {
        b.apply(op);
    }
    // Both replicas folded in every op — the loss is not a buffered-forever
    // artifact, it is an insert that applied into a displaced sequence and
    // vanished.
    assert_eq!(
        a.seen().count(),
        b.seen().count(),
        "both replicas applied the same ops"
    );
    assert_eq!(list_len(&a, b"k"), 2, "the author holds both inserts");
    assert_eq!(list_len(&b, b"k"), list_len(&a, b"k"), "list diverged");
}

#[test]
fn a_group_text_member_survives_its_own_create_losing_the_slot() {
    let (a, mut b, _reg, losing, winning) = displaced_group(|d, n| {
        let s = if n == 1 { "x" } else { "y" };
        d.atomic_transact(|tx| {
            tx.text(b"k").insert(0, s);
        })
    });
    for op in losing.iter().chain(winning.iter()) {
        b.apply(op);
    }
    assert_eq!(a.seen().count(), b.seen().count());
    assert_eq!(text(&a, b"k"), "yx", "the author holds both runs");
    assert_eq!(text(&b, b"k"), text(&a, b"k"), "text diverged");
}

#[test]
fn a_group_map_member_survives_its_own_create_losing_the_slot() {
    let (a, mut b, _reg, losing, winning) = displaced_group(|d, n| {
        let slot = if n == 1 { b"x".to_vec() } else { b"y".to_vec() };
        d.atomic_transact(move |tx| {
            tx.map(b"k").register(&slot, Scalar::Int(n as i64));
        })
    });
    for op in losing.iter().chain(winning.iter()) {
        b.apply(op);
    }
    assert_eq!(a.seen().count(), b.seen().count());
    assert_eq!(nested(&a, b"k", b"x"), Some(Scalar::Int(1)));
    assert_eq!(
        nested(&b, b"k", b"x"),
        nested(&a, b"k", b"x"),
        "nested slot diverged"
    );
}

#[test]
fn a_member_waiting_on_an_outside_op_does_not_hold_back_its_group() {
    let mut a = doc(1);
    let mut b = doc(2);
    let insert = a.transact(|tx| {
        tx.list(b"k").insert(0, Scalar::Int(1));
    });
    let node = match a.get(b"k") {
        Some(Element::List(l)) => l.borrow().node_ids(0, 1)[0],
        _ => panic!("no list"),
    };
    // One group both writes a register and deletes a node whose insert is still
    // in flight. The delete has nothing to remove yet, so it waits — on its own.
    let group = a.atomic_transact(|tx| {
        tx.register(b"r", Scalar::Int(9));
        tx.list(b"k").delete_id(node);
    });
    for op in &group {
        b.apply(op);
    }
    assert_eq!(
        reg(&b, b"r"),
        Some(Scalar::Int(9)),
        "an applicable member commits with its group"
    );

    // The insert arrives; the held delete drains behind it, so the node is gone
    // exactly as on the author.
    for op in &insert {
        b.apply(op);
    }
    assert_eq!(list_len(&a, b"k"), 0);
    assert_eq!(list_len(&b, b"k"), 0, "the held delete never landed");
}

/// A snapshot restore keeps the client id and the op-seq counter, so a group id
/// minted after one must not collide with a group minted before it — peers may
/// still be holding a partial group from the earlier incarnation, and two groups
/// sharing `(client, tx id)` land in one receiver bucket.
#[test]
fn a_group_minted_after_a_restore_cannot_collide_with_one_minted_before() {
    let mut a = doc(1);
    let old = a.atomic_transact(|tx| {
        tx.register(b"m1", Scalar::Int(1));
        tx.register(b"m2", Scalar::Int(2));
    });
    let mut a =
        Document::decode_state_as(a.client(), a.next_seq(), &a.encode_state()).expect("restore");
    let new = a.atomic_transact(|tx| {
        tx.register(b"n1", Scalar::Int(3));
        tx.register(b"n2", Scalar::Int(4));
        tx.register(b"n3", Scalar::Int(5));
        tx.register(b"n4", Scalar::Int(6));
    });
    assert_ne!(
        old[0].tx.as_ref().expect("tagged").id,
        new[0].tx.as_ref().expect("tagged").id,
        "a restore re-minted a group id the peers already hold"
    );

    // Only the first member of the stale group has reached this peer. Merging it
    // into the new group's bucket would commit a mixed set — making `m1` visible
    // without `m2` — and strand whatever the size gate left over.
    let mut b = doc(2);
    b.apply(&old[0]);
    for op in &new {
        b.apply(op);
    }
    assert_eq!(reg(&b, b"m1"), None, "a partial transaction stayed hidden");
    for key in [b"n1", b"n2", b"n3", b"n4"] {
        assert!(
            reg(&b, key).is_some(),
            "the new group committed whole alongside the stale partial"
        );
    }

    // The stale group completes and lands like any other.
    b.apply(&old[1]);
    assert_eq!(reg(&b, b"m1"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"m2"), Some(Scalar::Int(2)));

    // The plain restore keeps its own client and seq counter, so it carries the
    // same obligation.
    let mut plain = Document::decode_state(&a.encode_state()).expect("restore");
    let later = plain.atomic_transact(|tx| {
        tx.register(b"p1", Scalar::Int(7));
        tx.register(b"p2", Scalar::Int(8));
    });
    for group in [&old, &new] {
        assert_ne!(
            group[0].tx.as_ref().expect("tagged").id,
            later[0].tx.as_ref().expect("tagged").id,
            "a plain restore re-minted a live group id"
        );
    }
}

/// Replace a snapshot's trailing framed op buffer with `ops`. `encode_state`
/// ends with a `u32` length and that many framed bytes, so a snapshot taken with
/// an empty buffer has a known-size tail to swap out.
fn with_buffer(empty_snapshot: &[u8], ops: &[Op]) -> Vec<u8> {
    let tail = 4 + crdtsync_core::encode_ops(&[]).len();
    let mut out = empty_snapshot[..empty_snapshot.len() - tail].to_vec();
    let framed = crdtsync_core::encode_ops(ops);
    out.extend_from_slice(&(framed.len() as u32).to_le_bytes());
    out.extend_from_slice(&framed);
    out
}

/// A decode drains the buffer it just read. An honest replica never serializes a
/// complete transaction — it drains to a fixpoint before anything can encode it —
/// so a buffer holding two is a shape only bytes this replica did not produce can
/// take, which is exactly what arrives over the wire. Which group commits first
/// decides whether the other's members still resolve, so the choice comes from
/// the buffer, not from hash order: two replicas reading identical bytes reach
/// identical state whatever those bytes hold.
#[test]
fn a_snapshot_holding_two_complete_transactions_decodes_the_same_way_every_time() {
    // One group installs a nested map and writes a slot in it; the other takes
    // that same key with a register that outranks the map's create.
    let mut author = doc(1);
    let nested_write = author.atomic_transact(|tx| {
        tx.map(b"k").register(b"x", Scalar::Int(9));
    });
    let mut rival = doc(2);
    let takeover = rival.atomic_transact(|tx| {
        tx.register(b"k", Scalar::Int(1));
        tx.register(b"z", Scalar::Int(2));
    });
    assert_eq!(nested_write.len(), 2, "create plus write");
    assert_eq!(takeover.len(), 2, "two registers");

    // Later, the map is re-created above the register and takes the key back —
    // which is what makes the nested write's fate observable.
    for op in &takeover {
        author.apply(op);
    }
    let revival = author.transact(|tx| {
        tx.map(b"k").register(b"y", Scalar::Int(4));
    });

    let mut buffered: Vec<Op> = nested_write.clone();
    buffered.extend(takeover.iter().cloned());
    let snapshot = with_buffer(&doc(3).encode_state(), &buffered);

    let read_back = |bytes: &[u8]| -> (usize, Option<Scalar>) {
        let mut d = Document::decode_state(bytes).expect("decode");
        let seen = d.seen().count();
        for op in &revival {
            d.apply(op);
        }
        (seen, nested(&d, b"k", b"x"))
    };

    let first = read_back(&snapshot);
    for _ in 0..200 {
        assert_eq!(
            read_back(&snapshot),
            first,
            "the same snapshot bytes decoded to different state"
        );
    }
    // The buffer's order is the tie-break, so the group sitting first commits
    // first and its write survives the takeover.
    assert_eq!(first, (4, Some(Scalar::Int(9))));
}

// --- A filter that splits a group ------------------------------------------
//
// Several per-recipient redaction seams withhold individual ops from a batch: the
// catch-up delta's read and zone filters, the live fan-out's read filter, the
// per-channel zone filter, and the zone projection's buffer. A withheld member
// leaves the survivors carrying a `count` their bucket can never reach, so the
// recipient buffers them against a member that will never arrive.
// `split_groups` + `destrand_split` are the shared rule those seams apply — the
// one the migration translation seam already applied alone.

/// The group `ops` belongs to, as its members carry it.
fn group_of(ops: &[Op]) -> (ClientId, crdtsync_core::TxId) {
    let tx = ops[0].tx.expect("an atomic member carries its group");
    (ops[0].id.client, tx.id)
}

#[test]
fn a_survivor_of_a_split_group_is_stranded_while_it_keeps_its_tag() {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"kept", Scalar::Int(1));
        tx.register(b"withheld", Scalar::Int(2));
    });

    // Delivering the survivor tagged is the defect: it is held, invisible, and no
    // later traffic completes its group.
    let mut b = doc(2);
    assert!(
        !b.apply(&ops[0]),
        "a lone member of a group does not commit"
    );
    assert_eq!(reg(&b, b"kept"), None);
    for i in 0..8 {
        let later = a.transact(|tx| tx.register(b"later", Scalar::Int(i)));
        for op in &later {
            b.apply(op);
        }
    }
    assert_eq!(
        reg(&b, b"kept"),
        None,
        "later traffic cannot complete a group whose member was withheld"
    );
}

#[test]
fn destranding_a_split_group_lands_its_survivors() {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"kept", Scalar::Int(1));
        tx.register(b"withheld", Scalar::Int(2));
    });

    // What a filter does when it withholds `ops[1]`: name the split group, then
    // untag what it still delivers.
    let split = crdtsync_core::split_groups(&ops[1..]);
    assert_eq!(
        split.iter().copied().collect::<Vec<_>>(),
        vec![group_of(&ops)],
        "the withheld member names its own group"
    );
    let mut delivered = ops[..1].to_vec();
    crdtsync_core::destrand_split(&mut delivered, &split);
    assert!(delivered[0].tx.is_none(), "a survivor rides untagged");

    let mut b = doc(2);
    assert!(b.apply(&delivered[0]), "a destranded survivor applies now");
    assert_eq!(reg(&b, b"kept"), Some(Scalar::Int(1)));
    assert_eq!(
        b.seen().collect::<Vec<_>>(),
        vec![delivered[0].id],
        "the survivor is applied, not held"
    );
}

#[test]
fn destranding_leaves_an_uncut_group_atomic() {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"one", Scalar::Int(1));
        tx.register(b"two", Scalar::Int(2));
    });
    // A filter that withholds nothing names no split group, so every tag survives.
    let split = crdtsync_core::split_groups(std::iter::empty());
    let mut delivered = ops.clone();
    crdtsync_core::destrand_split(&mut delivered, &split);
    assert_eq!(delivered, ops, "an uncut batch is delivered unchanged");

    let mut b = doc(2);
    b.apply(&delivered[0]);
    assert_eq!(reg(&b, b"one"), None, "the group is still all-or-nothing");
    b.apply(&delivered[1]);
    assert_eq!(reg(&b, b"one"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"two"), Some(Scalar::Int(2)));
}

#[test]
fn destranding_cuts_only_the_split_group() {
    let mut a = doc(1);
    let cut = a.atomic_transact(|tx| {
        tx.register(b"c1", Scalar::Int(1));
        tx.register(b"c2", Scalar::Int(2));
    });
    let whole = a.atomic_transact(|tx| {
        tx.register(b"w1", Scalar::Int(3));
        tx.register(b"w2", Scalar::Int(4));
    });
    let split = crdtsync_core::split_groups(&cut[1..]);
    let mut delivered: Vec<Op> = cut[..1].iter().chain(&whole).cloned().collect();
    crdtsync_core::destrand_split(&mut delivered, &split);
    assert!(
        delivered[0].tx.is_none(),
        "the cut group's survivor is untagged"
    );
    assert!(
        delivered[1..].iter().all(|op| op.tx.is_some()),
        "a group the filter carried whole keeps its tags"
    );

    let mut b = doc(2);
    for op in &delivered {
        b.apply(op);
    }
    for (key, want) in [(&b"c1"[..], 1), (&b"w1"[..], 3), (&b"w2"[..], 4)] {
        assert_eq!(reg(&b, key), Some(Scalar::Int(want)));
    }
    assert_eq!(reg(&b, b"c2"), None, "the withheld member never arrived");
}

#[test]
fn a_destranded_survivor_still_waits_on_its_own_dependencies() {
    // C1's rule: completeness is the only group-level gate, and each member passes
    // the readiness gate at its own apply moment. Destranding hands a survivor to
    // that same gate — it applies when its target is reachable, not before.
    let mut a = doc(1);
    let create = a.transact(|tx| {
        tx.map(b"m");
    });
    let ops = a.atomic_transact(|tx| {
        tx.map(b"m").register(b"inner", Scalar::Int(7));
        tx.register(b"withheld", Scalar::Int(2));
    });
    let split = crdtsync_core::split_groups(&ops[1..]);
    let mut delivered = ops[..1].to_vec();
    crdtsync_core::destrand_split(&mut delivered, &split);

    let mut b = doc(2);
    assert!(
        !b.apply(&delivered[0]),
        "the survivor's target is not reachable yet"
    );
    for op in &create {
        b.apply(op);
    }
    assert_eq!(
        nested(&b, b"m", b"inner"),
        Some(Scalar::Int(7)),
        "it drains once its own dependency lands"
    );
}

#[test]
fn a_group_built_over_a_reused_sequence_does_not_collide_with_the_first() {
    // A filter that withholds an op from its *author's* catch-up leaves a hole in
    // the sequences the replica holds, and a hole is free again. Two groups can
    // therefore share a lowest member seq — which is why the group id is derived
    // from the whole member set.
    let mut a = doc(1);
    let first = a.atomic_transact(|tx| {
        tx.register(b"f1", Scalar::Int(1));
        tx.register(b"f2", Scalar::Int(2));
        tx.register(b"f3", Scalar::Int(3));
    });
    assert_eq!(
        first.iter().map(|op| op.id.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    // The replica restarts holding everything but seq 0, so seq 0 is free.
    let mut restarted = doc(1);
    for op in &first[1..] {
        restarted.apply(op);
    }
    let second = restarted.atomic_transact(|tx| {
        tx.register(b"s1", Scalar::Int(4));
        tx.register(b"s2", Scalar::Int(5));
        tx.register(b"s3", Scalar::Int(6));
    });
    assert_eq!(
        second.iter().map(|op| op.id.seq).collect::<Vec<_>>(),
        vec![0, 3, 4],
        "the hole at seq 0 is re-minted"
    );
    assert_ne!(
        group_of(&first),
        group_of(&second),
        "two groups sharing a lowest member seq landed in one bucket"
    );

    // A peer holding the first group partially does not have it completed by the
    // second group's members.
    let mut peer = doc(2);
    for op in &first[1..] {
        peer.apply(op);
    }
    for op in &second {
        peer.apply(op);
    }
    assert_eq!(reg(&peer, b"f2"), None, "a partial group stayed hidden");
    for key in [&b"s1"[..], b"s2", b"s3"] {
        assert!(
            reg(&peer, key).is_some(),
            "the second group committed whole beside the stale partial"
        );
    }
}

// --- malformed group sizes ---
//
// `count` is an instruction to hold the group's members until that many arrive.
// A rewritten one instructs the receiver to hold them for good: nothing else
// releases a group, and `encode_state` carries the buffer, so the next replica
// starts holding them too. The bound at the decode boundary keeps the
// unreachable sizes off the wire and a member carrying one past `apply` is
// untagged on its own account; unanimity across a bucket keeps a group's size
// from being whichever member landed first; and eviction is the way out for a
// group that is merely never completed, whatever left it that way.

/// `op` re-tagged as a member of group `id` declaring `count` members — the
/// envelope rewrite a hostile peer or a relay can perform on a member in flight.
fn retagged(op: &Op, id: crdtsync_core::TxId, count: u32) -> Op {
    let mut op = op.clone();
    op.tx = Some(Tx { id, count });
    op
}

/// The group id a tagged op carries.
fn tx_id(op: &Op) -> crdtsync_core::TxId {
    op.tx.expect("an atomic member carries its group").id
}

/// A two-member atomic group over `x` and `y`, and its author.
fn pair() -> (Document, Vec<Op>) {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });
    (a, ops)
}

/// A three-member atomic group over `x`, `y` and `z`, and its author.
fn triple() -> (Document, Vec<Op>) {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
        tx.register(b"z", Scalar::Int(3));
    });
    (a, ops)
}

/// Every ordering of `0..n`, so a fold is checked against the whole arrival space
/// rather than the two orders that happen to disagree.
fn orderings(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for rest in orderings(n - 1) {
        for at in 0..n {
            let mut order = rest.clone();
            order.insert(at, n - 1);
            out.push(order);
        }
    }
    out
}

/// Fold `ops` in every arrival order and assert every fold reads one state,
/// returning it. The comparison is over the canonical snapshot bytes, so nothing
/// the replica holds — buffer and resolved group keys included — is left out of
/// it, and every fold runs under one client id so the bytes are comparable.
fn one_state_in_every_order(ops: &[Op]) -> Document {
    let mut folded: Option<(Vec<usize>, Vec<u8>, Document)> = None;
    for order in orderings(ops.len()) {
        let mut d = doc(9);
        for &i in &order {
            d.apply(&ops[i]);
        }
        let bytes = d.encode_state();
        match &folded {
            None => folded = Some((order, bytes, d)),
            Some((first, first_bytes, _)) => assert_eq!(
                &bytes, first_bytes,
                "order {order:?} folded to a different state than {first:?}"
            ),
        }
    }
    folded.expect("an op set has at least one order").2
}

#[test]
fn the_codec_refuses_a_group_that_declares_no_members() {
    let (_, ops) = pair();
    let forged = retagged(&ops[0], tx_id(&ops[0]), 0);
    assert!(
        crdtsync_core::decode_op(&crdtsync_core::encode_op(&forged)).is_err(),
        "a group of zero members completes on no arrival"
    );
}

#[test]
fn the_codec_refuses_a_group_past_the_member_cap() {
    let (_, ops) = pair();
    for count in [crdtsync_core::MAX_TX_MEMBERS + 1, u32::MAX] {
        let forged = retagged(&ops[0], tx_id(&ops[0]), count);
        assert!(
            crdtsync_core::decode_op(&crdtsync_core::encode_op(&forged)).is_err(),
            "a group of {count} members is past the cap"
        );
    }
}

#[test]
fn the_codec_carries_a_group_at_the_member_cap() {
    let (_, ops) = pair();
    let capped = retagged(&ops[0], tx_id(&ops[0]), crdtsync_core::MAX_TX_MEMBERS);
    assert_eq!(
        crdtsync_core::decode_op(&crdtsync_core::encode_op(&capped)).expect("decode"),
        capped,
        "the cap itself is a declarable size"
    );
}

#[test]
#[cfg_attr(miri, ignore = "thousand-member groups are slow under Miri")]
fn the_member_cap_is_the_largest_group_a_local_transaction_tags() {
    let mut a = doc(1);
    let capped = a.atomic_transact(|tx| {
        for i in 0..crdtsync_core::MAX_TX_MEMBERS {
            tx.register(format!("k{i}").as_bytes(), Scalar::Int(i64::from(i)));
        }
    });
    assert_eq!(capped.len() as u32, crdtsync_core::MAX_TX_MEMBERS);
    assert!(
        capped
            .iter()
            .all(|op| op.tx.map(|tx| tx.count) == Some(crdtsync_core::MAX_TX_MEMBERS)),
        "the cap itself is a taggable size"
    );

    let mut b = doc(1);
    let over = b.atomic_transact(|tx| {
        for i in 0..=crdtsync_core::MAX_TX_MEMBERS {
            tx.register(format!("k{i}").as_bytes(), Scalar::Int(i64::from(i)));
        }
    });
    assert_eq!(over.len() as u32, crdtsync_core::MAX_TX_MEMBERS + 1);
    assert!(
        over.iter().all(|op| op.tx.is_none()),
        "a group no receiver may accept is streamed rather than tagged"
    );

    // Untagged, the oversized group's ops still cross the wire and still merge —
    // which tagging them would have cost, the codec refusing the whole frame.
    let mut c = doc(2);
    for op in &over {
        let wire = crdtsync_core::decode_op(&crdtsync_core::encode_op(op)).expect("decode");
        c.apply(&wire);
    }
    assert_eq!(reg(&c, b"k0"), Some(Scalar::Int(0)));
    assert_eq!(
        reg(&c, format!("k{}", crdtsync_core::MAX_TX_MEMBERS).as_bytes()),
        Some(Scalar::Int(i64::from(crdtsync_core::MAX_TX_MEMBERS)))
    );
}

#[test]
fn a_member_declaring_a_size_outside_the_cap_is_refused_on_arrival() {
    // An op reaching `apply` without crossing the codec — an in-process relay, an
    // SDK handing one over — carries a size the boundary never checked. The
    // judgement is the member's own, so it holds nothing whatever else has landed,
    // and refusing means it is not held at all: the honest member under the same
    // id still arrives and still completes its group.
    for count in [0, crdtsync_core::MAX_TX_MEMBERS + 1, u32::MAX] {
        let (a, ops) = pair();
        let forged = retagged(&ops[0], tx_id(&ops[0]), count);

        let mut b = doc(2);
        assert!(!b.apply(&forged), "a size of {count} is not applied");
        assert_eq!(reg(&b, b"x"), None);
        assert!(!b.apply(&ops[1]), "the honest member is still held");
        assert!(
            b.apply(&ops[0]),
            "the refused size of {count} left nothing holding its id"
        );
        assert_eq!(reg(&b, b"x"), reg(&a, b"x"));
        assert_eq!(reg(&b, b"y"), reg(&a, b"y"));

        // And the refusal does not depend on what has landed before it.
        let mut c = doc(2);
        assert!(!c.apply(&ops[1]));
        assert!(!c.apply(&forged), "a size of {count} is refused either way");
        assert_eq!(reg(&c, b"y"), None);
    }
}

#[test]
fn a_rewritten_first_member_count_does_not_commit_the_group_at_the_wrong_size() {
    // Three members; the one that lands first is rewritten to declare two. Read
    // off that member, the size is met by the pair — committing a group two
    // thirds of the way through, and leaving the third holding a size its bucket
    // has already spent. The bucket has to agree on its size instead.
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
        tx.register(b"z", Scalar::Int(3));
    });
    assert_eq!(ops.len(), 3);
    let id = tx_id(&ops[0]);

    let mut b = doc(2);
    assert!(!b.apply(&retagged(&ops[0], id, 2)));
    assert!(!b.apply(&ops[1]));
    assert_eq!(reg(&b, b"x"), None, "the pair is not the group");
    assert_eq!(reg(&b, b"y"), None, "the pair is not the group");

    // The third arrives; the bucket still names no size, so nothing commits —
    // and eviction is what releases all three.
    assert!(!b.apply(&ops[2]));
    assert_eq!(reg(&b, b"z"), None);
    assert_eq!(b.evict_partial_transactions(), 1);
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&b, key), reg(&a, key), "member {key:?} did not land");
    }
}

#[test]
fn a_bucket_whose_members_disagree_on_the_size_never_completes() {
    // The disagreement arriving last is the other half: the bucket is at its
    // declared size in members, and still names no group.
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
        tx.register(b"z", Scalar::Int(3));
    });
    let id = tx_id(&ops[0]);

    let mut b = doc(2);
    b.apply(&ops[0]);
    b.apply(&ops[1]);
    assert!(!b.apply(&retagged(&ops[2], id, 2)));
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(
            reg(&b, key),
            None,
            "member {key:?} committed a size-3 bucket"
        );
    }
    assert_eq!(b.evict_partial_transactions(), 1);
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&b, key), reg(&a, key));
    }
}

#[test]
fn a_rewritten_count_holds_the_same_set_whatever_order_it_arrives_in() {
    // Whether a bucket looks unreachable depends on which of its members have
    // landed, so nothing may be decided from that: two replicas served the same
    // ops in different orders would then release different sets and diverge.
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
        tx.register(b"z", Scalar::Int(3));
    });
    let id = tx_id(&ops[0]);
    let forged = [ops[0].clone(), ops[1].clone(), retagged(&ops[2], id, 2)];

    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [2, 0, 1],
        [2, 1, 0],
        [1, 2, 0],
        [1, 0, 2],
    ] {
        let mut b = doc(2);
        for i in order {
            b.apply(&forged[i]);
        }
        for key in [&b"x"[..], b"y", b"z"] {
            assert_eq!(reg(&b, key), None, "order {order:?} released {key:?} early");
        }
        b.evict_partial_transactions();
        for key in [&b"x"[..], b"y", b"z"] {
            assert_eq!(
                reg(&b, key),
                reg(&a, key),
                "order {order:?} stranded {key:?}"
            );
        }
    }
}

// --- a resolved group key ---
//
// A bucket key is spent when its group commits, so a member that arrives after it
// re-enters an arrival count the bucket already met — a count no further arrival
// can satisfy. Which members commit and which are left holding is the arrival
// order's, so the same ops fold to two states. Recording that a key has resolved
// is what closes it: a later member of a resolved key is untagged and merges
// standalone, so every order lands the same set. Three rewrites reach the shape,
// and none is malformed on any member's own terms.

#[test]
fn a_group_rewritten_smaller_on_every_member_folds_one_state_in_every_order() {
    // A rewrite consistent across every member is, to a receiver, an honest group
    // of the smaller size followed by a stray, so it commits at the size it was
    // told. Whichever two members that is, the third is a stray of a key that has
    // resolved, and lands.
    let (a, ops) = triple();
    let id = tx_id(&ops[0]);
    let shrunk: Vec<Op> = ops.iter().map(|op| retagged(op, id, 2)).collect();

    let b = one_state_in_every_order(&shrunk);
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&b, key), reg(&a, key), "member {key:?} did not land");
    }
}

#[test]
fn an_unrelated_op_retagged_into_a_group_folds_one_state_in_every_order() {
    // The same shape built the other way round: an honest two-member group plus a
    // fourth op of the same author re-tagged with its id. Whichever two of the
    // three the order commits, the remaining one is a stray of a resolved key.
    let mut a = doc(1);
    let group = a.atomic_transact(|tx| {
        tx.register(b"m1", Scalar::Int(1));
        tx.register(b"m2", Scalar::Int(2));
    });
    let stray = a.transact(|tx| {
        tx.register(b"m4", Scalar::Int(4));
    });
    let ops = [
        group[0].clone(),
        group[1].clone(),
        retagged(&stray[0], tx_id(&group[0]), 2),
    ];

    let b = one_state_in_every_order(&ops);
    for key in [&b"m1"[..], b"m2", b"m4"] {
        assert_eq!(reg(&b, key), reg(&a, key), "member {key:?} did not land");
    }
}

#[test]
fn one_member_under_two_envelopes_folds_one_state_in_every_order() {
    // `apply` dedups on op id before it looks at the tag, so one member arriving
    // twice under different envelopes leaves the bucket reading whichever copy the
    // dedup kept. The copy that lost the dedup is a member the bucket will never
    // hold under that envelope, so its key resolves and the rest of the group merges
    // standalone. Both copies name a group here: a copy carrying *no* tag — what a
    // filtering seam's destranding produces — leaves the buffer holding nothing to
    // contradict, and is C46's shape rather than this one.
    let (a, ops) = triple();
    let dup = retagged(&ops[2], tx_id(&ops[0]), 2);
    let set = [ops[0].clone(), ops[1].clone(), ops[2].clone(), dup];

    let b = one_state_in_every_order(&set);
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&b, key), reg(&a, key), "member {key:?} did not land");
    }
}

#[test]
fn an_inadmissible_envelope_decides_nothing_about_the_held_groups() {
    // Admissibility is judged before the dedup, so an envelope no replica may hold
    // never reaches the conflict rule. Otherwise a forged tag on a *held* member's id
    // would spend both keys and release the honest group waiting under one of them —
    // an op every replica refuses deciding what this one shows.
    let (a, ops) = triple();
    let mut b = doc(9);
    assert!(!b.apply(&ops[0]));
    assert!(!b.apply(&ops[1]));
    let settled = b.encode_state();

    // A size no group can have, on an id the buffer is holding under a live group.
    for count in [0, crdtsync_core::MAX_TX_MEMBERS + 1, u32::MAX] {
        let forged = retagged(&ops[0], crdtsync_core::TxId(0xbeef), count);
        assert!(!b.apply(&forged), "a size of {count} is refused");
        assert_eq!(
            b.encode_state(),
            settled,
            "a size of {count} moved the replica's state"
        );
    }
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(
            reg(&b, key),
            None,
            "the group was released by a refused envelope"
        );
    }

    // And the group still commits on its own last member.
    assert!(b.apply(&ops[2]));
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&b, key), reg(&a, key));
    }
}

#[test]
fn a_resent_member_of_a_waiting_group_leaves_it_atomic() {
    // The resolved key is read off a *disagreeing* envelope. A plain resend of a
    // member already held carries the envelope the bucket holds, so it decides
    // nothing: the group is still waiting, whole, on the member that has not come.
    let (_, ops) = triple();
    let mut b = doc(9);
    assert!(!b.apply(&ops[0]));
    assert!(!b.apply(&ops[0]), "a resend is a duplicate");
    assert!(!b.apply(&ops[1]));
    assert!(!b.apply(&ops[1]));
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&b, key), None, "a resend released the group early");
    }
    assert!(b.apply(&ops[2]), "the group commits on its own last member");
    for key in [&b"x"[..], b"y", b"z"] {
        assert!(reg(&b, key).is_some(), "member {key:?} did not commit");
    }
}

#[test]
fn two_envelopes_naming_different_groups_spend_both_keys() {
    // When the copies name different *groups* rather than two sizes of one, only
    // the group the dedup kept the copy under can ever hold this id, and that is
    // the arrival order's — so a genuine member of the other would be left holding.
    // Both keys are spent, and each group's own member lands whichever copy won.
    let (mut a, first) = pair();
    let second = a.atomic_transact(|tx| {
        tx.register(b"p", Scalar::Int(3));
        tx.register(b"q", Scalar::Int(4));
    });
    let (t1, t2) = (tx_id(&first[0]), tx_id(&second[0]));

    for order in [[0, 1], [1, 0]] {
        let both = [retagged(&first[0], t1, 2), retagged(&first[0], t2, 2)];
        let mut b = doc(9);
        assert!(!b.apply(&both[order[0]]), "the first copy is held");
        assert!(!b.apply(&both[order[1]]), "the second copy is a duplicate");
        assert_eq!(reg(&b, b"x"), reg(&a, b"x"), "the held copy was released");
        assert!(b.apply(&first[1]), "the first group's own member lands");
        assert!(b.apply(&second[1]), "the second group's own member lands");
        assert_eq!(reg(&b, b"y"), reg(&a, b"y"));
        assert_eq!(reg(&b, b"q"), reg(&a, b"q"));
    }
}

/// The residue the record does not reach, and does not claim to. A second copy is
/// a duplicate only where the first is still *held*, so a copy whose forged tag
/// carried it into another group's bucket — completing that bucket, and applying
/// the id — has left the buffer before the honest copy arrives, and the honest
/// group's own member is left holding. Eviction is what lands it, as for every
/// group nobody will complete. This is a *fourth* shape, not one of the three the
/// record closes: it needs the two copies to name different groups, it is
/// order-dependent on `main` before the record exists, and the record narrows it
/// from a split in what a replica reads to a split in which keys it has spent.
/// Filed as its own unit (KANBAN C46).
#[test]
fn a_copy_absorbed_by_another_groups_bucket_strands_its_own_group_until_eviction() {
    let (mut a, first) = pair();
    let second = a.atomic_transact(|tx| {
        tx.register(b"p", Scalar::Int(3));
        tx.register(b"q", Scalar::Int(4));
    });
    let ops = [
        retagged(&first[0], tx_id(&first[0]), 2),
        retagged(&first[0], tx_id(&second[0]), 2),
        first[1].clone(),
        second[1].clone(),
    ];

    // The forged copy completes the second group's bucket, so the first group's
    // own member waits on an id that has already applied elsewhere.
    let mut b = doc(9);
    for i in [3, 1, 0, 2] {
        b.apply(&ops[i]);
    }
    assert_eq!(reg(&b, b"y"), None, "the stranded member");

    // What every order does agree on, once evicted, is the state a reader sees.
    for order in orderings(ops.len()) {
        let mut d = doc(9);
        for &i in &order {
            d.apply(&ops[i]);
        }
        d.evict_partial_transactions();
        for key in [&b"x"[..], b"y", b"q"] {
            assert_eq!(
                reg(&d, key),
                reg(&a, key),
                "order {order:?} left {key:?} stranded past eviction"
            );
        }
    }
}

#[test]
fn a_stray_of_the_authors_own_group_lands_at_the_author_too() {
    // The author applies its own edits as it makes them and buckets nothing, so a
    // group it tags leaves no bucket to commit. Resolved at the mint instead, or a
    // stray under that id would be held at the author while every receiver merged
    // it — one op set, two states.
    let mut a = doc(1);
    let group = a.atomic_transact(|tx| {
        tx.register(b"m1", Scalar::Int(1));
        tx.register(b"m2", Scalar::Int(2));
    });
    // An op of the author's own id space that the author has not published — what a
    // relay forges when it re-tags a stray into a live group.
    let mut relay = doc(1);
    relay.transact(|tx| tx.register(b"burn1", Scalar::Int(0)));
    relay.transact(|tx| tx.register(b"burn2", Scalar::Int(0)));
    let stray = relay.transact(|tx| {
        tx.register(b"m4", Scalar::Int(4));
    });
    let forged = retagged(&stray[0], tx_id(&group[0]), 2);

    let mut b = doc(9);
    for op in [&group[0], &group[1], &forged] {
        b.apply(op);
    }
    assert_eq!(
        reg(&b, b"m4"),
        Some(Scalar::Int(4)),
        "the receiver merged it"
    );

    assert!(a.apply(&forged), "the author merged it too");
    assert_eq!(reg(&a, b"m4"), reg(&b, b"m4"));
}

#[test]
fn a_duplicate_of_an_applied_member_changes_nothing() {
    // A redelivered op is state a replica already holds, so it may not move any:
    // the resend seams replay more than a peer kept, and a delivery that decided
    // something would make state a function of how often an op arrived. Only a
    // member the buffer is *holding* under another group is evidence of anything.
    let (_, ops) = triple();
    let id = tx_id(&ops[0]);

    let mut b = doc(9);
    b.apply(&ops[0]);
    b.apply(&ops[1]);
    b.evict_partial_transactions();
    let settled = b.encode_state();

    // Every envelope of an already-applied member: the honest one, one naming a
    // size the group never had, one naming a group that never existed.
    for forged in [
        ops[0].clone(),
        retagged(&ops[0], id, 2),
        retagged(&ops[0], crdtsync_core::TxId(0xfeed), 2),
    ] {
        assert!(!b.apply(&forged));
        assert_eq!(
            b.encode_state(),
            settled,
            "a duplicate moved the replica's state"
        );
    }

    // And the last member of the evicted group lands the same way whether or not
    // those duplicates arrived — the eviction spent its key, not the duplicates.
    let mut c = Document::decode_state(&settled).expect("decode");
    assert!(b.apply(&ops[2]));
    assert!(c.apply(&ops[2]));
    assert_eq!(b.encode_state(), c.encode_state());
}

#[test]
fn many_resolved_keys_encode_in_one_order() {
    // The record is a set, so its iteration order is per-instance and says nothing
    // about what a replica folded. Replicas that resolved the same groups in
    // different orders have to encode identical bytes, or the record is itself the
    // divergence it exists to prevent.
    // Two authors, so the key's client component is exercised and not just its id —
    // `TxId::derive` hashes member sequences alone, so two clients collide on a group
    // id readily, and a sort that ignored the client would leave those pairs in set
    // order.
    let mut a = doc(1);
    let mut second = doc(2);
    let groups: Vec<Vec<Op>> = (0..12)
        .map(|i| {
            let author = if i % 2 == 0 { &mut a } else { &mut second };
            author.atomic_transact(|tx| {
                tx.register(format!("k{i}a").as_bytes(), Scalar::Int(i));
                tx.register(format!("k{i}b").as_bytes(), Scalar::Int(i));
            })
        })
        .collect();

    // Several rotations of the fold order, so the answer does not rest on two hash
    // sets happening to disagree.
    let mut folded: Option<Vec<u8>> = None;
    for rotation in 0..groups.len() {
        let mut d = doc(9);
        for i in 0..groups.len() {
            for op in &groups[(i + rotation) % groups.len()] {
                d.apply(op);
            }
        }
        let bytes = d.encode_state();
        match &folded {
            None => folded = Some(bytes),
            Some(first) => assert_eq!(
                &bytes, first,
                "rotation {rotation} encoded the record in its own order"
            ),
        }
    }

    // And the order is a named one, not merely a stable one: every group committed,
    // so the buffer is empty and the record is the section just before its length.
    let bytes = folded.expect("a fold");
    let count = groups.len();
    assert_eq!(
        bytes[bytes.len() - 4..],
        [0, 0, 0, 0],
        "every group committed, so the buffer is empty"
    );
    let keys_end = bytes.len() - 4;
    let keys_at = keys_end - count * 24;
    assert_eq!(
        u32::from_le_bytes(bytes[keys_at - 4..keys_at].try_into().expect("a length")),
        count as u32,
        "the record section is where its length says"
    );
    let encoded: Vec<(&[u8], u64)> = (0..count)
        .map(|i| {
            let at = keys_at + i * 24;
            (
                &bytes[at..at + 16],
                u64::from_le_bytes(bytes[at + 16..at + 24].try_into().expect("a key")),
            )
        })
        .collect();
    let mut ascending = encoded.clone();
    ascending.sort();
    assert_eq!(
        encoded, ascending,
        "the record is not encoded key-ascending"
    );
}

#[test]
fn the_mint_drains_what_spending_its_own_key_releases() {
    // The author's group is complete the moment it is tagged, so spending its key
    // releases whatever the buffer held under that id — and a released member applies
    // on the drain, not on whenever the next unrelated arrival happens to run one.
    let id = crdtsync_core::TxId::derive([0, 1]);

    // A foreign op under the author's own client id, tagged into the group the next
    // local transaction derives, at a sequence the mint will not take.
    let mut elsewhere = doc(7);
    let mut stray = elsewhere.transact(|tx| {
        tx.register(b"stray", Scalar::Int(3));
    })[0]
        .clone();
    stray.id.seq = 100;
    stray.tx = Some(Tx { id, count: 2 });

    let mut a = doc(7);
    assert!(!a.apply(&stray), "held under the group it names");
    assert_eq!(reg(&a, b"stray"), None);

    let ops = a.atomic_transact(|tx| {
        tx.register(b"m1", Scalar::Int(1));
        tx.register(b"m2", Scalar::Int(2));
    });
    assert_eq!(tx_id(&ops[0]), id, "the group the stray named");
    assert_eq!(
        reg(&a, b"stray"),
        Some(Scalar::Int(3)),
        "the mint spent its key and left what it released in the buffer"
    );
}

#[test]
fn a_projection_serves_no_record_of_the_groups_it_withholds() {
    // A key names an author and a group, never a partition, so a projection that kept
    // one would count the groups a withheld partition resolved. It goes whole, and the
    // recipient buckets a later stray as a group it has never seen resolve.
    let (mut a, group) = pair();
    let stray = a.transact(|tx| {
        tx.register(b"loose", Scalar::Int(7));
    });
    let forged = retagged(&stray[0], tx_id(&group[0]), 2);
    // The group's other member, writing its own key so the reading below proves both
    // members landed rather than just one.
    let partner = retagged(
        &a.transact(|tx| {
            tx.register(b"looser", Scalar::Int(8));
        })[0],
        tx_id(&group[0]),
        2,
    );

    let mut room = doc(9);
    for op in &group {
        room.apply(op);
    }
    let served = room.encode_state();

    let mut whole = Document::decode_state(&served).expect("decode");
    assert!(
        whole.apply(&forged),
        "the unprojected replica holds the record and merges the stray"
    );

    let mut projected = Document::decode_state(&served).expect("decode");
    projected.project_read_paths(|path| path != [b"x".to_vec()], None);
    let mut reader = Document::decode_state(&projected.encode_state()).expect("decode");
    assert!(
        !reader.apply(&forged),
        "the projection served its record of the withheld groups"
    );
    assert_eq!(
        reg(&reader, b"loose"),
        None,
        "the stray is held, not applied"
    );
    // Held as a *bucket* rather than refused: a second member under that id completes
    // the pair, which is what tells the two apart.
    assert!(
        reader.apply(&partner),
        "the bucket completed on its second member"
    );
    assert_eq!(reg(&reader, b"loose"), Some(Scalar::Int(7)));
    assert_eq!(reg(&reader, b"looser"), Some(Scalar::Int(8)));
}

#[test]
fn a_decoded_member_of_a_spent_key_does_not_wait_on_it() {
    // A snapshot can present both at once — a spent key and a member still tagged
    // under it — which an honest encoder never holds together. Whatever the bytes
    // carry, the member merges rather than waiting on a bucket the record has closed.
    let (a, ops) = triple();
    let id = tx_id(&ops[0]);
    let shrunk: Vec<Op> = ops.iter().map(|op| retagged(op, id, 2)).collect();

    // A replica holding the record, and one holding the stray still tagged.
    let mut resolved = doc(9);
    resolved.apply(&shrunk[0]);
    resolved.apply(&shrunk[1]);
    let with_record = resolved.encode_state();

    // The committed bucket leaves the buffer empty, so the snapshot's last four bytes
    // are its length; replacing them frames the stray as still held under the key.
    assert_eq!(
        with_record[with_record.len() - 4..],
        [0, 0, 0, 0],
        "the committed group left the buffer empty"
    );
    let framed = crdtsync_core::encode_ops(&[shrunk[2].clone()]);
    let mut forged = with_record[..with_record.len() - 4].to_vec();
    forged.extend_from_slice(&(framed.len() as u32).to_le_bytes());
    forged.extend_from_slice(&framed);

    let restored = Document::decode_state(&forged).expect("decode");
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(
            reg(&restored, key),
            reg(&a, key),
            "member {key:?} waited on a bucket the record had closed"
        );
    }
}

#[test]
fn a_resolved_group_key_survives_a_snapshot_restore() {
    // The record rides `encode_state`: a stray of a key resolved before a restart
    // has to land at the restored replica too, or a restore re-opens the hole the
    // record closes.
    let (a, ops) = triple();
    let id = tx_id(&ops[0]);
    let shrunk: Vec<Op> = ops.iter().map(|op| retagged(op, id, 2)).collect();

    let mut b = doc(9);
    b.apply(&shrunk[0]);
    b.apply(&shrunk[1]);
    let mut restored = Document::decode_state(&b.encode_state()).expect("decode");
    assert!(
        restored.apply(&shrunk[2]),
        "the stray of a resolved key was held across the restore"
    );
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&restored, key), reg(&a, key));
    }

    // And the restored replica re-encodes to the bytes it decoded from, the record
    // included.
    let settled = restored.encode_state();
    assert_eq!(
        Document::decode_state(&settled)
            .expect("decode")
            .encode_state(),
        settled,
        "the record is not byte-stable through a round-trip"
    );
}

#[test]
fn a_snapshot_holding_more_members_than_the_count_declares_evicts() {
    // The presentation of that shape that never commits at all: a buffer decoded
    // already holding more members than their unanimous size admits, so the bucket
    // is never *at* its size. It reaches a replica the way any buffer does —
    // inside a peer-supplied snapshot, whose framed op log is spliced here with
    // the same public codec that wrote it.
    let (mut a, ops) = pair();
    let extra = a.transact(|tx| tx.register(b"z", Scalar::Int(3)));
    let id = tx_id(&ops[0]);

    let mut b = doc(2);
    b.apply(&ops[0]);
    let snapshot = b.encode_state();

    // `encode_state` ends on the framed buffer behind its own u32 length, so
    // replacing the tail from that length onwards rewrites the buffer whole.
    let held = crdtsync_core::encode_ops(&ops[..1]);
    let at = snapshot
        .windows(held.len())
        .position(|w| w == held)
        .expect("the buffered member is framed in the snapshot");
    assert_eq!(
        at + held.len(),
        snapshot.len(),
        "the buffer is the state stream's last section"
    );
    let overfull = crdtsync_core::encode_ops(&[
        retagged(&ops[0], id, 2),
        retagged(&ops[1], id, 2),
        retagged(&extra[0], id, 2),
    ]);
    let mut forged = snapshot[..at - 4].to_vec();
    forged.extend_from_slice(&(overfull.len() as u32).to_le_bytes());
    forged.extend_from_slice(&overfull);

    let mut restored = Document::decode_state(&forged).expect("decode");
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(reg(&restored, key), None, "an overfull bucket committed");
    }
    assert_eq!(restored.evict_partial_transactions(), 1);
    for key in [&b"x"[..], b"y", b"z"] {
        assert_eq!(
            reg(&restored, key),
            reg(&a, key),
            "member {key:?} stayed buffered"
        );
    }
}

// --- evicting a partial transaction ---

#[test]
fn evicting_a_partial_transaction_lands_its_members() {
    let (a, ops) = pair();
    let mut b = doc(2);
    assert!(!b.apply(&ops[0]));
    assert_eq!(
        reg(&b, b"x"),
        None,
        "the member is held while the group can"
    );

    assert_eq!(b.evict_partial_transactions(), 1);
    assert_eq!(
        reg(&b, b"x"),
        reg(&a, b"x"),
        "an evicted member merges standalone"
    );

    // Giving up on a group spends its bucket key, so a member arriving afterwards is
    // a stray of a group this replica has already released rather than the first
    // member of a fresh one — it merges standalone, without a second eviction. Two
    // replicas on one policy would otherwise disagree over nothing but which of them
    // had already ticked when the last member landed.
    assert!(b.apply(&ops[1]), "the late member merges standalone");
    assert_eq!(b.evict_partial_transactions(), 0, "nothing is left waiting");
    assert_eq!(reg(&b, b"y"), reg(&a, b"y"));
}

#[test]
fn evicting_replicas_agree_whichever_of_them_had_ticked() {
    // The record covers eviction because eviction resolves a bucket: one replica
    // evicts between two members and the other after both, and they hold the same
    // state — down to which keys each has spent, so a later stray lands at both.
    let (mut a, ops) = pair();
    let stray = a.transact(|tx| {
        tx.register(b"late", Scalar::Int(9));
    });
    let forged = retagged(&stray[0], tx_id(&ops[0]), 2);

    let mut early = doc(9);
    early.apply(&ops[0]);
    early.evict_partial_transactions();
    early.apply(&ops[1]);
    early.evict_partial_transactions();

    let mut late = doc(9);
    late.apply(&ops[0]);
    late.apply(&ops[1]);
    late.evict_partial_transactions();

    assert_eq!(
        early.encode_state(),
        late.encode_state(),
        "when each replica ticked decided its state"
    );
    assert_eq!(early.apply(&forged), late.apply(&forged));
    assert_eq!(reg(&early, b"late"), reg(&late, b"late"));
}

#[test]
fn eviction_counts_the_transactions_it_gives_up_on() {
    let mut a = doc(1);
    let first = a.atomic_transact(|tx| {
        tx.register(b"f1", Scalar::Int(1));
        tx.register(b"f2", Scalar::Int(2));
    });
    let second = a.atomic_transact(|tx| {
        tx.register(b"s1", Scalar::Int(3));
        tx.register(b"s2", Scalar::Int(4));
    });

    let mut b = doc(2);
    b.apply(&first[0]);
    b.apply(&second[0]);
    assert_eq!(b.evict_partial_transactions(), 2, "two groups were partial");
    assert_eq!(b.evict_partial_transactions(), 0, "none is left to evict");
}

#[test]
fn eviction_leaves_a_complete_transaction_atomic() {
    let (mut a, ops) = pair();
    let mut b = doc(2);
    for op in &ops {
        b.apply(op);
    }
    assert_eq!(
        b.evict_partial_transactions(),
        0,
        "a group that committed is not a group waiting"
    );
    assert_eq!(reg(&b, b"x"), reg(&a, b"x"));
    assert_eq!(reg(&b, b"y"), reg(&a, b"y"));

    // And a group still arriving beside it keeps its atomic boundary: eviction
    // counts and releases the waiting group only.
    let second = a.atomic_transact(|tx| {
        tx.register(b"p", Scalar::Int(8));
        tx.register(b"q", Scalar::Int(9));
    });
    assert!(!b.apply(&second[0]));
    assert_eq!(b.evict_partial_transactions(), 1, "only the partial group");
    assert_eq!(reg(&b, b"p"), reg(&a, b"p"));
    assert_eq!(reg(&b, b"q"), None);
}

#[test]
fn an_evicted_member_is_neither_reapplied_nor_its_id_freed() {
    // The buffer is where a replica's own ops wait after a catch-up, and the op
    // counter walks the ids it holds. Eviction moves a member from held-and-
    // waiting to held-and-applied, so the counter must not see an id go free,
    // and a resend of the member must still dedup away.
    let (a, ops) = pair();
    let mut b = doc(2);
    b.apply(&ops[0]);
    b.adopt_as(a.client(), 0);
    let before = b.next_seq();

    assert_eq!(b.evict_partial_transactions(), 1);
    assert_eq!(
        b.next_seq(),
        before,
        "an evicted member's id is still published"
    );
    assert!(
        !b.apply(&ops[0]),
        "a resend of an evicted member is a no-op"
    );
    assert_eq!(reg(&b, b"x"), reg(&a, b"x"));
}

#[test]
fn an_evicted_member_whose_target_is_unreachable_keeps_waiting() {
    // Eviction gives up on the group, not on the readiness gate: a member whose
    // container has not arrived waits on alone, untagged, and lands when the
    // create does — never applied into a container that does not exist.
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.map(b"profile").register(b"name", Scalar::Int(7));
    });
    assert!(ops.len() >= 2, "a create plus a set");

    let mut b = doc(2);
    b.apply(&ops[ops.len() - 1]);
    assert_eq!(b.evict_partial_transactions(), 1);
    assert!(
        b.get(b"profile").is_none(),
        "the set did not conjure its container"
    );

    for op in &ops[..ops.len() - 1] {
        b.apply(op);
    }
    b.evict_partial_transactions();
    assert_eq!(nested(&b, b"profile", b"name"), Some(Scalar::Int(7)));
}

#[test]
fn a_partial_transaction_evicts_after_a_snapshot_restore() {
    // The buffer rides the state encoding, so a group held across a restart is
    // held by the replica that decodes it — and evictable there.
    let (a, ops) = pair();
    let mut b = doc(2);
    b.apply(&ops[0]);

    let mut restored = Document::decode_state(&b.encode_state()).expect("decode");
    assert_eq!(
        reg(&restored, b"x"),
        None,
        "the member survived the restart"
    );
    assert_eq!(restored.evict_partial_transactions(), 1);
    assert_eq!(reg(&restored, b"x"), reg(&a, b"x"));

    // An evicted buffer is not a buffer that re-holds the member on the next
    // round trip: the member is applied state now, and nothing is left to evict.
    let mut again = Document::decode_state(&restored.encode_state()).expect("decode");
    assert_eq!(reg(&again, b"x"), reg(&a, b"x"));
    assert_eq!(again.evict_partial_transactions(), 0);
}

#[test]
fn eager_and_deferred_eviction_reach_the_same_state() {
    // The atomic view is what eviction spends; convergence is not. A replica that
    // gives up after every arrival and one that gives up once at the end fold the
    // same ops to the same state, and both to the author's.
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.map(b"profile").register(b"name", Scalar::Int(7));
        tx.register(b"y", Scalar::Int(2));
    });

    let mut eager = doc(2);
    let mut deferred = doc(3);
    for (i, op) in ops.iter().enumerate() {
        eager.apply(op);
        eager.evict_partial_transactions();
        deferred.apply(&ops[ops.len() - 1 - i]);
    }
    deferred.evict_partial_transactions();

    for key in [&b"x"[..], b"y"] {
        assert_eq!(reg(&eager, key), reg(&a, key));
        assert_eq!(reg(&deferred, key), reg(&a, key));
    }
    assert_eq!(
        nested(&eager, b"profile", b"name"),
        nested(&a, b"profile", b"name")
    );
    assert_eq!(
        nested(&deferred, b"profile", b"name"),
        nested(&a, b"profile", b"name")
    );
}
