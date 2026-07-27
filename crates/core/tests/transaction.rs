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
    // lamport. At the ceiling that derivation must neither overflow-panic nor
    // collapse two codepoints onto one saturated stamp: every codepoint survives
    // with a distinct id, through the public apply() boundary.
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
            lamport: u64::MAX,
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
