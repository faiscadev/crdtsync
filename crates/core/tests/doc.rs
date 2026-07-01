//! Document — the top-level replica: a tree of Maps, a lamport clock, and the
//! transact/apply seam that turns editing intentions into ops and ops back
//! into state.
//!
//! A `transact` mutates the live tree through a cursor and returns the ops it
//! emitted; `apply` folds a foreign op back in. Two documents that exchange
//! ops converge regardless of arrival order. Ops are keyed by `(client, seq)`
//! for idempotent dedup, ordered by their stamp for LWW, and target a Map by
//! id plus a slot key; a nested map is reached by resolving that target.

use crdtsync_core::doc::Document;
use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::{Element, Scalar};

mod common;
use common::{cid, eid, stmp};

fn doc(client_first: u8) -> Document {
    Document::new(cid(client_first))
}

fn int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

fn counter(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a Counter"),
    }
}

/// Replay every op from `src` into `dst`, in the given order.
fn replay(dst: &mut Document, ops: &[crdtsync_core::op::Op]) {
    for op in ops {
        dst.apply(op);
    }
}

// --- construction ---

#[test]
fn new_carries_its_client() {
    let d = doc(1);
    assert_eq!(d.client(), cid(1));
}

#[test]
fn all_replicas_share_the_root_id() {
    // The root is a well-known slot so every replica derives children under
    // the same parent.
    assert_eq!(doc(1).root_id(), doc(2).root_id());
}

#[test]
fn fresh_document_is_empty() {
    let d = doc(1);
    assert!(d.get(b"missing").is_none());
}

// --- transact: mutate + emit ---

#[test]
fn transact_applies_locally() {
    let mut d = doc(1);
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(int(d.get(b"age")), 30);
}

#[test]
fn transact_returns_the_emitted_ops() {
    let mut d = doc(1);
    let ops = d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(ops.len(), 1);
    assert_eq!(
        ops[0].kind,
        OpKind::RegisterSet {
            key: b"age".to_vec(),
            value: Scalar::Int(30),
        }
    );
}

#[test]
fn emitted_ops_carry_the_documents_client() {
    let mut d = doc(7);
    let ops = d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.inc(b"n", 2);
    });
    assert!(ops.iter().all(|op| op.id.client == cid(7)));
}

#[test]
fn client_seq_increases_per_op() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
        tx.register(b"c", Scalar::Int(3));
    });
    let seqs: Vec<u64> = ops.iter().map(|op| op.id.seq).collect();
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "seqs not increasing: {seqs:?}"
    );
}

#[test]
fn lamport_increases_per_op() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
    });
    assert!(ops[0].stamp < ops[1].stamp);
}

#[test]
fn seq_continues_across_transacts() {
    let mut d = doc(1);
    let first = d.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = d.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    assert!(second[0].id.seq > first[0].id.seq);
    assert!(second[0].stamp > first[0].stamp);
}

// --- op kinds per intention ---

#[test]
fn counter_intents_emit_directional_ops() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.inc(b"n", 5);
        tx.dec(b"n", 2);
    });
    assert_eq!(
        ops[0].kind,
        OpKind::CounterInc {
            key: b"n".to_vec(),
            amount: 5
        }
    );
    assert_eq!(
        ops[1].kind,
        OpKind::CounterDec {
            key: b"n".to_vec(),
            amount: 2
        }
    );
}

#[test]
fn delete_emits_a_delete_op() {
    let mut d = doc(1);
    d.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let ops = d.transact(|tx| tx.delete(b"a"));
    assert_eq!(ops[0].kind, OpKind::MapDelete { key: b"a".to_vec() });
    assert!(d.get(b"a").is_none());
}

// --- apply: idempotent, ordered, convergent ---

#[test]
fn apply_reconstructs_state_on_a_peer() {
    let mut a = doc(1);
    let ops = a.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let mut b = doc(2);
    replay(&mut b, &ops);
    assert_eq!(int(b.get(b"age")), 30);
}

#[test]
fn apply_is_idempotent_on_op_id() {
    // A replayed / duplicated op (reconnect, retry) must not double-count.
    let mut a = doc(1);
    let ops = a.transact(|tx| tx.inc(b"n", 5));
    let mut b = doc(2);
    b.apply(&ops[0]);
    b.apply(&ops[0]);
    assert_eq!(counter(b.get(b"n")), 5);
}

#[test]
fn concurrent_ops_converge_regardless_of_order() {
    // Two clients set the same key concurrently; both replicas must land on
    // the same value (higher stamp wins).
    let mut a = doc(1);
    let mut b = doc(2);
    let oa = a.transact(|tx| tx.register(b"x", Scalar::Int(10)));
    let ob = b.transact(|tx| tx.register(b"x", Scalar::Int(20)));

    replay(&mut a, &ob); // a: local then remote
    replay(&mut b, &oa); // b: remote then local

    assert_eq!(int(a.get(b"x")), int(b.get(b"x")));
}

#[test]
fn apply_advances_the_local_clock() {
    // After absorbing a high-lamport op, the next local op must sort after it.
    let mut a = doc(1);
    let far = a.transact(|tx| {
        for _ in 0..5 {
            tx.inc(b"n", 1);
        }
    });
    let mut b = doc(2);
    replay(&mut b, &far);
    let next = b.transact(|tx| tx.register(b"z", Scalar::Int(0)));
    assert!(next[0].stamp > far.last().unwrap().stamp);
}

// --- displacement: orphaning is never silent ---

#[test]
fn overwriting_a_composite_slot_orphans_the_displaced_element() {
    let mut d = doc(1);
    let created = d.transact(|tx| tx.register(b"slot", Scalar::Int(1)));
    let orphaned_id = created[0].target; // the register lived under this map...
    let _ = orphaned_id;

    d.transact(|tx| tx.set(b"slot", Scalar::Bool(true))); // scalar over composite
    let orphans = d.take_orphans();
    assert_eq!(
        orphans.len(),
        1,
        "displacement must surface exactly one orphan"
    );
}

#[test]
fn plain_scalar_overwrite_does_not_orphan() {
    let mut d = doc(1);
    d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    d.transact(|tx| tx.set(b"k", Scalar::Int(2)));
    assert!(d.take_orphans().is_empty());
}

// --- slot stamp tracks composite child ops ---

#[test]
fn composite_update_survives_a_stale_concurrent_scalar_set() {
    // A register updated at a high stamp must not be displaced by a concurrent
    // scalar set carrying a lower stamp — the child op has to advance the
    // parent slot stamp, or the two replicas diverge.
    let mut a = doc(1);
    let mut b = doc(2);

    let create = a.transact(|tx| tx.register(b"slot", Scalar::Int(1)));
    replay(&mut b, &create);

    let scalar = b.transact(|tx| tx.set(b"slot", Scalar::Bool(true)));

    // Push a's clock ahead so its register update outranks b's scalar set.
    a.transact(|tx| {
        tx.set(b"pad", Scalar::Int(0));
        tx.set(b"pad", Scalar::Int(0));
        tx.set(b"pad", Scalar::Int(0));
    });
    let update = a.transact(|tx| tx.register(b"slot", Scalar::Int(2)));
    assert!(update[0].stamp > scalar[0].stamp);

    replay(&mut a, &scalar); // a: local update then stale remote scalar
    replay(&mut b, &update); // b: remote update over local scalar

    assert_eq!(int(a.get(b"slot")), 2);
    assert_eq!(int(b.get(b"slot")), int(a.get(b"slot")));
}

#[test]
fn apply_ignores_ops_for_a_foreign_target() {
    // An op naming a parent that isn't this replica's root must not leak into
    // the root map.
    let foreign = Op::new(
        OpId {
            client: cid(9),
            seq: 0,
        },
        stmp(1, 9),
        eid(0xAB, 0),
        OpKind::MapSet {
            key: b"k".to_vec(),
            value: Scalar::Int(99),
        },
    );
    let mut d = doc(1);
    assert!(!d.apply(&foreign));
    assert!(d.get(b"k").is_none());
}

// --- nested maps ---

use crdtsync_core::Map;
use std::cell::RefCell;
use std::rc::Rc;

fn child_map(e: Option<Element>) -> Rc<RefCell<Map>> {
    match e {
        Some(Element::Map(m)) => m,
        _ => panic!("expected a nested Map"),
    }
}

#[test]
fn nested_map_edit_reads_back() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut sub = tx.map(b"profile");
        sub.register(b"age", Scalar::Int(30));
    });
    let profile = child_map(d.get(b"profile"));
    let age = profile.borrow().get(b"age");
    assert_eq!(int(age), 30);
}

#[test]
fn nested_edit_converges_on_a_peer() {
    let mut a = doc(1);
    let ops = a.transact(|tx| {
        let mut sub = tx.map(b"p");
        sub.inc(b"hits", 4);
    });
    let mut b = doc(2);
    replay(&mut b, &ops);
    let p = child_map(b.get(b"p"));
    let hits = p.borrow().get(b"hits");
    assert_eq!(counter(hits), 4);
}

#[test]
fn deeply_nested_maps() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut a = tx.map(b"a");
        let mut b = a.map(b"b");
        b.register(b"deep", Scalar::Int(7));
    });
    let a = child_map(d.get(b"a"));
    let b = child_map(a.borrow().get(b"b"));
    assert_eq!(int(b.borrow().get(b"deep")), 7);
}

#[test]
fn concurrent_edits_to_the_same_nested_map_merge() {
    // Both clients create "shared" (same derived id) and write different keys.
    let mut a = doc(1);
    let mut b = doc(2);
    let oa = a.transact(|tx| {
        let mut s = tx.map(b"shared");
        s.register(b"x", Scalar::Int(1));
    });
    let ob = b.transact(|tx| {
        let mut s = tx.map(b"shared");
        s.register(b"y", Scalar::Int(2));
    });

    replay(&mut a, &ob);
    replay(&mut b, &oa);

    let sa = child_map(a.get(b"shared"));
    let sb = child_map(b.get(b"shared"));
    assert_eq!(int(sa.borrow().get(b"x")), int(sb.borrow().get(b"x")));
    assert_eq!(int(sa.borrow().get(b"y")), int(sb.borrow().get(b"y")));
    assert_eq!(int(sa.borrow().get(b"x")), 1);
    assert_eq!(int(sa.borrow().get(b"y")), 2);
}

#[test]
fn child_ops_of_a_losing_map_create_are_not_applied() {
    // If a MapCreate loses its slot to a higher-stamped value, the nested map
    // is unreachable; ops targeting it must not be marked applied.
    let mut a = doc(1);
    let a_ops = a.transact(|tx| {
        let mut sub = tx.map(b"k");
        sub.register(b"x", Scalar::Int(9));
    });

    // A peer already holding a higher-stamped register at "k".
    let mut b = doc(2);
    b.transact(|tx| {
        for _ in 0..5 {
            tx.set(b"pad", Scalar::Int(0));
        }
    });
    let breg = b.transact(|tx| tx.register(b"k", Scalar::Int(6)));

    let mut c = doc(3);
    replay(&mut c, &breg); // c: "k" = register(6) at a high stamp

    assert!(c.apply(&a_ops[0])); // MapCreate applies at root but loses the slot
    assert!(!c.apply(&a_ops[1])); // child op targets the unreachable map
    assert_eq!(int(c.get(b"k")), 6); // slot is still the register
}

#[test]
fn nested_ops_target_the_child_map_not_root() {
    // A nested edit must not write into the root slot of the same key.
    let mut d = doc(1);
    d.transact(|tx| {
        let mut sub = tx.map(b"n");
        sub.register(b"n", Scalar::Int(5)); // key "n" inside the nested map "n"
    });
    // Root "n" is the nested Map, not the register 5.
    let n = child_map(d.get(b"n"));
    assert_eq!(int(n.borrow().get(b"n")), 5);
}

// --- lists in the document ---

use crdtsync_core::List;

fn child_list(e: Option<Element>) -> Rc<RefCell<List>> {
    match e {
        Some(Element::List(l)) => l,
        _ => panic!("expected a List"),
    }
}

/// A one-byte scalar list item.
fn sb(c: u8) -> Scalar {
    Scalar::Bytes(vec![c])
}

/// The live list as a string (each item is one byte).
fn list_str(l: &Rc<RefCell<List>>) -> String {
    l.borrow()
        .values()
        .iter()
        .map(|e| match e {
            Element::Scalar(Scalar::Bytes(b)) if b.len() == 1 => b[0] as char,
            _ => panic!("expected a one-byte scalar item"),
        })
        .collect()
}

#[test]
fn list_insert_reads_back() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut l = tx.list(b"items");
        l.insert(0, sb(b'a'));
        l.insert(1, sb(b'b'));
        l.insert(1, sb(b'x'));
    });
    let items = child_list(d.get(b"items"));
    assert_eq!(list_str(&items), "axb");
}

#[test]
fn list_edits_converge_on_a_peer() {
    let mut a = doc(1);
    let ops = a.transact(|tx| {
        let mut l = tx.list(b"items");
        l.insert(0, sb(b'h'));
        l.insert(1, sb(b'i'));
    });
    let mut b = doc(2);
    replay(&mut b, &ops);
    assert_eq!(list_str(&child_list(b.get(b"items"))), "hi");
}

#[test]
fn list_delete_removes_an_item() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut l = tx.list(b"items");
        l.insert(0, sb(b'a'));
        l.insert(1, sb(b'b'));
        l.insert(2, sb(b'c'));
    });
    d.transact(|tx| tx.list(b"items").delete(1));
    assert_eq!(list_str(&child_list(d.get(b"items"))), "ac");
}

#[test]
fn concurrent_list_inserts_converge_without_interleaving() {
    let mut a = doc(1);
    let mut b = doc(2);
    let oa = a.transact(|tx| {
        let mut l = tx.list(b"items");
        l.insert(0, sb(b'A'));
        l.insert(1, sb(b'B'));
    });
    let ob = b.transact(|tx| {
        let mut l = tx.list(b"items");
        l.insert(0, sb(b'X'));
        l.insert(1, sb(b'Y'));
    });

    replay(&mut a, &ob);
    replay(&mut b, &oa);

    let sa = list_str(&child_list(a.get(b"items")));
    let sb_ = list_str(&child_list(b.get(b"items")));
    assert_eq!(sa, sb_, "replicas diverged");
    assert!(sa == "ABXY" || sa == "XYAB", "runs interleaved: {sa}");
}

#[test]
fn a_re_navigated_list_defends_its_slot_against_a_stale_scalar() {
    // Re-entering a list to edit it re-stamps its parent slot, so a scalar set
    // carrying a lower stamp can no longer displace the list — the ListCreate
    // has to advance the slot stamp, matching register/counter child ops.
    let mut d = doc(1);
    d.transact(|tx| tx.list(b"cards").insert(0, sb(b'x')));
    d.transact(|tx| {
        for _ in 0..3 {
            tx.set(b"pad", Scalar::Int(0));
        }
    });
    d.transact(|tx| tx.list(b"cards").insert(1, sb(b'y')));

    // A stale scalar set, stamped below the list's re-navigation, must lose.
    let stale = Op::new(
        OpId {
            client: cid(9),
            seq: 0,
        },
        stmp(4, 9),
        d.root_id(),
        OpKind::MapSet {
            key: b"cards".to_vec(),
            value: Scalar::Int(0),
        },
    );
    d.apply(&stale);

    assert_eq!(list_str(&child_list(d.get(b"cards"))), "xy");
}

#[test]
fn list_in_a_nested_map() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut sub = tx.map(b"board");
        let mut l = sub.list(b"cards");
        l.insert(0, sb(b'z'));
    });
    let board = child_map(d.get(b"board"));
    let cards = child_list(board.borrow().get(b"cards"));
    assert_eq!(list_str(&cards), "z");
}

// --- text in the document ---

use crdtsync_core::Text;

fn child_text(e: Option<Element>) -> Rc<RefCell<Text>> {
    match e {
        Some(Element::Text(t)) => t,
        _ => panic!("expected a Text"),
    }
}

fn text_str(t: &Rc<RefCell<Text>>) -> String {
    t.borrow().as_string()
}

#[test]
fn text_insert_reads_back() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut t = tx.text(b"body");
        t.insert(0, "hlo");
        t.insert(1, "el"); // "h" + "el" + "lo"
    });
    assert_eq!(text_str(&child_text(d.get(b"body"))), "hello");
}

#[test]
fn text_edits_converge_on_a_peer() {
    let mut a = doc(1);
    let ops = a.transact(|tx| {
        let mut t = tx.text(b"body");
        t.insert(0, "hi there");
    });
    let mut b = doc(2);
    replay(&mut b, &ops);
    assert_eq!(text_str(&child_text(b.get(b"body"))), "hi there");
}

#[test]
fn text_delete_removes_codepoints() {
    let mut d = doc(1);
    d.transact(|tx| tx.text(b"body").insert(0, "hello"));
    d.transact(|tx| tx.text(b"body").delete(1, 3)); // drop "ell"
    assert_eq!(text_str(&child_text(d.get(b"body"))), "ho");
}

#[test]
fn a_text_run_reserves_its_lamports() {
    // Each codepoint in a run takes its own char_id, so the whole run must
    // reserve that many lamports — the next op has to sort after the last
    // codepoint, not after the run's base.
    let mut d = doc(1);
    let run = d.transact(|tx| tx.text(b"body").insert(0, "abcde"));
    let next = d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    let last_char = run.last().unwrap().stamp.lamport + 4; // base + (len-1)
    assert!(
        next[0].stamp.lamport > last_char,
        "next op {} did not clear the run's last codepoint {last_char}",
        next[0].stamp.lamport
    );
}

#[test]
fn concurrent_text_inserts_converge_without_interleaving() {
    let mut a = doc(1);
    let mut b = doc(2);
    let oa = a.transact(|tx| tx.text(b"body").insert(0, "AB"));
    let ob = b.transact(|tx| tx.text(b"body").insert(0, "XY"));

    replay(&mut a, &ob);
    replay(&mut b, &oa);

    let sa = text_str(&child_text(a.get(b"body")));
    let sb_ = text_str(&child_text(b.get(b"body")));
    assert_eq!(sa, sb_, "replicas diverged");
    assert!(sa == "ABXY" || sa == "XYAB", "runs interleaved: {sa}");
}

#[test]
fn text_in_a_nested_map() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut sub = tx.map(b"doc");
        let mut t = sub.text(b"title");
        t.insert(0, "hi");
    });
    let sub = child_map(d.get(b"doc"));
    let title = child_text(sub.borrow().get(b"title"));
    assert_eq!(text_str(&title), "hi");
}
