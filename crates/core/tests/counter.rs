use crdtsync_core::{ClientId, Counter};

mod common;
use common::{cid, default_id, eid};

fn fresh() -> Counter {
    Counter::new(default_id())
}

#[test]
fn create_stores_id() {
    let id = eid(7, 42);
    assert_eq!(Counter::new(id).id(), id);
}

#[test]
fn empty_reads_zero() {
    assert_eq!(fresh().read(), 0);
}

#[test]
fn single_inc() {
    let mut c = fresh();
    c.inc(cid(1), 5);
    assert_eq!(c.read(), 5);
}

#[test]
fn inc_then_dec_nets() {
    let mut c = fresh();
    c.inc(cid(1), 5);
    c.dec(cid(1), 2);
    assert_eq!(c.read(), 3);
}

// Local repeated ops on one client accumulate (NOT max).
#[test]
fn local_inc_accumulates() {
    let mut c = fresh();
    c.inc(cid(1), 5);
    c.inc(cid(1), 2);
    assert_eq!(c.read(), 7);
}

#[test]
fn read_can_go_negative() {
    let mut c = fresh();
    c.dec(cid(1), 3);
    assert_eq!(c.read(), -3);
}

#[test]
fn two_clients_sum_in_one_replica() {
    let mut c = fresh();
    c.inc(cid(1), 5);
    c.inc(cid(2), 3);
    c.dec(cid(2), 1);
    assert_eq!(c.read(), 7);
}

// Clients distinguished by the FULL 16-byte id, not a prefix.
#[test]
fn client_ids_distinguished_by_full_bytes() {
    let mut c = fresh();
    let mut a = [9u8; 16];
    let mut b = [9u8; 16];
    a[15] = 1;
    b[15] = 2;
    c.inc(ClientId::from_bytes(a), 5);
    c.inc(ClientId::from_bytes(b), 3);
    assert_eq!(c.read(), 8);
}

// --- merge ---

#[test]
fn merge_disjoint_clients_unions() {
    let mut a = fresh();
    let mut b = fresh();
    a.inc(cid(1), 5);
    b.inc(cid(2), 3);
    a.merge(&b);
    assert_eq!(a.read(), 8);
}

#[test]
fn concurrent_inc_converges() {
    let mut a = fresh();
    let mut b = fresh();
    a.inc(cid(1), 5);
    b.inc(cid(2), 3);
    a.merge(&b);
    b.merge(&a);
    assert_eq!(a.read(), 8);
    assert_eq!(b.read(), 8);
}

// Same client on two replicas: merge takes MAX, not sum.
#[test]
fn merge_same_client_takes_max_not_sum() {
    let mut a = fresh();
    let mut b = fresh();
    a.inc(cid(1), 5);
    b.inc(cid(1), 3);
    a.merge(&b);
    assert_eq!(a.read(), 5);
}

#[test]
fn merge_same_client_max_on_both_directions() {
    let mut a = fresh();
    let mut b = fresh();
    a.inc(cid(1), 10);
    a.dec(cid(1), 2);
    b.inc(cid(1), 4);
    b.dec(cid(1), 6);
    a.merge(&b);
    assert_eq!(a.read(), 4); // max(inc)=10, max(dec)=6
}

#[test]
fn merge_idempotent() {
    let mut a = fresh();
    let mut b = fresh();
    a.inc(cid(1), 5);
    b.inc(cid(2), 3);
    a.merge(&b);
    let once = a.read();
    a.merge(&b);
    assert_eq!(a.read(), once);
    assert_eq!(a.read(), 8);
}

#[test]
fn merge_commutative() {
    let mut a1 = fresh();
    let mut b1 = fresh();
    a1.inc(cid(1), 5);
    a1.dec(cid(1), 1);
    b1.inc(cid(2), 3);
    a1.merge(&b1);

    let mut a2 = fresh();
    let mut b2 = fresh();
    a2.inc(cid(1), 5);
    a2.dec(cid(1), 1);
    b2.inc(cid(2), 3);
    b2.merge(&a2);

    assert_eq!(a1.read(), b2.read());
}

#[test]
fn merge_associative() {
    let mut a = fresh();
    let mut b = fresh();
    let mut c = fresh();
    a.inc(cid(1), 5);
    b.inc(cid(2), 3);
    c.inc(cid(3), 2);
    a.merge(&b);
    a.merge(&c);

    let mut a2 = fresh();
    let mut b2 = fresh();
    let mut c2 = fresh();
    a2.inc(cid(1), 5);
    b2.inc(cid(2), 3);
    c2.inc(cid(3), 2);
    b2.merge(&c2);
    a2.merge(&b2);

    assert_eq!(a.read(), a2.read());
    assert_eq!(a.read(), 10);
}

#[test]
fn merge_does_not_mutate_src() {
    let mut a = fresh();
    let mut b = fresh();
    a.inc(cid(1), 5);
    b.inc(cid(2), 3);
    a.merge(&b);
    assert_eq!(b.read(), 3);
}

#[test]
fn local_inc_after_merge_accumulates() {
    let mut a = fresh();
    let mut b = fresh();
    b.inc(cid(2), 3);
    a.merge(&b);
    a.inc(cid(2), 4); // accumulate from merged-in 3
    assert_eq!(a.read(), 7);
}

// --- deep_clone ---

#[test]
fn clone_empty_reads_zero() {
    let src = fresh();
    assert_eq!(src.deep_clone().read(), 0);
}

#[test]
fn clone_preserves_id() {
    let id = eid(7, 42);
    assert_eq!(Counter::new(id).deep_clone().id(), id);
}

#[test]
fn clone_preserves_tallies() {
    let mut src = fresh();
    src.inc(cid(1), 5);
    src.inc(cid(2), 3);
    assert_eq!(src.deep_clone().read(), 8);
}

#[test]
fn clone_independent_of_src() {
    let mut src = fresh();
    src.inc(cid(1), 5);
    let mut clone = src.deep_clone();
    src.inc(cid(1), 100);
    clone.inc(cid(2), 7);
    assert_eq!(src.read(), 105);
    assert_eq!(clone.read(), 12);
}

// --- displacement ---

#[test]
fn create_starts_not_displaced() {
    assert!(!fresh().is_displaced());
}

#[test]
fn displace_sets_flag() {
    let c = fresh();
    c.displace();
    assert!(c.is_displaced());
}

#[test]
fn displaced_counter_still_mutable() {
    let mut c = fresh();
    c.inc(cid(1), 5);
    c.displace();
    c.inc(cid(1), 3);
    assert_eq!(c.read(), 8);
}

#[test]
fn clone_of_displaced_is_not_displaced() {
    let src = fresh();
    src.displace();
    assert!(!src.deep_clone().is_displaced());
}
