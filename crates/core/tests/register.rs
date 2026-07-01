use crdtsync_core::{Register, Scalar};

mod common;
use common::{default_id, eid, stmp};

fn fresh(value: Scalar, stamp: crdtsync_core::Stamp) -> Register {
    Register::new(default_id(), value, stamp)
}

fn bytes(s: &str) -> Scalar {
    Scalar::Bytes(s.as_bytes().to_vec())
}

#[test]
fn create_stores_id() {
    let id = eid(7, 42);
    let r = Register::new(id, Scalar::Int(0), stmp(1, 1));
    assert_eq!(r.id(), id);
}

// --- create / read ---

#[test]
fn create_seeds_value() {
    let r = fresh(Scalar::Int(42), stmp(1, 1));
    assert_eq!(r.read(), &Scalar::Int(42));
}

#[test]
fn create_with_string() {
    let r = fresh(bytes("hello"), stmp(1, 1));
    assert_eq!(r.read(), &bytes("hello"));
}

#[test]
fn create_with_null() {
    let r = fresh(Scalar::Null, stmp(1, 1));
    assert_eq!(r.read(), &Scalar::Null);
}

#[test]
fn create_with_bool() {
    let r = fresh(Scalar::Bool(true), stmp(1, 1));
    assert_eq!(r.read(), &Scalar::Bool(true));
}

// --- LWW: local set ---

#[test]
fn higher_lamport_wins() {
    let mut r = fresh(Scalar::Int(10), stmp(1, 1));
    r.set(Scalar::Int(20), stmp(2, 1));
    assert_eq!(r.read(), &Scalar::Int(20));
}

#[test]
fn lower_lamport_ignored() {
    let mut r = fresh(Scalar::Int(20), stmp(5, 1));
    r.set(Scalar::Int(10), stmp(3, 1));
    assert_eq!(r.read(), &Scalar::Int(20));
}

#[test]
fn equal_lamport_higher_client_wins() {
    let mut r = fresh(Scalar::Int(10), stmp(5, 1));
    r.set(Scalar::Int(20), stmp(5, 2));
    assert_eq!(r.read(), &Scalar::Int(20));
}

#[test]
fn equal_lamport_lower_client_ignored() {
    let mut r = fresh(Scalar::Int(20), stmp(5, 2));
    r.set(Scalar::Int(10), stmp(5, 1));
    assert_eq!(r.read(), &Scalar::Int(20));
}

#[test]
fn set_same_stamp_idempotent() {
    let mut r = fresh(Scalar::Int(42), stmp(5, 1));
    r.set(Scalar::Int(42), stmp(5, 1));
    assert_eq!(r.read(), &Scalar::Int(42));
}

#[test]
fn out_of_order_sets_converge() {
    let mut r = fresh(Scalar::Int(1), stmp(1, 1));
    r.set(Scalar::Int(99), stmp(10, 1));
    r.set(Scalar::Int(2), stmp(2, 1)); // older — ignored
    assert_eq!(r.read(), &Scalar::Int(99));
}

#[test]
fn kind_changes_on_newer_write() {
    let mut r = fresh(Scalar::Int(42), stmp(1, 1));
    r.set(bytes("hi"), stmp(2, 1));
    assert_eq!(r.read(), &bytes("hi"));
}

#[test]
fn newer_string_replaces_older() {
    let mut r = fresh(bytes("first"), stmp(1, 1));
    r.set(bytes("second"), stmp(2, 1));
    assert_eq!(r.read(), &bytes("second"));
}

// --- merge ---

#[test]
fn merge_src_newer_wins() {
    let mut a = fresh(Scalar::Int(10), stmp(1, 1));
    let b = fresh(Scalar::Int(20), stmp(2, 2));
    a.merge(&b);
    assert_eq!(a.read(), &Scalar::Int(20));
}

#[test]
fn merge_src_older_ignored() {
    let mut a = fresh(Scalar::Int(20), stmp(5, 1));
    let b = fresh(Scalar::Int(10), stmp(2, 2));
    a.merge(&b);
    assert_eq!(a.read(), &Scalar::Int(20));
}

#[test]
fn merge_equal_lamport_client_tiebreak() {
    let mut a = fresh(Scalar::Int(10), stmp(5, 1));
    let b = fresh(Scalar::Int(20), stmp(5, 2));
    a.merge(&b);
    assert_eq!(a.read(), &Scalar::Int(20));
}

#[test]
fn merge_commutative() {
    let mut a1 = fresh(Scalar::Int(10), stmp(5, 1));
    let b1 = fresh(Scalar::Int(20), stmp(5, 2));
    a1.merge(&b1);

    let a2 = fresh(Scalar::Int(10), stmp(5, 1));
    let mut b2 = fresh(Scalar::Int(20), stmp(5, 2));
    b2.merge(&a2);

    assert_eq!(a1.read(), b2.read());
    assert_eq!(a1.read(), &Scalar::Int(20));
}

#[test]
fn merge_idempotent() {
    let mut a = fresh(Scalar::Int(10), stmp(1, 1));
    let b = fresh(Scalar::Int(20), stmp(2, 1));
    a.merge(&b);
    let once = a.read().clone();
    a.merge(&b);
    assert_eq!(a.read(), &once);
    assert_eq!(a.read(), &Scalar::Int(20));
}

#[test]
fn merge_associative() {
    let mut a = fresh(Scalar::Int(10), stmp(1, 1));
    let b = fresh(Scalar::Int(20), stmp(2, 1));
    let c = fresh(Scalar::Int(30), stmp(3, 1));
    a.merge(&b);
    a.merge(&c);

    let mut a2 = fresh(Scalar::Int(10), stmp(1, 1));
    let mut b2 = fresh(Scalar::Int(20), stmp(2, 1));
    let c2 = fresh(Scalar::Int(30), stmp(3, 1));
    b2.merge(&c2);
    a2.merge(&b2);

    assert_eq!(a.read(), a2.read());
    assert_eq!(a.read(), &Scalar::Int(30));
}

#[test]
fn merge_does_not_mutate_src() {
    let mut a = fresh(Scalar::Int(99), stmp(10, 1));
    let b = fresh(Scalar::Int(7), stmp(1, 1));
    a.merge(&b);
    assert_eq!(b.read(), &Scalar::Int(7));
}

// dst owns its own copy of a winning string; dropping src leaves it intact.
#[test]
fn merge_string_survives_src_drop() {
    let mut a = fresh(Scalar::Int(0), stmp(1, 1));
    let b = fresh(bytes("hello"), stmp(5, 1));
    a.merge(&b);
    drop(b);
    assert_eq!(a.read(), &bytes("hello"));
}

// --- deep_clone ---

#[test]
fn clone_preserves_id_and_value() {
    let id = eid(7, 42);
    let src = Register::new(id, Scalar::Int(42), stmp(5, 1));
    let clone = src.deep_clone();
    assert_eq!(clone.id(), id);
    assert_eq!(clone.read(), &Scalar::Int(42));
}

#[test]
fn clone_string_survives_src_drop() {
    let src = fresh(bytes("hello"), stmp(1, 1));
    let clone = src.deep_clone();
    drop(src);
    assert_eq!(clone.read(), &bytes("hello"));
}

#[test]
fn clone_independent_of_src() {
    let mut src = fresh(Scalar::Int(1), stmp(1, 1));
    let mut clone = src.deep_clone();
    src.set(Scalar::Int(99), stmp(10, 1));
    clone.set(Scalar::Int(7), stmp(10, 1));
    assert_eq!(src.read(), &Scalar::Int(99));
    assert_eq!(clone.read(), &Scalar::Int(7));
}

#[test]
fn clone_preserves_stamp() {
    let src = fresh(Scalar::Int(10), stmp(5, 1));
    let mut clone = src.deep_clone();
    clone.set(Scalar::Int(99), stmp(3, 1)); // older, must lose
    assert_eq!(clone.read(), &Scalar::Int(10));
}

// --- displacement ---

#[test]
fn create_starts_not_displaced() {
    assert!(!fresh(Scalar::Int(0), stmp(1, 1)).is_displaced());
}

#[test]
fn displace_sets_flag() {
    let r = fresh(Scalar::Int(0), stmp(1, 1));
    r.displace();
    assert!(r.is_displaced());
}

#[test]
fn displaced_register_still_mutable() {
    let mut r = fresh(Scalar::Int(1), stmp(1, 1));
    r.displace();
    r.set(Scalar::Int(2), stmp(2, 1));
    assert_eq!(r.read(), &Scalar::Int(2));
}

#[test]
fn clone_of_displaced_is_not_displaced() {
    let src = fresh(Scalar::Int(0), stmp(1, 1));
    src.displace();
    assert!(!src.deep_clone().is_displaced());
}
