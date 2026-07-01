mod common;
use common::stmp;

#[test]
fn larger_lamport_wins() {
    assert!(stmp(2, 1).gt(&stmp(1, 1)));
}

#[test]
fn smaller_lamport_loses() {
    assert!(!stmp(1, 1).gt(&stmp(2, 1)));
}

#[test]
fn equal_lamport_larger_client_wins() {
    assert!(stmp(5, 2).gt(&stmp(5, 1)));
}

#[test]
fn equal_lamport_smaller_client_loses() {
    assert!(!stmp(5, 1).gt(&stmp(5, 2)));
}

#[test]
fn lamport_dominates_client() {
    // Higher lamport wins even with a smaller client id.
    assert!(stmp(6, 1).gt(&stmp(5, 9)));
}

#[test]
fn equal_stamps_returns_false() {
    assert!(!stmp(5, 1).gt(&stmp(5, 1)));
}

#[test]
fn irreflexive() {
    let s = stmp(3, 4);
    assert!(!s.gt(&s));
}

#[test]
fn antisymmetric_lamport() {
    let (a, b) = (stmp(2, 1), stmp(1, 1));
    assert!(a.gt(&b));
    assert!(!b.gt(&a));
}

#[test]
fn antisymmetric_client() {
    let (a, b) = (stmp(5, 2), stmp(5, 1));
    assert!(a.gt(&b));
    assert!(!b.gt(&a));
}

#[test]
fn transitive() {
    let (a, b, c) = (stmp(3, 1), stmp(2, 1), stmp(1, 1));
    assert!(a.gt(&b) && b.gt(&c));
    assert!(a.gt(&c));
}

#[test]
fn trichotomy_distinct() {
    let (a, b) = (stmp(5, 2), stmp(5, 1));
    assert!(a.gt(&b) ^ b.gt(&a)); // exactly one direction holds
}
