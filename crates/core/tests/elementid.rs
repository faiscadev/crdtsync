use crdtsync_core::{ElementId, ElementKind};

mod common;
use common::eid;

#[test]
fn from_bytes_roundtrips() {
    let mut b = [0u8; 16];
    for (i, x) in b.iter_mut().enumerate() {
        *x = (i * 3 + 1) as u8;
    }
    assert_eq!(ElementId::from_bytes(b).as_bytes(), b);
}

#[test]
fn derive_is_deterministic() {
    let parent = eid(7, 42);
    let a = ElementId::derive(parent, b"votes", ElementKind::Counter);
    let b = ElementId::derive(parent, b"votes", ElementKind::Counter);
    assert_eq!(a, b);
}

// Two replicas with the same parent + key + kind converge on one id.
#[test]
fn derive_converges_across_replicas() {
    let parent = eid(7, 42);
    let a = ElementId::derive(parent, b"title", ElementKind::Register);
    let b = ElementId::derive(parent, b"title", ElementKind::Register);
    assert_eq!(a, b);
}

// Same key, different kind -> different id (how merge tells Counter@"x" from
// Register@"x").
#[test]
fn same_key_different_kind_distinct() {
    let parent = eid(7, 42);
    let counter = ElementId::derive(parent, b"x", ElementKind::Counter);
    let register = ElementId::derive(parent, b"x", ElementKind::Register);
    assert_ne!(counter, register);
}

#[test]
fn different_key_distinct() {
    let parent = eid(7, 42);
    let a = ElementId::derive(parent, b"a", ElementKind::Map);
    let b = ElementId::derive(parent, b"b", ElementKind::Map);
    assert_ne!(a, b);
}

#[test]
fn different_parent_distinct() {
    let a = ElementId::derive(eid(1, 1), b"k", ElementKind::Counter);
    let b = ElementId::derive(eid(2, 2), b"k", ElementKind::Counter);
    assert_ne!(a, b);
}

// Binary-safe key: embedded NUL is part of the derivation input.
#[test]
fn key_is_binary_safe() {
    let parent = eid(7, 42);
    let a = ElementId::derive(parent, &[0x01, 0x00, 0x02], ElementKind::Counter);
    let b = ElementId::derive(parent, &[0x01, 0x00, 0x03], ElementKind::Counter);
    assert_ne!(a, b);
}
