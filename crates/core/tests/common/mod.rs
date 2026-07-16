// Each test binary pulls in only a subset of these helpers.
#![allow(dead_code)]

use crdtsync_core::{ClientId, Element, ElementId, Scalar, Stamp};

/// ClientId from a single leading byte (rest zero).
pub fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// ElementId from two big-endian u64 halves.
pub fn eid(hi: u64, lo: u64) -> ElementId {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&hi.to_be_bytes());
    b[8..].copy_from_slice(&lo.to_be_bytes());
    ElementId::from_bytes(b)
}

pub fn default_id() -> ElementId {
    eid(0xFF, 0)
}

pub fn stmp(lamport: u64, client_first: u8) -> Stamp {
    Stamp {
        lamport,
        client: cid(client_first),
        offset: 0,
    }
}

#[track_caller]
pub fn assert_scalar(e: &Element, expected: Scalar) {
    match e {
        Element::Scalar(s) => assert_eq!(*s, expected),
        _ => panic!("expected a SCALAR element"),
    }
}
