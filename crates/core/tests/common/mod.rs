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

fn put_stamp(out: &mut Vec<u8>, s: Stamp) {
    out.extend_from_slice(&s.lamport.to_le_bytes());
    out.extend_from_slice(&s.client.as_bytes());
    match s.offset {
        0 => out.push(0),
        offset => {
            out.push(1);
            out.extend_from_slice(&offset.to_le_bytes());
        }
    }
}

/// Append a sequence tombstone-run record — start stamp, id count, and an anchor
/// hanging right off `parent` (or the sequence start). Lets a test hand a
/// decoder a run of any length, and streams an encoder would never emit.
pub fn put_run_record(out: &mut Vec<u8>, start: Stamp, len: u32, parent: Option<Stamp>) {
    put_stamp(out, start);
    out.extend_from_slice(&len.to_le_bytes());
    match parent {
        None => out.push(0),
        Some(p) => {
            out.push(1);
            put_stamp(out, p);
        }
    }
    out.push(1); // Side::Right
}

/// A list state snapshot holding no live items and the given run records.
pub fn dead_run_snapshot(id: ElementId, runs: &[(Stamp, u32, Option<Stamp>)]) -> Vec<u8> {
    let mut out = id.as_bytes().to_vec();
    out.extend_from_slice(&0u32.to_le_bytes()); // no live items
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (start, len, parent) in runs {
        put_run_record(&mut out, *start, *len, *parent);
    }
    out
}

#[track_caller]
pub fn assert_scalar(e: &Element, expected: Scalar) {
    match e {
        Element::Scalar(s) => assert_eq!(*s, expected),
        _ => panic!("expected a SCALAR element"),
    }
}
