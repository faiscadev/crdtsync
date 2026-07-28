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

/// The state codec's fixed header: a version byte, the client id, the root
/// lamport clock, then the op-seq position — every integer little-endian. The
/// helpers below patch one field of an encoded snapshot so a test can hand a
/// decoder a clock or a counter no encoder would write.
const STATE_CLOCK_AT: usize = 1 + 16;
const STATE_SEQ_AT: usize = STATE_CLOCK_AT + 8;
/// The zone clocks follow the header: a `u32` count, then `(u32 zone, u64
/// lamport)` per entry, zone-id sorted.
const STATE_ZONE_CLOCKS_AT: usize = STATE_SEQ_AT + 8;

/// `bytes` with the encoded root lamport clock replaced by `lamport`.
pub fn with_root_clock(mut bytes: Vec<u8>, lamport: u64) -> Vec<u8> {
    bytes[STATE_CLOCK_AT..STATE_CLOCK_AT + 8].copy_from_slice(&lamport.to_le_bytes());
    bytes
}

/// `bytes` with the encoded op-seq position replaced by `seq`.
pub fn with_seq(mut bytes: Vec<u8>, seq: u64) -> Vec<u8> {
    bytes[STATE_SEQ_AT..STATE_SEQ_AT + 8].copy_from_slice(&seq.to_le_bytes());
    bytes
}

/// The byte offset of the encoded stamp-high-water section — a `u32` count then
/// `(ClientId, u64 lamport)` per entry, client-id sorted — which follows the
/// variable-length zone clocks.
fn stamp_high_water_at(bytes: &[u8]) -> usize {
    let zones = u32::from_le_bytes(
        bytes[STATE_ZONE_CLOCKS_AT..STATE_ZONE_CLOCKS_AT + 4]
            .try_into()
            .expect("four bytes"),
    ) as usize;
    STATE_ZONE_CLOCKS_AT + 4 + zones * (4 + 8)
}

/// `bytes` with the encoded stamp high-water section replaced by `entries` — the
/// declaration a snapshot makes about the ids each client holds. Entries are
/// written in the order given, so a test can hand the decoder a duplicate or an
/// out-of-order record as well as an under-declared one.
pub fn with_stamp_high_water(bytes: Vec<u8>, entries: &[(ClientId, u64)]) -> Vec<u8> {
    let at = stamp_high_water_at(&bytes);
    let old = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes")) as usize;
    let end = at + 4 + old * (16 + 8);
    let mut out = bytes[..at].to_vec();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (client, lamport) in entries {
        out.extend_from_slice(&client.as_bytes());
        out.extend_from_slice(&lamport.to_le_bytes());
    }
    out.extend_from_slice(&bytes[end..]);
    out
}

/// `bytes` with the clock of the sole encoded zone replaced by `lamport`.
/// Panics unless exactly one zone clock is present, so a codec change that moves
/// the field fails loudly rather than patching the wrong bytes.
pub fn with_only_zone_clock(mut bytes: Vec<u8>, lamport: u64) -> Vec<u8> {
    let count = u32::from_le_bytes(
        bytes[STATE_ZONE_CLOCKS_AT..STATE_ZONE_CLOCKS_AT + 4]
            .try_into()
            .expect("four bytes"),
    );
    assert_eq!(count, 1, "expected exactly one encoded zone clock");
    let at = STATE_ZONE_CLOCKS_AT + 4 + 4;
    bytes[at..at + 8].copy_from_slice(&lamport.to_le_bytes());
    bytes
}
