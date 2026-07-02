//! Diff as a randomized property.
//!
//! A structural diff must be sound and complete: it reports a change exactly
//! when the two snapshots differ observably, and nothing when they read the
//! same. Over many random edit sequences this checks three invariants — the
//! diff is empty iff an observable read of the two replicas is equal, it
//! round-trips through its codec, and it is deterministic — so a regression
//! names a reproducing seed rather than a lucky example.
//!
//! The oracle is a materialized read over the fixed edit vocabulary, not
//! `encode_state`: two replicas can hold identical observable state yet encode
//! differently (a no-op edit still advances the clock and the dedup set), which
//! the diff correctly ignores.

use crdtsync_core::diff::{decode_changes, diff, encode_changes};
use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Scalar};

/// A small linear-congruential PRNG — deterministic, seedable, reproducible.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(0x5851_F42D_4C95_7F2D).wrapping_add(1);
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

const KEYS: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
const SUBKEYS: [&[u8]; 2] = [b"x", b"y"];

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn rng_val(rng: &mut Rng) -> i64 {
    (rng.below(7) as i64) - 3
}

fn list_len(d: &Document, k: &[u8]) -> usize {
    match d.get(k) {
        Some(Element::List(l)) => l.borrow().len(),
        _ => 0,
    }
}

fn text_len(d: &Document, k: &[u8]) -> usize {
    match d.get(k) {
        Some(Element::Text(t)) => t.borrow().len(),
        _ => 0,
    }
}

/// A leaf element's value as a canonical string — an inline scalar or a register
/// reads the same way, a counter its integer.
fn leaf(e: &Element) -> String {
    match e {
        Element::Scalar(s) => format!("s{s:?}"),
        Element::Register(r) => format!("s{:?}", r.borrow().read()),
        Element::Counter(c) => format!("c{}", c.borrow().read()),
        other => format!("?{:?}", other.kind()),
    }
}

/// An observable read of `d` over the fixed edit vocabulary: every top-level
/// key, a map's subkeys, a list's values, a text's string. Two replicas that
/// observe equal hold the same materialized state, whatever their causal
/// metadata — this is the oracle the diff is checked against.
fn observe(d: &Document) -> Vec<String> {
    let mut out = Vec::new();
    for k in KEYS {
        let rendered = match d.get(k) {
            None => "none".to_string(),
            Some(Element::Map(m)) => {
                let m = m.borrow();
                let subs: Vec<String> = SUBKEYS
                    .iter()
                    .map(|sk| match m.get(sk) {
                        Some(e) => format!("{}={}", String::from_utf8_lossy(sk), leaf(&e)),
                        None => format!("{}=none", String::from_utf8_lossy(sk)),
                    })
                    .collect();
                format!("map[{}]", subs.join(","))
            }
            Some(Element::List(l)) => {
                let vals: Vec<String> = l.borrow().values().iter().map(leaf).collect();
                format!("list[{}]", vals.join(","))
            }
            Some(Element::Text(t)) => format!("text[{}]", t.borrow().as_string()),
            Some(e) => leaf(&e),
        };
        out.push(format!("{}:{rendered}", String::from_utf8_lossy(k)));
    }
    out
}

/// Apply one random edit — some are no-ops on the current state (a delete of an
/// absent key, a re-set of the same value), so the sequence produces both
/// changed and unchanged pairs.
fn random_edit(d: &mut Document, rng: &mut Rng) {
    let k = KEYS[rng.below(KEYS.len())];
    match rng.below(12) {
        0 => d.transact(|tx| tx.register(k, Scalar::Int(rng_val(rng)))),
        1 => d.transact(|tx| tx.inc(k, 1 + rng.below(4) as u32)),
        2 => d.transact(|tx| tx.dec(k, 1 + rng.below(4) as u32)),
        3 => d.transact(|tx| tx.set(k, Scalar::Bool(rng.below(2) == 0))),
        4 => d.transact(|tx| tx.delete(k)),
        5 | 6 => {
            let sk = SUBKEYS[rng.below(SUBKEYS.len())];
            d.transact(|tx| tx.map(k).register(sk, Scalar::Int(rng_val(rng))))
        }
        7 => {
            let idx = rng.below(list_len(d, k) + 1);
            d.transact(|tx| tx.list(k).insert(idx, Scalar::Int(rng_val(rng))))
        }
        8 => {
            let n = list_len(d, k);
            if n > 0 {
                let idx = rng.below(n);
                d.transact(|tx| tx.list(k).delete(idx))
            } else {
                Vec::new()
            }
        }
        9 | 10 => {
            let idx = rng.below(text_len(d, k) + 1);
            d.transact(|tx| tx.text(k).insert(idx, "hi"))
        }
        _ => {
            let n = text_len(d, k);
            if n > 0 {
                let idx = rng.below(n);
                let count = 1 + rng.below(n - idx);
                d.transact(|tx| tx.text(k).delete(idx, count))
            } else {
                Vec::new()
            }
        }
    };
}

#[test]
fn diff_is_empty_exactly_when_the_states_are_equal() {
    let seeds = if cfg!(miri) { 4 } else { 400 };
    for seed in 0..seeds {
        let mut rng = Rng::new(seed);
        let mut d = Document::new(cid(1));
        // A base state, then a snapshot of it as the "old" replica.
        for _ in 0..rng.below(8) {
            random_edit(&mut d, &mut rng);
        }
        let old = Document::decode_state(&d.encode_state()).unwrap();
        // Further edits (possibly none, possibly no-ops) produce the "new" state.
        for _ in 0..rng.below(6) {
            random_edit(&mut d, &mut rng);
        }

        let changes = diff(&old, &d);
        let observably_equal = observe(&old) == observe(&d);
        assert_eq!(
            changes.is_empty(),
            observably_equal,
            "seed {seed}: diff empty={} but observably_equal={observably_equal}",
            changes.is_empty(),
        );

        // The change list round-trips through its codec.
        assert_eq!(
            decode_changes(&encode_changes(&changes)).unwrap(),
            changes,
            "seed {seed}: change-list codec did not round-trip",
        );

        // The diff is deterministic.
        assert_eq!(
            diff(&old, &d),
            changes,
            "seed {seed}: diff not deterministic"
        );
    }
}

#[test]
fn a_snapshot_never_differs_from_itself() {
    let seeds = if cfg!(miri) { 4 } else { 200 };
    for seed in 0..seeds {
        let mut rng = Rng::new(seed);
        let mut d = Document::new(cid(1));
        for _ in 0..rng.below(10) {
            random_edit(&mut d, &mut rng);
        }
        let copy = Document::decode_state(&d.encode_state()).unwrap();
        assert!(
            diff(&d, &copy).is_empty(),
            "seed {seed}: a replica differs from its own snapshot",
        );
    }
}
