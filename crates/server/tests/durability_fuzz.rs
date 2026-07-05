// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Durability — a randomized property: a store-backed hub, reopened over its
//! persisted log, is indistinguishable from the one that wrote it.
//!
//! Several clients emit concurrent edits over a shared key vocabulary; every op
//! is ingested into a hub whose writes go to a [`Store`]. Reloading the store
//! into a fresh hub must reproduce the same merged state and the same server
//! sequence, and a subscriber catching up from zero must converge to it. A
//! deterministic PRNG drives generation, so a failure names a reproducing seed.

use std::fs;

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar};
use crdtsync_server::store::Store;
use crdtsync_server::{Catchup, Hub};

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 17
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

const SERVER: u8 = 0xFF;
const ROOM: &[u8] = b"room";
const KEYS: &[&[u8]] = &[b"a", b"b", b"c"];
const SUBKEYS: &[&[u8]] = &[b"x", b"y"];

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// One random edit on a client's document, returning the ops it emitted.
fn random_edit(d: &mut Document, rng: &mut Rng) -> Vec<crdtsync_core::Op> {
    let k = KEYS[rng.below(KEYS.len())];
    let sk = SUBKEYS[rng.below(SUBKEYS.len())];
    let v = rng.below(100) as i64;
    match rng.below(9) {
        0 => d.transact(|tx| tx.register(k, Scalar::Int(v))),
        1 => d.transact(|tx| tx.inc(k, 1 + rng.below(4) as u32)),
        2 => d.transact(|tx| tx.dec(k, 1 + rng.below(4) as u32)),
        3 => d.transact(|tx| tx.set(k, Scalar::Int(v))),
        4 => d.transact(|tx| tx.delete(k)),
        5 => d.transact(|tx| tx.map(k).register(sk, Scalar::Int(v))),
        6 => d.transact(|tx| tx.map(k).inc(sk, 1 + rng.below(4) as u32)),
        7 => d.transact(|tx| tx.list(k).insert(0, Scalar::Int(v))),
        _ => d.transact(|tx| tx.text(k).insert(0, "z")),
    }
}

fn fp_element(e: &Element) -> String {
    match e {
        Element::Scalar(s) => format!("S{s:?}"),
        Element::Register(r) => format!("R{:?}", r.borrow().read()),
        Element::Counter(c) => format!("C{}", c.borrow().read()),
        Element::Map(m) => {
            let m = m.borrow();
            let parts: Vec<String> = SUBKEYS
                .iter()
                .filter_map(|sk| {
                    m.get(sk)
                        .map(|v| format!("{}={}", String::from_utf8_lossy(sk), fp_element(&v)))
                })
                .collect();
            format!("M[{}]", parts.join(","))
        }
        Element::List(l) => {
            let l = l.borrow();
            let parts: Vec<String> = (0..l.len())
                .filter_map(|i| l.get(i).map(|v| fp_element(&v)))
                .collect();
            format!("L[{}]", parts.join(","))
        }
        Element::Text(t) => format!("T{:?}", t.borrow().as_string()),
    }
}

/// A stable rendering of a room's merged state over the fixed vocabulary.
fn fingerprint(hub: &Hub) -> String {
    KEYS.iter()
        .map(|k| {
            let slot = hub
                .get(ROOM, k)
                .as_ref()
                .map_or_else(|| "_".to_string(), fp_element);
            format!("{}={}", String::from_utf8_lossy(k), slot)
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// Unwrap a catch-up that must be a plain op delta — these rooms are never
/// compacted.
fn ops(c: Catchup) -> Vec<Op> {
    match c {
        Catchup::Ops(v) => v,
        Catchup::Snapshot { .. } => panic!("expected an op delta, got a snapshot"),
    }
}

/// The same rendering for a plain document — the catch-up oracle.
fn doc_fingerprint(d: &Document) -> String {
    KEYS.iter()
        .map(|k| {
            let slot = d
                .get(k)
                .as_ref()
                .map_or_else(|| "_".to_string(), fp_element);
            format!("{}={}", String::from_utf8_lossy(k), slot)
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[test]
fn a_reopened_hub_reproduces_state_sequence_and_catch_up() {
    for seed in 0..150u64 {
        let mut rng = Rng::new(seed);
        let tmp = tempdir();

        // Build a store-backed hub and ingest a pooled stream of concurrent
        // edits from three clients.
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        let mut clients = [
            Document::new(cid(1)),
            Document::new(cid(2)),
            Document::new(cid(3)),
        ];
        for _ in 0..16 {
            let which = rng.below(clients.len());
            let ops = random_edit(&mut clients[which], &mut rng);
            hub.ingest(ROOM, ops, None).unwrap();
        }

        let live = fingerprint(&hub);
        let seq = hub.seq(ROOM);

        // Reopen the store into a fresh hub: same state, same sequence.
        let mut reloaded = Hub::from_rooms(
            cid(SERVER),
            Store::open(tmp.path()).unwrap().load().unwrap(),
        )
        .unwrap();
        assert_eq!(fingerprint(&reloaded), live, "seed {seed}: reload diverged");
        assert_eq!(reloaded.seq(ROOM), seq, "seed {seed}: sequence drifted");

        // A subscriber catching up from zero converges to the same state.
        let mut fresh = Document::new(cid(9));
        for op in ops(reloaded.catch_up(ROOM, 0)) {
            fresh.apply(&op);
        }
        assert_eq!(
            doc_fingerprint(&fresh),
            live,
            "seed {seed}: catch-up diverged"
        );

        // Re-ingesting the whole log is idempotent — no double-count, no growth.
        let replay = ops(reloaded.catch_up(ROOM, 0));
        assert!(
            reloaded.ingest(ROOM, replay, None).unwrap().is_empty(),
            "seed {seed}: a resend of the log grew it"
        );
        assert_eq!(reloaded.seq(ROOM), seq);
    }
}

// --- a tempdir without pulling in a dev-dependency ---

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-durfuzz-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
