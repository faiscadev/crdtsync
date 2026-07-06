//! Convergence — the core CRDT law as a randomized property: replicas that
//! apply the same set of ops reach the same observable state, whatever the
//! arrival order.
//!
//! Several replicas each emit a burst of concurrent edits over a small, shared
//! key vocabulary — registers, counters, nested maps, lists, text, and scalar
//! overwrites that displace whatever a slot held. Every op they produce is
//! pooled, then replayed into fresh replicas in many permutations. A
//! deterministic PRNG drives generation and shuffling, so a failure names a
//! reproducing seed. The state is read back over the fixed vocabulary and
//! fingerprinted; every permutation must match.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::{Element, Scalar};

mod common;
use common::cid;

/// A small linear-congruential PRNG — deterministic, seedable, reproducible.
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

/// Top-level slots the edits fight over. Keeping the set small forces frequent
/// collisions and displacements.
const KEYS: &[&[u8]] = &[b"a", b"b", b"c"];
/// Sub-slots inside a nested map.
const SUBKEYS: &[&[u8]] = &[b"x", b"y"];
/// Tags an xml-element edit picks from — a small set so concurrent creates at
/// one key collide on tag, exercising the retag-is-replace identity split.
const TAGS: &[&[u8]] = &[b"div", b"span"];

fn key(rng: &mut Rng) -> &'static [u8] {
    KEYS[rng.below(KEYS.len())]
}

fn subkey(rng: &mut Rng) -> &'static [u8] {
    SUBKEYS[rng.below(SUBKEYS.len())]
}

/// The live length of a list slot, or 0 if the slot holds anything else.
fn list_len(d: &Document, k: &[u8]) -> usize {
    match d.get(k) {
        Some(Element::List(l)) => l.borrow().len(),
        _ => 0,
    }
}

/// The live length of a text slot, or 0 if the slot holds anything else.
fn text_len(d: &Document, k: &[u8]) -> usize {
    match d.get(k) {
        Some(Element::Text(t)) => t.borrow().len(),
        _ => 0,
    }
}

/// Apply one random edit to a document, returning the ops it emitted. Deletes
/// on a list or text pick a live index off the generating replica, so they are
/// real removals; on the peers the same op waits for its target to arrive.
fn random_edit(d: &mut Document, rng: &mut Rng) -> Vec<Op> {
    let k = key(rng);
    match rng.below(18) {
        0 => d.transact(|tx| tx.register(k, Scalar::Int(rng_val(rng)))),
        1 => d.transact(|tx| tx.inc(k, 1 + rng.below(4) as u32)),
        2 => d.transact(|tx| tx.dec(k, 1 + rng.below(4) as u32)),
        3 => d.transact(|tx| tx.set(k, Scalar::Int(rng_val(rng)))),
        4 => d.transact(|tx| tx.set(k, Scalar::Bool(rng.below(2) == 0))),
        5 => d.transact(|tx| tx.delete(k)),
        6 => {
            let sk = subkey(rng);
            d.transact(|tx| tx.map(k).register(sk, Scalar::Int(rng_val(rng))))
        }
        7 => {
            let sk = subkey(rng);
            d.transact(|tx| tx.map(k).inc(sk, 1 + rng.below(4) as u32))
        }
        8 => {
            let idx = rng.below(list_len(d, k) + 1);
            d.transact(|tx| tx.list(k).insert(idx, Scalar::Int(rng_val(rng))))
        }
        9 => {
            let len = list_len(d, k);
            if len == 0 {
                return Vec::new();
            }
            let idx = rng.below(len);
            d.transact(|tx| tx.list(k).delete(idx))
        }
        10 => {
            let idx = rng.below(text_len(d, k) + 1);
            d.transact(|tx| tx.text(k).insert(idx, "z"))
        }
        11 => {
            let len = text_len(d, k);
            if len == 0 {
                return Vec::new();
            }
            let idx = rng.below(len);
            d.transact(|tx| tx.text(k).delete(idx, 1))
        }
        12 => {
            // A second level of nesting: a map inside a map.
            let sk = subkey(rng);
            let ssk = subkey(rng);
            d.transact(|tx| tx.map(k).map(sk).register(ssk, Scalar::Int(rng_val(rng))))
        }
        13 => {
            let sk = subkey(rng);
            let ssk = subkey(rng);
            d.transact(|tx| tx.map(k).map(sk).inc(ssk, 1 + rng.below(4) as u32))
        }
        14 => {
            // Create an xml element and set one attr through its reused Map.
            let tag = TAGS[rng.below(TAGS.len())];
            let sk = subkey(rng);
            let v = rng_val(rng);
            d.transact(|tx| tx.xml_element(k, tag).attrs().register(sk, Scalar::Int(v)))
        }
        15 => d.transact(|tx| {
            tx.xml_fragment(k);
        }),
        16 => {
            // Create an xml element and insert one child — an element or a text
            // run — into its children sequence.
            let tag = TAGS[rng.below(TAGS.len())];
            if rng.below(2) == 0 {
                let ctag = TAGS[rng.below(TAGS.len())];
                d.transact(|tx| {
                    tx.xml_element(k, tag).children().insert_element(0, ctag);
                })
            } else {
                d.transact(|tx| {
                    tx.xml_element(k, tag)
                        .children()
                        .insert_text(0)
                        .insert(0, "z");
                })
            }
        }
        _ => d.transact(|tx| tx.map(k).set(subkey(rng), Scalar::Bool(true))),
    }
}

fn rng_val(rng: &mut Rng) -> i64 {
    rng.below(100) as i64
}

/// A stable, order-independent rendering of a document's observable state over
/// the fixed vocabulary — the equality oracle for convergence.
fn fingerprint(d: &Document) -> String {
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
        Element::XmlElement(x) => {
            let x = x.borrow();
            format!(
                "X{:?}{{{}}}[{}]",
                x.tag(),
                fp_attrs(&x.attrs()),
                fp_children(&x.children())
            )
        }
        Element::XmlFragment(f) => format!("F[{}]", fp_children(&f.borrow().children())),
    }
}

/// Fingerprint a children sequence in order — the convergence-critical structure.
fn fp_children(children: &std::rc::Rc<std::cell::RefCell<crdtsync_core::list::List>>) -> String {
    children
        .borrow()
        .values()
        .iter()
        .map(fp_element)
        .collect::<Vec<_>>()
        .join(",")
}

/// Fingerprint an attrs map by sorted key, so a divergent attr shows up.
fn fp_attrs(attrs: &std::rc::Rc<std::cell::RefCell<crdtsync_core::map::Map>>) -> String {
    let a = attrs.borrow();
    let mut keys = a.keys();
    keys.sort();
    keys.iter()
        .filter_map(|k| {
            a.get(k)
                .map(|v| format!("{}={}", String::from_utf8_lossy(k), fp_element(&v)))
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Fisher-Yates shuffle under the PRNG.
fn shuffle(ops: &[Op], rng: &mut Rng) -> Vec<Op> {
    let mut out = ops.to_vec();
    for i in (1..out.len()).rev() {
        out.swap(i, rng.below(i + 1));
    }
    out
}

/// Apply every op to a fresh replica and return its fingerprint. Buffering in
/// `apply` absorbs ops that arrive before their causal dependencies.
fn converge(ops: &[Op], client: u8) -> String {
    let mut d = Document::new(cid(client));
    for op in ops {
        d.apply(op);
    }
    fingerprint(&d)
}

#[test]
fn pooled_ops_converge_under_every_permutation() {
    // Miri interprets every op, so keep its sweep short; a native run covers a
    // far wider band of seeds.
    let seeds = if cfg!(miri) { 4 } else { 400 };
    for seed in 0..seeds {
        let mut rng = Rng::new(seed);

        // Three replicas each emit a burst of edits without seeing one another,
        // so every op is concurrent with the others.
        let mut replicas = [
            Document::new(cid(1)),
            Document::new(cid(2)),
            Document::new(cid(3)),
        ];
        // Each replica edits; between edits it sometimes catches up on the ops
        // its peers have pooled so far, so later edits build on a partly-merged
        // state — richer displacement histories than pure concurrency.
        let mut pool: Vec<Op> = Vec::new();
        let mut delivered = [0usize; 3];
        for _ in 0..18 {
            let which = rng.below(replicas.len());
            if rng.below(2) == 0 {
                for op in &pool[delivered[which]..] {
                    replicas[which].apply(op);
                }
                delivered[which] = pool.len();
            }
            let ops = random_edit(&mut replicas[which], &mut rng);
            pool.extend(ops);
        }

        // The reference is the pool applied in generation order.
        let reference = converge(&pool, 100);

        // Reverse, then several shuffles, must all land on the same state.
        let mut reversed = pool.clone();
        reversed.reverse();
        assert_eq!(
            converge(&reversed, 101),
            reference,
            "seed {seed}: reversed order diverged"
        );

        for round in 0..8 {
            let permuted = shuffle(&pool, &mut rng);
            assert_eq!(
                converge(&permuted, 110 + round as u8),
                reference,
                "seed {seed}: shuffle {round} diverged"
            );
        }

        // Idempotence: applying the whole pool twice changes nothing.
        let mut doubled = pool.clone();
        doubled.extend(pool.iter().cloned());
        assert_eq!(
            converge(&doubled, 120),
            reference,
            "seed {seed}: re-delivery changed the state"
        );
    }
}
