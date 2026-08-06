//! Convergence — the core CRDT law as a randomized property: replicas that
//! apply the same set of ops reach the same observable state, whatever the
//! arrival order.
//!
//! Several replicas each emit a burst of concurrent edits over a small, shared
//! key vocabulary — registers, counters, nested maps, lists, text, xml, scalar
//! overwrites that displace whatever a slot held, and the doc-level sets: ranged
//! marks (create / set-payload / delete) and ACL tuples (grant / revoke). Every
//! op they produce is pooled, then replayed into fresh replicas in many
//! permutations. A deterministic PRNG drives generation and shuffling, so a
//! failure names a reproducing seed. The state is read back over the fixed
//! vocabulary — the keyed tree plus the annotation and ACL sets — and
//! fingerprinted; every permutation must match.

use crdtsync_core::acl::{AclEffect, AclGrant, AclSubject, Capability};
use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::op::Op;
use crdtsync_core::path::encode_path;
use crdtsync_core::ranged::RangeAnchor;
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

/// Mark names the ranged arm picks from — a small set so concurrent marks collide.
const MARK_NAMES: &[&[u8]] = &[b"bold", b"link"];

fn ranchor(seq: ElementId, pos: RelativePosition) -> RangeAnchor {
    RangeAnchor { seq, pos }
}

/// Collect, from the live tree under the shared keys, every movable xml node (an
/// element or text run that lives in a children sequence) and every xml container
/// that can host a move (an element or fragment). Ids are stable across replicas,
/// so the generating replica's pick names the same nodes everywhere.
fn xml_move_targets(d: &Document) -> (Vec<ElementId>, Vec<ElementId>) {
    fn walk(
        children: &std::rc::Rc<std::cell::RefCell<crdtsync_core::list::List>>,
        movable: &mut Vec<ElementId>,
        parents: &mut Vec<ElementId>,
    ) {
        for child in children.borrow().values() {
            match child {
                Element::XmlElement(x) => {
                    let x = x.borrow();
                    movable.push(x.id());
                    parents.push(x.id());
                    walk(&x.children(), movable, parents);
                }
                Element::Text(t) => movable.push(t.borrow().id()),
                _ => {}
            }
        }
    }
    let mut movable = Vec::new();
    let mut parents = Vec::new();
    for k in KEYS {
        match d.get(k) {
            Some(Element::XmlElement(x)) => {
                let x = x.borrow();
                parents.push(x.id());
                walk(&x.children(), &mut movable, &mut parents);
            }
            Some(Element::XmlFragment(f)) => {
                let f = f.borrow();
                parents.push(f.id());
                walk(&f.children(), &mut movable, &mut parents);
            }
            _ => {}
        }
    }
    (movable, parents)
}

/// A top-level xml container at `k`, with the info a child-delete needs: the
/// element's tag (to re-address it) or that it is a fragment, plus its live child
/// count. `None` if `k` holds no xml container.
enum TopXml {
    Element(Vec<u8>),
    Fragment,
}

fn top_xml(d: &Document, k: &[u8]) -> Option<(TopXml, usize)> {
    match d.get(k) {
        Some(Element::XmlElement(x)) => {
            let x = x.borrow();
            Some((
                TopXml::Element(x.tag().to_vec()),
                x.children().borrow().len(),
            ))
        }
        Some(Element::XmlFragment(f)) => {
            Some((TopXml::Fragment, f.borrow().children().borrow().len()))
        }
        _ => None,
    }
}

/// Apply one random edit to a document, returning the ops it emitted. Deletes
/// on a list or text pick a live index off the generating replica, so they are
/// real removals; on the peers the same op waits for its target to arrive.
fn random_edit(d: &mut Document, rng: &mut Rng) -> Vec<Op> {
    let k = key(rng);
    match rng.below(28) {
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
        17 => {
            // Mark a text body — a RangedElement in the doc's annotation set. The
            // three shared keys are so heavily displaced that a live Text rarely
            // survives to be marked, so force one first (an ordinary text create,
            // itself a displacement) to keep this op family actually exercised.
            let seq = ElementId::derive(d.root_id(), k, ElementKind::Text);
            let name = MARK_NAMES[rng.below(MARK_NAMES.len())];
            let needs_body = text_len(d, k) == 0;
            d.transact(|tx| {
                if needs_body {
                    tx.text(k).insert(0, "z");
                }
                tx.ranged().mark(
                    name,
                    ranchor(seq, RelativePosition::Start),
                    ranchor(seq, RelativePosition::End),
                    Scalar::Bool(true),
                );
            })
        }
        18 => {
            // Change the payload of a live ranged element (last-writer-wins).
            let live = d.ranged_elements();
            if live.is_empty() {
                return Vec::new();
            }
            let rid = live[rng.below(live.len())].id;
            let v = rng_val(rng);
            d.transact(|tx| tx.ranged().set_payload(rid, Scalar::Int(v)))
        }
        19 => {
            // Delete a live ranged element (delete-wins over a concurrent payload).
            let live = d.ranged_elements();
            if live.is_empty() {
                return Vec::new();
            }
            let rid = live[rng.below(live.len())].id;
            d.transact(|tx| tx.ranged().delete(rid))
        }
        20 => {
            // Grant a path-scoped ACL tuple over a top-level slot.
            let subject = if rng.below(2) == 0 {
                AclSubject::Anyone
            } else {
                AclSubject::Actor(cid((1 + rng.below(3)) as u8))
            };
            let effect = if rng.below(2) == 0 {
                AclEffect::Allow
            } else {
                AclEffect::Deny
            };
            let path = encode_path(&[k]);
            d.transact(|tx| {
                tx.acl().grant(
                    subject,
                    AclGrant::Capability(Capability::Read),
                    effect,
                    path,
                    cid(1),
                );
            })
        }
        21 => {
            // Revoke a live ACL tuple (tombstone, delete-wins).
            let live = d.acl_tuples();
            if live.is_empty() {
                return Vec::new();
            }
            let id = live[rng.below(live.len())].id;
            d.transact(|tx| tx.acl().revoke(id))
        }
        22 => {
            // Reparent an xml node: move a movable node under a new xml parent —
            // the displacing tree-move family. Node and parent are picked off the
            // generating replica's live tree; a move under the node's own
            // descendant is dropped as a cycle at apply time.
            let (movable, parents) = xml_move_targets(d);
            if movable.is_empty() || parents.is_empty() {
                return Vec::new();
            }
            let node = movable[rng.below(movable.len())];
            let parent = parents[rng.below(parents.len())];
            let idx = rng.below(2);
            d.transact(|tx| tx.move_xml(node, parent, idx))
        }
        23 => {
            // Delete an xml child by index — exercises delete-wins-over-move and a
            // delete into a displaced children sequence (the birth list of a moved
            // node whose slot a scalar later displaces).
            let Some((which, len)) = top_xml(d, k) else {
                return Vec::new();
            };
            if len == 0 {
                return Vec::new();
            }
            let idx = rng.below(len);
            match which {
                TopXml::Element(tag) => d.transact(|tx| {
                    tx.xml_element(k, &tag).children().delete(idx);
                }),
                TopXml::Fragment => d.transact(|tx| {
                    tx.xml_fragment(k).children().delete(idx);
                }),
            }
        }
        24 => {
            // A multi-codepoint text run: consecutive char_ids chained
            // parent-to-child, the shape a delete collapses to one range.
            let idx = rng.below(text_len(d, k) + 1);
            d.transact(|tx| tx.text(k).insert(idx, "quick"))
        }
        25 => {
            // Delete a contiguous span of text — the delete-heavy family that
            // extends and welds ranges.
            let len = text_len(d, k);
            if len == 0 {
                return Vec::new();
            }
            let idx = rng.below(len);
            let count = 1 + rng.below(len - idx);
            d.transact(|tx| tx.text(k).delete(idx, count))
        }
        26 => {
            // Delete a contiguous span of list items.
            let len = list_len(d, k);
            if len == 0 {
                return Vec::new();
            }
            let idx = rng.below(len);
            let count = 1 + rng.below(len - idx);
            d.transact(|tx| {
                let mut list = tx.list(k);
                for _ in 0..count {
                    list.delete(idx);
                }
            })
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
    let slots = KEYS
        .iter()
        .map(|k| {
            let slot = d
                .get(k)
                .as_ref()
                .map_or_else(|| "_".to_string(), fp_element);
            format!("{}={}", String::from_utf8_lossy(k), slot)
        })
        .collect::<Vec<_>>()
        .join(";");
    // The doc-level annotation and ACL sets are order-independent by id, so a
    // sorted rendering is the equality oracle for the tree-move / mark / ACL
    // op families the shuffle sweep folds.
    format!(
        "{slots}||RANGED[{}]||ACL[{}]",
        fp_sorted(d.ranged_elements().iter().map(|r| format!("{r:?}"))),
        fp_sorted(d.acl_tuples().iter().map(|t| format!("{t:?}"))),
    )
}

/// A stable rendering of a doc-level CRDT set: sort the per-entry debug strings so
/// the result is independent of iteration order, and equal iff the sets converged.
fn fp_sorted(entries: impl Iterator<Item = String>) -> String {
    let mut parts: Vec<String> = entries.collect();
    parts.sort();
    parts.join(",")
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

/// Deliver `ops` to a fresh replica in `rounds` independently shuffled passes,
/// so every op arrives out of order and is re-delivered, then fingerprint it.
fn converge_shuffled(ops: &[Op], client: u8, rounds: usize, rng: &mut Rng) -> String {
    let mut d = Document::new(cid(client));
    for _ in 0..rounds {
        for op in shuffle(ops, rng) {
            d.apply(&op);
        }
    }
    fingerprint(&d)
}

/// Atomic grouping must not change what a set of ops merges to. The same
/// identically-seeded generator drives two pools — one shipping each burst as an
/// atomic transaction, one streaming the same edits ungrouped — and both must
/// converge under shuffled re-delivery. Grouping is a visibility boundary; a
/// member the state cannot express when its group commits — its target's create
/// still in flight — is re-held untagged rather than applied to nothing.
#[test]
fn atomic_groups_do_not_change_what_ops_merge_to() {
    // Miri interprets every op, and this sweep folds each pool many times over;
    // keep its share of the core Miri shard small — a native run covers the band.
    let seeds = if cfg!(miri) { 1 } else { 120 };
    let shuffles = if cfg!(miri) { 2 } else { 6 };
    for seed in 0..seeds {
        for &atomic in &[true, false] {
            let mut rng = Rng::new(seed);
            let mut replicas = [
                Document::new(cid(1)),
                Document::new(cid(2)),
                Document::new(cid(3)),
            ];
            let mut pool: Vec<Op> = Vec::new();
            let mut delivered = [0usize; 3];
            for _ in 0..14 {
                let which = rng.below(replicas.len());
                if rng.below(2) == 0 {
                    for op in &pool[delivered[which]..] {
                        replicas[which].apply(op);
                    }
                    delivered[which] = pool.len();
                }
                // A burst of 1–3 edits, shipped as one atomic transaction or as
                // the same ops ungrouped.
                let burst = 1 + rng.below(3);
                if atomic {
                    replicas[which].begin_atomic();
                    for _ in 0..burst {
                        let _ = random_edit(&mut replicas[which], &mut rng);
                    }
                    pool.extend(replicas[which].commit_atomic());
                } else {
                    for _ in 0..burst {
                        let ops = random_edit(&mut replicas[which], &mut rng);
                        pool.extend(ops);
                    }
                }
            }
            // Guard the oracle against a silently empty or single-member pool:
            // groups of one commit the moment they arrive and would exercise none
            // of the multi-member commit path this test exists for.
            assert!(
                pool.len() > 10,
                "seed {seed}: the generator produced too little to exercise a group"
            );
            let tagged = pool.iter().filter(|op| op.tx.is_some()).count();
            if atomic {
                assert_eq!(tagged, pool.len(), "seed {seed}: every op rides a group");
                assert!(
                    pool.iter()
                        .any(|op| op.tx.as_ref().is_some_and(|tx| tx.count > 1)),
                    "seed {seed}: no group holds more than one member"
                );
            } else {
                assert_eq!(tagged, 0, "seed {seed}: the control ships nothing tagged");
            }

            let reference = converge(&pool, 100);
            for round in 0..shuffles {
                assert_eq!(
                    converge_shuffled(&pool, 130 + round as u8, 2, &mut rng),
                    reference,
                    "seed {seed}: atomic={atomic} shuffle {round} diverged"
                );
            }

            // The authors themselves are replicas: each has applied its own edits
            // eagerly and every peer op it was handed, so folding in the whole
            // pool must land them on the same state as a fresh replica.
            for (i, r) in replicas.iter_mut().enumerate() {
                for op in shuffle(&pool, &mut rng) {
                    r.apply(&op);
                }
                assert_eq!(
                    fingerprint(r),
                    reference,
                    "seed {seed}: atomic={atomic} author {i} diverged from a fresh replica"
                );
            }
        }
    }
}

/// A rewritten `count` must not make convergence depend on delivery order. The
/// same pool is built as atomic groups and then re-tagged by a hostile relay —
/// sizes no arrival meets, and sizes a group's members do not share — and every
/// replica must fold it to one state on the ops alone, before any eviction.
///
/// Whether a *bucket* looks unreachable is a property of which of its members
/// have landed, so nothing may be decided from it: a replica that released a
/// bucket the moment its members disagreed would release a different set from one
/// served the same ops in another order, and the member that arrived after the
/// release would be held against a size its bucket had already given up on. Only
/// a judgement on a member's own declared size is order-free. Eviction then has to
/// leave them converged too, on whatever each was still holding.
#[test]
fn rewritten_group_counts_converge_under_every_order() {
    let seeds = if cfg!(miri) { 1 } else { 60 };
    let shuffles = if cfg!(miri) { 2 } else { 6 };
    for seed in 0..seeds {
        let mut rng = Rng::new(seed);
        let mut author = Document::new(cid(1));
        let mut pool: Vec<Op> = Vec::new();
        for _ in 0..8 {
            author.begin_atomic();
            for _ in 0..1 + rng.below(3) {
                let _ = random_edit(&mut author, &mut rng);
            }
            pool.extend(author.commit_atomic());
        }
        assert!(
            pool.len() > 8,
            "seed {seed}: too little to exercise a group"
        );

        // The relay rewrites one member in three: a size no arrival meets, or one
        // its group-mates do not share.
        let forged: Vec<Op> = pool
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let mut op = op.clone();
                if i % 3 == 0 {
                    let tx = op.tx.expect("every op rides a group");
                    op.tx = Some(match rng.below(3) {
                        0 => crdtsync_core::Tx { count: 0, ..tx },
                        1 => crdtsync_core::Tx {
                            count: u32::MAX,
                            ..tx
                        },
                        _ => crdtsync_core::Tx {
                            count: tx.count + 1,
                            ..tx
                        },
                    });
                }
                op
            })
            .collect();
        assert!(
            forged.iter().zip(&pool).any(|(f, o)| f.tx != o.tx),
            "seed {seed}: the relay rewrote nothing"
        );

        let held = converge_shuffled(&forged, 100, 1, &mut Rng::new(seed));
        let evicted = converge_evicting(&forged, 100, &mut Rng::new(seed));
        for round in 0..shuffles {
            assert_eq!(
                converge_shuffled(&forged, 140 + round as u8, 1, &mut rng),
                held,
                "seed {seed}: shuffle {round} diverged on the ops alone"
            );
            assert_eq!(
                converge_evicting(&forged, 140 + round as u8, &mut rng),
                evicted,
                "seed {seed}: shuffle {round} diverged after eviction"
            );
        }

        // A rewrite consistent across every member of a group is one no receiver
        // can tell from an honest group of that size, so it *does* commit — at
        // that size, over whichever members the order delivered first. The members
        // left over are strays of a key that has resolved, so they land on the ops
        // alone, and every order lands the same ones.
        let shrunk: Vec<Op> = pool
            .iter()
            .map(|op| {
                let mut op = op.clone();
                if let Some(tx) = op.tx {
                    if tx.count > 1 {
                        op.tx = Some(crdtsync_core::Tx {
                            count: tx.count - 1,
                            ..tx
                        });
                    }
                }
                op
            })
            .collect();
        let held = converge_shuffled(&shrunk, 100, 1, &mut Rng::new(seed));
        let reference = converge_evicting(&shrunk, 100, &mut Rng::new(seed));
        assert_eq!(
            held, reference,
            "seed {seed}: a group rewritten smaller left something for eviction"
        );
        for round in 0..shuffles {
            assert_eq!(
                converge_shuffled(&shrunk, 160 + round as u8, 1, &mut rng),
                held,
                "seed {seed}: shuffle {round} diverged on a group rewritten smaller"
            );
            assert_eq!(
                converge_evicting(&shrunk, 160 + round as u8, &mut rng),
                reference,
                "seed {seed}: shuffle {round} diverged after eviction"
            );
        }
    }
}

/// Deliver `ops` shuffled, then evict whatever the replica is still holding —
/// the policy every deployment runs, and what makes a group nobody will complete
/// converge rather than sit.
fn converge_evicting(ops: &[Op], client: u8, rng: &mut Rng) -> String {
    let mut d = Document::new(cid(client));
    for op in shuffle(ops, rng) {
        d.apply(&op);
    }
    d.evict_partial_transactions();
    fingerprint(&d)
}
