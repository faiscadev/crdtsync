//! Deleted runs — the sequence's in-memory tombstone range representation.
//!
//! A tombstone must survive to anchor later inserts, but a contiguous delete
//! removes items whose ids and placements chain, so the sequence keeps one
//! record per deleted *run* instead of one node per deleted item: live memory
//! tracks the number of runs, not the number of deletions. The logical state is
//! untouched — a run still carries every id, an insert anchored inside one still
//! lands there (the walk expands the run around it), and the encoding is exactly
//! the collapse the state codec already emitted.

use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::{path, Element, List, Op, Scalar, Text, UndoManager};

mod common;
use common::{cid, dead_run_snapshot, default_id, eid, stmp};

/// Sequence lengths the representation assertions need to be convincing. Miri
/// interprets every tree walk, so it runs the same shapes at a smaller size —
/// still far more than the handful of records a correct run collapses to.
const LONG: usize = if cfg!(miri) { 24 } else { 200 };
const MEDIUM: usize = if cfg!(miri) { 16 } else { 64 };
const TEXT_BODY: usize = if cfg!(miri) { 30 } else { 500 };

fn list() -> List {
    List::new(default_id())
}

/// A one-byte scalar item, standing in for a character.
fn ch(c: u8) -> Element {
    Element::Scalar(Scalar::Bytes(vec![c]))
}

/// The live sequence as a string (each item is one byte).
fn text(l: &List) -> String {
    l.values()
        .iter()
        .map(|e| match e {
            Element::Scalar(Scalar::Bytes(b)) if b.len() == 1 => b[0] as char,
            _ => panic!("expected a one-byte scalar item"),
        })
        .collect()
}

/// A list holding `n` letters inserted left to right by one client on
/// consecutive lamports — the shape a typed run has.
fn alphabet(n: usize) -> List {
    let mut l = list();
    for i in 0..n {
        l.insert(i, ch(b'a' + (i % 26) as u8), stmp(i as u64 + 1, 1));
    }
    l
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

/// Whether `id` is present in the sequence but no longer rendered.
fn deleted(l: &List, id: crdtsync_core::Stamp) -> bool {
    l.contains(id) && l.live_index(id).is_none()
}

// --- representation: a run costs one record ---

#[test]
fn a_contiguous_delete_stores_one_record() {
    let mut l = alphabet(LONG);
    assert_eq!(l.stored_records(), LONG);

    for _ in 0..LONG {
        l.delete(0);
    }
    assert_eq!(l.len(), 0);
    assert_eq!(
        l.stored_records(),
        1,
        "{LONG} contiguous deletes must collapse to a single run record"
    );
}

#[test]
fn a_run_collapses_whatever_order_the_deletes_arrive() {
    // Back to front: each delete prepends to the run its successor started.
    let mut back = alphabet(MEDIUM);
    for i in (0..MEDIUM).rev() {
        back.delete(i);
    }
    assert_eq!(back.stored_records(), 1);

    // Every other item first, then the gaps: the second pass welds the singles
    // into one run.
    let half = MEDIUM / 2;
    let mut alternating = alphabet(MEDIUM);
    for i in (0..half).rev() {
        alternating.delete(i * 2);
    }
    assert_eq!(alternating.len(), half);
    for _ in 0..half {
        alternating.delete(0);
    }
    assert_eq!(alternating.stored_records(), 1);

    // Outside in.
    let mut ends = alphabet(MEDIUM);
    while !ends.is_empty() {
        ends.delete(ends.len() - 1);
        if !ends.is_empty() {
            ends.delete(0);
        }
    }
    assert_eq!(ends.stored_records(), 1);
}

#[test]
fn a_delete_extends_the_neighbouring_run() {
    let mut l = alphabet(6);

    l.delete(2); // c
    assert_eq!(l.stored_records(), 6, "5 live items plus one run");

    l.delete(2); // d — chains onto c's run
    assert_eq!(l.stored_records(), 5);

    l.delete(1); // b — prepends to the same run
    assert_eq!(l.stored_records(), 4);

    assert_eq!(text(&l), "aef");
}

#[test]
fn disjoint_deletes_stay_separate_runs() {
    let mut l = alphabet(6);
    l.delete(1); // b
    l.delete(2); // d, with c still live between them
    assert_eq!(text(&l), "acef");
    assert_eq!(l.stored_records(), 6, "4 live items plus two runs");
}

#[test]
fn a_deleted_document_text_keeps_one_record_per_run() {
    let mut d = Document::new(cid(1));
    let body = "z".repeat(TEXT_BODY);
    d.transact(|tx| tx.text(b"t").insert(0, &body));
    d.transact(|tx| tx.text(b"t").delete(0, TEXT_BODY));

    let Some(Element::Text(t)) = d.get(b"t") else {
        panic!("expected a text slot")
    };
    assert_eq!(t.borrow().len(), 0);
    assert_eq!(t.borrow().stored_records(), 1);
}

// --- a run still anchors: inserts inside it split the walk, not the record ---

#[test]
fn an_insert_inside_a_deleted_run_still_lands_there() {
    let mut a = alphabet(6); // abcdef
    let mut b = a.deep_clone();

    // A deletes the middle four.
    for _ in 0..4 {
        a.delete(1);
    }
    assert_eq!(text(&a), "af");
    assert_eq!(a.stored_records(), 3);

    // B, which never saw the deletes, inserts inside what A removed.
    b.insert(3, ch(b'X'), stmp(10, 2));
    assert_eq!(text(&b), "abcXdef");

    let a_deleted = a.deep_clone();
    a.merge(&b);
    b.merge(&a_deleted);

    assert_eq!(text(&a), "aXf");
    assert_eq!(text(&b), "aXf");
    assert_eq!(a.encode_state(), b.encode_state());
    // The insert splits the walk, not the storage: the run is still one record.
    assert_eq!(a.stored_records(), 4, "a, f and X plus the one run");
}

#[test]
fn concurrent_inserts_across_a_run_keep_their_order() {
    let mut a = alphabet(6); // abcdef
    let seed = a.deep_clone();

    for _ in 0..6 {
        a.delete(0);
    }
    assert_eq!(a.stored_records(), 1);

    // Two peers insert at different depths of the region A erased.
    let mut b = seed.deep_clone();
    b.insert(1, ch(b'X'), stmp(20, 2));
    let mut c = seed;
    c.insert(5, ch(b'Y'), stmp(30, 3));

    let a_deleted = a.deep_clone();
    a.merge(&b);
    a.merge(&c);
    assert_eq!(text(&a), "XY");

    // The other arrival order agrees.
    c.merge(&b);
    c.merge(&a_deleted);
    assert_eq!(text(&c), "XY");
    assert_eq!(c.encode_state(), a.encode_state());
}

#[test]
fn inserting_against_a_fully_collapsed_sequence() {
    let mut l = alphabet(8);
    for _ in 0..8 {
        l.delete(0);
    }
    assert_eq!(l.stored_records(), 1);

    l.insert(0, ch(b'X'), stmp(20, 2));
    l.insert(1, ch(b'Y'), stmp(21, 2));
    l.insert(0, ch(b'W'), stmp(22, 3));
    assert_eq!(text(&l), "WXY");
    assert_eq!(l.stored_records(), 4);
}

#[test]
fn a_relative_position_inside_a_run_resolves_to_its_neighbour() {
    let mut l = alphabet(6); // abcdef
    let inside = stmp(3, 1); // c
    let before = RelativePosition::Before(inside);
    let after = RelativePosition::After(inside);
    assert_eq!(l.resolve_position(&before), 2);
    assert_eq!(l.resolve_position(&after), 3);

    for _ in 0..4 {
        l.delete(1); // bcde
    }
    assert_eq!(text(&l), "af");
    // The id is still addressable through the run, resolving to the gap it left.
    assert!(deleted(&l, inside));
    assert_eq!(l.resolve_position(&before), 1);
    assert_eq!(l.resolve_position(&after), 1);
}

// --- merge ---

#[test]
fn a_run_merged_into_an_untouched_replica_deletes_its_items() {
    let mut a = alphabet(6);
    let mut b = a.deep_clone();
    for _ in 0..4 {
        a.delete(1);
    }

    b.merge(&a);
    assert_eq!(text(&b), "af");
    assert_eq!(b.stored_records(), 3);
    assert_eq!(b.encode_state(), a.encode_state());
}

#[test]
fn a_run_merged_into_a_replica_that_never_saw_the_items() {
    // B learns of the items only as a deleted run — the ids must still arrive,
    // so a later insert anchored against them resolves.
    let mut a = alphabet(6);
    for _ in 0..4 {
        a.delete(1);
    }

    let mut b = list();
    b.merge(&a);
    assert_eq!(text(&b), "af");
    assert_eq!(b.stored_records(), 3);
    assert!(deleted(&b, stmp(3, 1)));
    assert_eq!(b.encode_state(), a.encode_state());
}

#[test]
fn overlapping_runs_merge_to_one_record() {
    let mut a = alphabet(9);
    let mut b = a.deep_clone();
    // A deletes b..e, B deletes d..g — the union is one contiguous run.
    for _ in 0..4 {
        a.delete(1);
    }
    for _ in 0..4 {
        b.delete(3);
    }

    let a_only = a.deep_clone();
    a.merge(&b);
    b.merge(&a_only);
    assert_eq!(text(&a), "ahi");
    assert_eq!(text(&b), "ahi");
    assert_eq!(a.stored_records(), 4);
    assert_eq!(a.encode_state(), b.encode_state());
}

#[test]
fn merge_is_idempotent_over_runs() {
    let mut a = alphabet(20);
    for _ in 0..10 {
        a.delete(5);
    }
    let snapshot = a.deep_clone();
    let before = a.encode_state();

    a.merge(&snapshot);
    a.merge(&snapshot);
    assert_eq!(a.encode_state(), before);
    assert_eq!(a.stored_records(), 11);
}

// --- codec ---

#[test]
fn a_collapsed_run_round_trips() {
    let mut l = alphabet(MEDIUM);
    for _ in 0..MEDIUM / 2 {
        l.delete(4);
    }
    let bytes = l.encode_state();
    let back = List::decode_state(&bytes).unwrap();

    assert_eq!(text(&back), text(&l));
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
    assert_eq!(back.stored_records(), l.stored_records());
}

#[test]
fn the_delete_order_does_not_change_the_bytes() {
    let mut forward = alphabet(12);
    for _ in 0..8 {
        forward.delete(2);
    }
    let mut backward = alphabet(12);
    for i in (2..10).rev() {
        backward.delete(i);
    }
    assert_eq!(text(&forward), text(&backward));
    assert_eq!(hex(&forward.encode_state()), hex(&backward.encode_state()));
}

#[test]
fn the_sequence_encoding_is_unchanged_by_the_in_memory_collapse() {
    // Byte fixtures captured before the range representation landed: collapsing
    // tombstones in memory must not move a single byte of the wire/disk format.
    let mut l = List::new(eid(1, 1));
    for (i, c) in b"abcdefgh".iter().enumerate() {
        l.insert(i, ch(*c), stmp(i as u64 + 1, 1));
    }
    l.delete(1); // b
    l.delete(1); // c
    l.delete(1); // d
    l.delete(2); // f
    l.delete(2); // g
    assert_eq!(text(&l), "aeh");
    assert_eq!(hex(&l.encode_state()), "000000000000000100000000000000010300000001000000000000000100000000000000000000000000000000000301000000610001050000000000000001000000000000000000000000000000000003010000006501040000000000000001000000000000000000000000000000000108000000000000000100000000000000000000000000000000000301000000680107000000000000000100000000000000000000000000000000010200000002000000000000000100000000000000000000000000000000030000000101000000000000000100000000000000000000000000000000010600000000000000010000000000000000000000000000000002000000010500000000000000010000000000000000000000000000000001");

    let mut t = Text::new(eid(2, 2));
    t.insert(0, "hello world", stmp(1, 1));
    t.delete(2, 5);
    assert_eq!(t.as_string(), "heorld");
    assert_eq!(hex(&t.encode_state()), "00000000000000020000000000000002060000000100000000000000010000000000000000000000000000000000026800000000000000000102000000000000000100000000000000000000000000000000000265000000000000000101000000000000000100000000000000000000000000000000010800000000000000010000000000000000000000000000000000026f0000000000000001070000000000000001000000000000000000000000000000000109000000000000000100000000000000000000000000000000000272000000000000000108000000000000000100000000000000000000000000000000010a00000000000000010000000000000000000000000000000000026c000000000000000109000000000000000100000000000000000000000000000000010b00000000000000010000000000000000000000000000000000026400000000000000010a00000000000000010000000000000000000000000000000001010000000300000000000000010000000000000000000000000000000005000000010200000000000000010000000000000000000000000000000001");
}

#[test]
fn a_run_longer_than_one_record_welds_back_on_decode() {
    // The encoder chains records past the per-record cap; decode must weld them
    // into the single run they describe, or a snapshot-restored replica would
    // hold a different record shape — and encode a different snapshot — from a
    // peer that saw the deletes as ops. The total id count is deliberately
    // unbounded: a run costs one record, so a long-lived document reaches counts
    // no per-id budget could carry, and it must still load its own snapshot.
    const CAP: u64 = 1 << 20;
    let chunks: Vec<(crdtsync_core::Stamp, u32, Option<crdtsync_core::Stamp>)> = (0..5)
        .map(|i| {
            let parent = (i > 0).then(|| stmp(i * CAP, 1));
            (stmp(1 + i * CAP, 1), CAP as u32, parent)
        })
        .collect();
    let bytes = dead_run_snapshot(eid(4, 4), &chunks);

    let back = List::decode_state(&bytes).unwrap();
    assert!(back.is_empty());
    assert_eq!(
        back.stored_records(),
        1,
        "the chained records describe one run"
    );
    assert!(deleted(&back, stmp(1 + 3 * CAP, 1)));
    assert_eq!(
        back.encode_state(),
        bytes,
        "re-encode splits at the same points"
    );
}

#[test]
fn an_insert_hanging_right_off_a_run_interior_renders_in_place() {
    // The other half of the split rule: a record anchored to the *right* of an id
    // inside a run shares that id's bucket with the run's own continuation, so
    // the walk must cut there too. Only partial delivery reaches it — the peer
    // must anchor against an id before the ids after it exist.
    let mut early = alphabet(2); // ab, and nothing after it yet
    early.insert(2, ch(b'X'), stmp(2, 2)); // hangs right off b
    assert_eq!(text(&early), "abX");

    let mut whole = alphabet(4); // abcd
    whole.delete(1); // b
    whole.delete(1); // c — one run, with X's anchor at its head
    assert_eq!(text(&whole), "ad");

    let deleted_only = whole.deep_clone();
    whole.merge(&early);
    early.merge(&deleted_only);

    // X's stamp sorts below c's, so it renders between them — inside the run.
    assert_eq!(text(&whole), "aXd");
    assert_eq!(text(&early), "aXd");
    assert_eq!(whole.stored_records(), 4, "a, d and X plus the one run");
    assert_eq!(whole.encode_state(), early.encode_state());

    // And an insert after X lands after it, not before the run's tail.
    whole.insert(2, ch(b'Y'), stmp(30, 3));
    assert_eq!(text(&whole), "aXYd");
}

#[test]
fn a_snapshot_of_a_wholly_deleted_text_round_trips() {
    let mut t = Text::new(eid(3, 3));
    t.insert(0, &"q".repeat(TEXT_BODY), stmp(5, 1));
    t.delete(0, TEXT_BODY);
    assert_eq!(t.stored_records(), 1);

    let bytes = t.encode_state();
    let back = Text::decode_state(&bytes).unwrap();
    assert_eq!(back.as_string(), "");
    assert_eq!(back.stored_records(), 1);
    assert_eq!(back.encode_state(), bytes);
}

// --- randomized: delete-heavy replicas converge to the same bytes ---

/// A small linear-congruential PRNG — deterministic, seedable, reproducible.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 17) as usize) % n
    }
}

/// A burst of delete-heavy edits by one replica: mostly contiguous deletions,
/// with a few inserts that land inside what a peer is erasing.
fn burst(l: &mut List, client: u8, rng: &mut Rng) {
    let mut lamport = 1_000 * client as u64;
    for _ in 0..12 {
        if l.is_empty() || rng.below(4) == 0 {
            let idx = rng.below(l.len() + 1);
            lamport += 1;
            l.insert(idx, ch(b'A' + client), stmp(lamport, client));
        } else {
            let idx = rng.below(l.len());
            let count = 1 + rng.below(l.len() - idx);
            for _ in 0..count {
                l.delete(idx);
            }
        }
    }
}

#[test]
fn delete_heavy_replicas_converge_to_identical_bytes() {
    let seeds = if cfg!(miri) { 4 } else { 300 };
    for seed in 0..seeds {
        let mut rng = Rng::new(seed);
        let base = alphabet(1 + rng.below(24));

        let mut replicas: Vec<List> = (0..3).map(|_| base.deep_clone()).collect();
        for (i, r) in replicas.iter_mut().enumerate() {
            burst(r, 1 + i as u8, &mut rng);
        }

        // Every merge order must land on the same state, byte for byte.
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let mut reference: Option<(String, Vec<u8>)> = None;
        for order in orders {
            let mut merged = base.deep_clone();
            for i in order {
                merged.merge(&replicas[i]);
            }
            // Re-delivery must change nothing.
            merged.merge(&replicas[order[0]]);
            let got = (text(&merged), merged.encode_state());
            match &reference {
                None => reference = Some(got),
                Some(want) => assert_eq!(&got, want, "seed {seed}: merge order {order:?} diverged"),
            }
        }

        // A snapshot round-trip preserves the state exactly.
        let (want_text, want_bytes) = reference.unwrap();
        let back = List::decode_state(&want_bytes).unwrap();
        assert_eq!(
            text(&back),
            want_text,
            "seed {seed}: round-trip lost content"
        );
        assert_eq!(
            back.encode_state(),
            want_bytes,
            "seed {seed}: re-encode drifted"
        );
    }
}

// --- interaction with the document layer: undo and tree move ---

#[test]
fn undo_revives_a_run_that_collapsed() {
    let mut d = Document::new(cid(1));
    let u = UndoManager::new();
    u.track(&mut d);
    let t = path::encode_path(&[b"t"]);
    path::text_insert(&mut d, &t, 0, "hello world");
    path::text_delete(&mut d, &t, 2, 5);

    assert_eq!(text_of(&d, b"t"), "heorld");
    assert_eq!(text_records(&d, b"t"), 7, "6 live codepoints plus one run");

    u.undo(&mut d);
    assert_eq!(text_of(&d, b"t"), "hello world");

    u.redo(&mut d);
    assert_eq!(text_of(&d, b"t"), "heorld");
}

fn text_of(d: &Document, key: &[u8]) -> String {
    match d.get(key) {
        Some(Element::Text(t)) => t.borrow().as_string(),
        _ => panic!("expected a text slot"),
    }
}

fn text_records(d: &Document, key: &[u8]) -> usize {
    match d.get(key) {
        Some(Element::Text(t)) => t.borrow().stored_records(),
        _ => panic!("expected a text slot"),
    }
}

/// A parenthesised rendering of the fragment under slot `key`.
fn tree(d: &Document, key: &[u8]) -> String {
    match d.get(key) {
        Some(Element::XmlFragment(f)) => {
            let kids: Vec<String> = f
                .borrow()
                .children()
                .borrow()
                .values()
                .iter()
                .map(render)
                .collect();
            format!("frag({})", kids.join(","))
        }
        _ => "∅".to_string(),
    }
}

fn render(e: &Element) -> String {
    match e {
        Element::XmlElement(x) => {
            let x = x.borrow();
            let kids: Vec<String> = x.children().borrow().values().iter().map(render).collect();
            format!("{}({})", String::from_utf8_lossy(x.tag()), kids.join(","))
        }
        Element::Text(t) => format!("{:?}", t.borrow().as_string()),
        _ => "?".to_string(),
    }
}

/// The encoded children sequence of the fragment at `key` — the representation
/// two converged replicas must agree on byte for byte.
fn frag_bytes(d: &Document, key: &[u8]) -> Vec<u8> {
    match d.get(key) {
        Some(Element::XmlFragment(f)) => f.borrow().children().borrow().encode_state(),
        _ => panic!("expected a fragment"),
    }
}

/// The record count of the children sequence of the fragment at `key`.
fn frag_records(d: &Document, key: &[u8]) -> usize {
    match d.get(key) {
        Some(Element::XmlFragment(f)) => f.borrow().children().borrow().stored_records(),
        _ => panic!("expected a fragment"),
    }
}

/// Build `frag(p(),m(),q(),host())`, returning the ops plus the ids of `m` and
/// `host`.
fn frag_p_m_q_host(d: &mut Document) -> (Vec<Op>, ElementId, ElementId) {
    let mut moved = ElementId::from_bytes([0u8; 16]);
    let mut host = ElementId::from_bytes([0u8; 16]);
    let ops = d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.insert_element(0, b"p");
        moved = kids.insert_element(1, b"m").id();
        kids.insert_element(2, b"q");
        host = kids.insert_element(3, b"host").id();
    });
    (ops, moved, host)
}

#[test]
fn a_moved_away_node_is_not_folded_into_a_run() {
    // `moved_away` suppresses a placement reversibly, so a node a tree move took
    // elsewhere must stay a node: folding it into a neighbouring run would bury
    // it as a tombstone and the move back would have nothing to reveal.
    let mut d = Document::new(cid(1));
    let (_ops, moved, host) = frag_p_m_q_host(&mut d);
    assert_eq!(tree(&d, b"doc"), "frag(p(),m(),q(),host())");

    d.transact(|tx| tx.move_xml(moved, host, 0));
    assert_eq!(tree(&d, b"doc"), "frag(p(),q(),host(m()))");

    // Delete the two children bracketing the suppressed placement.
    d.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.delete(1); // q
        kids.delete(0); // p
    });
    assert_eq!(tree(&d, b"doc"), "frag(host(m()))");
    assert_eq!(
        frag_records(&d, b"doc"),
        4,
        "the suppressed node still parts p and q into two runs"
    );

    // Its birth placement survived the collapse, so moving it back reveals it.
    let frag = ElementId::derive(d.root_id(), b"doc", ElementKind::XmlFragment);
    d.transact(|tx| tx.move_xml(moved, frag, 0));
    assert_eq!(tree(&d, b"doc"), "frag(m(),host())");
}

#[test]
fn a_delete_wins_over_a_concurrent_move_across_a_run() {
    let mut src = Document::new(cid(1));
    let (build, moved, host) = frag_p_m_q_host(&mut src);

    let mut peer = Document::new(cid(2));
    apply_all(&mut peer, &build);

    // Concurrently: src deletes p, m and q as one contiguous run; peer moves m.
    let del = src.transact(|tx| {
        let mut frag = tx.xml_fragment(b"doc");
        let mut kids = frag.children();
        kids.delete(0);
        kids.delete(0);
        kids.delete(0);
    });
    let mv = peer.transact(|tx| tx.move_xml(moved, host, 0));

    apply_all(&mut src, &mv);
    apply_all(&mut peer, &del);
    assert_eq!(tree(&src, b"doc"), "frag(host())");
    assert_eq!(tree(&peer, b"doc"), "frag(host())");
    assert_eq!(frag_bytes(&src, b"doc"), frag_bytes(&peer, b"doc"));
}
