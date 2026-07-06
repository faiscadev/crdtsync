//! Marks — the read-time mark model over the RangedElement annotation set
//! (XmlElement Unit 4b).
//!
//! A mark is a `RangedElement` carrying a `name`; there is no per-character
//! storage. `Document::marks_at(seq, index)` computes a character's active marks
//! by gathering every live same-named range covering it and combining per the
//! schema flavor: boolean → LWW presence, value → LWW value, object → the set of
//! covering instances. Anchor gravity (Before/After) is the mark's expansion, so a
//! boundary insert falls inside or outside per the anchors chosen at author time.

use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::list::Side;
use crdtsync_core::marks::MarkState;
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::{ClientId, Element, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

// A schema declaring the three mark flavors over a text body.
const SCHEMA: &str = r#"{
    "schema": "doc", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map", "children": { "body": "Body" } }, "Body": { "kind": "text" } },
    "marks": {
        "bold":    { "flavor": "boolean" },
        "link":    { "flavor": "value" },
        "comment": { "flavor": "object" }
    }
}"#;

fn text_id(d: &Document) -> ElementId {
    ElementId::derive(d.root_id(), b"body", ElementKind::Text)
}

/// A fresh replica with the schema bound and a "body" Text holding `s`.
fn doc_with_body(client: u8, s: &str) -> (Document, Vec<Op>) {
    let mut d = Document::new(cid(client));
    d.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
    let ops = d.transact(|tx| {
        let mut t = tx.text(b"body");
        t.insert(0, s);
    });
    (d, ops)
}

/// The live Text handle of the body.
fn body(d: &Document) -> std::rc::Rc<std::cell::RefCell<crdtsync_core::text::Text>> {
    match d.get(b"body") {
        Some(Element::Text(t)) => t,
        _ => panic!("no body text"),
    }
}

/// A range anchor at codepoint `idx` with the given gravity over the body.
fn anchor(d: &Document, idx: usize, side: Side) -> RangeAnchor {
    let pos = body(d).borrow().relative_position(idx, side);
    RangeAnchor {
        seq: text_id(d),
        pos,
    }
}

/// A non-growing span `[i, j)`: start pinned right (Before the first char), end
/// pinned left (After the last), so a boundary insert stays outside.
fn span(d: &Document, i: usize, j: usize) -> (RangeAnchor, RangeAnchor) {
    (anchor(d, i, Side::Right), anchor(d, j, Side::Left))
}

/// A both-growing span `[i, j)`: start pinned left, end pinned right, so a
/// boundary insert on either side falls inside.
fn grow_span(d: &Document, i: usize, j: usize) -> (RangeAnchor, RangeAnchor) {
    (anchor(d, i, Side::Left), anchor(d, j, Side::Right))
}

/// Whether `name` reads as a present boolean mark on character `index`.
fn is_bold(d: &Document, index: usize, name: &[u8]) -> bool {
    d.marks_at(text_id(d), index)
        .into_iter()
        .any(|m| m.name == name && m.state == MarkState::Boolean(true))
}

fn value_of(d: &Document, index: usize, name: &[u8]) -> Option<Scalar> {
    d.marks_at(text_id(d), index).into_iter().find_map(|m| {
        if m.name == name {
            match m.state {
                MarkState::Value(v) => Some(v),
                _ => None,
            }
        } else {
            None
        }
    })
}

#[test]
fn a_boolean_mark_covers_exactly_its_span() {
    // "hello world" — bold [0,5) covers "hello", not the space or "world".
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s, e) = span(&d, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    for i in 0..5 {
        assert!(is_bold(&d, i, b"bold"), "char {i} bold");
    }
    for i in 5..11 {
        assert!(!is_bold(&d, i, b"bold"), "char {i} not bold");
    }
}

#[test]
fn a_value_mark_carries_the_winning_value() {
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s, e) = span(&d, 0, 5);
    d.transact(|tx| {
        tx.ranged()
            .mark(b"link", s, e, Scalar::Bytes(b"http://a".to_vec()));
    });
    assert_eq!(
        value_of(&d, 2, b"link"),
        Some(Scalar::Bytes(b"http://a".to_vec()))
    );
    assert_eq!(value_of(&d, 7, b"link"), None, "outside the span");
}

#[test]
fn concurrent_boolean_add_and_remove_is_lww() {
    // One replica bolds a span, another concurrently un-bolds the same span
    // (present=false). The higher-stamped op wins on every replica.
    let (base, build) = doc_with_body(1, "hello world");
    let (s, e) = span(&base, 0, 5);

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    for r in [&mut r1, &mut r2] {
        r.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
        apply_all(r, &build);
    }
    let add = r1.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    let remove = r2.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(false));
    });
    apply_all(&mut r1, &remove);
    apply_all(&mut r2, &add);

    // r2 (cid 3) authored the remove at the higher client-id tiebreak, so
    // "not bold" wins on both.
    assert_eq!(
        is_bold(&r1, 2, b"bold"),
        is_bold(&r2, 2, b"bold"),
        "converge"
    );
    assert!(!is_bold(&r1, 2, b"bold"), "the higher-stamped remove wins");
}

#[test]
fn a_value_mark_change_is_lww_across_ranges() {
    let (base, build) = doc_with_body(1, "hello world");
    let (s1, e1) = span(&base, 0, 5);
    let (s2, e2) = span(&base, 0, 5);

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    for r in [&mut r1, &mut r2] {
        r.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
        apply_all(r, &build);
    }
    let a = r1.transact(|tx| {
        tx.ranged()
            .mark(b"link", s1, e1, Scalar::Bytes(b"a".to_vec()));
    });
    let b = r2.transact(|tx| {
        tx.ranged()
            .mark(b"link", s2, e2, Scalar::Bytes(b"b".to_vec()));
    });
    apply_all(&mut r1, &b);
    apply_all(&mut r2, &a);
    assert_eq!(
        value_of(&r1, 2, b"link"),
        value_of(&r2, 2, b"link"),
        "converge"
    );
    assert_eq!(
        value_of(&r1, 2, b"link"),
        Some(Scalar::Bytes(b"b".to_vec())),
        "higher client wins"
    );
}

#[test]
fn overlapping_object_marks_coexist() {
    // Two comments over overlapping spans — [0,6) and [3,9). The overlap (chars
    // 3..6) carries both instances; each end carries only one.
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s1, e1) = span(&d, 0, 6);
    let (s2, e2) = span(&d, 3, 9);
    let mut id1 = ElementId::from_bytes([0u8; 16]);
    let mut id2 = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        id1 = tx.ranged().mark(b"comment", s1, e1, Scalar::Int(1));
        id2 = tx.ranged().mark(b"comment", s2, e2, Scalar::Int(2));
    });

    let overlap = comment_ids(&d, 4);
    assert_eq!(overlap.len(), 2, "both comments on the overlap");
    assert!(overlap.contains(&id1) && overlap.contains(&id2));
    assert_eq!(
        comment_ids(&d, 1),
        vec![id1],
        "only the first before the overlap"
    );
    assert_eq!(comment_ids(&d, 7), vec![id2], "only the second after");
}

fn comment_ids(d: &Document, index: usize) -> Vec<ElementId> {
    d.marks_at(text_id(d), index)
        .into_iter()
        .find_map(|m| {
            if m.name == b"comment" {
                match m.state {
                    MarkState::Object(ids) => Some(ids),
                    _ => None,
                }
            } else {
                None
            }
        })
        .unwrap_or_default()
}

#[test]
fn a_grow_both_mark_expands_over_a_boundary_insert() {
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s, e) = grow_span(&d, 0, 5); // bold "hello", growing both edges
    d.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    // Insert "XX" at index 5 (the end boundary) — a growing mark swallows it.
    d.transact(|tx| tx.text(b"body").insert(5, "XX"));
    assert!(is_bold(&d, 5, b"bold"), "grew over the end insert");
    assert!(is_bold(&d, 6, b"bold"));
}

#[test]
fn a_no_grow_mark_does_not_expand_over_a_boundary_insert() {
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s, e) = span(&d, 0, 5); // non-growing
    d.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    d.transact(|tx| tx.text(b"body").insert(5, "XX"));
    assert!(!is_bold(&d, 5, b"bold"), "did not grow over the end insert");
    assert!(
        is_bold(&d, 4, b"bold"),
        "still covers its original last char"
    );
}

#[test]
fn an_insert_before_the_span_shifts_it() {
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s, e) = span(&d, 6, 11); // bold "world"
    d.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    // Insert 3 chars at the front — the anchored span tracks the same characters.
    d.transact(|tx| tx.text(b"body").insert(0, "abc"));
    assert!(!is_bold(&d, 6, b"bold"), "old start no longer bold");
    for i in 9..14 {
        assert!(is_bold(&d, i, b"bold"), "char {i} (shifted 'world') bold");
    }
}

#[test]
fn marks_survive_a_snapshot() {
    let (mut d, _) = doc_with_body(1, "hello world");
    let (s, e) = span(&d, 0, 5);
    let (ls, le) = span(&d, 6, 11);
    d.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
        tx.ranged()
            .mark(b"link", ls, le, Scalar::Bytes(b"u".to_vec()));
    });
    let bytes = d.encode_state();
    let mut restored = Document::decode_state(&bytes).unwrap();
    restored.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());

    assert!(is_bold(&restored, 2, b"bold"));
    assert!(!is_bold(&restored, 8, b"bold"));
    assert_eq!(
        value_of(&restored, 8, b"link"),
        Some(Scalar::Bytes(b"u".to_vec()))
    );
    assert_eq!(restored.encode_state(), bytes, "re-encode diverged");
}

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
}

/// The per-character mark reading across the whole body — the convergence oracle.
fn fingerprint(d: &Document) -> String {
    let seq = text_id(d);
    let len = body(d).borrow().as_string().chars().count();
    (0..len)
        .map(|i| format!("{i}:{:?}", d.marks_at(seq, i)))
        .collect::<Vec<_>>()
        .join(";")
}

#[test]
fn random_orderings_converge() {
    // Marks authored across three replicas (bold add/remove, a link, two comments)
    // delivered shuffled to a fourth — every ordering lands on the same per-char
    // mark reading.
    let (author, build) = doc_with_body(1, "hello world");
    let (bs, be) = span(&author, 0, 5);
    let (bs2, be2) = span(&author, 2, 8);
    let (ls, le) = span(&author, 6, 11);
    let (cs1, ce1) = span(&author, 0, 6);
    let (cs2, ce2) = span(&author, 3, 9);

    let mut r = [
        Document::new(cid(2)),
        Document::new(cid(3)),
        Document::new(cid(4)),
    ];
    for d in r.iter_mut() {
        d.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
        apply_all(d, &build);
    }

    let mut ops: Vec<Op> = Vec::new();
    ops.extend(r[0].transact(|tx| {
        tx.ranged().mark(b"bold", bs, be, Scalar::Bool(true));
        tx.ranged().mark(b"comment", cs1, ce1, Scalar::Int(1));
    }));
    ops.extend(r[1].transact(|tx| {
        tx.ranged().mark(b"bold", bs2, be2, Scalar::Bool(false));
        tx.ranged()
            .mark(b"link", ls, le, Scalar::Bytes(b"x".to_vec()));
    }));
    ops.extend(r[2].transact(|tx| {
        tx.ranged().mark(b"comment", cs2, ce2, Scalar::Int(2));
    }));

    let mut reference = Document::new(cid(9));
    reference.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
    apply_all(&mut reference, &build);
    apply_all(&mut reference, &ops);
    let expect = fingerprint(&reference);

    for seed in 0..64u64 {
        let mut shuffled = ops.clone();
        let mut rng = Rng::new(seed);
        for i in (1..shuffled.len()).rev() {
            let j = (rng.next() as usize) % (i + 1);
            shuffled.swap(i, j);
        }
        let mut d = Document::new(cid(10));
        d.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
        apply_all(&mut d, &build);
        apply_all(&mut d, &shuffled);
        assert_eq!(fingerprint(&d), expect, "seed {seed} diverged");
    }
}

#[test]
fn a_mark_whose_anchor_chars_have_not_arrived_covers_nothing() {
    // A RangedCreate is accept-and-store — a mark can apply before the inserts
    // that carry its span's codepoints. Until those codepoints arrive its anchors
    // cannot resolve, so it covers nothing; it must not collapse onto a boundary
    // and paint the whole current text.
    let mut author = Document::new(cid(1));
    author.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
    let ins_hello = author.transact(|tx| {
        tx.text(b"body").insert(0, "hello");
    });
    let ins_world = author.transact(|tx| {
        tx.text(b"body").insert(5, " world");
    });
    // Bold "world" — anchors bind to codepoints inserted by ins_world.
    let (s, e) = span(&author, 6, 11);
    let mark = author.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });

    // A replica with only "hello" plus the mark: its anchor codepoints are absent.
    let mut r = Document::new(cid(2));
    r.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
    apply_all(&mut r, &ins_hello);
    apply_all(&mut r, &mark);
    for i in 0..5 {
        assert!(
            !is_bold(&r, i, b"bold"),
            "char {i} not bold — the mark's span has not arrived"
        );
    }

    // Once " world" arrives the mark resolves and covers exactly "world".
    apply_all(&mut r, &ins_world);
    for i in 0..6 {
        assert!(!is_bold(&r, i, b"bold"), "char {i} still not bold");
    }
    for i in 6..11 {
        assert!(is_bold(&r, i, b"bold"), "char {i} now bold");
    }
}

#[test]
fn a_removed_boolean_mark_is_absent_from_the_active_set() {
    // marks_at yields only the marks on a character: a boolean whose winning
    // range is a remove is omitted entirely, not surfaced as Boolean(false), so a
    // consumer keying on the name's presence never renders removed formatting.
    let (base, build) = doc_with_body(1, "hello world");
    let (s, e) = span(&base, 0, 5);
    let (s2, e2) = span(&base, 0, 5);

    let mut r = Document::new(cid(2));
    r.set_schema(crdtsync_core::schema::Schema::parse(SCHEMA).unwrap());
    apply_all(&mut r, &build);

    let add = r.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    apply_all(&mut r, &add);
    assert!(is_bold(&r, 2, b"bold"), "bold on before the remove");

    // A later remove out-stamps the add on the same replica.
    let remove = r.transact(|tx| {
        tx.ranged().mark(b"bold", s2, e2, Scalar::Bool(false));
    });
    apply_all(&mut r, &remove);
    assert!(
        r.marks_at(text_id(&r), 2).iter().all(|m| m.name != b"bold"),
        "a removed boolean mark is absent from the active set, not present-as-false"
    );
}
