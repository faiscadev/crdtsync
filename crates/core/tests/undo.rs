//! Per-user undo / redo over the core record-seam.
//!
//! A replica records the inverse of every op it *emits* while an origin is set —
//! whatever surface authored it — so an SDK editing through its own handle graph
//! is undoable without routing through the undo module. The recorded inverse is
//! an ordinary forward op: undo converges on peers exactly like any other edit,
//! with no wire change and no server-side undo state. A remote op is folded in by
//! `apply`, never emitted, so a collaborator's edit can never land on this
//! replica's stack.
//!
//! Selection is by origin tag: a document may carry several interleaved
//! histories (a user's, a subtree-scoped manager's), and undo takes the newest
//! intention of the origin asked for, skipping the rest. An intention is one
//! transact, one explicit group, or one atomic transaction — and an atomic
//! intention undoes and redoes as one atomic transaction in turn.

use crdtsync_core::acl::{AclEffect, AclGrant, AclSubject, Capability};
use crdtsync_core::doc::{Document, SlotFate};
use crdtsync_core::elementid::ElementId;
use crdtsync_core::marks::MarkState;
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::schema::Schema;
use crdtsync_core::RelativePosition;
use crdtsync_core::{
    path, Channel, ClientId, ClientSession, Element, Message, Op, Scalar, Side, UndoManager,
};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A replica already recording under the default origin.
fn doc(first: u8) -> Document {
    let mut d = Document::new(cid(first));
    d.set_undo_origin(ORIGIN);
    d
}

const ORIGIN: &[u8] = b"local";
const OTHER: &[u8] = b"other";

fn p(keys: &[&[u8]]) -> Vec<u8> {
    path::encode_path(keys)
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

fn reg(d: &Document, path: &[u8]) -> Option<Scalar> {
    path::get_register(d, path)
}

fn counter(d: &Document, path: &[u8]) -> i64 {
    path::get_counter(d, path).unwrap_or(0)
}

fn list_vals(d: &Document, path: &[u8]) -> Vec<Vec<u8>> {
    let n = path::list_len(d, path).unwrap_or(0);
    (0..n).filter_map(|i| path::list_get(d, path, i)).collect()
}

fn text(d: &Document, path: &[u8]) -> String {
    path::text_get(d, path).unwrap_or_default()
}

/// The marks on the first character, by name and resolved value. An
/// object-flavored mark resolves to the *ids* of its covering ranges, and a
/// revival always mints a fresh id — so the count stands in for them, or every
/// comparison across a revival would fail on identity alone.
fn marks(d: &Document, path: &[u8]) -> Vec<String> {
    path::marks_at(d, path, 0)
        .into_iter()
        .map(|m| {
            let state = match m.state {
                MarkState::Object(ids) => format!("object({})", ids.len()),
                other => format!("{other:?}"),
            };
            format!("{:?}={state}", m.name)
        })
        .collect()
}

fn undo(d: &mut Document) -> Vec<Op> {
    d.undo(ORIGIN).expect("an intention to undo")
}

fn redo(d: &mut Document) -> Vec<Op> {
    d.redo(ORIGIN).expect("an intention to redo")
}

/// Everything a fixture document holds, read back through the public surface —
/// the fingerprint two replicas must agree on once they have seen the same ops.
/// XML is rendered down to each child's tag or string, so an element that came
/// back as an empty shell is distinguishable from the one that was deleted.
fn observe(d: &Document) -> Vec<String> {
    let mut out = Vec::new();
    for key in path::map_keys(d, &p(&[])).unwrap_or_default() {
        let path = p(&[&key]);
        out.push(format!(
            "{key:?}=reg:{:?} ctr:{:?} list:{:?} text:{:?} xml:{:?}{:?} marks:{:?}",
            reg(d, &path),
            path::get_counter(d, &path),
            path::list_len(d, &path).map(|_| list_vals(d, &path)),
            path::text_get(d, &path),
            path::xml_tag(d, &path),
            child_shapes(d, &key),
            marks(d, &path),
        ));
    }
    out.sort();
    out
}

// --- registers, scalars, blob refs ---

#[test]
fn undo_restores_a_registers_prior_value() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"title"]), Scalar::Int(1));
    path::register(&mut d, &p(&[b"title"]), Scalar::Int(2));
    assert_eq!(reg(&d, &p(&[b"title"])), Some(Scalar::Int(2)));

    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"title"])), Some(Scalar::Int(1)));
}

#[test]
fn undo_of_the_first_set_empties_the_slot() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"title"]), Scalar::Int(1));
    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"title"])), None);
}

#[test]
fn undo_restores_a_nested_registers_prior_value() {
    let mut d = doc(1);
    let path = p(&[b"outer", b"inner", b"n"]);
    path::register(&mut d, &path, Scalar::Int(7));
    path::register(&mut d, &path, Scalar::Int(8));

    undo(&mut d);
    assert_eq!(reg(&d, &path), Some(Scalar::Int(7)));
    undo(&mut d);
    assert_eq!(reg(&d, &path), None, "the slot's first value is undone too");
}

#[test]
fn undo_restores_a_deleted_registers_value() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"k"]), Scalar::Int(5));
    path::delete(&mut d, &p(&[b"k"]));
    assert_eq!(reg(&d, &p(&[b"k"])), None);

    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"k"])), Some(Scalar::Int(5)));
}

#[test]
fn undo_restores_a_prior_blob_ref() {
    let mut d = doc(1);
    let path = p(&[b"avatar"]);
    path::set_blob_ref(&mut d, &path, [1u8; 16], "image/png", 10);
    path::set_blob_ref(&mut d, &path, [2u8; 16], "image/png", 20);
    assert_eq!(path::get_blob(&d, &path).map(|b| b.id), Some([2u8; 16]));

    undo(&mut d);
    assert_eq!(
        path::get_blob(&d, &path).map(|b| b.id),
        Some([1u8; 16]),
        "a blob ref is a leaf value and undoes like one"
    );
}

#[test]
fn undo_restores_a_prior_element_ref() {
    let mut d = doc(1);
    let path = p(&[b"mention"]);
    let a = ElementId::from_bytes([1u8; 16]);
    let b = ElementId::from_bytes([2u8; 16]);
    path::register(&mut d, &path, Scalar::ElementRef(a));
    path::register(&mut d, &path, Scalar::ElementRef(b));

    undo(&mut d);
    assert_eq!(reg(&d, &path), Some(Scalar::ElementRef(a)));
}

#[test]
fn undo_restores_a_map_scalar_slot() {
    let mut d = doc(1);
    d.transact(|c| c.set(b"k", Scalar::Int(1)));
    d.transact(|c| c.set(b"k", Scalar::Int(2)));
    assert!(matches!(d.get(b"k"), Some(Element::Scalar(Scalar::Int(2)))));

    undo(&mut d);
    assert!(
        matches!(d.get(b"k"), Some(Element::Scalar(Scalar::Int(1)))),
        "a bare map value restores as a bare map value"
    );
}

// --- counters ---

#[test]
fn undo_cancels_a_counter_increment() {
    let mut d = doc(1);
    let path = p(&[b"votes"]);
    path::inc(&mut d, &path, 3);
    path::inc(&mut d, &path, 4);
    assert_eq!(counter(&d, &path), 7);

    undo(&mut d);
    assert_eq!(counter(&d, &path), 3);
    redo(&mut d);
    assert_eq!(counter(&d, &path), 7);
}

#[test]
fn undo_cancels_a_nested_counter_decrement() {
    let mut d = doc(1);
    let path = p(&[b"scores", b"a"]);
    path::inc(&mut d, &path, 10);
    path::dec(&mut d, &path, 4);
    assert_eq!(counter(&d, &path), 6);

    undo(&mut d);
    assert_eq!(counter(&d, &path), 10);
}

// --- containers in map slots ---

#[test]
fn undo_of_a_container_create_empties_the_slot() {
    let mut d = doc(1);
    path::list_insert(&mut d, &p(&[b"items"]), 0, b"x");
    assert_eq!(list_vals(&d, &p(&[b"items"])), vec![b"x".to_vec()]);

    // The insert's transact created the list and inserted into it as one step.
    undo(&mut d);
    assert_eq!(path::list_len(&d, &p(&[b"items"])), None);
}

#[test]
fn undo_of_an_overwrite_restores_the_displaced_container_with_its_contents() {
    let mut d = doc(1);
    let path = p(&[b"body"]);
    path::text_insert(&mut d, &path, 0, "hello");
    path::register(&mut d, &path, Scalar::Int(1));
    assert_eq!(
        path::text_get(&d, &path),
        None,
        "the register took the slot"
    );

    undo(&mut d);
    assert_eq!(
        text(&d, &path),
        "hello",
        "the displaced text is the same logical element, content intact"
    );
}

#[test]
fn undo_of_a_container_delete_restores_its_contents() {
    let mut d = doc(1);
    let path = p(&[b"body"]);
    path::text_insert(&mut d, &path, 0, "hello");
    path::delete(&mut d, &path);
    assert_eq!(path::text_get(&d, &path), None);

    undo(&mut d);
    assert_eq!(text(&d, &path), "hello");
}

#[test]
fn undo_of_an_xml_element_create_empties_the_slot() {
    let mut d = doc(1);
    let path = p(&[b"root"]);
    path::xml_element(&mut d, &path, b"section");
    assert_eq!(path::xml_tag(&d, &path), Some(b"section".to_vec()));

    undo(&mut d);
    assert_eq!(path::xml_tag(&d, &path), None);
}

// --- lists ---

#[test]
fn undo_of_a_list_insert_removes_the_item() {
    let mut d = doc(1);
    let path = p(&[b"items"]);
    path::list_insert(&mut d, &path, 0, b"a");
    path::list_insert(&mut d, &path, 1, b"b");

    undo(&mut d);
    assert_eq!(list_vals(&d, &path), vec![b"a".to_vec()]);
    redo(&mut d);
    assert_eq!(list_vals(&d, &path), vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn undo_of_a_list_delete_revives_the_value_in_place() {
    let mut d = doc(1);
    let path = p(&[b"items"]);
    path::list_insert(&mut d, &path, 0, b"a");
    path::list_insert(&mut d, &path, 1, b"b");
    path::list_insert(&mut d, &path, 2, b"c");
    path::list_delete(&mut d, &path, 1);
    assert_eq!(list_vals(&d, &path), vec![b"a".to_vec(), b"c".to_vec()]);

    undo(&mut d);
    assert_eq!(
        list_vals(&d, &path),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        "the revived item returns between its old neighbours"
    );
}

#[test]
fn a_revived_list_item_lands_at_its_old_position_after_a_later_insert() {
    let mut d = doc(1);
    let path = p(&[b"items"]);
    path::list_insert(&mut d, &path, 0, b"a");
    path::list_insert(&mut d, &path, 1, b"b");
    path::list_delete(&mut d, &path, 0);
    // A later insert at the front shifts indices; the revival is anchored on the
    // tombstone, not on an index captured at delete time.
    path::list_insert(&mut d, &path, 0, b"z");
    assert_eq!(list_vals(&d, &path), vec![b"z".to_vec(), b"b".to_vec()]);

    assert!(d.undo(ORIGIN).is_some(), "the later insert undoes");
    assert!(d.undo(ORIGIN).is_some(), "then the delete");
    assert_eq!(
        list_vals(&d, &path),
        vec![b"a".to_vec(), b"b".to_vec()],
        "'a' returns before 'b', where it was deleted from"
    );
}

// --- text ---

#[test]
fn undo_of_a_text_insert_removes_the_run() {
    let mut d = doc(1);
    let path = p(&[b"body"]);
    path::text_insert(&mut d, &path, 0, "hello");
    path::text_insert(&mut d, &path, 5, " world");
    assert_eq!(text(&d, &path), "hello world");

    undo(&mut d);
    assert_eq!(text(&d, &path), "hello");
    redo(&mut d);
    assert_eq!(text(&d, &path), "hello world");
}

#[test]
fn undo_of_a_text_delete_revives_the_substring() {
    let mut d = doc(1);
    let path = p(&[b"body"]);
    path::text_insert(&mut d, &path, 0, "hello world");
    path::text_delete(&mut d, &path, 5, 6);
    assert_eq!(text(&d, &path), "hello");

    undo(&mut d);
    assert_eq!(text(&d, &path), "hello world");
}

#[test]
fn undo_of_an_interior_text_delete_revives_in_place() {
    let mut d = doc(1);
    let path = p(&[b"body"]);
    path::text_insert(&mut d, &path, 0, "abcdef");
    path::text_delete(&mut d, &path, 2, 2);
    assert_eq!(text(&d, &path), "abef");

    undo(&mut d);
    assert_eq!(text(&d, &path), "abcdef");
}

// --- xml ---

#[test]
fn undo_of_an_xml_child_insert_removes_the_child() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    path::xml_insert_element(&mut d, &root, 0, b"p");
    assert_eq!(path::xml_children_len(&d, &root), Some(1));

    undo(&mut d);
    assert_eq!(path::xml_children_len(&d, &root), Some(0));
}

#[test]
fn undo_of_an_xml_child_delete_revives_the_subtree() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    path::xml_insert_element(&mut d, &root, 0, b"p");
    // A sibling text run after the element, so a revival that lands at the wrong
    // position is visible.
    let child_ops = d.transact(|c| {
        if let Some(mut kids) = c.xml_children(b"root") {
            let mut text = kids.insert_text(1);
            text.insert(0, "hello");
        }
    });
    assert!(!child_ops.is_empty());
    assert_eq!(child_shapes(&d, b"root"), vec!["<p>", "\"hello\""]);

    path::xml_child_delete(&mut d, &root, 0);
    assert_eq!(child_shapes(&d, b"root"), vec!["\"hello\""]);

    undo(&mut d);
    assert_eq!(child_shapes(&d, b"root"), vec!["<p>", "\"hello\""]);
}

#[test]
fn undo_of_an_xml_text_child_delete_revives_its_content() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    path::xml_insert_text(&mut d, &root, 0, "hello");
    assert_eq!(child_shapes(&d, b"root"), vec!["\"hello\""]);

    path::xml_child_delete(&mut d, &root, 0);
    assert!(child_shapes(&d, b"root").is_empty());

    undo(&mut d);
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["\"hello\""],
        "the revived text child comes back with its content, not as an empty shell"
    );
}

#[test]
fn undo_of_a_nested_xml_delete_revives_the_whole_subtree() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    // <doc><p>deep</p></doc>
    d.transact(|c| {
        if let Some(mut kids) = c.xml_children(b"root") {
            let mut para = kids.insert_element(0, b"p");
            para.attrs().register(b"align", Scalar::Int(1));
            let mut inner = para.children();
            let mut text = inner.insert_text(0);
            text.insert(0, "deep");
        }
    });
    assert_eq!(grandchild_shapes(&d, b"root", 0), vec!["\"deep\""]);
    assert_eq!(para_align(&d), Some(Scalar::Int(1)));

    path::xml_child_delete(&mut d, &root, 0);
    assert!(child_shapes(&d, b"root").is_empty());

    undo(&mut d);
    assert_eq!(
        grandchild_shapes(&d, b"root", 0),
        vec!["\"deep\""],
        "a deleted paragraph comes back with its text"
    );
    assert_eq!(
        para_align(&d),
        Some(Scalar::Int(1)),
        "and with its attributes"
    );
}

#[test]
fn undo_of_an_xml_move_returns_the_node_to_its_prior_parent() {
    let mut d = doc(1);
    let a = p(&[b"a"]);
    let b = p(&[b"b"]);
    path::xml_element(&mut d, &a, b"a");
    path::xml_element(&mut d, &b, b"b");
    path::xml_insert_element(&mut d, &a, 0, b"moved");
    assert_eq!(path::xml_children_len(&d, &a), Some(1));

    path::xml_move_child(&mut d, &a, 0, &b, 0);
    assert_eq!(path::xml_children_len(&d, &a), Some(0));
    assert_eq!(path::xml_children_len(&d, &b), Some(1));

    undo(&mut d);
    assert_eq!(path::xml_children_len(&d, &a), Some(1));
    assert_eq!(path::xml_children_len(&d, &b), Some(0));
}

#[test]
fn undo_of_a_reorder_restores_the_prior_index() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    path::xml_insert_text(&mut d, &root, 0, "one");
    path::xml_insert_text(&mut d, &root, 1, "two");
    path::xml_insert_text(&mut d, &root, 2, "three");
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["\"one\"", "\"two\"", "\"three\""]
    );

    path::xml_move_child(&mut d, &root, 2, &root, 0);
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["\"three\"", "\"one\"", "\"two\""]
    );

    undo(&mut d);
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["\"one\"", "\"two\"", "\"three\""],
        "the reordered node returns to index 2"
    );
}

/// The live children of the XML node in the root slot `key`.
fn children(d: &Document, key: &[u8]) -> Vec<Element> {
    let children = match d.get(key) {
        Some(Element::XmlElement(x)) => x.borrow().children(),
        Some(Element::XmlFragment(f)) => f.borrow().children(),
        _ => return Vec::new(),
    };
    let vals = children.borrow().values();
    vals
}

/// A rendering of each live child: `<tag>` for an element, the quoted string for
/// a text run — the shape and the content in one read.
fn child_shapes(d: &Document, key: &[u8]) -> Vec<String> {
    children(d, key).iter().map(render).collect()
}

fn render(child: &Element) -> String {
    match child {
        Element::XmlElement(x) => format!("<{}>", String::from_utf8_lossy(x.borrow().tag())),
        Element::Text(t) => format!("{:?}", t.borrow().as_string()),
        other => format!("?{:?}", other.kind()),
    }
}

/// The `align` attribute of the element child at index 0 of `root`.
fn para_align(d: &Document) -> Option<Scalar> {
    match children(d, b"root").first() {
        Some(Element::XmlElement(x)) => {
            let attrs = x.borrow().attrs();
            let value = attrs.borrow().get(b"align");
            match value {
                Some(Element::Register(r)) => Some(r.borrow().read().clone()),
                Some(Element::Scalar(s)) => Some(s),
                _ => None,
            }
        }
        _ => None,
    }
}

/// A rendering of the children of the element child at `index`.
fn grandchild_shapes(d: &Document, key: &[u8], index: usize) -> Vec<String> {
    match children(d, key).get(index) {
        Some(Element::XmlElement(x)) => {
            let kids = x.borrow().children();
            let vals = kids.borrow().values();
            vals.iter().map(render).collect()
        }
        _ => Vec::new(),
    }
}

#[test]
fn undo_of_an_overwrite_restores_a_displaced_xml_fragment() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_fragment(&mut d, &root);
    path::xml_insert_text(&mut d, &root, 0, "kept");
    assert_eq!(child_shapes(&d, b"root"), vec!["\"kept\""]);

    path::register(&mut d, &root, Scalar::Int(1));
    assert!(child_shapes(&d, b"root").is_empty());

    undo(&mut d);
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["\"kept\""],
        "a fragment is restored by re-creating it at the same key"
    );
}

#[test]
fn a_revived_xml_subtree_carries_its_attributes_and_counters() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    d.transact(|c| {
        if let Some(mut kids) = c.xml_children(b"root") {
            let mut para = kids.insert_element(0, b"p");
            {
                let mut attrs = para.attrs();
                attrs.register(b"align", Scalar::Int(1));
                attrs.inc(b"revision", 4);
                attrs.list(b"tags").insert(0, Scalar::Int(9));
            }
            let mut inner = para.children();
            let mut text = inner.insert_text(0);
            text.insert(0, "deep");
        }
    });
    assert_eq!(para_counter(&d, b"revision"), Some(4));

    path::xml_child_delete(&mut d, &root, 0);
    assert!(child_shapes(&d, b"root").is_empty());

    undo(&mut d);
    assert_eq!(grandchild_shapes(&d, b"root", 0), vec!["\"deep\""]);
    assert_eq!(para_align(&d), Some(Scalar::Int(1)));
    assert_eq!(
        para_counter(&d, b"revision"),
        Some(4),
        "a counter's tally rides the snapshot as increments toward it"
    );
    assert_eq!(para_list(&d, b"tags"), vec![Scalar::Int(9)]);
}

/// A counter attribute of the element child at index 0 of `root`.
fn para_counter(d: &Document, key: &[u8]) -> Option<i64> {
    match para_attr(d, key)? {
        Element::Counter(c) => Some(c.borrow().read()),
        _ => None,
    }
}

/// A list attribute of the element child at index 0 of `root`, as its values.
fn para_list(d: &Document, key: &[u8]) -> Vec<Scalar> {
    match para_attr(d, key) {
        Some(Element::List(l)) => l
            .borrow()
            .values()
            .into_iter()
            .filter_map(|v| match v {
                Element::Scalar(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn para_attr(d: &Document, key: &[u8]) -> Option<Element> {
    match children(d, b"root").first() {
        Some(Element::XmlElement(x)) => {
            let attrs = x.borrow().attrs();
            let held = attrs.borrow().get(key);
            held
        }
        _ => None,
    }
}

// --- marks (the document-level annotation set) ---

#[test]
fn undo_of_a_mark_removes_it() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "hello");
    let (ops, id) = path::mark(
        &mut d,
        &body,
        0,
        Side::Left,
        5,
        Side::Right,
        b"bold",
        Scalar::Bool(true),
    );
    assert!(!ops.is_empty() && id.is_some());
    assert_eq!(path::marks_at(&d, &body, 0).len(), 1);

    undo(&mut d);
    assert!(
        path::marks_at(&d, &body, 0).is_empty(),
        "undoing a mark tombstones it"
    );
}

#[test]
fn undo_of_a_mark_value_change_restores_the_prior_value() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "hello");
    let (_, id) = path::mark(
        &mut d,
        &body,
        0,
        Side::Left,
        5,
        Side::Right,
        b"link",
        Scalar::Int(1),
    );
    let id = id.expect("a mark handle");
    path::mark_set_value(&mut d, &id, Scalar::Int(2));
    assert_eq!(mark_value(&d), Some(Scalar::Int(2)));

    undo(&mut d);
    assert_eq!(mark_value(&d), Some(Scalar::Int(1)));
}

#[test]
fn undo_of_a_mark_delete_restores_the_mark() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "hello");
    let (_, id) = path::mark(
        &mut d,
        &body,
        0,
        Side::Left,
        5,
        Side::Right,
        b"link",
        Scalar::Int(9),
    );
    let id = id.expect("a mark handle");
    path::mark_delete(&mut d, &id);
    assert!(path::marks_at(&d, &body, 0).is_empty());

    undo(&mut d);
    assert_eq!(
        mark_value(&d),
        Some(Scalar::Int(9)),
        "the mark comes back over the same span with its value"
    );
}

/// The payload of the one live named annotation, whatever id it now carries — a
/// revived mark is a fresh range, so it cannot be read back by the old handle.
fn mark_value(d: &Document) -> Option<Scalar> {
    let live: Vec<_> = d
        .ranged_elements()
        .into_iter()
        .filter(|r| r.name.is_some())
        .collect();
    assert!(live.len() <= 1, "the fixture holds at most one mark");
    live.first().and_then(|r| r.scalar().cloned())
}

#[test]
fn undo_of_a_delete_of_a_composite_payload_mark_rebuilds_it() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "hello");
    let seq = seq_id(&d, b"body");
    let mut id = None;
    d.transact(|c| {
        let start = anchor_at(0);
        let end = anchor_at(5);
        let mut r = c.ranged();
        let made = r.create_map(
            RangeAnchor {
                seq,
                pos: start.pos,
            },
            RangeAnchor { seq, pos: end.pos },
        );
        id = Some(made);
    });
    let id = id.expect("a range id");
    // Fill the composite payload so the delete has something to lose.
    let payload = d.ranged_payload(id).expect("a live payload");
    let payload_id = payload.id();
    d.transact(|c| {
        if let Some(mut m) = c.ranged().payload_map(id) {
            m.register(b"author", Scalar::Int(42));
            m.inc(b"votes", 7);
        }
    });
    let _ = payload_id;
    assert_eq!(payload_slot(&d, id, b"author"), Some(Scalar::Int(42)));
    assert_eq!(payload_counter(&d, id, b"votes"), Some(7));

    d.transact(|c| c.ranged().delete(id));
    assert!(d.ranged_element(id).is_none());

    undo(&mut d);
    let back = d
        .ranged_elements()
        .into_iter()
        .find(|r| r.id != id)
        .expect("the range came back under a fresh id");
    assert_eq!(
        payload_slot(&d, back.id, b"author"),
        Some(Scalar::Int(42)),
        "the composite payload is rebuilt, not left empty"
    );
    assert_eq!(
        payload_counter(&d, back.id, b"votes"),
        Some(7),
        "including its counter tally"
    );
}

#[test]
fn undo_of_a_payload_change_on_a_composite_is_inert() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "hello");
    let seq = seq_id(&d, b"body");
    let mut id = None;
    d.transact(|c| {
        id = Some(c.ranged().create_list(
            RangeAnchor {
                seq,
                pos: anchor_at(0).pos,
            },
            RangeAnchor {
                seq,
                pos: anchor_at(5).pos,
            },
        ));
    });
    let id = id.expect("a range id");
    // Undo everything recorded so far, so the assertion below is about this op.
    while d.undo(ORIGIN).is_some() {}
    path::text_insert(&mut d, &body, 0, "x");
    let mark = d.can_undo(ORIGIN);
    assert!(mark);

    // A composite payload is edited through its container, never replaced, so a
    // scalar set against one emits nothing — and records nothing.
    let ops = d.transact(|c| c.ranged().set_payload(id, Scalar::Int(1)));
    assert!(ops.is_empty(), "the op is refused at the cursor");
    d.undo(ORIGIN)
        .expect("the text insert is still the newest step");
    assert_eq!(
        text(&d, &body),
        "",
        "the inert payload set recorded no step of its own"
    );
}

/// The element id of the sequence in the root slot `key`.
fn seq_id(d: &Document, key: &[u8]) -> ElementId {
    match d.get(key) {
        Some(Element::Text(t)) => t.borrow().id(),
        _ => panic!("expected a text slot"),
    }
}

/// A range endpoint bound to codepoint `index` from the left.
fn anchor_at(index: usize) -> RangeAnchor {
    RangeAnchor {
        seq: ElementId::from_bytes([0u8; 16]),
        pos: match index {
            0 => RelativePosition::Start,
            _ => RelativePosition::End,
        },
    }
}

fn payload_slot(d: &Document, id: ElementId, key: &[u8]) -> Option<Scalar> {
    match d.ranged_payload(id)? {
        Element::Map(m) => match m.borrow().get(key)? {
            Element::Register(r) => Some(r.borrow().read().clone()),
            Element::Scalar(s) => Some(s),
            _ => None,
        },
        _ => None,
    }
}

fn payload_counter(d: &Document, id: ElementId, key: &[u8]) -> Option<i64> {
    match d.ranged_payload(id)? {
        Element::Map(m) => match m.borrow().get(key)? {
            Element::Counter(c) => Some(c.borrow().read()),
            _ => None,
        },
        _ => None,
    }
}

// --- ACL tuples (the document-level authorization set) ---

#[test]
fn undo_of_an_acl_grant_revokes_it() {
    let mut d = doc(1);
    let author = cid(1);
    let mut id = None;
    d.transact(|c| {
        id = Some(c.acl().grant(
            AclSubject::Actor(cid(7)),
            AclGrant::Capability(Capability::Write),
            AclEffect::Allow,
            p(&[b"doc"]),
            author,
        ));
    });
    let id = id.expect("a tuple id");
    assert_eq!(d.acl_tuples().len(), 1);

    undo(&mut d);
    assert!(
        d.acl_tuple(id).is_none(),
        "the tuple is tombstoned out of the live set"
    );
}

#[test]
fn undo_of_an_acl_revoke_restores_the_grant() {
    let mut d = doc(1);
    let author = cid(1);
    let mut id = None;
    d.transact(|c| {
        id = Some(c.acl().grant(
            AclSubject::Actor(cid(7)),
            AclGrant::Capability(Capability::Write),
            AclEffect::Allow,
            p(&[b"doc"]),
            author,
        ));
    });
    let id = id.expect("a tuple id");
    let granted = d.acl_tuple(id).expect("the live tuple");
    d.transact(|c| c.acl().revoke(id));
    assert!(d.acl_tuple(id).is_none());

    undo(&mut d);
    let live = d.acl_tuples();
    assert_eq!(live.len(), 1, "an equivalent grant is re-issued");
    let back = &live[0];
    assert_eq!(back.subject, granted.subject);
    assert_eq!(back.grant, granted.grant);
    assert_eq!(back.effect, granted.effect);
    assert_eq!(back.scope, granted.scope);
    assert_eq!(back.grantor, granted.grantor);
    assert_ne!(
        back.id, granted.id,
        "a revoke is terminal, so this is a new tuple"
    );
}

// --- origin scoping ---

#[test]
fn recording_is_off_until_an_origin_is_set() {
    let mut d = Document::new(cid(1));
    path::register(&mut d, &p(&[b"k"]), Scalar::Int(1));
    assert!(!d.can_undo(ORIGIN));
    assert_eq!(d.undo(ORIGIN), None);
}

#[test]
fn undo_selects_only_its_own_origins_intention() {
    let mut d = Document::new(cid(1));
    d.set_undo_origin(ORIGIN);
    path::register(&mut d, &p(&[b"mine"]), Scalar::Int(1));
    d.set_undo_origin(OTHER);
    path::register(&mut d, &p(&[b"theirs"]), Scalar::Int(2));

    d.undo(ORIGIN).expect("this origin's own intention");
    assert_eq!(reg(&d, &p(&[b"mine"])), None, "my edit is undone");
    assert_eq!(
        reg(&d, &p(&[b"theirs"])),
        Some(Scalar::Int(2)),
        "the newer edit of another origin is untouched"
    );
}

#[test]
fn an_origin_with_no_intention_has_nothing_to_undo() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"k"]), Scalar::Int(1));
    assert!(d.can_undo(ORIGIN));
    assert!(!d.can_undo(OTHER));
    assert_eq!(d.undo(OTHER), None);
}

#[test]
fn a_peers_edit_is_never_recorded() {
    let mut author = doc(1);
    let ops = path::register(&mut author, &p(&[b"k"]), Scalar::Int(1));

    let mut peer = doc(2);
    apply_all(&mut peer, &ops);
    assert_eq!(reg(&peer, &p(&[b"k"])), Some(Scalar::Int(1)));
    assert!(
        !peer.can_undo(ORIGIN),
        "a remote op is applied, never emitted, so it is not undoable here"
    );
}

#[test]
fn an_undo_reverts_only_this_replicas_own_edit() {
    let mut a = doc(1);
    let mut b = doc(2);
    let from_a = path::register(&mut a, &p(&[b"a"]), Scalar::Int(1));
    apply_all(&mut b, &from_a);
    let from_b = path::register(&mut b, &p(&[b"b"]), Scalar::Int(2));
    apply_all(&mut a, &from_b);

    undo(&mut b);
    assert_eq!(reg(&b, &p(&[b"b"])), None, "b's own edit is reverted");
    assert_eq!(
        reg(&b, &p(&[b"a"])),
        Some(Scalar::Int(1)),
        "a's edit is untouched — global undo is not supported"
    );
}

#[test]
fn an_edit_from_another_origin_leaves_this_origins_redo_alone() {
    let mut d = Document::new(cid(1));
    d.set_undo_origin(ORIGIN);
    path::register(&mut d, &p(&[b"k"]), Scalar::Int(1));
    d.undo(ORIGIN);
    assert!(d.can_redo(ORIGIN));

    d.set_undo_origin(OTHER);
    path::register(&mut d, &p(&[b"other"]), Scalar::Int(9));
    assert!(
        d.can_redo(ORIGIN),
        "another origin's edit does not make my redo ambiguous"
    );

    d.set_undo_origin(ORIGIN);
    path::register(&mut d, &p(&[b"k"]), Scalar::Int(3));
    assert!(!d.can_redo(ORIGIN), "my own fresh edit clears my redo");
}

// --- intentions, grouping, atomic transactions ---

#[test]
fn a_group_undoes_as_one_step() {
    let mut d = doc(1);
    d.begin_intention();
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    path::register(&mut d, &p(&[b"b"]), Scalar::Int(2));
    path::inc(&mut d, &p(&[b"c"]), 5);
    d.end_intention();

    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"a"])), None);
    assert_eq!(reg(&d, &p(&[b"b"])), None);
    assert_eq!(counter(&d, &p(&[b"c"])), 0);
    assert!(!d.can_undo(ORIGIN), "the group was a single step");
}

#[test]
fn an_empty_intention_is_not_recorded() {
    let mut d = doc(1);
    // A recorded edit, so "nothing to undo" below cannot be read as "recording
    // is off".
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    undo(&mut d);
    assert!(!d.can_undo(ORIGIN));

    d.begin_intention();
    d.end_intention();
    assert!(!d.can_undo(ORIGIN), "an empty group is not a step");

    d.transact(|_| {});
    assert!(
        !d.can_undo(ORIGIN),
        "a transact that emits nothing is not one either"
    );

    // A path edit that matches nothing emits no ops and records no step.
    path::mark_delete(&mut d, b"not-a-handle");
    assert!(!d.can_undo(ORIGIN));
}

#[test]
fn each_transact_is_its_own_intention() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    path::register(&mut d, &p(&[b"b"]), Scalar::Int(2));

    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"b"])), None);
    assert_eq!(
        reg(&d, &p(&[b"a"])),
        Some(Scalar::Int(1)),
        "only the last transact is undone"
    );
}

#[test]
fn an_atomic_group_undoes_as_one_transaction() {
    let mut d = doc(1);
    let ops = d.atomic_transact(|c| {
        c.register(b"a", Scalar::Int(1));
        c.register(b"b", Scalar::Int(2));
    });
    assert_eq!(ops.len(), 2);
    assert!(ops.iter().all(|o| o.tx.is_some()));

    let undone = undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"a"])), None);
    assert_eq!(reg(&d, &p(&[b"b"])), None);
    let tx: Vec<_> = undone.iter().filter_map(|o| o.tx).collect();
    assert_eq!(tx.len(), undone.len(), "every inverse op is in the group");
    assert!(
        tx.windows(2).all(|w| w[0].id == w[1].id),
        "the inverses ship as one atomic transaction"
    );
}

#[test]
fn a_redone_atomic_group_is_atomic_again() {
    let mut d = doc(1);
    d.atomic_transact(|c| {
        c.register(b"a", Scalar::Int(1));
        c.register(b"b", Scalar::Int(2));
    });
    undo(&mut d);
    let ops = redo(&mut d);
    assert_eq!(reg(&d, &p(&[b"a"])), Some(Scalar::Int(1)));
    assert!(!ops.is_empty() && ops.iter().all(|o| o.tx.is_some()));
}

#[test]
fn undo_is_refused_while_an_atomic_transaction_is_open() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    d.begin_atomic();
    assert_eq!(
        d.undo(ORIGIN),
        None,
        "an undo inside an open group would be swallowed by it"
    );
    d.commit_atomic();
    assert!(d.undo(ORIGIN).is_some(), "and works once the group closes");
}

// --- redo ---

#[test]
fn redo_replays_an_undone_edit() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"n"]), Scalar::Int(1));
    path::register(&mut d, &p(&[b"n"]), Scalar::Int(2));

    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"n"])), Some(Scalar::Int(1)));
    redo(&mut d);
    assert_eq!(reg(&d, &p(&[b"n"])), Some(Scalar::Int(2)));
}

#[test]
fn a_fresh_edit_clears_the_redo_stack() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"n"]), Scalar::Int(1));
    undo(&mut d);
    assert!(d.can_redo(ORIGIN));

    path::register(&mut d, &p(&[b"n"]), Scalar::Int(5));
    assert!(!d.can_redo(ORIGIN));
    assert_eq!(d.redo(ORIGIN), None);
}

#[test]
fn undo_and_redo_alternate_indefinitely() {
    let mut d = doc(1);
    let path = p(&[b"body"]);
    path::text_insert(&mut d, &path, 0, "hello");
    for _ in 0..5 {
        undo(&mut d);
        assert_eq!(text(&d, &path), "");
        redo(&mut d);
        assert_eq!(text(&d, &path), "hello");
    }
}

#[test]
fn a_deeper_stack_undoes_and_redoes_in_order() {
    let mut d = doc(1);
    let path = p(&[b"n"]);
    for i in 1..=4 {
        path::register(&mut d, &path, Scalar::Int(i));
    }
    for expected in [3, 2, 1] {
        undo(&mut d);
        assert_eq!(reg(&d, &path), Some(Scalar::Int(expected)));
    }
    for expected in [2, 3, 4] {
        redo(&mut d);
        assert_eq!(reg(&d, &path), Some(Scalar::Int(expected)));
    }
}

// --- convergence: an inverse is an ordinary forward op ---

#[test]
fn a_peer_converges_after_an_undo() {
    let mut a = doc(1);
    let mut peer = Document::new(cid(2));
    let setup = path::text_insert(&mut a, &p(&[b"body"]), 0, "hello world");
    apply_all(&mut peer, &setup);

    let edit = path::text_delete(&mut a, &p(&[b"body"]), 5, 6);
    apply_all(&mut peer, &edit);
    assert_eq!(text(&peer, &p(&[b"body"])), "hello");

    let inverse = undo(&mut a);
    apply_all(&mut peer, &inverse);
    assert_eq!(text(&peer, &p(&[b"body"])), "hello world");
    assert_eq!(observe(&a), observe(&peer));
}

#[test]
fn a_peer_converges_after_undo_and_redo_across_op_kinds() {
    let mut a = doc(1);
    let mut peer = Document::new(cid(2));
    let ship = |ops: Vec<Op>, peer: &mut Document| apply_all(peer, &ops);

    ship(
        path::register(&mut a, &p(&[b"title"]), Scalar::Int(1)),
        &mut peer,
    );
    ship(path::inc(&mut a, &p(&[b"votes"]), 3), &mut peer);
    ship(
        path::list_insert(&mut a, &p(&[b"items"]), 0, b"x"),
        &mut peer,
    );
    ship(
        path::text_insert(&mut a, &p(&[b"body"]), 0, "hello"),
        &mut peer,
    );
    ship(path::xml_element(&mut a, &p(&[b"root"]), b"doc"), &mut peer);
    ship(
        path::xml_insert_text(&mut a, &p(&[b"root"]), 0, "para"),
        &mut peer,
    );
    assert_eq!(observe(&a), observe(&peer));
    let before = observe(&a);

    // Undo every intention, then redo them all; the peer only ever sees ops.
    let mut undone = 0;
    while let Some(ops) = a.undo(ORIGIN) {
        ship(ops, &mut peer);
        undone += 1;
    }
    assert_eq!(undone, 6);
    assert_eq!(observe(&a), observe(&peer));
    assert!(
        observe(&a).iter().all(|line| line.contains("text:None")),
        "every edit is undone, so nothing is left: {:?}",
        observe(&a)
    );

    while let Some(ops) = a.redo(ORIGIN) {
        ship(ops, &mut peer);
    }
    assert_eq!(observe(&a), observe(&peer));
    assert_eq!(
        observe(&a),
        before,
        "redoing everything lands back on the state undo started from"
    );
}

#[test]
fn an_undone_edit_converges_even_after_a_peer_edits_the_same_text() {
    let mut a = doc(1);
    let mut b = Document::new(cid(2));
    let setup = path::text_insert(&mut a, &p(&[b"body"]), 0, "hello");
    apply_all(&mut b, &setup);

    let from_b = path::text_insert(&mut b, &p(&[b"body"]), 5, "!");
    let inverse = undo(&mut a);
    apply_all(&mut a, &from_b);
    apply_all(&mut b, &inverse);
    assert_eq!(text(&a, &p(&[b"body"])), text(&b, &p(&[b"body"])));
}

// --- the networked, per-channel seat ---

const ROOM: &[u8] = b"room";

fn ops_of(m: Message) -> Vec<Op> {
    match m {
        Message::Ops { ops, .. } => ops,
        other => panic!("expected Ops, got {other:?}"),
    }
}

fn channel_doc(s: &ClientSession, ch: Channel) -> &Document {
    s.document(ch).expect("a held channel")
}

#[test]
fn a_channels_edits_are_undoable_and_ship_as_ordinary_ops() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    s.set_undo_origin(ch, ORIGIN);
    let first = ops_of(
        s.edit(ch, |c| c.register(b"title", Scalar::Int(1)))
            .unwrap(),
    );
    let second = ops_of(
        s.edit(ch, |c| c.register(b"title", Scalar::Int(2)))
            .unwrap(),
    );
    assert!(s.can_undo(ch, ORIGIN));

    let edits: Vec<Op> = [first, second].concat();
    let mut peer = Document::new(cid(2));
    apply_all(&mut peer, &edits);
    assert_eq!(reg(&peer, &p(&[b"title"])), Some(Scalar::Int(2)));

    let ops = ops_of(s.undo(ch, ORIGIN).expect("an intention to undo"));
    assert!(!ops.is_empty());
    assert_eq!(
        path::get_register(channel_doc(&s, ch), &p(&[b"title"])),
        Some(Scalar::Int(1))
    );

    apply_all(&mut peer, &ops);
    assert_eq!(
        reg(&peer, &p(&[b"title"])),
        Some(Scalar::Int(1)),
        "the inverse is an ordinary op the peer folds in with no notion of undo"
    );
    assert!(s.can_redo(ch, ORIGIN));
}

#[test]
fn a_channels_undo_ops_go_through_the_outbox() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    s.set_undo_origin(ch, ORIGIN);
    let edit = ops_of(s.edit(ch, |c| c.register(b"a", Scalar::Int(1))).unwrap());
    let undone = ops_of(s.undo(ch, ORIGIN).unwrap());
    assert_eq!(
        s.outbox_len(ch),
        edit.len() + undone.len(),
        "an inverse is resent on reconnect like any other authored op"
    );
}

#[test]
fn each_channel_keeps_its_own_undo_stack() {
    let mut s = ClientSession::new(cid(1));
    let (a, _) = s.subscribe(b"room-a");
    let (b, _) = s.subscribe(b"room-b");
    s.set_undo_origin(a, ORIGIN);
    s.set_undo_origin(b, ORIGIN);
    s.edit(a, |c| c.register(b"k", Scalar::Int(1)));

    assert!(s.can_undo(a, ORIGIN));
    assert!(!s.can_undo(b, ORIGIN), "the other seat recorded nothing");
    assert_eq!(s.undo(b, ORIGIN), None);
}

#[test]
fn a_channel_with_no_origin_records_nothing() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    s.edit(ch, |c| c.register(b"k", Scalar::Int(1)));
    assert!(!s.can_undo(ch, ORIGIN));
    assert_eq!(s.undo(ch, ORIGIN), None);
}

#[test]
fn a_channels_remote_ops_are_not_undoable() {
    let mut author = Document::new(cid(9));
    let ops = path::register(&mut author, &p(&[b"k"]), Scalar::Int(1));

    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    s.set_undo_origin(ch, ORIGIN);
    s.receive(Message::Ops { channel: ch, ops }).unwrap();
    assert_eq!(
        path::get_register(channel_doc(&s, ch), &p(&[b"k"])),
        Some(Scalar::Int(1))
    );
    assert!(
        !s.can_undo(ch, ORIGIN),
        "a collaborator's op never lands on this seat's stack"
    );
}

#[test]
fn a_channels_atomic_edit_undoes_as_one_transaction() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    s.set_undo_origin(ch, ORIGIN);
    s.atomic_edit(ch, |c| {
        c.register(b"a", Scalar::Int(1));
        c.register(b"b", Scalar::Int(2));
    });

    let ops = ops_of(s.undo(ch, ORIGIN).expect("the group"));
    assert_eq!(ops.len(), 2);
    assert!(ops.iter().all(|o| o.tx.is_some()));
    assert_eq!(
        path::get_register(channel_doc(&s, ch), &p(&[b"a"])),
        None,
        "the whole group is reverted"
    );
}

// --- the hazards a plain reversal introduces ---
//
// An intention's inverses replay newest-first, but the forward order carried
// dependencies a plain reversal breaks. Each of these diverges or loses content
// if the replay is not planned.

#[test]
fn an_undo_converges_when_a_peer_displaced_the_slot_meanwhile() {
    let mut a = Document::new(cid(1));
    let mut b = doc(2);
    let body = p(&[b"body"]);
    let setup = path::text_insert(&mut b, &body, 0, "hi");
    apply_all(&mut a, &setup);

    // b deletes a codepoint; its intention is [restore the slot, revive "h"].
    let edit = path::text_delete(&mut b, &body, 0, 1);
    apply_all(&mut a, &edit);
    // a displaces the whole slot with a register.
    let displace = path::register(&mut a, &body, Scalar::Int(1));
    apply_all(&mut b, &displace);

    let inverse = undo(&mut b);
    apply_all(&mut a, &inverse);
    assert_eq!(
        observe(&a),
        observe(&b),
        "an inverse emitted onto an unreachable target is applied inertly here \
         but buffered and replayed by a peer"
    );
    // The reversal put the revival ahead of the slot restore that makes it
    // reachable; without the hoist the revival is skipped and "h" never returns.
    assert_eq!(
        text(&b, &body),
        "hi",
        "and the undo actually restores the text"
    );
}

#[test]
fn an_intention_that_drops_and_re_creates_a_slot_undoes_both_writes() {
    let mut d = doc(1);
    let items = p(&[b"items"]);
    path::list_insert(&mut d, &items, 0, b"a");

    // One gesture: clear the slot, then put an item back. The second call
    // re-creates the list, so the slot is written twice inside one intention.
    d.begin_intention();
    path::delete(&mut d, &items);
    path::list_insert(&mut d, &items, 0, b"z");
    d.end_intention();
    // The list is retained by id, so re-creating the slot brings "a" back with
    // it and "z" lands in front.
    assert_eq!(list_vals(&d, &items), vec![b"z".to_vec(), b"a".to_vec()]);

    undo(&mut d);
    assert_eq!(
        list_vals(&d, &items),
        vec![b"a".to_vec()],
        "a container another step puts back is not really removed, and two \
         writes to one slot keep their order"
    );
}

#[test]
fn redo_restores_a_child_deleted_after_it_was_moved() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    let sink = p(&[b"sink"]);
    path::xml_element(&mut d, &root, b"doc");
    path::xml_element(&mut d, &sink, b"sink");
    path::xml_insert_element(&mut d, &root, 0, b"p");
    path::xml_move_child(&mut d, &root, 0, &sink, 0);
    assert_eq!(child_shapes(&d, b"sink"), vec!["<p>"]);

    undo(&mut d);
    assert_eq!(child_shapes(&d, b"root"), vec!["<p>"], "the move is undone");
    undo(&mut d);
    assert!(child_shapes(&d, b"root").is_empty(), "then the insert");

    // The delete landed on a placement the move had suppressed; its inverse has
    // to reach the node by id or the redo has nothing to bring back.
    redo(&mut d);
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["<p>"],
        "redo brings the child back"
    );
    assert!(d.can_redo(ORIGIN), "and the move is still redoable");
    redo(&mut d);
    assert_eq!(child_shapes(&d, b"sink"), vec!["<p>"]);
}

#[test]
fn every_undo_leaves_exactly_one_redo() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    let sink = p(&[b"sink"]);
    path::xml_element(&mut d, &root, b"doc");
    path::xml_element(&mut d, &sink, b"sink");
    path::xml_insert_element(&mut d, &root, 0, b"p");
    path::xml_move_child(&mut d, &root, 0, &sink, 0);

    let mut undone = 0;
    while d.undo(ORIGIN).is_some() {
        undone += 1;
    }
    let mut redone = 0;
    while d.redo(ORIGIN).is_some() {
        redone += 1;
    }
    assert_eq!(
        undone, redone,
        "an intention whose inverses were all inert still owes a redo"
    );
}

#[test]
fn redo_of_a_sequence_node_create_keeps_what_the_intention_put_inside_it() {
    let mut d = doc(1);
    let root = p(&[b"root"]);
    path::xml_element(&mut d, &root, b"doc");
    // One transact: insert the text child and fill it.
    path::xml_insert_text(&mut d, &root, 0, "hello");
    assert_eq!(child_shapes(&d, b"root"), vec!["\"hello\""]);

    undo(&mut d);
    assert!(child_shapes(&d, b"root").is_empty());
    redo(&mut d);
    assert_eq!(
        child_shapes(&d, b"root"),
        vec!["\"hello\""],
        "tearing the content down before the node would leave the redo an empty shell"
    );
}

#[test]
fn redo_of_a_container_create_keeps_what_the_intention_put_inside_it() {
    let mut d = doc(1);
    let items = p(&[b"items"]);
    // One transact per call, each creating the list and inserting into it.
    path::list_insert(&mut d, &items, 0, b"a");
    path::list_insert(&mut d, &items, 1, b"b");

    d.undo(ORIGIN);
    d.undo(ORIGIN);
    assert_eq!(path::list_len(&d, &items), None);
    d.redo(ORIGIN);
    d.redo(ORIGIN);
    assert_eq!(
        list_vals(&d, &items),
        vec![b"a".to_vec(), b"b".to_vec()],
        "the list comes back with both items"
    );
}

#[test]
fn undoing_an_insert_also_removes_what_an_earlier_undo_revived() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "X");
    path::text_insert(&mut d, &body, 1, "hello");
    path::text_delete(&mut d, &body, 2, 3);
    assert_eq!(text(&d, &body), "Xho");

    undo(&mut d);
    assert_eq!(text(&d, &body), "Xhello");
    undo(&mut d);
    assert_eq!(
        text(&d, &body),
        "X",
        "the revived codepoints came back under fresh ids; the insert's undo \
         must follow the substitution or it leaves them behind"
    );
}

#[test]
fn undoing_a_list_insert_also_removes_a_revived_item() {
    let mut d = doc(1);
    let items = p(&[b"items"]);
    path::list_insert(&mut d, &items, 0, b"keep");
    path::list_insert(&mut d, &items, 1, b"gone");
    path::list_delete(&mut d, &items, 1);
    assert_eq!(list_vals(&d, &items), vec![b"keep".to_vec()]);

    undo(&mut d);
    assert_eq!(
        list_vals(&d, &items),
        vec![b"keep".to_vec(), b"gone".to_vec()]
    );
    undo(&mut d);
    assert_eq!(
        list_vals(&d, &items),
        vec![b"keep".to_vec()],
        "the revived item is removed with the insert that first made it"
    );
}

#[test]
fn undo_is_refused_while_an_explicit_intention_is_open() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    d.begin_intention();
    path::register(&mut d, &p(&[b"b"]), Scalar::Int(2));
    assert_eq!(
        d.undo(ORIGIN),
        None,
        "an undo here would record into the open group and carry it off"
    );
    d.end_intention();

    // Both intentions are intact and undo in order.
    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"b"])), None);
    assert_eq!(reg(&d, &p(&[b"a"])), Some(Scalar::Int(1)));
    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"a"])), None);
}

#[test]
fn undo_of_a_counter_restores_what_the_delta_displaced() {
    let mut d = doc(1);
    let k = p(&[b"k"]);
    path::register(&mut d, &k, Scalar::Int(7));
    path::inc(&mut d, &k, 5);
    assert_eq!(reg(&d, &k), None, "the counter took the slot");

    undo(&mut d);
    assert_eq!(
        reg(&d, &k),
        Some(Scalar::Int(7)),
        "cancelling the tally is only half the inverse — the counter re-won the \
         slot on the way past"
    );
    assert_eq!(counter(&d, &k), 0);
}

#[test]
fn undo_of_the_first_counter_delta_empties_the_slot() {
    let mut d = doc(1);
    let k = p(&[b"votes"]);
    path::inc(&mut d, &k, 3);
    assert_eq!(path::get_counter(&d, &k), Some(3));

    undo(&mut d);
    assert_eq!(
        path::get_counter(&d, &k),
        None,
        "a counter empties its slot on undo like every other kind"
    );
}

#[test]
fn a_counter_delta_over_a_container_restores_it() {
    let mut d = doc(1);
    let k = p(&[b"k"]);
    path::text_insert(&mut d, &k, 0, "hi");
    path::inc(&mut d, &k, 1);
    assert_eq!(path::text_get(&d, &k), None);

    undo(&mut d);
    assert_eq!(text(&d, &k), "hi", "the displaced text comes back intact");
}

#[test]
fn switching_origin_mid_group_keeps_the_recorded_edits_undoable() {
    let mut d = Document::new(cid(1));
    d.set_undo_origin(ORIGIN);
    d.begin_intention();
    path::register(&mut d, &p(&[b"mine"]), Scalar::Int(1));
    d.set_undo_origin(OTHER);
    path::register(&mut d, &p(&[b"theirs"]), Scalar::Int(2));
    d.end_intention();

    assert!(d.can_undo(ORIGIN), "the first edit is still mine to undo");
    d.undo(ORIGIN);
    assert_eq!(reg(&d, &p(&[b"mine"])), None);
    assert_eq!(reg(&d, &p(&[b"theirs"])), Some(Scalar::Int(2)));
}

#[test]
fn committing_without_an_open_transaction_does_not_close_an_intention() {
    let mut d = doc(1);
    d.begin_intention();
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    assert!(d.commit_atomic().is_empty(), "no transaction was open");
    path::register(&mut d, &p(&[b"b"]), Scalar::Int(2));
    d.end_intention();

    undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"a"])), None, "the whole group is one step");
    assert_eq!(reg(&d, &p(&[b"b"])), None);
    assert!(!d.can_undo(ORIGIN));
}

#[test]
fn a_revival_in_one_zone_does_not_re_point_another_zones_delete() {
    // An op is stamped from its own zone's clock, so two zones mint the same
    // `Stamp`. A revival records the id its item came back under; keyed by the
    // id alone it would re-point the *other* zone's delete at it.
    let mut d = doc(1);
    d.set_schema(
        Schema::parse(
            r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } },
                 "zones": { "west": "/west", "east": "/east" } }"#,
        )
        .expect("schema parses"),
    );
    let west = p(&[b"west", b"items"]);
    let east = p(&[b"east", b"items"]);

    path::list_insert(&mut d, &east, 0, b"E0");
    path::register(&mut d, &p(&[b"west", b"x"]), Scalar::Int(1));
    path::register(&mut d, &p(&[b"west", b"y"]), Scalar::Int(2));
    path::list_insert(&mut d, &west, 0, b"W");
    path::list_insert(&mut d, &east, 1, b"E1");
    path::list_delete(&mut d, &west, 0);
    assert!(list_vals(&d, &west).is_empty());

    // Revives W under a fresh id, recording a substitution in the west zone.
    undo(&mut d);
    assert_eq!(list_vals(&d, &west), vec![b"W".to_vec()]);

    // Now undo the east insert. Its inverse names an east id that happens to
    // equal the west id the revival replaced.
    undo(&mut d);
    assert_eq!(
        list_vals(&d, &east),
        vec![b"E0".to_vec()],
        "the east delete must reach E1, not the west revival"
    );
    assert_eq!(
        list_vals(&d, &west),
        vec![b"W".to_vec()],
        "west is untouched"
    );
}

#[test]
fn an_inverse_onto_a_container_a_peer_displaced_lands_in_the_retained_one() {
    let mut a = doc(1);
    let mut b = Document::new(cid(2));
    let root = p(&[b"root"]);
    let setup = path::xml_element(&mut a, &root, b"doc");
    apply_all(&mut b, &setup);
    let fill = path::xml_insert_text(&mut a, &root, 0, "x");
    apply_all(&mut b, &fill);

    // A child delete emits only a ListDelete on the children sequence — no
    // container-create rides along, so nothing in the intention can make the
    // sequence reachable again.
    let del = path::xml_child_delete(&mut a, &root, 0);
    apply_all(&mut b, &del);
    // The peer displaces the whole element out of its slot.
    let displace = path::register(&mut b, &root, Scalar::Int(1));
    apply_all(&mut a, &displace);

    let ops = a.undo(ORIGIN).expect("the intention comes off the stack");
    assert!(
        !ops.is_empty(),
        "the retained element is still the target its children address, so the \
         inverse is emitted rather than lost on the one replica that holds it"
    );
    apply_all(&mut b, &ops);
    assert_eq!(observe(&a), observe(&b));
    assert!(
        a.can_redo(ORIGIN),
        "an intention whose inverses landed hidden still owes a redo"
    );

    // And the revival is there on both once the element takes its slot back.
    let back = path::xml_element(&mut a, &root, b"doc");
    apply_all(&mut b, &back);
    assert_eq!(observe(&a), observe(&b));
    assert_eq!(
        path::xml_children_len(&a, &root),
        Some(1),
        "the undone delete is visible again with the element"
    );
}

#[test]
fn redo_restores_the_interior_of_a_container_the_intention_only_displaced() {
    let mut d = doc(1);
    let a = p(&[b"a"]);
    path::xml_element(&mut d, &a, b"p");

    // One gesture: add a child, then displace the element with a counter. The
    // element is *retained* holding the child — it is not removed from the tree
    // — so undoing the child cannot be skipped just because the counter took
    // the slot.
    d.begin_intention();
    path::xml_insert_text(&mut d, &a, 0, "z");
    path::inc(&mut d, &a, 2);
    d.end_intention();

    undo(&mut d);
    assert_eq!(child_shapes(&d, b"a"), Vec::<String>::new());
    redo(&mut d);
    assert_eq!(counter(&d, &a), 2);

    // Bring the element back and look inside: the redo had to put the child back
    // into the retained element, not drop the step.
    path::xml_element(&mut d, &a, b"p");
    assert_eq!(
        child_shapes(&d, b"a"),
        vec!["\"z\""],
        "a displaced container keeps what the intention put in it, so the redo \
         must restore it there"
    );
}

#[test]
fn a_deleted_map_slot_stays_undone_when_the_container_comes_back() {
    let mut d = doc(1);
    let nested = p(&[b"b", b"sub"]);
    // Creates the map at `b` and a text inside it.
    path::text_insert(&mut d, &nested, 0, "xy");
    assert_eq!(text(&d, &nested), "xy");

    undo(&mut d);
    assert_eq!(path::map_keys(&d, &p(&[b"b"])), None, "the slot is empty");

    // A map slot is never terminally removed: the container is retained by id,
    // so anything that installs the slot brings it back — with whatever the
    // undone intention left inside, unless the interior was undone too.
    path::register(&mut d, &p(&[b"b", b"other"]), Scalar::Int(1));
    assert_eq!(
        path::text_get(&d, &nested),
        None,
        "the undone text must not reappear with the retained container"
    );
}

#[test]
fn every_revival_of_an_intention_lands_when_a_peer_displaced_the_slot() {
    let mut a = Document::new(cid(1));
    let mut b = doc(2);
    let body = p(&[b"body"]);
    let setup = path::text_insert(&mut b, &body, 0, "hello");
    apply_all(&mut a, &setup);

    // Two deletes in one intention, so the slot is written twice by the
    // inverses — the second write must not stop the first revival from being
    // made reachable.
    b.begin_intention();
    let mut edits = path::text_delete(&mut b, &body, 0, 1);
    edits.extend(path::text_delete(&mut b, &body, 0, 1));
    b.end_intention();
    assert_eq!(text(&b, &body), "llo");
    apply_all(&mut a, &edits);

    let displace = path::register(&mut a, &body, Scalar::Int(1));
    apply_all(&mut b, &displace);

    let inverse = undo(&mut b);
    assert_eq!(text(&b, &body), "hello", "both revivals land");
    apply_all(&mut a, &inverse);
    assert_eq!(observe(&a), observe(&b));
}

#[test]
fn a_counter_and_a_container_in_one_intention_both_undo_when_displaced() {
    let mut a = Document::new(cid(1));
    let mut b = doc(2);
    let k = p(&[b"k"]);
    let setup = path::text_insert(&mut b, &k, 0, "hi");
    apply_all(&mut a, &setup);

    b.begin_intention();
    let mut edits = path::inc(&mut b, &k, 1);
    edits.extend(path::text_insert(&mut b, &k, 0, "Z"));
    b.end_intention();
    assert_eq!(text(&b, &k), "Zhi");

    // The peer must see b's edits first, or its own write is behind them in
    // lamport order and loses the slot instead of taking it.
    apply_all(&mut a, &edits);
    let displace = path::register(&mut a, &k, Scalar::Int(9));
    apply_all(&mut b, &displace);
    assert_eq!(
        path::text_get(&b, &k),
        None,
        "the peer's write really did take the slot"
    );

    undo(&mut b);
    assert_eq!(
        text(&b, &k),
        "hi",
        "the inserted run is undone even though the slot was displaced"
    );
}

#[test]
fn undo_of_an_edit_inside_a_revived_annotation_reaches_the_revived_one() {
    let mut d = doc(1);
    let body = p(&[b"body"]);
    path::text_insert(&mut d, &body, 0, "hello");
    let seq = seq_id(&d, b"body");
    let mut id = None;
    d.transact(|c| {
        id = Some(c.ranged().create_map(
            RangeAnchor {
                seq,
                pos: anchor_at(0).pos,
            },
            RangeAnchor {
                seq,
                pos: anchor_at(5).pos,
            },
        ));
    });
    let id = id.expect("a range id");
    d.transact(|c| {
        if let Some(mut m) = c.ranged().payload_map(id) {
            m.register(b"author", Scalar::Int(42));
        }
    });
    d.transact(|c| c.ranged().delete(id));

    // Revives the range under a fresh id, rebuilding the payload.
    undo(&mut d);
    let back = d
        .ranged_elements()
        .into_iter()
        .find(|r| r.id != id)
        .expect("the range came back under a fresh id");
    assert_eq!(payload_slot(&d, back.id, b"author"), Some(Scalar::Int(42)));

    // The next undo targets the payload container of the *old* range id.
    undo(&mut d);
    assert_eq!(
        payload_slot(&d, back.id, b"author"),
        None,
        "the step has to follow the range's substitution into the revived payload"
    );
}

#[test]
fn undoing_a_revoke_then_the_grant_leaves_no_live_tuple() {
    let mut d = doc(1);
    let author = cid(1);
    let mut id = None;
    d.transact(|c| {
        id = Some(c.acl().grant(
            AclSubject::Actor(cid(7)),
            AclGrant::Capability(Capability::Write),
            AclEffect::Allow,
            p(&[b"doc"]),
            author,
        ));
    });
    let id = id.expect("a tuple id");
    d.transact(|c| c.acl().revoke(id));

    // Re-issues the grant under a fresh tuple id.
    undo(&mut d);
    assert_eq!(d.acl_tuples().len(), 1);

    // Undoing the grant must revoke the tuple that is actually live.
    undo(&mut d);
    assert!(
        d.acl_tuples().is_empty(),
        "undoing everything must not leave a live grant standing"
    );
}

#[test]
fn a_counter_delta_over_a_bare_scalar_slot_records_without_panicking() {
    let mut d = doc(1);
    // A bare map value has no element id at all; asking one for its id panics,
    // and a counter delta over that slot is an ordinary forward edit.
    d.transact(|c| c.set(b"k", Scalar::Int(1)));
    let ops = path::inc(&mut d, &p(&[b"k"]), 1);
    assert!(!ops.is_empty());
    assert_eq!(counter(&d, &p(&[b"k"])), 1);

    undo(&mut d);
    assert!(
        matches!(d.get(b"k"), Some(Element::Scalar(Scalar::Int(1)))),
        "and the scalar the counter displaced comes back"
    );
}

#[test]
fn a_reinstate_copy_is_not_emitted_onto_a_container_a_peer_displaced() {
    let mut a = Document::new(cid(1));
    let mut b = doc(2);
    let nested = p(&[b"a", b"t"]);
    let outer = p(&[b"a"]);
    let setup = path::text_insert(&mut b, &nested, 0, "hi");
    apply_all(&mut a, &setup);

    b.begin_intention();
    let mut edits = path::text_insert(&mut b, &nested, 0, "Z");
    edits.extend(path::inc(&mut b, &nested, 1));
    b.end_intention();
    apply_all(&mut a, &edits);

    // The peer displaces the *outer* map, so the container the undo would want
    // to re-create is itself unreachable.
    let displace = path::register(&mut a, &outer, Scalar::Int(9));
    apply_all(&mut b, &displace);

    let inverse = undo(&mut b);
    apply_all(&mut a, &inverse);
    assert_eq!(observe(&a), observe(&b));

    // Re-installing the outer map must not let a copy this replica applied to
    // nothing fire on the peer.
    let back = path::list_insert(&mut b, &p(&[b"a", b"other"]), 0, b"x");
    apply_all(&mut a, &back);
    assert_eq!(
        observe(&a),
        observe(&b),
        "a container-restoring copy emitted onto nothing here would be buffered \
         by a peer and fire once the container returned"
    );
    assert_eq!(path::text_get(&a, &nested), path::text_get(&b, &nested));
}

// --- a randomized undo/redo convergence property ---

/// A small deterministic PRNG, so a failure names a reproducing seed.
struct Rng(u64);

impl Rng {
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

#[test]
fn a_reorder_redoes_to_the_same_order() {
    for (count, from, to) in [(2usize, 1usize, 0usize), (3, 2, 0), (3, 0, 2)] {
        let mut d = doc(1);
        let root = p(&[b"root"]);
        path::xml_element(&mut d, &root, b"doc");
        for i in 0..count {
            path::xml_insert_text(&mut d, &root, i, &i.to_string());
        }
        let before = child_shapes(&d, b"root");
        path::xml_move_child(&mut d, &root, from, &root, to);
        let moved = child_shapes(&d, b"root");
        assert_ne!(before, moved, "the reorder moved something");

        undo(&mut d);
        assert_eq!(child_shapes(&d, b"root"), before, "undo restores the order");
        redo(&mut d);
        assert_eq!(child_shapes(&d, b"root"), moved, "redo restores it again");
    }
}

/// Undoing every intention must land back on the state the edits started from,
/// and redoing them all must land back on the state they ended at. Convergence
/// alone cannot see this — two replicas lose the same content identically — so
/// this oracle is what catches a replay that drops or reorders a step.
#[test]
fn undo_and_redo_round_trip_to_the_states_they_started_from() {
    const KEYS: &[&[u8]] = &[b"a", b"b", b"c"];
    // Miri interprets every op, so keep its sweep short; a native run covers a
    // far wider band of seeds.
    let seeds = if cfg!(miri) { 4 } else { 300 };
    for seed in 0..seeds {
        let mut rng = Rng(seed ^ 0x9E37_79B9_7F4A_7C15);
        let mut d = doc(1);
        let empty = deep_observe(&d);

        let mut intentions = 0;
        for _ in 0..40 {
            // A group carries several edits — one-edit groups are behaviourally
            // the ungrouped case, and the interesting hazards live in an
            // intention whose later edit displaces a container an earlier one
            // wrote into.
            let edits = if rng.below(3) == 0 {
                2 + rng.below(3)
            } else {
                1
            };
            if edits > 1 {
                d.begin_intention();
            }
            let mut any = false;
            for _ in 0..edits {
                let key = KEYS[rng.below(KEYS.len())];
                any |= !random_edit(&mut d, key, &mut rng, false).is_empty();
            }
            if edits > 1 {
                d.end_intention();
            }
            if any {
                intentions += 1;
            }
        }
        let filled = deep_observe(&d);

        let mut undone = 0;
        while d.undo(ORIGIN).is_some() {
            undone += 1;
        }
        assert_eq!(
            deep_observe(&d),
            empty,
            "seed {seed}: undoing everything must reach the starting state"
        );

        let mut redone = 0;
        while d.redo(ORIGIN).is_some() {
            redone += 1;
        }
        assert_eq!(
            deep_observe(&d),
            filled,
            "seed {seed}: redoing everything must reach the state undo left"
        );
        assert_eq!(
            undone, redone,
            "seed {seed}: every undo owes exactly one redo"
        );
        assert!(
            undone >= intentions.min(1),
            "seed {seed}: nothing was recorded"
        );
    }
}

/// One random edit over `key`, across the op families the seam inverts.
///
/// `moves` admits `xml_move_child`. The convergence oracle takes it; the
/// round-trip oracle does not, because a move's inverse re-derives its anchor
/// from live state at each replay, so after unrelated structural edits to the
/// same sequence its redo can land the node at a different index than the
/// original move did. The node is never lost or duplicated and replicas still
/// agree — it is a positional imprecision, not a correctness break — and the
/// exact round trip of a reorder on its own is pinned by
/// `a_reorder_redoes_to_the_same_order`.
fn random_edit(d: &mut Document, key: &[u8], rng: &mut Rng, moves: bool) -> Vec<Op> {
    // Every third edit addresses a nested slot rather than a root one.
    let path = if rng.below(3) == 0 {
        p(&[key, b"sub"])
    } else {
        p(&[key])
    };
    match rng.below(if moves { 14 } else { 13 }) {
        0 => path::register(d, &path, Scalar::Int(rng.below(4) as i64)),
        1 => path::inc(d, &path, rng.below(3) as u32 + 1),
        2 => path::dec(d, &path, rng.below(3) as u32 + 1),
        3 => path::delete(d, &path),
        4 => path::text_insert(d, &path, 0, "xy"),
        5 => {
            let len = path::text_len(d, &path).unwrap_or(0);
            path::text_delete(d, &path, 0, len.min(2))
        }
        6 => path::list_insert(d, &path, 0, b"i"),
        7 => {
            let len = path::list_len(d, &path).unwrap_or(0);
            if len == 0 {
                Vec::new()
            } else {
                path::list_delete(d, &path, rng.below(len))
            }
        }
        8 => path::xml_element(d, &path, b"p"),
        9 => {
            let n = path::xml_children_len(d, &path).unwrap_or(0);
            if rng.below(2) == 0 || n == 0 {
                path::xml_insert_text(d, &path, n, "z")
            } else {
                path::xml_child_delete(d, &path, rng.below(n))
            }
        }
        10 => {
            // A named mark over the text at this path, if there is any.
            let len = path::text_len(d, &path).unwrap_or(0);
            if len == 0 {
                Vec::new()
            } else {
                path::mark(
                    d,
                    &path,
                    0,
                    Side::Left,
                    len,
                    Side::Right,
                    b"bold",
                    Scalar::Int(rng.below(3) as i64),
                )
                .0
            }
        }
        11 => {
            // Change or delete a live annotation.
            let live: Vec<_> = d.ranged_elements().into_iter().map(|r| r.id).collect();
            if live.is_empty() {
                Vec::new()
            } else {
                let id = live[rng.below(live.len())];
                if rng.below(2) == 0 {
                    d.transact(|c| c.ranged().set_payload(id, Scalar::Int(7)))
                } else {
                    d.transact(|c| c.ranged().delete(id))
                }
            }
        }
        12 => {
            // Grant or revoke an ACL tuple.
            let live: Vec<_> = d.acl_tuples().into_iter().map(|t| t.id).collect();
            let author = d.client();
            if !live.is_empty() && rng.below(2) == 0 {
                let id = live[rng.below(live.len())];
                d.transact(|c| c.acl().revoke(id))
            } else {
                let scope = path.clone();
                d.transact(|c| {
                    c.acl().grant(
                        AclSubject::Actor(author),
                        AclGrant::Capability(Capability::Write),
                        AclEffect::Allow,
                        scope,
                        author,
                    );
                })
            }
        }
        _ => {
            let n = path::xml_children_len(d, &path).unwrap_or(0);
            if n < 2 {
                path::xml_insert_element(d, &path, n, b"q")
            } else {
                path::xml_move_child(d, &path, n - 1, &path, 0)
            }
        }
    }
}

/// `observe`, but rendering each XML subtree to its full depth — a nested
/// element that came back as an empty shell has to be distinguishable from the
/// one that was deleted.
fn deep_observe(d: &Document) -> Vec<String> {
    let mut out = observe(d);
    for key in path::map_keys(d, &p(&[])).unwrap_or_default() {
        out.push(format!("{key:?}=tree:{}", render_tree(d, &key)));
        let nested = p(&[&key, b"sub"]);
        out.push(format!(
            "{key:?}/sub=reg:{:?} ctr:{:?} list:{:?} text:{:?} xml:{:?}",
            path::get_register(d, &nested),
            path::get_counter(d, &nested),
            path::list_len(d, &nested),
            path::text_get(d, &nested),
            path::xml_tag(d, &nested),
        ));
    }
    let mut ranges: Vec<String> = d
        .ranged_elements()
        .into_iter()
        .map(|r| format!("range:{:?}/{:?}", r.name, r.scalar()))
        .collect();
    ranges.sort();
    out.push(format!("ranges:{}", ranges.join("|")));
    let mut acl: Vec<String> = d
        .acl_tuples()
        .into_iter()
        .map(|t| format!("{:?}/{:?}/{:?}/{:?}", t.subject, t.grant, t.effect, t.scope))
        .collect();
    acl.sort();
    out.push(format!("acl:{}", acl.join("|")));
    out.sort();
    out
}

fn render_tree(d: &Document, key: &[u8]) -> String {
    match d.get(key) {
        Some(Element::XmlElement(x)) => {
            let (tag, kids) = {
                let x = x.borrow();
                (String::from_utf8_lossy(x.tag()).into_owned(), x.children())
            };
            format!("<{tag}>{}", render_children(&kids))
        }
        Some(Element::XmlFragment(f)) => format!("<>{}", render_children(&f.borrow().children())),
        _ => String::new(),
    }
}

fn render_children(list: &std::rc::Rc<std::cell::RefCell<crdtsync_core::List>>) -> String {
    let vals = list.borrow().values();
    vals.iter()
        .map(|child| match child {
            Element::XmlElement(x) => {
                let (tag, kids) = {
                    let x = x.borrow();
                    (String::from_utf8_lossy(x.tag()).into_owned(), x.children())
                };
                format!("<{tag}>{}", render_children(&kids))
            }
            Element::Text(t) => format!("{:?}", t.borrow().as_string()),
            other => format!("?{:?}", other.kind()),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Two replicas edit a shared key vocabulary and undo/redo their own
/// intentions, exchanging every op they emit — an inverse included, since an
/// inverse is an ordinary forward op. Whatever the mix, the two must agree.
#[test]
fn undo_and_redo_converge_under_concurrent_editing() {
    const KEYS: &[&[u8]] = &[b"a", b"b", b"c"];
    // Two replicas plus shuffled delivery makes each seed heavier than the
    // round trip's, so Miri takes fewer still.
    let seeds = if cfg!(miri) { 3 } else { 300 };
    for seed in 0..seeds {
        let mut rng = Rng(seed ^ 0x9E37_79B9_7F4A_7C15);
        let mut docs = [doc(1), doc(2)];
        // Ops each replica has emitted but the other has not yet seen, so a
        // burst of concurrent edits lands out of order.
        let mut inflight: [Vec<Op>; 2] = [Vec::new(), Vec::new()];

        for _ in 0..80 {
            let who = rng.below(2);
            let key = KEYS[rng.below(KEYS.len())];
            let ops = match rng.below(13) {
                11 => docs[who].undo(ORIGIN).unwrap_or_default(),
                12 => docs[who].redo(ORIGIN).unwrap_or_default(),
                _ => {
                    // Re-roll inside the shared generator, which covers the same
                    // families plus tree moves.
                    let mut inner = Rng(rng.next());
                    random_edit(&mut docs[who], key, &mut inner, true)
                }
            };
            inflight[who].extend(ops);
            // Deliver a random prefix of the other side's backlog.
            if !inflight[1 - who].is_empty() {
                let take = rng.below(inflight[1 - who].len() + 1);
                let batch: Vec<Op> = inflight[1 - who].drain(..take).collect();
                apply_all(&mut docs[who], &batch);
            }
        }

        let pending: Vec<Op> = inflight[0].drain(..).collect();
        apply_all(&mut docs[1], &pending);
        let pending: Vec<Op> = inflight[1].drain(..).collect();
        apply_all(&mut docs[0], &pending);
        // Re-deliver everything once more so a buffered op has every chance to
        // resolve before the comparison.
        assert_eq!(
            observe(&docs[0]),
            observe(&docs[1]),
            "seed {seed}: replicas diverged"
        );
    }
}

#[test]
fn a_migration_drops_the_stack_but_keeps_recording() {
    let mut d = doc(1);
    path::register(&mut d, &p(&[b"old"]), Scalar::Int(1));
    assert!(d.can_undo(ORIGIN));

    // A rename rewrites the slot shape the recorded inverse names.
    assert!(d.migrate_leaf_slots(|key| if key == b"old" {
        SlotFate::Rename(b"new".to_vec())
    } else {
        SlotFate::Keep
    }));
    assert_eq!(path::get_register(&d, &p(&[b"new"])), Some(Scalar::Int(1)));
    assert!(
        !d.can_undo(ORIGIN),
        "the stack drops at the migration boundary"
    );

    path::register(&mut d, &p(&[b"new"]), Scalar::Int(2));
    assert!(d.can_undo(ORIGIN), "and recording continues past it");
    undo(&mut d);
    assert_eq!(path::get_register(&d, &p(&[b"new"])), Some(Scalar::Int(1)));
}

#[test]
fn a_channel_keeps_recording_across_a_snapshot_catch_up() {
    let mut author = Document::new(cid(9));
    path::register(&mut author, &p(&[b"seed"]), Scalar::Int(1));

    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    s.set_undo_origin(ch, ORIGIN);
    s.edit(ch, |c| c.register(b"k", Scalar::Int(1)));
    assert!(s.can_undo(ch, ORIGIN));

    s.receive(Message::Snapshot {
        channel: ch,
        seq: 1,
        state: author.encode_state(),
    })
    .unwrap();
    assert_eq!(
        channel_doc(&s, ch).undo_origin(),
        Some(ORIGIN),
        "adopting the server's state must not silently stop recording"
    );

    s.edit(ch, |c| c.register(b"after", Scalar::Int(2)));
    assert!(s.can_undo(ch, ORIGIN));
    s.undo(ch, ORIGIN).expect("the post-snapshot edit undoes");
    assert_eq!(
        path::get_register(channel_doc(&s, ch), &p(&[b"after"])),
        None
    );
}

// --- the UndoManager handle ---

#[test]
fn the_manager_tracks_and_undoes_over_its_own_origin() {
    let mut d = Document::new(cid(1));
    let u = UndoManager::with_origin(b"editor");
    u.track(&mut d);
    path::register(&mut d, &p(&[b"k"]), Scalar::Int(1));
    assert!(u.can_undo(&d));

    u.undo(&mut d).expect("an intention");
    assert_eq!(reg(&d, &p(&[b"k"])), None);
    assert!(u.can_redo(&d));
    u.redo(&mut d).expect("an undone intention");
    assert_eq!(reg(&d, &p(&[b"k"])), Some(Scalar::Int(1)));
}

#[test]
fn the_managers_group_records_one_step() {
    let mut d = Document::new(cid(1));
    let u = UndoManager::new();
    let ops = u.group(&mut d, |d| {
        let mut ops = path::register(d, &p(&[b"a"]), Scalar::Int(1));
        ops.extend(path::register(d, &p(&[b"b"]), Scalar::Int(2)));
        ops
    });
    assert_eq!(ops.len(), 2);

    u.undo(&mut d).expect("the group");
    assert_eq!(reg(&d, &p(&[b"a"])), None);
    assert_eq!(reg(&d, &p(&[b"b"])), None);
    assert!(!u.can_undo(&d));
}

#[test]
fn the_managers_atomic_group_ships_one_transaction() {
    let mut d = Document::new(cid(1));
    let u = UndoManager::new();
    let ops = u.atomic_group(&mut d, |d| {
        path::register(d, &p(&[b"a"]), Scalar::Int(1));
        path::register(d, &p(&[b"b"]), Scalar::Int(2));
    });
    assert_eq!(ops.len(), 2);
    assert!(ops.iter().all(|o| o.tx.is_some()));

    let undone = u.undo(&mut d).expect("the group");
    assert!(undone.iter().all(|o| o.tx.is_some()));
    assert_eq!(reg(&d, &p(&[b"a"])), None);
}

#[test]
fn untracking_stops_recording_but_keeps_the_stack() {
    let mut d = Document::new(cid(1));
    let u = UndoManager::new();
    u.track(&mut d);
    path::register(&mut d, &p(&[b"a"]), Scalar::Int(1));
    u.untrack(&mut d);
    path::register(&mut d, &p(&[b"b"]), Scalar::Int(2));

    assert_eq!(d.undo_origin(), None);
    u.undo(&mut d).expect("the recorded edit");
    assert_eq!(reg(&d, &p(&[b"a"])), None);
    assert_eq!(
        reg(&d, &p(&[b"b"])),
        Some(Scalar::Int(2)),
        "the untracked edit was never recorded"
    );
}
