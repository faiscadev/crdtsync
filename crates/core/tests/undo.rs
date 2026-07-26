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
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
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

fn undo(d: &mut Document) -> Vec<Op> {
    d.undo(ORIGIN).expect("an intention to undo")
}

fn redo(d: &mut Document) -> Vec<Op> {
    d.redo(ORIGIN).expect("an intention to redo")
}

/// Everything a fixture document holds, read back through the public surface —
/// the fingerprint two replicas must agree on once they have seen the same ops.
fn observe(d: &Document) -> Vec<String> {
    let mut out = Vec::new();
    for key in path::map_keys(d, &p(&[])).unwrap_or_default() {
        let path = p(&[&key]);
        out.push(format!(
            "{key:?}=reg:{:?} ctr:{:?} list:{:?} text:{:?} xml:{:?}/{:?}",
            reg(d, &path),
            path::get_counter(d, &path),
            path::list_len(d, &path).map(|_| list_vals(d, &path)),
            path::text_get(d, &path),
            path::xml_tag(d, &path),
            path::xml_children_len(d, &path),
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

    d.undo(ORIGIN);
    d.undo(ORIGIN);
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
    d.transact(|c| c.acl().revoke(id));
    assert!(d.acl_tuple(id).is_none());

    undo(&mut d);
    let live = d.acl_tuples();
    assert_eq!(live.len(), 1, "an equivalent grant is re-issued");
    assert_eq!(live[0].subject, AclSubject::Actor(cid(7)));
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
    d.begin_intention();
    d.end_intention();
    assert!(!d.can_undo(ORIGIN));

    d.transact(|_| {});
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

    // Undo every intention, then redo them all; the peer only ever sees ops.
    let mut undone = 0;
    while let Some(ops) = a.undo(ORIGIN) {
        ship(ops, &mut peer);
        undone += 1;
    }
    assert_eq!(undone, 6);
    assert_eq!(observe(&a), observe(&peer));

    while let Some(ops) = a.redo(ORIGIN) {
        ship(ops, &mut peer);
    }
    assert_eq!(observe(&a), observe(&peer));
    assert_eq!(text(&a, &p(&[b"body"])), "hello");
    assert_eq!(counter(&a, &p(&[b"votes"])), 3);
    assert_eq!(list_vals(&a, &p(&[b"items"])), vec![b"x".to_vec()]);
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
    s.edit(ch, |c| c.register(b"title", Scalar::Int(1)));
    s.edit(ch, |c| c.register(b"title", Scalar::Int(2)));
    assert!(s.can_undo(ch, ORIGIN));

    let ops = ops_of(s.undo(ch, ORIGIN).expect("an intention to undo"));
    assert!(!ops.is_empty());
    assert_eq!(
        path::get_register(channel_doc(&s, ch), &p(&[b"title"])),
        Some(Scalar::Int(1))
    );

    let mut peer = Document::new(cid(2));
    for op in &ops {
        peer.apply(op);
    }
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
