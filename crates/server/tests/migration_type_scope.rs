//! Type-scope applicability filter for migration field steps.
//!
//! A migration field step (`renameField` / `removeField` / `addField`) is
//! declared for a schema *type*, but the per-op key rewrite acts purely on the
//! slot key — so, unnarrowed, a rename of `title` on type `Note` would also
//! rewrite a `title` slot on an unrelated type `Task`, correct only while field
//! names are globally unique. The fan-out narrows a field step to the ops whose
//! owning element (an op's target map) is of the step's declared type, resolving
//! the type through the room-document projection. The snapshot seam narrows the
//! same way over the same tree, so an op-served joiner and a snapshot-served one
//! converge. An unresolvable owning element falls back to the key-based rewrite.

use crdtsync_core::schema::Schema;
use crdtsync_core::{ClientId, Document, Element, ElementId, Op, OpId, OpKind, Scalar, Stamp};
use crdtsync_server::index::element_types;
use crdtsync_server::schema_registry::SchemaRegistry;
use crdtsync_server::translate::{resolve_chain, translate_snapshot_scoped};

const APP: &[u8] = b"app";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// Schema `v`: `Note` and `Task` both hold a `title`, until a rename moves
/// `Note`'s field. Field types are positionally resolved, so the map elements
/// type the same way at every version regardless of the renamed slot.
fn schema_src(note_field: &str) -> String {
    format!(
        r#"{{ "schema": "s", "version": 1, "root": "Doc", "types": {{
            "Doc": {{ "kind": "map", "children": {{ "note": "Note", "task": "Task" }} }},
            "Note": {{ "kind": "map", "children": {{ "{note_field}": "Str" }} }},
            "Task": {{ "kind": "map", "children": {{ "title": "Str" }} }},
            "Str": {{ "kind": "register" }} }} }}"#
    )
}

fn schema(note_field: &str) -> Schema {
    Schema::parse(&schema_src(note_field)).expect("schema parses")
}

/// A registry whose `APP` chain carries `edges[0]` as the 1→2 migration, etc.
/// The schema body registered at each version is opaque to translation.
fn registry_with(edges: &[&str]) -> SchemaRegistry {
    let mut reg = SchemaRegistry::new();
    reg.register(APP, 1, b"{}", b"").unwrap();
    for (i, e) in edges.iter().enumerate() {
        reg.register(APP, (i + 2) as u32, b"{}", e.as_bytes())
            .unwrap();
    }
    reg
}

fn rename_note(to_version: u32, from: &str, to: &str) -> String {
    format!(
        r#"{{ "from": {}, "to": {to_version}, "steps": [
            {{ "kind": "renameField", "type": "Note", "from": "{from}", "to": "{to}" }} ] }}"#,
        to_version - 1
    )
}

/// A document with `note.title` and `task.title`, and the ops that authored it.
fn note_and_task() -> (Document, Vec<Op>) {
    let mut w = Document::new(cid(1));
    let ops = w.transact(|tx| {
        tx.map(b"note").register(b"title", Scalar::Int(1));
        tx.map(b"task").register(b"title", Scalar::Int(2));
    });
    (w, ops)
}

/// The Int behind `outer.inner`, or `None` when either level is absent.
fn nested_int(d: &Document, outer: &[u8], inner: &[u8]) -> Option<i64> {
    let m = match d.get(outer)? {
        Element::Map(m) => m,
        _ => return None,
    };
    let child = m.borrow().get(inner)?;
    match child {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => None,
        },
        _ => None,
    }
}

/// A fresh document with `ops` folded in.
fn apply(ops: &[Op]) -> Document {
    let mut d = Document::new(cid(2));
    for op in ops {
        d.apply(op);
    }
    d
}

/// The `(note.heading, note.title, task.heading, task.title)` reading — the four
/// slots a `Note`-scoped rename of `title`→`heading` moves or leaves.
fn reading(d: &Document) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
    (
        nested_int(d, b"note", b"heading"),
        nested_int(d, b"note", b"title"),
        nested_int(d, b"task", b"heading"),
        nested_int(d, b"task", b"title"),
    )
}

#[test]
fn a_type_scoped_rename_narrows_to_its_type() {
    // 1→2 renames Note.title→heading; a v2 recipient sees Note's slot re-keyed but
    // Task's same-named slot untouched — the core bug the narrowing fixes.
    let reg = registry_with(&[&rename_note(2, "title", "heading")]);
    let chain = resolve_chain(&reg, APP, 1, 2).unwrap();
    let (w, ops) = note_and_task();
    let types = element_types(&w, &schema("heading"));

    let out = chain.translate_ops_scoped(&ops, &types);
    let d = apply(&out);
    assert_eq!(
        reading(&d),
        (Some(1), None, None, Some(2)),
        "Note.title re-keys to heading; Task.title is left untouched"
    );
}

#[test]
fn no_schema_rewrites_by_key_as_before() {
    // With no owning-type projection every field step acts by key: both maps'
    // `title` re-keys — the field-name-unique fallback, and the pre-narrowing
    // behaviour translate_ops preserves.
    let reg = registry_with(&[&rename_note(2, "title", "heading")]);
    let chain = resolve_chain(&reg, APP, 1, 2).unwrap();
    let (_, ops) = note_and_task();

    let out = chain.translate_ops_scoped(&ops, &Default::default());
    let d = apply(&out);
    assert_eq!(
        reading(&d),
        (Some(1), None, Some(2), None),
        "with no types both same-named slots re-key by field name"
    );
    // The public key-based entry point is exactly the empty-projection behaviour.
    assert_eq!(reading(&apply(&chain.translate_ops(&ops))), reading(&d));
}

#[test]
fn the_op_seam_and_snapshot_seam_converge() {
    // A joiner served the op delta and one served the cold-start snapshot must
    // reach the same state under a Note-scoped rename: Note re-keyed, Task intact.
    let reg = registry_with(&[&rename_note(2, "title", "heading")]);
    let s = schema("heading");
    let (w, ops) = note_and_task();
    let snapshot = w.encode_state();

    // Op-delta joiner (above the floor): the history down/up-translated per op.
    let chain = resolve_chain(&reg, APP, 1, 2).unwrap();
    let types = element_types(&w, &s);
    let via_delta = apply(&chain.translate_ops_scoped(&ops, &types));

    // Snapshot joiner (below the floor): the whole v1 state up-migrated to v2.
    let projected = translate_snapshot_scoped(&reg, APP, &snapshot, 1, 2, Some(&s));
    let via_snapshot = Document::decode_state(&projected).unwrap();

    assert_eq!(
        reading(&via_delta),
        reading(&via_snapshot),
        "the op-delta joiner and the snapshot joiner converge"
    );
    assert_eq!(
        reading(&via_snapshot),
        (Some(1), None, None, Some(2)),
        "and it is the correct type-scoped projection"
    );
    // The snapshot seam also leaves Task's subtree byte-for-byte: re-encoding the
    // projected state and re-projecting is stable.
    let reprojected = translate_snapshot_scoped(&reg, APP, &projected, 2, 2, Some(&s));
    assert_eq!(
        reprojected, projected,
        "a same-version reprojection is verbatim"
    );
}

#[test]
fn a_chain_narrows_at_each_edge() {
    // Note.title→heading at 1→2, then Note.heading→label at 2→3. A v1 op reaches
    // v3 as Note.label; a Task.title op is inert at both edges and stays title.
    let reg = registry_with(&[
        &rename_note(2, "title", "heading"),
        &rename_note(3, "heading", "label"),
    ]);
    let chain = resolve_chain(&reg, APP, 1, 3).unwrap();
    let (w, ops) = note_and_task();
    let types = element_types(&w, &schema("label"));

    let d = apply(&chain.translate_ops_scoped(&ops, &types));
    assert_eq!(
        nested_int(&d, b"note", b"label"),
        Some(1),
        "Note title→heading→label"
    );
    assert_eq!(nested_int(&d, b"note", b"title"), None);
    assert_eq!(
        nested_int(&d, b"task", b"title"),
        Some(2),
        "Task is inert at each edge"
    );
    assert_eq!(nested_int(&d, b"task", b"label"), None);
}

#[test]
fn an_unresolvable_owning_element_falls_back_to_key() {
    // An op whose target is in no projection (a since-deleted or never-indexed
    // element) has no resolvable owning type, so it rewrites by key rather than
    // panicking or being dropped — the totality guarantee.
    let reg = registry_with(&[&rename_note(2, "title", "heading")]);
    let chain = resolve_chain(&reg, APP, 1, 2).unwrap();
    let orphan = Op::new(
        OpId {
            client: cid(1),
            seq: 1,
        },
        Stamp {
            lamport: 1,
            client: cid(1),
        },
        ElementId::from_bytes([9u8; 16]),
        OpKind::MapSet {
            key: b"title".to_vec(),
            value: Scalar::Int(7),
        },
    );
    // Empty projection: the orphan's owning type is unresolved.
    let out = chain.translate_ops_scoped(std::slice::from_ref(&orphan), &Default::default());
    assert_eq!(out.len(), 1, "the op is neither dropped nor a panic");
    match &out[0].kind {
        OpKind::MapSet { key, .. } => {
            assert_eq!(key, b"heading", "it rewrites by key, the fallback")
        }
        other => panic!("expected a MapSet, got {other:?}"),
    }
}

/// A container field's schema, `foo` renamed to `bar` at the Doc level.
fn container_schema(field: &str) -> Schema {
    Schema::parse(&format!(
        r#"{{ "schema": "s", "version": 1, "root": "Doc", "types": {{
            "Doc": {{ "kind": "map", "children": {{ "{field}": "Inner" }} }},
            "Inner": {{ "kind": "map", "children": {{ "x": "Str" }} }},
            "Str": {{ "kind": "register" }} }} }}"#
    ))
    .expect("schema parses")
}

/// The shared state (container registries, parent links) with the replica-local
/// identity — the client id and the local seq counter, which legitimately differ
/// between a fresh op-applying replica and a decoded snapshot — normalized away.
fn shared_state(d: &Document) -> Vec<u8> {
    Document::decode_state_as(cid(9), 1000, &d.encode_state())
        .expect("re-decodes")
        .encode_state()
}

#[test]
fn a_deleted_container_field_converges_across_seams() {
    // The load-bearing property: a created-then-deleted container field, renamed,
    // reaches BYTE-IDENTICAL state through the op seam and the snapshot seam. The
    // op seam carries the container-create verbatim (resurrecting it live at the
    // old key) and re-keys the delete; the snapshot seam does the same from the
    // persisted create-stamp. On main the two hold the delete's tombstone under
    // different dead keys — this extends the leaf convergence of #274 to containers.
    let edge = r#"{ "from": 1, "to": 2, "steps": [
        { "kind": "renameField", "type": "Doc", "from": "foo", "to": "bar" } ] }"#;
    let mut reg = SchemaRegistry::new();
    reg.register(APP, 1, b"{}", b"").unwrap();
    reg.register(APP, 2, b"{}", edge.as_bytes()).unwrap();
    let s2 = container_schema("bar");

    let mut w = Document::new(cid(1));
    let mut ops = w.transact(|tx| {
        tx.map(b"foo").register(b"x", Scalar::Int(1));
    });
    ops.extend(w.transact(|tx| tx.delete(b"foo")));
    let snapshot = w.encode_state();

    // Op-delta joiner: the writer's client id, so only replica-local sync state
    // (the local seq counter) can differ — normalized by `shared_state`.
    let chain = resolve_chain(&reg, APP, 1, 2).unwrap();
    let types = element_types(&w, &s2);
    let mut via_delta = Document::new(cid(1));
    for op in chain.translate_ops_scoped(&ops, &types) {
        via_delta.apply(&op);
    }

    // Snapshot joiner: the whole v1 state up-migrated to v2.
    let projected = translate_snapshot_scoped(&reg, APP, &snapshot, 1, 2, Some(&s2));
    let via_snapshot = Document::decode_state(&projected).unwrap();

    // Value-level: the container resurrects live at the old key on both seams, and
    // nothing lands live at the new key (the delete re-keyed there).
    assert!(matches!(via_delta.get(b"foo"), Some(Element::Map(_))));
    assert!(matches!(via_snapshot.get(b"foo"), Some(Element::Map(_))));
    assert!(via_delta.get(b"bar").is_none());
    assert!(via_snapshot.get(b"bar").is_none());

    // Byte-level: the shared state is identical.
    assert_eq!(
        shared_state(&via_delta),
        shared_state(&via_snapshot),
        "the op seam and the snapshot seam reach byte-identical shared state"
    );
}
