//! Per-recipient migration translation — the engine the fan-out seam (live
//! delta) and cold-start snapshot drive. Resolves an app's registered migration
//! chain into the edge slice between two schema versions and rewrites one op
//! from its creation version to a recipient's version: identity when the
//! versions match, forward (up) when the recipient is newer, inverse (down)
//! when older, and refuses a down across a breaking (forward-only) gap.

use crdtsync_core::migration::OpRewrite;
use crdtsync_core::{ClientId, ElementId, Op, OpId, OpKind, Scalar, Stamp};
use crdtsync_server::schema_registry::SchemaRegistry;
use crdtsync_server::translate::{reachable, translate_op, translate_ops, TranslateError};

const APP: &[u8] = b"app";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A RegisterSet op addressing `key`. Identity fields are fixed so a rewrite
/// that only touches the key compares equal to the same op re-keyed.
fn set(key: &str) -> Op {
    Op::new(
        OpId {
            client: cid(1),
            seq: 1,
        },
        Stamp {
            lamport: 1,
            client: cid(1),
        },
        ElementId::from_bytes([0u8; 16]),
        OpKind::RegisterSet {
            key: key.as_bytes().to_vec(),
            value: Scalar::Int(30),
        },
    )
}

/// The migration edge reaching `to` (from `to - 1`) carrying a single step.
fn edge(to: u32, step: &str) -> String {
    format!(
        r#"{{ "from": {}, "to": {to}, "steps": [ {step} ] }}"#,
        to - 1
    )
}

/// A registry whose app is a chain of the given per-version migration bodies:
/// `edges[0]` is the 1->2 edge, `edges[1]` the 2->3 edge, and so on. Version 1
/// has no predecessor, so it carries an empty edge. The schema body is opaque
/// to translation, so any bytes suffice.
fn registry_with(edges: &[&str]) -> SchemaRegistry {
    let mut reg = SchemaRegistry::new();
    reg.register(APP, 1, b"{}", b"").unwrap();
    for (i, e) in edges.iter().enumerate() {
        reg.register(APP, (i + 2) as u32, b"{}", e.as_bytes())
            .unwrap();
    }
    reg
}

#[test]
fn identity_when_the_versions_match() {
    // No edge is even consulted when source and target are the same version.
    let reg = registry_with(&[]);
    assert_eq!(
        translate_op(&reg, APP, &set("age"), 1, 1),
        Ok(OpRewrite::Keep(set("age")))
    );
}

#[test]
fn up_renames_a_field_forward() {
    // 1->2 renames age to years; an op created at v1 reaches a v2 recipient with
    // its key rewritten. A key touched by no step passes through unchanged.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "renameField", "type": "m", "from": "age", "to": "years" }"#,
    )]);
    assert_eq!(
        translate_op(&reg, APP, &set("age"), 1, 2),
        Ok(OpRewrite::Keep(set("years")))
    );
    assert_eq!(
        translate_op(&reg, APP, &set("name"), 1, 2),
        Ok(OpRewrite::Keep(set("name")))
    );
}

#[test]
fn up_composes_a_multi_edge_chain() {
    // rename a->b at 1->2, then b->c at 2->3: an op created at v1 reaches v3 as c.
    let reg = registry_with(&[
        &edge(
            2,
            r#"{ "kind": "renameField", "type": "m", "from": "a", "to": "b" }"#,
        ),
        &edge(
            3,
            r#"{ "kind": "renameField", "type": "m", "from": "b", "to": "c" }"#,
        ),
    ]);
    assert_eq!(
        translate_op(&reg, APP, &set("a"), 1, 3),
        Ok(OpRewrite::Keep(set("c")))
    );
}

#[test]
fn up_drops_an_op_on_a_removed_field() {
    // 1->2 removes the "secret" slot; an op addressing it has no image forward.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "removeField", "type": "m", "field": "secret" }"#,
    )]);
    assert_eq!(
        translate_op(&reg, APP, &set("secret"), 1, 2),
        Ok(OpRewrite::Drop)
    );
}

#[test]
fn down_inverts_a_back_compatible_edge() {
    // 1->2 adds the "note" slot (back-compatible). Serving a v2 op down to a v1
    // recipient drops the op that references the addition; an op on a
    // pre-existing slot survives unchanged.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "addField", "type": "m", "field": "note", "fieldType": "text" }"#,
    )]);
    assert_eq!(
        translate_op(&reg, APP, &set("note"), 2, 1),
        Ok(OpRewrite::Drop)
    );
    assert_eq!(
        translate_op(&reg, APP, &set("title"), 2, 1),
        Ok(OpRewrite::Keep(set("title")))
    );
}

#[test]
fn down_composes_a_back_compatible_chain() {
    // Two back-compatible adds: a v3 op reaches a v1 recipient, dropping the ops
    // on either addition and keeping the rest.
    let reg = registry_with(&[
        &edge(
            2,
            r#"{ "kind": "addField", "type": "m", "field": "note", "fieldType": "text" }"#,
        ),
        &edge(
            3,
            r#"{ "kind": "addField", "type": "m", "field": "tag", "fieldType": "text" }"#,
        ),
    ]);
    assert_eq!(
        translate_op(&reg, APP, &set("tag"), 3, 1),
        Ok(OpRewrite::Drop)
    );
    assert_eq!(
        translate_op(&reg, APP, &set("note"), 3, 1),
        Ok(OpRewrite::Drop)
    );
    assert_eq!(
        translate_op(&reg, APP, &set("title"), 3, 1),
        Ok(OpRewrite::Keep(set("title")))
    );
}

#[test]
fn down_across_a_breaking_edge_is_unreachable() {
    // 1->2 removes a required slot (breaking, no inverse): a v2 op cannot be
    // served down to a v1 recipient — refused, never served a corrupt op.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "removeField", "type": "m", "field": "old" }"#,
    )]);
    assert_eq!(
        translate_op(&reg, APP, &set("x"), 2, 1),
        Err(TranslateError::Unreachable)
    );
    assert_eq!(reachable(&reg, APP, 2, 1), Ok(false));
}

#[test]
fn down_is_unreachable_if_any_edge_on_the_path_breaks() {
    // A back-compatible add above a breaking remove: the whole down path is
    // unreachable even for an op the upper edge would otherwise drop.
    let reg = registry_with(&[
        &edge(
            2,
            r#"{ "kind": "removeField", "type": "m", "field": "old" }"#,
        ),
        &edge(
            3,
            r#"{ "kind": "addField", "type": "m", "field": "note", "fieldType": "text" }"#,
        ),
    ]);
    assert_eq!(reachable(&reg, APP, 3, 1), Ok(false));
    assert_eq!(
        translate_op(&reg, APP, &set("note"), 3, 1),
        Err(TranslateError::Unreachable)
    );
}

#[test]
fn reachability_is_direction_aware() {
    // A single back-compatible edge: reachable both ways. Forward is always
    // reachable; down here because the edge inverts.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "addField", "type": "m", "field": "note", "fieldType": "text" }"#,
    )]);
    assert_eq!(reachable(&reg, APP, 1, 2), Ok(true));
    assert_eq!(reachable(&reg, APP, 2, 1), Ok(true));
    assert_eq!(reachable(&reg, APP, 2, 2), Ok(true));
}

#[test]
fn a_version_past_the_head_is_a_missing_edge() {
    // Only version 1 is registered; reaching version 2 has no edge.
    let reg = registry_with(&[]);
    assert_eq!(
        translate_op(&reg, APP, &set("age"), 1, 2),
        Err(TranslateError::MissingEdge { version: 2 })
    );
    assert_eq!(
        reachable(&reg, APP, 1, 2),
        Err(TranslateError::MissingEdge { version: 2 })
    );
}

#[test]
fn an_unknown_app_has_no_edge() {
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "addField", "type": "m", "field": "note", "fieldType": "text" }"#,
    )]);
    assert_eq!(
        translate_op(&reg, b"other", &set("age"), 1, 2),
        Err(TranslateError::MissingEdge { version: 2 })
    );
}

#[test]
fn translate_ops_keeps_the_translatable_and_drops_the_rest() {
    // 1->2 renames age->years and removes secret. Up, a batch of [age, secret,
    // name] yields [years, name]: age renamed, secret dropped, name untouched.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "renameField", "type": "m", "from": "age", "to": "years" }, { "kind": "removeField", "type": "m", "field": "secret" }"#,
    )]);
    let batch = [set("age"), set("secret"), set("name")];
    let out = translate_ops(&reg, APP, &batch, 1, 2);
    assert_eq!(out, vec![set("years"), set("name")]);
}

#[test]
fn translate_ops_drops_an_op_it_cannot_reach() {
    // A breaking down edge: every op is dropped rather than served wrong.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "removeField", "type": "m", "field": "old" }"#,
    )]);
    let batch = [set("a"), set("b")];
    assert!(translate_ops(&reg, APP, &batch, 2, 1).is_empty());
}

#[test]
fn unparseable_edge_bytes_are_a_bad_migration() {
    // Version 1 registers cleanly; version 2's migration body is garbage.
    let reg = registry_with(&["not json"]);
    assert_eq!(
        translate_op(&reg, APP, &set("age"), 1, 2),
        Err(TranslateError::BadMigration { version: 2 })
    );
}

#[test]
fn a_version_match_at_the_top_of_the_space_does_not_overflow() {
    // A same-version op/recipient at u32::MAX is a no-op that must never compute
    // an edge boundary past the version space.
    let reg = registry_with(&[]);
    let top = u32::MAX;
    assert_eq!(
        translate_op(&reg, APP, &set("age"), top, top),
        Ok(OpRewrite::Keep(set("age")))
    );
    assert_eq!(reachable(&reg, APP, top, top), Ok(true));
}

#[test]
fn the_edge_walk_runs_to_the_top_of_the_space_without_overflowing() {
    // A one-version step ending at u32::MAX drives edge_slice's increment loop
    // right up to the boundary (not the identity shortcut). The edge is
    // unregistered, so it surfaces as MissingEdge at the top version — the walk
    // must reach it, never overflow computing the range.
    let reg = registry_with(&[]);
    let top = u32::MAX;
    assert_eq!(
        translate_op(&reg, APP, &set("age"), top - 1, top),
        Err(TranslateError::MissingEdge { version: top })
    );
    assert_eq!(
        reachable(&reg, APP, top, top - 1),
        Err(TranslateError::MissingEdge { version: top })
    );
}

#[test]
fn a_down_path_reports_a_bad_edge() {
    // Error classification holds on the inverse (to < from) direction too: the
    // down slice edge_slice(to, from) walks version 2, whose body is garbage.
    let reg = registry_with(&["not json"]);
    assert_eq!(
        translate_op(&reg, APP, &set("x"), 2, 1),
        Err(TranslateError::BadMigration { version: 2 })
    );
    assert_eq!(
        reachable(&reg, APP, 2, 1),
        Err(TranslateError::BadMigration { version: 2 })
    );
}

#[test]
fn a_down_path_reports_a_missing_edge_past_the_head() {
    // A well-formed v1->v2 chain; a v3 op down to v1 walks past the head, so
    // version 3 has no edge — surfaced on the inverse direction.
    let reg = registry_with(&[&edge(
        2,
        r#"{ "kind": "addField", "type": "m", "field": "note", "fieldType": "text" }"#,
    )]);
    assert_eq!(
        translate_op(&reg, APP, &set("x"), 3, 1),
        Err(TranslateError::MissingEdge { version: 3 })
    );
    assert_eq!(
        reachable(&reg, APP, 3, 1),
        Err(TranslateError::MissingEdge { version: 3 })
    );
}
