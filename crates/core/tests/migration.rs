//! The migration model + its parse-time validation — the spec.
//!
//! A migration is one edge of an app's schema chain: the transform reaching
//! version `to` from `from` (a single contiguous step, `to == from + 1`). It is
//! a JSON file (parsed by `core::json`) carrying an ordered list of structural
//! `steps` over the built primitives — add / remove / rename a named type or a
//! map field. Parsing is total — every input yields a `Migration` or a
//! `MigrationError`, never a panic — and validates the envelope at parse time
//! (contiguous versions, well-formed step params, non-empty names, no unknown
//! keys). Each step carries a compatibility class (back-compatible vs breaking)
//! and a per-op rewrite (forward always, backward when back-compatible).

use crdtsync_core::migration::{Compat, Migration, MigrationErrorKind, OpRewrite, Step};
use crdtsync_core::op::{Op, OpId, OpKind, Tx, TxId};
use crdtsync_core::schema::TypeDef;
use crdtsync_core::{Anchor, Scalar, Side};

mod common;
use common::{cid, eid, stmp};

fn parse(s: &str) -> Migration {
    Migration::parse(s).unwrap_or_else(|e| panic!("parse of migration failed: {e:?}\n{s}"))
}

fn kind(s: &str) -> MigrationErrorKind {
    Migration::parse(s)
        .expect_err(&format!("expected a migration error, parsed:\n{s}"))
        .kind
}

#[test]
fn an_empty_edge_carries_its_versions_and_no_steps() {
    let m = parse(r#"{ "from": 1, "to": 2, "steps": [] }"#);
    assert_eq!(m.from, 1);
    assert_eq!(m.to, 2);
    assert!(m.steps().is_empty());
}

#[test]
fn add_type_round_trips_with_its_type_def() {
    let m = parse(
        r#"{ "from": 3, "to": 4, "steps": [
            { "kind": "addType", "name": "tag", "def": { "kind": "register" } }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::AddType { name, def } => {
            assert_eq!(name, "tag");
            assert_eq!(
                *def,
                TypeDef::Register {
                    min: None,
                    max: None
                }
            );
        }
        other => panic!("expected AddType, got {other:?}"),
    }
}

#[test]
fn add_type_accepts_a_map_def_with_children() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "addType", "name": "todo",
              "def": { "kind": "map", "children": { "title": "text", "done": "flag" } } }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::AddType {
            def: TypeDef::Map { children },
            ..
        } => {
            assert_eq!(
                children,
                &[
                    ("title".to_string(), "text".to_string()),
                    ("done".to_string(), "flag".to_string()),
                ]
            );
        }
        other => panic!("expected AddType map, got {other:?}"),
    }
}

#[test]
fn remove_type_round_trips() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "removeType", "name": "obsolete" }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::RemoveType { name } => assert_eq!(name, "obsolete"),
        other => panic!("expected RemoveType, got {other:?}"),
    }
}

#[test]
fn rename_type_round_trips() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "renameType", "from": "task", "to": "todo" }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::RenameType { from, to } => {
            assert_eq!(from, "task");
            assert_eq!(to, "todo");
        }
        other => panic!("expected RenameType, got {other:?}"),
    }
}

#[test]
fn add_field_round_trips() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "addField", "type": "todo", "field": "priority", "fieldType": "register" }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::AddField {
            ty,
            field,
            field_type,
        } => {
            assert_eq!(ty, "todo");
            assert_eq!(field, "priority");
            assert_eq!(field_type, "register");
        }
        other => panic!("expected AddField, got {other:?}"),
    }
}

#[test]
fn remove_field_round_trips() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "removeField", "type": "todo", "field": "legacy" }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::RemoveField { ty, field } => {
            assert_eq!(ty, "todo");
            assert_eq!(field, "legacy");
        }
        other => panic!("expected RemoveField, got {other:?}"),
    }
}

#[test]
fn rename_field_round_trips() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "renameField", "type": "todo", "from": "done", "to": "completed" }
        ] }"#,
    );
    match &m.steps()[0] {
        Step::RenameField { ty, from, to } => {
            assert_eq!(ty, "todo");
            assert_eq!(from, "done");
            assert_eq!(to, "completed");
        }
        other => panic!("expected RenameField, got {other:?}"),
    }
}

#[test]
fn a_multi_step_edge_preserves_step_order() {
    let m = parse(
        r#"{ "from": 2, "to": 3, "steps": [
            { "kind": "addType", "name": "tag", "def": { "kind": "text" } },
            { "kind": "addField", "type": "todo", "field": "tag", "fieldType": "tag" },
            { "kind": "renameField", "type": "todo", "from": "done", "to": "completed" },
            { "kind": "removeField", "type": "todo", "field": "legacy" }
        ] }"#,
    );
    assert_eq!(m.steps().len(), 4);
    assert!(matches!(m.steps()[0], Step::AddType { .. }));
    assert!(matches!(m.steps()[1], Step::AddField { .. }));
    assert!(matches!(m.steps()[2], Step::RenameField { .. }));
    assert!(matches!(
        m.steps()[3],
        Step::RemoveType { .. } | Step::RemoveField { .. }
    ));
}

#[test]
fn a_non_contiguous_edge_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 3, "steps": [] }"#),
        MigrationErrorKind::NonContiguous
    );
    // A backward edge is non-contiguous too.
    assert_eq!(
        kind(r#"{ "from": 5, "to": 4, "steps": [] }"#),
        MigrationErrorKind::NonContiguous
    );
    // An edge to itself.
    assert_eq!(
        kind(r#"{ "from": 2, "to": 2, "steps": [] }"#),
        MigrationErrorKind::NonContiguous
    );
}

#[test]
fn a_from_of_zero_is_rejected_the_chain_starts_at_one() {
    // Version 1 has no predecessor to migrate from, so the lowest edge is 1->2.
    assert_eq!(
        kind(r#"{ "from": 0, "to": 1, "steps": [] }"#),
        MigrationErrorKind::BadVersion
    );
}

#[test]
fn a_from_at_the_top_of_the_version_space_does_not_overflow() {
    // `from` == u32::MAX is a valid u32 with no in-range successor: the
    // contiguity check must reject it, not panic on `from + 1`.
    assert_eq!(
        kind(r#"{ "from": 4294967295, "to": 4294967295, "steps": [] }"#),
        MigrationErrorKind::NonContiguous
    );
}

#[test]
fn an_unknown_step_kind_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [ { "kind": "wrap", "type": "todo" } ] }"#),
        MigrationErrorKind::UnknownStepKind
    );
}

#[test]
fn an_unknown_top_level_key_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [], "extra": true }"#),
        MigrationErrorKind::UnknownField
    );
}

#[test]
fn an_unknown_step_key_is_rejected() {
    assert_eq!(
        kind(
            r#"{ "from": 1, "to": 2, "steps": [
                { "kind": "removeType", "name": "x", "typo": 1 }
            ] }"#
        ),
        MigrationErrorKind::UnknownField
    );
}

#[test]
fn a_missing_step_field_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [ { "kind": "removeType" } ] }"#),
        MigrationErrorKind::MissingField
    );
}

#[test]
fn a_missing_envelope_field_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "steps": [] }"#),
        MigrationErrorKind::MissingField
    );
}

#[test]
fn a_wrong_typed_field_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [ { "kind": "removeType", "name": 7 } ] }"#),
        MigrationErrorKind::WrongType
    );
    // A version that is not an integer.
    assert_eq!(
        kind(r#"{ "from": "one", "to": 2, "steps": [] }"#),
        MigrationErrorKind::WrongType
    );
    // `steps` that is not an array.
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": {} }"#),
        MigrationErrorKind::WrongType
    );
}

#[test]
fn an_empty_name_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [ { "kind": "removeType", "name": "" } ] }"#),
        MigrationErrorKind::EmptyName
    );
    assert_eq!(
        kind(
            r#"{ "from": 1, "to": 2, "steps": [
                { "kind": "renameField", "type": "todo", "from": "done", "to": "" }
            ] }"#
        ),
        MigrationErrorKind::EmptyName
    );
}

#[test]
fn a_bad_type_def_in_add_type_is_rejected() {
    // The type-def body is validated by the shared schema type-def parser, so an
    // unknown kind there surfaces as a migration error, not a panic — located at
    // this step's `def`, like every other step-level error.
    let err = Migration::parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "removeType", "name": "old" },
            { "kind": "addType", "name": "x", "def": { "kind": "nonsense" } }
        ] }"#,
    )
    .expect_err("expected a type-def error");
    assert!(matches!(err.kind, MigrationErrorKind::TypeDef(_)));
    assert!(
        err.at.starts_with("steps[1].def"),
        "type-def error located at {:?}, want a steps[1].def prefix",
        err.at
    );
}

#[test]
fn an_unknown_key_inside_an_add_type_def_is_rejected() {
    // Fail-loud extends into the nested type-def: a stray key there is rejected
    // by the shared schema parser, surfacing as a TypeDef error.
    assert!(matches!(
        kind(
            r#"{ "from": 1, "to": 2, "steps": [
                { "kind": "addType", "name": "x", "def": { "kind": "register", "bogus": 1 } }
            ] }"#
        ),
        MigrationErrorKind::TypeDef(_)
    ));
}

#[test]
fn a_step_that_is_not_an_object_is_rejected() {
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [ 3 ] }"#),
        MigrationErrorKind::NotAnObject
    );
    assert_eq!(
        kind(r#"{ "from": 1, "to": 2, "steps": [ null ] }"#),
        MigrationErrorKind::NotAnObject
    );
}

#[test]
fn malformed_json_is_a_json_error_not_a_panic() {
    assert!(matches!(
        kind(r#"{ "from": 1, "to": 2, "steps": ["#),
        MigrationErrorKind::Json(_)
    ));
}

#[test]
fn the_document_must_be_an_object() {
    assert_eq!(kind("[1, 2, 3]"), MigrationErrorKind::NotAnObject);
}

// --- compatibility classification ---

fn add_type() -> Step {
    Step::AddType {
        name: "tag".into(),
        def: TypeDef::Text { max: None },
    }
}

fn add_field() -> Step {
    Step::AddField {
        ty: "todo".into(),
        field: "priority".into(),
        field_type: "register".into(),
    }
}

fn remove_field() -> Step {
    Step::RemoveField {
        ty: "todo".into(),
        field: "legacy".into(),
    }
}

fn rename_field() -> Step {
    Step::RenameField {
        ty: "todo".into(),
        from: "done".into(),
        to: "completed".into(),
    }
}

#[test]
fn additive_steps_are_back_compatible() {
    assert_eq!(add_type().compat(), Compat::BackCompatible);
    assert_eq!(add_field().compat(), Compat::BackCompatible);
}

#[test]
fn removals_and_renames_are_breaking() {
    assert_eq!(
        Step::RemoveType {
            name: "obsolete".into()
        }
        .compat(),
        Compat::Breaking
    );
    assert_eq!(remove_field().compat(), Compat::Breaking);
    assert_eq!(
        Step::RenameType {
            from: "task".into(),
            to: "todo".into()
        }
        .compat(),
        Compat::Breaking
    );
    assert_eq!(rename_field().compat(), Compat::Breaking);
}

#[test]
fn classification_ignores_the_names() {
    // A rename of any names is still breaking; an add of any names still back-compat.
    let other_rename = Step::RenameField {
        ty: "note".into(),
        from: "x".into(),
        to: "y".into(),
    };
    assert_eq!(other_rename.compat(), Compat::Breaking);
    let other_add = Step::AddField {
        ty: "note".into(),
        field: "z".into(),
        field_type: "text".into(),
    };
    assert_eq!(other_add.compat(), Compat::BackCompatible);
}

#[test]
fn an_empty_edge_is_back_compatible() {
    let m = parse(r#"{ "from": 1, "to": 2, "steps": [] }"#);
    assert_eq!(m.compat(), Compat::BackCompatible);
}

#[test]
fn an_all_additive_edge_is_back_compatible() {
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "addType", "name": "tag", "def": { "kind": "text" } },
            { "kind": "addField", "type": "todo", "field": "priority", "fieldType": "register" }
        ] }"#,
    );
    assert_eq!(m.compat(), Compat::BackCompatible);
}

#[test]
fn one_breaking_step_makes_the_whole_edge_breaking() {
    // A single removal among additions is the weakest link.
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "addType", "name": "tag", "def": { "kind": "text" } },
            { "kind": "removeField", "type": "todo", "field": "legacy" },
            { "kind": "addField", "type": "todo", "field": "priority", "fieldType": "register" }
        ] }"#,
    );
    assert_eq!(m.compat(), Compat::Breaking);
    // As does a single rename.
    let m = parse(
        r#"{ "from": 1, "to": 2, "steps": [
            { "kind": "addField", "type": "todo", "field": "priority", "fieldType": "register" },
            { "kind": "renameField", "type": "todo", "from": "done", "to": "completed" }
        ] }"#,
    );
    assert_eq!(m.compat(), Compat::Breaking);
}

// --- op-rewrite ---

fn mk_op(kind: OpKind) -> Op {
    // A non-`None` tx so a rewrite that dropped the op envelope is caught — with
    // a `None` tx on every op a lost tx would still compare equal.
    Op {
        id: OpId {
            client: cid(1),
            seq: 1,
        },
        stamp: stmp(1, 1),
        target: eid(1, 2),
        kind,
        tx: Some(Tx {
            id: TxId(9),
            count: 2,
        }),
    }
}

/// One op of every key-bearing kind, all addressing `key`. Each carries a
/// distinct, non-trivial payload so a rewrite that dropped or defaulted it (not
/// just the key) would fail the equality check.
fn key_bearing_ops(key: &str) -> Vec<Op> {
    let k = || key.as_bytes().to_vec();
    vec![
        mk_op(OpKind::RegisterSet {
            key: k(),
            value: Scalar::Bool(true),
        }),
        mk_op(OpKind::CounterInc {
            key: k(),
            amount: 7,
        }),
        mk_op(OpKind::CounterDec {
            key: k(),
            amount: 5,
        }),
        mk_op(OpKind::MapSet {
            key: k(),
            value: Scalar::Bool(false),
        }),
        mk_op(OpKind::MapDelete { key: k() }),
        mk_op(OpKind::MapCreate { key: k() }),
        mk_op(OpKind::ListCreate { key: k() }),
        mk_op(OpKind::TextCreate { key: k() }),
    ]
}

/// The sequence-internal kinds, which carry no map key.
fn non_key_ops() -> Vec<Op> {
    let anchor = Anchor {
        parent: None,
        side: Side::Right,
    };
    vec![
        mk_op(OpKind::ListInsert {
            value: Scalar::Null,
            anchor: anchor.clone(),
        }),
        mk_op(OpKind::ListDelete { id: stmp(1, 1) }),
        mk_op(OpKind::TextInsert {
            s: "hi".into(),
            anchor,
        }),
        mk_op(OpKind::TextDelete {
            ids: vec![stmp(1, 1)],
        }),
    ]
}

#[test]
fn rename_field_up_rewrites_a_matching_key() {
    let step = Step::RenameField {
        ty: "todo".into(),
        from: "done".into(),
        to: "completed".into(),
    };
    // Each key-bearing op with key "done" becomes the same op with key "completed".
    for (before, after) in key_bearing_ops("done")
        .into_iter()
        .zip(key_bearing_ops("completed"))
    {
        assert_eq!(step.rewrite_up(&before), OpRewrite::Keep(after));
    }
}

#[test]
fn rename_field_up_leaves_other_keys_and_nonkey_ops_untouched() {
    let step = Step::RenameField {
        ty: "todo".into(),
        from: "done".into(),
        to: "completed".into(),
    };
    for o in key_bearing_ops("other").into_iter().chain(non_key_ops()) {
        assert_eq!(step.rewrite_up(&o), OpRewrite::Keep(o.clone()));
    }
}

#[test]
fn remove_field_up_drops_the_matching_key_and_keeps_the_rest() {
    let step = Step::RemoveField {
        ty: "todo".into(),
        field: "legacy".into(),
    };
    for o in key_bearing_ops("legacy") {
        assert_eq!(step.rewrite_up(&o), OpRewrite::Drop);
    }
    for o in key_bearing_ops("keep").into_iter().chain(non_key_ops()) {
        assert_eq!(step.rewrite_up(&o), OpRewrite::Keep(o.clone()));
    }
}

#[test]
fn add_field_up_is_identity_and_down_drops_the_added_key() {
    let step = Step::AddField {
        ty: "todo".into(),
        field: "priority".into(),
        field_type: "register".into(),
    };
    for o in key_bearing_ops("priority") {
        // Up never rewrites an existing op; down drops the op on the added slot.
        assert_eq!(step.rewrite_up(&o), OpRewrite::Keep(o.clone()));
        assert_eq!(step.rewrite_down(&o), Some(OpRewrite::Drop));
    }
    for o in key_bearing_ops("other").into_iter().chain(non_key_ops()) {
        assert_eq!(step.rewrite_up(&o), OpRewrite::Keep(o.clone()));
        assert_eq!(step.rewrite_down(&o), Some(OpRewrite::Keep(o.clone())));
    }
}

#[test]
fn type_steps_are_inert_on_every_op() {
    let steps = [
        Step::AddType {
            name: "tag".into(),
            def: TypeDef::Text { max: None },
        },
        Step::RemoveType { name: "old".into() },
        Step::RenameType {
            from: "a".into(),
            to: "b".into(),
        },
    ];
    for step in &steps {
        for o in key_bearing_ops("done").into_iter().chain(non_key_ops()) {
            assert_eq!(step.rewrite_up(&o), OpRewrite::Keep(o.clone()));
            // When a down-rewrite exists (the back-compatible AddType), it too is
            // inert on the op stream.
            if let Some(r) = step.rewrite_down(&o) {
                assert_eq!(r, OpRewrite::Keep(o.clone()));
            }
        }
    }
}

#[test]
fn rewrite_down_exists_exactly_for_back_compatible_steps() {
    let sample = mk_op(OpKind::MapDelete { key: b"k".to_vec() });
    let cases = [
        (
            Step::AddType {
                name: "t".into(),
                def: TypeDef::Text { max: None },
            },
            true,
        ),
        (
            Step::AddField {
                ty: "m".into(),
                field: "f".into(),
                field_type: "text".into(),
            },
            true,
        ),
        (Step::RemoveType { name: "t".into() }, false),
        (
            Step::RemoveField {
                ty: "m".into(),
                field: "f".into(),
            },
            false,
        ),
        (
            Step::RenameType {
                from: "a".into(),
                to: "b".into(),
            },
            false,
        ),
        (
            Step::RenameField {
                ty: "m".into(),
                from: "a".into(),
                to: "b".into(),
            },
            false,
        ),
    ];
    for (step, has_down) in cases {
        assert_eq!(step.rewrite_down(&sample).is_some(), has_down);
        // The down-rewrite exists exactly when the step is back-compatible.
        assert_eq!(step.compat() == Compat::BackCompatible, has_down);
    }
}

#[test]
fn parse_is_total_on_assorted_garbage() {
    for src in [
        "",
        "   ",
        "null",
        "42",
        "\"just a string\"",
        "{}",
        r#"{ "from": 1, "to": 2 }"#,
        r#"{ "from": 1, "to": 2, "steps": [ null ] }"#,
        r#"{ "from": 1, "to": 2, "steps": [ { } ] }"#,
        r#"{ "from": 1, "to": 2, "steps": [ { "kind": 3 } ] }"#,
        r#"{ "from": -1, "to": 0, "steps": [] }"#,
        r#"{ "from": 4294967296, "to": 4294967297, "steps": [] }"#,
    ] {
        // Every one is an error, none panics.
        assert!(Migration::parse(src).is_err(), "expected error for {src:?}");
    }
}
