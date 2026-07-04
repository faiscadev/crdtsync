//! The schema model + its parse-time validation — the spec.
//!
//! A schema is a JSON file (parsed by `core::json`) describing a document's
//! shape over the built primitives: a `root` map type and named `types`
//! (map/list/text/register/counter) with their constraints. Parsing is
//! total — every input yields a `Schema` or a `SchemaError`, never a panic —
//! and it validates the *closure* property at parse time: every type a schema
//! names is declared, `root` is a map, and every numeric bound is well-formed,
//! so no accepted schema can describe a state the engine cannot later repair.

use crdtsync_core::json::JsonErrorKind;
use crdtsync_core::schema::{
    Action, AwarenessEntry, Effect, Schema, SchemaErrorKind, Subject, SubjectClass, TemplateVar,
    TypeDef,
};

fn parse(s: &str) -> Schema {
    Schema::parse(s).unwrap_or_else(|e| panic!("parse of schema failed: {e:?}\n{s}"))
}

fn err(s: &str) -> SchemaErrorKind {
    Schema::parse(s).expect_err("expected a schema error").kind
}

// A well-formed schema exercising every kind, reused across the happy-path tests.
const FULL: &str = r#"
{
    "schema": "notes",
    "version": 3,
    "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "title": "Title", "body": "Body", "tags": "Tags", "hits": "Hits" } },
        "Title": { "kind": "register", "min": 0, "max": 280 },
        "Body": { "kind": "text", "max": 100000 },
        "Tags": { "kind": "list", "items": "Title", "min": 0, "max": 16 },
        "Hits": { "kind": "counter", "min": 0 }
    }
}
"#;

// --- happy path: model + accessors ---

#[test]
fn parses_a_minimal_schema() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } } }"#,
    );
    assert_eq!(s.name(), "s");
    assert_eq!(s.version(), 1);
    assert_eq!(s.root(), "R");
    assert_eq!(s.type_def("R"), Some(&TypeDef::Map { children: vec![] }));
    assert_eq!(s.type_def("missing"), None);
}

#[test]
fn parses_every_built_primitive_kind_with_its_constraints() {
    let s = parse(FULL);
    assert_eq!(s.name(), "notes");
    assert_eq!(s.version(), 3);
    assert_eq!(s.root(), "Doc");
    assert_eq!(
        s.type_def("Doc"),
        Some(&TypeDef::Map {
            children: vec![
                ("title".into(), "Title".into()),
                ("body".into(), "Body".into()),
                ("tags".into(), "Tags".into()),
                ("hits".into(), "Hits".into()),
            ],
        })
    );
    assert_eq!(
        s.type_def("Title"),
        Some(&TypeDef::Register {
            min: Some(0),
            max: Some(280),
        })
    );
    assert_eq!(
        s.type_def("Body"),
        Some(&TypeDef::Text { max: Some(100_000) })
    );
    assert_eq!(
        s.type_def("Tags"),
        Some(&TypeDef::List {
            items: "Title".into(),
            min: Some(0),
            max: Some(16),
        })
    );
    assert_eq!(
        s.type_def("Hits"),
        Some(&TypeDef::Counter {
            min: Some(0),
            max: None,
        })
    );
}

#[test]
fn types_keep_declaration_order() {
    let s = parse(FULL);
    let names: Vec<&str> = s.types().iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["Doc", "Title", "Body", "Tags", "Hits"]);
}

#[test]
fn map_children_keep_declaration_order() {
    let s = parse(FULL);
    let TypeDef::Map { children } = s.type_def("Doc").unwrap() else {
        panic!("Doc is a map");
    };
    let slots: Vec<&str> = children.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(slots, ["title", "body", "tags", "hits"]);
}

#[test]
fn a_map_with_no_children_is_an_empty_allowlist() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } } }"#,
    );
    assert_eq!(s.type_def("R"), Some(&TypeDef::Map { children: vec![] }));
}

#[test]
fn numeric_bounds_are_optional_and_default_to_none() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": {
                "R": { "kind": "map", "children": { "n": "N", "c": "C", "l": "L", "t": "T" } },
                "N": { "kind": "register" },
                "C": { "kind": "counter" },
                "L": { "kind": "list", "items": "N" },
                "T": { "kind": "text" }
            } }"#,
    );
    assert_eq!(
        s.type_def("N"),
        Some(&TypeDef::Register {
            min: None,
            max: None
        })
    );
    assert_eq!(
        s.type_def("C"),
        Some(&TypeDef::Counter {
            min: None,
            max: None
        })
    );
    assert_eq!(
        s.type_def("L"),
        Some(&TypeDef::List {
            items: "N".into(),
            min: None,
            max: None
        })
    );
    assert_eq!(s.type_def("T"), Some(&TypeDef::Text { max: None }));
}

#[test]
fn a_register_bound_may_be_negative() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map", "children": { "n": "N" } },
                       "N": { "kind": "register", "min": -10, "max": 10 } } }"#,
    );
    assert_eq!(
        s.type_def("N"),
        Some(&TypeDef::Register {
            min: Some(-10),
            max: Some(10)
        })
    );
}

// --- awareness ---

#[test]
fn a_schema_without_awareness_has_no_entries() {
    let s = parse(FULL);
    assert!(s.awareness().is_empty());
    assert_eq!(s.awareness_entry("cursor"), None);
}

#[test]
fn parses_awareness_entries_with_ttl_and_throttle() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "awareness": {
                "cursor": { "ttl": 30000, "throttle": 50 },
                "selection": { "ttl": 30000 },
                "presence": {}
            } }"#,
    );
    assert_eq!(
        s.awareness_entry("cursor"),
        Some(&AwarenessEntry {
            ttl: Some(30000),
            throttle: Some(50),
        })
    );
    assert_eq!(
        s.awareness_entry("selection"),
        Some(&AwarenessEntry {
            ttl: Some(30000),
            throttle: None,
        })
    );
    assert_eq!(
        s.awareness_entry("presence"),
        Some(&AwarenessEntry {
            ttl: None,
            throttle: None,
        })
    );
    assert_eq!(s.awareness_entry("missing"), None);
}

#[test]
fn awareness_kinds_keep_declaration_order() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "awareness": { "cursor": {}, "selection": {}, "presence": {} } }"#,
    );
    let kinds: Vec<&str> = s.awareness().iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(kinds, ["cursor", "selection", "presence"]);
}

#[test]
fn awareness_must_be_an_object() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } }, "awareness": [] }"#),
        SchemaErrorKind::NotAnObject
    );
}

#[test]
fn an_awareness_entry_must_be_an_object() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } }, "awareness": { "cursor": 5 } }"#),
        SchemaErrorKind::NotAnObject
    );
}

#[test]
fn an_awareness_timer_must_be_a_non_negative_integer() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } },
                 "awareness": { "cursor": { "ttl": "soon" } } }"#),
        SchemaErrorKind::WrongType
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } },
                 "awareness": { "cursor": { "throttle": -1 } } }"#),
        SchemaErrorKind::BadRange
    );
}

#[test]
fn an_unknown_top_level_key_is_rejected_not_ignored() {
    // A typo'd key must fail loud rather than be silently dropped.
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } }, "awarness": {} }"#),
        SchemaErrorKind::UnknownField
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map" } }, "typo": 1 }"#),
        SchemaErrorKind::UnknownField
    );
}

#[test]
fn an_unknown_field_inside_a_type_is_rejected() {
    // A typo'd bound (`mni` for `min`) would silently not-constrain — reject it.
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map", "children": { "n": "N" } },
                       "N": { "kind": "register", "mni": 0 } } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::UnknownField);
    assert_eq!(e.at, "N.mni", "the error names the type and field");
    // A field valid for one kind but not another is rejected too.
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                 "types": { "R": { "kind": "map", "children": {} }, "T": { "kind": "text", "min": 0 } } }"#),
        SchemaErrorKind::UnknownField
    );
}

#[test]
fn an_unknown_field_inside_an_awareness_entry_is_rejected() {
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "awareness": { "cursor": { "tt": 100 } } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::UnknownField);
    assert_eq!(e.at, "awareness.cursor.tt");
}

#[test]
fn the_language_defined_top_level_keys_are_accepted() {
    // `marks` is declared by the language (not yet modelled here), so a schema
    // using it still parses; `awareness` and `auth` are modelled and must be
    // well-formed.
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "marks": { "bold": {} },
            "awareness": { "cursor": {} },
            "auth": { "roles": ["editor"] } }"#,
    );
    assert_eq!(s.name(), "s");
    assert_eq!(s.awareness_entry("cursor").map(|_| ()), Some(()));
}

// --- errors: each a distinct kind, none a panic ---

#[test]
fn malformed_json_surfaces_as_a_json_error() {
    assert_eq!(
        err("{"),
        SchemaErrorKind::Json(JsonErrorKind::UnexpectedEof)
    );
    assert_eq!(
        err("not json"),
        SchemaErrorKind::Json(JsonErrorKind::Unexpected)
    );
}

#[test]
fn a_duplicate_json_key_surfaces_as_a_json_error() {
    let src = r#"{ "schema": "s", "version": 1, "version": 2, "root": "R", "types": { "R": { "kind": "map" } } }"#;
    assert_eq!(err(src), SchemaErrorKind::Json(JsonErrorKind::DuplicateKey));
}

#[test]
fn the_top_level_must_be_an_object() {
    assert_eq!(err("42"), SchemaErrorKind::NotAnObject);
    assert_eq!(err("[]"), SchemaErrorKind::NotAnObject);
    assert_eq!(err("\"s\""), SchemaErrorKind::NotAnObject);
}

#[test]
fn every_required_top_level_field_must_be_present() {
    assert_eq!(
        err(r#"{ "version": 1, "root": "R", "types": { "R": { "kind": "map" } } }"#),
        SchemaErrorKind::MissingField
    );
    assert_eq!(
        err(r#"{ "schema": "s", "root": "R", "types": { "R": { "kind": "map" } } }"#),
        SchemaErrorKind::MissingField
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "types": { "R": { "kind": "map" } } }"#),
        SchemaErrorKind::MissingField
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R" }"#),
        SchemaErrorKind::MissingField
    );
}

#[test]
fn the_version_must_be_an_integer() {
    assert_eq!(
        err(
            r#"{ "schema": "s", "version": "1", "root": "R", "types": { "R": { "kind": "map" } } }"#
        ),
        SchemaErrorKind::WrongType
    );
    assert_eq!(
        err(
            r#"{ "schema": "s", "version": 1.5, "root": "R", "types": { "R": { "kind": "map" } } }"#
        ),
        SchemaErrorKind::WrongType
    );
}

#[test]
fn the_root_must_be_a_string() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": 7, "types": { "R": { "kind": "map" } } }"#),
        SchemaErrorKind::WrongType
    );
}

#[test]
fn types_must_be_an_object() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R", "types": [] }"#),
        SchemaErrorKind::NotAnObject
    );
}

#[test]
fn the_root_must_name_a_declared_type() {
    assert_eq!(
        err(
            r#"{ "schema": "s", "version": 1, "root": "Nope", "types": { "R": { "kind": "map" } } }"#
        ),
        SchemaErrorKind::UnknownTypeRef
    );
}

#[test]
fn the_root_type_must_be_a_map() {
    assert_eq!(
        err(
            r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "register" } } }"#
        ),
        SchemaErrorKind::RootNotMap
    );
}

#[test]
fn a_type_def_must_be_an_object() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": 5 } }"#),
        SchemaErrorKind::NotAnObject
    );
}

#[test]
fn a_type_needs_a_known_kind() {
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "widget" } } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::UnknownKind);
    assert_eq!(e.at, "R.kind", "the error names the kind field");
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": {} } }"#),
        SchemaErrorKind::MissingField
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": 1 } } }"#),
        SchemaErrorKind::WrongType
    );
}

#[test]
fn a_map_child_must_name_a_declared_type() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "x": "Gone" } } } }"#),
        SchemaErrorKind::UnknownTypeRef
    );
}

#[test]
fn a_map_child_value_must_be_a_string() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "x": 3 } } } }"#),
        SchemaErrorKind::WrongType
    );
}

#[test]
fn map_children_must_be_an_object() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": [] } } }"#),
        SchemaErrorKind::NotAnObject
    );
}

#[test]
fn a_list_must_declare_a_known_item_type() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "l": "L" } },
                           "L": { "kind": "list", "items": "Gone" } } }"#),
        SchemaErrorKind::UnknownTypeRef
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "l": "L" } },
                           "L": { "kind": "list" } } }"#),
        SchemaErrorKind::MissingField
    );
}

#[test]
fn min_greater_than_max_is_a_bad_range() {
    for kind in ["register", "counter"] {
        let src = format!(
            r#"{{ "schema": "s", "version": 1, "root": "R",
                 "types": {{ "R": {{ "kind": "map", "children": {{ "x": "X" }} }},
                             "X": {{ "kind": "{kind}", "min": 5, "max": 4 }} }} }}"#
        );
        let e = Schema::parse(&src).expect_err("bad range");
        assert_eq!(e.kind, SchemaErrorKind::BadRange, "{kind}");
        assert_eq!(
            e.at, "X.min_gt_max",
            "dot-separated, operator-free location"
        );
    }
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "l": "L" } },
                           "N": { "kind": "register" },
                           "L": { "kind": "list", "items": "N", "min": 5, "max": 4 } } }"#),
        SchemaErrorKind::BadRange
    );
}

#[test]
fn a_negative_count_bound_is_a_bad_range() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "t": "T" } },
                           "T": { "kind": "text", "max": -1 } } }"#),
        SchemaErrorKind::BadRange
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "l": "L" } },
                           "N": { "kind": "register" },
                           "L": { "kind": "list", "items": "N", "min": -1 } } }"#),
        SchemaErrorKind::BadRange
    );
}

#[test]
fn a_bound_must_be_an_integer() {
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "x": "X" } },
                           "X": { "kind": "register", "min": "low" } } }"#),
        SchemaErrorKind::WrongType
    );
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "x": "X" } },
                           "X": { "kind": "counter", "max": 1.5 } } }"#),
        SchemaErrorKind::WrongType
    );
}

#[test]
fn a_json_error_keeps_its_byte_offset() {
    // The leading-zero `01` is a bad number partway into the document; the
    // schema error must carry that location, not flatten it to "document".
    let e = Schema::parse(r#"{ "schema": 01 }"#).unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::Json(JsonErrorKind::BadNumber));
    assert_eq!(e.at, "byte 12", "offset preserved");
    assert!(format!("{e}").contains("byte 12"), "shown: {e}");
}

#[test]
fn a_nested_field_error_names_its_type() {
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map", "children": { "x": "X" } },
                       "X": { "kind": "register", "min": "low" } } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::WrongType);
    assert_eq!(e.at, "X.min", "the error names the offending type");
}

#[test]
fn a_bad_child_value_names_the_type_and_slot() {
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map", "children": { "x": 3 } } } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::WrongType);
    assert_eq!(e.at, "R.children.x", "the error names the type and slot");
}

#[test]
fn schema_error_displays_and_is_an_error() {
    let e = Schema::parse("[]").unwrap_err();
    let shown = format!("{e}");
    assert!(!shown.is_empty());
    let _: &dyn std::error::Error = &e;
}

#[test]
fn hostile_inputs_never_panic() {
    let inputs = [
        "",
        "{",
        "{}",
        "[]",
        "null",
        r#"{ "schema": "s" }"#,
        r#"{ "schema": 1, "version": "x", "root": [], "types": 3 }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map", "children": { "x": { "kind": "map" } } } } }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "list", "items": "R" } } }"#,
        "\u{1F600}",
    ];
    for s in inputs {
        // The contract is only that it returns — Ok or Err, never a panic.
        let _ = Schema::parse(s);
    }
}

// --- @auth: static role-based defaults (roles vocabulary + grants) ---

// A schema carrying a full `@auth` block: a role vocabulary and grants covering
// every subject flavour (declared role, ownership template, subject class), both
// effects, and every schema-grantable action.
const AUTHED: &str = r#"
{
    "schema": "notes",
    "version": 1,
    "root": "Doc",
    "types": { "Doc": { "kind": "map" } },
    "auth": {
        "roles": ["viewer", "editor"],
        "grants": [
            { "allow": "read",  "to": "viewer", "on": "/" },
            { "allow": "write", "to": "editor", "on": "/body" },
            { "deny":  "write", "to": "viewer", "on": "/body" },
            { "allow": "write", "to": "${author_id}", "on": "/comments" },
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "publish_awareness", "to": "anyone", "on": "/cursors" }
        ]
    }
}
"#;

#[test]
fn auth_roles_parse_in_declaration_order() {
    let s = parse(AUTHED);
    assert_eq!(s.auth().roles(), ["viewer", "editor"]);
}

#[test]
fn auth_grants_parse_effect_action_subject_path() {
    let s = parse(AUTHED);
    let g = s.auth().grants();
    assert_eq!(g.len(), 6);

    assert_eq!(g[0].effect, Effect::Allow);
    assert_eq!(g[0].action, Action::Read);
    assert_eq!(g[0].subject, Subject::Role("viewer".to_string()));
    assert_eq!(g[0].path, "/");

    assert_eq!(g[1].effect, Effect::Allow);
    assert_eq!(g[1].action, Action::Write);
    assert_eq!(g[1].subject, Subject::Role("editor".to_string()));
    assert_eq!(g[1].path, "/body");

    // deny keeps its effect distinct from an allow of the same action/path.
    assert_eq!(g[2].effect, Effect::Deny);
    assert_eq!(g[2].action, Action::Write);
    assert_eq!(g[2].subject, Subject::Role("viewer".to_string()));
}

#[test]
fn auth_subject_flavours() {
    let s = parse(AUTHED);
    let g = s.auth().grants();
    assert_eq!(g[3].subject, Subject::Template(TemplateVar::AuthorId));
    assert_eq!(g[4].subject, Subject::Class(SubjectClass::Authenticated));
    assert_eq!(g[5].subject, Subject::Class(SubjectClass::Anyone));
    assert_eq!(g[5].action, Action::PublishAwareness);
}

#[test]
fn every_template_var_parses() {
    for (tok, want) in [
        ("${actor_id}", TemplateVar::ActorId),
        ("${author_id}", TemplateVar::AuthorId),
        ("${room_id}", TemplateVar::RoomId),
        ("${branch_id}", TemplateVar::BranchId),
    ] {
        let src = format!(
            r#"{{ "schema": "s", "version": 1, "root": "R",
                 "types": {{ "R": {{ "kind": "map" }} }},
                 "auth": {{ "grants": [ {{ "allow": "read", "to": "{tok}", "on": "/" }} ] }} }}"#
        );
        let s = parse(&src);
        assert_eq!(s.auth().grants()[0].subject, Subject::Template(want));
    }
}

#[test]
fn every_subject_class_parses() {
    for (tok, want) in [
        ("authenticated", SubjectClass::Authenticated),
        ("anonymous", SubjectClass::Anonymous),
        ("anyone", SubjectClass::Anyone),
    ] {
        let src = format!(
            r#"{{ "schema": "s", "version": 1, "root": "R",
                 "types": {{ "R": {{ "kind": "map" }} }},
                 "auth": {{ "grants": [ {{ "allow": "read", "to": "{tok}", "on": "/" }} ] }} }}"#
        );
        let s = parse(&src);
        assert_eq!(s.auth().grants()[0].subject, Subject::Class(want));
    }
}

#[test]
fn absent_auth_is_empty() {
    let s = parse(FULL);
    assert!(s.auth().roles().is_empty());
    assert!(s.auth().grants().is_empty());
}

#[test]
fn auth_may_declare_roles_without_grants() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "auth": { "roles": ["viewer"] } }"#,
    );
    assert_eq!(s.auth().roles(), ["viewer"]);
    assert!(s.auth().grants().is_empty());
}

#[test]
fn auth_may_grant_only_classes_without_roles() {
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "auth": { "grants": [ { "allow": "read", "to": "anyone", "on": "/" } ] } }"#,
    );
    assert!(s.auth().roles().is_empty());
    assert_eq!(s.auth().grants().len(), 1);
}

// --- @auth errors: closure, arity, vocabulary, path ---

fn auth_err(grants_and_roles: &str) -> SchemaErrorKind {
    let src = format!(
        r#"{{ "schema": "s", "version": 1, "root": "R",
             "types": {{ "R": {{ "kind": "map" }} }},
             "auth": {} }}"#,
        grants_and_roles
    );
    err(&src)
}

#[test]
fn grant_to_an_undeclared_role_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "ghost", "on": "/" } ] }"#),
        SchemaErrorKind::UnknownRoleRef
    );
}

#[test]
fn ownership_action_is_not_schema_grantable() {
    // `own` is dynamic doc-level ACL state, never a static schema default.
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "own", "to": "anyone", "on": "/" } ] }"#),
        SchemaErrorKind::UnknownAction
    );
}

#[test]
fn an_unknown_action_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "frobnicate", "to": "anyone", "on": "/" } ] }"#),
        SchemaErrorKind::UnknownAction
    );
}

#[test]
fn a_grant_with_both_effects_is_rejected() {
    assert_eq!(
        auth_err(
            r#"{ "grants": [ { "allow": "read", "deny": "read", "to": "anyone", "on": "/" } ] }"#
        ),
        SchemaErrorKind::BadGrant
    );
}

#[test]
fn a_grant_with_no_effect_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "to": "anyone", "on": "/" } ] }"#),
        SchemaErrorKind::BadGrant
    );
}

#[test]
fn a_grant_missing_its_subject_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "on": "/" } ] }"#),
        SchemaErrorKind::MissingField
    );
}

#[test]
fn a_grant_missing_its_path_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "anyone" } ] }"#),
        SchemaErrorKind::MissingField
    );
}

#[test]
fn an_unknown_grant_field_is_rejected() {
    assert_eq!(
        auth_err(
            r#"{ "grants": [ { "allow": "read", "to": "anyone", "on": "/", "when": "now" } ] }"#
        ),
        SchemaErrorKind::UnknownField
    );
}

#[test]
fn an_unknown_template_var_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "${session_id}", "on": "/" } ] }"#),
        SchemaErrorKind::BadSubject
    );
}

#[test]
fn a_malformed_template_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "${unclosed", "on": "/" } ] }"#),
        SchemaErrorKind::BadSubject
    );
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "${}", "on": "/" } ] }"#),
        SchemaErrorKind::BadSubject
    );
}

#[test]
fn a_path_without_a_leading_slash_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "anyone", "on": "body" } ] }"#),
        SchemaErrorKind::BadPath
    );
}

#[test]
fn a_path_with_an_empty_segment_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "anyone", "on": "/a//b" } ] }"#),
        SchemaErrorKind::BadPath
    );
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "anyone", "on": "/body/" } ] }"#),
        SchemaErrorKind::BadPath
    );
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": "anyone", "on": "" } ] }"#),
        SchemaErrorKind::BadPath
    );
}

#[test]
fn a_duplicate_role_is_rejected() {
    assert_eq!(
        auth_err(r#"{ "roles": ["editor", "editor"] }"#),
        SchemaErrorKind::DuplicateRole
    );
}

#[test]
fn a_role_named_like_a_subject_class_is_rejected() {
    // Reserving the class keywords keeps a grant's `to` unambiguous.
    for kw in ["authenticated", "anonymous", "anyone"] {
        let src = format!(r#"{{ "roles": ["{kw}"] }}"#);
        assert_eq!(auth_err(&src), SchemaErrorKind::ReservedRole, "{kw}");
    }
}

#[test]
fn a_duplicate_role_error_names_the_array_element() {
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "auth": { "roles": ["a", "b", "b"] } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::DuplicateRole);
    assert_eq!(e.at, "auth.roles[2]", "names the duplicate's index");
}

#[test]
fn grant_value_errors_name_the_real_key() {
    // An unknown action names the allow/deny key that carries it, not a
    // pseudo-key spelled after the value.
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "auth": { "grants": [ { "allow": "frobnicate", "to": "anyone", "on": "/" } ] } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::UnknownAction);
    assert!(e.at.ends_with(".allow"), "names the allow key: {}", e.at);

    // A bad subject names the `to` field, not a pseudo-key spelled after the
    // offending value.
    for (to, kind) in [
        ("ghost", SchemaErrorKind::UnknownRoleRef),
        ("${bogus}", SchemaErrorKind::BadSubject),
    ] {
        let src = format!(
            r#"{{ "schema": "s", "version": 1, "root": "R",
                 "types": {{ "R": {{ "kind": "map" }} }},
                 "auth": {{ "grants": [ {{ "allow": "read", "to": "{to}", "on": "/" }} ] }} }}"#
        );
        let e = Schema::parse(&src).unwrap_err();
        assert_eq!(e.kind, kind, "{to}");
        assert!(
            e.at.ends_with(".to"),
            "names the to field for {to}: {}",
            e.at
        );
    }
}

#[test]
fn malformed_auth_shapes_are_rejected() {
    assert_eq!(auth_err("[]"), SchemaErrorKind::NotAnObject);
    assert_eq!(
        auth_err(r#"{ "roles": "editor" }"#),
        SchemaErrorKind::WrongType
    );
    assert_eq!(auth_err(r#"{ "roles": [1] }"#), SchemaErrorKind::WrongType);
    assert_eq!(auth_err(r#"{ "grants": {} }"#), SchemaErrorKind::WrongType);
    assert_eq!(
        auth_err(r#"{ "grants": [ "nope" ] }"#),
        SchemaErrorKind::NotAnObject
    );
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": 5, "to": "anyone", "on": "/" } ] }"#),
        SchemaErrorKind::WrongType
    );
    assert_eq!(
        auth_err(r#"{ "grants": [ { "allow": "read", "to": 5, "on": "/" } ] }"#),
        SchemaErrorKind::WrongType
    );
    assert_eq!(auth_err(r#"{ "wat": 1 }"#), SchemaErrorKind::UnknownField);
}

#[test]
fn a_wrong_typed_effect_value_names_its_own_key() {
    // The location must name the key that actually carries the action (`allow`
    // or `deny`), not a synthetic `action` key that does not exist in the JSON.
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "auth": { "grants": [ { "deny": 5, "to": "anyone", "on": "/" } ] } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::WrongType);
    assert!(
        e.at.ends_with(".deny"),
        "location names the deny key: {}",
        e.at
    );
}

#[test]
fn auth_error_locations_name_the_offending_grant() {
    let e = Schema::parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "auth": { "grants": [ { "allow": "read", "to": "anyone", "on": "/" },
                                  { "allow": "read", "to": "ghost", "on": "/" } ] } }"#,
    )
    .unwrap_err();
    assert_eq!(e.kind, SchemaErrorKind::UnknownRoleRef);
    assert!(
        e.at.contains("grants"),
        "location names the grants block: {}",
        e.at
    );
}
