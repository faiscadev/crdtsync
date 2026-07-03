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
use crdtsync_core::schema::{AwarenessEntry, Schema, SchemaErrorKind, TypeDef};

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
