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
    Action, AutoVersion, AwarenessEntry, Effect, MarkDef, MarkExpand, MarkFlavor, Schema,
    SchemaErrorKind, Subject, SubjectClass, TemplateVar, Trigger, TriggerEvent, TypeDef,
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
        "Tags": { "kind": "list", "items": "Title", "max": 16 },
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

// --- marks ---

fn with_marks(body: &str) -> String {
    format!(
        r#"{{ "schema": "s", "version": 1, "root": "R",
            "types": {{ "R": {{ "kind": "map" }} }},
            "marks": {body} }}"#
    )
}

#[test]
fn a_schema_without_marks_has_no_declarations() {
    let s = parse(FULL);
    assert!(s.marks().is_empty());
    assert_eq!(s.mark("bold"), None);
}

#[test]
fn parses_mark_flavors_and_expansion() {
    let s = parse(&with_marks(
        r#"{
            "bold":    { "flavor": "boolean", "expand": "both" },
            "link":    { "flavor": "value" },
            "comment": { "flavor": "object", "expand": "none" }
        }"#,
    ));
    assert_eq!(
        s.mark("bold"),
        Some(&MarkDef {
            flavor: MarkFlavor::Boolean,
            expand: MarkExpand::Both,
        })
    );
    assert_eq!(
        s.mark("link"),
        Some(&MarkDef {
            flavor: MarkFlavor::Value,
            expand: MarkExpand::None,
        })
    );
    assert_eq!(
        s.mark("comment"),
        Some(&MarkDef {
            flavor: MarkFlavor::Object,
            expand: MarkExpand::None,
        })
    );
    assert_eq!(s.mark("missing"), None);
}

#[test]
fn mark_expand_defaults_to_none() {
    // A mark with no `expand` neither grows at insertion boundary — the
    // conservative default (link-like), overridden per mark that should grow.
    let s = parse(&with_marks(r#"{ "italic": { "flavor": "boolean" } }"#));
    assert_eq!(s.mark("italic").unwrap().expand, MarkExpand::None);
}

#[test]
fn every_expand_direction_parses() {
    let s = parse(&with_marks(
        r#"{
            "a": { "flavor": "boolean", "expand": "none" },
            "b": { "flavor": "boolean", "expand": "before" },
            "c": { "flavor": "boolean", "expand": "after" },
            "d": { "flavor": "boolean", "expand": "both" }
        }"#,
    ));
    assert_eq!(s.mark("a").unwrap().expand, MarkExpand::None);
    assert_eq!(s.mark("b").unwrap().expand, MarkExpand::Before);
    assert_eq!(s.mark("c").unwrap().expand, MarkExpand::After);
    assert_eq!(s.mark("d").unwrap().expand, MarkExpand::Both);
}

#[test]
fn marks_keep_declaration_order() {
    let s = parse(&with_marks(
        r#"{ "bold": { "flavor": "boolean" }, "link": { "flavor": "value" }, "comment": { "flavor": "object" } }"#,
    ));
    let names: Vec<&str> = s.marks().iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["bold", "link", "comment"]);
}

#[test]
fn an_unknown_flavor_is_rejected() {
    let src = with_marks(r#"{ "bold": { "flavor": "wibble" } }"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownFlavor);
}

#[test]
fn an_unknown_expand_is_rejected() {
    let src = with_marks(r#"{ "bold": { "flavor": "boolean", "expand": "sideways" } }"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownExpand);
}

#[test]
fn a_mark_without_a_flavor_is_rejected() {
    let src = with_marks(r#"{ "bold": { "expand": "both" } }"#);
    assert_eq!(err(&src), SchemaErrorKind::MissingField);
}

#[test]
fn a_flavor_of_the_wrong_json_type_is_rejected() {
    let src = with_marks(r#"{ "bold": { "flavor": 7 } }"#);
    assert_eq!(err(&src), SchemaErrorKind::WrongType);
}

#[test]
fn an_unknown_mark_field_is_rejected() {
    let src = with_marks(r#"{ "bold": { "flavor": "boolean", "color": "red" } }"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownField);
}

#[test]
fn a_non_object_mark_def_is_rejected() {
    let src = with_marks(r#"{ "bold": 3 }"#);
    assert_eq!(err(&src), SchemaErrorKind::NotAnObject);
}

#[test]
fn a_non_object_marks_block_is_rejected() {
    let src = with_marks(r#"[]"#);
    assert_eq!(err(&src), SchemaErrorKind::NotAnObject);
}

// --- xml element / fragment types ---

// A prose schema: a fragment root holding block elements, an element with attrs
// + a marks allowlist, and the leaf/attr types + marks all resolve.
fn with_xml_types(types_body: &str) -> String {
    format!(
        r#"{{ "schema": "prose", "version": 1, "root": "Doc",
            "types": {{ "Doc": {{ "kind": "map", "children": {{ "body": "Article" }} }},
                        {types_body} }},
            "marks": {{ "bold": {{ "flavor": "boolean" }}, "link": {{ "flavor": "value" }} }} }}"#
    )
}

const XML_TYPES: &str = r#"
    "Article": { "kind": "fragment", "children": { "Para": {}, "Heading": { "max": 1 } }, "repair": { "orphanInline": "Para" } },
    "Para":    { "kind": "xml", "tag": "p", "children": { "Span": {} }, "marks": ["bold", "link"] },
    "Heading": { "kind": "xml", "tag": "h1", "attrs": { "level": "Level" }, "children": { "Span": {} } },
    "Span":    { "kind": "text", "max": 10000 },
    "Level":   { "kind": "register", "min": 1, "max": 6 }
"#;

#[test]
fn parses_an_xml_element_type_with_all_fields() {
    let s = parse(&with_xml_types(XML_TYPES));
    assert_eq!(
        s.type_def("Para"),
        Some(&TypeDef::Xml {
            tag: Some("p".into()),
            children: vec![("Span".into(), None)],
            attrs: vec![],
            marks: vec!["bold".into(), "link".into()],
            orphan_inline: None,
        })
    );
    assert_eq!(
        s.type_def("Heading"),
        Some(&TypeDef::Xml {
            tag: Some("h1".into()),
            children: vec![("Span".into(), None)],
            attrs: vec![("level".into(), "Level".into())],
            marks: vec![],
            orphan_inline: None,
        })
    );
}

#[test]
fn parses_a_tagless_fragment_type() {
    let s = parse(&with_xml_types(XML_TYPES));
    assert_eq!(
        s.type_def("Article"),
        Some(&TypeDef::Xml {
            tag: None,
            children: vec![("Para".into(), None), ("Heading".into(), Some(1))],
            attrs: vec![],
            marks: vec![],
            orphan_inline: Some("Para".into()),
        })
    );
}

#[test]
fn an_xml_type_defaults_its_allowlists_to_empty() {
    let s = parse(&with_xml_types(
        r#""Article": { "kind": "fragment" }, "Br": { "kind": "xml", "tag": "br" }"#,
    ));
    assert_eq!(
        s.type_def("Br"),
        Some(&TypeDef::Xml {
            tag: Some("br".into()),
            children: vec![],
            attrs: vec![],
            marks: vec![],
            orphan_inline: None,
        })
    );
}

#[test]
fn xml_children_attrs_and_marks_keep_declaration_order() {
    let s = parse(&with_xml_types(
        r#""Article": { "kind": "xml", "tag": "x", "children": { "Span": {}, "Para": {}, "Heading": {} },
                       "attrs": { "z": "Level", "a": "Level", "m": "Level" }, "marks": ["link", "bold"] },
           "Para": { "kind": "xml", "tag": "p" }, "Heading": { "kind": "xml", "tag": "h1" },
           "Span": { "kind": "text" }, "Level": { "kind": "register" }"#,
    ));
    let TypeDef::Xml {
        children,
        attrs,
        marks,
        ..
    } = s.type_def("Article").unwrap()
    else {
        panic!("expected an xml type");
    };
    let child_names: Vec<&str> = children.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(child_names, ["Span", "Para", "Heading"]);
    let keys: Vec<&str> = attrs.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["z", "a", "m"]);
    assert_eq!(marks, &["link", "bold"]);
}

#[test]
fn an_xml_child_referencing_an_undeclared_type_is_rejected() {
    let src =
        with_xml_types(r#""Article": { "kind": "xml", "tag": "x", "children": { "Ghost": {} } }"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownTypeRef);
}

#[test]
fn an_xml_attr_referencing_an_undeclared_type_is_rejected() {
    let src = with_xml_types(
        r#""Article": { "kind": "xml", "tag": "x", "attrs": { "level": "Ghost" } }"#,
    );
    assert_eq!(err(&src), SchemaErrorKind::UnknownTypeRef);
}

#[test]
fn an_orphan_inline_referencing_an_undeclared_type_is_rejected() {
    let src = with_xml_types(
        r#""Article": { "kind": "fragment", "repair": { "orphanInline": "Ghost" } }"#,
    );
    assert_eq!(err(&src), SchemaErrorKind::UnknownTypeRef);
}

#[test]
fn an_xml_marks_allowlist_referencing_an_undeclared_mark_is_rejected() {
    let src =
        with_xml_types(r#""Article": { "kind": "xml", "tag": "x", "marks": ["bold", "strike"] }"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownMarkRef);
}

#[test]
fn an_xml_element_without_a_tag_is_rejected() {
    let src = with_xml_types(r#""Article": { "kind": "xml" }"#);
    assert_eq!(err(&src), SchemaErrorKind::MissingField);
}

#[test]
fn an_unknown_field_on_an_xml_type_is_rejected() {
    let src = with_xml_types(r#""Article": { "kind": "xml", "tag": "x", "color": "red" }"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownField);
}

#[test]
fn a_fragment_may_not_declare_a_tag_attrs_or_marks() {
    for field in [r#""tag": "x""#, r#""attrs": {}"#, r#""marks": []"#] {
        let src = with_xml_types(&format!(r#""Article": {{ "kind": "fragment", {field} }}"#));
        assert_eq!(
            err(&src),
            SchemaErrorKind::UnknownField,
            "fragment must reject {field}"
        );
    }
}

#[test]
fn an_unknown_field_under_repair_is_rejected() {
    let src = with_xml_types(
        r#""Article": { "kind": "fragment", "repair": { "orphanBlock": "Para" } }, "Para": { "kind": "xml", "tag": "p" }"#,
    );
    assert_eq!(err(&src), SchemaErrorKind::UnknownField);
}

#[test]
fn a_non_object_children_allowlist_is_rejected() {
    let src = with_xml_types(r#""Article": { "kind": "xml", "tag": "x", "children": "Span" }"#);
    assert_eq!(err(&src), SchemaErrorKind::NotAnObject);
}

#[test]
fn a_non_object_child_constraint_is_rejected() {
    // A child value must be a (possibly empty) constraints object, not a bare
    // value.
    let src =
        with_xml_types(r#""Article": { "kind": "xml", "tag": "x", "children": { "Span": 7 } }"#);
    assert_eq!(err(&src), SchemaErrorKind::NotAnObject);
}

#[test]
fn a_per_child_max_is_parsed() {
    let s = parse(&with_xml_types(
        r#""Article": { "kind": "xml", "tag": "x", "children": { "Span": { "max": 1 } } },
           "Span": { "kind": "text" }"#,
    ));
    let TypeDef::Xml { children, .. } = s.type_def("Article").unwrap() else {
        panic!("expected an xml type");
    };
    assert_eq!(children, &[("Span".to_string(), Some(1))]);
}

#[test]
fn a_negative_child_max_is_rejected() {
    let src = with_xml_types(
        r#""Article": { "kind": "xml", "tag": "x", "children": { "Span": { "max": -1 } } },
           "Span": { "kind": "text" }"#,
    );
    assert_eq!(err(&src), SchemaErrorKind::BadRange);
}

#[test]
fn an_unknown_field_under_a_child_constraint_is_rejected() {
    let src = with_xml_types(
        r#""Article": { "kind": "xml", "tag": "x", "children": { "Span": { "min": 1 } } },
           "Span": { "kind": "text" }"#,
    );
    assert_eq!(err(&src), SchemaErrorKind::UnknownField);
}

#[test]
fn a_non_object_attrs_allowlist_is_rejected() {
    let src = with_xml_types(r#""Article": { "kind": "xml", "tag": "x", "attrs": [] }"#);
    assert_eq!(err(&src), SchemaErrorKind::NotAnObject);
}

// --- autoVersion ---

fn with_auto_version(body: &str) -> String {
    format!(
        r#"{{ "schema": "s", "version": 1, "root": "R",
            "types": {{ "R": {{ "kind": "map" }} }},
            "autoVersion": {body} }}"#
    )
}

#[test]
fn a_schema_without_auto_version_has_no_triggers() {
    let s = parse(FULL);
    assert!(s.auto_version().is_empty());
}

#[test]
fn parses_event_and_schedule_triggers_with_retention() {
    let s = parse(&with_auto_version(
        r#"[
            { "on": "before-publish", "name": "auto/publish/${timestamp}", "keep": 20 },
            { "every": "1h", "name": "auto/hourly/${timestamp}", "keep": 24 },
            { "on": "subscribe", "name": "auto/join" }
        ]"#,
    ));
    assert_eq!(
        s.auto_version(),
        [
            AutoVersion {
                trigger: Trigger::On(TriggerEvent::BeforePublish),
                name: "auto/publish/${timestamp}".to_string(),
                keep: Some(20),
            },
            AutoVersion {
                // 1h in milliseconds.
                trigger: Trigger::Every(3_600_000),
                name: "auto/hourly/${timestamp}".to_string(),
                keep: Some(24),
            },
            AutoVersion {
                trigger: Trigger::On(TriggerEvent::Subscribe),
                name: "auto/join".to_string(),
                keep: None,
            },
        ]
    );
}

#[test]
fn every_duration_unit_converts_to_milliseconds() {
    for (spec, millis) in [
        ("30s", 30_000),
        ("5m", 300_000),
        ("2h", 7_200_000),
        ("1d", 86_400_000),
    ] {
        let s = parse(&with_auto_version(&format!(
            r#"[ {{ "every": "{spec}", "name": "n" }} ]"#
        )));
        assert_eq!(s.auto_version()[0].trigger, Trigger::Every(millis));
    }
}

#[test]
fn the_reserved_events_are_declarable() {
    // The branch/migration events parse now and fire once those layers land.
    for event in ["before-publish", "after-restore", "before-migration"] {
        let s = parse(&with_auto_version(&format!(
            r#"[ {{ "on": "{event}", "name": "n" }} ]"#
        )));
        assert!(matches!(s.auto_version()[0].trigger, Trigger::On(_)));
    }
}

#[test]
fn an_unknown_trigger_event_is_rejected() {
    let src = with_auto_version(r#"[ { "on": "subscrib", "name": "n" } ]"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownEvent);
}

#[test]
fn a_trigger_needs_exactly_one_of_on_or_every() {
    let both = with_auto_version(r#"[ { "on": "subscribe", "every": "1h", "name": "n" } ]"#);
    assert_eq!(err(&both), SchemaErrorKind::BadTrigger);
    let neither = with_auto_version(r#"[ { "name": "n" } ]"#);
    assert_eq!(err(&neither), SchemaErrorKind::BadTrigger);
}

#[test]
fn a_malformed_schedule_duration_is_rejected() {
    // A zero interval is rejected too — it would fire every sweep, flooding versions.
    for spec in ["1", "h", "1y", "", "1.5h", "1h30m", "x", "0s", "0h", "00m"] {
        let src = with_auto_version(&format!(r#"[ {{ "every": "{spec}", "name": "n" }} ]"#));
        assert_eq!(
            err(&src),
            SchemaErrorKind::BadDuration,
            "duration {spec:?} should be rejected"
        );
    }
}

#[test]
fn an_empty_or_missing_name_is_rejected() {
    let empty = with_auto_version(r#"[ { "on": "subscribe", "name": "" } ]"#);
    assert_eq!(err(&empty), SchemaErrorKind::EmptyName);
    let missing = with_auto_version(r#"[ { "on": "subscribe" } ]"#);
    assert_eq!(err(&missing), SchemaErrorKind::MissingField);
}

#[test]
fn a_negative_keep_is_rejected() {
    let src = with_auto_version(r#"[ { "on": "subscribe", "name": "n", "keep": -1 } ]"#);
    assert_eq!(err(&src), SchemaErrorKind::BadRange);
}

#[test]
fn an_unknown_trigger_field_is_rejected() {
    let src = with_auto_version(r#"[ { "on": "subscribe", "name": "n", "retain": 5 } ]"#);
    assert_eq!(err(&src), SchemaErrorKind::UnknownField);
}

#[test]
fn auto_version_must_be_an_array() {
    let src = with_auto_version(r#"{ "on": "subscribe", "name": "n" }"#);
    assert_eq!(err(&src), SchemaErrorKind::WrongType);
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
    // Every modelled top-level block parses together — `marks`, `awareness`, and
    // `auth` each well-formed.
    let s = parse(
        r#"{ "schema": "s", "version": 1, "root": "R",
            "types": { "R": { "kind": "map" } },
            "marks": { "bold": { "flavor": "boolean" } },
            "awareness": { "cursor": {} },
            "auth": { "roles": ["editor"] } }"#,
    );
    assert_eq!(s.name(), "s");
    assert_eq!(s.mark("bold").map(|d| d.flavor), Some(MarkFlavor::Boolean));
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
}

#[test]
fn a_list_min_is_rejected() {
    // A below-min list is unrepairable (items cannot be invented), so `min` is
    // not an accepted list field — it is rejected as unknown, not stored.
    assert_eq!(
        err(r#"{ "schema": "s", "version": 1, "root": "R",
                "types": { "R": { "kind": "map", "children": { "l": "L" } },
                           "N": { "kind": "register" },
                           "L": { "kind": "list", "items": "N", "min": 0 } } }"#),
        SchemaErrorKind::UnknownField
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
        // autoVersion shapes, incl. a multibyte `every` unit — the duration
        // parser slices off the last char, so a non-ASCII unit must not panic.
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "autoVersion": 3 }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "autoVersion": [ 3 ] }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "autoVersion": [ { "every": "1€", "name": "n" } ] }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "autoVersion": [ { "every": "€", "name": "n" } ] }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "autoVersion": [ { "every": "999999999999999999999d", "name": "n" } ] }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "autoVersion": [ { "on": 7, "name": "n" } ] }"#,
        // marks shapes
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "marks": 3 }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "marks": { "b": 3 } }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "marks": { "b": { "flavor": "nope" } } }"#,
        r#"{ "schema": "s", "version": 1, "root": "R", "types": { "R": { "kind": "map" } }, "marks": { "b": { "flavor": "boolean", "expand": "€" } } }"#,
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

#[test]
fn every_trigger_event_kebab_round_trips_through_parse() {
    // `TriggerEvent::as_kebab` (the `${event}` name-template value) and the
    // `on:` parser are inverse tables; a drift between them would let a template
    // expand to a name whose event the parser rejects. Every variant must parse
    // back from its own kebab.
    let all = [
        TriggerEvent::Connect,
        TriggerEvent::Disconnect,
        TriggerEvent::Subscribe,
        TriggerEvent::VersionCreated,
        TriggerEvent::VersionRenamed,
        TriggerEvent::VersionDeleted,
        TriggerEvent::Compaction,
        TriggerEvent::BeforePublish,
        TriggerEvent::AfterRestore,
        TriggerEvent::BeforeMigration,
    ];
    for ev in all {
        let kebab = ev.as_kebab();
        let src = format!(
            r#"{{ "schema": "s", "version": 1, "root": "R",
                "types": {{ "R": {{ "kind": "map" }} }},
                "autoVersion": [{{ "on": "{kebab}", "name": "v" }}] }}"#
        );
        let schema = parse(&src);
        assert_eq!(
            schema.auto_version()[0].trigger,
            Trigger::On(ev),
            "kebab {kebab:?} must parse back to its own event",
        );
    }
}
