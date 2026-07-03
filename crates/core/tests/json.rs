//! The minimal JSON value parser — enough to read a schema file.
//!
//! Total-decode: every input yields a `Json` value or a `JsonError`, never a
//! panic. Integers stay distinct from floats (a schema's versions and bounds are
//! integers), objects keep declaration order, and nesting is depth-bounded so a
//! hostile document cannot overflow the stack.

use crdtsync_core::json::{Json, JsonErrorKind};

fn parse(s: &str) -> Json {
    Json::parse(s).unwrap_or_else(|e| panic!("parse of {s:?} failed: {e:?}"))
}

fn err(s: &str) -> JsonErrorKind {
    Json::parse(s).expect_err("expected a parse error").kind
}

// --- scalars ---

#[test]
fn parses_the_keyword_scalars() {
    assert_eq!(parse("null"), Json::Null);
    assert_eq!(parse("true"), Json::Bool(true));
    assert_eq!(parse("false"), Json::Bool(false));
}

#[test]
fn parses_integers_including_the_i64_extremes() {
    assert_eq!(parse("0"), Json::Int(0));
    assert_eq!(parse("42"), Json::Int(42));
    assert_eq!(parse("-7"), Json::Int(-7));
    assert_eq!(parse("9223372036854775807"), Json::Int(i64::MAX));
    assert_eq!(parse("-9223372036854775808"), Json::Int(i64::MIN));
}

#[test]
fn an_integer_past_i64_degrades_to_a_float() {
    // 2^64, well past i64 — a float rather than a parse failure.
    match parse("18446744073709551616") {
        Json::Float(f) => assert!((f - 1.8446744073709552e19).abs() < 1e4),
        other => panic!("expected a float, got {other:?}"),
    }
}

#[test]
fn parses_floats() {
    assert_eq!(parse("1.5"), Json::Float(1.5));
    assert_eq!(parse("-0.25"), Json::Float(-0.25));
    assert_eq!(parse("1e10"), Json::Float(1e10));
    assert_eq!(parse("6.022e23"), Json::Float(6.022e23));
    assert_eq!(parse("2E-3"), Json::Float(2e-3));
}

#[test]
fn a_decimal_point_makes_a_float_not_an_int() {
    assert_eq!(parse("1.0"), Json::Float(1.0));
    assert_eq!(parse("1.0").as_i64(), None, "a float is not coerced to i64");
}

// --- strings ---

#[test]
fn parses_plain_and_escaped_strings() {
    assert_eq!(parse("\"hello\""), Json::Str("hello".into()));
    assert_eq!(parse("\"\""), Json::Str(String::new()));
    assert_eq!(
        parse(r#""a\"b\\c\/d\n\t\r\b\f""#),
        Json::Str("a\"b\\c/d\n\t\r\u{0008}\u{000C}".into())
    );
}

#[test]
fn parses_a_bmp_unicode_escape() {
    assert_eq!(parse(r#""é""#), Json::Str("é".into()));
    assert_eq!(parse(r#""A""#), Json::Str("A".into()));
}

#[test]
fn combines_a_surrogate_pair_into_one_astral_scalar() {
    // U+1F600 GRINNING FACE = 😀
    assert_eq!(parse(r#""😀""#), Json::Str("😀".into()));
}

#[test]
fn parses_multibyte_utf8_in_a_string_literally() {
    assert_eq!(parse("\"café 😀 ok\""), Json::Str("café 😀 ok".into()));
}

// --- containers ---

#[test]
fn parses_empty_and_nested_containers() {
    assert_eq!(parse("[]"), Json::Array(vec![]));
    assert_eq!(parse("{}"), Json::Object(vec![]));
    assert_eq!(
        parse("[1, 2, [3, 4]]"),
        Json::Array(vec![
            Json::Int(1),
            Json::Int(2),
            Json::Array(vec![Json::Int(3), Json::Int(4)]),
        ])
    );
}

#[test]
fn an_object_keeps_declaration_order() {
    let v = parse(r#"{"b": 1, "a": 2, "c": 3}"#);
    let keys: Vec<&str> = v
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, _)| k.as_str())
        .collect();
    assert_eq!(keys, ["b", "a", "c"], "keys stay in the order written");
}

#[test]
fn accessors_and_get_read_the_expected_shapes() {
    let v = parse(r#"{"name": "doc", "version": 3, "on": true, "kids": [1, 2]}"#);
    assert_eq!(v.get("name").and_then(Json::as_str), Some("doc"));
    assert_eq!(v.get("version").and_then(Json::as_i64), Some(3));
    assert_eq!(v.get("on").and_then(Json::as_bool), Some(true));
    assert_eq!(
        v.get("kids").and_then(Json::as_array).map(<[_]>::len),
        Some(2)
    );
    assert_eq!(v.get("missing"), None);
    assert_eq!(Json::Int(1).get("x"), None, "get on a non-object is None");
}

#[test]
fn parses_a_schema_shaped_document() {
    let src = r#"
    {
        "schema": "notes",
        "version": 1,
        "root": "Doc",
        "types": {
            "Doc": { "kind": "map", "children": { "title": "Title" } },
            "Title": { "kind": "register", "min": 0, "max": 280 }
        }
    }
    "#;
    let v = parse(src);
    assert_eq!(v.get("version").and_then(Json::as_i64), Some(1));
    let title = v.get("types").and_then(|t| t.get("Title")).unwrap();
    assert_eq!(title.get("max").and_then(Json::as_i64), Some(280));
}

// --- whitespace ---

#[test]
fn tolerates_whitespace_around_and_between_tokens() {
    assert_eq!(parse("  \n\t 42 \r\n "), Json::Int(42));
    assert_eq!(
        parse("{ \n \"a\" : \t 1 , \"b\":2 }"),
        Json::Object(vec![("a".into(), Json::Int(1)), ("b".into(), Json::Int(2))])
    );
}

// --- errors: each a distinct kind, none a panic ---

#[test]
fn empty_and_whitespace_only_input_is_eof() {
    assert_eq!(err(""), JsonErrorKind::UnexpectedEof);
    assert_eq!(err("   "), JsonErrorKind::UnexpectedEof);
}

#[test]
fn an_unterminated_string_is_eof() {
    assert_eq!(err("\"abc"), JsonErrorKind::UnexpectedEof);
}

#[test]
fn a_bad_escape_is_rejected() {
    assert_eq!(err(r#""a\xb""#), JsonErrorKind::BadEscape);
}

#[test]
fn a_lone_surrogate_is_bad_unicode() {
    assert_eq!(err(r#""\uD83D""#), JsonErrorKind::BadUnicode);
    assert_eq!(err(r#""\uDE00""#), JsonErrorKind::BadUnicode);
    assert_eq!(err(r#""\uD83Dx""#), JsonErrorKind::BadUnicode);
}

#[test]
fn a_short_unicode_escape_is_rejected() {
    assert_eq!(err(r#""\u12""#), JsonErrorKind::BadUnicode);
}

#[test]
fn a_trailing_comma_is_rejected() {
    assert_eq!(err("[1, 2,]"), JsonErrorKind::Unexpected);
    assert_eq!(err(r#"{"a": 1,}"#), JsonErrorKind::Unexpected);
}

#[test]
fn a_missing_colon_is_rejected() {
    assert_eq!(err(r#"{"a" 1}"#), JsonErrorKind::Unexpected);
}

#[test]
fn an_unclosed_container_is_eof() {
    assert_eq!(err("[1, 2"), JsonErrorKind::UnexpectedEof);
    assert_eq!(err(r#"{"a": 1"#), JsonErrorKind::UnexpectedEof);
}

#[test]
fn a_duplicate_object_key_is_rejected() {
    assert_eq!(err(r#"{"a": 1, "a": 2}"#), JsonErrorKind::DuplicateKey);
}

#[test]
fn trailing_content_after_the_value_is_rejected() {
    assert_eq!(err("1 2"), JsonErrorKind::TrailingBytes);
    assert_eq!(err("{}x"), JsonErrorKind::TrailingBytes);
    assert_eq!(err("null null"), JsonErrorKind::TrailingBytes);
}

#[test]
fn a_bare_non_value_is_unexpected() {
    assert_eq!(err("nul"), JsonErrorKind::Unexpected);
    assert_eq!(err("}"), JsonErrorKind::Unexpected);
    assert_eq!(err("+3"), JsonErrorKind::Unexpected);
}

#[test]
fn a_bare_minus_is_a_bad_number() {
    assert_eq!(err("-"), JsonErrorKind::BadNumber);
    assert_eq!(err("-x"), JsonErrorKind::BadNumber);
}

#[test]
fn a_leading_zero_integer_is_a_bad_number() {
    // JSON's integer grammar is `0 | [1-9][0-9]*` — no leading zeros.
    assert_eq!(err("01"), JsonErrorKind::BadNumber);
    assert_eq!(err("00"), JsonErrorKind::BadNumber);
    assert_eq!(err("-01"), JsonErrorKind::BadNumber);
    assert_eq!(err("007"), JsonErrorKind::BadNumber);
    // A single zero, and a zero fraction/exponent, stay valid.
    assert_eq!(parse("0"), Json::Int(0));
    assert_eq!(parse("0.5"), Json::Float(0.5));
    assert_eq!(parse("0e1"), Json::Float(0.0));
}

#[test]
fn nesting_past_the_depth_cap_is_rejected_not_a_stack_overflow() {
    let deep_arrays = "[".repeat(1000) + &"]".repeat(1000);
    assert_eq!(
        Json::parse(&deep_arrays).unwrap_err().kind,
        JsonErrorKind::DepthLimit
    );
    let deep_objects = "{\"a\":".repeat(1000);
    assert_eq!(
        Json::parse(&deep_objects).unwrap_err().kind,
        JsonErrorKind::DepthLimit
    );
}

#[test]
fn hostile_inputs_never_panic() {
    let inputs = [
        "",
        "\"",
        "\"\\",
        "\"\\u",
        "\"\\uZZZZ\"",
        "[",
        "{",
        "{\"",
        "{\"a\"",
        "{\"a\":",
        "-",
        "-.",
        "1.",
        "1e",
        "1e+",
        ".5",
        "truex",
        "[,]",
        "[1,,2]",
        "\u{1F600}",
        "\t\n\r",
        "\\",
        "]",
        ":",
        ",",
    ];
    for s in inputs {
        // The contract is only that it returns — Ok or Err, never a panic.
        let _ = Json::parse(s);
    }
}
