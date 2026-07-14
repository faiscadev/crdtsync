//! Zone-membership resolution + cross-zone detection — the pure predicate.
//!
//! A zone is a contiguous subtree rooted at its schema-declared path: every doc
//! location under that path is in the zone, by structure. `zone_of` resolves a
//! runtime key path to the zone whose root is its longest prefix (the innermost,
//! most-specific zone when zones nest); `same_zone` / `crosses_zones` compare two
//! locations' resolved zones. These are the pure predicates the later cross-zone
//! enforcement (forbidding a cross-zone tree move or anchor) consumes.

use crdtsync_core::schema::Schema;
use crdtsync_core::zone::{crosses_zones, same_zone, zone_id_of, zone_of};

/// Build a schema carrying a `zones` block from a name→root-path JSON body.
fn schema_with_zones(zones: &str) -> Schema {
    let src = format!(
        r#"{{ "schema": "s", "version": 1, "root": "R",
             "types": {{ "R": {{ "kind": "map" }} }},
             "zones": {zones} }}"#
    );
    Schema::parse(&src).expect("schema parses")
}

/// A runtime key path built from string segments (the map keys of the location).
fn keys(segs: &[&str]) -> Vec<Vec<u8>> {
    segs.iter().map(|s| s.as_bytes().to_vec()).collect()
}

#[test]
fn a_location_under_a_zone_root_resolves_to_that_zone() {
    let s = schema_with_zones(r#"{ "private": "/comments" }"#);
    assert_eq!(zone_of(&s, &keys(&["comments"])), Some("private"));
    assert_eq!(
        zone_of(&s, &keys(&["comments", "0", "body"])),
        Some("private")
    );
}

#[test]
fn the_root_location_itself_is_in_the_zone() {
    // A zone root path is inclusive: the element at the root path is in the zone.
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    assert_eq!(zone_of(&s, &keys(&["board"])), Some("z"));
}

#[test]
fn a_location_outside_all_roots_is_unzoned() {
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    assert_eq!(zone_of(&s, &keys(&["title"])), None);
    // A sibling sharing a key prefix byte-wise but not a full segment prefix is
    // outside — matching is segment-wise, not substring.
    assert_eq!(zone_of(&s, &keys(&["boardroom"])), None);
    assert_eq!(zone_of(&s, &[]), None);
}

#[test]
fn a_prefix_match_is_segment_wise_not_a_shorter_key() {
    // `/board` must not match a location whose first key is `boar`.
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    assert_eq!(zone_of(&s, &keys(&["boar", "x"])), None);
}

#[test]
fn a_schema_with_no_zones_leaves_every_location_unzoned() {
    let s = schema_with_zones(r#"{}"#);
    assert_eq!(zone_of(&s, &keys(&["anything", "deep"])), None);
    assert_eq!(zone_of(&s, &[]), None);
}

#[test]
fn a_whole_doc_zone_covers_every_location() {
    // Root `/` = the doc root; its subtree is the whole document, so every
    // location — including the doc root itself — falls in it.
    let s = schema_with_zones(r#"{ "all": "/" }"#);
    assert_eq!(zone_of(&s, &[]), Some("all"));
    assert_eq!(zone_of(&s, &keys(&["a"])), Some("all"));
    assert_eq!(zone_of(&s, &keys(&["a", "b", "c"])), Some("all"));
}

#[test]
fn nested_zones_resolve_to_the_innermost_by_longest_prefix() {
    // Zones may nest; the most-specific (longest-prefix) root wins, so a location
    // under the inner root is in the inner zone, not its enclosing one.
    let s = schema_with_zones(r#"{ "outer": "/content", "inner": "/content/secret" }"#);
    assert_eq!(zone_of(&s, &keys(&["content"])), Some("outer"));
    assert_eq!(zone_of(&s, &keys(&["content", "public"])), Some("outer"));
    assert_eq!(zone_of(&s, &keys(&["content", "secret"])), Some("inner"));
    assert_eq!(
        zone_of(&s, &keys(&["content", "secret", "deep"])),
        Some("inner")
    );
}

#[test]
fn the_longest_prefix_wins_regardless_of_declaration_order() {
    // Same nesting, inner declared first — resolution is by prefix length, not
    // order.
    let s = schema_with_zones(r#"{ "inner": "/content/secret", "outer": "/content" }"#);
    assert_eq!(zone_of(&s, &keys(&["content", "secret"])), Some("inner"));
    assert_eq!(zone_of(&s, &keys(&["content", "public"])), Some("outer"));
}

#[test]
fn equal_roots_resolve_to_the_first_declared() {
    // Two zone names over the same root is a degenerate schema (names dedup, roots
    // do not); resolution is deterministic — declaration order breaks the tie.
    let s = schema_with_zones(r#"{ "a": "/x", "b": "/x" }"#);
    assert_eq!(zone_of(&s, &keys(&["x"])), Some("a"));
}

#[test]
fn same_zone_holds_within_one_zone() {
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    let a = keys(&["board", "1"]);
    let b = keys(&["board", "2", "x"]);
    assert!(same_zone(&s, &a, &b));
    assert!(!crosses_zones(&s, &a, &b));
}

#[test]
fn different_zones_cross() {
    let s = schema_with_zones(r#"{ "a": "/x", "b": "/y" }"#);
    let a = keys(&["x", "1"]);
    let b = keys(&["y", "1"]);
    assert!(crosses_zones(&s, &a, &b));
    assert!(!same_zone(&s, &a, &b));
}

#[test]
fn a_zoned_and_an_unzoned_location_cross() {
    // Moving/anchoring between a zone and the unzoned default region crosses the
    // isolation boundary just as two distinct zones do.
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    let zoned = keys(&["board", "1"]);
    let unzoned = keys(&["title"]);
    assert!(crosses_zones(&s, &zoned, &unzoned));
    assert!(crosses_zones(&s, &unzoned, &zoned));
    assert!(!same_zone(&s, &zoned, &unzoned));
}

#[test]
fn two_unzoned_locations_do_not_cross() {
    // Both in the unzoned default region — no zone boundary between them.
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    let a = keys(&["title"]);
    let b = keys(&["author", "name"]);
    assert!(same_zone(&s, &a, &b));
    assert!(!crosses_zones(&s, &a, &b));
}

#[test]
fn nested_inner_and_outer_locations_cross() {
    // A move from the inner zone to its enclosing outer zone crosses — they are
    // distinct zones despite the structural containment.
    let s = schema_with_zones(r#"{ "outer": "/content", "inner": "/content/secret" }"#);
    let inner = keys(&["content", "secret", "x"]);
    let outer = keys(&["content", "public"]);
    assert!(crosses_zones(&s, &inner, &outer));
}

#[test]
fn resolution_is_deterministic() {
    let s = schema_with_zones(r#"{ "outer": "/content", "inner": "/content/secret" }"#);
    let path = keys(&["content", "secret", "deep"]);
    let first = zone_of(&s, &path);
    for _ in 0..8 {
        assert_eq!(zone_of(&s, &path), first);
    }
}

#[test]
fn the_resolver_is_total_on_any_key_path() {
    // Arbitrary key paths — empty, deep, non-utf8 bytes — never panic; each
    // resolves to a zone or to the unzoned region.
    let s = schema_with_zones(r#"{ "z": "/board" }"#);
    let _ = zone_of(&s, &[]);
    let _ = zone_of(&s, &[vec![0xff, 0x00, 0xfe]]);
    let _ = zone_of(&s, &keys(&["board", ""]));
    let deep: Vec<Vec<u8>> = (0..64).map(|i| vec![i as u8]).collect();
    let _ = zone_of(&s, &deep);
    // Cross-zone detection is likewise total on any pair.
    assert!(!crosses_zones(&s, &[], &[]));
}

// --- compact zone ids (the op-envelope partition dimension) ---

#[test]
fn a_zone_id_is_its_declaration_index() {
    // The compact id is the zone's position in the order-preserving `zones()`, so
    // every replica sharing the schema keys the same partition the same way.
    let s = schema_with_zones(r#"{ "a": "/x", "b": "/y", "c": "/z" }"#);
    assert_eq!(zone_id_of(&s, &keys(&["x"])), Some(0));
    assert_eq!(zone_id_of(&s, &keys(&["y", "deep"])), Some(1));
    assert_eq!(zone_id_of(&s, &keys(&["z"])), Some(2));
}

#[test]
fn an_unzoned_location_has_no_zone_id() {
    let s = schema_with_zones(r#"{ "a": "/x" }"#);
    assert_eq!(zone_id_of(&s, &keys(&["other"])), None);
    assert_eq!(zone_id_of(&s, &[]), None);
}

#[test]
fn the_zone_id_agrees_with_the_named_resolution() {
    // The id resolves the same zone the name does — longest-prefix, innermost
    // wins — just projected to the declaration index.
    let s = schema_with_zones(r#"{ "outer": "/content", "inner": "/content/secret" }"#);
    let deep = keys(&["content", "secret", "x"]);
    assert_eq!(zone_of(&s, &deep), Some("inner"));
    assert_eq!(zone_id_of(&s, &deep), Some(1));
    let shallow = keys(&["content", "public"]);
    assert_eq!(zone_of(&s, &shallow), Some("outer"));
    assert_eq!(zone_id_of(&s, &shallow), Some(0));
}
