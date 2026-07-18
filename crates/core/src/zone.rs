//! Zone-membership resolution over the parsed schema.
//!
//! A zone is a contiguous subtree rooted at its schema-declared path (§Zones): by
//! structure, every doc location under that root path is in the zone. This module
//! turns that structural definition into pure predicates over a runtime key path
//! (`&[Vec<u8>]`, as [`parse_path`](crate::path::parse_path) yields): [`zone_of`]
//! resolves a location to its zone, and [`same_zone`] / [`crosses_zones`] compare
//! two locations' zones.
//!
//! These are the causal-independence predicates the cross-zone rule consumes:
//! cross-zone tree moves and cross-zone anchors are forbidden, so the per-zone
//! lamport clocks never need cross-zone ordering. Enforcement (refusing an actual
//! op) is a separate concern; this module is the pure detection only — no
//! `Document`, no ops, no mutation, deterministic on `&Schema`.

use crate::schema::Schema;

/// Split a zone root path string into its key segments. A root is an absolute
/// path (`/` or `/seg/seg…`, validated at schema parse): `/` is the doc root (the
/// empty key path, whose subtree is the whole document); `/content/body` is
/// `[b"content", b"body"]`. Total — a malformed root just yields whatever segments
/// splitting produces, never a panic.
fn root_keys(root: &str) -> Vec<Vec<u8>> {
    let body = root.strip_prefix('/').unwrap_or(root);
    if body.is_empty() {
        return Vec::new();
    }
    body.split('/').map(|s| s.as_bytes().to_vec()).collect()
}

/// Whether `root` is a segment-wise prefix of `path` — i.e. `path` is at or under
/// the zone root. Matching is per whole key, so `/board` does not cover a `boar`
/// or `boardroom` sibling. An empty `root` (the doc root) is a prefix of every
/// path.
fn is_prefix(root: &[Vec<u8>], path: &[Vec<u8>]) -> bool {
    root.len() <= path.len() && root.iter().zip(path).all(|(a, b)| a == b)
}

/// The key-path segments of the declared root of the zone named `name`, or `None`
/// when the schema declares no zone by that name. `/board` → `[b"board"]`; the doc
/// root `/` → `[]` (the empty key path). The cross-zone-move token's destination
/// authority check resolves a zone name to the subtree it governs through this.
pub fn zone_root_keys(schema: &Schema, name: &str) -> Option<Vec<Vec<u8>>> {
    schema
        .zones()
        .iter()
        .find(|(zone_name, _)| zone_name == name)
        .map(|(_, root)| root_keys(root))
}

/// The zone a doc location falls in: the zone whose root path is the *longest*
/// prefix of `path_keys` (the innermost, most-specific zone when zones nest). A
/// location under no zone root is unzoned (the default region) → `None`. On a tie
/// (two zones over the same root — a degenerate schema, since names dedup but roots
/// do not) the first in declaration order wins, so resolution stays deterministic.
/// Total on any key path.
pub fn zone_of<'a>(schema: &'a Schema, path_keys: &[Vec<u8>]) -> Option<&'a str> {
    // The name of the zone the id resolves to — one longest-prefix walk, projected
    // to its declared name rather than its index.
    zone_id_of(schema, path_keys).map(|id| schema.zones()[id as usize].0.as_str())
}

/// The compact id of the zone a doc location falls in: the *index* of the
/// resolved zone in the schema's [`zones()`](Schema::zones) declaration order, or
/// `None` when the location is unzoned (the root partition). The id is a pure
/// function of the order-preserving `zones()`, so every replica sharing the schema
/// assigns the same partition to the same location. It is the op-envelope's zone
/// dimension and the key of a per-zone lamport clock; the same longest-prefix rule
/// as [`zone_of`] chooses which zone (and thus which id) wins. Total on any key
/// path.
pub fn zone_id_of(schema: &Schema, path_keys: &[Vec<u8>]) -> Option<u32> {
    let mut best: Option<(u32, usize)> = None;
    for (i, (_, root)) in schema.zones().iter().enumerate() {
        let root = root_keys(root);
        if is_prefix(&root, path_keys) {
            let len = root.len();
            if best.is_none_or(|(_, best_len)| len > best_len) {
                best = Some((i as u32, len));
            }
        }
    }
    best.map(|(id, _)| id)
}

/// Whether two doc locations resolve to the same zone. Both in one zone, or both
/// unzoned, is the same zone; a zoned location and an unzoned one, or two distinct
/// zones, are not. Total on any pair of key paths.
pub fn same_zone(schema: &Schema, a_keys: &[Vec<u8>], b_keys: &[Vec<u8>]) -> bool {
    zone_of(schema, a_keys) == zone_of(schema, b_keys)
}

/// Whether two doc locations straddle a zone boundary — they resolve to different
/// zones (counting the unzoned default region as distinct from any zone). This is
/// the predicate the cross-zone rule applies to a tree move's (source parent, dest
/// parent) and an anchor's endpoints. Total on any pair of key paths.
pub fn crosses_zones(schema: &Schema, a_keys: &[Vec<u8>], b_keys: &[Vec<u8>]) -> bool {
    !same_zone(schema, a_keys, b_keys)
}
