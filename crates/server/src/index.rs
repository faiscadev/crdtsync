//! The per-room element-context index — id → context, derived from the room's
//! authoritative document projection.
//!
//! The op fan-out and migration seams see only ops plus a [`Schema`], yet
//! cross-zone move enforcement (and the later per-zone replication streams and
//! type-scoped migration) each need to resolve an element id to its *context*:
//! which subtree holds it, and which zone it falls in. This module is that lean
//! derived resolution. It holds no state of its own — it projects the room's
//! already-materialized [`Document`] to `id → core::path` (the same walk the
//! read-redaction seam uses) and resolves that path's zone by the schema's
//! longest-prefix rule. Deriving from the one authoritative tree rather than a
//! separately-maintained replica means the index cannot drift from it — a move
//! refold, an LWW displacement, a buffered-then-drained op, and a subtree delete
//! are all already reflected in the document the projection reads.

use std::collections::{HashMap, HashSet};

use crdtsync_core::op::OpKind;
use crdtsync_core::schema::TypeDef;
use crdtsync_core::zone;
use crdtsync_core::{Document, Element, ElementId, Op, Schema};

/// An element id mapped to its `core::path` key sequence — the projection the
/// zone (and later type) resolution reads. A positional XML child inherits its
/// holding node's path, since a zone governs a whole subtree.
pub type ElementPaths = HashMap<ElementId, Vec<Vec<u8>>>;

/// A container element id mapped to the schema type name that governs it — the
/// projection a type-scoped migration reads to narrow a field rewrite to the
/// elements of the step's declared type. Only map elements are ever named by a
/// field-bearing op (the op's target is the map holding the slot), so those are
/// the ids this resolves; an element the walk cannot type (a since-deleted map,
/// or one whose runtime kind does not match its declared type) is simply absent,
/// and the migration falls back to a key-based rewrite for it.
pub type ElementTypes = HashMap<ElementId, String>;

/// Project `doc` to the element-context index: every container element mapped to
/// its `core::path`. Walks the authoritative tree, so the result reflects the
/// document exactly — no separate state to fall out of sync.
pub fn element_paths(doc: &Document) -> ElementPaths {
    let mut out = HashMap::new();
    crate::index_container(&Element::Map(doc.root()), &[], &mut out);
    out
}

/// A blob's public handle id mapped to the encoded `core::path`s that currently
/// hold a live reference to it — the blob-fetch authorization index. A blob is
/// content-addressed and immutable, so authorization cannot attach to the bytes;
/// it attaches to the **reference site**, and this is the id → sites projection a
/// fetch resolves read authority against (the [`recipient_reads_path`] evaluator,
/// exactly the paths op redaction gates). A map-slot ref is keyed at its slot's
/// leaf path (`container + key`), the same governing path a keyed op reads at; a
/// node-addressed ref (a list item, an XML child) inherits its holding container's
/// path, since read authority governs a whole subtree. One id may map to several
/// paths — the same blob referenced from two slots — and a fetch is authorized on
/// **any** readable one.
pub type BlobRefPaths = HashMap<[u8; 16], Vec<Vec<u8>>>;

/// Project `doc` to the blob-reference index: every live [`BlobRef`] slot's blob
/// id mapped to the encoded paths that reference it. Walks the authoritative tree,
/// so an unreferenced id is simply absent (fetch fail-closed) and a moved or
/// deleted reference is reflected exactly. Paths per id are deduped.
///
/// [`BlobRef`]: crdtsync_core::BlobRef
pub fn blob_ref_paths(doc: &Document) -> BlobRefPaths {
    let mut out = HashMap::new();
    crate::index_blob_refs(&Element::Map(doc.root()), &[], &mut out);
    for paths in out.values_mut() {
        paths.sort_unstable();
        paths.dedup();
    }
    out
}

/// The zone `id` falls in under `schema`, or `None` when it is unzoned (the
/// default region), not present in the projection, or the schema declares no
/// zones.
pub fn zone_of<'a>(paths: &ElementPaths, schema: &'a Schema, id: ElementId) -> Option<&'a str> {
    paths.get(&id).and_then(|path| zone::zone_of(schema, path))
}

/// Project `doc` to `id → declared type name` for every map element the schema
/// governs, resolved by the same root-down, position-keyed descent
/// [`crdtsync_core::validate`] walks: the root is the schema's root type, a map
/// slot's child is the type its key names in the parent type's allowlist, and a
/// list item is the list's declared item type. A map whose key names no declared
/// slot, or whose runtime kind does not match its declared type, is not typed —
/// it drops from the projection, so a migration rewriting one of its slots falls
/// back to a key-based rewrite. The walk descends only through map slots and list
/// items, the paths that can reach a map; it terminates on a cycle or shared
/// subtree, since each element id is entered once.
pub fn element_types(doc: &Document, schema: &Schema) -> ElementTypes {
    let mut out = HashMap::new();
    let mut visited = HashSet::new();
    let mut stack: Vec<(Element, &str)> = vec![(Element::Map(doc.root()), schema.root())];
    while let Some((element, type_name)) = stack.pop() {
        let Some(td) = schema.type_def(type_name) else {
            continue;
        };
        match (td, &element) {
            (TypeDef::Map { children }, Element::Map(m)) => {
                let m = m.borrow();
                if !visited.insert(m.id()) {
                    continue;
                }
                out.insert(m.id(), type_name.to_string());
                let allow: HashMap<&[u8], &str> = children
                    .iter()
                    .map(|(s, t)| (s.as_bytes(), t.as_str()))
                    .collect();
                for key in m.keys() {
                    if let Some(&child_type) = allow.get(key.as_slice()) {
                        if let Some(child) = m.get(&key) {
                            stack.push((child, child_type));
                        }
                    }
                }
            }
            (TypeDef::List { items, .. }, Element::List(l)) => {
                let l = l.borrow();
                if !visited.insert(l.id()) {
                    continue;
                }
                for item in l.values() {
                    stack.push((item, items.as_str()));
                }
            }
            _ => {}
        }
    }
    out
}

/// Whether applying `ops` to `doc` relocates any node across a zone boundary —
/// the cross-zone tree move the per-zone clocks cannot order, and which is not
/// detectable from the post-move tree (the moved node simply renders under its
/// new parent).
///
/// A moved node that lives both before and after the batch crosses when its zone
/// changes: a move to a different zone (the unzoned region counting as distinct
/// from every zone) crossed, a reorder — or a cycle the fold drops — that keeps
/// its zone did not. A node the batch itself *creates* or *deletes* is not a
/// crossing: it holds no committed position on one side, so there is no persistent
/// cross-zone edge to forbid (a node born and placed, or moved and then removed,
/// within one atomic batch). The batch is simulated on an independent copy of the
/// document, so a destination created within the same batch resolves and the real
/// fold (readiness buffering, Kleppmann move refold, slot LWW) decides the
/// outcome — the check reflects exactly what the document will hold, never a
/// divergent re-derivation. `false` when the batch moves nothing or the schema
/// declares no zones (every path resolving unzoned).
pub fn batch_crosses_zone(doc: &Document, ops: &[Op], schema: &Schema) -> bool {
    let movers: Vec<ElementId> = ops.iter().filter_map(move_node).collect();
    if movers.is_empty() {
        return false;
    }
    let before = element_paths(doc);
    // An independent copy: decode fresh bytes rather than share the live tree's
    // handles, so the simulation never touches the committed document.
    let Ok(mut simulated) = Document::decode_state(&doc.encode_state()) else {
        return false;
    };
    for op in ops {
        simulated.apply(op);
    }
    let after = element_paths(&simulated);
    movers.iter().any(|node| {
        // Compare zones only for a mover that lives on both sides — a node absent
        // before (created this batch) or after (deleted this batch) is not a
        // committed crossing, and "absent" must not read as "unzoned".
        match (before.get(node), after.get(node)) {
            (Some(from), Some(to)) => zone::zone_of(schema, from) != zone::zone_of(schema, to),
            _ => false,
        }
    })
}

/// The node a tree move relocates, or `None` for any other op.
fn move_node(op: &Op) -> Option<ElementId> {
    match &op.kind {
        OpKind::XmlMove { node, .. } => Some(*node),
        _ => None,
    }
}
