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

use std::collections::HashMap;

use crdtsync_core::op::OpKind;
use crdtsync_core::zone;
use crdtsync_core::{Document, Element, ElementId, Op, Schema};

/// An element id mapped to its `core::path` key sequence — the projection the
/// zone (and later type) resolution reads. A positional XML child inherits its
/// holding node's path, since a zone governs a whole subtree.
pub type ElementPaths = HashMap<ElementId, Vec<Vec<u8>>>;

/// Project `doc` to the element-context index: every container element mapped to
/// its `core::path`. Walks the authoritative tree, so the result reflects the
/// document exactly — no separate state to fall out of sync.
pub fn element_paths(doc: &Document) -> ElementPaths {
    let mut out = HashMap::new();
    crate::index_container(&Element::Map(doc.root()), &[], &mut out);
    out
}

/// The zone `id` falls in under `schema`, or `None` when it is unzoned (the
/// default region), not present in the projection, or the schema declares no
/// zones.
pub fn zone_of<'a>(paths: &ElementPaths, schema: &'a Schema, id: ElementId) -> Option<&'a str> {
    paths.get(&id).and_then(|path| zone::zone_of(schema, path))
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
