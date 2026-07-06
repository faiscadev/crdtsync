//! RangedElement — a generic ranged annotation over the document's sequences.
//!
//! A [`RangedElement`] ties two anchors and a payload to a stable id, and lives in
//! a document-level set keyed by that id (not inside the sequence it annotates).
//! Each endpoint is a [`RangeAnchor`] — a [`RelativePosition`] inside a named
//! sequence — and the two endpoints may name *different* sequences, so a range can
//! span elements (a comment across paragraphs). Endpoints are fixed at create (a
//! re-range is delete + create); the payload is last-writer-wins. Marks, comments,
//! suggestions, and highlights are all conventions over this one primitive.

use crate::anchor::RelativePosition;
use crate::elementid::{ElementId, ElementKind};
use crate::scalar::Scalar;

/// One endpoint of a range: a stable [`RelativePosition`] inside the sequence
/// element `seq`. The two endpoints of a [`RangedElement`] may carry different
/// `seq`s, so a range can cross element boundaries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RangeAnchor {
    pub seq: ElementId,
    pub pos: RelativePosition,
}

/// What a [`RangedElement`] created its payload as. A `Scalar` is a leaf value
/// held inline in the annotation set. A `Composite` installs a nested container
/// (Map / List / Text) addressed by an id derived from the RangedElement id, so a
/// comment's structured body — `{author, text, timestamp}` — or an object-flavored
/// mark's value is a first-class CRDT edited through the normal container ops.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RangedInit {
    Scalar(Scalar),
    Composite(ElementKind),
}

/// The payload read view of a [`RangedElement`]: a leaf `Scalar` (last-writer-wins,
/// replaced through [`set_payload`](crate::doc::RangedCursor::set_payload)), or a
/// `Composite` nested container at a derived id — read/edit it through
/// [`Document::ranged_payload`](crate::doc::Document::ranged_payload).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RangedPayload {
    Scalar(Scalar),
    Composite { id: ElementId, kind: ElementKind },
}

/// A generic ranged annotation: two anchors and a payload under a stable id.
/// A read view over the document's annotation set — obtain one from
/// [`Document::ranged_element`](crate::doc::Document::ranged_element) or
/// [`ranged_elements`](crate::doc::Document::ranged_elements).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RangedElement {
    pub id: ElementId,
    pub start: RangeAnchor,
    pub end: RangeAnchor,
    pub payload: RangedPayload,
}

impl RangedElement {
    /// The leaf payload, or `None` for a composite one — the ergonomic read for
    /// the common scalar-payload annotation.
    pub fn scalar(&self) -> Option<&Scalar> {
        match &self.payload {
            RangedPayload::Scalar(s) => Some(s),
            RangedPayload::Composite { .. } => None,
        }
    }
}

/// Whether `kind` is a container a RangedElement may carry as a composite payload.
/// Register/Counter live in a Map slot, Xml nodes in a sequence — a structured
/// payload nests them inside one of these three.
pub(crate) fn is_composite_payload_kind(kind: ElementKind) -> bool {
    matches!(
        kind,
        ElementKind::Map | ElementKind::List | ElementKind::Text
    )
}
