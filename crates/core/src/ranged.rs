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
use crate::elementid::ElementId;
use crate::scalar::Scalar;

/// One endpoint of a range: a stable [`RelativePosition`] inside the sequence
/// element `seq`. The two endpoints of a [`RangedElement`] may carry different
/// `seq`s, so a range can cross element boundaries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RangeAnchor {
    pub seq: ElementId,
    pub pos: RelativePosition,
}

/// A generic ranged annotation: two anchors and an LWW payload under a stable id.
/// A read view over the document's annotation set — obtain one from
/// [`Document::ranged_element`](crate::doc::Document::ranged_element) or
/// [`ranged_elements`](crate::doc::Document::ranged_elements).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RangedElement {
    pub id: ElementId,
    pub start: RangeAnchor,
    pub end: RangeAnchor,
    pub payload: Scalar,
}
