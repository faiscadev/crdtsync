//! Marks — a read-time convention over the RangedElement annotation set.
//!
//! A mark is a [`RangedElement`](crate::ranged::RangedElement) carrying a `name`;
//! there is no per-character mark storage. The active marks on a character are
//! *computed* by gathering every live same-named range whose span covers it and
//! combining them per the name's schema-declared
//! [`MarkFlavor`](crate::schema::MarkFlavor): a **boolean** mark resolves to the
//! presence of the highest-stamped covering range (LWW), a **value** mark to that
//! range's value, an **object** mark to the set of every covering instance. Because
//! the result is a deterministic function of the merged set, per-character mark
//! state converges by construction.

use crate::elementid::ElementId;
use crate::scalar::Scalar;

/// The resolved state of one mark name on a character.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MarkState {
    /// A boolean mark — present or absent, decided last-writer-wins by the
    /// highest-stamped covering range.
    Boolean(bool),
    /// A value mark — the value of the highest-stamped covering range.
    Value(Scalar),
    /// An object mark — the ids of every covering instance, each independent
    /// (overlapping comments coexist), ordered by id for a stable read.
    Object(Vec<ElementId>),
}

/// One mark name active on a character, with its resolved [`MarkState`]. A query
/// over a character returns one per distinct mark name covering it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ResolvedMark {
    pub name: Vec<u8>,
    pub state: MarkState,
}
