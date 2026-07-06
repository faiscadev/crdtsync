//! RelativePosition — a stable position inside a sequence CRDT.
//!
//! A cursor, selection edge, or mark boundary must survive concurrent edits: an
//! integer offset drifts the moment another replica inserts or deletes ahead of
//! it. A [`RelativePosition`] instead binds to the stable id of a nearby item (or
//! a sequence boundary), so it resolves to the right live index however the
//! sequence has since changed. It is captured with
//! [`List::relative_position`](crate::list::List::relative_position) (or the Text
//! equivalent) and read back with
//! [`List::resolve_position`](crate::list::List::resolve_position).
//!
//! The gravity is in the binding: [`Before`](RelativePosition::Before) sits on an
//! item's left edge and [`After`](RelativePosition::After) on its right, so a
//! concurrent insert at the gap lands on the expected side. When the bound item
//! is deleted, resolution walks the retained tombstones to the nearest live
//! neighbour on that side.

use crate::codec::{put_rel_position, Cursor, DecodeError};
use crate::stamp::Stamp;

/// A stable position in a List or Text sequence.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelativePosition {
    /// The very start of the sequence — always resolves to `0`.
    Start,
    /// The very end of the sequence — always resolves to `len`.
    End,
    /// The left edge of the item with this id (right gravity toward the item).
    Before(Stamp),
    /// The right edge of the item with this id (left gravity toward the item).
    After(Stamp),
}

impl RelativePosition {
    /// Encode to bytes so a position can cross the wire (an awareness cursor
    /// carries one). The tag scheme lives once in [`put_rel_position`], shared
    /// with the range-anchor codec.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_rel_position(&mut out, self);
        out
    }

    /// Decode exactly one position; trailing bytes are an error. Total — any
    /// input yields a value or a [`DecodeError`], never a panic.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let pos = cur.rel_position()?;
        if !cur.at_end() {
            return Err(DecodeError::TrailingBytes);
        }
        Ok(pos)
    }
}
