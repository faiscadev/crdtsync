//! Text — a collaborative character sequence.
//!
//! CRDT identity is the codepoint: each codepoint is one node in the same
//! Fugue sequence that backs [`List`], so concurrent edits converge and never
//! interleave. A run inserted together takes consecutive char_ids from its
//! base stamp. Indices are codepoint indices; the core stays Unicode-neutral
//! beyond codepoint identity (normalization and grapheme segmentation are SDK
//! concerns).

use crate::anchor::RelativePosition;
use crate::codec::{Cursor, DecodeError};
use crate::element::Element;
use crate::elementid::ElementId;
use crate::list::{Anchor, List, Side};
use crate::scalar::Scalar;
use crate::stamp::Stamp;

pub struct Text {
    inner: List,
}

impl Text {
    pub fn new(id: ElementId) -> Self {
        Self {
            inner: List::new(id),
        }
    }

    pub fn id(&self) -> ElementId {
        self.inner.id()
    }

    /// Append this text's state to `out` — the same sequence encoding as List.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        self.inner.encode_state_into(out);
    }

    /// Read a text from `cur`, advancing it. Every live node must be a valid
    /// Unicode codepoint, so a malformed snapshot is rejected here rather than
    /// panicking when the text is later read. Tombstones carry no value (they
    /// are compressed away in the encoding and never rendered), so only the live
    /// codepoints are checked.
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<Text, DecodeError> {
        let (inner, refs) = List::decode_state_from(cur)?;
        if !refs.is_empty() {
            return Err(DecodeError::BadTag {
                what: "text: composite node reference",
                tag: 0,
            });
        }
        for value in inner.live_values() {
            match value {
                Element::Scalar(Scalar::Int(cp)) if char::from_u32(*cp as u32).is_some() => {}
                _ => {
                    return Err(DecodeError::BadTag {
                        what: "text codepoint",
                        tag: 0,
                    })
                }
            }
        }
        Ok(Text { inner })
    }

    /// Serialize this text's state to self-contained bytes.
    pub fn encode_state(&self) -> Vec<u8> {
        self.inner.encode_state()
    }

    /// Read a text from a complete byte slice, rejecting trailing bytes.
    pub fn decode_state(bytes: &[u8]) -> Result<Text, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let text = Text::decode_state_from(&mut cur)?;
        if cur.at_end() {
            Ok(text)
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }

    /// Number of live codepoints.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The live text.
    pub fn as_string(&self) -> String {
        self.inner.values().iter().map(codepoint).collect()
    }

    /// Insert `s` at codepoint `index`; its codepoints take consecutive
    /// char_ids from `stamp`.
    pub fn insert(&mut self, index: usize, s: &str, stamp: Stamp) {
        let anchor = self.place(index);
        self.insert_run(stamp, s, anchor);
    }

    /// The Fugue placement for inserting at codepoint `index`, computed without
    /// mutating. Feed it to [`insert_run`](Self::insert_run) to reproduce the
    /// insert on any replica.
    pub fn place(&self, index: usize) -> Anchor {
        self.inner.place(index)
    }

    /// Insert the codepoints of `s` with explicit placement: the first uses
    /// `anchor`, each subsequent one hangs to the right of its predecessor, and
    /// char_ids run consecutively from `base`. Idempotent per char_id.
    pub fn insert_run(&mut self, base: Stamp, s: &str, anchor: Anchor) {
        let mut anchor = anchor;
        for (k, c) in s.chars().enumerate() {
            let char_id = base.run_member(k as u64);
            self.inner
                .insert_at(char_id, Element::Scalar(Scalar::Int(c as i64)), anchor);
            anchor = Anchor {
                parent: Some(char_id),
                side: Side::Right,
            };
        }
    }

    /// The char_ids of up to `count` live codepoints starting at `index`.
    pub fn node_ids(&self, index: usize, count: usize) -> Vec<Stamp> {
        self.inner.node_ids(index, count)
    }

    /// Tombstone `count` live codepoints starting at `index`.
    pub fn delete(&mut self, index: usize, count: usize) {
        let ids = self.node_ids(index, count);
        self.delete_ids(&ids);
    }

    /// Whether the codepoint with char_id `id` is present (live or tombstoned).
    pub fn contains(&self, id: Stamp) -> bool {
        self.inner.contains(id)
    }

    /// The live codepoint index of char_id `id`, if it is present and not
    /// tombstoned.
    pub fn live_index(&self, id: Stamp) -> Option<usize> {
        self.inner.live_index(id)
    }

    /// Capture a stable codepoint position at `index` with the given gravity.
    pub fn relative_position(&self, index: usize, side: Side) -> RelativePosition {
        self.inner.relative_position(index, side)
    }

    /// The current live codepoint index of a captured [`RelativePosition`].
    pub fn resolve_position(&self, pos: &RelativePosition) -> usize {
        self.inner.resolve_position(pos)
    }

    /// The live codepoint index of a [`RelativePosition`], or `None` when it is
    /// bound to a codepoint absent from the text — an anchor whose referent has
    /// not arrived yet — instead of clamping to a boundary.
    pub fn resolve_position_present(&self, pos: &RelativePosition) -> Option<usize> {
        self.inner.resolve_position_present(pos)
    }

    /// Tombstone the codepoints with these char_ids. Idempotent.
    pub fn delete_ids(&mut self, ids: &[Stamp]) {
        for id in ids {
            self.inner.delete_id(*id);
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.inner.merge(&other.inner);
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            inner: self.inner.deep_clone(),
        }
    }

    pub fn displace(&self) {
        self.inner.displace();
    }

    pub fn reinstate(&self) {
        self.inner.reinstate();
    }

    pub fn is_displaced(&self) -> bool {
        self.inner.is_displaced()
    }
}

/// Decode a stored codepoint node back to its character.
fn codepoint(e: &Element) -> char {
    match e {
        Element::Scalar(Scalar::Int(cp)) => {
            char::from_u32(*cp as u32).expect("text nodes hold valid Unicode scalar values")
        }
        _ => panic!("text node is not a codepoint scalar"),
    }
}
