//! Text — a collaborative character sequence.
//!
//! CRDT identity is the codepoint: each codepoint is one node in the same
//! Fugue sequence that backs [`List`], so concurrent edits converge and never
//! interleave. A run inserted together takes consecutive char_ids from its
//! base stamp. Indices are codepoint indices; the core stays Unicode-neutral
//! beyond codepoint identity (normalization and grapheme segmentation are SDK
//! concerns).

use crate::element::Element;
use crate::elementid::ElementId;
use crate::list::List;
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
        for (k, c) in s.chars().enumerate() {
            let char_id = Stamp {
                lamport: stamp.lamport + k as u64,
                client: stamp.client,
            };
            self.inner
                .insert(index + k, Element::Scalar(Scalar::Int(c as i64)), char_id);
        }
    }

    /// Tombstone `count` live codepoints starting at `index`.
    pub fn delete(&mut self, index: usize, count: usize) {
        for _ in 0..count {
            self.inner.delete(index);
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
