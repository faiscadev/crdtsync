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
            let char_id = Stamp {
                lamport: base.lamport + k as u64,
                client: base.client,
            };
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
        (index..index.saturating_add(count))
            .map_while(|i| self.inner.node_at(i))
            .collect()
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
