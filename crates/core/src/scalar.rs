//! Scalar — the leaf value type (Register payload, Map scalar slots).
//!
//! A value, not an entity: no id, no merge, no displacement. `Bytes` is
//! binary-safe (embedded NULs are part of the value).

use crate::codec::{put_scalar, Cursor, DecodeError};
use crate::elementid::ElementId;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Bytes(Vec<u8>),
    /// A handle to out-of-band binary content (file, image, media). The bytes
    /// live in a separate blob store, fetched by handle on render; only the ref
    /// travels in the op stream. Assigning a new ref is a plain LWW replace, so
    /// a ref is a leaf value like any other — it has no substructure and does
    /// not merge.
    BlobRef(BlobRef),
    /// A link to another element in the same room (a mention, a foreign key).
    /// The target is a bare [`ElementId`]; references never cross rooms, so no
    /// room qualifier is carried. A plain LWW value like any other leaf — no
    /// substructure, no merge; a dangling target is an app concern.
    ElementRef(ElementId),
}

/// The largest blob that carries its bytes inline in the ref. A blob at or below
/// this size rides in [`BlobRef::inline`] and needs no store — the producer mints
/// the ref straight from the bytes. A larger one leaves `inline` empty and is held
/// by the store, fetched by handle. The server-side store bounds inlining by its
/// own copy of this size, not a shared one across the crate boundary.
pub const INLINE_MAX: usize = 4096;

/// An opaque reference to a blob in the store.
///
/// `id` is a public, unguessable handle (never the content hash), so it can
/// travel to any recipient without leaking whether the store already holds the
/// bytes. Small blobs carry their bytes in `inline` to skip the fetch round
/// trip; larger ones leave it empty and are fetched by `id`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlobRef {
    pub id: [u8; 16],
    pub mime: String,
    pub size: u64,
    pub inline: Option<Vec<u8>>,
}

impl Scalar {
    /// Append this value's state to `out`. The shared seam for the composite
    /// and Document codecs.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        put_scalar(out, self);
    }

    /// Read a value from `cur`, advancing it.
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<Scalar, DecodeError> {
        cur.scalar()
    }

    /// Serialize this value's state to self-contained bytes.
    pub fn encode_state(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_state_into(&mut out);
        out
    }

    /// Read a value from a complete byte slice, rejecting trailing bytes.
    pub fn decode_state(bytes: &[u8]) -> Result<Scalar, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let value = Scalar::decode_state_from(&mut cur)?;
        if cur.at_end() {
            Ok(value)
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}
