//! Scalar — the leaf value type (Register payload, Map scalar slots).
//!
//! A value, not an entity: no id, no merge, no displacement. `Bytes` is
//! binary-safe (embedded NULs are part of the value).

use crate::codec::{put_scalar, Cursor, DecodeError};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Bytes(Vec<u8>),
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
