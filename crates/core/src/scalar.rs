//! Scalar — the leaf value type (Register payload, Map scalar slots).
//!
//! A value, not an entity: no id, no merge, no displacement. `Bytes` is
//! binary-safe (embedded NULs are part of the value).

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Bytes(Vec<u8>),
}
