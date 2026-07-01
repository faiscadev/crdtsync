//! ElementId — UUIDv5, derived convergently from (parent, key, kind) so two
//! replicas independently creating "the same element at the same slot" agree.

use uuid::{Builder, Uuid};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ElementId(Uuid);

/// The four value kinds a Map slot can hold. The discriminant feeds id
/// derivation, so Counter@"x" and Register@"x" get distinct ids.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ElementKind {
    Scalar,
    Register,
    Counter,
    Map,
}

impl ElementId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Builder::from_bytes(bytes).into_uuid())
    }

    /// Derive a child id via `uuid::Uuid::new_v5` (a pure SHA-1 hash, no
    /// platform inputs) over the parent id as namespace and `key ‖ kind` as
    /// name, so every replica derives the same id for the same slot.
    pub fn derive(parent: ElementId, key: &[u8], kind: ElementKind) -> Self {
        Self(Uuid::new_v5(&parent.0, &[key, &[kind as u8]].concat()).into())
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        self.0.into_bytes()
    }
}
