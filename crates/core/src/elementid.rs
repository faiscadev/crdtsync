//! ElementId — UUIDv5, derived convergently from (parent, key, kind) so two
//! replicas independently creating "the same element at the same slot" agree.

use uuid::Uuid;

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
        let _ = bytes;
        todo!()
    }

    /// Derive a child id via UUIDv5 over (parent.id, key, kind).
    pub fn derive(parent: ElementId, key: &[u8], kind: ElementKind) -> Self {
        let _ = (parent, key, kind);
        todo!()
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        todo!()
    }
}
