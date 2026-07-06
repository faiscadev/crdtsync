//! ElementId — UUIDv5, derived convergently from (parent, key, kind) so two
//! replicas independently creating "the same element at the same slot" agree.

use uuid::{Builder, Uuid};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ElementId(Uuid);

/// The value kinds a Map slot can hold. The discriminant feeds id
/// derivation, so Counter@"x" and Register@"x" get distinct ids.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ElementKind {
    Scalar,
    Register,
    Counter,
    Map,
    List,
    Text,
    XmlElement,
    XmlFragment,
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

impl ElementKind {
    /// The kind for a tag byte (`kind as u8`), or `None` if it names no kind.
    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Scalar),
            1 => Some(Self::Register),
            2 => Some(Self::Counter),
            3 => Some(Self::Map),
            4 => Some(Self::List),
            5 => Some(Self::Text),
            6 => Some(Self::XmlElement),
            7 => Some(Self::XmlFragment),
            _ => None,
        }
    }
}
