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

    /// Whether this kind is a nested container rather than a leaf — the kinds
    /// addressed by element id, whose create a migration leaves at its key. The
    /// single source of truth for the container/leaf split, which
    /// [`Element::is_container`](crate::element::Element::is_container) reads
    /// through; exhaustive with no catch-all, so a new kind must be classified.
    pub fn is_container(self) -> bool {
        match self {
            Self::Map | Self::List | Self::Text | Self::XmlElement | Self::XmlFragment => true,
            Self::Scalar | Self::Register | Self::Counter => false,
        }
    }

    /// Whether a container of this kind derives its id from its parent and key — so
    /// the key alone names it, and a snapshot migration can resurrect it there. An
    /// XML *element* mixes its tag in below the key, so the key alone does not name
    /// it; a fragment derives by key like the rest. A leaf is not a container at
    /// all. Exhaustive with no catch-all, so a new kind must be classified here
    /// rather than defaulting to unresurrectable.
    pub(crate) fn is_key_derived_container(self) -> bool {
        match self {
            Self::Map | Self::List | Self::Text | Self::XmlFragment => true,
            Self::Scalar | Self::Register | Self::Counter | Self::XmlElement => false,
        }
    }

    /// The container kinds a key names — the candidates a snapshot migration
    /// resolves a retained create against. Kept in step with
    /// [`is_key_derived_container`](Self::is_key_derived_container) by
    /// `every_key_derived_kind_is_a_candidate`.
    pub(crate) const KEY_DERIVED_CONTAINERS: [Self; 4] =
        [Self::Map, Self::List, Self::Text, Self::XmlFragment];
}

#[cfg(test)]
mod tests {
    use super::ElementKind;

    #[test]
    fn every_key_derived_kind_is_a_candidate() {
        // The candidate list a migration walks and the predicate that classifies a
        // kind are two statements of one fact; a kind in either and not the other
        // records a create no resolution ever reaches, or the reverse.
        for tag in 0..=u8::MAX {
            let Some(kind) = ElementKind::from_tag(tag) else {
                continue;
            };
            assert_eq!(
                kind.is_key_derived_container(),
                ElementKind::KEY_DERIVED_CONTAINERS.contains(&kind),
                "{kind:?} is classified one way and listed the other"
            );
        }
    }
}
