//! Op — the immutable, append-only unit a document emits.
//!
//! The envelope carries only what the pure CRDT core needs: identity
//! (`OpId = (client, seq)`), causal position (`Stamp = lamport + client`),
//! the `target` element, and the `kind` — a closed enum of primitive
//! mutations. `OpId` is also the idempotence key: a replica ignores an op
//! whose id it has already applied. Authorship, scope, schema version, and
//! wall time are wire/server concerns and live outside the core.

use crate::clientid::ClientId;
use crate::elementid::ElementId;
use crate::list::Anchor;
use crate::ranged::RangeAnchor;
use crate::scalar::Scalar;
use crate::stamp::Stamp;

/// Op identity: the minting client plus its per-client monotonic sequence.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct OpId {
    pub client: ClientId,
    pub seq: u64,
}

/// Membership handle for an atomic transaction; ops in the same transaction
/// share one id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TxId(pub u64);

/// An op's membership in an atomic transaction: the shared [`TxId`] and the size
/// of the group, so a receiver knows when every member has arrived and can apply
/// them together. Members are ordered by their op seq within the group.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tx {
    pub id: TxId,
    pub count: u32,
}

/// A primitive mutation, addressed by the key of a slot in the target Map.
/// The receiver reaches the child through the map's get-or-create, re-deriving
/// its id. Leaf children (Register, Counter) are created implicitly by their
/// first value op; a nested Map is created explicitly by `MapCreate` so that
/// later ops can target it. Closed: one variant per composite operation the
/// core understands. The acting client and causal order live on the [`Op`],
/// not here.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum OpKind {
    RegisterSet {
        key: Vec<u8>,
        value: Scalar,
    },
    CounterInc {
        key: Vec<u8>,
        amount: u32,
    },
    CounterDec {
        key: Vec<u8>,
        amount: u32,
    },
    MapSet {
        key: Vec<u8>,
        value: Scalar,
    },
    MapDelete {
        key: Vec<u8>,
    },
    /// Install a nested Map child at `key` in the target map.
    MapCreate {
        key: Vec<u8>,
    },
    /// Install a List child at `key` in the target map.
    ListCreate {
        key: Vec<u8>,
    },
    /// Insert an item into the target List. The new node's id is the op's
    /// stamp; `anchor` fixes its Fugue position.
    ListInsert {
        value: Scalar,
        anchor: Anchor,
    },
    /// Tombstone the node `id` in the target List.
    ListDelete {
        id: Stamp,
    },
    /// Install a Text child at `key` in the target map.
    TextCreate {
        key: Vec<u8>,
    },
    /// Insert a run into the target Text. The op's stamp is the first
    /// codepoint's char_id; the rest run consecutively from it. `anchor` fixes
    /// the run's Fugue position.
    TextInsert {
        s: String,
        anchor: Anchor,
    },
    /// Tombstone the codepoints with these char_ids in the target Text.
    TextDelete {
        ids: Vec<Stamp>,
    },
    /// Install an `XmlElement` child at `key` in the target map. The `tag`
    /// participates in the child's derived id, so a concurrent create of the
    /// same key with a different tag is a distinct identity the slot's LWW
    /// resolves — a retag is a replace, never an in-place mutation.
    XmlElementCreate {
        key: Vec<u8>,
        tag: Vec<u8>,
    },
    /// Install a tagless `XmlFragment` child at `key` in the target map.
    XmlFragmentCreate {
        key: Vec<u8>,
    },
    /// Insert a child into the target XML children List. `tag` present installs
    /// an `XmlElement` child; absent installs a `Text` child (a text run). The
    /// new node's id is the op's stamp, and the child's element id derives from
    /// it, so every replica builds the same child; `anchor` fixes its Fugue
    /// position. Deleting a child reuses [`ListDelete`](Self::ListDelete) on the
    /// same children List.
    XmlInsertChild {
        tag: Option<Vec<u8>>,
        anchor: Anchor,
    },
    /// Move the node `node` under the target children List at `anchor`. The
    /// target addresses the destination sequence; `node` keeps its element id, so
    /// its attrs and descendants ride along — only which sequence renders it
    /// changes. Ordered by the op's stamp (Kleppmann 2021): a concurrent move of
    /// the same node resolves to one parent, a move under the node's own
    /// descendant is dropped as a cycle.
    XmlMove {
        node: ElementId,
        anchor: Anchor,
    },
    /// Create a [`RangedElement`](crate::ranged::RangedElement) in the document's
    /// annotation set. The new element's id derives from the op's stamp, so every
    /// replica agrees and concurrent creates are distinct entries. `start`/`end`
    /// are fixed at create; `payload` is the initial LWW value.
    RangedCreate {
        start: RangeAnchor,
        end: RangeAnchor,
        payload: Scalar,
    },
    /// Replace a RangedElement's payload, last-writer-wins by the op's stamp.
    RangedSetPayload {
        id: ElementId,
        payload: Scalar,
    },
    /// Tombstone a RangedElement. Delete wins over a concurrent payload change.
    RangedDelete {
        id: ElementId,
    },
}

impl OpKind {
    /// Whether this op installs a nested container (map / list / text) at a key.
    /// These are the only ops whose child gets a derived [`ElementId`] that later
    /// ops target *without* naming a key, so their subtree is addressed by
    /// element id, not field name — the property the fan-out translation relies
    /// on to know it cannot rewrite or drop a container-create without tearing
    /// its subtree.
    pub fn creates_container(&self) -> bool {
        // Exhaustive with no catch-all: a new container kind must be classified
        // here or the crate does not compile — the source of truth cannot drift.
        match self {
            OpKind::MapCreate { .. }
            | OpKind::ListCreate { .. }
            | OpKind::TextCreate { .. }
            | OpKind::XmlElementCreate { .. }
            | OpKind::XmlFragmentCreate { .. }
            | OpKind::XmlInsertChild { .. } => true,
            OpKind::RegisterSet { .. }
            | OpKind::CounterInc { .. }
            | OpKind::CounterDec { .. }
            | OpKind::MapSet { .. }
            | OpKind::MapDelete { .. }
            | OpKind::ListInsert { .. }
            | OpKind::ListDelete { .. }
            | OpKind::TextInsert { .. }
            | OpKind::TextDelete { .. }
            | OpKind::XmlMove { .. }
            | OpKind::RangedCreate { .. }
            | OpKind::RangedSetPayload { .. }
            | OpKind::RangedDelete { .. } => false,
        }
    }
}

/// A single CRDT operation. Immutable once minted; the op log is append-only.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Op {
    pub id: OpId,
    pub stamp: Stamp,
    pub target: ElementId,
    pub kind: OpKind,
    pub tx: Option<Tx>,
}

impl Op {
    /// A standalone (non-atomic) op.
    pub fn new(id: OpId, stamp: Stamp, target: ElementId, kind: OpKind) -> Self {
        Self {
            id,
            stamp,
            target,
            kind,
            tx: None,
        }
    }
}
