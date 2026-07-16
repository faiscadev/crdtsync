//! Op — the immutable, append-only unit a document emits.
//!
//! The envelope carries only what the pure CRDT core needs: identity
//! (`OpId = (client, seq)`), causal position (`Stamp = lamport + client`),
//! the `target` element, and the `kind` — a closed enum of primitive
//! mutations. `OpId` is also the idempotence key: a replica ignores an op
//! whose id it has already applied. Authorship, scope, schema version, and
//! wall time are wire/server concerns and live outside the core.

use crate::acl::{AclEffect, AclGrant, AclScope, AclSubject};
use crate::clientid::ClientId;
use crate::elementid::ElementId;
use crate::list::Anchor;
use crate::ranged::{RangeAnchor, RangedInit};
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
    /// are fixed at create; `payload` is the initial payload — a leaf
    /// [`Scalar`](RangedInit::Scalar) or a nested
    /// [`Composite`](RangedInit::Composite) container installed at a derived id.
    /// `name` tags the range as a mark of that name (a convention over the
    /// annotation set — the read model combines same-named marks per the schema
    /// flavor); `None` is a plain, unnamed annotation.
    RangedCreate {
        start: RangeAnchor,
        end: RangeAnchor,
        payload: RangedInit,
        name: Option<Vec<u8>>,
    },
    /// Replace a RangedElement's scalar payload, last-writer-wins by the op's
    /// stamp. A composite payload is edited through its container, not replaced,
    /// so this is inert against one.
    RangedSetPayload {
        id: ElementId,
        payload: Scalar,
    },
    /// Tombstone a RangedElement. Delete wins over a concurrent payload change.
    RangedDelete {
        id: ElementId,
    },
    /// Grant an [`AclTuple`](crate::acl::AclTuple) into the document's ACL set:
    /// an allow/deny of a capability-or-role, to a subject, on a [scope](AclScope)
    /// — a fixed path or a stable element id that follows the element across a
    /// tree-move. The new tuple's id derives from the op's stamp, so every replica
    /// agrees and concurrent grants are distinct entries. `grantor` is the authoring
    /// actor, carried explicitly on the op (authorship is a wire/envelope concern,
    /// not a core op field) and stored faithfully — core enforces no provenance here.
    AclGrant {
        subject: AclSubject,
        grant: AclGrant,
        effect: AclEffect,
        scope: AclScope,
        grantor: ClientId,
    },
    /// Tombstone an ACL tuple. A tuple is immutable once created; a revoke is the
    /// only mutation, and it wins (retained tombstone).
    AclRevoke {
        id: ElementId,
    },
    /// Reveal a movable node's shell — its stable [`ElementId`] and current `tag`
    /// (an element for `Some`, a text run for `None`) — with no placement. This is a
    /// **redaction-time synthesis**, never authored and never persisted: the server
    /// injects it into a partial reader's op stream to reveal a node born in a subtree
    /// that reader cannot read, once an [`XmlMove`](Self::XmlMove) relocates the node
    /// into one it can (reveal-on-move-in). Applying it materializes the node shell so
    /// the (readable) move can place it and the node's readable content ops drain onto
    /// it — the op-stream analogue of the snapshot projection keeping the node at its
    /// readable current position. It carries only the node's current identity and tag,
    /// so no op of the node's private origin leaks.
    XmlReveal {
        node: ElementId,
        tag: Option<Vec<u8>>,
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
            | OpKind::XmlInsertChild { .. }
            // A reveal installs a movable node whose attrs Map and children List are
            // addressed by derived id — the same subtree-anchoring property, so a
            // translation that cannot rewrite a container-create cannot rewrite it.
            | OpKind::XmlReveal { .. } => true,
            // A composite RangedElement create installs its payload container at a
            // derived id later ops target keylessly; a scalar create installs no
            // container.
            OpKind::RangedCreate { payload, .. } => matches!(payload, RangedInit::Composite(_)),
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
            | OpKind::RangedSetPayload { .. }
            | OpKind::RangedDelete { .. }
            // An ACL tuple is pure data held in the doc-level set — no container.
            | OpKind::AclGrant { .. }
            | OpKind::AclRevoke { .. } => false,
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
    /// Which replication partition the op belongs to: the compact id of the zone
    /// (`Schema::zones()` declaration index, see [`zone::zone_id_of`](crate::zone::zone_id_of))
    /// its `target` resolves to, or `None` for the root partition (an unzoned
    /// target, or a document with no zones). The op is stamped from that
    /// partition's own lamport clock, so zones are causally independent — an op in
    /// one zone never orders an op in another. The dimension travels on the
    /// envelope rather than being re-derived on receipt, so a replica advances the
    /// right clock even for an op whose target it cannot yet resolve, and a later
    /// per-zone stream can route by it without materialising the tree.
    pub zone: Option<u32>,
}

impl Op {
    /// A standalone (non-atomic) op in the root partition. The zone dimension is
    /// assigned by the emitting [`Document`](crate::Document) from the target's
    /// position; an op minted without that context (a test fixture, a translated
    /// or replayed op) defaults to the root partition.
    pub fn new(id: OpId, stamp: Stamp, target: ElementId, kind: OpKind) -> Self {
        Self {
            id,
            stamp,
            target,
            kind,
            tx: None,
            zone: None,
        }
    }
}
