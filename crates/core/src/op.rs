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
}

/// A single CRDT operation. Immutable once minted; the op log is append-only.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Op {
    pub id: OpId,
    pub stamp: Stamp,
    pub target: ElementId,
    pub kind: OpKind,
    pub tx: Option<TxId>,
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
