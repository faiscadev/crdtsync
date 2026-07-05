//! Per-recipient migration translation.
//!
//! The server is the compatibility layer: an op is stored once at its creation
//! schema version and rewritten, at fan-out, to each recipient's version. This
//! module is that rewrite — it resolves the app's registered migration chain
//! into the contiguous edge slice between two versions and composes the
//! per-edge op-rewrites along it. Forward (up) is always defined; backward
//! (down) exists only across back-compatible edges, so a recipient below a
//! breaking gap is [`Unreachable`](TranslateError::Unreachable) and must be
//! refused at the handshake, never served a corrupt op.
//!
//! Pure over the registry, no connection state; the live fan-out seam and the
//! cold-start snapshot both drive it.

use std::collections::{HashMap, HashSet};

use crdtsync_core::doc::SlotFate;
use crdtsync_core::migration::{
    reachable_down, rewrite_down_along, rewrite_up_along, Migration, OpRewrite,
};
use crdtsync_core::op::{OpId, OpKind, TxId};
use crdtsync_core::stamp::Stamp;
use crdtsync_core::{ClientId, Document, ElementId, Op, Scalar};

use crate::schema_registry::SchemaRegistry;
use crate::store::StoredOp;

/// Why a translation could not be produced.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TranslateError {
    /// An edge on the path between the two versions is not registered — a chain
    /// gap, or a version past the registered head (or an unknown app).
    MissingEdge { version: u32 },
    /// A registered edge's stored bytes do not parse as a migration.
    BadMigration { version: u32 },
    /// The recipient's version cannot be reached from the op's creation version:
    /// a breaking (forward-only) edge lies on the down path. The recipient is
    /// refused at the handshake (`onUpdateRequired`), never served here.
    Unreachable,
}

/// A resolved, parsed migration path between two versions in one direction —
/// the edge slice plus which way it is walked. Resolve it once (parsing each
/// edge on the way) and reuse it across a whole batch and every same-version
/// recipient, rather than re-parsing the chain per op.
pub struct Chain {
    edges: Vec<Migration>,
    up: bool,
}

/// Resolve and parse the migration path from `from` to `to` for `app_id`.
/// Identity when the versions match; forward when `to > from`; backward when
/// `to < from`, [`Unreachable`](TranslateError::Unreachable) if the down path
/// crosses a breaking edge. `MissingEdge` / `BadMigration` when the chain is
/// gapped or an edge does not parse.
///
/// Both versions are real, registered versions (`>= 1`): a chain starts at
/// version 1, and the handshake resolves the version-0 dynamic sentinel to the
/// head before an op is tagged, so 0 is not a creation version reaching here.
pub fn resolve_chain(
    reg: &SchemaRegistry,
    app_id: &[u8],
    from: u32,
    to: u32,
) -> Result<Chain, TranslateError> {
    match from.cmp(&to) {
        std::cmp::Ordering::Equal => Ok(Chain {
            edges: Vec::new(),
            up: true,
        }),
        std::cmp::Ordering::Less => Ok(Chain {
            edges: edge_slice(reg, app_id, from, to)?,
            up: true,
        }),
        std::cmp::Ordering::Greater => {
            let edges = edge_slice(reg, app_id, to, from)?;
            if !reachable_down(&edges) {
                return Err(TranslateError::Unreachable);
            }
            Ok(Chain { edges, up: false })
        }
    }
}

impl Chain {
    /// Rewrite one op along this resolved path. A down chain is only ever built
    /// when reachable, so the inverse rewrite is always defined.
    pub fn translate_op(&self, op: &Op) -> OpRewrite {
        if self.up {
            rewrite_up_along(&self.edges, op)
        } else {
            rewrite_down_along(&self.edges, op)
                .expect("a resolved down chain is reachable, so every op inverts")
        }
    }

    /// Rewrite a batch of ops for this recipient, dropping any the chain removes.
    ///
    /// A container-create ([`MapCreate`]/[`ListCreate`]/[`TextCreate`]) is carried
    /// verbatim, never key-rewritten, and never dropped by the chain. Per-op
    /// rewriting is key-local; it cannot see a container's descendants (an insert
    /// into it carries no field key, so a migration step never matches it).
    /// Dropping the create while keeping its descendants would strand them against
    /// a container that never arrives; rewriting the create's key would repoint it
    /// away from descendants that derive their element id from the original key.
    /// Either way the subtree tears. Carrying the create as-is keeps the subtree
    /// whole and internally consistent — a field the recipient's version does not
    /// model surfaces as an unknown slot its invariant repair elides, never a
    /// strand. Faithful subtree elision needs per-recipient element-set awareness,
    /// which this per-op seam does not have.
    ///
    /// An atomic transaction with a member this version cannot carry can never
    /// reach its `count` at the recipient, so its surviving members are
    /// **destranded** — each delivered with its tx tag stripped, applying
    /// standalone rather than buffering forever as members of a group that never
    /// completes. Delivering them (rather than dropping the group whole) is a
    /// convergence requirement: every op the recipient's version *can* represent
    /// must reach it, or the recipient diverges from the correct down-projection
    /// of the writer's state. The transaction's atomic-view boundary is lost at
    /// such a recipient — unavoidably, since it cannot see the member that could
    /// not cross — but the underlying ops still merge, so state converges. A fully
    /// carried transaction keeps its tags and stays atomic.
    ///
    /// [`MapCreate`]: crdtsync_core::OpKind::MapCreate
    /// [`ListCreate`]: crdtsync_core::OpKind::ListCreate
    /// [`TextCreate`]: crdtsync_core::OpKind::TextCreate
    /// The fate of a leaf slot at `key` under this chain — the state-level image
    /// of translating a key-bearing op at that key. A drop of the op is a
    /// [`SlotFate::Drop`], a key rewrite a [`SlotFate::Rename`], an unchanged
    /// keep a [`SlotFate::Keep`]. Drives the snapshot migration exactly as the
    /// op-rewrite drives the live/catch-up seam, so the two converge.
    pub fn translate_key(&self, key: &[u8]) -> SlotFate {
        match self.translate_op(&key_probe(key)) {
            OpRewrite::Drop => SlotFate::Drop,
            OpRewrite::Keep(out) => match out.kind {
                OpKind::MapSet { key: rekeyed, .. } if rekeyed != key => SlotFate::Rename(rekeyed),
                _ => SlotFate::Keep,
            },
        }
    }

    pub fn translate_ops(&self, ops: &[Op]) -> Vec<Op> {
        let rewritten: Vec<OpRewrite> = ops
            .iter()
            .map(|op| {
                if op.kind.creates_container() {
                    OpRewrite::Keep(op.clone())
                } else {
                    self.translate_op(op)
                }
            })
            .collect();
        // A transaction with any dropped member cannot reach its count here.
        let mut poisoned: HashSet<(ClientId, TxId)> = HashSet::new();
        for (op, r) in ops.iter().zip(&rewritten) {
            if matches!(r, OpRewrite::Drop) {
                if let Some(tx) = &op.tx {
                    poisoned.insert((op.id.client, tx.id));
                }
            }
        }
        ops.iter()
            .zip(rewritten)
            .filter_map(|(op, r)| {
                let out = match r {
                    OpRewrite::Keep(out) => out,
                    OpRewrite::Drop => return None,
                };
                let poisoned_group = op
                    .tx
                    .as_ref()
                    .is_some_and(|tx| poisoned.contains(&(op.id.client, tx.id)));
                // A survivor of a poisoned group is destranded so it applies
                // standalone; a survivor of an intact group keeps its tag.
                Some(if poisoned_group {
                    Op { tx: None, ..out }
                } else {
                    out
                })
            })
            .collect()
    }
}

/// Rewrite `op`, created under schema version `from`, for a recipient at `to` —
/// a convenience over [`resolve_chain`] for a single op. See it for the version
/// preconditions and error cases.
pub fn translate_op(
    reg: &SchemaRegistry,
    app_id: &[u8],
    op: &Op,
    from: u32,
    to: u32,
) -> Result<OpRewrite, TranslateError> {
    resolve_chain(reg, app_id, from, to).map(|chain| chain.translate_op(op))
}

/// Translate a batch of ops from `from` to `to` — a convenience over
/// [`resolve_chain`] + [`Chain::translate_ops`]. A broken or unreachable chain
/// drops the whole batch (fail-closed): the recipient receives nothing it
/// cannot be served correctly, pending the handshake range-check that refuses an
/// unreachable recipient outright.
pub fn translate_ops(
    reg: &SchemaRegistry,
    app_id: &[u8],
    ops: &[Op],
    from: u32,
    to: u32,
) -> Vec<Op> {
    match resolve_chain(reg, app_id, from, to) {
        Ok(chain) => chain.translate_ops(ops),
        Err(_) => Vec::new(),
    }
}

/// Translate a heterogeneous catch-up delta to a single recipient version `to`.
///
/// A catch-up delta is a slice of the room's log, so its ops may carry different
/// creation versions (mixed-version writers). Each op is translated from *its*
/// stored version to `to` along `app_id`'s chain. Consecutive ops at one version
/// are translated together — one resolved [`Chain`] per distinct source version
/// (cached across the delta) driving [`Chain::translate_ops`] — so ordering is
/// preserved and an atomic transaction (contiguous in the log, one version) stays
/// within a single run and keeps its all-or-nothing / destrand handling. A relay
/// op (no stored version) or an op already at `to` passes verbatim; a source
/// whose chain to `to` is broken or unreachable drops its run, fail-closed.
pub fn translate_delta(
    reg: &SchemaRegistry,
    app_id: &[u8],
    delta: Vec<StoredOp>,
    to: u32,
) -> Vec<Op> {
    let mut out = Vec::new();
    let mut chains: HashMap<u32, Option<Chain>> = HashMap::new();
    let mut records = delta.into_iter().peekable();
    while let Some(first) = records.next() {
        let version = first.schema_version;
        let mut run = vec![first.op];
        while records
            .peek()
            .is_some_and(|rec| rec.schema_version == version)
        {
            run.push(records.next().expect("peeked a record").op);
        }
        match version {
            Some(from) if from != to => {
                let chain = chains
                    .entry(from)
                    .or_insert_with(|| resolve_chain(reg, app_id, from, to).ok());
                if let Some(chain) = chain {
                    out.extend(chain.translate_ops(&run));
                }
            }
            _ => out.extend(run),
        }
    }
    out
}

/// Migrate a room snapshot (`Document::encode_state` bytes materialised at
/// version `from`) for a recipient at version `to`, mirroring the op seam so a
/// snapshot-served joiner converges with a peer served the same history as a
/// translated op delta. Each leaf slot's key is run through the same chain that
/// rewrites an op's key — dropped (an added field down, a removed field up),
/// re-keyed (a renamed field), or kept — while a container slot is carried
/// verbatim, exactly as [`Chain::translate_ops`] carries a container-create. A
/// same-version recipient, a migration that changed nothing, or bytes that
/// cannot be decoded or whose chain cannot be resolved are returned verbatim
/// (fail-safe); an unreachable down recipient is already refused at the
/// handshake, so the chain here is always reachable.
pub fn translate_snapshot(
    reg: &SchemaRegistry,
    app_id: &[u8],
    state: &[u8],
    from: u32,
    to: u32,
) -> Vec<u8> {
    if from == to {
        return state.to_vec();
    }
    let chain = match resolve_chain(reg, app_id, from, to) {
        Ok(chain) => chain,
        Err(_) => return state.to_vec(),
    };
    let mut doc = match Document::decode_state(state) {
        Ok(doc) => doc,
        Err(_) => return state.to_vec(),
    };
    if doc.migrate_leaf_slots(|key| chain.translate_key(key)) {
        doc.encode_state()
    } else {
        state.to_vec()
    }
}

/// A synthetic key-bearing op, so a slot's key can be run through the same
/// rewrite an op of that key would take. Only the key is read back; the id,
/// stamp, target, and value are inert.
fn key_probe(key: &[u8]) -> Op {
    Op::new(
        OpId {
            client: ClientId::from_bytes([0; 16]),
            seq: 0,
        },
        Stamp {
            lamport: 0,
            client: ClientId::from_bytes([0; 16]),
        },
        ElementId::from_bytes([0; 16]),
        OpKind::MapSet {
            key: key.to_vec(),
            value: Scalar::Null,
        },
    )
}

/// Whether a recipient at `to` can be reached from an op created at `from`.
/// Forward (`to >= from`) always, once the edges are registered; down only when
/// every edge on the path is back-compatible. A `MissingEdge` / `BadMigration`
/// on the path is an error, not a `false` — the chain is broken, distinct from
/// an intact-but-breaking gap.
pub fn reachable(
    reg: &SchemaRegistry,
    app_id: &[u8],
    from: u32,
    to: u32,
) -> Result<bool, TranslateError> {
    if to == from {
        return Ok(true);
    }
    if to > from {
        edge_slice(reg, app_id, from, to)?;
        Ok(true)
    } else {
        let edges = edge_slice(reg, app_id, to, from)?;
        Ok(reachable_down(&edges))
    }
}

/// The ascending, contiguous edge slice reaching versions `(low, high]` — the
/// migration stored at each version `low + 1 ..= high`, parsed. `low == high`
/// is the empty slice. The registry keeps a chain contiguous from version 1, so
/// this is the ascending path both `rewrite_up_along` and `rewrite_down_along`
/// expect (each walks it in its own direction).
fn edge_slice(
    reg: &SchemaRegistry,
    app_id: &[u8],
    low: u32,
    high: u32,
) -> Result<Vec<Migration>, TranslateError> {
    let mut edges = Vec::new();
    // Exclusive `low`, inclusive `high`, incrementing rather than a `low + 1`
    // range so the top of the version space never overflows. Empty when
    // `low >= high`.
    let mut version = low;
    while version < high {
        version += 1;
        let bytes = reg
            .migration(app_id, version)
            .ok_or(TranslateError::MissingEdge { version })?;
        let src =
            std::str::from_utf8(bytes).map_err(|_| TranslateError::BadMigration { version })?;
        let edge = Migration::parse(src).map_err(|_| TranslateError::BadMigration { version })?;
        edges.push(edge);
    }
    Ok(edges)
}
