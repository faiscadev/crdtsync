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

use std::collections::HashSet;

use crdtsync_core::migration::{
    reachable_down, rewrite_down_along, rewrite_up_along, Migration, OpRewrite,
};
use crdtsync_core::op::TxId;
use crdtsync_core::{ClientId, Op};

use crate::schema_registry::SchemaRegistry;

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
