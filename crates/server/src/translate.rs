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

use crdtsync_core::migration::{
    reachable_down, rewrite_down_along, rewrite_up_along, Migration, OpRewrite,
};
use crdtsync_core::Op;

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

/// Rewrite `op`, created under schema version `from`, for a recipient served at
/// version `to`. Identity when `from == to`; composes the chain forward when
/// `to > from`; inverts it when `to < from`, [`Unreachable`] if any edge on the
/// down path is breaking. A `Drop` propagates.
///
/// Both versions are real, registered versions (`>= 1`): a chain starts at
/// version 1, and the handshake resolves the version-0 dynamic sentinel to the
/// head before an op is ever tagged, so 0 is not a creation version reaching
/// here.
///
/// [`Unreachable`]: TranslateError::Unreachable
pub fn translate_op(
    reg: &SchemaRegistry,
    app_id: &[u8],
    op: &Op,
    from: u32,
    to: u32,
) -> Result<OpRewrite, TranslateError> {
    match from.cmp(&to) {
        std::cmp::Ordering::Equal => Ok(OpRewrite::Keep(op.clone())),
        std::cmp::Ordering::Less => {
            let edges = edge_slice(reg, app_id, from, to)?;
            Ok(rewrite_up_along(&edges, op))
        }
        std::cmp::Ordering::Greater => {
            let edges = edge_slice(reg, app_id, to, from)?;
            rewrite_down_along(&edges, op).ok_or(TranslateError::Unreachable)
        }
    }
}

/// Translate a batch of ops created at version `from` for a recipient at `to`,
/// keeping each op's rewritten image and dropping any the chain removes. An op
/// the recipient cannot be served — a `Drop`, or an error (an unreachable
/// breaking gap, a chain gap, an unparseable edge) — is omitted rather than
/// delivered wrong; the handshake range-check (a later slice) refuses an
/// unreachable recipient outright, so a drop here is a safe interim.
pub fn translate_ops(
    reg: &SchemaRegistry,
    app_id: &[u8],
    ops: &[Op],
    from: u32,
    to: u32,
) -> Vec<Op> {
    ops.iter()
        .filter_map(|op| match translate_op(reg, app_id, op, from, to) {
            Ok(OpRewrite::Keep(op)) => Some(op),
            Ok(OpRewrite::Drop) | Err(_) => None,
        })
        .collect()
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
