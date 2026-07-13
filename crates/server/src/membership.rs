//! A node's live view of its cluster's static membership.
//!
//! A node learns its members from static config — its own advertise address (or
//! an explicit node id) plus a peer list — with no discovery service. It holds a
//! [`Membership`]: its own [`NodeId`], the canonical member set (`{self} ∪
//! peers`), and the [`Cluster`] placement built from them. The node's view is
//! just the shared placement evaluated for its own id, so `owns`/`is_primary_for`
//! never diverge from what another node computes for the same room. The routing
//! (Unit 3) and replication (Unit 4) layers consult this. The member set is fixed
//! for the process lifetime, but a node tracks each member's *liveness* — a peer
//! whose relay link is down is skipped when electing a room's effective leader
//! (failover, Unit 6a), so a dead placement primary's rooms promote to the next
//! live replica rather than stranding. Membership discovery by gossip is Unit 7.

use std::collections::HashSet;

use crate::placement::{Cluster, NodeId};

/// The default per-room replication factor: the number of members that hold each
/// room, primary first. Clamps to the member count, so a small cluster resolves.
pub const DEFAULT_REPLICATION_FACTOR: usize = 3;

/// A malformed static membership configuration, surfaced at startup instead of a
/// panic or a silently wrong member set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipConfigError {
    /// A blank entry in the peer list (e.g. a doubled or trailing comma) — a
    /// config typo, not an anonymous member.
    EmptyPeer,
    /// Peers were configured but the node has no advertise address or node id, so
    /// it cannot place itself in its own cluster.
    MissingSelfId,
}

impl std::fmt::Display for MembershipConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MembershipConfigError::EmptyPeer => {
                write!(f, "cluster peer list has a blank entry")
            }
            MembershipConfigError::MissingSelfId => write!(
                f,
                "cluster peers configured but no node id or advertise address for self"
            ),
        }
    }
}

impl std::error::Error for MembershipConfigError {}

/// A node's view of its cluster: its own id, the canonical member set, and the
/// placement over them. Every membership query is the shared [`Cluster`]
/// placement evaluated for `self_id`, so two nodes built from the same member set
/// answer identically for any room.
#[derive(Clone, Debug)]
pub struct Membership {
    self_id: NodeId,
    cluster: Cluster,
    replication_factor: usize,
    /// The members currently believed DOWN — unreachable over their inter-node
    /// relay link. Empty by default (every peer optimistically live until an
    /// observed dial failure or link drop marks it down), so a steady-state
    /// cluster's effective leadership is byte-identical to its placement. `self`
    /// is never in this set: a node is always live to itself.
    down: HashSet<NodeId>,
}

impl Membership {
    /// A membership over `self_id` and `peers`. Self is always a member, so it is
    /// added to the peer set (duplicates collapse in the canonical [`Cluster`]).
    pub fn new(
        self_id: NodeId,
        peers: impl IntoIterator<Item = NodeId>,
        replication_factor: usize,
    ) -> Self {
        let members = std::iter::once(self_id.clone()).chain(peers);
        Self {
            self_id,
            cluster: Cluster::new(members),
            replication_factor,
            down: HashSet::new(),
        }
    }

    /// Build the node's membership from static config values, as read from the
    /// `CRDTSYNC_*` environment. Self's id is `node_id` if given, else derived
    /// from `advertise_addr`; with neither, [`MissingSelfId`](MembershipConfigError::MissingSelfId).
    /// `peers` is the raw comma-separated advertise-address list — empty or all
    /// whitespace yields single-node membership (self only); a blank entry is
    /// [`EmptyPeer`](MembershipConfigError::EmptyPeer).
    pub fn from_static_config(
        node_id: Option<&str>,
        advertise_addr: Option<&str>,
        peers: &str,
        replication_factor: usize,
    ) -> Result<Self, MembershipConfigError> {
        // Trim both self carriers and treat an empty value as absent, so a blank
        // env var (`CRDTSYNC_ADVERTISE_ADDR=`) fails with `MissingSelfId` rather
        // than joining under a zero-length id, and a padded `CRDTSYNC_NODE_ID`
        // derives the same id every peer's trimmed `from_addr` does.
        let node_id = node_id.map(str::trim).filter(|s| !s.is_empty());
        let advertise_addr = advertise_addr.map(str::trim).filter(|s| !s.is_empty());
        let self_id = match (node_id, advertise_addr) {
            (Some(id), _) => NodeId::from(id),
            (None, Some(addr)) => NodeId::from_addr(addr),
            (None, None) => return Err(MembershipConfigError::MissingSelfId),
        };
        let peers = parse_peers(peers)?;
        Ok(Self::new(self_id, peers, replication_factor))
    }

    /// The node's own id.
    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// The canonical (sorted, de-duplicated) member set, self included.
    pub fn members(&self) -> &[NodeId] {
        self.cluster.nodes()
    }

    /// Whether `node` is this node.
    pub fn is_self(&self, node: &NodeId) -> bool {
        &self.self_id == node
    }

    /// The per-room replication factor this view places with.
    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    /// The shared placement over the member set.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// The ordered replica set for `room`, primary first — the placement Unit 3
    /// routing and Unit 4 replication consult.
    pub fn replicas_for(&self, room: &[u8]) -> Vec<NodeId> {
        self.cluster.replicas(room, self.replication_factor)
    }

    /// The primary (leader) for `room`, or `None` for an empty cluster.
    pub fn primary_for(&self, room: &[u8]) -> Option<NodeId> {
        self.cluster.primary(room)
    }

    /// Whether this node is in `room`'s replica set — whether it holds the room.
    pub fn owns(&self, room: &[u8]) -> bool {
        self.replicas_for(room).contains(&self.self_id)
    }

    /// Whether this node is the primary (leader) for `room`.
    pub fn is_primary_for(&self, room: &[u8]) -> bool {
        self.primary_for(room).as_ref() == Some(&self.self_id)
    }

    /// Mark `node` reachable again — its relay link connected. A live node is a
    /// candidate for effective leadership once more; a recovered placement primary
    /// reclaims its rooms. No-op for a node already live.
    pub fn mark_node_live(&mut self, node: &NodeId) {
        self.down.remove(node);
    }

    /// Mark `node` unreachable — its relay link dropped or failed to dial — so it
    /// is skipped when electing a room's effective leader. `self` is never marked
    /// down: a node is always live to itself.
    pub fn mark_node_down(&mut self, node: &NodeId) {
        if !self.is_self(node) {
            self.down.insert(node.clone());
        }
    }

    /// Whether `node` is currently reachable. `self` is always live.
    pub fn is_live(&self, node: &NodeId) -> bool {
        self.is_self(node) || !self.down.contains(node)
    }

    /// The effective leader for `room` under liveness: the first replica in
    /// `replicas_for` (HRW order) that is currently live. Equal to
    /// [`primary_for`](Self::primary_for) while every replica is up — only a down
    /// placement primary promotes the next live replica. `None` for an empty
    /// cluster, or when every replica of the room is down (self, always live, is a
    /// candidate whenever it holds the room).
    pub fn effective_primary_for(&self, room: &[u8]) -> Option<NodeId> {
        self.replicas_for(room)
            .into_iter()
            .find(|node| self.is_live(node))
    }

    /// Whether this node is `room`'s effective (live) leader — the liveness-aware
    /// counterpart to [`is_primary_for`](Self::is_primary_for). True when the
    /// placement primary is up and is self, or when self is the promoted next-live
    /// replica.
    pub fn is_effective_primary_for(&self, room: &[u8]) -> bool {
        self.effective_primary_for(room).as_ref() == Some(&self.self_id)
    }
}

/// Parse a comma-separated peer advertise-address list into member ids. Entries
/// are trimmed; an empty or all-whitespace list is no peers; a blank entry
/// between commas is [`EmptyPeer`](MembershipConfigError::EmptyPeer).
fn parse_peers(list: &str) -> Result<Vec<NodeId>, MembershipConfigError> {
    if list.trim().is_empty() {
        return Ok(Vec::new());
    }
    list.split(',')
        .map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                Err(MembershipConfigError::EmptyPeer)
            } else {
                Ok(NodeId::from_addr(entry))
            }
        })
        .collect()
}
