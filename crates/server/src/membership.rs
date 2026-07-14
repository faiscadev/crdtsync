//! A node's live view of its cluster's membership.
//!
//! A node seeds its members from config — its own advertise address (or an
//! explicit node id) plus a seed-peer list — with no discovery service. It holds
//! a [`Membership`]: its own [`NodeId`], the canonical member set (`{self} ∪
//! peers`), each member's dial address, and the [`Cluster`] placement built from
//! them. The node's view is just the shared placement evaluated for its own id,
//! so `owns`/`is_primary_for` never diverge from what another node computes for
//! the same room. The routing (Unit 3) and replication (Unit 4) layers consult
//! this.
//!
//! The member set is *dynamic*: gossip membership discovery (Unit 7) grows it by
//! anti-entropy — a node need only know one seed peer at boot, then learns the
//! rest by [`add_member`](Membership::add_member) unioning in the members a peer
//! advertises. Placement stays deterministic and order-independent: the member
//! set is canonicalized (sorted, de-duplicated) before the [`Cluster`] is built,
//! so two nodes that have learned the same set place every room identically no
//! matter the order they learned it in. A node also tracks each member's
//! *liveness* — a peer whose relay link is down is skipped when electing a room's
//! effective leader (failover, Unit 6a), so a dead placement primary's rooms
//! promote to the next live replica rather than stranding.

use std::collections::{HashMap, HashSet};

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
    /// Each member's advertise (dial) address, keyed by node id — what a node
    /// gossips so a peer can dial a member it just learned. A member derived from
    /// its address (every seed peer) maps to that address verbatim; `self` maps to
    /// its configured advertise address when one was given, else its node-id bytes.
    addrs: HashMap<NodeId, Vec<u8>>,
}

impl Membership {
    /// A membership over `self_id` and `peers`. Self is always a member, so it is
    /// added to the peer set (duplicates collapse in the canonical [`Cluster`]).
    pub fn new(
        self_id: NodeId,
        peers: impl IntoIterator<Item = NodeId>,
        replication_factor: usize,
    ) -> Self {
        let members: Vec<NodeId> = std::iter::once(self_id.clone()).chain(peers).collect();
        // A member seeded without a distinct advertise address dials at its node id
        // — the identity every seed peer is derived from (`NodeId::from_addr`), so
        // the id and the dial address coincide. `self` overrides this in
        // `from_static_config` when a separate advertise address was configured.
        let addrs = members
            .iter()
            .map(|node| (node.clone(), node.as_bytes().to_vec()))
            .collect();
        Self {
            self_id,
            cluster: Cluster::new(members),
            replication_factor,
            down: HashSet::new(),
            addrs,
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
        let mut membership = Self::new(self_id.clone(), peers, replication_factor);
        // A node configured with an explicit id *and* a separate advertise address
        // dials at that address, not at its id — record it so gossip advertises the
        // dialable address for self.
        if let Some(addr) = advertise_addr {
            membership.addrs.insert(self_id, addr.as_bytes().to_vec());
        }
        Ok(membership)
    }

    /// The node's own id.
    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// The canonical (sorted, de-duplicated) member set, self included.
    pub fn members(&self) -> &[NodeId] {
        self.cluster.nodes()
    }

    /// Learn a member, dialable at `addr` — the anti-entropy union gossip applies
    /// for each `(node, addr)` pair a peer advertises. Idempotent: a member already
    /// known leaves the set and its placement untouched (no churn on re-gossip of a
    /// fully-known set); a genuinely new member is added and the [`Cluster`]
    /// placement rebuilt from the canonicalized set, so every node that has learned
    /// the same members places every room identically regardless of learning order.
    /// A node never learns itself anew — `self` is a member from construction.
    pub fn add_member(&mut self, node: NodeId, addr: Vec<u8>) {
        if self.cluster.nodes().contains(&node) {
            return;
        }
        self.addrs.insert(node.clone(), addr);
        let members = self.cluster.nodes().iter().cloned().chain(Some(node));
        self.cluster = Cluster::new(members);
    }

    /// The members this node knows, each with its dial address — the payload a
    /// node gossips. Canonical order (the sorted member set), so the advertisement
    /// is deterministic. A member's address falls back to its node-id bytes if none
    /// was recorded, keeping every member dialable.
    pub fn known_members(&self) -> Vec<(NodeId, Vec<u8>)> {
        self.cluster
            .nodes()
            .iter()
            .map(|node| {
                let addr = self
                    .addrs
                    .get(node)
                    .cloned()
                    .unwrap_or_else(|| node.as_bytes().to_vec());
                (node.clone(), addr)
            })
            .collect()
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
