//! Deterministic room→replica-set placement for the horizontal-scaling cluster.
//!
//! Every node must independently compute the *same* ordered replica set for a
//! room, from only the room id and the current member set — the cluster layer
//! is internal, with no external coordination service to hold a shared ring.
//! [`Cluster`] answers `room_id → ordered replica set` by rendezvous
//! (highest-random-weight) hashing: each member is scored for the room and the
//! `n` highest-scoring members are the replicas, the highest the primary. This
//! needs no virtual-node ring, distributes rooms evenly across members, and on
//! a membership change moves only a member's share of rooms.

/// A cluster member — an opaque node id or address as bytes.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct NodeId(Vec<u8>);

impl NodeId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Derive a node's id from its advertise address. The id is the trimmed
    /// address bytes verbatim — a pure function of the address, so every node
    /// configured with the same peer address string derives the identical id and
    /// the cluster agrees on placement. Trimming absorbs padding from the
    /// comma-separated peer-list carrier.
    pub fn from_addr(addr: &str) -> Self {
        Self(addr.trim().as_bytes().to_vec())
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<Vec<u8>> for NodeId {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

/// A snapshot of cluster membership that maps rooms to ordered replica sets.
///
/// Members are held in a canonical (sorted, de-duplicated) form, so two
/// clusters built from the same members in any order are identical and place
/// every room the same way.
#[derive(Clone, Debug)]
pub struct Cluster {
    nodes: Vec<NodeId>,
}

impl Cluster {
    /// Build a cluster from a member set. Duplicate members collapse; input
    /// order is irrelevant to placement.
    pub fn new(members: impl IntoIterator<Item = NodeId>) -> Self {
        let mut nodes: Vec<NodeId> = members.into_iter().collect();
        nodes.sort();
        nodes.dedup();
        Self { nodes }
    }

    /// The canonical member set.
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The ordered replica set for `room_id`: the `n` highest-weight members,
    /// primary (leader-candidate) first, followers after. Fewer than `n`
    /// members yields all of them; an empty cluster or `n == 0` yields an empty
    /// set. The order is total and identical on every node.
    pub fn replicas(&self, room_id: &[u8], n: usize) -> Vec<NodeId> {
        let take = n.min(self.nodes.len());
        if take == 0 {
            return Vec::new();
        }
        let seed = room_seed(room_id);
        let mut ranked: Vec<(u64, &NodeId)> = self
            .nodes
            .iter()
            .map(|node| (weight(seed, node.as_bytes()), node))
            .collect();
        // Highest weight first; equal weights break by node id so the order is
        // total and every node agrees.
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        ranked
            .into_iter()
            .take(take)
            .map(|(_, node)| node.clone())
            .collect()
    }

    /// The primary (leader-candidate) for `room_id`, or `None` if the cluster
    /// is empty.
    pub fn primary(&self, room_id: &[u8]) -> Option<NodeId> {
        self.replicas(room_id, 1).into_iter().next()
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a over the room id — the per-room seed each member's weight continues
/// from, so every member of a room is scored against the same room hash.
fn room_seed(room_id: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in room_id {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// The rendezvous weight of a member for a room: FNV-1a continues from the room
/// seed over the node id, then a SplitMix64 finalizer avalanches the bits so
/// weights spread evenly and stay uncorrelated across rooms. Fixed-width
/// wrapping arithmetic only — the same value on every platform.
fn weight(room_seed: u64, node_id: &[u8]) -> u64 {
    let mut h = room_seed;
    for &b in node_id {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    mix64(h)
}

fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
