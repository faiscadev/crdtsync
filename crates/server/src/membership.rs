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
//! matter the order they learned it in.
//!
//! A node also tracks each member's *liveness* from two independent signals,
//! unioned so any evidence of death excludes a member from room leadership:
//!
//!  - the **relay-link** signal (Unit 6a): a peer whose inter-node replication link
//!    is down is marked down. Connection-local — only the node whose link dropped
//!    observes it.
//!  - the **gossip** signal (Unit 7b): a SWIM-style [`MemberState`] per member,
//!    disseminated by anti-entropy gossip. A node that misses enough direct gossip
//!    probes to a member escalates it `Alive → Suspect → Dead`; the state (and a
//!    monotonic per-node *incarnation* that lets a node refute a false suspicion of
//!    itself) rides every gossip frame, so a `Dead` verdict reaches every node —
//!    cluster-wide, not connection-local.
//!
//! A member is *live* iff its relay link is up **and** gossip has not declared it
//! `Dead`. [`effective_primary_for`](Membership::effective_primary_for) elects the
//! first live replica in HRW order, so a dead placement primary's rooms promote to
//! the next live replica rather than stranding, and a refuted or recovered node
//! reclaims them.

use std::collections::{HashMap, HashSet};

use crdtsync_core::MemberState;

use crate::placement::{Cluster, NodeId};

/// Consecutive failed direct gossip probes to a member before this node escalates
/// it from `Alive` to `Suspect` — enough that a single dropped round is not a false
/// positive, few enough that a genuine failure is doubted within a handful of
/// gossip rounds.
pub const SUSPECT_AFTER_FAILURES: u32 = 3;

/// Consecutive failed direct gossip probes before a `Suspect` member is declared
/// `Dead` and excluded from leadership. The gap above [`SUSPECT_AFTER_FAILURES`] is
/// the refutation window: a live member falsely suspected has this many further
/// rounds to bump its incarnation and re-disseminate `Alive` before it is evicted.
pub const DEAD_AFTER_FAILURES: u32 = 6;

/// A member's gossip liveness: its SWIM state, the incarnation that state was
/// asserted at, and this node's count of consecutive failed direct probes to it
/// (the local escalation clock — reset by any success or fresher gossip).
#[derive(Clone, Debug)]
struct MemberLiveness {
    incarnation: u64,
    state: MemberState,
    failed_probes: u32,
}

impl MemberLiveness {
    /// A freshly-learned member: alive at the incarnation it was advertised with,
    /// with no failed probes yet.
    fn new(incarnation: u64, state: MemberState) -> Self {
        Self {
            incarnation,
            state,
            failed_probes: 0,
        }
    }
}

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
    /// The members whose inter-node relay link is currently DOWN (Unit 6a). Empty
    /// by default (every peer optimistically live until an observed dial failure or
    /// link drop), so a steady-state cluster's effective leadership is byte-
    /// identical to its placement. `self` is never in this set: a node is always
    /// live to itself. Unioned with the gossip signal below in [`is_live`](Self::is_live).
    relay_down: HashSet<NodeId>,
    /// Each member's gossip liveness (Unit 7b): its SWIM [`MemberState`] and
    /// incarnation, keyed by node id, `self` included (always `Alive`, at the
    /// incarnation it bumps to refute a false suspicion of itself). A member
    /// reaching `Dead` is excluded from effective leadership cluster-wide.
    liveness: HashMap<NodeId, MemberLiveness>,
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
        // Every seeded member starts alive at incarnation 0 — the optimistic
        // default gossip then either confirms or escalates.
        let liveness = members
            .iter()
            .map(|node| (node.clone(), MemberLiveness::new(0, MemberState::Alive)))
            .collect();
        Self {
            self_id,
            cluster: Cluster::new(members),
            replication_factor,
            relay_down: HashSet::new(),
            liveness,
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

    /// Learn a member, dialable at `addr` — the anti-entropy union gossip applies
    /// for each `(node, addr)` pair a peer advertises. See [`add_members`](Self::add_members).
    pub fn add_member(&mut self, node: NodeId, addr: Vec<u8>) {
        self.add_members(std::iter::once((node, addr)));
    }

    /// Union a batch of learned members in, rebuilding the [`Cluster`] placement
    /// once if any were genuinely new. Idempotent: a member already known is
    /// skipped, so a re-gossip of a fully-known set rebuilds no placement (no
    /// churn). A member with an empty node id is dropped — it is neither placeable
    /// nor dialable, so a malformed gossip pair cannot poison the set. When new
    /// members land, placement is rebuilt from the canonicalized (sorted, de-duped)
    /// set, so every node that has learned the same members places every room
    /// identically regardless of the order it learned them in. `self` is a member
    /// from construction and is never relearned.
    pub fn add_members(&mut self, members: impl IntoIterator<Item = (NodeId, Vec<u8>)>) {
        let mut added = false;
        for (node, addr) in members {
            if node.as_bytes().is_empty() || self.addrs.contains_key(&node) {
                continue;
            }
            self.liveness
                .insert(node.clone(), MemberLiveness::new(0, MemberState::Alive));
            self.addrs.insert(node, addr);
            added = true;
        }
        if added {
            self.cluster = Cluster::new(self.addrs.keys().cloned());
        }
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

    /// Whether `node` is a member this view knows — in the canonical member set.
    /// A node this view has never learned is not one it can vouch for, so an
    /// indirect probe about it is answered unreachable rather than optimistically
    /// alive.
    pub fn is_member(&self, node: &NodeId) -> bool {
        self.addrs.contains_key(node)
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

    /// Mark `node`'s relay link reachable again — it connected (Unit 6a). Clears
    /// only the relay-link signal; a node gossip has declared `Dead` stays down
    /// until gossip refutes it. No-op for a link already up.
    pub fn mark_node_live(&mut self, node: &NodeId) {
        self.relay_down.remove(node);
    }

    /// Mark `node`'s relay link down — it dropped or failed to dial (Unit 6a) — so
    /// it is skipped when electing a room's effective leader. `self` is never marked
    /// down: a node is always live to itself.
    pub fn mark_node_down(&mut self, node: &NodeId) {
        if !self.is_self(node) {
            self.relay_down.insert(node.clone());
        }
    }

    /// Whether `node` is currently reachable, unioning both liveness signals: live
    /// iff its relay link is up **and** gossip has not declared it `Dead`. Either
    /// signal alone marking it down excludes it, so neither can mask the other — a
    /// gossip-`Alive` does not resurrect a relay-down node, nor a relay reconnect a
    /// gossip-`Dead` one. `self` is always live. A `Suspect` member is still live:
    /// suspicion routes optimistically until it hardens to `Dead`.
    pub fn is_live(&self, node: &NodeId) -> bool {
        self.is_self(node)
            || (!self.relay_down.contains(node) && self.gossip_state(node) != MemberState::Dead)
    }

    /// `node`'s current gossip [`MemberState`] — `Alive` for `self` or a member
    /// this node has no liveness record for.
    pub fn gossip_state(&self, node: &NodeId) -> MemberState {
        if self.is_self(node) {
            return MemberState::Alive;
        }
        self.liveness
            .get(node)
            .map(|m| m.state)
            .unwrap_or(MemberState::Alive)
    }

    /// `node`'s current incarnation — its own refutation counter for `self`, else
    /// the highest incarnation this node has heard the member asserted at. `0` for
    /// an unknown member.
    pub fn incarnation(&self, node: &NodeId) -> u64 {
        self.liveness.get(node).map(|m| m.incarnation).unwrap_or(0)
    }

    /// The gossip liveness payload this node advertises: every known member with
    /// its dial address, current incarnation, and state — canonical (sorted) order,
    /// so the advertisement is deterministic. `self` rides at its own incarnation,
    /// always `Alive`.
    pub fn known_liveness(&self) -> Vec<(NodeId, Vec<u8>, u64, MemberState)> {
        self.cluster
            .nodes()
            .iter()
            .map(|node| {
                let addr = self
                    .addrs
                    .get(node)
                    .cloned()
                    .unwrap_or_else(|| node.as_bytes().to_vec());
                (
                    node.clone(),
                    addr,
                    self.incarnation(node),
                    self.gossip_state(node),
                )
            })
            .collect()
    }

    /// Record a *successful* direct gossip exchange with `node` — first-hand proof
    /// it is alive. Clears its failed-probe count and restores it to `Alive` at the
    /// known incarnation. It does **not** bump the incarnation — only the member
    /// itself refutes a suspicion with a bump. On its own, restoring `Alive` at the
    /// same incarnation is inert against a suspicion others already gossiped (an
    /// equal-incarnation `Suspect`/`Dead` re-wins the next [`merge_liveness`]). That
    /// is safe because this call is always **paired** with that same round's push:
    /// the successful exchange sent this node's view (carrying the stale suspicion of
    /// `node`) *to* `node`, so `node` sees the suspicion of itself and refutes with a
    /// higher incarnation, and its reply — merged immediately after this call — lifts
    /// it to that refuted `Alive`. The reset here keeps the interim view live until
    /// the refutation lands. No-op for `self`.
    pub fn note_gossip_reachable(&mut self, node: &NodeId) {
        if self.is_self(node) {
            return;
        }
        if let Some(m) = self.liveness.get_mut(node) {
            m.failed_probes = 0;
            m.state = MemberState::Alive;
        }
    }

    /// Record a *failed* direct gossip exchange with `node` (dial, handshake, or
    /// reply timeout). Each failure counts toward suspicion: at
    /// [`SUSPECT_AFTER_FAILURES`] the member escalates `Alive → Suspect`, at
    /// [`DEAD_AFTER_FAILURES`] `Suspect → Dead`. `self` is never suspected.
    pub fn note_gossip_unreachable(&mut self, node: &NodeId) {
        if self.is_self(node) {
            return;
        }
        let Some(m) = self.liveness.get_mut(node) else {
            return;
        };
        if m.state == MemberState::Dead {
            return;
        }
        m.failed_probes = m.failed_probes.saturating_add(1);
        if m.failed_probes >= DEAD_AFTER_FAILURES {
            m.state = MemberState::Dead;
        } else if m.failed_probes >= SUSPECT_AFTER_FAILURES {
            m.state = MemberState::Suspect;
        }
    }

    /// Merge a gossiped liveness payload into this node's view — the SWIM anti-
    /// entropy of failure detection. For each `(node, addr, incarnation, state)`:
    ///
    ///  - a member this node does not know is learned (address recorded, placement
    ///    rebuilt) at the advertised incarnation and state — the same union
    ///    [`add_members`](Self::add_members) performs, now carrying liveness;
    ///  - for a known member, a strictly higher incarnation always wins (adopting
    ///    its state and clearing local suspicion, since it is fresher information);
    ///    at equal incarnation the more-suspicious state wins (`Dead > Suspect >
    ///    Alive`), so a detected failure disseminates rather than being masked by a
    ///    stale `Alive`;
    ///  - a tuple reporting *`self`* as `Suspect`/`Dead` (or at an incarnation at or
    ///    above this node's own) is a false positive this node **refutes**: it bumps
    ///    its own incarnation above the received one and re-asserts `Alive`, so its
    ///    correction wins everywhere the stale suspicion reached.
    ///
    /// A malformed pair (empty node id) is dropped, as on the additive path. Order-
    /// independent and idempotent: two nodes that received the same updates in any
    /// order converge on the same liveness.
    pub fn merge_liveness(
        &mut self,
        payload: impl IntoIterator<Item = (NodeId, Vec<u8>, u64, MemberState)>,
    ) {
        let mut rebuilt = false;
        for (node, addr, incarnation, state) in payload {
            if node.as_bytes().is_empty() {
                continue;
            }
            if self.is_self(&node) {
                self.refute_if_stale(incarnation, state);
                continue;
            }
            match self.liveness.get_mut(&node) {
                None => {
                    self.liveness
                        .insert(node.clone(), MemberLiveness::new(incarnation, state));
                    self.addrs.insert(node, addr);
                    rebuilt = true;
                }
                Some(m) => {
                    if incarnation > m.incarnation {
                        m.incarnation = incarnation;
                        m.state = state;
                        m.failed_probes = 0;
                    } else if incarnation == m.incarnation && state > m.state {
                        m.state = state;
                    }
                }
            }
        }
        if rebuilt {
            self.cluster = Cluster::new(self.addrs.keys().cloned());
        }
    }

    /// Refute a stale suspicion of `self`: if a peer reported this node `Suspect`/
    /// `Dead`, or asserted anything about it at an incarnation at or above this
    /// node's own, bump the incarnation above the received one and re-assert
    /// `Alive`. A higher-incarnation `Alive` beats the stale state everywhere it
    /// propagates, overriding the false positive.
    fn refute_if_stale(&mut self, received: u64, state: MemberState) {
        let me = self
            .liveness
            .get_mut(&self.self_id)
            .expect("self is always tracked");
        if received > me.incarnation || (received == me.incarnation && state != MemberState::Alive)
        {
            me.incarnation = received.max(me.incarnation).saturating_add(1);
            me.state = MemberState::Alive;
            me.failed_probes = 0;
        }
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
