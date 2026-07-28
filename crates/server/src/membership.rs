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
//!
//! Learning a member and *placing* rooms on it are two different admissions. The
//! roster above is what a node dials, probes and gossips; the [`Cluster`] placement
//! is built from the **adopted** members only, and a member learned by gossip is
//! held *pending* until the cluster has vouched for it. Placement is HRW over the
//! member set — a pure and publicly computable function — so which rooms a node
//! replicates follows from its node id, and an unchecked join path would let a node
//! mint an id that places it into any room's replica set. Pending keeps such a node
//! dialable and gossipable (so it converges, and so a genuine joiner is reached)
//! while it holds no room and counts toward no room's quorum.
//!
//! Adoption is a decision the *cluster* makes, never one node: placement must be
//! identical everywhere or the ring splits, so it cannot be a local predicate over a
//! shared member set. A node records only what it knows first-hand — that it has
//! completed an identity-checked peer link to a member ([`note_verified`]) — and
//! that claim rides gossip attributed to the node that made it. A member is adopted
//! once [`ADOPTION_VERIFIERS`] *already-adopted* members have verified it, so the
//! evidence is a grow-only set that merges the same way liveness does and converges
//! on the same anti-entropy. A member never verifies itself, so no node can place
//! itself; the members a node was *configured* with are adopted from birth, since
//! the operator's config is the root of trust the cluster starts from.
//!
//! [`note_verified`]: Membership::note_verified

use std::collections::{HashMap, HashSet};

use crdtsync_core::MemberState;

use crate::dial::member_host;
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

/// How many reap checks a member must stay `Dead` through before it is reaped —
/// removed from the roster entirely. One reap check runs per membership sweep, so
/// this is a dead-time in sweep intervals. Comfortably longer than the death and
/// refutation windows above, so a flapping or briefly-partitioned node reclaims its
/// place (its incarnation refutation resets the clock) long before it would be
/// reaped; only a durably-gone node crosses it.
pub const REAP_AFTER_DEAD_TICKS: u32 = 30;

/// How many reap checks a reap *tombstone* is retained before it is pruned — the
/// reaped member forgotten entirely. A tombstone keeps stale `Dead`/`Suspect` gossip
/// from resurrecting a reaped member (no reap-then-resurrect flap); it must outlive
/// any such in-flight gossip. This retention is an order of magnitude past
/// [`REAP_AFTER_DEAD_TICKS`], and a reaped member is dropped from the roster so it is
/// no longer gossiped — so by the time a tombstone is pruned every replica has long
/// since reaped the member and stopped referencing it, and no live gossip can still
/// name it. A member that reappears after its tombstone is pruned is treated as a
/// fresh join, which is safe: only a reachable node produces an `Alive` return, and
/// the SWIM incarnation/liveness merge reconciles it as a new member. Pruning is
/// convergent (every replica prunes on the same tick-count rule) and idempotent.
pub const TOMBSTONE_RETENTION_TICKS: u32 = 300;

/// A member's gossip liveness: its SWIM state, the incarnation that state was
/// asserted at, this node's count of consecutive failed direct probes to it (the
/// local escalation clock — reset by any success or fresher gossip), and the count
/// of reap checks it has stayed `Dead` through (the reap clock — zero unless `Dead`,
/// reset whenever it leaves `Dead`).
#[derive(Clone, Debug)]
struct MemberLiveness {
    incarnation: u64,
    state: MemberState,
    failed_probes: u32,
    dead_ticks: u32,
}

impl MemberLiveness {
    /// A freshly-learned member: alive at the incarnation it was advertised with,
    /// with no failed probes and no reap clock yet.
    fn new(incarnation: u64, state: MemberState) -> Self {
        Self {
            incarnation,
            state,
            failed_probes: 0,
            dead_ticks: 0,
        }
    }
}

/// The default per-room replication factor: the number of members that hold each
/// room, primary first. Clamps to the member count, so a small cluster resolves.
pub const DEFAULT_REPLICATION_FACTOR: usize = 3;

/// How many adopted members must have completed an identity-checked peer link to a
/// gossip-learned member before it is adopted — placed on rooms and counted toward
/// their quorums. More than one, because the whole point is that no *single* member
/// can put a node of its choosing into the placement ring: one compromised member
/// grinding a node id needs an honest member to have independently reached that id
/// too. Small, because every honest member probes every member it knows on its own
/// gossip cadence, so a genuine joiner clears the bar within a few rounds. Clamped
/// Not clamped to the cluster's size: the bar is the same constant on every node, so
/// two nodes never disagree about what the evidence has to show. A cluster with fewer
/// than this many adopted members therefore grows by *configuration* rather than by
/// gossip — which is where a cluster that small gets its members anyway.
pub const ADOPTION_VERIFIERS: usize = 2;

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
    /// gossips so a peer can dial a member it just learned. Every *configured* member,
    /// `self` included, maps to its own node-id bytes: a seed peer's id is derived from
    /// its address, and `self`'s is its advertise address unless an explicit node id
    /// overrode it. Only a member learned by gossip can hold an address distinct from
    /// its id, and never one it advertised for itself — the peer plane admits a
    /// self-introduction only at its own id (see
    /// [`Registry::apply_gossip`](crate::Registry)).
    addrs: HashMap<NodeId, Vec<u8>>,
    /// Reaped members — a tombstone that makes reaping convergent and fail-safe,
    /// mapped to the count of reap checks it has been retained through (its prune
    /// clock). A node that has reaped a member ignores any gossip re-advertising it as
    /// `Dead`/`Suspect`, so a peer that has not yet reaped it cannot resurrect it (no
    /// reap-then-resurrect flapping). The escape is *state-based*: a member re-learned
    /// `Alive` — a genuinely-live return, which only a reachable node produces —
    /// leaves the tombstone and rejoins, at whatever incarnation it advertises. A
    /// crash-restarted node comes back at incarnation 0 (it cannot know the
    /// incarnation it was reaped at), so an incarnation gate would exclude it forever;
    /// keying the escape on liveness, not incarnation, lets it rejoin, and the SWIM
    /// merge (`Dead > Alive` at equal incarnation, refutation on a higher one)
    /// reconciles any lingering `Dead` view from a peer mid-reap.
    ///
    /// The tombstone is not kept forever: a reap check ages every tombstone by one,
    /// and one past [`TOMBSTONE_RETENTION_TICKS`] is pruned — the reaped member
    /// forgotten — so the set stays bounded on a long-lived cluster with churn. The
    /// retention outlives any in-flight gossip that could reference the member, so the
    /// prune never resurrects it.
    reaped: HashMap<NodeId, u32>,
    /// The members this node was *configured* with — `self` and the seed peers. The
    /// root of trust a cluster starts from, adopted from birth because there is no
    /// earlier authority for them to be vouched for by.
    configured: HashSet<NodeId>,
    /// The members rooms are actually placed on — the subset of the roster the
    /// [`Cluster`] above is built from: the configured members plus every
    /// gossip-learned member the evidence now carries. **Derived, never accumulated**
    /// — [`rebuild_placement`](Self::rebuild_placement) recomputes it from
    /// `configured` + `verifiers` — so it is a pure function of state and two nodes
    /// holding the same evidence hold the same ring, however they came by it.
    adopted: HashSet<NodeId>,
    /// Who has verified each member: for every member, the nodes that reported
    /// completing an identity-checked peer link to it. A node inserts itself here
    /// from its own links, and inserts a peer only from that peer's own gossip
    /// (attributed to the member its link is bound to), so every entry is a
    /// first-hand claim by the node named — a member can vouch for itself only, and
    /// never *for* itself ([`note_verified`](Self::note_verified) refuses that).
    /// Grow-only per member, so it merges by union and converges however gossip
    /// interleaves. Claims by members that are not (yet) adopted are retained but do
    /// not count, so a pending member cannot vouch another pending member in.
    verifiers: HashMap<NodeId, HashSet<NodeId>>,
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
        // Every seeded member dials at its node id — the identity each is derived
        // from (`NodeId::from_addr`), so the id and the dial address are the same
        // string, self included. Gossip may later record a distinct address for a
        // member it learns; a seeded one has none to distinguish.
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
        // A configured member is adopted from birth: the operator's config is the
        // root of trust a cluster starts from, and there is no earlier authority for
        // it to be vouched for by.
        let configured: HashSet<NodeId> = members.iter().cloned().collect();
        Self {
            self_id,
            cluster: Cluster::new(members),
            replication_factor,
            relay_down: HashSet::new(),
            liveness,
            addrs,
            reaped: HashMap::new(),
            adopted: configured.clone(),
            configured,
            verifiers: HashMap::new(),
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

    /// The canonical (sorted, de-duplicated) roster, self included — every member
    /// this node knows, whether or not the cluster has adopted it. What a node
    /// dials, probes and gossips; [`adopted_members`](Self::adopted_members) is the
    /// narrower set rooms are placed on.
    pub fn members(&self) -> Vec<NodeId> {
        self.roster()
    }

    /// The canonical (sorted, de-duplicated) *adopted* member set — the members
    /// rooms are placed on and counted toward quorum. Equal to the roster in a
    /// cluster with no pending joiners.
    pub fn adopted_members(&self) -> &[NodeId] {
        self.cluster.nodes()
    }

    /// Learn a member — the anti-entropy union gossip applies for each `(node, addr)`
    /// pair a peer advertises. The advertised `addr` is **not** what the member is
    /// dialed at; see [`add_members`](Self::add_members).
    pub fn add_member(&mut self, node: NodeId, addr: Vec<u8>) {
        self.add_members(std::iter::once((node, addr)));
    }

    /// Union a batch of learned members into the roster — dialable, probable and
    /// gossipable, but **pending**: a member learned this way is placed on no room
    /// until the cluster adopts it ([`note_verified`](Self::note_verified)).
    /// Idempotent: a member already known is skipped, so a re-gossip of a fully-known
    /// set changes nothing (no churn). A member with an empty node id is dropped — it
    /// is neither placeable nor dialable, so a malformed gossip pair cannot poison the
    /// set. `self` is a member from construction and is never relearned.
    ///
    /// **A member is recorded at its own id, whatever address the pair carries.** A
    /// node id *is* an advertise address, so a second address for the same member is a
    /// second, unauthenticated name for one thing — and the ring turns on it, because a
    /// node dials a member to verify it. Keeping the advertised one made the roster
    /// first-write-wins over a field any peer may set: whoever advertised a member
    /// first decided where every later dial went, so two nodes that saw it in a
    /// different order verified different endpoints and placed rooms differently,
    /// forever. Ignoring it makes the dial address a function of the id alone, which
    /// every node agrees on by construction.
    pub fn add_members(&mut self, members: impl IntoIterator<Item = (NodeId, Vec<u8>)>) {
        let mut added = false;
        for (node, addr) in members {
            // A reaped member is never re-added by a plain re-advertise: only a live
            // return (an `Alive` tuple through `merge_liveness`) escapes the
            // tombstone, so a bare gossip of the member's address — which carries no
            // liveness — cannot resurrect it.
            if node.as_bytes().is_empty()
                || self.addrs.contains_key(&node)
                || self.reaped.contains_key(&node)
            {
                continue;
            }
            let _ = addr;
            self.liveness
                .insert(node.clone(), MemberLiveness::new(0, MemberState::Alive));
            let dial = node.as_bytes().to_vec();
            self.addrs.insert(node, dial);
            added = true;
        }
        if added {
            self.rebuild_placement();
        }
    }

    /// The roster in canonical (sorted) order — every member this node knows,
    /// pending ones included. What a node dials, probes and gossips, as against the
    /// [`Cluster`] placement, which holds the adopted members only.
    fn roster(&self) -> Vec<NodeId> {
        let mut roster: Vec<NodeId> = self.addrs.keys().cloned().collect();
        roster.sort();
        roster
    }

    /// The members this node knows, each with its dial address — the payload a
    /// node gossips. Canonical order (the sorted roster), so the advertisement is
    /// deterministic. A member's address falls back to its node-id bytes if none was
    /// recorded, keeping every member dialable. Pending members are included: a node
    /// still unadopted has to be reachable and gossiped about, or it could never be
    /// verified into the ring.
    pub fn known_members(&self) -> Vec<(NodeId, Vec<u8>)> {
        self.roster()
            .into_iter()
            .map(|node| {
                let addr = self
                    .addrs
                    .get(&node)
                    .cloned()
                    .unwrap_or_else(|| node.as_bytes().to_vec());
                (node, addr)
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

    /// Whether `node`'s advertise address declares a TLS transport — whether a dial
    /// to it authenticates the far end before a byte is written. Unlike the *inbound*
    /// direction, where a member's advertised scheme describes its own listener and
    /// says nothing about the link carrying its identity here, an outbound dial runs
    /// over exactly the transport that address declares.
    pub fn advertises_tls(&self, node: &NodeId) -> bool {
        self.addrs
            .get(node)
            .and_then(|addr| std::str::from_utf8(addr).ok())
            .and_then(|addr| crate::dial::PeerEndpoint::parse(addr).ok())
            .is_some_and(|endpoint| endpoint.is_tls())
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
    /// its dial address, current incarnation, state, and whether **this node** has
    /// verified it — canonical (sorted) order, so the advertisement is deterministic.
    /// `self` rides at its own incarnation, always `Alive`.
    ///
    /// The last field is first-hand and self-scoped: a node advertises the links *it*
    /// completed, never one it heard about, so the claim a receiver acts on is always
    /// the claim of the member whose link carried it. Relaying another node's
    /// verifications would make one member's word enough to place any id it liked.
    pub fn known_liveness(&self) -> Vec<(NodeId, Vec<u8>, u64, MemberState, bool)> {
        self.roster()
            .into_iter()
            .map(|node| {
                let addr = self
                    .addrs
                    .get(&node)
                    .cloned()
                    .unwrap_or_else(|| node.as_bytes().to_vec());
                let incarnation = self.incarnation(&node);
                let state = self.gossip_state(&node);
                let verified = self.verified_by_self(&node);
                (node, addr, incarnation, state, verified)
            })
            .collect()
    }

    /// Whether `verifier` has reported a completed identity-checked peer link to
    /// `node` — one entry of the evidence adoption is decided from. A claim is
    /// admitted only from the member whose link carried it, so this reads "what
    /// `verifier` itself told this node about `node`", never a relayed opinion.
    pub fn has_verified(&self, verifier: &NodeId, node: &NodeId) -> bool {
        self.verifiers
            .get(node)
            .is_some_and(|vs| vs.contains(verifier))
    }

    /// Whether this node has itself verified `node`. `self` is not verified by
    /// itself — a node vouching for its own place in the ring is exactly what
    /// adoption exists to refuse — and needs no vouching, since a node is a member of
    /// its own view from construction.
    fn verified_by_self(&self, node: &NodeId) -> bool {
        self.has_verified(&self.self_id, node)
    }

    /// Whether `node` is adopted — placed on rooms and counted toward their quorums,
    /// as against merely known (dialed, probed and gossiped about).
    pub fn is_adopted(&self, node: &NodeId) -> bool {
        self.adopted.contains(node)
    }

    /// Record that this node has itself completed an identity-checked peer link to
    /// `node` — its dial verifying the acceptor's certificate against the address, or
    /// `node` dialing in with a certificate that names it. This is the one piece of
    /// evidence a node produces on its own; adoption is what the cluster does with
    /// enough of them.
    ///
    /// Two members are never verified. `self`, because a node's own place in the ring
    /// is not its to vouch for. And a node outside the roster, because a link is
    /// evidence about a member and there is no member here to be evidence about —
    /// learning one is gossip's job, a link that could add one would restore the
    /// unchecked join this exists to close, and peer admission takes the id a link
    /// claims, so a certified member could otherwise open a link per port on its own
    /// host and grow the evidence without limit.
    pub fn note_verified(&mut self, node: &NodeId) {
        if !self.can_be_verified(node) {
            return;
        }
        let me = self.self_id.clone();
        if self.verifiers.entry(node.clone()).or_default().insert(me) {
            self.rebuild_placement();
        }
    }

    /// Whether a verification of `node` is admissible evidence at all — the bar every
    /// verifier claim clears, this node's own and a peer's alike. See
    /// [`note_verified`](Self::note_verified) for what each clause refuses.
    fn can_be_verified(&self, node: &NodeId) -> bool {
        !self.is_self(node) && self.addrs.contains_key(node)
    }

    /// How many **trust units** have vouched for `node`: the distinct hosts of the
    /// adopted members that verified it, its own host excluded.
    ///
    /// Hosts rather than node ids, because a host is the unit a certificate names
    /// (§Peer Identity) and one host mints as many node ids as it likes. Counting ids
    /// would let a member that holds two of them on one host raise the bar by itself,
    /// which is the mint one level up. Its own host is excluded for the same reason: a
    /// member vouching for a sibling on its own host is vouching for itself. A member
    /// whose id names no host counts for nobody — there is no unit to attribute it to.
    fn verifier_units(&self, node: &NodeId) -> usize {
        let Some(own_host) = member_host(node.as_bytes()) else {
            return 0;
        };
        let Some(verifiers) = self.verifiers.get(node) else {
            return 0;
        };
        verifiers
            .iter()
            .filter(|v| self.adopted.contains(*v))
            .filter_map(|v| member_host(v.as_bytes()))
            .filter(|host| host != &own_host)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Recompute which members rooms are placed on, then rebuild the [`Cluster`] over
    /// them. **Derived from scratch, never accumulated:** the adopted set is the
    /// configured members plus every roster member [`ADOPTION_VERIFIERS`] adopted
    /// trust units have verified, so it is a pure function of `configured` +
    /// `verifiers` and two nodes holding the same evidence hold the same ring however
    /// they came by it. Accumulating instead would make it a function of history —
    /// a member adopted before its vouchers were reaped would stay placed on the node
    /// that saw that order and never be placed on one that did not, and the ring would
    /// split permanently.
    ///
    /// Run to a fixpoint, because adopting a member makes its own verifications count:
    /// each round admits every member that meets the bar against the same adopted set,
    /// so the outcome never depends on the order members were visited in. Terminates
    /// because each round strictly grows a set bounded by the roster.
    fn rebuild_placement(&mut self) {
        self.adopted.clone_from(&self.configured);
        let bar = self.adoption_bar();
        loop {
            let newly: Vec<NodeId> = self
                .addrs
                .keys()
                .filter(|node| !self.adopted.contains(*node))
                .filter(|node| self.verifier_units(node) >= bar)
                .cloned()
                .collect();
            if newly.is_empty() {
                break;
            }
            self.adopted.extend(newly);
        }
        self.cluster = Cluster::new(self.adopted.iter().cloned());
    }

    /// How many trust units must vouch for a member before it is placed:
    /// [`ADOPTION_VERIFIERS`], except for a node **configured with no peers at all**.
    ///
    /// That exception is narrow and it keys on configuration, not on the running set,
    /// so it is a fixed property of a node rather than a bar that moves as the ring
    /// grows. A node whose config names no peer has no cluster to be outvoted by, and
    /// the constant would freeze it forever — a second voucher can only ever come from
    /// a member it has already adopted — so the cluster it places on is exactly the
    /// members it has itself reached. That is the single-node deployment; a node
    /// meant to join a cluster is given a seed peer, which is adopted from birth and
    /// lifts it past this case at boot. Configure one: a node with none takes the ring
    /// from whoever reaches it.
    fn adoption_bar(&self) -> usize {
        match self.configured.len() <= 1 {
            true => 1,
            false => ADOPTION_VERIFIERS,
        }
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
            m.dead_ticks = 0;
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

    /// Run one reap check, removing every member that has stayed `Dead` through
    /// [`REAP_AFTER_DEAD_TICKS`] checks, and return the ids reaped. Each check ages
    /// the reap clock of every `Dead` member by one; a member crossing the threshold
    /// is dropped from the roster (liveness, address, relay-link) and tombstoned, so
    /// stale `Dead` gossip cannot resurrect it and the placement is rebuilt over the
    /// survivors. `self` and any non-`Dead` member are untouched.
    /// Deterministic (tick-driven, no wall clock) and convergent: every replica reaps
    /// the same durably-gone member off its own `Dead` observation, and the tombstone
    /// makes the removal monotonic. Idempotent once a member is gone — a later check
    /// reaps it no further.
    ///
    /// The same check also ages every tombstone by one and prunes any past
    /// [`TOMBSTONE_RETENTION_TICKS`], so the tombstone set stays bounded on a
    /// long-lived cluster. Pruning changes no roster (a tombstoned member is already
    /// off it) — it only forgets the member, convergently (the retention is a shared
    /// tick-count rule every replica applies) and safely (the retention outlives any
    /// gossip that could still name the member, so a pruned member only ever reappears
    /// as a fresh join).
    pub fn reap_dead(&mut self) -> Vec<NodeId> {
        // Age and prune tombstones first — one tick per reap check, independent of
        // whether anything is reaped this round.
        self.reaped.retain(|_node, age| {
            *age = age.saturating_add(1);
            *age < TOMBSTONE_RETENTION_TICKS
        });
        let me = self.self_id.clone();
        let mut to_reap = Vec::new();
        for (node, m) in self.liveness.iter_mut() {
            if node == &me || m.state != MemberState::Dead {
                continue;
            }
            m.dead_ticks = m.dead_ticks.saturating_add(1);
            if m.dead_ticks >= REAP_AFTER_DEAD_TICKS {
                to_reap.push(node.clone());
            }
        }
        for node in &to_reap {
            self.reaped.insert(node.clone(), 0);
            self.liveness.remove(node);
            self.addrs.remove(node);
            self.relay_down.remove(node);
            // A reaped member no longer vouches: its own verifier set goes with it and
            // it is struck from every other member's, so the ring is recomputed without
            // its word. It leaves `configured` too — a configured member that departed
            // durably is gone, and keeping it would place rooms on a node the roster no
            // longer holds. Were it to return it would be a fresh join, verified again.
            self.configured.remove(node);
            self.verifiers.remove(node);
            for vs in self.verifiers.values_mut() {
                vs.remove(node);
            }
        }
        if !to_reap.is_empty() {
            self.rebuild_placement();
        }
        to_reap
    }

    /// Whether `node` is currently tombstoned — reaped and not yet pruned. Stale
    /// `Dead`/`Suspect` gossip about a tombstoned member is ignored; once the
    /// tombstone is pruned (past [`TOMBSTONE_RETENTION_TICKS`]) the member is
    /// forgotten and a later reappearance is a fresh join.
    pub fn is_tombstoned(&self, node: &NodeId) -> bool {
        self.reaped.contains_key(node)
    }

    /// Merge a gossiped liveness payload from the member `sender` into this node's
    /// view — the SWIM anti-entropy of failure detection. For each
    /// `(node, addr, incarnation, state, verified)`:
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
    /// The `verified` flag is `sender`'s own claim to have completed an
    /// identity-checked peer link to the member the tuple names, and it is recorded
    /// **against `sender`** — the member this link is bound to — rather than against
    /// whoever the payload might name. That is what keeps the evidence first-hand: a
    /// member can add itself to another's verifier set and to nobody else's, so no
    /// member can manufacture the verifiers that would place an id it minted. A claim
    /// naming the sender itself is dropped; the verifier sets are grow-only, so the
    /// merge is a union and converges however rounds interleave.
    ///
    /// A malformed pair (empty node id) is dropped, as on the additive path. Order-
    /// independent and idempotent: two nodes that received the same updates in any
    /// order converge on the same liveness.
    pub fn merge_liveness(
        &mut self,
        sender: &NodeId,
        payload: impl IntoIterator<Item = (NodeId, Vec<u8>, u64, MemberState, bool)>,
    ) {
        let mut rebuilt = false;
        let mut claimed = Vec::new();
        for (node, addr, incarnation, state, verified) in payload {
            if node.as_bytes().is_empty() {
                continue;
            }
            if verified && &node != sender && !self.is_self(&node) {
                claimed.push(node.clone());
            }
            if self.is_self(&node) {
                self.refute_if_stale(incarnation, state);
                continue;
            }
            // A reaped member stays out unless it returns `Alive` — a genuinely-live
            // node, which only a reachable member produces (a truly-gone member is
            // only ever gossiped `Dead`). That escapes the tombstone and falls through
            // to be re-learned below; a `Dead`/`Suspect` re-advertisement from a peer
            // still holding the reaped member is ignored, so it cannot resurrect it
            // and no reap-then-resurrect flap occurs. Keying on liveness, not
            // incarnation, lets a crash-restarted node (back at incarnation 0) rejoin;
            // the SWIM merge then reconciles any peer still mid-reap.
            if self.reaped.contains_key(&node) {
                if state == MemberState::Alive {
                    self.reaped.remove(&node);
                } else {
                    continue;
                }
            }
            match self.liveness.get_mut(&node) {
                None => {
                    let _ = addr;
                    self.liveness
                        .insert(node.clone(), MemberLiveness::new(incarnation, state));
                    let dial = node.as_bytes().to_vec();
                    self.addrs.insert(node, dial);
                    rebuilt = true;
                }
                Some(m) => {
                    if incarnation > m.incarnation {
                        m.incarnation = incarnation;
                        m.state = state;
                        m.failed_probes = 0;
                        m.dead_ticks = 0;
                    } else if incarnation == m.incarnation && state > m.state {
                        m.state = state;
                    }
                }
            }
        }
        // Record the sender's verifications only for members that survived the merge
        // above — a claim about a node that was dropped as malformed, or that is
        // tombstoned, is a claim about no member of this view — and only where the
        // member is coherently addressed, the same bar this node's own links clear.
        for node in claimed {
            if self.can_be_verified(&node) {
                rebuilt |= self
                    .verifiers
                    .entry(node)
                    .or_default()
                    .insert(sender.clone());
            }
        }
        if rebuilt {
            self.rebuild_placement();
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
