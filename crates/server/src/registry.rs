//! The many-connection fan-out over one hub.
//!
//! A [`Registry`] holds every live connection, each with its own session and
//! an outbox of messages awaiting send. [`Registry::deliver`] drives one
//! connection's session, queues its replies, and fans a broadcast out to the
//! room's other connections. Pure, synchronous routing; the async transport
//! pumps bytes through it.

use std::collections::{HashMap, HashSet};
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crdtsync_core::schema::Trigger;
use crdtsync_core::{ClientId, ElementId, ErrorCode, MemberState, Message, Op, OpKind, Schema};

use crate::acl::authorized;
use crate::auth::{AllowAll, Identity, Verifier};
use crate::authz::{Action, Authorizer, Decision, PermitAll, Resource};
use crate::auto_version::{
    expand_name, expand_schedule_name, schedule_origin, trigger_origin, AutoVersionSink,
    AutoVersionState,
};
use crate::clock::{Clock, SystemClock};
use crate::leadership::LeadershipEpochs;
use crate::membership::Membership;
use crate::placement::NodeId;
use crate::replication::Replication;
use crate::schema_registry::SchemaRegistry;
use crate::{
    step, AwarenessPolicy, Catchup, EngineEvent, EventSink, Hub, RoomId, SchemaAwarenessPolicy,
    Session, Store, MAIN_BRANCH,
};

/// How long a departed client's presence is retained before a sweep clears it,
/// so a brief reconnect keeps its awareness alive across the gap.
const DEFAULT_GRACE_MILLIS: u64 = 5000;

/// A live connection's handle, minted by [`Registry::connect`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ConnId(u64);

/// One connection: its protocol session and the messages queued to send it.
struct Conn {
    session: Session,
    outbox: Vec<Message>,
}

/// A client write-ack withheld pending majority replication. The leader owes the
/// author an [`Message::Accepted`] for the write that reached server sequence
/// `seq` in `room`; it is released to `conn` once a majority of `room`'s replica
/// set holds that sequence.
struct PendingAck {
    room: RoomId,
    seq: u64,
    conn: ConnId,
    accepted: Message,
}

/// The set of live connections sharing one hub.
pub struct Registry {
    hub: Hub,
    conns: HashMap<ConnId, Conn>,
    next: u64,
    verifier: Box<dyn Verifier>,
    authorizer: Box<dyn Authorizer>,
    clock: Arc<dyn Clock>,
    grace_millis: u64,
    /// Departed clients whose presence is retained until the wall-clock deadline,
    /// keyed by client. A reconnect cancels the entry; a [`sweep`](Registry::sweep)
    /// past the deadline clears the presence and tells the room.
    stale: HashMap<ClientId, u64>,
    /// The schema registry the handshake resolves a client's `{app_id, version}`
    /// against. Shared with the registration admin plane, which writes it; empty
    /// by default, so every connection resolves to a relay.
    schema: Arc<Mutex<SchemaRegistry>>,
    /// An injected timed-TTL policy, authoritative when set: it alone governs
    /// expiry — one declaring no TTLs suppresses it entirely. `None` (the default)
    /// leaves the sweep to resolve TTLs from each room's governing schema.
    awareness_policy: Option<Arc<dyn AwarenessPolicy>>,
    /// The `{app_id, version}` governing each room's awareness, seeded when an
    /// enforcing client subscribes and reconciled each sweep against who is
    /// present ([`reconcile_room_apps`](Registry::reconcile_room_apps)): the first
    /// (incumbent) app governs while the room stays live — a foreign app never
    /// seizes it — at the incumbent's highest present version. Dormant rooms are
    /// dropped.
    room_apps: HashMap<RoomId, (Vec<u8>, u32)>,
    /// Parsed schemas keyed by `{app_id, version}`, the sweep's TTL source. A
    /// registry link is immutable once locked, so the outcome — the parsed schema
    /// or `None` for an absent/unparseable one — is cached for the process
    /// lifetime and never re-resolved for the same version.
    schema_cache: HashMap<(Vec<u8>, u32), Option<Arc<Schema>>>,
    /// Auto-version signals a recording sink queued during a delivery, drained
    /// after it: each room-bearing lifecycle event, awaiting its schema's trigger
    /// match. Shared with the sink the hub holds.
    auto_version: Rc<AutoVersionState>,
    /// The wall time each `every:` schedule trigger last fired, keyed by
    /// `(room, schedule-origin)`. A trigger is armed to `now` the first sweep it is
    /// seen and captures once its interval has since elapsed; entries for unbound
    /// rooms are pruned each sweep, so a rebound room re-arms.
    schedule_fires: HashMap<(RoomId, Vec<u8>), u64>,
    /// The node's static cluster membership + placement view. `None` is
    /// single-node mode: every room is served locally. Held for the routing and
    /// replication layers to consult; this layer does not yet route on it.
    membership: Option<Membership>,
    /// Leader-to-follower replication state: frames queued for each follower and
    /// the acknowledged per-`(room, follower)` watermark. Inert in single-node
    /// mode — a node with no membership never leads a room, so it never
    /// replicates.
    replication: Replication,
    /// Per-room leadership epochs — the split-brain fence (see
    /// [`LeadershipEpochs`]). Empty (inert) in single-node mode and until a room's
    /// leadership first changes.
    epochs: LeadershipEpochs,
    /// Client write-acks withheld pending majority replication: for each write
    /// the leader has committed but not yet confirmed durable, the `Accepted` owed
    /// to its author and the server sequence a majority of the replica set must
    /// reach to release it. Empty in single-node mode, where a write is majority-
    /// durable on commit and acked at once.
    pending_acks: Vec<PendingAck>,
}

/// The disposition of a node-to-node replication frame for a room, decided by the
/// shared membership + leadership-epoch fence — see
/// [`gate_replica_frame`](Registry::gate_replica_frame).
enum ReplicaGate {
    /// A stray frame — the node lacks membership, does not hold the room, leads it
    /// without being superseded, or it names a non-`main` branch: drop the connection.
    Reject,
    /// A stale-epoch frame from a demoted-then-recovered leader: no apply, but the
    /// connection stays open (the stale leader steps down when it observes the higher
    /// epoch on the new leader's stream).
    Fenced,
    /// Committed to apply — the fence has been advanced and persisted, so the caller
    /// folds the frame's payload into the replica.
    Apply,
}

impl Registry {
    /// An in-memory registry whose hub's replicas are owned by `server`.
    pub fn new(server: ClientId) -> Self {
        Self::from_hub(Hub::new(server))
    }

    /// A registry over an existing hub — durable or not. Defaults to the
    /// dev-mode [`AllowAll`] verifier; set one with [`Registry::set_verifier`].
    pub(crate) fn from_hub(mut hub: Hub) -> Self {
        // The built-in auto-version sink records room-bearing lifecycle events; the
        // registry drains them after each delivery. Registered here, so a room
        // whose governing schema declares `autoVersion` triggers auto-versions with
        // no further wiring.
        let auto_version = Rc::new(AutoVersionState::default());
        hub.add_event_sink(Box::new(AutoVersionSink(auto_version.clone())));
        // Restore the persisted split-brain fence: seed the live leadership epochs
        // from the epochs the store carried across the load seam, so a restarted node
        // remembers the highest epoch it had seen per room and cannot re-accept a
        // demoted leader's stale-epoch frames it would have fenced before the restart.
        // A store-less or never-led hub carries none, leaving the fence at its
        // in-memory default.
        let mut epochs = LeadershipEpochs::default();
        for (room, epoch) in hub.loaded_epochs() {
            epochs.observe(room, *epoch);
        }
        Self {
            hub,
            conns: HashMap::new(),
            next: 0,
            verifier: Box::new(AllowAll),
            authorizer: Box::new(PermitAll),
            clock: Arc::new(SystemClock),
            grace_millis: DEFAULT_GRACE_MILLIS,
            stale: HashMap::new(),
            schema: Arc::new(Mutex::new(SchemaRegistry::new())),
            awareness_policy: None,
            room_apps: HashMap::new(),
            schema_cache: HashMap::new(),
            auto_version,
            schedule_fires: HashMap::new(),
            membership: None,
            replication: Replication::default(),
            epochs,
            pending_acks: Vec::new(),
        }
    }

    /// Resolve handshakes against `schema` — the registry the registration admin
    /// plane writes. A connection that shares it sees every registered app.
    pub fn set_schema_registry(&mut self, schema: Arc<Mutex<SchemaRegistry>>) {
        self.schema = schema;
    }

    /// Inject `policy` as the authoritative timer for awareness entries — it
    /// alone governs expiry (one declaring no TTLs suppresses it). By default no
    /// policy is injected and the sweep resolves TTLs from each room's schema.
    pub fn set_awareness_policy(&mut self, policy: Arc<dyn AwarenessPolicy>) {
        self.awareness_policy = Some(policy);
    }

    /// Use `verifier` to authenticate connections' credentials.
    pub fn set_verifier(&mut self, verifier: Box<dyn Verifier>) {
        self.verifier = verifier;
    }

    /// Use `authorizer` to decide what each authenticated actor may do.
    pub fn set_authorizer(&mut self, authorizer: Box<dyn Authorizer>) {
        self.authorizer = authorizer;
    }

    /// Register an [`EventSink`] to observe the engine's lifecycle events —
    /// connections and subscribes here, versions and compaction from the hub. The
    /// one seam every lifecycle moment fans out through.
    pub fn add_event_sink(&mut self, sink: Box<dyn EventSink>) {
        self.hub.add_event_sink(sink);
    }

    /// Verify a credential presented at the transport upgrade, returning the
    /// server-derived [`Identity`], or `None` if refused. The fast path uses this
    /// to establish auth during accept, so the connection skips the in-band Auth.
    pub fn verify_credential(&self, credential: &[u8]) -> Option<Identity> {
        self.verifier.verify(credential)
    }

    /// Read wall time from `clock` for the reconnect-grace window — a shared
    /// [`ManualClock`](crate::clock::ManualClock) drives it deterministically in
    /// tests.
    pub fn set_clock(&mut self, clock: Arc<dyn Clock>) {
        self.clock = clock;
    }

    /// How long a departed client's presence lingers before a sweep may clear it.
    pub fn set_grace_millis(&mut self, millis: u64) {
        self.grace_millis = millis;
    }

    /// Hold the node's static cluster [`Membership`] — its member view and
    /// placement. Unset (the default) is single-node mode, every room local.
    pub fn set_membership(&mut self, membership: Membership) {
        self.membership = Some(membership);
    }

    /// The node's cluster membership view, or `None` in single-node mode. Routing
    /// (Unit 3) and replication (Unit 4) read placement through this.
    pub fn membership(&self) -> Option<&Membership> {
        self.membership.as_ref()
    }

    /// Record a peer's reachability, the failover liveness signal (Unit 6a): its
    /// inter-node relay link connecting marks it `live`, dropping or failing to
    /// dial marks it down. A down member is skipped when electing a room's
    /// effective leader, so a dead placement primary's rooms promote to the next
    /// live replica. Inert in single-node mode (no membership) — there are no
    /// peers to track.
    pub fn set_peer_liveness(&mut self, node: NodeId, live: bool) {
        if let Some(membership) = &mut self.membership {
            if live {
                membership.mark_node_live(&node);
            } else {
                membership.mark_node_down(&node);
            }
        }
    }

    /// Queue a [`Message::Replicate`] for each follower of `room` when this node
    /// leads it, mirroring the fresh `ops` on `branch`. A node with no membership
    /// leads nothing, so it never replicates — single-node behavior is unchanged.
    fn enqueue_replication(&mut self, room: &[u8], branch: &[u8], ops: &[Op]) {
        // Unit 4 mirrors the room's `main` stream. A branch write is not
        // replicated: a follower has no copy of the fork it diverges from (branch
        // lifecycle is not yet mirrored), so replicating the tail alone would be
        // discarded there — the branch replication path is a later unit.
        if branch != MAIN_BRANCH {
            return;
        }
        let Some(membership) = &self.membership else {
            return;
        };
        // Origination follows *effective* (live) leadership: a node promoted over a
        // down placement primary originates replication for its newly-led rooms,
        // and a demoted-but-recovered old primary defers until it leads again.
        if !membership.is_effective_primary_for(room) {
            return;
        }
        // The same replica-set-minus-self the majority gate counts, so the fan-out
        // and the quorum never disagree on who is a follower.
        let followers = self.quorum(room).1;
        if followers.is_empty() {
            return;
        }
        let base_seq = self.hub.base_seq(room);
        // Stamp the frames with this node's leadership epoch for the room. A
        // promotion opens a fresh (higher) epoch — persist the advance so the fence
        // survives a restart and this node never re-leads at a stale epoch.
        let before = self.epochs.highest_seen(room);
        let epoch = self.epochs.claim_leadership(room);
        self.persist_epoch_if_advanced(room, before);
        for follower in followers {
            self.replication.enqueue(
                follower,
                Message::Replicate {
                    room: room.to_vec(),
                    branch: branch.to_vec(),
                    ops: ops.to_vec(),
                    base_seq,
                    epoch,
                },
            );
        }
    }

    /// Catch a just-(re)connected `follower` up to this leader's state for every
    /// room this node leads that the follower replicates — the late-joiner
    /// replication dial. The steady replication path (`enqueue_replication`) mirrors
    /// only *fresh* commits, so a follower dialed after the leader advanced never
    /// received the backlog. On its link coming up, the leader sends it the ops it is
    /// missing — from the follower's acknowledged watermark, so a store-backed
    /// reconnecting follower gets only its tail and a brand-new one (watermark `0`)
    /// gets the whole retained log — which the follower ingests and dedups exactly as
    /// a live commit, converging it before it is routed to. Inert without membership
    /// (single-node) and on a node that does not lead the room.
    ///
    /// A follower below the compaction floor (a brand-new follower joining a room the
    /// leader has since compacted) needs a whole-replica snapshot the ops path cannot
    /// carry: the pre-floor ops are gone, so a delta would leave it divergent. Such a
    /// room is caught up by a [`Message::ReplicateSnapshot`] state-transfer instead —
    /// the leader branches on the follower's watermark versus the room's floor, which
    /// [`Hub::catch_up`](crate::Hub::catch_up) folds into its reply.
    ///
    /// The catch-up ranges from the follower's *acknowledged* watermark, which is its
    /// durable position under the same persist-before-ack assumption the majority-ack
    /// durability layer already relies on (a follower appends an op to its store
    /// before it acks the sequence). So this introduces no new durability assumption:
    /// a follower that loses durable state *below* an acked watermark — a store-less
    /// node, a wiped disk, a restore from an older backup — is a non-durable
    /// configuration whose earlier acks were themselves not durable, and it is
    /// under-served here exactly as it already undercounts toward quorum durability.
    /// Making a wiped follower self-heal (it reports its true head on reconnect, or
    /// the leader re-sends from the floor and leans on op-dedup) is a follow-on.
    pub fn catch_up_follower(&mut self, follower: &NodeId) {
        for room in self.rooms_led_for(follower) {
            let floor = self.replication.watermark(&room, follower);
            if let Some(frame) = self.catch_up_room_frame(&room, floor) {
                self.replication.enqueue(follower.clone(), frame);
            }
        }
    }

    /// Catch a follower up from the durable heads it *reported* on (re)join, honoring
    /// each reported head over any acknowledged watermark this leader remembers — the
    /// wiped-follower self-heal. A follower whose durable state was wiped below its
    /// last ack reports its true (lower) current head per room; the leader uses that
    /// as the catch-up floor, so the follower is re-converged from where it actually
    /// is (an ops tail, or a snapshot when the reported head is below the compaction
    /// floor) rather than trusted at a stale ack it can no longer honor and left with
    /// a silent gap. Fail-closed: a room this node leads that the follower replicates
    /// but that is ABSENT from `heads` (a fully-wiped room the follower no longer
    /// holds) is treated as head `0`, so it gets a full catch-up rather than being
    /// trusted at its remembered watermark. The reported head also *replaces* the
    /// leader's watermark for the follower (it may move it DOWN), so majority-ack
    /// durability stops counting the follower toward quorum for data it can no longer
    /// prove. The reported head is **clamped to this leader's own head** before it
    /// sets the watermark: a follower can only durably hold ops the leader produced,
    /// so a report above the leader's head (e.g. a freshly-promoted lagging leader
    /// hearing a head from an older, higher log) must never credit the follower past
    /// what this leader has, which would falsely satisfy quorum and prematurely
    /// release an `Accepted`. Inert without membership (single-node) and on a room
    /// this node does not lead.
    pub fn catch_up_follower_reporting(&mut self, follower: &NodeId, heads: &[(RoomId, u64)]) {
        let reported: HashMap<&[u8], u64> = heads.iter().map(|(r, h)| (r.as_slice(), *h)).collect();
        for room in self.rooms_led_for(follower) {
            // The reported head is authoritative — a room the follower did not name is
            // one it can no longer prove any of, so its floor is 0 (fail-closed) —
            // but never trusted ABOVE this leader's own head: a follower cannot hold
            // ops this leader never produced, so crediting it past our head would
            // falsely satisfy majority-ack durability.
            let reported_head = reported.get(room.as_slice()).copied().unwrap_or(0);
            let floor = reported_head.min(self.hub.seq(&room));
            // Honor the report over the remembered ack, moving the watermark to the
            // reported head even when that lowers it.
            self.replication
                .set_watermark(follower.clone(), &room, floor);
            if let Some(frame) = self.catch_up_room_frame(&room, floor) {
                self.replication.enqueue(follower.clone(), frame);
            }
        }
    }

    /// The rooms this node effectively leads that `follower` replicates — the set a
    /// catch-up ranges over. Collected up front so the membership borrow is released
    /// before the hub/replication mutations. Empty without membership, and for a
    /// catch-up targeting this node itself.
    fn rooms_led_for(&self, follower: &NodeId) -> Vec<RoomId> {
        let Some(membership) = &self.membership else {
            return Vec::new();
        };
        if membership.is_self(follower) {
            return Vec::new();
        }
        self.hub
            .room_ids()
            .into_iter()
            .filter(|room| {
                membership.is_effective_primary_for(room)
                    && membership.replicas_for(room).contains(follower)
            })
            .collect()
    }

    /// The catch-up frame that lands a follower at `room`'s head from `floor`, or
    /// `None` when it is already at the head (an empty ops tail). The leader branches
    /// by comparing `floor` to the room's compaction floor, which `catch_up` folds
    /// into its reply: at or above the floor it yields the ops past `floor` (an
    /// ordinary delta), below it — the ops the follower needs are compacted away — it
    /// yields the whole-replica snapshot at the head. So a follower below the floor is
    /// caught up by a state-transfer rather than a futile ops-replay that would leave
    /// it divergent; one at or above it keeps the ops-tail path. The frame is stamped
    /// with this node's leadership epoch, fenced exactly as a steady replication frame.
    fn catch_up_room_frame(&mut self, room: &[u8], floor: u64) -> Option<Message> {
        match self.hub.catch_up(room, floor) {
            Catchup::Ops(records) => {
                let ops: Vec<Op> = records.into_iter().map(|rec| rec.op).collect();
                if ops.is_empty() {
                    return None;
                }
                let base_seq = self.hub.base_seq(room);
                Some(Message::Replicate {
                    room: room.to_vec(),
                    branch: MAIN_BRANCH.to_vec(),
                    ops,
                    base_seq,
                    epoch: self.claim_and_persist_epoch(room),
                })
            }
            // Below the floor: send the whole-replica snapshot the ops path cannot
            // carry, tagged with the sequence it lands the follower at. The follower
            // decodes it, converging before it serves; the steady path resumes the
            // tail above it.
            Catchup::Snapshot { seq, state } => Some(Message::ReplicateSnapshot {
                room: room.to_vec(),
                branch: MAIN_BRANCH.to_vec(),
                seq,
                state,
                epoch: self.claim_and_persist_epoch(room),
            }),
        }
    }

    /// This node's durable-verified heads — the current server sequence it can prove
    /// it holds for each room it replicates, read from its own state (not a remembered
    /// ack). A (re)joining follower reports these to its leader so the leader catches
    /// it up from where it actually is; a follower whose state was wiped reports its
    /// true (lower) head, or omits a room it no longer holds entirely (fail-closed —
    /// the leader treats an omitted room as head `0`). Empty without membership.
    pub fn durable_heads(&self) -> Vec<(RoomId, u64)> {
        let Some(membership) = &self.membership else {
            return Vec::new();
        };
        self.hub
            .room_ids()
            .into_iter()
            .filter(|room| membership.owns(room))
            .map(|room| {
                let head = self.hub.seq(&room);
                (room, head)
            })
            .collect()
    }

    /// Claim this node's leadership epoch for `room` and persist it when it advances
    /// — the stamp a catch-up frame carries, as the steady replication path does. A
    /// steady leader keeps its stable epoch (no spurious bump); any advance is written
    /// through so a restart reloads the fence.
    fn claim_and_persist_epoch(&mut self, room: &[u8]) -> u64 {
        let before = self.epochs.highest_seen(room);
        let epoch = self.epochs.claim_leadership(room);
        self.persist_epoch_if_advanced(room, before);
        epoch
    }

    /// The shared membership + leadership-epoch fence for a node-to-node replication
    /// frame (`Replicate` and `ReplicateSnapshot`) for `room` on `branch` stamped
    /// `epoch`. A frame is applied only while this node merely *follows* `room`: it
    /// must hold the room (placement) and not itself lead it, unless a strictly higher
    /// `epoch` supersedes that leadership (the recovered-stale-leader reconciliation),
    /// and it must name the `main` stream (a leader replicates only `main`). A frame
    /// below the highest epoch this node has seen is fenced — it comes from a demoted
    /// leader that missed the promotion, and applying it would resurrect its writes.
    /// On [`Apply`](ReplicaGate::Apply) the fence is advanced (stepping down if
    /// superseded) and persisted, so a restart reloads it and a later lower-epoch frame
    /// is fenced; the step-down is deferred to here so a rejected frame never churns
    /// this node's leadership epoch.
    fn gate_replica_frame(&mut self, room: &[u8], branch: &[u8], epoch: u64) -> ReplicaGate {
        let Some(membership) = &self.membership else {
            return ReplicaGate::Reject;
        };
        let owns = membership.owns(room);
        let leads = membership.is_effective_primary_for(room);
        if epoch < self.epochs.highest_seen(room) {
            return ReplicaGate::Fenced;
        }
        let supersedes = self.epochs.leads_below(room, epoch);
        if !owns || (leads && !supersedes) || branch != MAIN_BRANCH {
            return ReplicaGate::Reject;
        }
        let before = self.epochs.highest_seen(room);
        self.epochs.supersede_if_leading(room, epoch);
        self.epochs.observe(room, epoch);
        self.persist_epoch_if_advanced(room, before);
        ReplicaGate::Apply
    }

    /// Apply a leader's replicated `ops` into this node's follower replica of
    /// `room`, queueing a [`Message::ReplicaAck`] on the peer connection `id` with
    /// the sequence the replica has reached. Gated by [`gate_replica_frame`](Registry::gate_replica_frame):
    /// a stray frame drops the connection, a stale-epoch one is fenced. Returns
    /// whether the connection stays open.
    fn apply_replicate(
        &mut self,
        id: ConnId,
        room: RoomId,
        branch: Vec<u8>,
        ops: Vec<Op>,
        base_seq: u64,
        epoch: u64,
    ) -> bool {
        match self.gate_replica_frame(&room, &branch, epoch) {
            ReplicaGate::Reject => return false,
            ReplicaGate::Fenced => return true,
            ReplicaGate::Apply => {}
        }
        // `base_seq` is the leader's compaction floor. Unit 4 replicates the whole log
        // from the first op, so a follower on the ops path already tracks the leader's
        // sequence space (a below-floor follower takes the snapshot path instead), and
        // the ack needs no adjustment.
        let _ = base_seq;
        // Ingest through the same path a client `Ops` write uses. A replicated write
        // carries no schema version — the leader logs its writers' ops untagged on the
        // relay seam, and the follower mirrors them verbatim.
        if self.hub.ingest(&room, ops, None).is_err() {
            return false;
        }
        let through_seq = self.hub.seq(&room);
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.outbox.push(Message::ReplicaAck { room, through_seq });
        }
        true
    }

    /// Install a leader's whole-replica `state` snapshot into this node's follower
    /// replica of `room` — the below-floor state-transfer catch-up. A follower whose
    /// acked watermark fell below the leader's compaction floor is missing ops that
    /// have been compacted away, so a `Replicate` delta cannot converge it; the leader
    /// sends the snapshot instead, and the follower `decode_state`-loads it, landing
    /// its sequence at `seq` and acking it. Replaces any existing replica, so a
    /// re-sent snapshot is idempotent. Gated by [`gate_replica_frame`](Registry::gate_replica_frame),
    /// exactly as [`apply_replicate`](Registry::apply_replicate). Returns whether the
    /// connection stays open.
    fn apply_replicate_snapshot(
        &mut self,
        id: ConnId,
        room: RoomId,
        branch: Vec<u8>,
        seq: u64,
        state: Vec<u8>,
        epoch: u64,
    ) -> bool {
        match self.gate_replica_frame(&room, &branch, epoch) {
            ReplicaGate::Reject => return false,
            ReplicaGate::Fenced => return true,
            ReplicaGate::Apply => {}
        }
        if self.hub.install_snapshot(&room, &state, seq).is_err() {
            return false;
        }
        let through_seq = self.hub.seq(&room);
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.outbox.push(Message::ReplicaAck { room, through_seq });
        }
        true
    }

    /// Persist `room`'s leadership epoch when it advanced past `before` — the
    /// highest-seen value the caller captured before mutating the fence. Monotone by
    /// construction (the fence never lowers), so this writes only on a genuine
    /// advance, keeping the blocking store write off the steady-state path. A
    /// store-less hub is a no-op, so an in-memory deployment is unchanged.
    fn persist_epoch_if_advanced(&mut self, room: &[u8], before: u64) {
        let now = self.epochs.highest_seen(room);
        if now > before {
            self.hub.persist_epoch(room, now);
        }
    }

    /// This node's known cluster members, each with its dial address — the member
    /// set without liveness. Empty in single-node mode (no membership).
    pub fn known_members(&self) -> Vec<(NodeId, Vec<u8>)> {
        self.membership
            .as_ref()
            .map(Membership::known_members)
            .unwrap_or_default()
    }

    /// This node's known cluster members with liveness — the payload it gossips.
    /// Empty in single-node mode (no membership), so a non-cluster node advertises
    /// nothing.
    pub fn known_liveness(&self) -> Vec<(NodeId, Vec<u8>, u64, MemberState)> {
        self.membership
            .as_ref()
            .map(Membership::known_liveness)
            .unwrap_or_default()
    }

    /// Merge a gossiped liveness payload into this node's membership — the SWIM
    /// anti-entropy merge that both grows the member set and converges its liveness
    /// toward a cluster-wide view. Inert in single-node mode (no membership).
    pub fn merge_gossip(&mut self, members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState)>) {
        if let Some(membership) = &mut self.membership {
            membership.merge_liveness(
                members
                    .into_iter()
                    .map(|(node, addr, inc, state)| (NodeId::from(node), addr, inc, state)),
            );
        }
    }

    /// Run one reap check over the cluster membership: remove members that have
    /// stayed `Dead` past the bounded dead-time ([`Membership::reap_dead`]), so a
    /// durably-departed node stops lingering as a placement replica. Driven once per
    /// membership sweep. Inert in single-node mode (no membership); the next delivery
    /// recomputes placement over the reaped roster, so nothing needs flushing here.
    pub fn reap_dead_members(&mut self) {
        if let Some(membership) = &mut self.membership {
            membership.reap_dead();
        }
    }

    /// Record the outcome of a direct gossip round to `node`: a success is
    /// first-hand proof it is alive, a failure counts toward suspicion (escalating
    /// it `Alive → Suspect → Dead` over enough rounds). Inert in single-node mode.
    /// This is the gossip-driven failover signal, cluster-wide where the relay-link
    /// signal ([`set_peer_liveness`](Self::set_peer_liveness)) is connection-local.
    pub fn note_gossip_probe(&mut self, node: NodeId, reachable: bool) {
        if let Some(membership) = &mut self.membership {
            if reachable {
                membership.note_gossip_reachable(&node);
            } else {
                membership.note_gossip_unreachable(&node);
            }
        }
    }

    /// Apply an inbound [`Message::Gossip`] on peer connection `id`: merge the
    /// advertised liveness into this node's view, then answer with this node's own
    /// so the exchange syncs both directions (push-pull anti-entropy). Honored only
    /// in cluster mode — a Gossip on a single-node deployment (no membership) is a
    /// stray frame and the connection is dropped. Returns whether the connection
    /// stays open.
    fn apply_gossip(
        &mut self,
        id: ConnId,
        members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState)>,
    ) -> bool {
        if self.membership.is_none() {
            return false;
        }
        self.merge_gossip(members);
        let reply = crate::gossip::gossip_frame(&self.known_liveness());
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.outbox.push(reply);
        }
        true
    }

    /// Apply an inbound [`Message::FollowerHeads`]: catch the reporting follower up
    /// from the durable heads it named, honoring them over any remembered ack (the
    /// wiped-follower self-heal). Self-describing — the follower's id rides the frame
    /// — so this needs no connection→node mapping, exactly like a Gossip. Honored only
    /// in cluster mode; a report on a single-node deployment is a stray frame and the
    /// connection is dropped. The catch-up frames are queued for the follower and the
    /// transport routes them over its peer connection. Returns whether the connection
    /// stays open.
    fn apply_follower_heads(&mut self, reporter: Vec<u8>, heads: Vec<(RoomId, u64)>) -> bool {
        if self.membership.is_none() {
            return false;
        }
        let follower = NodeId::from(reporter);
        self.catch_up_follower_reporting(&follower, &heads);
        true
    }

    /// Queue this node's durable-head report for `leader` — the (re)join handshake
    /// that lets `leader` catch this node up from where it actually is (the
    /// wiped-follower self-heal), honoring the reported heads over any ack it
    /// remembers. Sent when this node's peer link to `leader` comes up; `leader`
    /// filters the report to the rooms it actually leads that this node replicates.
    /// Inert without membership (single-node) and toward this node itself.
    pub fn report_heads_to(&mut self, leader: &NodeId) {
        let Some(membership) = &self.membership else {
            return;
        };
        if membership.is_self(leader) {
            return;
        }
        let frame = Message::FollowerHeads {
            reporter: membership.self_id().as_bytes().to_vec(),
            heads: self.durable_heads(),
        };
        self.replication.enqueue(leader.clone(), frame);
    }

    /// Answer a SWIM indirect-probe request on peer connection `id`: report this
    /// node's own liveness view of the member at advertise address `target` as a
    /// [`Message::PingAck`]. The requester's direct probe of the target failed, so it
    /// asks this relay for a second opinion — reachable iff `target` is a member this
    /// node knows **and** its own liveness ([`is_live`](Membership::is_live) — relay
    /// link up and gossip has not declared it `Dead`) says it is up. A target this
    /// node has never learned is answered unreachable, not optimistically alive, so
    /// the relay never vouches for (nor is induced to dial) an address outside its
    /// member set. Honored only in cluster mode — a ping-req on a single-node
    /// deployment (no membership) is a stray frame and the connection is dropped.
    /// Returns whether the connection stays open.
    fn apply_ping_req(&mut self, id: ConnId, target: Vec<u8>) -> bool {
        let Some(membership) = &self.membership else {
            return false;
        };
        let node = NodeId::from(target);
        let reachable = membership.is_member(&node) && membership.is_live(&node);
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.outbox.push(Message::PingAck { reachable });
        }
        true
    }

    /// Take every replication frame queued since the last drain — the transport
    /// routes each to its follower's peer connection.
    pub fn take_replication(&mut self) -> Vec<(NodeId, Message)> {
        self.replication.take_pending()
    }

    /// Record a follower's [`Message::ReplicaAck`], advancing its acknowledged
    /// watermark for the room, then release any withheld client ack the fresh
    /// watermark now carries to a majority. The leader's peer connection calls this
    /// when the follower answers a Replicate.
    pub fn record_replica_ack(&mut self, follower: NodeId, room: &[u8], through_seq: u64) {
        self.replication.record_ack(follower, room, through_seq);
        self.release_pending_acks(room);
    }

    /// `room`'s quorum: the majority threshold and this leader's followers — its
    /// replica set (the primary self plus followers), of size R, minus self. A
    /// majority is `R / 2 + 1`; self, which holds any write it committed, is the
    /// implicit one every quorum count starts from. Single-node mode (no
    /// membership) or a self-only replica set is `(1, [])` — a majority of one self
    /// alone satisfies. The one place the replica set is turned into followers, so
    /// the majority count and [`enqueue_replication`](Self::enqueue_replication)'s
    /// fan-out never diverge on who is a follower.
    fn quorum(&self, room: &[u8]) -> (usize, Vec<NodeId>) {
        let Some(membership) = &self.membership else {
            return (1, Vec::new());
        };
        let replicas = membership.replicas_for(room);
        let majority = replicas.len() / 2 + 1;
        let followers = replicas
            .into_iter()
            .filter(|node| !membership.is_self(node))
            .collect();
        (majority, followers)
    }

    /// Whether a majority of `room`'s replica set holds the write at server
    /// sequence `seq`: self (always one, holding the committed write) plus each
    /// `follower` whose acknowledged watermark has reached `seq`, against the
    /// majority threshold.
    fn quorum_met(&self, room: &[u8], majority: usize, followers: &[NodeId], seq: u64) -> bool {
        let held = 1 + followers
            .iter()
            .filter(|node| self.replication.watermark(room, node) >= seq)
            .count();
        held >= majority
    }

    /// Whether a majority of `room`'s replica set holds the write at server
    /// sequence `seq` — the single-write form of [`quorum_met`](Self::quorum_met),
    /// resolving `room`'s quorum first.
    fn write_has_majority(&self, room: &[u8], seq: u64) -> bool {
        let (majority, followers) = self.quorum(room);
        self.quorum_met(room, majority, &followers, seq)
    }

    /// Release every write withheld for `room` that a majority of its replica set
    /// now holds — a follower ack advanced a watermark — queueing each owed
    /// `Accepted` to its author's outbox and dropping the record. `room`'s quorum
    /// is resolved once, since it is invariant across the withheld writes. A write
    /// whose author has since disconnected is discarded.
    fn release_pending_acks(&mut self, room: &[u8]) {
        let (majority, followers) = self.quorum(room);
        let mut i = 0;
        while i < self.pending_acks.len() {
            let entry = &self.pending_acks[i];
            let release =
                entry.room == room && self.quorum_met(room, majority, &followers, entry.seq);
            if release {
                let pending = self.pending_acks.swap_remove(i);
                if let Some(conn) = self.conns.get_mut(&pending.conn) {
                    conn.outbox.push(pending.accepted);
                }
            } else {
                i += 1;
            }
        }
    }

    /// The server sequence `follower` has acknowledged for `room` — the watermark
    /// a later majority-ack durability unit reads. `0` if nothing yet.
    pub fn replica_watermark(&self, room: &[u8], follower: &NodeId) -> u64 {
        self.replication.watermark(room, follower)
    }

    /// The highest leadership epoch this node has seen for `room` — the split-brain
    /// fence value ([`LeadershipEpochs::highest_seen`]). `0` for a room whose
    /// leadership has never changed. Restored from the store on startup, so it
    /// survives a restart.
    pub fn highest_epoch(&self, room: &[u8]) -> u64 {
        self.epochs.highest_seen(room)
    }

    /// Auto-compact a room once its retained log reaches `threshold` ops, so a
    /// below-floor joiner is served a snapshot instead of a delta. `0` (default)
    /// never compacts.
    pub fn set_compaction_threshold(&mut self, threshold: u64) {
        self.hub.set_compaction_threshold(threshold);
    }

    /// A registry backed by `store`: its hub replays the persisted log, and
    /// every op the hub ingests is appended before it fans out to peers. Each
    /// room's persisted governing binding seeds the live `room_apps`, so a
    /// populated room comes back bound — its first subscriber is served
    /// translated catch-up, not verbatim — before any live subscriber rebuilds it.
    pub fn with_store(server: ClientId, store: Store) -> io::Result<Self> {
        let rooms = store.load()?;
        let room_apps: HashMap<RoomId, (Vec<u8>, u32)> = rooms
            .iter()
            .filter_map(|(room, log)| {
                log.meta
                    .as_ref()
                    .and_then(|meta| meta.governing.clone())
                    .map(|governing| (room.clone(), governing))
            })
            .collect();
        let mut hub = Hub::from_rooms(server, rooms)?;
        hub.attach_store(store);
        let mut registry = Self::from_hub(hub);
        registry.room_apps = room_apps;
        Ok(registry)
    }

    /// Open a connection whose client authenticates in band, returning its
    /// handle.
    pub fn connect(&mut self) -> ConnId {
        self.insert_conn(Session::new())
    }

    /// Open a connection already authenticated as `identity` — the upgrade fast
    /// path (credential verified at accept) or anonymous mode (a minted actor).
    /// Its client skips the in-band Auth phase.
    pub fn connect_authenticated(&mut self, identity: Identity) -> ConnId {
        self.insert_conn(Session::authenticated(identity))
    }

    fn insert_conn(&mut self, session: Session) -> ConnId {
        let id = ConnId(self.next);
        self.next += 1;
        self.conns.insert(
            id,
            Conn {
                session,
                outbox: Vec::new(),
            },
        );
        self.hub.emit(EngineEvent::Connected { conn: id });
        id
    }

    /// Close a connection, dropping its session and any queued messages. Its
    /// ephemeral awareness is not cleared at once: the client is marked stale
    /// with a grace deadline, so a reconnect within the window keeps its presence
    /// and only a later [`sweep`](Registry::sweep) past the deadline drops it.
    pub fn disconnect(&mut self, id: ConnId) {
        // A withheld write-ack for this author is moot once it is gone — drop it, so
        // a room that never reaches a majority (dead followers, no failure detection
        // yet) does not accumulate orphaned records for the process lifetime.
        self.pending_acks.retain(|pending| pending.conn != id);
        if let Some(conn) = self.conns.remove(&id) {
            // The counterpart to the Connected emitted at accept — fires for every
            // closed connection, authenticated or not, so a connect/disconnect
            // pairing stays balanced.
            self.hub.emit(EngineEvent::Disconnected { conn: id });
            // Only an authenticated connection can have published awareness, so
            // only one may influence its grace retention — an unauthenticated
            // Hello-only socket cannot schedule or refresh a sweep for a client
            // id it merely asserted.
            if conn.session.actor().is_none() {
                return;
            }
            if let Some(client) = conn.session.client() {
                // Another live connection under the same client still owns that
                // presence, so a sweep must not clear it — this covers a
                // reconnect race (the new connection registered before the old
                // one's close) and a second connection asserting the same id.
                let still_held = self
                    .conns
                    .values()
                    .any(|c| c.session.client() == Some(client) && c.session.actor().is_some());
                // Only a client with live presence and no other live connection
                // needs a grace timer; otherwise there is nothing a sweep should
                // clear.
                if !still_held && self.hub.has_client_awareness(client) {
                    let deadline = self.clock.now_millis().saturating_add(self.grace_millis);
                    self.stale.insert(client, deadline);
                }
            }
        }
    }

    /// Clear the presence of every client whose grace deadline has passed,
    /// telling each affected room's remaining subscribers with an AwarenessClear
    /// on their own channel. Idempotent; a reconnected client is no longer stale
    /// and is left untouched.
    pub fn sweep(&mut self) {
        let now = self.clock.now_millis();
        let due: Vec<ClientId> = self
            .stale
            .iter()
            .filter(|(_, &deadline)| deadline <= now)
            .map(|(client, _)| *client)
            .collect();
        for client in due {
            self.stale.remove(&client);
            // An actor-wide clear when the actor fully departed the room, else a
            // per-key clear for a key no sibling connection still holds.
            let removals = self.hub.clear_client_awareness(client);
            self.fan_out_removals(removals);
        }
        // Timed-TTL expiry: an entry silent past the TTL its kind is assigned is
        // dropped and its removal fanned out per-key, leaving the actor's other
        // entries (and connection) intact — unlike the actor-wide grace clear. An
        // injected policy is authoritative — it alone governs, suppressing expiry
        // when it declares no TTLs; with none injected the TTLs are resolved from
        // each room's schema. Either way a policy that declares none skips the scan.
        // Reconcile each room's schema binding against who is present every sweep:
        // it governs the authorization tier (consulted under any awareness policy)
        // as well as schema-resolved TTLs, and its pruning bounds the map.
        self.reconcile_room_apps();
        match self.awareness_policy.clone() {
            Some(policy) => self.apply_awareness_policy(now, &*policy),
            None => {
                let policy = self.resolve_schema_policy();
                self.apply_awareness_policy(now, &policy);
            }
        }
        self.fire_schedule_triggers(now);
    }

    /// Fire each bound room's `every:` schedule triggers whose interval has elapsed.
    /// The same sweep that ages the awareness grace window drives the schedules off
    /// one `Clock` read: a trigger is armed to `now` the first sweep it is seen (it
    /// does not capture on the sweep that binds its room) and thereafter captures
    /// once its interval has passed, at most once per sweep — a long gap between
    /// sweeps produces one capture, not a burst catching up every missed interval.
    /// A schedule state whose room is no longer bound is pruned, so a rebound room
    /// re-arms rather than firing on a stale timer.
    fn fire_schedule_triggers(&mut self, now: u64) {
        let bindings: Vec<(RoomId, (Vec<u8>, u32))> = self
            .room_apps
            .iter()
            .map(|(room, app)| (room.clone(), app.clone()))
            .collect();
        let mut live: HashSet<(RoomId, Vec<u8>)> = HashSet::new();
        // (room, template, origin, keep) for each schedule due this sweep, and the
        // keys to stamp with `now`. The fire decision reads the last-fire map as it
        // stood at sweep start and stamping is deferred, so two schedules sharing a
        // key (same interval + name) do not shadow each other — the first's stamp
        // cannot make the second read a fresh `now` and skip. Collected first so the
        // schema borrow is released before the hub is mutated.
        let mut due: Vec<(RoomId, String, Vec<u8>, Option<u64>)> = Vec::new();
        let mut stamp: Vec<(RoomId, Vec<u8>)> = Vec::new();
        for (room, app) in bindings {
            // `parsed_schema` returns an owned `Arc`, so iterating its triggers
            // borrows the schema, not `self` — the last-fire map is read directly and
            // a name is cloned only on the sweep a schedule actually fires.
            let Some(schema) = self.parsed_schema(&app) else {
                continue;
            };
            for av in schema.auto_version() {
                let Trigger::Every(millis) = av.trigger else {
                    continue;
                };
                let origin = schedule_origin(millis, &av.name);
                let key = (room.clone(), origin.clone());
                live.insert(key.clone());
                match self.schedule_fires.get(&key) {
                    // First sight — arm to now, capture one interval later.
                    None => stamp.push(key),
                    // The wall clock stepped backward (an NTP correction) below the
                    // last fire; re-arm to now rather than stall the schedule for the
                    // whole regression (the elapsed would floor to zero until the
                    // clock climbs back past it).
                    Some(&last) if now < last => stamp.push(key),
                    Some(&last) if now - last >= millis => {
                        stamp.push(key);
                        due.push((room.clone(), av.name.clone(), origin, av.keep));
                    }
                    Some(_) => {}
                }
            }
        }
        for key in stamp {
            self.schedule_fires.insert(key, now);
        }
        // Prune schedules whose room unbound, bounding the map and re-arming a room
        // that later rebinds.
        self.schedule_fires.retain(|key, _| live.contains(key));

        if due.is_empty() {
            return;
        }
        // A capture re-emits VersionCreated; suppress the sink recording it, as the
        // post-delivery drain does, so a scheduled version never cascades.
        self.auto_version.set_draining(true);
        for (room, template, origin, keep) in due {
            let name = expand_schedule_name(&template, now);
            self.capture_version(&room, &name, &origin, keep);
        }
        self.auto_version.set_draining(false);
    }

    /// Run `policy` over the current presence: expire entries silent past their
    /// TTL, fanning each removal out to the room's readable peers. Throttling is
    /// enforced on the set path (a coalesced update is simply not fanned out), so
    /// the sweep has nothing to flush. A policy that declares no timed TTL does
    /// nothing.
    fn apply_awareness_policy(&mut self, now: u64, policy: &dyn AwarenessPolicy) {
        if policy.has_timed_ttls() {
            let removals = self.hub.expire_silent_awareness(now, policy);
            self.fan_out_removals(removals);
        }
    }

    /// Recompute every live room's governing `{app_id, version}` and drop the
    /// bindings of dormant rooms. The first enforcing app to bind a room governs
    /// it for as long as the room stays live — a foreign app subscribing never
    /// seizes it, so it cannot grief-expire the incumbent's presence (a room is
    /// served by one app; cross-app reuse governs by the first app until the room
    /// fully empties, then rebinds). The governing version is the highest version
    /// of that app seen while the room has held presence — the bound version is a
    /// floor a present higher version lifts, so a rolling upgrade adopts the newer
    /// schema and a just-departed newer client's grace-held presence keeps its own
    /// (longer) TTL rather than an older peer's; the floor resets when the room
    /// goes dormant. A room with neither presence nor any subscriber is dropped,
    /// bounding the map on a server that churns through rooms.
    fn reconcile_room_apps(&mut self) {
        // One pass over connections: the enforcing apps present per room (each at
        // its highest version) and the set of rooms anyone subscribes.
        let mut present: HashMap<RoomId, HashMap<Vec<u8>, u32>> = HashMap::new();
        let mut subscribed: HashSet<RoomId> = HashSet::new();
        for conn in self.conns.values() {
            let version = conn.session.schema_version();
            for room in conn.session.subscribed_rooms() {
                subscribed.insert(room.clone());
                if let Some(version) = version {
                    let by_app = present.entry(room.clone()).or_default();
                    let entry = by_app
                        .entry(conn.session.app_id().to_vec())
                        .or_insert(version);
                    *entry = (*entry).max(version);
                }
            }
        }
        // A room is live if it holds presence or has a subscriber.
        let mut live: HashSet<RoomId> = self.hub.awareness_rooms().cloned().collect();
        live.extend(subscribed);
        let mut next: HashMap<RoomId, (Vec<u8>, u32)> = HashMap::new();
        for room in live {
            let apps = present.get(&room);
            let governing = match self.room_apps.get(&room) {
                // The incumbent app keeps governing at the highest version of it
                // seen while the room has held presence — the bound version is a
                // floor a currently-present higher version lifts, never lowered.
                // So a rolling upgrade adopts the newer schema and a just-departed
                // newer client's grace-held presence is not expired early under an
                // older peer's shorter TTL; the floor resets when the room goes
                // dormant and the binding is dropped below.
                Some((bound_app, bound_version)) => {
                    let present_version = apps
                        .and_then(|apps| apps.get(bound_app).copied())
                        .unwrap_or(0);
                    Some((bound_app.clone(), present_version.max(*bound_version)))
                }
                // No binding yet — the first present enforcing app takes it.
                None => apps.and_then(pick_app),
            };
            if let Some(governing) = governing {
                next.insert(room, governing);
            }
        }
        self.room_apps = next;
    }

    /// Resolve the timed TTLs for this sweep from each bound room's schema. The
    /// binding is already reconciled against who is present, so this parses each
    /// governing schema out of the shared registry (cached across sweeps, since a
    /// link is immutable) and maps it to the room. A room with no binding resolves
    /// to no schema, so its presence is session-lifetime.
    fn resolve_schema_policy(&mut self) -> SchemaAwarenessPolicy {
        let bindings: Vec<(RoomId, (Vec<u8>, u32))> = self
            .room_apps
            .iter()
            .map(|(room, app)| (room.clone(), app.clone()))
            .collect();
        let mut schemas: HashMap<RoomId, Arc<Schema>> = HashMap::new();
        for (room, app) in bindings {
            if let Some(schema) = self.parsed_schema(&app) {
                schemas.insert(room, schema);
            }
        }
        SchemaAwarenessPolicy::new(schemas)
    }

    /// The coalesce window for entry `key` in `room`: an injected policy's, else
    /// the room's governing schema's `awareness.<kind>.throttle`. Resolved on the
    /// set path, so a room with no binding (relay) or no throttle is unthrottled.
    fn resolve_throttle(&mut self, room: &[u8], key: &[u8]) -> Option<u64> {
        if let Some(policy) = &self.awareness_policy {
            return policy.throttle(room, key);
        }
        let app = self.room_apps.get(room)?.clone();
        let schema = self.parsed_schema(&app)?;
        let kind = std::str::from_utf8(key).ok()?;
        schema.awareness_entry(kind).and_then(|e| e.throttle)
    }

    /// The parsed schema for `{app_id, version}`, resolved out of the shared
    /// registry and cached for the process lifetime — a link never changes once
    /// registered, so the outcome is cached even when it is `None` (a version the
    /// registry does not hold, or a body that fails to parse), sparing a re-lock
    /// and re-parse on every sweep of a room bound to an unparseable schema.
    fn parsed_schema(&mut self, app: &(Vec<u8>, u32)) -> Option<Arc<Schema>> {
        let schema = match self.schema_cache.get(app) {
            Some(schema) => schema.clone(),
            None => {
                let registry = match self.schema.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                let schema = registry
                    .resolve(&app.0, app.1)
                    .and_then(|src| std::str::from_utf8(src).ok())
                    .and_then(|src| Schema::parse(src).ok())
                    .map(Arc::new);
                drop(registry);
                self.schema_cache.insert(app.clone(), schema.clone());
                schema
            }
        };
        // Arm auto-version recording the first time a schema that declares any
        // trigger is resolved — resolved for a subscribe's authorization *before*
        // its `Subscribed` fires, so the arming subscribe is itself recorded (a
        // room already populated at a fresh server's first subscribe still
        // captures). Until armed the sink records nothing, so a deployment with no
        // `autoVersion` pays no per-event cost.
        if !self.auto_version.is_armed()
            && schema
                .as_ref()
                .is_some_and(|s| !s.auto_version().is_empty())
        {
            self.auto_version.arm();
        }
        schema
    }

    /// The parsed schema a connection declared — its own `{app_id, version}`
    /// resolved against the registry. The authorization fallback for a room not
    /// yet bound (its first subscriber, about to become the room's incumbent):
    /// once a room is bound, [`governing_schema`](Registry::governing_schema) —
    /// the room's, not the connection's — governs, so a foreign app cannot pick a
    /// permissive schema to escalate. `None` for a relay connection.
    fn connection_schema(&mut self, id: ConnId) -> Option<Arc<Schema>> {
        let conn = self.conns.get(&id)?;
        let version = conn.session.schema_version()?;
        let app = (conn.session.app_id().to_vec(), version);
        self.parsed_schema(&app)
    }

    /// The parsed schema governing `room` — the app bound to it — which gates a
    /// peer's read of the room's fan-out. `None` for a relay room none enforces.
    fn governing_schema(&mut self, room: &[u8]) -> Option<Arc<Schema>> {
        let app = self.room_apps.get(room)?.clone();
        self.parsed_schema(&app)
    }

    /// Whether `identity` may fetch the blob whose public handle is `blob_id` — the
    /// out-of-band blob-fetch authorization. A blob is content-addressed and
    /// immutable, so authority cannot attach to the bytes: it attaches to the
    /// **reference site**. The fetch is allowed iff `identity` holds READ authority
    /// on at least one live `core::path` that currently references `blob_id`,
    /// resolved through the SAME per-recipient read evaluator op redaction uses
    /// ([`recipient_reads_path`](crate::acl::recipient_reads_path)) — deployment
    /// policy, doc-ACL tuples, and schema grants composed exactly as the op stream
    /// composes them. A blob's handle is room-independent, so every room is scanned
    /// and the first readable reference grants.
    ///
    /// Fail-closed on every ambiguous case: a blob no live path references (a leaked
    /// or guessed id, a since-deleted reference), or one referenced only under paths
    /// `identity` cannot read (a redacted or denied position), is **denied** — even
    /// for an authenticated caller, and even for the room creator who owns `/` (an
    /// owner still cannot fetch a blob nothing in the document references). This
    /// mirrors the element-id redaction model: a reference the recipient cannot see
    /// must not be fetchable (the drag-to-exfil analogue for blobs).
    pub fn authorize_blob_fetch(&mut self, identity: &Identity, blob_id: &[u8; 16]) -> bool {
        for room in self.hub.room_ids() {
            let refs = self.hub.blob_ref_paths(&room);
            let Some(paths) = refs.get(blob_id) else {
                continue;
            };
            let records = self.hub.acl_records(&room);
            let creator = self.hub.room_creator(&room);
            let index = self.hub.element_paths(&room);
            let schema = self.governing_schema(&room);
            let authorizer = &*self.authorizer;
            if paths.iter().any(|path| {
                crate::acl::recipient_reads_path(
                    authorizer,
                    &records,
                    creator.as_deref(),
                    &index,
                    schema.as_deref(),
                    identity,
                    &room,
                    path,
                )
            }) {
                return true;
            }
        }
        false
    }

    /// The `(app, version)` a broadcast is translated *from*, or `None` when it
    /// needs no translation. Migration translation walks the room's governing
    /// app's chain, so it applies only when the write carried a version and the
    /// writer speaks that same app — a relay write, an unbound room, or a
    /// foreign-app write (whose version number is a different app's space) is
    /// left verbatim.
    fn translation_source(
        &self,
        room: &[u8],
        writer: &ConnId,
        version: Option<u32>,
    ) -> Option<(Vec<u8>, u32)> {
        let from = version?;
        let (app, _) = self.room_apps.get(room)?;
        let writer_app = self.conns.get(writer)?.session.app_id();
        (writer_app == app.as_slice()).then(|| (app.clone(), from))
    }

    /// The parsed migration chain from `from` to each distinct target version
    /// among the room's same-app recipients, resolved once (the registry is
    /// locked only here, not across the fan-out). A target whose chain is
    /// unreachable, gapped, or unparseable maps to `None`, so the fan-out drops
    /// that recipient's batch rather than serving it wrong.
    fn resolve_chains(
        &self,
        writer: &ConnId,
        app: &[u8],
        from: u32,
    ) -> HashMap<u32, Option<crate::translate::Chain>> {
        let targets: HashSet<u32> = self
            .conns
            .iter()
            .filter(|(peer, _)| *peer != writer)
            .filter(|(_, conn)| conn.session.app_id() == app)
            .filter_map(|(_, conn)| conn.session.schema_version())
            .filter(|target| *target != from)
            .collect();
        if targets.is_empty() {
            return HashMap::new();
        }
        let registry = match self.schema.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        targets
            .into_iter()
            .map(|target| {
                (
                    target,
                    crate::translate::resolve_chain(&registry, app, from, target).ok(),
                )
            })
            .collect()
    }

    /// Fan a committed op batch out to `(room, branch)`'s subscribers with
    /// per-recipient doc-ACL redaction: each recipient receives only the ops in
    /// subtrees its actor may read, the rest silently withheld from *it* while an
    /// authorized peer still gets them. The room's creator (owns `/`) reads every
    /// op; a subtree-scoped reader receives just its granted subtrees.
    ///
    /// Redaction runs on the *authored* ops (their target resolves through the
    /// room's element-path index), then the surviving subset is migration-translated
    /// to each recipient's version — redact-then-translate, since translation can
    /// drop ops and would otherwise desync the path lookup. An op whose target the
    /// index cannot resolve reads at the root ([`op_read_path`](crate::acl::op_read_path)),
    /// so only a whole-document reader carries it — never a subtree-scoped one.
    fn fan_out_ops_redacted(
        &mut self,
        writer: ConnId,
        room: &[u8],
        branch: &[u8],
        broadcast: &[Op],
        broadcast_version: Option<u32>,
        records: Vec<crdtsync_core::acl::AclRecord>,
    ) {
        let creator = self.hub.room_creator(room);
        let schema = self.governing_schema(room);
        let index = self.hub.element_paths(room);
        // A RangedElement op resolves its governing seq paths through the held anchor
        // set (a SetPayload/Delete carries only the range id); tombstoned ranges are
        // included so a just-applied delete still resolves.
        let ranged_anchors = self.hub.ranged_anchors(room);
        // The owning-element type of each op, resolved once over the room document
        // — a type-scoped migration step narrows to the ops whose owning element is
        // of its declared type. Empty (no narrowing) when the room binds no schema.
        let types = schema
            .as_ref()
            .map(|s| self.hub.element_types(room, s))
            .unwrap_or_default();
        // Each op's set of governing document paths is recipient-independent, so
        // resolve it once. A recipient must read every path in an op's set to receive
        // it (require-all — a Ranged op's distinct anchor seq paths, one path for every
        // other op).
        let op_paths: Vec<Vec<Vec<u8>>> = broadcast
            .iter()
            .map(|op| crate::acl::op_read_paths(&index, &ranged_anchors, &records, op))
            .collect();
        // Migration translation rides the same seam as redaction (scoped to the
        // room's governing app); resolve each distinct target's chain once.
        let source = self.translation_source(room, &writer, broadcast_version);
        let chains = source
            .as_ref()
            .map(|(app, from)| self.resolve_chains(&writer, app, *from));
        let authorizer = &*self.authorizer;
        // The nodes this batch relocates. A move that carries a node into a subtree a
        // recipient can read, out of one it could not, reveals a born-denied node to
        // that recipient (reveal-on-move-in) — a shell must precede the move so the
        // recipient can materialize it, mirroring the catch-up seam.
        let hub = &self.hub;
        // Each relocated node paired with its move's zone: a reveal shell (and its
        // back-filled content) is stamped with the move's zone so the per-channel zone
        // filter co-travels them — a shell never rides to a channel whose zone filter
        // drops its placing move (which would strand an unplaced node). For a room with no
        // zones every zone is `None`, so this is a no-op.
        let moved_nodes: Vec<(ElementId, Option<u32>)> = broadcast
            .iter()
            .filter_map(|op| match &op.kind {
                OpKind::XmlMove { node, .. } => Some((*node, op.zone)),
                _ => None,
            })
            .collect();
        for (peer, conn) in self.conns.iter_mut() {
            if *peer == writer {
                continue;
            }
            let Some(identity) = conn.session.identity() else {
                continue;
            };
            // Keep the authored ops this recipient may read — every governing path in
            // the op's set (require-all). The read verdict depends only on the path, so
            // a batch touching one subtree resolves once — memoized per distinct path to
            // avoid re-hashing the actor per op.
            let mut verdict: HashMap<&[u8], bool> = HashMap::new();
            let readable: Vec<Op> = broadcast
                .iter()
                .zip(&op_paths)
                .filter_map(|(op, paths)| {
                    let ok = paths.iter().all(|path| {
                        *verdict.entry(path).or_insert_with(|| {
                            crate::acl::recipient_reads_path(
                                authorizer,
                                &records,
                                creator.as_deref(),
                                &index,
                                schema.as_deref(),
                                identity,
                                room,
                                path,
                            )
                        })
                    });
                    ok.then(|| op.clone())
                })
                .collect();
            if readable.is_empty() {
                continue;
            }
            // Reveal-on-move-in: for every node this batch moves into a position this
            // recipient can read but was born where it could not, prepend a shell so the
            // recipient materializes the node and the (readable) move folds it into place
            // — the live-fan-out mirror of the catch-up reveal, derived from the same read
            // predicate. A recipient reading the node's origin all along gets no shell
            // (`reveal_ops` returns it only when the birth path is denied). Shells lead so
            // the move lands onto them.
            let readable = if moved_nodes.is_empty() {
                readable
            } else {
                let shells: Vec<Op> = hub
                    .reveal_ops(
                        room,
                        crate::acl::recipient_reads_predicate(
                            authorizer,
                            &records,
                            creator.as_deref(),
                            &index,
                            schema.as_deref(),
                            identity,
                            room,
                        ),
                    )
                    .into_iter()
                    .filter_map(|mut op| match &op.kind {
                        OpKind::XmlReveal { node, .. } => moved_nodes
                            .iter()
                            .find(|(n, _)| n == node)
                            .map(|(_, zone)| {
                                op.zone = *zone;
                                op
                            }),
                        _ => None,
                    })
                    .collect();
                if shells.is_empty() {
                    readable
                } else {
                    // Each revealed node's shell, then its now-readable subtree content
                    // replayed from the log — content authored while the subtree was
                    // private is withheld on the live stream and absent from this batch,
                    // so without the back-fill a live reader would materialize an empty
                    // node and diverge from a fresh/snapshot joiner. The shell + content
                    // lead the delta; the readable move (in `readable`) then folds them
                    // into place. The shell and content carry the move's zone so the
                    // per-channel zone filter keeps them together.
                    let mut prefix: Vec<Op> = Vec::new();
                    for shell in shells {
                        let OpKind::XmlReveal { node, .. } = &shell.kind else {
                            continue;
                        };
                        let node = *node;
                        let zone = shell.zone;
                        prefix.push(shell);
                        prefix.extend(
                            hub.reveal_backfill(room, node, &records, |p| {
                                crate::acl::recipient_reads_path(
                                    authorizer,
                                    &records,
                                    creator.as_deref(),
                                    &index,
                                    schema.as_deref(),
                                    identity,
                                    room,
                                    p,
                                )
                            })
                            .into_iter()
                            .map(|mut op| {
                                op.zone = zone;
                                op
                            }),
                        );
                    }
                    prefix.into_iter().chain(readable).collect()
                }
            };
            // Translate the surviving subset to the recipient's version, or send it
            // verbatim (a same-version, relay, or foreign-app recipient). An
            // unresolved chain drops the batch, fail-closed, pending the handshake
            // range-check that refuses that recipient outright.
            let translated = match (&source, conn.session.schema_version()) {
                (Some((app, from)), Some(target))
                    if conn.session.app_id() == app && target != *from =>
                {
                    match chains.as_ref().and_then(|c| c.get(&target)) {
                        Some(Some(chain)) => chain.translate_ops_scoped(&readable, &types),
                        _ => Vec::new(),
                    }
                }
                _ => readable,
            };
            if translated.is_empty() {
                continue;
            }
            for channel in conn.session.channels_for_stream(room, branch) {
                // Narrow to the channel's authorized zone partitions — the wire
                // redaction for per-zone streams. A channel scoped to a subset of the
                // room's zones drops the rest; an unauthorized zone never surfaces,
                // and a channel left with nothing is not sent an empty frame.
                let ops = conn.session.zone_filter(channel, &translated);
                if ops.is_empty() {
                    continue;
                }
                conn.outbox.push(Message::Ops { channel, ops });
            }
        }
    }

    /// Bind `room`'s governing schema to `{app_id, version}`. The first app to
    /// bind a room governs it — a later subscribe naming a *different* app is
    /// ignored, so a room is never re-governed by a foreign app's (shorter) TTL.
    /// A later subscribe on the *same* app lifts the binding to the higher
    /// version, so a rolling upgrade resolves to the newest version. The incumbent
    /// is the live binding, or — where a dormant sweep or a restart dropped it —
    /// the durable one the hub restored, so a foreign app cannot seize a
    /// persisted room by subscribing first after it went idle. A same-app bind (a
    /// new room or a version lift) is mirrored into the hub's durable binding.
    fn bind_room_app(&mut self, room: RoomId, app_id: Vec<u8>, version: u32) {
        let incumbent = self
            .room_apps
            .get(&room)
            .cloned()
            .or_else(|| self.hub.governing_app(&room));
        let bound = match incumbent {
            Some((bound_app, bound_version)) if bound_app == app_id => {
                (app_id, version.max(bound_version))
            }
            Some(existing) => existing,
            None => (app_id, version),
        };
        self.hub.bind_governing(&room, bound.0.clone(), bound.1);
        self.room_apps.insert(room, bound);
    }

    /// Tell each room's readable subscribers of the awareness removals a sweep
    /// produced, on every channel they opened for the room. Learning a peer's
    /// presence cleared is a read of the room, so the same per-recipient gate as
    /// the set fan-out applies — a read-revoked peer is not told of the removal.
    fn fan_out_removals(&mut self, removals: Vec<crate::AwarenessRemoval>) {
        for removal in removals {
            let room = removal.room().to_vec();
            let schema = self.governing_schema(&room);
            let authorizer = &*self.authorizer;
            for conn in self.conns.values_mut() {
                if !peer_may_read(authorizer, schema.as_deref(), &conn.session, &room) {
                    continue;
                }
                for channel in conn.session.channels_for_room(&room) {
                    conn.outbox.push(removal.message(channel));
                }
            }
        }
    }

    /// Drive one inbound message through the connection's session, queueing its
    /// replies and fanning any broadcast out to the room's other connections.
    /// Returns whether the connection should stay open.
    pub fn deliver(&mut self, id: ConnId, msg: Message) -> bool {
        // A Replicate or a Gossip arrives node-to-node on a peer connection, not
        // from a client on its data plane — intercept each before the client
        // session step (which treats both as a violation) and handle it as a peer.
        let msg = match msg {
            Message::Replicate {
                room,
                branch,
                ops,
                base_seq,
                epoch,
            } => return self.apply_replicate(id, room, branch, ops, base_seq, epoch),
            Message::ReplicateSnapshot {
                room,
                branch,
                seq,
                state,
                epoch,
            } => return self.apply_replicate_snapshot(id, room, branch, seq, state, epoch),
            Message::Gossip { members } => return self.apply_gossip(id, members),
            // A follower's durable-head report arrives node-to-node on a peer
            // connection; catch it up from the reported heads off the client session
            // path. Self-describing (it names the reporting node), so no connection
            // identity is needed — handled exactly as a Gossip.
            Message::FollowerHeads { reporter, heads } => {
                return self.apply_follower_heads(reporter, heads)
            }
            // A ping-req arrives node-to-node on a peer connection asking this relay
            // for its liveness view of a third member; answer it off the client
            // session path. A ping-ack is only ever read inline by the requester that
            // sent the ping-req (never delivered), so one reaching here is unsolicited
            // — drop the connection.
            Message::PingReq { target } => return self.apply_ping_req(id, target),
            Message::PingAck { .. } => return false,
            other => other,
        };
        // An awareness set consults the clock (to stamp last-seen); a cross-zone
        // token request stamps the token's expiry and its redemption checks that
        // expiry — so those three read wall time. The ordinary op hot path does not.
        let now = if matches!(
            msg,
            Message::AwarenessSet { .. }
                | Message::CrossZoneToken { .. }
                | Message::CrossZoneOps { .. }
        ) {
            self.clock.now_millis()
        } else {
            0
        };
        // An awareness set consults the room's throttle for its kind, to coalesce
        // a within-window update; resolved here (from the channel's room) since
        // `step` has no policy.
        let throttle = match &msg {
            Message::AwarenessSet { channel, key, .. } => self
                .conns
                .get(&id)
                .and_then(|conn| conn.session.room_for_channel(*channel).cloned())
                .and_then(|room| self.resolve_throttle(&room, key)),
            _ => None,
        };
        // A subscribe binds the room's governing schema to the connection's app,
        // once the subscribe is known to have been accepted below.
        let subscribed_room = match &msg {
            Message::Subscribe { room, .. } => Some(room.clone()),
            _ => None,
        };
        // The room this message authorizes against, so its enforcement composes
        // under the schema that governs *that room* — never the actor's own,
        // self-declared app, which a foreign connection could pick to escalate.
        let authz_room: Option<RoomId> = match &msg {
            Message::Subscribe { .. } => subscribed_room.clone(),
            // Room-keyed like branch/clone management: the token request names its
            // room directly, so its schema binds off the frame's room.
            Message::CrossZoneToken { room, .. } => Some(room.clone()),
            Message::Ops { channel, .. }
            | Message::CrossZoneOps { channel, .. }
            | Message::AwarenessSet { channel, .. }
            | Message::VersionCreate { channel, .. }
            | Message::VersionRename { channel, .. }
            | Message::VersionDelete { channel, .. }
            | Message::VersionList { channel, .. }
            | Message::VersionFetch { channel, .. } => self
                .conns
                .get(&id)
                .and_then(|c| c.session.room_for_channel(*channel).cloned()),
            _ => None,
        };
        // The acted-on room's binding, resolved once: `Some(Some((app, ver)))`
        // bound, `Some(None)` an addressed-but-unbound room, `None` no room. A
        // room missing from the live map falls back to the hub's durable binding,
        // so a populated room a dormant sweep or a restart left unbound is still
        // governed by its persisted app — its first subscriber is served
        // translated, not verbatim.
        let room_binding = authz_room.as_deref().map(|room| {
            self.room_apps
                .get(room)
                .cloned()
                .or_else(|| self.hub.governing_app(room))
        });
        // The schema whose `@auth` grants the enforcement points compose under the
        // deployment authorizer. A room already bound is governed by *its* app's
        // schema — never the connection's own, even when that schema fails to parse
        // (then `None`: no grants, default-deny), so a foreign connection cannot
        // escalate against a permissive self-declared app. The connection's own app
        // is the fallback only for a room not yet in the bindings — its first
        // subscriber, about to become the incumbent.
        let acting_schema = match &room_binding {
            // Bound: governed by the room's own app's schema — never the
            // connection's — even when it fails to parse (`None`: no grants).
            Some(Some(app)) => self.parsed_schema(app),
            // Unbound (first subscriber): fall back to the connection's own app.
            Some(None) => self.connection_schema(id),
            None => None,
        };
        // The app governing the acted-on room — the chain a catch-up delta is
        // translated along and the space a write's version is tagged in. Resolved
        // only for the two data-plane messages that consult it (a subscribe's
        // catch-up, an ops write's tag), and only for a *bound* room: an unbound
        // room's governing app is unknown (a catch-up there serves the delta
        // verbatim; an ops write is impossible until the room is bound by the
        // writer's own subscribe). The binding a dormant sweep or a restart
        // dropped is recovered above from the hub's durable record, not inferred
        // from the connecting app — inferring would let a foreign first subscriber
        // translate the log along the wrong chain.
        let governing = match room_binding {
            Some(Some((app, version)))
                if matches!(
                    msg,
                    Message::Subscribe { .. } | Message::Ops { .. } | Message::CrossZoneOps { .. }
                ) =>
            {
                Some((app, version))
            }
            _ => None,
        };
        // An ops write's room and its op-version high-water before the write. If
        // the ingest raises the high-water past a joined enforcing peer's reach,
        // that peer is re-checked and evicted below — captured pre-step so the
        // lift is the pre/post delta.
        let lift_room: Option<(RoomId, Option<u32>)> = match &msg {
            Message::Ops { .. } | Message::CrossZoneOps { .. } => authz_room
                .as_ref()
                .map(|room| (room.clone(), self.hub.max_op_version(room))),
            _ => None,
        };
        // A write's `Accepted` is the one reply gated on majority replication, so
        // it is pulled out of the step's replies below and released only once the
        // room's replica set confirms the write durable.
        let is_ops_write = matches!(msg, Message::Ops { .. } | Message::CrossZoneOps { .. });
        let (
            broadcast,
            broadcast_version,
            close,
            room,
            broadcast_branch,
            awareness,
            authed_client,
            bind,
            newly_subscribed,
            owed_accept,
        ) = {
            let Some(conn) = self.conns.get_mut(&id) else {
                return false;
            };
            // Whether the acted-on room was already subscribed before this step, so
            // an accepted subscribe is told from a rejected re-subscribe of an
            // already-mapped room — only the transition is the lifecycle event.
            let was_subscribed = subscribed_room
                .as_deref()
                .is_some_and(|room| conn.session.subscribed_rooms().any(|r| r == room));
            // Pass the shared registry unlocked: `step` locks it only for the
            // Hello resolve, so a slow verifier in the Auth branch never holds it
            // and cannot stall the admin plane's writes. `now` stamps an awareness
            // set's last-seen time, the basis for its timed-TTL expiry.
            let resp = step(
                &mut self.hub,
                &mut conn.session,
                &*self.verifier,
                &*self.authorizer,
                acting_schema.as_deref(),
                &self.schema,
                governing
                    .as_ref()
                    .map(|(app, version)| (app.as_slice(), *version)),
                self.membership.as_ref(),
                now,
                throttle,
                msg,
            );
            // A write's `Accepted` is withheld from the outbox and carried out to
            // the majority gate below; every other reply — errors, adverts, the
            // catch-up, an awareness fan-out — is queued for send now.
            let owed_accept = if is_ops_write {
                let mut owed = None;
                for reply in resp.replies {
                    match reply {
                        accepted @ Message::Accepted { .. } => owed = Some(accepted),
                        other => conn.outbox.push(other),
                    }
                }
                owed
            } else {
                conn.outbox.extend(resp.replies);
                None
            };
            // Only an authenticated session may touch a client's grace timer, so
            // a bare Hello-only socket can neither cancel a pending sweep nor
            // keep a foreign client id's presence alive.
            let authed_client = conn
                .session
                .actor()
                .is_some()
                .then(|| conn.session.client())
                .flatten();
            // Whether the acted-on room is subscribed after the step — the single
            // acceptance fact `bind` and the lifecycle event both read.
            let is_subscribed = subscribed_room
                .as_deref()
                .is_some_and(|room| conn.session.subscribed_rooms().any(|r| r == room));
            // Bind the room only if the subscribe was accepted — a channel now
            // maps it — and the connection is enforcing (a resolved version). A
            // rejected (unauthenticated or read-denied) or relay subscribe
            // governs nothing, so it cannot schema-expire a room's presence.
            let bind = if is_subscribed {
                subscribed_room.as_deref().and_then(|room| {
                    let version = conn.session.schema_version()?;
                    Some((room.to_vec(), conn.session.app_id().to_vec(), version))
                })
            } else {
                None
            };
            // A Subscribed fires only on the transition — this delivery is what
            // subscribed the room — so a rejected re-subscribe of an already-mapped
            // room does not re-fire. Relay or enforcing alike, broader than `bind`.
            let newly_subscribed = is_subscribed && !was_subscribed;
            (
                resp.broadcast,
                resp.broadcast_version,
                resp.close,
                resp.broadcast_room,
                resp.broadcast_branch,
                resp.awareness,
                authed_client,
                bind,
                newly_subscribed,
                owed_accept,
            )
        };
        if newly_subscribed {
            if let Some(room) = &subscribed_room {
                self.hub.emit(EngineEvent::Subscribed {
                    conn: id,
                    room: room.as_slice(),
                });
            }
        }
        // A client reappearing within its grace window cancels the pending
        // clear once it re-authenticates, so its presence survives the gap.
        if let Some(client) = authed_client {
            self.stale.remove(&client);
        }
        // Bind the subscribed room to the enforcing app governing it, so both the
        // schema-authorization tier and (with no injected policy) presence expiry
        // resolve its schema. Bound unconditionally — authorization consults the
        // binding on every room even under an injected awareness policy — and a
        // sweep's reconcile prunes dormant rooms, so the map stays bounded.
        if let Some((room, app_id, version)) = bind {
            self.bind_room_app(room, app_id, version);
        }
        // A leader mirrors each fresh commit to its follower replicas, so a client
        // redirected to the leader reaches a node that already holds the state.
        // Queued here, before the client fan-out, from the same durably-logged
        // ops; single-node mode (no membership) and a non-leading node enqueue
        // nothing.
        if !broadcast.is_empty() {
            if let (Some(room), Some(branch)) = (&room, &broadcast_branch) {
                self.enqueue_replication(room, branch, &broadcast);
            }
        }
        // Gate the write's ack on majority durability. Only a main-stream write
        // with fresh ops in a room that has not yet reached a majority is
        // withheld — held in `pending_acks` until a follower ack meets the quorum.
        // Everything else is durable now and released at once: a branch write (not
        // yet mirrored to followers), a no-op resend (no fresh ops to replicate), a
        // single-node or self-only replica set, or — defensively — an `Accepted`
        // with no committed room, which is sent rather than silently dropped.
        if let Some(accepted) = owed_accept {
            let withhold = match &room {
                Some(room)
                    if broadcast_branch.as_deref() == Some(MAIN_BRANCH)
                        && !broadcast.is_empty() =>
                {
                    let write_seq = self.hub.seq(room);
                    (!self.write_has_majority(room, write_seq)).then(|| (room.clone(), write_seq))
                }
                _ => None,
            };
            match withhold {
                Some((room, seq)) => self.pending_acks.push(PendingAck {
                    room,
                    seq,
                    conn: id,
                    accepted,
                }),
                None => {
                    if let Some(conn) = self.conns.get_mut(&id) {
                        conn.outbox.push(accepted);
                    }
                }
            }
        }
        // A broadcast holds only ops the hub durably logged (see `Hub::ingest`),
        // so fanning it out never advertises an unpersisted write. Each peer is
        // sent the ops on the channel it opened for the room, so a peer
        // multiplexing several rooms can route what it receives.
        if !broadcast.is_empty() {
            // A broadcast is scoped to its `(room, branch)` stream: an `Ops` write
            // always names both, so a branch write reaches only that branch's
            // subscribers and never crosses into another branch's stream.
            if let (Some(room), Some(branch)) = (room, broadcast_branch) {
                // A room with live doc-ACL tuples redacts per-recipient by the op's
                // document path — a recipient receives only the ops in subtrees its
                // actor may read. A room with none (the `else`) fans out unredacted:
                // the whole-document read gate plus per-target migration translation,
                // no path walk.
                let records = self.hub.acl_records(&room);
                if !records.is_empty() {
                    self.fan_out_ops_redacted(
                        id,
                        &room,
                        &branch,
                        &broadcast,
                        broadcast_version,
                        records,
                    );
                } else {
                    // The room's governing schema gates each peer's read consistently,
                    // resolved once (owned) so the peer loop can borrow the conns.
                    let schema = self.governing_schema(&room);
                    let authorizer = &*self.authorizer;
                    // The owning-element type of each op, resolved once over the room
                    // document, so a type-scoped migration step narrows to the ops
                    // whose owning element is of its declared type. Empty (no
                    // narrowing) when the room binds no schema.
                    let types = schema
                        .as_ref()
                        .map(|s| self.hub.element_types(&room, s))
                        .unwrap_or_default();
                    // Per-recipient migration translation rides the same seam as
                    // redaction. It is scoped to the room's governing app: the write
                    // is translated only when the writer speaks that app (its version
                    // number lives in that app's space), and only to recipients of
                    // that app — a foreign-app connection's version is a different
                    // space and must never drive the room's chain.
                    let source = self.translation_source(&room, &id, broadcast_version);
                    // Resolve every distinct target version's chain up front, holding
                    // the registry lock only for that (not across the fan-out), then
                    // translate the peer loop against the owned, parsed chains.
                    let chains = source
                        .as_ref()
                        .map(|(app, from)| self.resolve_chains(&id, app, *from));
                    // Translate the batch once per distinct target version — the
                    // rewrite depends only on the version, not the recipient, so a
                    // same-version fleet shares one result. A resolved chain
                    // translates; an unresolved one (unreachable / gapped /
                    // unparseable) yields an empty batch, dropping it for that
                    // target's recipients pending the handshake range-check that
                    // refuses them outright.
                    let translated_by_target: HashMap<u32, Vec<Op>> = chains
                        .iter()
                        .flatten()
                        .map(|(target, chain)| {
                            let ops = match chain {
                                Some(chain) => chain.translate_ops_scoped(&broadcast, &types),
                                None => Vec::new(),
                            };
                            (*target, ops)
                        })
                        .collect();
                    for (peer, conn) in self.conns.iter_mut() {
                        if *peer == id {
                            continue;
                        }
                        // Per-recipient redaction: a peer whose read was revoked
                        // mid-session stops receiving the room's ops at once, without
                        // waiting for it to resubscribe.
                        if !peer_may_read(authorizer, schema.as_deref(), &conn.session, &room) {
                            continue;
                        }
                        // Translate to the recipient's version, or send verbatim when
                        // there is nothing to bridge — a same-version, relay, or
                        // foreign-app recipient, or a relay write.
                        let translated = match (&source, conn.session.schema_version()) {
                            (Some((app, from)), Some(target))
                                if conn.session.app_id() == app && target != *from =>
                            {
                                // Total over every eligible recipient: `resolve_chains`
                                // keyed the memo on this same (same-app, target != from)
                                // predicate, so the target is always present.
                                Some(translated_by_target[&target].as_slice())
                            }
                            _ => None,
                        };
                        let ops = translated.unwrap_or(&broadcast);
                        if ops.is_empty() {
                            continue;
                        }
                        for channel in conn.session.channels_for_stream(&room, &branch) {
                            // Narrow to the channel's authorized zone partitions — the
                            // per-zone wire redaction. A channel scoped to a subset of
                            // the room's zones drops the rest, so an unauthorized zone
                            // never surfaces on it; an emptied channel gets no frame.
                            let zoned = conn.session.zone_filter(channel, ops);
                            if zoned.is_empty() {
                                continue;
                            }
                            conn.outbox.push(Message::Ops {
                                channel,
                                ops: zoned,
                            });
                        }
                    }
                }
            }
        }
        // Awareness is ephemeral: fan the entry out to the room's other
        // subscribers on each peer's channel; nothing is echoed back to the
        // originating connection.
        if let Some(a) = awareness {
            let schema = self.governing_schema(&a.room);
            let authorizer = &*self.authorizer;
            for (peer, conn) in self.conns.iter_mut() {
                if *peer == id {
                    continue;
                }
                // Seeing a peer's presence is a read of the room, so the same
                // per-recipient check gates the awareness fan-out.
                if !peer_may_read(authorizer, schema.as_deref(), &conn.session, &a.room) {
                    continue;
                }
                for channel in conn.session.channels_for_room(&a.room) {
                    conn.outbox.push(Message::AwarenessUpdate {
                        channel,
                        actor: a.actor.clone(),
                        key: a.key.clone(),
                        value: a.value.clone(),
                    });
                }
            }
        }
        // A write that raised the room's op-version high-water can strand a joined
        // enforcing peer whose back-compat reach the new high-water opens past:
        // fan-out fail-closes its down-drop, so evict it with `UpdateRequired`
        // rather than leaving it silently un-updated. Only a genuine lift re-checks
        // — a same-or-lower write moves nothing.
        if let (Some((room, pre_high_water)), Some((app, version))) = (lift_room, &governing) {
            let post_high_water = self.hub.max_op_version(&room);
            if post_high_water > pre_high_water {
                self.evict_stranded(&room, (app.as_slice(), *version), post_high_water);
            }
        }
        // Every room-bearing lifecycle event this delivery emitted (a subscribe, a
        // version mutation, a compaction) was recorded by the auto-version sink;
        // act on them now that the delivery has committed.
        self.drain_auto_versions();
        !close
    }

    /// Re-check each joined enforcing subscriber of `room` against the lifted
    /// op-version `high_water` and evict any the write just stranded — a peer of
    /// the governing app whose version can no longer down-reach the high-water
    /// across a back-compatible path. The evicted peer is sent `UpdateRequired`
    /// and dropped from the room, so it stops receiving fan-out and must
    /// re-subscribe after updating. A relay, foreign-app, versionless, or
    /// still-reachable peer — the writer included — is untouched: eviction reuses
    /// the exact predicate the subscribe gate admits on, so admission and eviction
    /// agree.
    fn evict_stranded(&mut self, room: &[u8], governing: (&[u8], u32), high_water: Option<u32>) {
        let schema = &self.schema;
        let stranded: Vec<ConnId> = self
            .conns
            .iter()
            .filter(|(_, conn)| {
                conn.session
                    .subscribed_rooms()
                    .any(|r| r.as_slice() == room)
            })
            .filter(|(_, conn)| {
                !crate::session::subscriber_reaches_governing(
                    schema,
                    Some(governing),
                    &conn.session,
                    high_water,
                )
            })
            .map(|(id, _)| *id)
            .collect();
        for id in stranded {
            if let Some(conn) = self.conns.get_mut(&id) {
                // `Error` names no channel, so one frame evicts the peer from the
                // room however many channels it held it on.
                if !conn.session.drop_room(room).is_empty() {
                    conn.outbox.push(Message::Error {
                        code: ErrorCode::UpdateRequired,
                        message: "a write raised the room's version beyond this peer's reach"
                            .to_string(),
                        details: Vec::new(),
                    });
                }
            }
        }
    }

    /// Capture the auto-versions the recorded lifecycle events call for. For each
    /// signal, resolve the room's governing schema and, for every `on:` trigger it
    /// declares matching the event, capture a version named by the expanded
    /// template and prune the trigger's captures to its `keep` window. A relay room
    /// (no governing schema) or an unmatched event captures nothing. `every:`
    /// schedule triggers are the sweep's concern, not an event's.
    ///
    /// A capture re-emits `VersionCreated`; the `draining` flag suppresses the sink
    /// recording that, so an auto-created version never cascades into another.
    fn drain_auto_versions(&mut self) {
        if self.auto_version.is_empty() {
            return;
        }
        let signals = self.auto_version.take();
        // Read wall time once for every `${timestamp}` in this drain — off the op
        // hot path, which emits no room-bearing event and so never reaches here.
        let now = self.clock.now_millis();
        self.auto_version.set_draining(true);
        for (room, event) in signals {
            let Some(app) = self.room_apps.get(&room).cloned() else {
                continue;
            };
            let Some(schema) = self.parsed_schema(&app) else {
                continue;
            };
            // Copy the matching triggers out, releasing the schema borrow before
            // mutating the hub.
            let triggers: Vec<(String, Option<u64>)> = schema
                .auto_version()
                .iter()
                .filter(|av| matches!(av.trigger, Trigger::On(e) if e == event))
                .map(|av| (av.name.clone(), av.keep))
                .collect();
            for (template, keep) in triggers {
                let origin = trigger_origin(event, &template);
                let name = expand_name(&template, now, event);
                self.capture_version(&room, &name, &origin, keep);
            }
        }
        self.auto_version.set_draining(false);
    }

    /// Capture one trigger's version under the already-expanded `name` and its
    /// stable `origin`, then hold the `keep` retention window. Best-effort: a room
    /// with no state yet or a name already taken this tick is a silent no-op
    /// (`Ok(false)`); a durable-store persist failure is logged (a snapshot the
    /// operator wanted was not captured) but never aborts the caller — auto-
    /// versioning is a passive server-side observer.
    ///
    /// `origin` tags the version so retention prunes only this trigger's own
    /// captures — never a manual version or a different trigger's — ordered by the
    /// hub's monotonic capture ordinal, not the wall-clock name. `keep: 0` retains
    /// nothing, so the capture is skipped; `keep: none` retains all (no pruning). A
    /// lowered `keep` takes effect on the trigger's next capture.
    fn capture_version(&mut self, room: &[u8], name: &str, origin: &[u8], keep: Option<u64>) {
        // `keep: 0` retains nothing, so skip the capture — but still prune, so a
        // trigger whose window was lowered to 0 clears its earlier captures.
        if keep != Some(0) {
            match self.hub.create_auto_version(room, name.as_bytes(), origin) {
                // A no-op (`Ok(false)`: empty room / name taken this tick) still
                // falls through to retention, so a lowered `keep` applies and a
                // colliding name never leaves the group over its window.
                Ok(_) => {}
                // A capture fails only on a store write error; retention writes the
                // same store and would fail identically, so skip it and log once.
                Err(e) => {
                    eprintln!(
                        "crdtsync: auto-version capture of {name:?} in room {room:?} failed: {e}"
                    );
                    return;
                }
            }
        }
        // `keep: none` retains all — no pruning. Otherwise hold the window (`0`
        // prunes the whole group).
        if let Some(keep) = keep {
            if let Err(e) = self.hub.retain_by_origin(room, origin, keep) {
                eprintln!("crdtsync: auto-version retention in room {room:?} failed: {e}");
            }
        }
    }

    /// Take and clear the messages queued to send a connection.
    pub fn take_outbox(&mut self, id: ConnId) -> Vec<Message> {
        self.conns
            .get_mut(&id)
            .map(|c| std::mem::take(&mut c.outbox))
            .unwrap_or_default()
    }

    /// The shared hub, for reading merged room state.
    pub fn hub(&self) -> &Hub {
        &self.hub
    }

    /// The shared hub, mutably — the seam an engine operation (forking a branch,
    /// importing a room) drives that has no client-facing wire message.
    pub fn hub_mut(&mut self) -> &mut Hub {
        &mut self.hub
    }

    /// Restore `room` to named version `version` as a fresh branch `new_branch`,
    /// switching the active HEAD to it — the registry entry point for
    /// [`Hub::restore_as_branch`], which additionally drives the auto-version
    /// drain so an `after-restore` trigger the room's schema declares captures the
    /// restored state. Returns whether the restore took (`false` for an unknown
    /// version or an already-taken branch name).
    pub fn restore_as_branch(
        &mut self,
        room: &[u8],
        version: &[u8],
        new_branch: &[u8],
    ) -> io::Result<bool> {
        let restored = self.hub.restore_as_branch(room, version, new_branch)?;
        // The restore's `AfterRestore` was recorded by the auto-version sink; act on
        // it now, as a delivery's post-step drain does, so an `after-restore`
        // trigger fires.
        self.drain_auto_versions();
        Ok(restored)
    }

    /// Publish the active editor branch's state onto the read-only `published`
    /// branch — the registry entry point for [`Hub::publish`], which additionally
    /// drives the auto-version drain so an `on: before-publish` trigger the room's
    /// schema declares captures at the publish point. Returns whether the publish
    /// took (`false` for an empty/unknown room or a `published` naming the editor
    /// branch).
    pub fn publish(&mut self, room: &[u8], published: &[u8]) -> io::Result<bool> {
        let did = self.hub.publish(room, published)?;
        // The publish's `BeforePublish` was recorded by the auto-version sink; act on
        // it now, as a delivery's post-step drain does, so an `before-publish`
        // trigger fires.
        self.drain_auto_versions();
        Ok(did)
    }
}

/// The app to govern a room among those present, chosen deterministically: the
/// lexicographically-smallest app id — a room is normally served by a single
/// app, so this only needs to be stable — at its highest present version.
fn pick_app(apps: &HashMap<Vec<u8>, u32>) -> Option<(Vec<u8>, u32)> {
    apps.iter()
        .min_by(|a, b| a.0.cmp(b.0))
        .map(|(app, version)| (app.clone(), *version))
}

/// Whether a peer connection may currently read `room` — the per-recipient gate
/// on every fan-out. An unauthenticated connection holds no room subscription,
/// so it never qualifies.
fn peer_may_read(
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    session: &Session,
    room: &[u8],
) -> bool {
    match session.identity() {
        // The per-recipient fan-out gate does not yet consult the doc-ACL tier
        // (outbound redaction over doc-ACL reads is a later sub-slice); it abstains,
        // so the deployment and schema tiers decide as before.
        Some(identity) => authorized(
            authorizer,
            Decision::Abstain,
            schema,
            identity,
            Action::Read,
            &Resource::Room(room),
        ),
        None => false,
    }
}
