//! The many-connection fan-out over one hub.
//!
//! A [`Registry`] holds every live connection, each with its own session and
//! an outbox of messages awaiting send. [`Registry::deliver`] drives one
//! connection's session, queues its replies, and fans a broadcast out to the
//! room's other subscribed channels. Pure, synchronous routing; the async
//! transport pumps bytes through it.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crdtsync_core::schema::Trigger;
use crdtsync_core::{
    Channel, ClientId, ElementId, ErrorCode, MemberState, Message, Op, OpKind, Schema,
};
use subtle::ConstantTimeEq;

use crate::acl::authorized;
use crate::auth::{AllowAll, Identity, Verifier};
use crate::authz::{Action, Authorizer, Decision, PermitAll, Resource};
use crate::auto_version::{
    expand_name, expand_schedule_name, schedule_origin, trigger_origin, AutoVersionSink,
    AutoVersionState,
};
use crate::clock::{Clock, SystemClock};
use crate::gossip::GossipRoundOutcome;
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

/// Where a committed batch entered this node, and with it the replica the
/// fan-out does not send it back to.
///
/// A [`Local`](WriteOrigin::Local) write excludes per *channel*, not per
/// connection. A connection multiplexes several subscriptions and two of them
/// may name one room; each holds its own replica under its own
/// [`ClientId::for_channel`] author, so a sibling channel is as distinct a
/// recipient as a peer connection is — it converges, and its seen sequence
/// advances, only if the write actually reaches it. Only the authoring channel,
/// whose replica already folded the ops locally, is skipped. Channel handles are
/// numbered per connection, so that exclusion binds to `conn`: a peer that
/// happens to have opened the same handle is untouched.
///
/// A [`Replicated`](WriteOrigin::Replicated) batch arrived from the room's
/// leader, so **no** local replica already holds it and the exclusion set is
/// empty: every channel subscribed to the stream is a recipient. It also has no
/// local author, and so no author's declared schema version to translate a
/// migration from — the relay seam carries the leader's ops untagged, exactly as
/// the follower logs them and exactly as its own catch-up replays them.
#[derive(Clone, Copy)]
enum WriteOrigin {
    /// Authored on this node by `channel` of `conn`, whose replica folded it
    /// locally before it was submitted.
    Local { conn: ConnId, channel: Channel },
    /// Ingested from the room's leader on the replication plane.
    Replicated,
}

impl WriteOrigin {
    /// The channels of `peer`'s subscription to `(room, branch)` that receive
    /// this write — every one it bound to the stream, minus the authoring
    /// channel when `peer` is the authoring connection.
    fn recipients(
        &self,
        peer: ConnId,
        session: &Session,
        room: &[u8],
        branch: &[u8],
    ) -> Vec<Channel> {
        let mut channels = session.channels_for_stream(room, branch);
        if let WriteOrigin::Local { conn, channel } = self {
            if peer == *conn {
                channels.retain(|c| c != channel);
            }
        }
        channels
    }

    /// The connection that authored this batch on this node, if any — the
    /// declared-app authority a migration's source version is read against. A
    /// replicated batch has none.
    fn author(&self) -> Option<ConnId> {
        match self {
            WriteOrigin::Local { conn, .. } => Some(*conn),
            WriteOrigin::Replicated => None,
        }
    }
}

/// One connection: its protocol session and the messages queued to send it.
struct Conn {
    session: Session,
    outbox: Vec<Message>,
    /// The cluster member this connection speaks as, once it has presented the
    /// cluster secret in a [`Message::PeerAuth`] — the peer plane's admission and
    /// its identity in one. `None` for every connection until then, so an ordinary
    /// client — and any socket that has said nothing at all — reaches none of the
    /// node-to-node handlers. Every gate downstream of admission reads *this*, never
    /// a node id a frame asserts.
    peer: Option<NodeId>,
    /// The hosts the verified mTLS client certificate this connection presented names
    /// — its `dNSName` and `iPAddress` SANs, and nothing else it carries. `None` is a
    /// connection that presented no certificate at all: a plaintext or
    /// server-auth-only link, or one authenticated by an in-band credential.
    /// `Some(hosts)` is a certificate the listener's trust anchors verified, and those
    /// hosts are the only names [`authenticate_peer`](Registry::authenticate_peer)
    /// will bind a member to — `Some(vec![])` therefore binds nothing at all, which is
    /// not the same as having presented nothing.
    cert_hosts: Option<Vec<Vec<u8>>>,
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
    /// registry link is immutable once locked, so a version that resolves is cached
    /// for the process lifetime and never re-parsed. A version that does **not**
    /// resolve is *absent* from the map rather than held as a negative entry — the
    /// control plane can register it at any moment ([`Registry::parsed_schema`]) —
    /// which is why the value is an `Arc<Schema>` and not an `Option`: the type is
    /// what keeps a negative entry unrepresentable.
    schema_cache: HashMap<(Vec<u8>, u32), Arc<Schema>>,
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
    /// The deployment's cluster secret — what a node presents to open the peer
    /// plane on a connection ([`Message::PeerAuth`]). `None` (the default) admits
    /// no peer at all, so every node-to-node frame is refused as the client-plane
    /// protocol violation it is; a clustered deployment configures one or it does
    /// not replicate.
    cluster_secret: Option<Vec<u8>>,
    /// Leader-to-follower replication state: frames queued for each follower and
    /// the acknowledged per-`(room, follower)` watermark. Inert in single-node
    /// mode — a node with no membership never leads a room, so it never
    /// replicates.
    replication: Replication,
    /// Per-room leadership epochs — the split-brain fence (see
    /// [`LeadershipEpochs`]). Empty (inert) in single-node mode and until a room's
    /// leadership first changes.
    epochs: LeadershipEpochs,
    /// Refuse the peer plane to a link that carries no verified certificate — how a
    /// deployment declares that every member is identified, not merely
    /// secret-holding. `false` (the default) admits an uncertified link at the
    /// identity it claims, which separates one member's link from another's but
    /// vouches for neither.
    require_peer_identity: bool,
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

/// The addressing a node-to-node replication frame carries, apart from its payload:
/// the member whose link it arrived on, the room and stream it names, and the
/// leadership epoch it is stamped with. This is exactly what
/// [`gate_replica_frame`](Registry::gate_replica_frame) decides on, so `Replicate`
/// and `ReplicateSnapshot` hand it over whole.
struct ReplicaFrame<'a> {
    sender: &'a NodeId,
    room: RoomId,
    branch: Vec<u8>,
    epoch: u64,
}

/// A dial's catch-up frames, ordered by what a dial emits more of. A peer's outbound
/// channel is bounded and drops on overflow (`dispatch_replication`), so the order
/// frames are queued in decides which are lost when a dial spans more rooms than the
/// channel holds.
///
/// Neither kind is lost *permanently*: a dropped frame advances no watermark, so the
/// next dial recomputes the same delta and re-sends the same root. What differs is
/// **how many of each a dial builds**. A delta (an ops tail or a state transfer) is
/// built only for a room the follower is actually behind on; a root-only
/// [`Message::ReplicateMeta`] is built for every rooted room this node leads,
/// converged or not. Unordered, the repairs would crowd the deltas out of the channel
/// by sheer count and delay the convergence a whole dial cycle for no gain. So deltas
/// are queued first and roots last, and an overflowing dial sheds the frames whose
/// loss costs nothing this cycle.
#[derive(Default)]
struct DialFrames {
    deltas: Vec<Message>,
    roots: Vec<Message>,
}

impl DialFrames {
    fn push(&mut self, frame: Option<Message>) {
        match frame {
            Some(frame @ Message::ReplicateMeta { .. }) => self.roots.push(frame),
            Some(frame) => self.deltas.push(frame),
            None => {}
        }
    }

    fn enqueue_to(self, replication: &mut Replication, follower: &NodeId) {
        for frame in self.deltas.into_iter().chain(self.roots) {
            replication.enqueue(follower.clone(), frame);
        }
    }
}

/// A room's server-side metadata record, as the two replication frames carry it and as
/// the store writes it: the doc-ACL authority root, the governing `{app_id, version}`
/// binding, and the op-version high-water. None of the three rides the ops or the
/// snapshot bytes, and each decides how what does ride is read — the root is the
/// authority the doc-ACL denies resolve against, and the binding and the high-water
/// together are what the handshake range-check refuses an under-versioned joiner on. So
/// a replica that took only the payload holds a room it cannot decide anything about,
/// and it serves the client writes that would establish any of them only once a failover
/// has made it a leader.
struct ReplicatedMeta {
    creator: Option<Vec<u8>>,
    governing: Option<(Vec<u8>, u32)>,
    max_op_version: Option<u32>,
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
            cluster_secret: None,
            require_peer_identity: false,
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

    /// The membership view, mutably — the seam a test drives to put a node into a
    /// state the network would take minutes to produce, such as having reaped every
    /// configured peer.
    #[doc(hidden)]
    pub fn membership_mut_for_test(&mut self) -> &mut Membership {
        self.membership.as_mut().expect("clustered")
    }

    /// Hold the deployment's cluster secret — the credential a peer presents to
    /// open the peer plane on its connection. Unset (the default) leaves the peer
    /// plane closed to every connection: a node-to-node frame is then refused as
    /// the client-plane protocol violation it is, so a clustered node without a
    /// secret does not replicate rather than replicating for anyone. An empty
    /// secret configures none for the same reason — it would be matched by a
    /// `PeerAuth` carrying no secret at all — so passing one disarms a plane
    /// already armed. This is the mechanism; how long a secret must be, and that a
    /// clustered node must have one at all, is [`ServeConfig`](crate::runtime::ServeConfig)'s
    /// policy, enforced before a deployment starts.
    pub fn set_cluster_secret(&mut self, secret: Vec<u8>) {
        self.cluster_secret = (!secret.is_empty()).then_some(secret);
    }

    /// Refuse the peer plane to any link that presents no verified client
    /// certificate naming a member — the deployment's declaration that peer identity
    /// is established rather than claimed. Off by default, which admits an
    /// uncertified link at the node id it asserts. This is the mechanism; that a
    /// deployment turning it on must also terminate client-certificate verification
    /// and issue this node an identity of its own is
    /// [`ServeConfig`](crate::runtime::ServeConfig)'s policy, enforced before it
    /// starts.
    pub fn set_require_peer_identity(&mut self, require: bool) {
        self.require_peer_identity = require;
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

    /// The nodes this registry originates `room`'s replication to — its replica set
    /// minus self — and empty where it originates none: a node with no membership
    /// leads nothing (single-node), and a node that does not *effectively* lead the
    /// room defers to whoever does. Origination follows effective (live) leadership,
    /// so a node promoted over a down placement primary originates for its newly-led
    /// rooms and a demoted-but-recovered old primary waits until it leads again. The
    /// set is the same replica-set-minus-self the majority gate counts, so the fan-out
    /// and the quorum never disagree on who is a follower — and it is the one gate
    /// every origination seam reads, so an ops frame and a metadata-only one are
    /// addressed alike.
    fn replication_followers(&self, room: &[u8]) -> Vec<NodeId> {
        let Some(membership) = &self.membership else {
            return Vec::new();
        };
        if !membership.is_effective_primary_for(room) {
            return Vec::new();
        }
        self.quorum(room).1
    }

    /// Queue a [`Message::ReplicateMeta`] for each follower of `room` when this node
    /// leads it — the carrier for a doc-ACL authority root that has no op batch to
    /// ride out on. A write the room's dedup swallowed whole establishes a root and
    /// broadcasts nothing, so the frame that would have carried it is never built; a
    /// replica left creatorless that way serves every doc-ACL deny in the room as
    /// inert until the room's next *fresh* commit.
    ///
    /// A no-op for a room with no root to assert, so a peer is never sent a frame
    /// saying nothing.
    fn enqueue_root_replication(&mut self, room: &[u8]) {
        let Some(creator) = self.hub.room_creator(room) else {
            return;
        };
        let followers = self.replication_followers(room);
        if followers.is_empty() {
            return;
        }
        let epoch = self.claim_and_persist_epoch(room);
        for follower in followers {
            self.replication.enqueue(
                follower,
                Message::ReplicateMeta {
                    room: room.to_vec(),
                    epoch,
                    creator: Some(creator.clone()),
                },
            );
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
        let followers = self.replication_followers(room);
        if followers.is_empty() {
            return;
        }
        let base_seq = self.hub.base_seq(room);
        // The room's metadata record rides the frame — its doc-ACL authority root, its
        // governing binding and its op-version high-water — so a follower holds what
        // decides how the replicated log is read rather than only the log. Read per
        // fan-out: the write that establishes a fresh room establishes each of them, and
        // that same write is the frame that carries them.
        let meta = self.replicated_room_meta(room);
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
                    creator: meta.creator.clone(),
                    governing: meta.governing.clone(),
                    max_op_version: meta.max_op_version,
                },
            );
        }
    }

    /// The room's metadata record as this node holds it — the three fields the two
    /// replication frames carry beside the log or the state.
    ///
    /// The binding is [resolved](Registry::governing_binding) the way the write that
    /// raised the high-water resolved it, so the pair a frame carries is the pair the
    /// leader itself decided under. Reading either source alone strands the number: the
    /// receiver adopts a high-water only beside the app it was measured in, so a record
    /// naming none is dropped there and the room's range-check stays inert.
    fn replicated_room_meta(&self, room: &[u8]) -> ReplicatedMeta {
        ReplicatedMeta {
            creator: self.hub.room_creator(room),
            governing: self.governing_binding(room),
            max_op_version: self.hub.max_op_version(room),
        }
    }

    /// Adopt a frame's whole metadata record onto this node's replica of `room`, each
    /// field on its own composition rule: the root set-once, the binding on the
    /// incumbent-app rule, and the high-water as a max — but only under the binding that
    /// won that composition, since the number is meaningless in any other app's version
    /// space. A frame is an *assertion*, so every one of them composes against what
    /// stands rather than replacing it — a peer cannot re-root a room, talk a replica's
    /// high-water down into admitting a joiner its state defeats, re-govern it under
    /// another app, or slip a number in under an app that is not the one answering.
    fn adopt_replicated_meta(&mut self, room: &[u8], meta: &ReplicatedMeta) {
        // The binding first: it decides whose version space the high-water is read in,
        // and the two writes after it each persist the whole record, so the last one to
        // land is a complete one.
        self.adopt_replicated_binding(room, meta.governing.as_ref());
        if let Some(creator) = &meta.creator {
            self.hub.ensure_creator(room, creator);
        }
        // The high-water only alongside the binding that gives it meaning, and only when
        // that binding is the one this room is read under. It is a number in some app's
        // version space: named without one it says nothing, and named under an app that
        // lost the incumbent-wins composition it says something about a chain this room
        // is not read by — either way the range-check would be deciding against a
        // version it cannot interpret. An honest leader sends neither pair: a write
        // raises the high-water only through a version the room's own binding resolved,
        // so on a leader a high-water implies a binding and the two agree at the source.
        // The one shape that breaks that is a leader whose metadata write was lost while
        // its log kept the versions its ops were tagged with (C55), and a replica is
        // better off not adopting a number that arrives with nothing to read it in.
        let incumbent = self.governing_binding(room).map(|(app, _)| app);
        if let (Some(incumbent), Some((app, _))) = (incumbent, meta.governing.as_ref()) {
            if incumbent == *app {
                self.hub.raise_max_op_version(room, meta.max_op_version);
            }
        }
    }

    /// The room's governing binding, resolved exactly as a write's own version tag
    /// resolves it: the live presence map, falling back to the room's own binding on the
    /// hub. The one resolution for every seam that reads which app answers for a room —
    /// what a replication frame carries, what a replicated high-water is judged under,
    /// and what an eviction decides against — because a high-water is only meaningful
    /// beside the binding the write that raised it was tagged from.
    ///
    /// Neither source alone is sufficient, and each covers the other's gap. A
    /// dormant-room sweep drops the *presence map's* entry for a room nobody subscribes,
    /// so the hub carries a template room's binding. The same sweep drops the *hub's*
    /// for a room the hub does not hold — a subscribe binds before the first write
    /// materializes anything — while the map keeps it, so a room bound and then written
    /// has its binding only there until its next subscribe. Reading the hub alone in
    /// that state emits a high-water with no app beside it, which every replica then
    /// discards.
    fn governing_binding(&self, room: &[u8]) -> Option<(Vec<u8>, u32)> {
        self.room_apps
            .get(room)
            .cloned()
            .or_else(|| self.hub.governing_app(room))
    }

    /// Evict every subscriber of `room` a just-applied replicated frame stranded — one
    /// admitted under the high-water `pre` that the frame opened past. A leader runs this
    /// on its own writes; a replica has to run it on the frames that are its writes, or a
    /// follower-served subscriber outlives a lift that a leader-served one does not. The
    /// two fan-outs differ in what the survivor is then handed, which is why the replica
    /// runs this *before* its own: a leader's fan-out translates per recipient and drops
    /// a batch whose chain will not resolve, so its stranded peer is told to update and
    /// given nothing, while a replicated batch is served verbatim and would reach that
    /// peer at a version it cannot model.
    /// The lift test assumes the admission it is re-checking was taken under the same
    /// binding, which is what makes it a short-circuit rather than a rule: `evict_stranded`
    /// re-runs the predicate the subscribe gate admitted on, so an unchanged high-water
    /// reproduces each subscriber's own answer. The assumption holds wherever a room's
    /// high-water and its binding travel together: the two replication apply seams, the
    /// write path that re-asserts what it tagged from, and the clone. It is not a
    /// property of every state a `Hub` can be put in — `ingest` takes a version and binds
    /// nothing. Where a room holds one without the other the gate *abstains* at
    /// admission, so a peer can be let in against a number it cannot reach and then bind
    /// the room itself, and this never re-checks it — C129, which predates this seam.
    ///
    /// The binding is resolved rather than assumed. An unbound room does reach here — a
    /// relay write carries no version, so it lands on one — and returns at the lift test
    /// above, because an untagged write raises no high-water for it to have opened past.
    /// So the `if let` answers a room that is genuinely unbound rather than a case the
    /// callers cannot produce, and is what makes this total rather than a panic.
    fn evict_stranded_by_lift(&mut self, room: &[u8], pre: Option<u32>) {
        let post = self.hub.max_op_version(room);
        if post <= pre {
            return;
        }
        if let Some((app, version)) = self.governing_binding(room) {
            self.evict_stranded(room, (&app, version), post);
        }
    }

    /// Adopt a frame's governing binding, routed through
    /// [`bind_room_app`](Registry::bind_room_app) so a peer's assertion composes on the
    /// incumbent-app rule a client subscribe and the durable load already take: the
    /// incumbent keeps the room, and only a same-app frame lifts the version.
    fn adopt_replicated_binding(&mut self, room: &[u8], governing: Option<&(Vec<u8>, u32)>) {
        if let Some((app, version)) = governing {
            self.bind_room_app(room.to_vec(), app.clone(), *version);
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
        let mut dial = DialFrames::default();
        for room in self.rooms_led_for(follower) {
            let floor = self.replication.watermark(&room, follower);
            dial.push(self.catch_up_room_frame(&room, floor));
        }
        dial.enqueue_to(&mut self.replication, follower);
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
        let mut dial = DialFrames::default();
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
            dial.push(self.catch_up_room_frame(&room, floor));
        }
        dial.enqueue_to(&mut self.replication, follower);
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

    /// The catch-up frame that lands a follower at `room`'s head from `floor`. The
    /// leader branches by comparing `floor` to the room's compaction floor, which
    /// `catch_up` folds into its reply: at or above the floor it yields the ops past
    /// `floor` (an ordinary delta), below it — the ops the follower needs are
    /// compacted away — it yields the whole-replica snapshot at the head. So a
    /// follower below the floor is caught up by a state-transfer rather than a futile
    /// ops-replay that would leave it divergent; one at or above it keeps the ops-tail
    /// path. The frame is stamped with this node's leadership epoch, fenced exactly as
    /// a steady replication frame.
    ///
    /// A follower already at the head is sent no delta — an empty `Replicate` would
    /// create the room on a node that does not hold it, leaving an empty replica its
    /// read-serving then advertises. It is sent the room's root on its own instead
    /// ([`Message::ReplicateMeta`], which creates nothing), because a caught-up
    /// follower is exactly the one that cannot recover a root it lost: persisting the
    /// root is best-effort and set-once retries nothing, so a replica whose metadata
    /// write failed reloads creatorless and, serving no client write, waits for a
    /// commit a quiescent room never makes. `None` only when there is neither a delta
    /// nor a root to send.
    fn catch_up_room_frame(&mut self, room: &[u8], floor: u64) -> Option<Message> {
        // The room's metadata record, carried by a dialed catch-up exactly as by a
        // steady commit — a follower converged either way holds the authority its
        // replicated ACL tuples are decided under, and the binding and high-water its
        // handshake range-check runs on.
        let meta = self.replicated_room_meta(room);
        match self.hub.catch_up(room, floor) {
            Catchup::Ops(records) => {
                let ops: Vec<Op> = records.into_iter().map(|rec| rec.op).collect();
                if ops.is_empty() {
                    // Nothing to catch up but the room's root, and only where there
                    // is one — a rootless room has nothing this frame could assert.
                    // Nor where the room has reached no sequence at all: a follower
                    // holds no such room and this frame does not create one, so the
                    // root would be re-sent inertly on every dial forever. A room
                    // with no ops also has no ACL tuples for a root to decide, so
                    // there is nothing to repair. It is reachable — an authenticated
                    // `Ops` frame carrying no ops roots a zero-op room (C99) — which
                    // is why it is a case rather than an impossibility.
                    if meta.creator.is_none() || self.hub.seq(room) == 0 {
                        return None;
                    }
                    // Only the root rides this frame today; the room's binding and
                    // op-version high-water are repaired by the next delta instead
                    // (C125), so a follower that lost its whole metadata record
                    // recovers the root here and the rest on the room's next commit.
                    return Some(Message::ReplicateMeta {
                        room: room.to_vec(),
                        epoch: self.claim_and_persist_epoch(room),
                        creator: meta.creator,
                    });
                }
                let base_seq = self.hub.base_seq(room);
                Some(Message::Replicate {
                    room: room.to_vec(),
                    branch: MAIN_BRANCH.to_vec(),
                    ops,
                    base_seq,
                    epoch: self.claim_and_persist_epoch(room),
                    creator: meta.creator.clone(),
                    governing: meta.governing.clone(),
                    max_op_version: meta.max_op_version,
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
                creator: meta.creator,
                governing: meta.governing,
                max_op_version: meta.max_op_version,
            }),
            // `catch_up` reads main's log, never a branch base, so it has no
            // unservable answer. Dialling nothing is the fail-closed reading anyway:
            // a follower stays behind rather than being handed a frame built over a
            // stream this node could not read.
            Catchup::Unavailable => None,
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

    /// Admit connection `id` to the peer plane as the member `claimed`, if
    /// `presented` is this deployment's cluster secret. The secret is what separates
    /// a member from anyone else who can reach the port; `claimed` is what separates
    /// one member from another, and every gate downstream of this reads the bound
    /// identity rather than a node id a later frame asserts.
    ///
    /// The claim is only as strong as what establishes it. On a link carrying a
    /// verified mTLS client certificate, one of the *hosts* that certificate names must
    /// bind the claim ([`cert_names_member`]) — the trust anchors the listener verifies
    /// client certificates against vouch for the binding, so no member can speak as
    /// another. A certificate that names an actor but no *host* binds nothing and the
    /// link is refused: a presented certificate decides, and one that named nothing
    /// relevant must never widen what a link may claim. On an uncertified link the
    /// claim is taken at face value, which still binds the link to one identity but
    /// vouches for none; a deployment that will not have that sets
    /// [`set_require_peer_identity`](Self::set_require_peer_identity) and every
    /// uncertified link is refused.
    ///
    /// Fail-closed and silent. A node with no configured secret has no peer plane to
    /// open, so it refuses; so does any mismatch, an unnamed claim, a claim its
    /// certificate contradicts, an uncertified link under a deployment that requires
    /// identity, and a second `PeerAuth` on a link already admitted — an identity is
    /// bound once and holds for the connection, so a re-bind is a link speaking as two
    /// members. Either way the connection is dropped with no reply — which is what
    /// hides whether a secret is configured at all, since the unconfigured case
    /// returns before comparing anything. Where a secret *is* configured the
    /// comparison is constant-time over the content, so a rejection leaks no prefix
    /// of it. Returns whether the connection stays open.
    fn authenticate_peer(&mut self, id: ConnId, claimed: &[u8], presented: &[u8]) -> bool {
        let Some(expected) = &self.cluster_secret else {
            return false;
        };
        if !bool::from(expected.as_slice().ct_eq(presented)) {
            return false;
        }
        // A link that names no member has no identity to gate on, so there is nothing
        // to admit it as — and it must name it in the one spelling the roster and the
        // ring use, or the identity every gate decides against would be a member of
        // neither.
        let Some(member) = NodeId::canonical(claimed) else {
            return false;
        };
        let Some(conn) = self.conns.get_mut(&id) else {
            return false;
        };
        if conn.peer.is_some() {
            return false;
        }
        match conn.cert_hosts.as_deref() {
            // No certificate at all. The claim stands on its own, which is what a
            // deployment that has not issued per-node certificates has; one that has
            // says so and this is refused.
            None if self.require_peer_identity => return false,
            None => {}
            // A certificate *was* presented and verified, so it decides — including
            // when it names no host, which binds nothing. Treating that as certless
            // would make a certificate widen what a link may claim.
            Some(hosts)
                if !hosts
                    .iter()
                    .any(|host| crate::dial::cert_names_member(host, claimed)) =>
            {
                return false
            }
            Some(_) => {}
        }
        conn.peer = Some(member);
        // Admitting a link is **not** a verification of the member it names, however
        // well the certificate names it. A member chooses when to dial in and how
        // often, so a vouch earned that way is one the member caused rather than one
        // this node independently made — and a certificate names a *host*, which mints
        // as many node ids as it likes, so a member could dial in under each ground id
        // in turn and have this node vouch for every one of them. Verification is this
        // node's own dial and nothing else (see
        // [`note_peer_verified`](Self::note_peer_verified)).
        true
    }

    /// The cluster member connection `id` was admitted as, or `None` when it has
    /// presented no cluster secret — the gate every node-to-node frame passes before
    /// it reaches a peer handler, and the identity each of them is decided against.
    fn peer_identity(&self, id: ConnId) -> Option<NodeId> {
        self.conns.get(&id).and_then(|conn| conn.peer.clone())
    }

    /// The shared membership + leadership-epoch fence for a node-to-node replication
    /// frame (`Replicate` and `ReplicateSnapshot`) for `room` on `branch` stamped
    /// `epoch`, arriving from the member `sender` its link was admitted as. A frame is
    /// applied only while this node merely *follows* `room`: it must hold the room
    /// (placement) and not itself lead it, unless a strictly higher `epoch` supersedes
    /// that leadership (the recovered-stale-leader reconciliation), and it must name
    /// the `main` stream (a leader replicates only `main`). A frame below the highest
    /// epoch this node has seen is fenced — it comes from a demoted leader that missed
    /// the promotion, and applying it would resurrect its writes.
    ///
    /// **`sender` must itself hold `room`.** Placement says which members may ever
    /// lead a room, and a member outside its replica set can hold no copy of it and so
    /// can never be its leader — so a frame from one is applied under no epoch at all.
    /// Without that check every admitted member could push ops into any room this node
    /// replicates and supersede its leadership of any room at will. Inside the replica
    /// set the epoch is still the only arbiter: a genuinely promoted replica must be
    /// able to supersede a stale leader, and nothing here distinguishes it from a peer
    /// replica forging the bump — that needs a real election, not a stronger identity.
    ///
    /// The rejection **drops the link** rather than fencing the frame, and that is the
    /// repair path rather than a cost. Placement is a pure function of the member set,
    /// so two nodes disagree about who replicates a room for the propagation window of
    /// every join and every reap, and a legitimate leader is transiently outside a
    /// room's replica set as this node sees it. Its frames must not be *silently*
    /// discarded: the steady path mirrors only fresh commits, so the ops fenced during
    /// that window would never be re-sent and the follower would carry a permanent gap
    /// that later frames stack on top of. Dropping the link makes the leader redial,
    /// which re-runs the late-joiner catch-up from the follower's watermark and closes
    /// the window's gap. The cost is a few redials while gossip converges; the
    /// alternative is silent divergence.
    ///
    /// On [`Apply`](ReplicaGate::Apply) the fence is advanced (stepping down if
    /// superseded) and persisted, so a restart reloads it and a later lower-epoch frame
    /// is fenced; the step-down is deferred to here so a rejected frame never churns
    /// this node's leadership epoch.
    fn gate_replica_frame(&mut self, frame: &ReplicaFrame<'_>) -> ReplicaGate {
        let ReplicaFrame {
            sender,
            room,
            branch,
            epoch,
        } = frame;
        let (room, branch, epoch) = (room.as_slice(), branch.as_slice(), *epoch);
        let Some(membership) = &self.membership else {
            return ReplicaGate::Reject;
        };
        // A link claiming to be this node speaks for a leader that is not on the other
        // end of it; one claiming a node this view has never learned speaks for no
        // leader at all; and a member that does not replicate the room could never be
        // its leader.
        if membership.is_self(sender) || !membership.replicas_for(room).contains(sender) {
            return ReplicaGate::Reject;
        }
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
    ///
    /// `meta` is the room's metadata record as the leader holds it — its doc-ACL
    /// authority root, its governing binding, and its op-version high-water — adopted
    /// beside the ops, each on its own composition rule. The frame is where all three
    /// come from: a replica that took only the ops would hold the room's ACL tuples and
    /// not the authority they are decided under, and would hold its versioned state and
    /// not the high-water its handshake refuses an under-versioned joiner against. It
    /// serves the client writes that establish any of them only once a failover has made
    /// it a leader — by which time the tuples are long folded in and the state is long
    /// past the joiner's reach.
    fn apply_replicate(
        &mut self,
        id: ConnId,
        frame: ReplicaFrame<'_>,
        ops: Vec<Op>,
        meta: ReplicatedMeta,
    ) -> bool {
        match self.gate_replica_frame(&frame) {
            ReplicaGate::Reject => return false,
            ReplicaGate::Fenced => return true,
            ReplicaGate::Apply => {}
        }
        let (room, branch) = (frame.room, frame.branch);
        // The high-water before the batch, so the lift the frame delivers is the pre/post
        // delta — the same capture the client write path takes, for the same eviction.
        let pre_high_water = self.hub.max_op_version(&room);
        // Ingest through the same path a client `Ops` write uses. A replicated write
        // carries no schema version — the leader logs its writers' ops untagged on the
        // relay seam, and the follower mirrors them verbatim. What the room's ops were
        // authored at rides the frame as the room-level high-water instead, which is
        // what the range-check reads. The per-op source version is C71's.
        let Ok(applied) = self.hub.ingest(&room, ops, None) else {
            return false;
        };
        // After the ingest: the room must exist for the record to land on it, and the
        // frame that first creates it is the one that names it.
        self.adopt_replicated_meta(&room, &meta);
        // Serve this node's own subscribers. A follower is an ordinary read-serving
        // node, so a client subscribed here is subscribed to a stream the leader is
        // the sole author of, and its replica advances on exactly what this seam
        // delivers. Fanned out after the creator lands, since the redaction the
        // fan-out applies is decided against the room's authority root, and onto the
        // stream the frame itself names, so the recipient set is the frame's rather
        // than a restatement of which streams the gate admits.
        //
        // The exclusion set is empty: the batch was authored on the leader, so no
        // channel here already holds it. Everything else the fan-out decides —
        // per-recipient reads, zone scoping, migration translation — is re-decided
        // against this replica, never inherited from the leader ([`fan_out_ops`](Registry::fan_out_ops)).
        // Before the fan-out, which is what makes a follower-served subscriber's fate the
        // leader's. A frame that lifted the high-water strands a subscriber this replica
        // admitted under the old one; a leader's stranded peer never receives the batch,
        // because the leader's fan-out translates per recipient and drops one whose
        // chain will not resolve. This fan-out translates nothing — a replicated batch is
        // served verbatim (C71) — so a peer still in the room here would be handed an op
        // above its reach and only then told to update. Evicting first drops it from the
        // room, so the fan-out below no longer names it.
        self.evict_stranded_by_lift(&room, pre_high_water);
        if !applied.is_empty() {
            self.fan_out_ops(WriteOrigin::Replicated, &room, &branch, &applied, None);
        }
        let through_seq = self.hub.seq(&room);
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.outbox.push(Message::ReplicaAck { room, through_seq });
        }
        true
    }

    /// Adopt a leader's assertion of `room`'s doc-ACL authority root, with no ops
    /// beneath it — the carrier for a root the ops path cannot deliver. Gated by
    /// [`gate_replica_frame`](Registry::gate_replica_frame) exactly as an ops frame
    /// is, against `main`, since the root is a fact about the room rather than about
    /// any one stream. Returns whether the connection stays open.
    ///
    /// Composed set-once through the same
    /// [`Hub::ensure_creator`](crate::Hub::ensure_creator) the ops path uses, so this
    /// frame is judged by the rules every other arrival seam applies and can never
    /// displace a root already standing here.
    ///
    /// **Inert for a room this node does not hold, and inert before the fence.** A
    /// frame asserting nothing — no room here to assert it about, or no root asserted
    /// — is dropped ahead of [`gate_replica_frame`](Registry::gate_replica_frame),
    /// because the gate *advances and persists* this node's leadership fence for the
    /// room and a fence recorded for a stream this node holds none of is a durable
    /// record with no room behind it: `Store::load` materialises a room from an epoch
    /// record alone, so the next restart would come up holding an **empty** replica at
    /// the head that `holds_room` reports as servable — the very state that
    /// disqualified an empty `Replicate` from being this frame, arriving one restart
    /// later. Every other replication frame reaches the gate because each carries a
    /// stream position the fence exists to order; this one carries none. A node
    /// missing the room gets it from the ops or snapshot catch-up where the room has
    /// reached a sequence, and each of those still refuses a stray sender's link.
    ///
    /// Unacknowledged, and deliberately: the frame names no sequence and advances no
    /// stream, so there is no watermark for a [`Message::ReplicaAck`] to report. It
    /// runs no fan-out either — a root only ever *narrows* what this replica serves,
    /// and there are no ops to deliver under it.
    fn apply_replicate_meta(&mut self, frame: ReplicaFrame<'_>, creator: Option<Vec<u8>>) -> bool {
        let Some(creator) = creator else {
            return true;
        };
        if !self.hub.holds_room(&frame.room) {
            return true;
        }
        match self.gate_replica_frame(&frame) {
            ReplicaGate::Reject => return false,
            ReplicaGate::Fenced => return true,
            ReplicaGate::Apply => {}
        }
        self.hub.ensure_creator(&frame.room, &creator);
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
    ///
    /// `meta` — the room's metadata record, which the state bytes do not carry (see
    /// [`Message::ReplicateSnapshot`]) — is adopted beside it, through the same seam and
    /// on the same composition rules the ops path uses. The root rides the install
    /// itself, since the install replaces the room and has to compose it against what
    /// stood there. This frame is the only way a replica converged purely by state
    /// transfer ever learns any of the three, since it carries no ops at all.
    fn apply_replicate_snapshot(
        &mut self,
        id: ConnId,
        frame: ReplicaFrame<'_>,
        seq: u64,
        state: Vec<u8>,
        meta: ReplicatedMeta,
    ) -> bool {
        match self.gate_replica_frame(&frame) {
            ReplicaGate::Reject => return false,
            ReplicaGate::Fenced => return true,
            ReplicaGate::Apply => {}
        }
        let room = frame.room;
        // The high-water before the install, so the lift the frame delivers is the
        // pre/post delta — the same capture the ops path takes, for the same eviction.
        let pre_high_water = self.hub.max_op_version(&room);
        if self
            .hub
            .install_snapshot(&room, &state, seq, meta.creator.clone())
            .is_err()
        {
            return false;
        }
        // The same adopt the ops seam runs, after the fallible part exactly as there:
        // nothing of the record lands for a room this node failed to install, and the
        // last write to the store is the complete one.
        self.adopt_replicated_meta(&room, &meta);
        // A state transfer lifts the high-water exactly as an ops batch does, so it
        // strands the same subscribers and evicts them on the same rule.
        self.evict_stranded_by_lift(&room, pre_high_water);
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
    pub fn known_liveness(&self) -> Vec<(NodeId, Vec<u8>, u64, MemberState, bool)> {
        self.membership
            .as_ref()
            .map(Membership::known_liveness)
            .unwrap_or_default()
    }

    /// Record that this node has itself completed an identity-checked peer link to
    /// `node` — the promotion signal a room's placement is built from. Driven by the
    /// gossip loop's *direct* round, which dials the member and (on a TLS member)
    /// authenticates it before a byte of the cluster secret is written; an
    /// indirect-only round confirms a relay reached it and vouches for nothing here.
    /// Inert in single-node mode.
    ///
    /// A deployment that *requires* an identified peer requires the dial to have
    /// authenticated one, so a plaintext member is not verified this way: the dial had
    /// no certificate to check, and a round that proves only "something answers here"
    /// would let such a member take rooms in a cluster whose every inbound link from it
    /// is refused — stalling their quorums. Without that policy the same round is the
    /// honest floor, and vouches for reachability alone.
    pub fn note_peer_verified(&mut self, node: &NodeId) {
        if !self.dial_establishes_identity(node) {
            return;
        }
        if let Some(membership) = &mut self.membership {
            membership.note_verified(node);
        }
    }

    /// Merge a gossiped liveness payload into this node's membership — the SWIM
    /// anti-entropy merge that both grows the member set and converges its liveness
    /// toward a cluster-wide view. Inert in single-node mode (no membership).
    ///
    /// The payload is merged as given, so a caller hands over only what it is willing
    /// to have introduced. The *reply* half of a push-pull round hands over everything:
    /// it came from a node this one chose to dial, the set a node dials is its own
    /// member set rooted in static configuration, and growing it from a node already in
    /// it is how a joiner learns the cluster in one round. The *inbound* half is the one
    /// an unknown peer can reach, and it filters first — a peer introduces only itself,
    /// at its own address ([`apply_gossip`](Self::apply_gossip)).
    ///
    /// What the reply half hands over freely is the *roster*, and a member reaching the
    /// roster this way is **pending**: dialed, probed and gossiped about, but on no
    /// room and in no room's quorum until the cluster verifies it. So the freedom costs
    /// what it always should have — a joiner converges in one round — without letting
    /// the node this one dialed choose who takes a place in the ring.
    ///
    /// `sender` is the member the payload came from, and every `verified` flag in it is
    /// recorded as that member's own first-hand claim — but only where `sender`'s
    /// identity was established. This is the *reply* half of a round this node drove,
    /// so what establishes it is the dial: a deployment that requires an identified
    /// peer gets one from a `wss://` member's certificate and nothing at all from a
    /// plaintext one, and an unattributable claim must not become an adopted member's
    /// vouch. The liveness in the payload still merges either way — a member's
    /// reachability is not a claim about anyone's identity.
    pub fn merge_gossip(
        &mut self,
        sender: &NodeId,
        members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState, bool)>,
    ) {
        let attributable = self.dial_establishes_identity(sender);
        self.merge_gossip_attributed(sender, members, attributable);
    }

    /// Merge a gossip payload from `sender`, saying explicitly whether its `verified`
    /// flags may be attributed to it — the seam both halves of a round share, each
    /// deciding attribution from the link it actually holds.
    fn merge_gossip_attributed(
        &mut self,
        sender: &NodeId,
        members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState, bool)>,
        attributable: bool,
    ) {
        if let Some(membership) = &mut self.membership {
            membership.merge_liveness(
                sender,
                members
                    .into_iter()
                    .map(|(node, addr, inc, state, verified)| {
                        (
                            NodeId::from(node),
                            addr,
                            inc,
                            state,
                            verified && attributable,
                        )
                    }),
            );
        }
    }

    /// Whether a dial to `member` establishes who answered — a TLS member's
    /// certificate does, a plaintext member's transport does not, and a deployment
    /// that has not declared identity required takes the dial at face value. The
    /// member's *advertised* transport is the right thing to read here and the wrong
    /// thing to read of an inbound link: an outbound dial runs over exactly the
    /// transport that address declares, while an inbound link is one the member dialed
    /// and its own listener's scheme describes nothing about it.
    fn dial_establishes_identity(&self, member: &NodeId) -> bool {
        !self.require_peer_identity
            || self
                .membership
                .as_ref()
                .is_some_and(|m| m.advertises_tls(member))
    }

    /// Run one reap check over the cluster membership: remove members that have
    /// stayed `Dead` past the bounded dead-time ([`Membership::reap_dead`]), so a
    /// durably-departed node stops lingering as a placement replica. Driven once per
    /// membership sweep. Inert in single-node mode (no membership); `reap_dead` rebuilds
    /// the placement itself, so the next delivery routes over the reaped roster with
    /// nothing to recompute here.
    ///
    /// The reap also carries into the replication bookkeeping: each reaped member's
    /// acknowledged watermarks go with it ([`Replication::forget_members`]), so the map
    /// stays keyed on the roster rather than on departed members, and a member that
    /// returns is caught up from nothing instead of from a position it may no longer
    /// hold. [`record_replica_ack`](Self::record_replica_ack) keeps it that way — a
    /// non-member's late ack records nothing.
    ///
    /// A withheld client ack is deliberately **not** released here. A reap re-places
    /// rooms at the same size while the ring holds the replication factor, so the
    /// majority a withheld write waits on moves only once the ring falls *below* it —
    /// and a release there would hand the author an `Accepted` for a write held by
    /// fewer replicas than it was accepted against, which on the minority side of a
    /// partition is a write the majority side never saw. Which quorum a withheld ack
    /// waits on after the roster shrinks is C69's to settle.
    pub fn reap_dead_members(&mut self) {
        let Some(membership) = &mut self.membership else {
            return;
        };
        let reaped: HashSet<NodeId> = membership.reap_dead().into_iter().collect();
        if reaped.is_empty() {
            return;
        }
        self.replication.forget_members(&reaped);
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

    /// Record how one SWIM probe round reached `node` — the whole of what a round
    /// means to this view, in one place rather than split across the caller.
    ///
    /// Liveness and *identity* are different questions and a round answers them
    /// differently. Every reachable outcome is proof the member is alive, whichever
    /// path found it. Only [`Direct`](GossipRoundOutcome::Direct) is proof about the
    /// member itself: this node dialed the address the id names and the transport
    /// authenticated the far end before a byte was written, which is what a
    /// verification claims. A relay's second opinion says a *relay* reaches the target,
    /// which vouches for nobody here — if it did, one member confirming a target would
    /// place it, and the mint would be open one step over.
    pub fn note_gossip_round(&mut self, node: NodeId, outcome: GossipRoundOutcome) {
        self.note_gossip_probe(node.clone(), outcome.reachable());
        if outcome == GossipRoundOutcome::Direct {
            self.note_peer_verified(&node);
        }
    }

    /// Apply an inbound [`Message::Gossip`] from the member `sender` on peer
    /// connection `id`: merge the advertised liveness into this node's view, then
    /// answer with this node's own so the exchange syncs both directions (push-pull
    /// anti-entropy). Honored only in cluster mode — a Gossip on a single-node
    /// deployment (no membership) is a stray frame and the connection is dropped.
    /// Returns whether the connection stays open.
    ///
    /// **A member reached this way introduces only itself, at its own address.** A
    /// tuple naming a node this view already knows is merged whole (that is SWIM
    /// dissemination — a `Dead` verdict has to travel, and a known member's dial
    /// address is never re-learned anyway), but a tuple naming an *unknown* node is
    /// adopted only when it is the sender's own **and** the dial address it carries is
    /// that same id. Both halves are needed: without the first, any admitted peer
    /// plants an arbitrary member in every node's set; without the second it plants
    /// its own id pointing at an address it chose, which every node then dials and
    /// hands the cluster secret to just the same. A node id *is* its advertise
    /// address, so the constraint costs a legitimate joiner nothing. It holds for this
    /// path only: the reply half ([`merge_gossip`](Self::merge_gossip)) adopts a member
    /// at whatever address its tuple carries, because the node whose reply it is was
    /// chosen from this node's own member set.
    ///
    /// A joining node still converges: it learns the cluster from the seed it *dialed*
    /// (see [`merge_gossip`](Self::merge_gossip), the reply path) and then introduces
    /// itself to each member directly.
    ///
    /// **A self-introduction joins the roster, not the ring.** The member is dialed,
    /// probed and gossiped about, and placed on no room until the cluster has verified
    /// it — otherwise a member would grind a node id that HRW placed on the room it
    /// wanted and be inside that room's replica set the moment it said so. Its own
    /// tuple cannot carry that verification either: a claim naming the sender is
    /// dropped.
    fn apply_gossip(
        &mut self,
        id: ConnId,
        sender: &NodeId,
        members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState, bool)>,
    ) -> bool {
        let Some(membership) = &self.membership else {
            return false;
        };
        // An inbound frame introduces only its own sender: admitting a *third* member
        // from it would be a join with no dial behind it. A tuple that fails that test
        // is dropped whole, claim included. Holding the claim instead — against a
        // member this view has not met — sounds strictly better, and is not: those ids
        // are on no roster, so no reap can ever strike them, and one frame of
        // attacker-chosen ids banks entries that nothing reclaims. Measured, one 24.9 MB
        // frame retained 376.5 MB, a 15x amplification of the wire. Dropping costs a
        // window instead: the maker re-advertises `verified` for every member it has
        // verified on every round it sends, so a claim arriving before its subject is
        // re-sent on the next round *with that maker* — O(cluster size) intervals,
        // since a node gossips to one random peer per interval and claims are never
        // relayed. Bounded in bytes, unbounded in time only as far as C35's roster
        // growth allows.
        let members = members
            .into_iter()
            .filter(|(node, addr, ..)| {
                // `is_member` holds for self too — a node is a member of its own view
                // from construction and is never reaped out of it.
                let node = NodeId::from(node.clone());
                membership.is_member(&node) || (&node == sender && addr == node.as_bytes())
            })
            .collect();
        // Whether a claim is attributable is asked the same way on both halves of a
        // round, because `verifiers` has to converge and this is what decides what
        // enters it. Reading the link here (bound to `sender`, C13) while the reply
        // half reads the member's advertised transport made the two halves disagree
        // about the same member: a plaintext member under `require_peer_identity` had
        // its claims kept by every node it dialed and dropped by every node that dialed
        // it, so nodes holding identical evidence built different rings and placed
        // rooms differently — permanently. `dial_establishes_identity` is a function of
        // configuration and the member's own address, so every node computes it alike.
        let attributable = self.dial_establishes_identity(sender);
        self.merge_gossip_attributed(sender, members, attributable);
        let reply = crate::gossip::gossip_frame(&self.known_liveness());
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.outbox.push(reply);
        }
        true
    }

    /// Apply an inbound [`Message::FollowerHeads`] from the member `sender` its link
    /// was admitted as: catch the reporting follower up from the durable heads it
    /// named, honoring them over any remembered ack (the wiped-follower self-heal).
    /// Honored only in cluster mode; a report on a single-node deployment is a stray
    /// frame and the connection is dropped. The catch-up frames are queued for the
    /// follower and the transport routes them over its peer connection. Returns whether
    /// the connection stays open.
    ///
    /// **A node reports only its own heads.** The frame names its reporter, but the
    /// name is a claim checked against the link's established identity, not an
    /// instruction: a report is authoritative because a node is the only authority on
    /// what it durably holds, and the moment one member can name another it can credit
    /// a third node with data that node does not have — which raises that node's
    /// watermark and makes majority-ack release a client `Accepted` for a write no
    /// majority ever held. A mismatch is a member speaking for someone else, or a node
    /// whose configured id disagrees with the one its link claimed, and the connection
    /// is dropped either way.
    fn apply_follower_heads(
        &mut self,
        sender: &NodeId,
        reporter: Vec<u8>,
        heads: Vec<(RoomId, u64)>,
    ) -> bool {
        if self.membership.is_none() {
            return false;
        }
        if NodeId::from(reporter) != *sender {
            return false;
        }
        self.catch_up_follower_reporting(sender, &heads);
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
    ///
    /// An ack from a node the roster no longer holds is dropped: a replication link
    /// outlives the gossip verdict that reaped its far end, so an in-flight ack can
    /// arrive after the sweep, and recording it would re-key the map on a departed
    /// member that no later reap reaches. A non-member is no room's follower, so its
    /// watermark counts toward no quorum and dropping the ack costs the quorum nothing;
    /// it does cost the release pass a trigger it was only ever an accident of, which
    /// C64 and C69 own — what re-evaluates a withheld ack when the roster, rather than a
    /// watermark, is what moved. Single-node mode has no roster to gate on, so an ack
    /// records unconditionally there — it leads every room alone, at a majority of one,
    /// so a watermark drives none of its quorums.
    pub fn record_replica_ack(&mut self, follower: NodeId, room: &[u8], through_seq: u64) {
        if self
            .membership
            .as_ref()
            .is_some_and(|m| !m.is_member(&follower))
        {
            return;
        }
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
        // A stranded node's ring is empty, and an empty replica set would otherwise
        // compute a majority of one — satisfied by self alone, which is exactly the
        // majority-of-one commit the empty ring exists to prevent. The two states look
        // identical from `replicas_for` and are opposite in meaning: no membership is a
        // single-node deployment that owns every room, while a stranded node owns none
        // and must hold every write rather than release it unreplicated.
        if membership.is_stranded() {
            return (usize::MAX, Vec::new());
        }
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
        // The upgrade fast path authenticates at accept, ahead of any in-band Auth —
        // record the connect here (the app is not yet known, so its resource is the
        // empty app) so a fast-path connect is audited the same as an in-band one.
        self.authorizer
            .observe(&identity, Action::Connect, &Resource::App(&[]), true);
        self.insert_conn(Session::authenticated(identity))
    }

    /// Open a connection already authenticated as `identity` by a verified mTLS
    /// client certificate — [`connect_authenticated`](Self::connect_authenticated)
    /// plus the `hosts` that certificate names. Those hosts are what lets the peer
    /// plane bind the link to a member: an in-band credential names an actor the
    /// deployment's verifier chose to trust, which says nothing about which node is on
    /// the other end of the socket. They are read by a narrower rule than the actor —
    /// see [`host_names_from_client_cert`](crate::tls::host_names_from_client_cert) —
    /// so a certificate may authenticate an actor and bind no member at all.
    pub fn connect_cert_authenticated(
        &mut self,
        identity: Identity,
        hosts: Vec<Vec<u8>>,
    ) -> ConnId {
        let id = self.connect_authenticated(identity);
        if let Some(conn) = self.conns.get_mut(&id) {
            conn.cert_hosts = Some(hosts);
        }
        id
    }

    fn insert_conn(&mut self, session: Session) -> ConnId {
        let id = ConnId(self.next);
        self.next += 1;
        self.conns.insert(
            id,
            Conn {
                session,
                outbox: Vec::new(),
                peer: None,
                cert_hosts: None,
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
    /// (longer) TTL rather than an older peer's. A room with neither presence nor
    /// any subscriber is dropped from this map, bounding it on a server that churns
    /// through rooms — but the *binding* survives on the hub, which holds the room's
    /// own governing app, so a dormant room's next subscriber rebinds to what
    /// governed it rather than to whatever app arrives first. The version floor
    /// resets to that binding, not to nothing.
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
                // older peer's shorter TTL. A room that goes dormant drops out of
                // this map and rebinds from the hub's own binding, so the floor
                // resets to what governs the room, not to nothing.
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
        // The live map is the *presence* view; the hub's binding is the room's own,
        // read when this one has nothing (a dormant room, a restart). Both are keyed
        // by a room a subscribe named, and a subscribe can name a room that never
        // materializes, so the sweep bounds the hub's map to the rooms it holds.
        self.hub.forget_unheld_governing();
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
    /// registry and cached for the process lifetime — a link is immutable once
    /// registered, so a resolved schema is cached and never re-parsed.
    ///
    /// An **unresolved** version is not cached. The registry parses what it stores, so
    /// the only way this resolves to nothing is a version this node does not hold —
    /// which the control plane can register at any moment, and a negative entry would
    /// outlive that registration for the life of the process. The rooms bound to such
    /// a version are the ones that most need to pick it up: until it resolves they
    /// have no `@auth` grants and no declared zones, so a whole-room channel there is
    /// served every partition.
    ///
    /// The cost is a registry lock per resolve — not a re-parse, since an absent
    /// version has no bytes to parse — for as long as a bound version stays
    /// unresolvable. That is usually the startup window before an app re-registers,
    /// but it can be permanent: a binding only ever rises
    /// ([`bind_room_app`](Registry::bind_room_app)) and it is durable, so a room bound
    /// at a version above what this node's registry reaches — an operator rollback, a
    /// lagging node in a mixed fleet — pays that lock on every frame.
    ///
    /// An *uncontended* lock against a frame's decode and per-path ACL evaluation is
    /// not worth a registry generation counter to avoid, and the counter would move
    /// the same bug — an append that forgets to bump caches a miss forever — somewhere
    /// no test presses on it. What breaks that argument is **contention**, not how
    /// often the unresolvable binding occurs: this mutex is shared with the admin
    /// plane and with chain resolution, so a deployment registering frequently, or a
    /// mixed fleet driving `resolve_chains` hard, makes the lock contended while the
    /// misconfiguration stays exactly as rare. Reconsider there.
    fn parsed_schema(&mut self, app: &(Vec<u8>, u32)) -> Option<Arc<Schema>> {
        let schema = match self.schema_cache.get(app) {
            Some(schema) => Some(schema.clone()),
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
                if let Some(schema) = &schema {
                    self.schema_cache.insert(app.clone(), schema.clone());
                }
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
    /// resolved against the registry. It is the authorization fallback for a
    /// **subscribe** to a room not yet bound, and for nothing else: a subscriber is
    /// about to become that room's incumbent, while every other frame names a room
    /// it does not establish and so is governed by that room's own binding or by
    /// nothing. `None` for a relay connection.
    fn connection_schema(&mut self, id: ConnId) -> Option<Arc<Schema>> {
        let conn = self.conns.get(&id)?;
        let version = conn.session.schema_version()?;
        let app = (conn.session.app_id().to_vec(), version);
        self.parsed_schema(&app)
    }

    /// The parsed schema governing `room` — the app bound to it — which gates a
    /// peer's read of the room's fan-out and narrows each channel's zone scope.
    /// `None` for a relay room none enforces.
    ///
    /// The binding is the live map's, or — where a dormant sweep or a restart dropped
    /// it — the room's own, which the hub holds. The same resolution an authorizing
    /// frame takes ([`Registry::deliver`]'s `room_binding`), and it has to be: the two
    /// decide the same room's zone partitions, one at Subscribe and one at every write
    /// after it, so a fan-out that answered "this room declares nothing" where the
    /// subscribe answered "these zones" would serve a channel the partitions it was
    /// narrowed away from.
    ///
    /// The two are resolved at different moments in a frame, so a registration landing
    /// between them can leave the gate deciding under one schema and the fan-out under
    /// the next. Both directions are safe: the newer schema's zone block extends the
    /// older's, so the fan-out either narrows by the same partitions or by more of
    /// them, and the gate's answer is never the wider one for having been taken first.
    fn governing_schema(&mut self, room: &[u8]) -> Option<Arc<Schema>> {
        let app = self
            .room_apps
            .get(room)
            .cloned()
            .or_else(|| self.hub.governing_app(room))?;
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
    /// app's chain, so it applies only when the write carried a version and its
    /// author speaks that same app — a relay write, an unbound room, or a
    /// foreign-app write (whose version number is a different app's space) is
    /// left verbatim. A [`Replicated`](WriteOrigin::Replicated) batch has no
    /// author on this node and arrives untagged, so it is left verbatim too.
    fn translation_source(
        &self,
        room: &[u8],
        origin: &WriteOrigin,
        version: Option<u32>,
    ) -> Option<(Vec<u8>, u32)> {
        let from = version?;
        let (app, _) = self.room_apps.get(room)?;
        let writer_app = self.conns.get(&origin.author()?)?.session.app_id();
        (writer_app == app.as_slice()).then(|| (app.clone(), from))
    }

    /// The parsed migration chain from `from` to each distinct target version
    /// among the room's same-app recipients, resolved once (the registry is
    /// locked only here, not across the fan-out). A target whose chain is
    /// unreachable, gapped, or unparseable maps to `None`, so the fan-out drops
    /// that recipient's batch rather than serving it wrong. Every connection is
    /// a candidate — the writing one included, since its sibling channels
    /// receive the write — so the memo is total over what the fan-out walks.
    fn resolve_chains(
        &self,
        app: &[u8],
        from: u32,
    ) -> HashMap<u32, Option<crate::translate::Chain>> {
        let targets: HashSet<u32> = self
            .conns
            .iter()
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

    /// Fan a committed op batch out to `(room, branch)`'s subscribers, whatever
    /// brought it in — a local client's write or the room's leader replicating
    /// one onto this follower. A room holding live doc-ACL tuples redacts
    /// per-recipient by the op's document path
    /// ([`fan_out_ops_redacted`](Registry::fan_out_ops_redacted)); a room holding
    /// none takes the plain path ([`fan_out_ops_plain`](Registry::fan_out_ops_plain))
    /// — the whole-document read gate plus per-target migration translation, no
    /// path walk.
    ///
    /// **Every verdict on this seam is re-decided here, against the state this
    /// node is serving.** For a replicated batch that is the point: what the
    /// leader computed was a verdict for *its own* subscribers, a different set of
    /// actors holding different grants at different schema versions under
    /// different zone scopes, and the wire frame carries none of it — the leader
    /// relays the committed ops verbatim and untagged. Reusing it would be reusing
    /// an answer to another question; so the follower resolves the room's creator,
    /// its ACL tuples, its governing schema and each recipient's read from its own
    /// replica, which is the state being served (C28).
    fn fan_out_ops(
        &mut self,
        origin: WriteOrigin,
        room: &[u8],
        branch: &[u8],
        broadcast: &[Op],
        broadcast_version: Option<u32>,
    ) {
        // Both paths resolve the room's creator, its ACL tuples, its element-path
        // index and its element types before they reach a recipient, and each of
        // those walks the whole room document. A replicated batch is the one that
        // can arrive on a stream nobody here has bound a channel to — the steady
        // state of a replica, since a room is replicated to every node in its set
        // and subscribed on few of them — so it is checked for a recipient before
        // any of that is resolved. A local write is not checked: it arrived on a
        // channel of the writing connection, bound to this very stream (that
        // binding is what resolved the stream), so the scan could only ever say
        // yes, and the leader's hot path does not pay for an answer it knows.
        if matches!(origin, WriteOrigin::Replicated) {
            let served = self
                .conns
                .values()
                .any(|conn| conn.session.serves_stream(room, branch));
            if !served {
                return;
            }
        }
        let records = self.hub.acl_records(room);
        if records.is_empty() {
            self.fan_out_ops_plain(origin, room, branch, broadcast, broadcast_version);
        } else {
            self.fan_out_ops_redacted(origin, room, branch, broadcast, broadcast_version, records);
        }
    }

    /// Fan a committed op batch out to `(room, branch)`'s subscribers of a room
    /// with no doc-ACL tuples: the whole-document read gate per recipient, then
    /// per-target migration translation, then the per-channel zone filter. No
    /// path walk — there are no per-path verdicts to resolve.
    fn fan_out_ops_plain(
        &mut self,
        origin: WriteOrigin,
        room: &[u8],
        branch: &[u8],
        broadcast: &[Op],
        broadcast_version: Option<u32>,
    ) {
        // The room's governing schema gates each peer's read consistently,
        // resolved once (owned) so the peer loop can borrow the conns.
        let schema = self.governing_schema(room);
        let authorizer = &*self.authorizer;
        // The owning-element type of each op, resolved once over the room
        // document, so a type-scoped migration step narrows to the ops whose
        // owning element is of its declared type. Empty (no narrowing) when the
        // room binds no schema.
        let types = schema
            .as_ref()
            .map(|s| self.hub.element_types(room, s))
            .unwrap_or_default();
        // Per-recipient migration translation rides the same seam as redaction. It
        // is scoped to the room's governing app: the write is translated only when
        // the writer speaks that app (its version number lives in that app's
        // space), and only to recipients of that app — a foreign-app connection's
        // version is a different space and must never drive the room's chain.
        let source = self.translation_source(room, &origin, broadcast_version);
        // Resolve every distinct target version's chain up front, holding the
        // registry lock only for that (not across the fan-out), then translate the
        // peer loop against the owned, parsed chains.
        let chains = source
            .as_ref()
            .map(|(app, from)| self.resolve_chains(app, *from));
        // Translate the batch once per distinct target version — the rewrite
        // depends only on the version, not the recipient, so a same-version fleet
        // shares one result. A resolved chain translates; an unresolved one
        // (unreachable / gapped / unparseable) yields an empty batch, dropping it
        // for that target's recipients pending the handshake range-check that
        // refuses them outright.
        let translated_by_target: HashMap<u32, Vec<Op>> = chains
            .iter()
            .flatten()
            .map(|(target, chain)| {
                let ops = match chain {
                    Some(chain) => chain.translate_ops_scoped(broadcast, &types),
                    None => Vec::new(),
                };
                (*target, ops)
            })
            .collect();
        for (peer, conn) in self.conns.iter_mut() {
            // The channels this connection receives the write on — its whole
            // subscription to the stream, minus the channel that authored it.
            // Resolved first, so a connection with nothing to receive costs no
            // read verdict.
            let channels = origin.recipients(*peer, &conn.session, room, branch);
            if channels.is_empty() {
                continue;
            }
            // Per-recipient redaction: a peer whose read was revoked mid-session
            // stops receiving the room's ops at once, without waiting for it to
            // resubscribe.
            if !peer_may_read(authorizer, schema.as_deref(), &conn.session, room) {
                continue;
            }
            // Translate to the recipient's version, or send verbatim when there is
            // nothing to bridge — a same-version, relay, or foreign-app recipient,
            // or a relay write.
            let translated = match (&source, conn.session.schema_version()) {
                (Some((app, from)), Some(target))
                    if conn.session.app_id() == app && target != *from =>
                {
                    // Total over every eligible recipient: `resolve_chains` keyed
                    // the memo on this same (same-app, target != from) predicate,
                    // so the target is always present.
                    Some(translated_by_target[&target].as_slice())
                }
                _ => None,
            };
            let ops = translated.unwrap_or(broadcast);
            if ops.is_empty() {
                continue;
            }
            for channel in channels {
                // Narrow to the channel's authorized zone partitions — the
                // per-zone wire redaction. A channel scoped to a subset of the
                // room's zones drops the rest, so an unauthorized zone never
                // surfaces on it; an emptied channel gets no frame. The scope is
                // resolved against the room's governing schema as it governs now,
                // not the one that was acting when the channel joined.
                let zoned = conn
                    .session
                    .zone_filter(channel, ops, authorizer, schema.as_deref());
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

    /// Fan a committed op batch out to `(room, branch)`'s subscribers with
    /// per-recipient doc-ACL redaction: each recipient receives only the ops in
    /// subtrees its actor may read, the rest silently withheld from *it* while an
    /// authorized peer still gets them. The room's creator (owns `/`) reads every
    /// op; a subtree-scoped reader receives just its granted subtrees.
    ///
    /// Redaction runs on the *authored* ops (their target resolves through the
    /// element-path index of the tree this `(room, branch)` stream serves), then the
    /// surviving subset is migration-translated
    /// to each recipient's version — redact-then-translate, since translation can
    /// drop ops and would otherwise desync the path lookup. An op whose *container*
    /// target the index cannot resolve reads at the root
    /// ([`op_read_gate`](crate::acl::op_read_gate)), so whatever the root verdict admits
    /// carries it — which includes a root grant carved by a subtree deny, and is C67's
    /// ruling to narrow. An op naming no container — a mark whose anchor sequence has
    /// left the tree, an ACL scope whose target has — reaches only a reader denied
    /// nothing ([`op_read_gate`](crate::acl::op_read_gate), C52).
    fn fan_out_ops_redacted(
        &mut self,
        origin: WriteOrigin,
        room: &[u8],
        branch: &[u8],
        broadcast: &[Op],
        broadcast_version: Option<u32>,
        records: Vec<crdtsync_core::acl::AclRecord>,
    ) {
        let creator = self.hub.room_creator(room);
        let schema = self.governing_schema(room);
        // Every read verdict below resolves through the tree **this stream** serves, not
        // `main`'s. A branch owns its base — a captured version, a publish — or shares
        // only `main`'s history below its fork point, and `main` moves on past both: an
        // element scope `main` cannot resolve is an inert deny, and an op target it
        // cannot resolve falls back to the root, which a root-readable but
        // subtree-denied reader carries (C60). A stream with no tree — an undecodable
        // owned base, a shared base compaction has clipped — is fanned nothing: the only
        // indexes left to redact with resolve *less* than the truth, and a scope or a
        // target that resolves to nothing is admitted rather than withheld. The subscribe
        // seam refuses such a branch outright, so a joiner is told; a channel bound while
        // the room still held no doc-ACL tuple keeps its subscription and stops receiving
        // instead, which is the fail-closed reading of a stream this node cannot redact.
        let Some(index) = self.hub.stream_element_paths(room, branch) else {
            return;
        };
        // A RangedElement op resolves its governing seq paths through the held anchor
        // set (a SetPayload/Delete carries only the range id); tombstoned ranges are
        // included so a just-applied delete still resolves. Asked of the same stream as
        // the index, and refused on the same terms, so neither can describe a tree the
        // other does not.
        let Some(ranged_anchors) = self.hub.stream_ranged_anchors(room, branch) else {
            return;
        };
        // Which container ids that tree has materialised at all, live or retained — what
        // separates a target it *keeps* (unresolvable by the walk, still its own state)
        // from one belonging to another stream entirely. `op_read_gate` consults it only
        // for a target `index` does not resolve, so a batch whose targets all resolve —
        // the common case — never pays the walk, and the empty set it is handed then is
        // never read. The two lines are adjacent so that argument stays local.
        let unplaceable = broadcast.iter().any(|op| !index.contains_key(&op.target));
        let held = if unplaceable {
            match self.hub.stream_held_containers(room, branch) {
                Some(held) => held,
                None => return,
            }
        } else {
            HashSet::new()
        };
        // The owning-element type of each op, resolved once over the room document
        // — a type-scoped migration step narrows to the ops whose owning element is
        // of its declared type. Empty (no narrowing) when the room binds no schema.
        let types = schema
            .as_ref()
            .map(|s| self.hub.element_types(room, s))
            .unwrap_or_default();
        // Each op's read gate is recipient-independent, so resolve it once. A recipient
        // must read every path in an op's set to receive it (require-all — a Ranged op's
        // distinct anchor seq paths, one path for every other op), or read the document
        // whole where the op resolves to no path at all.
        let op_gates: Vec<crate::acl::OpReadGate> = broadcast
            .iter()
            .map(|op| crate::acl::op_read_gate(&index, &held, &ranged_anchors, &records, op))
            .collect();
        // Migration translation rides the same seam as redaction (scoped to the
        // room's governing app); resolve each distinct target's chain once.
        let source = self.translation_source(room, &origin, broadcast_version);
        let chains = source
            .as_ref()
            .map(|(app, from)| self.resolve_chains(app, *from));
        let authorizer = &*self.authorizer;
        // The nodes this batch relocates. A move that carries a node into a subtree a
        // recipient can read, out of one it could not, reveals a born-denied node to
        // that recipient (reveal-on-move-in) — a shell must precede the move so the
        // recipient can materialize it, mirroring the catch-up seam.
        let hub = &self.hub;
        // Each relocated node paired with the partition it **lands in**, read off the
        // tree this stream serves now the batch is folded. A reveal shell, its
        // back-filled content, and the batch's own copy of any op that content covers
        // all ride that one partition, so the per-channel zone filter takes a revealed
        // node's shell and the content the back-fill reaches together or not at all.
        //
        // The landing partition, not the move op's own — an op's envelope carries the
        // partition its author resolved when it was emitted, which for a move emitted
        // before the one that relocates its new parent is the partition the subtree is
        // *leaving*. Reading the folded tree instead gives every node inside one
        // relocated subtree the same answer whatever order the transaction emitted its
        // moves in, and answers in the partition ids a channel's zone scope was resolved
        // in, which an author's envelope only coincidentally does. A node the served
        // tree does not resolve keeps its move's answer, the only other partition claim
        // there is. For a room with no zones every zone is `None`, so this is a no-op.
        let shell_nodes: Vec<(ElementId, Option<u32>)> = broadcast
            .iter()
            .filter_map(|op| {
                // A move is not the only thing that staleses a shell: a node's tag is a
                // meet over the claims that named it, so a claim in this broadcast can
                // lower one. That claim is a birth into the node's own list, which a
                // recipient denied the origin never sees, so on the move gate alone it
                // would keep a tag its document can never revise.
                let node = match &op.kind {
                    OpKind::XmlMove { node, .. } => *node,
                    _ => crdtsync_core::retagged_node(op)?,
                };
                let landed = schema
                    .as_deref()
                    .zip(index.get(&node))
                    .map(|(s, path)| crdtsync_core::zone::zone_id_of(s, path));
                Some((node, landed.unwrap_or(op.zone)))
            })
            .collect();
        for (peer, conn) in self.conns.iter_mut() {
            // The channels this connection receives the write on — its whole
            // subscription to the stream, minus the channel that authored it.
            // Resolved before the per-recipient redaction so a connection with
            // nothing to receive (unsubscribed, or the author's sole channel)
            // costs no path walk.
            let channels = origin.recipients(*peer, &conn.session, room, branch);
            if channels.is_empty() {
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
            // The whole-document verdict an unplaceable op is gated on is likewise one
            // answer for this whole recipient, and costs a read verdict per governing
            // tuple path — so it is resolved at most once, and not at all for a batch
            // that holds no such op (the common case).
            let whole_verdict: Cell<Option<bool>> = Cell::new(None);
            let reads_whole = || match whole_verdict.get() {
                Some(v) => v,
                None => {
                    let v = crate::acl::reads_whole_document(
                        authorizer,
                        &records,
                        creator.as_deref(),
                        &index,
                        schema.as_deref(),
                        identity,
                        room,
                    );
                    whole_verdict.set(Some(v));
                    v
                }
            };
            debug_assert_eq!(op_gates.len(), broadcast.len());
            let readable: Vec<Op> = crate::session::retain_atomic_cloned(broadcast, |i, _| {
                // `op_gates` parallels `broadcast`, so the op's gate is its own position
                // in it.
                op_gates[i].admits(
                    |path| {
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
                    },
                    reads_whole,
                )
            });
            // An empty readable subset is not on its own a reason to send nothing: a
            // shell can be due for a batch whose every op this recipient is denied.
            // That is exactly the retag case — the claim that lowers a revealed node's
            // tag is a birth into the node's own denied list — so the shell would be
            // skipped precisely when it is the only thing that could carry the change.
            // A batch that yields neither is dropped by the emptiness check below.
            if readable.is_empty() && shell_nodes.is_empty() {
                continue;
            }
            // Reveal-on-move-in: for every node this batch moves into a position this
            // recipient can read but was born where it could not, prepend a shell so the
            // recipient materializes the node and the (readable) move folds it into place
            // — the live-fan-out mirror of the catch-up reveal, derived from the same read
            // predicate. A recipient reading the node's origin all along gets no shell
            // (`reveal_ops` returns it only when the birth path is denied). Shells lead so
            // the move lands onto them. A batch that only *retags* a revealed node sends
            // the shell alone: its payload and its identity both carry the tag, so the
            // re-emitted shell is the change.
            let readable = if shell_nodes.is_empty() {
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
                        OpKind::XmlReveal { node, .. } => shell_nodes
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
                    // into place.
                    let mut prefix: Vec<Op> = Vec::new();
                    // An op is back-filled only if this frame is not already carrying
                    // it — neither in the batch nor in an earlier shell's back-fill,
                    // since a revealed node's subtree contains its revealed descendants'.
                    // A second, untagged copy would be the one the recipient folds while
                    // the batch's own copy is discarded as an id it already holds, taking
                    // that op out of its transaction's count for good.
                    let mut carried: HashSet<crdtsync_core::OpId> =
                        readable.iter().map(|op| op.id).collect();
                    // The landing partition of each op the back-fill yielded but the batch
                    // already carries. That surviving copy is the batch's, stamped in the
                    // partition its author resolved when it was emitted — for an edit
                    // inside a node emitted before the move that relocates it, the
                    // partition the node is leaving. Left alone the zone filter would drop
                    // it and keep the shell, leaving the reader a materialised node it
                    // never fills, so it takes the same partition its back-filled copy
                    // would have (C16).
                    let mut co_travel: HashMap<crdtsync_core::OpId, Option<u32>> = HashMap::new();
                    for shell in shells {
                        let OpKind::XmlReveal { node, .. } = &shell.kind else {
                            continue;
                        };
                        let node = *node;
                        let zone = shell.zone;
                        prefix.push(shell);
                        let backfill = hub.reveal_backfill(
                            room,
                            node,
                            &records,
                            |p| {
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
                            },
                            reads_whole,
                        );
                        for mut op in backfill {
                            if carried.insert(op.id) {
                                op.zone = zone;
                                prefix.push(op);
                            } else {
                                // Either the batch carries it or an earlier shell's
                                // back-fill already stamped it; the map is read against
                                // the batch alone, so an entry for the second is inert.
                                // First shell wins, matching the copy `carried` kept —
                                // and nested shells agree anyway, since the path index
                                // does not extend through an XML subtree, so every node
                                // inside one shares its holding slot's partition.
                                co_travel.entry(op.id).or_insert(zone);
                            }
                        }
                    }
                    prefix
                        .into_iter()
                        .chain(readable.into_iter().map(|mut op| {
                            if let Some(zone) = co_travel.get(&op.id) {
                                op.zone = *zone;
                            }
                            op
                        }))
                        .collect()
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
            for channel in channels {
                // Narrow to the channel's authorized zone partitions — the wire
                // redaction for per-zone streams. A channel scoped to a subset of the
                // room's zones drops the rest; an unauthorized zone never surfaces,
                // and a channel left with nothing is not sent an empty frame. The
                // scope is resolved against the room's governing schema as it governs
                // now, not the one that was acting when the channel joined.
                let ops =
                    conn.session
                        .zone_filter(channel, &translated, authorizer, schema.as_deref());
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
    /// the room's own, which the hub holds, so a foreign app cannot seize a room by
    /// subscribing first after it went idle. A same-app bind (a new room or a
    /// version lift) is mirrored into the hub's binding.
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
    /// replies and fanning any broadcast out to the room's other subscribed
    /// channels. Returns whether the connection should stay open.
    pub fn deliver(&mut self, id: ConnId, msg: Message) -> bool {
        // The cluster secret admits this connection to the peer plane. Handled
        // ahead of everything else and never answered: a wrong or unconfigured
        // secret drops the connection with no reply, so a guess costs a fresh
        // connection and learns nothing from what comes back.
        if let Message::PeerAuth { node, secret } = &msg {
            return self.authenticate_peer(id, node, secret);
        }
        // A Replicate or a Gossip arrives node-to-node on a peer connection, not
        // from a client on its data plane — intercept each before the client
        // session step and handle it as a peer, but only on a connection that has
        // presented the cluster secret. On any other connection they fall through
        // to the session step, which answers each with the protocol violation it
        // is: the node-to-node handlers are unreachable from the client plane.
        let msg = if let Some(sender) = self.peer_identity(id) {
            match msg {
                Message::Replicate {
                    room,
                    branch,
                    ops,
                    base_seq,
                    epoch,
                    creator,
                    governing,
                    max_op_version,
                } => {
                    // `base_seq` is the leader's compaction floor. Unit 4 replicates
                    // the whole log from the first op, so a follower on the ops path
                    // already tracks the leader's sequence space (a below-floor
                    // follower takes the snapshot path instead), and the ack needs no
                    // adjustment.
                    let _ = base_seq;
                    let frame = ReplicaFrame {
                        sender: &sender,
                        room,
                        branch,
                        epoch,
                    };
                    return self.apply_replicate(
                        id,
                        frame,
                        ops,
                        ReplicatedMeta {
                            creator,
                            governing,
                            max_op_version,
                        },
                    );
                }
                Message::ReplicateSnapshot {
                    room,
                    branch,
                    seq,
                    state,
                    epoch,
                    creator,
                    governing,
                    max_op_version,
                } => {
                    let frame = ReplicaFrame {
                        sender: &sender,
                        room,
                        branch,
                        epoch,
                    };
                    return self.apply_replicate_snapshot(
                        id,
                        frame,
                        seq,
                        state,
                        ReplicatedMeta {
                            creator,
                            governing,
                            max_op_version,
                        },
                    );
                }
                // A metadata-only frame names no branch: the root is a fact about the
                // room, not about one of its streams, so the gate is handed `main` —
                // the stream every replication frame is fenced against.
                Message::ReplicateMeta {
                    room,
                    epoch,
                    creator,
                } => {
                    let frame = ReplicaFrame {
                        sender: &sender,
                        room,
                        branch: MAIN_BRANCH.to_vec(),
                        epoch,
                    };
                    return self.apply_replicate_meta(frame, creator);
                }
                Message::Gossip { members } => return self.apply_gossip(id, &sender, members),
                // A follower's durable-head report arrives node-to-node on a peer
                // connection; catch it up from the reported heads off the client session
                // path. The reporter it names must be the member the link was admitted
                // as — a node is authoritative for its own durable state and no other's.
                Message::FollowerHeads { reporter, heads } => {
                    return self.apply_follower_heads(&sender, reporter, heads)
                }
                // A ping-req arrives node-to-node on a peer connection asking this relay
                // for its liveness view of a third member; answer it off the client
                // session path. A ping-ack is only ever read inline by the requester that
                // sent the ping-req (never delivered), so one reaching here is unsolicited
                // — drop the connection.
                Message::PingReq { target } => return self.apply_ping_req(id, target),
                Message::PingAck { .. } => return false,
                other => other,
            }
        } else {
            msg
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
        // A clone installs `dst`'s content and, with it, the app that governs it —
        // the source's, or none. The live map is checked ahead of the hub's binding,
        // so it has to be re-pointed at the same answer when the clone lands, or a
        // name someone subscribed to first keeps governing the copy by whatever app
        // that subscriber declared.
        let cloned_dst = match &msg {
            Message::CloneRoom { dst, .. } => Some(dst.clone()),
            _ => None,
        };
        // The channel a write arrives on — the one replica in the room that
        // already holds its ops, and so the only one the fan-out below omits.
        let write_channel: Option<Channel> = match &msg {
            Message::Ops { channel, .. } | Message::CrossZoneOps { channel, .. } => Some(*channel),
            _ => None,
        };
        // The room this message authorizes against, so its enforcement composes
        // under the schema that governs *that room* — never the actor's own,
        // self-declared app, which a foreign connection could pick to escalate.
        let authz_room: Option<RoomId> = match &msg {
            Message::Subscribe { .. } => subscribed_room.clone(),
            // Room-keyed management: the frame carries the room it acts on, so a
            // caller needs no subscription to it and its schema binds off the frame's
            // room. Branch management and the cross-zone token request are the whole
            // set; a clone names two rooms and is resolved below.
            Message::CrossZoneToken { room, .. }
            | Message::BranchList { room }
            | Message::BranchFork { room, .. }
            | Message::BranchForkFromVersion { room, .. }
            | Message::BranchRestore { room, .. }
            | Message::BranchPublish { room, .. }
            | Message::BranchDelete { room, .. } => Some(room.clone()),
            // A clone is a read of `src` whole composed with a create of `dst`, so it
            // binds off the *source* — the room whose content, doc-ACL tuples and zone
            // declarations the clone carries, and so the room whose schema the read
            // gate must compose under. `dst` binds none of its own: the clone is
            // create-only, so it does not exist yet.
            Message::CloneRoom { src, .. } => Some(src.clone()),
            Message::Ops { channel, .. }
            | Message::CrossZoneOps { channel, .. }
            | Message::AwarenessSet { channel, .. }
            | Message::VersionCreate { channel, .. }
            | Message::VersionRename { channel, .. }
            | Message::VersionDelete { channel, .. }
            | Message::VersionList { channel, .. }
            | Message::VersionFetch { channel, .. }
            // A diff query is channel-keyed for exactly this: the change list it
            // answers with is narrowed by the room's zone declarations, and those
            // live in the acting schema, which is what this resolution finds.
            | Message::DiffQuery { channel, .. } => self
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
            // Unbound: the connection's own app is the fallback for a subscribe and
            // nothing else, because a subscribe is the one frame whose caller is
            // about to become the room's incumbent. A room-keyed frame names a room
            // it does not establish, so a self-declared app there would be the caller
            // choosing which `@auth` grants and zone declarations govern someone
            // else's room — the escalation this resolution exists to refuse. An
            // unbound room is governed by nothing instead.
            Some(None) if matches!(msg, Message::Subscribe { .. }) => self.connection_schema(id),
            _ => None,
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
        // Whether this connection was already authenticated before the step, so an
        // in-band Auth that just admitted a credential is told from a later message
        // on an already-authenticated session — only the transition is the auditable
        // connect event.
        let was_authed = self
            .conns
            .get(&id)
            .is_some_and(|c| c.session.identity().is_some());
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
            cloned,
            root_established,
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
            // Whether a clone actually landed — a no-op one (an absent or unled
            // source, a taken destination) installs nothing and so re-points nothing.
            // Read before the replies are drained into the outbox below.
            let cloned = resp
                .replies
                .iter()
                .any(|m| matches!(m, Message::CloneRoomResult { created, .. } if *created));
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
                cloned,
                resp.root_established,
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
        // An in-band Auth that just admitted a credential is a security-relevant
        // connect: record it once, on the transition, through the audit seam (the
        // authorizer's observe, which a durable-audited deployment persists). The
        // event names the connection's app as its resource; a rejected credential
        // never authenticates, so it takes the connection-closing error path above
        // rather than an audited connect.
        if !was_authed {
            let connected = self.conns.get(&id).and_then(|c| {
                c.session
                    .identity()
                    .map(|identity| (identity.clone(), c.session.app_id().to_vec()))
            });
            if let Some((identity, app)) = connected {
                self.authorizer
                    .observe(&identity, Action::Connect, &Resource::App(&app), true);
            }
        }
        // Bind the subscribed room to the enforcing app governing it, so both the
        // schema-authorization tier and (with no injected policy) presence expiry
        // resolve its schema. Bound unconditionally — authorization consults the
        // binding on every room even under an injected awareness policy — and a
        // sweep's reconcile prunes dormant rooms, so the map stays bounded.
        if let Some((room, app_id, version)) = bind {
            self.bind_room_app(room, app_id, version);
        }
        if let Some(dst) = cloned_dst.filter(|_| cloned) {
            match self.hub.governing_app(&dst) {
                Some(app) => {
                    self.room_apps.insert(dst, app);
                }
                None => {
                    self.room_apps.remove(&dst);
                }
            }
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
        } else if root_established {
            // A write can establish the room's authority root and broadcast nothing —
            // an authenticated resend of ops the room already holds is swallowed whole
            // by the dedup, and it is exactly the write that roots a room whose
            // establishing commit was anonymous. The frame above is the root's only
            // ride out; with no ops to build one, send the root on its own, so the
            // replicas do not stay creatorless — serving every doc-ACL deny in the
            // room as inert — until the room's next fresh commit.
            if let Some(room) = &room {
                self.enqueue_root_replication(room);
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
        // so fanning it out never advertises an unpersisted write. Each recipient
        // is sent the ops on the channel it opened for the room, so a connection
        // multiplexing several rooms can route what it receives.
        if !broadcast.is_empty() {
            // A write is the sole source of a broadcast, so the channel it
            // arrived on is present for every batch there is to fan out.
            debug_assert!(write_channel.is_some(), "a broadcast with no author");
            // A broadcast is scoped to its `(room, branch)` stream: an `Ops` write
            // always names both, so a branch write reaches only that branch's
            // subscribers and never crosses into another branch's stream.
            if let (Some(room), Some(branch), Some(channel)) =
                (room, broadcast_branch, write_channel)
            {
                // The write's authoring replica, the one this fan-out omits — the
                // channel it arrived on, not the whole connection that sent it.
                let origin = WriteOrigin::Local { conn: id, channel };
                self.fan_out_ops(origin, &room, &branch, &broadcast, broadcast_version);
            }
        }
        // Awareness is ephemeral: fan the entry out on each other subscriber's
        // channel. Presence is connection-scoped, so the *whole* originating
        // connection is excluded — unlike the op fan-out above, which omits only
        // the authoring channel. A connection holds one presence entry per room,
        // keyed by its Hello client id and its authenticated actor, both of which
        // its channels share: a sibling channel's set replaces this one rather
        // than coexisting with it, and an update carries only the actor, so an
        // echo would hand a client its own presence back as a peer's with nothing
        // on the wire to tell the two apart.
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
        if let Some((room, pre_high_water)) = lift_room {
            // A write tags its ops from the room's binding, so the room now holds a
            // high-water in that app's version space — and the two are read as a pair
            // wherever either is read. Re-assert the binding, so the room's own durable
            // record carries both: a dormant-room sweep prunes the hub's copy for a room
            // it does not yet *hold*, and a subscribe binds before the first write
            // materializes one, so a room bound, swept and then written keeps its number
            // and loses its app — after which its replication frames carry a version
            // nothing can read, and a restart makes that permanent.
            //
            // This cannot seize a room, on two independent grounds. `governing` is
            // resolved from the room's *existing* binding and is `None` for an unbound
            // one, so a write only ever re-states what a subscribe already established —
            // it can introduce no binding. And `bind_room_app` composes incumbent-wins
            // regardless, so even a caller that reached here with another app would leave
            // the incumbent standing.
            //
            // Off the steady path by construction: the assertion is only made when the
            // room's own record actually disagrees, which is the sweep's window and
            // nothing else, so an ordinary write compares and allocates nothing.
            if let Some((app, version)) = &governing {
                let stale = self
                    .hub
                    .governing_app(&room)
                    .is_none_or(|(bound, at)| bound != *app || at != *version);
                if stale {
                    self.bind_room_app(room.clone(), app.clone(), *version);
                }
            }
            // A write that raised the high-water strands the same joined peers a
            // replicated lift does, and is answered the same way — one predicate, one
            // seam, so admission and eviction cannot drift apart between the two.
            self.evict_stranded_by_lift(&room, pre_high_water);
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
