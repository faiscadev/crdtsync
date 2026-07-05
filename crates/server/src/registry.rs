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
use crdtsync_core::{ClientId, Message, Op, Schema};

use crate::acl::authorized;
use crate::auth::{AllowAll, Identity, Verifier};
use crate::authz::{Action, Authorizer, PermitAll, Resource};
use crate::auto_version::{
    expand_name, expand_schedule_name, schedule_origin, trigger_origin, AutoVersionSink,
    AutoVersionState,
};
use crate::clock::{Clock, SystemClock};
use crate::schema_registry::SchemaRegistry;
use crate::{
    step, AwarenessPolicy, EngineEvent, EventSink, Hub, RoomId, SchemaAwarenessPolicy, Session,
    Store,
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

    /// Auto-compact a room once its retained log reaches `threshold` ops, so a
    /// below-floor joiner is served a snapshot instead of a delta. `0` (default)
    /// never compacts.
    pub fn set_compaction_threshold(&mut self, threshold: u64) {
        self.hub.set_compaction_threshold(threshold);
    }

    /// A registry backed by `store`: its hub replays the persisted log, and
    /// every op the hub ingests is appended before it fans out to peers.
    pub fn with_store(server: ClientId, store: Store) -> io::Result<Self> {
        let mut hub = Hub::from_rooms(server, store.load()?)?;
        hub.attach_store(store);
        Ok(Self::from_hub(hub))
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
            let Some(schema) = self.parsed_schema(&app) else {
                continue;
            };
            let schedules: Vec<(u64, String, Option<u64>)> = schema
                .auto_version()
                .iter()
                .filter_map(|av| match av.trigger {
                    Trigger::Every(millis) => Some((millis, av.name.clone(), av.keep)),
                    Trigger::On(_) => None,
                })
                .collect();
            for (millis, template, keep) in schedules {
                let origin = schedule_origin(millis, &template);
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
                        due.push((room.clone(), template, origin, keep));
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

    /// Bind `room`'s governing schema to `{app_id, version}`. The first app to
    /// bind a room governs it — a later subscribe naming a *different* app is
    /// ignored, so a room is never re-governed by a foreign app's (shorter) TTL.
    /// A later subscribe on the *same* app lifts the binding to the higher
    /// version, so a rolling upgrade resolves to the newest version.
    fn bind_room_app(&mut self, room: RoomId, app_id: Vec<u8>, version: u32) {
        match self.room_apps.get(&room) {
            Some((bound_app, bound_version)) => {
                if *bound_app == app_id && version > *bound_version {
                    self.room_apps.insert(room, (app_id, version));
                }
            }
            None => {
                self.room_apps.insert(room, (app_id, version));
            }
        }
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
        // Only an awareness set consults the clock (to stamp last-seen), so the
        // op hot path never reads wall time.
        let now = if matches!(msg, Message::AwarenessSet { .. }) {
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
            Message::Ops { channel, .. }
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
        // bound, `Some(None)` an addressed-but-unbound room, `None` no room.
        let room_binding = authz_room
            .as_deref()
            .map(|room| self.room_apps.get(room).cloned());
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
        // writer's own subscribe). Inferring it from the connecting app would
        // let a foreign first subscriber to a room whose binding was dropped
        // (a dormant sweep, or a store restart that restores the log but not the
        // binding) translate that log along the wrong chain — a durable binding
        // that survives both is the robust fix, not yet built.
        let governing = match room_binding {
            Some(Some((app, version)))
                if matches!(msg, Message::Subscribe { .. } | Message::Ops { .. }) =>
            {
                Some((app, version))
            }
            _ => None,
        };
        let (
            broadcast,
            broadcast_version,
            close,
            room,
            awareness,
            authed_client,
            bind,
            newly_subscribed,
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
                now,
                throttle,
                msg,
            );
            conn.outbox.extend(resp.replies);
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
                resp.awareness,
                authed_client,
                bind,
                newly_subscribed,
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
        // A broadcast holds only ops the hub durably logged (see `Hub::ingest`),
        // so fanning it out never advertises an unpersisted write. Each peer is
        // sent the ops on the channel it opened for the room, so a peer
        // multiplexing several rooms can route what it receives.
        if !broadcast.is_empty() {
            if let Some(room) = room {
                // The room's governing schema gates each peer's read consistently,
                // resolved once (owned) so the peer loop can borrow the conns.
                let schema = self.governing_schema(&room);
                let authorizer = &*self.authorizer;
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
                            Some(chain) => chain.translate_ops(&broadcast),
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
                    for channel in conn.session.channels_for_room(&room) {
                        conn.outbox.push(Message::Ops {
                            channel,
                            ops: ops.to_vec(),
                        });
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
        // Every room-bearing lifecycle event this delivery emitted (a subscribe, a
        // version mutation, a compaction) was recorded by the auto-version sink;
        // act on them now that the delivery has committed.
        self.drain_auto_versions();
        !close
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
        Some(identity) => authorized(
            authorizer,
            schema,
            identity,
            Action::Read,
            &Resource::Room(room),
        ),
        None => false,
    }
}
