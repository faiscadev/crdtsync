//! The connection's protocol driver.
//!
//! A [`Session`] is one client connection. [`step`] sequences the protocol —
//! Hello, then Subscribe, then a stream of Ops — turning each inbound
//! [`Message`] into hub mutations plus a [`Response`]: messages to reply to
//! this client, ops to broadcast to the room's other subscribed channels, and
//! whether to close. Anything out of order is a protocol violation. Pure logic; the
//! async transport drives it.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crdtsync_core::diff::encode_changes;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::zone;
use crdtsync_core::{
    select_codec, BranchInfo, Channel, ClientId, DiffKind, Document, ElementId, ErrorCode, Message,
    Op, OpKind, CODEC_V1, SUPPORTED_CODECS,
};

use crdtsync_core::schema::Schema;

use crate::acl::{
    authorized, doc_acl_tier, doc_acl_write_at, has_any_read_grant, op_read_gate,
    reads_whole_document, recipient_reads_path,
};
use crate::auth::{Identity, Verifier};
use crate::authz::{Action, Authorizer, Decision, Resource};
use crate::membership::Membership;
use crate::schema_registry::{Resolution, SchemaRegistry};
use crate::zonetoken::CrossZoneGrant;
use crate::{Catchup, DiffError, Hub, RoomId, StoredOp, MAIN_BRANCH};

/// A unit of delivery that carries one [`Op`] — what every per-recipient filter
/// keeps or withholds, whether it flows as a bare op (fan-out) or as a log record
/// (catch-up).
pub(crate) trait CarriesOp {
    fn op(&self) -> &Op;
    fn op_mut(&mut self) -> &mut Op;
}

impl CarriesOp for Op {
    fn op(&self) -> &Op {
        self
    }
    fn op_mut(&mut self) -> &mut Op {
        self
    }
}

impl CarriesOp for StoredOp {
    fn op(&self) -> &Op {
        &self.op
    }
    fn op_mut(&mut self) -> &mut Op {
        &mut self.op
    }
}

/// Keep what `keep` admits — by position in `items` and by value — untagging the
/// survivors of every atomic transaction the filter splits.
///
/// This is the shape every per-recipient redaction seam takes — the catch-up
/// delta's read and zone filters, the live fan-out's read filter, the per-channel
/// zone filter — and the destranding is not optional at any of them: a survivor
/// still carrying the group's `count` is buffered at the recipient against a member
/// that will never arrive, invisible to it forever. See
/// [`destrand_split`](crdtsync_core::destrand_split) for why the survivors ride
/// rather than being dropped with their group.
///
/// The reach is this batch: a member already delivered in an earlier one is past
/// untagging, so a group split across two deliveries with the cut in the second
/// leaves the first delivery's members waiting. Evicting a group that can never
/// complete is the recipient's own concern.
pub(crate) fn retain_atomic<T: CarriesOp>(
    items: Vec<T>,
    mut keep: impl FnMut(usize, &T) -> bool,
) -> Vec<T> {
    let verdicts: Vec<bool> = items
        .iter()
        .enumerate()
        .map(|(i, item)| keep(i, item))
        .collect();
    let split = crdtsync_core::split_groups(
        items
            .iter()
            .zip(&verdicts)
            .filter(|(_, keep)| !**keep)
            .map(|(item, _)| item.op()),
    );
    let mut kept: Vec<T> = items
        .into_iter()
        .zip(verdicts)
        .filter_map(|(item, keep)| keep.then_some(item))
        .collect();
    crdtsync_core::destrand_split(kept.iter_mut().map(CarriesOp::op_mut), &split);
    kept
}

/// [`retain_atomic`] over a borrowed batch, cloning only what it keeps — the shape
/// the fan-out seams take, where one batch is filtered once per recipient and
/// cloning what every one of them withholds is the cost of the whole redaction.
pub(crate) fn retain_atomic_cloned<T: CarriesOp + Clone>(
    items: &[T],
    mut keep: impl FnMut(usize, &T) -> bool,
) -> Vec<T> {
    let verdicts: Vec<bool> = items
        .iter()
        .enumerate()
        .map(|(i, item)| keep(i, item))
        .collect();
    let split = crdtsync_core::split_groups(
        items
            .iter()
            .zip(&verdicts)
            .filter(|(_, keep)| !**keep)
            .map(|(item, _)| item.op()),
    );
    let mut kept: Vec<T> = items
        .iter()
        .zip(&verdicts)
        .filter(|(_, keep)| **keep)
        .map(|(item, _)| item.clone())
        .collect();
    crdtsync_core::destrand_split(kept.iter_mut().map(CarriesOp::op_mut), &split);
    kept
}

/// How long a freshly issued cross-zone-move capability token stays valid, in the
/// wall-clock milliseconds the session's `now` carries. A short life is sufficient —
/// a client requests a token and immediately redeems it in the same interaction — and
/// combined with the token's `(actor, element, src, dst)` binding and the op-id dedup
/// it bounds replay to this window.
const CROSS_ZONE_TOKEN_TTL_MILLIS: u64 = 30_000;

/// One channel's subscription: the room it joined, the branch within it, and the
/// zone selector it carries. An empty subscribe branch is normalized to
/// [`MAIN_BRANCH`] here, so every bound channel names a concrete branch and fan-out
/// matches `(room, branch)` exactly.
///
/// The selector is held as the **name** the channel subscribed under, never as the id
/// set it resolves to. A zone id is a *position* in the acting schema's declaration
/// order, and the schema acting over a room moves under a bound channel — a newer
/// client of the app subscribing lifts the governing version, a clone landing
/// re-points a name's binding, a room nothing had bound acquires one — so an id set
/// is a fact about one moment, not about the channel. Every seam that narrows by zone
/// resolves the name through [`acting_zone_scope`] against the schema it is about to
/// narrow with.
#[derive(Clone)]
struct Subscription {
    room: RoomId,
    branch: Vec<u8>,
    /// The zone this channel subscribed to: empty is the whole room — every zone
    /// the actor may read — and a name is that one zone.
    zone: Vec<u8>,
    /// The room read verdict this channel was admitted on — what a per-zone abstain
    /// inherits ([`zone_readable`]). Unlike the selector's resolution it is not
    /// re-taken here, and does not need to be: every caller of
    /// [`acting_zone_scope`] gates the room read itself, ahead of the scope, so a
    /// revoked room read stops the frame before the zone question is asked.
    room_read: bool,
}

/// Whether a subscription scoped to `zones` admits an op in partition `op_zone`:
/// the root partition (unzoned) always, and a zoned op only when its zone is in the
/// authorized set. An unfiltered subscription (`None`) admits everything — the
/// no-zones room and relay path, byte-identical to before zones.
fn zone_admits(zones: &Option<HashSet<u32>>, op_zone: Option<u32>) -> bool {
    match zones {
        None => true,
        Some(set) => match op_zone {
            None => true,
            Some(z) => set.contains(&z),
        },
    }
}

/// The catch-up watermark a reader scoped to `zones` may be told, for a state
/// captured at room sequence `at` on the `(room, branch)` stream, given the `floor`
/// that reader already holds — which is below `at`, since a reader is only handed a
/// state it does not already have.
///
/// A whole-room reader is told `at` — the room's own head — unchanged. A
/// **zone-limited** one is not: `at` counts the whole log, so the difference between
/// two of its readings counts the ops written into partitions this reader is never
/// served, and version names are a room-read fact, so a reader enumerates the
/// captures and charts a hidden partition's write volume from the scalars alone.
/// What it is told instead is [`Hub::partition_head`] — the last sequence in the
/// stream its own zone scope admits — so the number moves only when a partition it can
/// see is written, and within a compaction epoch a window of hidden-only writes reads
/// like an idle one. Across a compaction it can fall to `0`, which is C119's residue:
/// this scalar is the client's resume cursor, so it cannot be refused the way the
/// version seam's can.
///
/// It stays a room sequence, which is what keeps resume working: the client sends it
/// back as `last_seen_seq`, [`Hub::catch_up`] indexes the log with it, and
/// [`read_redirect_response`] compares it against the answering node's committed
/// watermark. The `floor` the reader arrived on is the lower bound — a watermark that
/// went backwards would let a lagging replica serve it a state older than the one it
/// has. Why a room sequence rather than a per-reader count or an opaque token, and why
/// the zone scope rather than the doc-ACL one, is DECISIONS 2026-08-09.
///
/// The gate is [`zone_narrowing`] — the same predicate that decides whether the
/// *state* projection runs — so the scalar and the bytes narrow together, including
/// where that predicate declines. It declines for a channel holding the empty scope
/// against an acting schema declaring no zones, which the op seam does narrow: there
/// the state goes out whole too, so the scalar tells that reader nothing the bytes
/// have not already. That disagreement is C106's, and closing it there closes it here.
fn narrowed_watermark(
    hub: &Hub,
    room: &[u8],
    branch: &[u8],
    schema: Option<&Schema>,
    zones: &Option<HashSet<u32>>,
    at: u64,
    floor: u64,
) -> u64 {
    if zone_narrowing(schema, zones).is_none() {
        return at;
    }
    let served = hub.partition_head(room, branch, at, |op_zone| zone_admits(zones, op_zone));
    served.max(floor)
}

/// One client connection's protocol state. The handshake runs Hello → Auth →
/// Subscribe: the client names itself, then presents a credential the server
/// turns into an [`Identity`] (actor plus roles and groups), then joins rooms. A
/// connection multiplexes several room subscriptions, each on its own
/// [`Channel`]; the client assigns the handle at Subscribe and every later frame
/// names it.
pub struct Session {
    client: Option<ClientId>,
    identity: Option<Identity>,
    channels: HashMap<Channel, Subscription>,
    /// The app named at Hello (empty for a relay connection with no app).
    app_id: Vec<u8>,
    /// The registered schema version this connection is enforced at, resolved at
    /// Hello; `None` for a relay connection (no app, or an unregistered app).
    schema_version: Option<u32>,
    /// The codec this connection speaks, selected at Hello from what the client
    /// advertised; `None` before the handshake settles, and for a handshake
    /// refused because the client shares no codec with this build.
    codec: Option<u32>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            client: None,
            identity: None,
            channels: HashMap::new(),
            app_id: Vec::new(),
            schema_version: None,
            codec: None,
        }
    }

    /// A session already authenticated as `identity` — the upgrade fast path,
    /// where the credential was verified during the transport accept (or
    /// anonymous mode minted the actor), so the in-band Auth phase is skipped.
    /// Hello still names the client; an in-band Auth afterward is out of order.
    pub fn authenticated(identity: Identity) -> Self {
        Self {
            client: None,
            identity: Some(identity),
            channels: HashMap::new(),
            app_id: Vec::new(),
            schema_version: None,
            codec: None,
        }
    }

    /// The client named at Hello, if the handshake is done.
    pub fn client(&self) -> Option<ClientId> {
        self.client
    }

    /// The app this connection named at Hello — empty for a relay connection that
    /// named no app.
    pub fn app_id(&self) -> &[u8] {
        &self.app_id
    }

    /// The registered schema version this connection is enforced at, resolved
    /// against the registry at Hello; `None` for a relay connection (no app, or
    /// an app that never registered a schema).
    pub fn schema_version(&self) -> Option<u32> {
        self.schema_version
    }

    /// The codec this connection speaks, selected at Hello out of what the client
    /// advertised; `None` until the handshake settles. A client that advertised
    /// nothing settles on [`CODEC_V1`], the codec silence carries.
    pub fn codec(&self) -> Option<u32> {
        self.codec
    }

    /// The server-derived actor for this connection, once it is authenticated —
    /// by the in-band Auth phase, the transport-upgrade fast path, or anonymous
    /// mode minting an actor.
    pub fn actor(&self) -> Option<&[u8]> {
        self.identity.as_ref().map(|i| i.actor())
    }

    /// The full identity (actor plus asserted roles and groups) for this
    /// connection, once it is authenticated — by in-band Auth, the fast path, or
    /// anonymous minting.
    pub fn identity(&self) -> Option<&Identity> {
        self.identity.as_ref()
    }

    /// The room this connection has bound to `channel`, if any — the reverse of a
    /// subscribe, for resolving an inbound frame's room from its channel handle.
    pub fn room_for_channel(&self, channel: Channel) -> Option<&RoomId> {
        self.channels.get(&channel).map(|s| &s.room)
    }

    /// The channels this connection has bound to `room`, across every branch. A
    /// room-scoped fan-out (awareness, stranded-peer eviction) reaches each — one
    /// connection may hold the room on more than one channel or branch.
    pub fn channels_for_room(&self, room: &[u8]) -> Vec<Channel> {
        self.channels
            .iter()
            .filter(|(_, s)| s.room == room)
            .map(|(c, _)| *c)
            .collect()
    }

    /// Whether this connection has bound any channel to the `(room, branch)`
    /// stream — the existence question on its own, so a caller asking only
    /// whether a stream has a recipient here neither builds nor drops a channel
    /// list to find out.
    pub fn serves_stream(&self, room: &[u8], branch: &[u8]) -> bool {
        self.channels
            .values()
            .any(|s| s.room == room && s.branch == branch)
    }

    /// The channels this connection has bound to the `(room, branch)` stream. A
    /// branch write fans out on each — the replication unit is `(room, branch)`,
    /// so a write on one branch never reaches another branch's subscribers.
    pub fn channels_for_stream(&self, room: &[u8], branch: &[u8]) -> Vec<Channel> {
        self.channels
            .iter()
            .filter(|(_, s)| s.room == room && s.branch == branch)
            .map(|(c, _)| *c)
            .collect()
    }

    /// The ops from `batch` this `channel`'s subscription admits, filtered to its
    /// authorized zone partitions — the wire-redaction seam for per-zone streams. A
    /// channel scoped to a subset of the room's zones receives only the root
    /// partition's ops plus its authorized zones'; another zone's ops are wholly
    /// omitted, so an unauthorized zone never surfaces on this stream. An unfiltered
    /// channel (a no-zones room, or a relay) takes the whole batch. An unbound
    /// channel admits nothing.
    ///
    /// `schema` is the room's governing schema as it governs now, and the selector is
    /// resolved through it here — so the partitions a channel is served track what the
    /// room declares rather than what was acting when the channel joined. Those differ
    /// even at the first write: a subscribe to a room nothing had bound is admitted
    /// against the connection's own schema, or none at all. An unauthenticated
    /// connection cannot hold a bound channel; if one somehow does, it admits nothing
    /// rather than everything.
    ///
    /// A batch carrying no zoned op is admitted whole without resolving the scope at
    /// all: every scope admits the root partition, so the resolution could only
    /// confirm it. That is the whole of a room declaring no zones and most batches in
    /// one that does, which keeps the per-channel resolution off the paths that have
    /// nothing to narrow.
    pub fn zone_filter(
        &self,
        channel: Channel,
        batch: &[Op],
        authorizer: &dyn Authorizer,
        schema: Option<&Schema>,
    ) -> Vec<Op> {
        let Some(sub) = self.channels.get(&channel) else {
            return Vec::new();
        };
        let Some(identity) = self.identity.as_ref() else {
            return Vec::new();
        };
        // Ordered after the identity check so that check is the backstop its comment
        // claims: an unauthenticated connection admits nothing, whatever the batch.
        if batch.iter().all(|op| op.zone.is_none()) {
            return batch.to_vec();
        }
        let zones = acting_zone_scope(authorizer, identity, schema, sub);
        match &zones {
            None => batch.to_vec(),
            Some(_) => retain_atomic_cloned(batch, |_, op| zone_admits(&zones, op.zone)),
        }
    }

    /// The rooms this connection currently subscribes, one entry per channel —
    /// the same room recurs if held on several channels, so the caller dedups.
    pub fn subscribed_rooms(&self) -> impl Iterator<Item = &RoomId> {
        self.channels.values().map(|s| &s.room)
    }

    /// Drop every channel this connection bound to `room`, returning them — the
    /// eviction counterpart to Unsubscribe. A peer stranded when a write lifts
    /// the room's version past its reach is dropped from the room and must
    /// re-subscribe after updating.
    pub(crate) fn drop_room(&mut self, room: &[u8]) -> Vec<Channel> {
        let channels = self.channels_for_room(room);
        for channel in &channels {
            self.channels.remove(channel);
        }
        channels
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// An ephemeral awareness entry to fan out to a room's other subscribers.
pub struct AwarenessBroadcast {
    pub room: RoomId,
    pub actor: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// What a [`step`] yields: replies to this client, ops to broadcast to the
/// other subscribed channels of `broadcast_room`, an ephemeral awareness entry
/// to fan out, and whether the connection should close. The broadcast fields are
/// `None`/empty when there is nothing to fan out.
#[derive(Default)]
pub struct Response {
    pub replies: Vec<Message>,
    pub broadcast: Vec<Op>,
    pub broadcast_room: Option<RoomId>,
    /// The branch the broadcast ops belong to — the `(room, branch)` stream they
    /// fan out to. `None` when there is nothing to fan out; a `main` write carries
    /// the normalized `main` name, so fan-out never crosses into another branch.
    pub broadcast_branch: Option<Vec<u8>>,
    /// The schema version the broadcast ops were created under — the writing
    /// connection's — so the fan-out translates each op from it to every
    /// recipient's own version. `None` for a relay write (no schema).
    pub broadcast_version: Option<u32>,
    pub awareness: Option<AwarenessBroadcast>,
    pub close: bool,
}

/// Drive one inbound message through the session, mutating the hub and
/// returning what to send and whether to close.
pub fn step(
    hub: &mut Hub,
    session: &mut Session,
    verifier: &dyn Verifier,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    registry: &Mutex<SchemaRegistry>,
    governing: Option<(&[u8], u32)>,
    membership: Option<&Membership>,
    now: u64,
    throttle: Option<u64>,
    msg: Message,
) -> Response {
    match msg {
        Message::Hello {
            client,
            app_id,
            schema_version,
            codecs,
        } => {
            if session.client.is_some() {
                return violation("already said hello");
            }
            // Settle the codec before anything else: a client that shares none
            // with this build cannot be answered in bytes it can read, so it is
            // refused here rather than served a frame it would misdecode.
            let Some(codec) = select_codec(&codecs, SUPPORTED_CODECS) else {
                return Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::UnsupportedVersion,
                        message: "no mutually supported codec".to_string(),
                        details: Vec::new(),
                    }],
                    close: true,
                    ..Response::default()
                };
            };
            // Only a selection that moves off the default is worth a frame — both
            // ends read silence as CODEC_V1 — so the negotiation adds nothing to a
            // connection's reply stream until a second codec exists to select.
            let selection = (codec != CODEC_V1).then_some(Message::CodecSelected { codec });
            // Resolve the app declaration against the registry: a registered app
            // for which the client asked a version the server does not hold is
            // refused and the connection closes; a relay or a known version
            // proceeds, and the enforced version (if any) is recorded. The lock
            // is taken only here — the sole registry read on the data plane — so
            // authentication below never runs under it and cannot stall the admin
            // plane's writes. A poisoned lock is recovered: the read leaves the
            // map intact.
            // An enforcing handshake is answered with the schema the server serves
            // this connection, so a dynamic client that did not bundle can adopt
            // it; a relay names no schema. The resolution carries the registered
            // bytes, so the advertisement needs no second registry read. The lock
            // is the sole registry read on the data plane.
            let resolution = registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .resolve_handshake(&app_id, schema_version);
            let advert = match resolution {
                Resolution::Reject => {
                    return Response {
                        replies: vec![Message::Error {
                            code: ErrorCode::UnsupportedVersion,
                            message: "unknown schema version for this app".to_string(),
                            details: Vec::new(),
                        }],
                        close: true,
                        ..Response::default()
                    };
                }
                Resolution::Relay => {
                    session.schema_version = None;
                    None
                }
                Resolution::Enforcing { version, schema } => {
                    session.schema_version = Some(version);
                    Some(Message::SchemaAdvert {
                        schema_version: version,
                        schema,
                    })
                }
            };
            session.app_id = app_id;
            session.client = Some(client);
            session.codec = Some(codec);
            Response {
                replies: selection.into_iter().chain(advert).collect(),
                ..Response::default()
            }
        }
        Message::Auth { credential } => {
            if session.client.is_none() {
                return violation("auth before hello");
            }
            if session.identity.is_some() {
                return violation("already authenticated");
            }
            // The server derives the identity from the credential; a client never
            // asserts its own identity. A refused credential closes the
            // connection. The credential bytes are never logged.
            match verifier.verify(&credential) {
                Some(identity) => {
                    let actor = identity.actor().to_vec();
                    session.identity = Some(identity);
                    Response {
                        replies: vec![Message::AuthOk { actor }],
                        ..Response::default()
                    }
                }
                None => Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::AuthFailed,
                        message: "credential rejected".to_string(),
                        details: Vec::new(),
                    }],
                    close: true,
                    ..Response::default()
                },
            }
        }
        Message::Subscribe {
            channel,
            room,
            // A subscription is scoped by `(room, branch)` — the replication unit.
            // An empty branch is the default `main`, the whole existing log; a
            // named branch serves the shared base up to its fork point plus its own
            // divergent tail.
            branch,
            // The zone selector picks which of the room's schema-declared partitions
            // the subscription carries. Empty is the whole room (every zone the actor
            // may read); a named zone scopes to that partition alone.
            zone,
            last_seen_seq,
        } => {
            let Some(identity) = session.identity() else {
                return violation("subscribe before auth");
            };
            // A connection authenticated by its transport (an accept-verified
            // credential, mTLS, anonymous mode) holds an identity without having
            // declared a client id, so it reaches here having said nothing. A channel's
            // catch-up is narrowed to the replica identity that channel authors under —
            // a projected snapshot carries the frontier of the ids that identity has
            // published, and nothing else — so the declaration has to precede the
            // channel, not merely the writes.
            if session.client.is_none() {
                return violation("subscribe before hello");
            }
            if session.channels.contains_key(&channel) {
                return violation("channel already subscribed");
            }
            // A read is served by the room's leader, or by a caught-up follower from
            // its own replica (bounded staleness). The follower serves only when it
            // holds the room and its committed watermark is at least the client's
            // `last_seen_seq` floor — the read-your-writes / monotonicity gate;
            // otherwise, and for a room led elsewhere it does not replicate, it
            // redirects to the leader. Single-node (no membership) leads every room.
            if let Some(redirect) =
                read_redirect_response(membership, hub, &room, &branch, last_seen_seq)
            {
                return redirect;
            }
            // A default (empty) subscribe follows the room's active HEAD — `main`
            // until a restore-as-branch switched it — so a plain subscriber tracks
            // the restored state. An explicitly named branch (including `main`) is
            // taken as given, so the old branch stays subscribable by name. The
            // resolved branch is stored on the channel, so a channel bound before a
            // later restore keeps writing to the branch it joined.
            //
            // Resolved before the read gate, because the gate is a verdict about the
            // stream's tree and cannot be reached without knowing which stream that is
            // (C60). Whether the branch *exists* is answered below the gate, so an
            // unauthorized reader still learns only that its read was denied.
            let branch = if branch.is_empty() {
                hub.active_branch(&room)
            } else {
                branch
            };
            // A subscription reads the room; the server never serves a room the
            // actor may not read. The doc-ACL read tier composes at the root: the
            // creator (owns `/`) and a root-level read grant pass here. A
            // subtree-scoped reader abstains at the root, so it is admitted on
            // holding read on *any* subtree — the per-recipient fan-out and catch-up
            // redaction then serve it exactly the subtrees it may read, so subscribe
            // and fan-out never disagree on doc-ACL.
            let records = hub.acl_records(&room);
            let creator = hub.room_creator(&room);
            // The element-context index resolves an element-scoped grant to its
            // element's current path, so a grant follows the element across a
            // tree-move. It is the tree **this stream serves**, built once and shared by
            // every doc-ACL read decision this subscribe makes — the root gate, subtree
            // admission, the per-op catch-up filter, and the whole-document snapshot
            // gate — so they all resolve an element to the same path, and that path is
            // one in the state being handed out (C28, C32, C60). On `main` that is the
            // live room. What it does *not* cover is the reveal-shell synthesis (C85) or
            // the migration type projection (C90) further down, both of which still read
            // the live room. A room with no doc-ACL records has no scopes to resolve and no op
            // to place, so skip the walk entirely — and with it the refusal below, since
            // an unredacted stream needs no tree to serve correctly.
            //
            // A stream with no tree cannot be redacted at all: `main`'s index and an
            // empty one both resolve *less* than the truth, and a scope or an op target
            // that resolves to nothing is admitted rather than withheld. So the branch is
            // refused — after the read gate, which borrows the live room's index purely
            // to keep "read denied" ahead of the refusal, and after the unknown-branch
            // check, which is the other thing that answers `None` here.
            let (index, no_tree) = if records.is_empty() {
                (HashMap::new(), false)
            } else {
                match hub.stream_element_paths(&room, &branch) {
                    Some(index) => (index, false),
                    None => (hub.element_paths(&room), true),
                }
            };
            let root_path = crdtsync_core::path::encode_path(&[]);
            // Whole-document read: the composed verdict at the root — the creator, a
            // root-level grant, or a deployment/schema room-read allow. It also
            // decides whether an unredactable snapshot catch-up may be served (below).
            let whole_doc_read = recipient_reads_path(
                authorizer,
                &records,
                creator.as_deref(),
                &index,
                schema,
                identity,
                &room,
                &root_path,
            );
            // A subtree-scoped reader abstains at the root, so it is admitted on
            // holding read on any subtree — but only where the deployment tier itself
            // abstains: a deployment read-deny stays terminal, so a doc-ACL subtree
            // grant never re-opens a subscription the deployment refused.
            let may_read = whole_doc_read
                || (authorizer.decide(identity, Action::Read, &Resource::Room(&room))
                    == Decision::Abstain
                    && has_any_read_grant(&records, creator.as_deref(), &index, identity));
            if !may_read {
                return forbidden("read denied");
            }
            // A named branch must already exist (forked via the engine) to be
            // served — an unknown one is refused rather than silently served
            // `main`'s stream, which would cross replication units. The default
            // `main` always resolves.
            if branch != MAIN_BRANCH && hub.branch(&room, &branch).is_none() {
                return Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::UnknownRoom,
                        message: "unknown branch".to_string(),
                        details: Vec::new(),
                    }],
                    ..Response::default()
                };
            }
            // The branch exists and this node cannot fold its tree, so the redaction this
            // room's tuples call for cannot be resolved. `Catchup::Unavailable` refuses
            // the below-fork-point catch-up on its own, but a subscriber already past the
            // fork point is served its tail delta — deliberately, since it holds the base
            // already — and that delta has no correct index to be filtered through.
            if no_tree {
                return Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::Internal,
                        message: "branch state is unreadable".to_string(),
                        details: Vec::new(),
                    }],
                    ..Response::default()
                };
            }
            // Zone scoping. A room with declared zones partitions into separately-
            // replicated streams; the selector picks which the subscription carries,
            // each gated independently so an unauthorized zone stays wholly hidden —
            // its ops, state, structure, count, and existence absent from this
            // stream. A refused named-zone subscribe returns the same generic denial
            // as a nonexistent zone, so it never confirms the partition is there.
            let zones = match zone_scope(authorizer, identity, schema, &room, &zone, may_read) {
                Ok(zones) => zones,
                // Every zone refusal is the one generic denial, so a nonexistent zone
                // and an unauthorized one are indistinguishable.
                Err(()) => return forbidden("read denied"),
            };
            // The handshake range-check: a joiner that cannot reach the room's
            // op-version high-water across a back-compatible path is refused with
            // `onUpdateRequired` before it becomes a subscriber, so down-
            // translation at fan-out only ever traverses invertible edges. The
            // high-water is the worst-case op version the merged state embodies,
            // not the sticky governing floor a departed higher-version peer left.
            let high_water = hub.max_op_version(&room);
            if !subscriber_reaches_governing(registry, governing, session, high_water) {
                return Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::UpdateRequired,
                        message: "schema version cannot reach the room's version".to_string(),
                        details: Vec::new(),
                    }],
                    ..Response::default()
                };
            }
            // Resolve the `(room, branch)` stream: `main` is the room's whole log
            // (today's behavior); a named branch is the shared base up to its fork
            // point followed by its divergent tail.
            let catchup = if branch == MAIN_BRANCH {
                hub.catch_up(&room, last_seen_seq)
            } else {
                hub.catch_up_branch(&room, &branch, last_seen_seq)
            };
            let reply = match catchup {
                Catchup::Ops(delta) => {
                    // Replay only the ops this subscriber may read — the same
                    // per-path read authority the live fan-out applies, so a fresh
                    // partial reader catches up on exactly its granted subtrees. A
                    // room with no doc-ACL state replays the delta unchanged. Snapshot
                    // catch-up (a compacted room) replays the materialized state whole:
                    // path redaction there is a state-level projection, not an op
                    // filter, so it rides the snapshot seam rather than this one.
                    let delta = if records.is_empty() {
                        delta
                    } else {
                        // The anchor set of the tree this stream serves, the co-input to
                        // `index` in the same gate — so a range's endpoints resolve in
                        // the tree its ops are being filtered for. It describes the tree
                        // `index` does, having been asked of the same stream, and a
                        // stream with none was refused above — and were that ever to
                        // change, an empty set gates a Ranged op on the whole-document
                        // verdict (C52), which withholds rather than admits.
                        let ranged_anchors = hub
                            .stream_ranged_anchors(&room, &branch)
                            .unwrap_or_default();
                        // The container ids that tree has materialised, live or retained
                        // — what tells an unresolvable target this stream *keeps* from
                        // one that belongs to another. Consulted only for a target
                        // `index` does not resolve, so a delta whose targets all resolve
                        // skips the walk and never reads the empty set it is handed.
                        let unplaceable =
                            delta.iter().any(|rec| !index.contains_key(&rec.op.target));
                        let held = if unplaceable {
                            hub.stream_held_containers(&room, &branch)
                                .unwrap_or_default()
                        } else {
                            HashSet::new()
                        };
                        // The whole-document verdict an unplaceable op is gated on is one
                        // answer for the whole delta, and costs a read verdict per
                        // governing tuple path — so it is resolved at most once, and not
                        // at all for a delta that holds no such op (the common case).
                        let whole_verdict: std::cell::Cell<Option<bool>> =
                            std::cell::Cell::new(None);
                        let reads_whole = || match whole_verdict.get() {
                            Some(v) => v,
                            None => {
                                let v = reads_whole_document(
                                    authorizer,
                                    &records,
                                    creator.as_deref(),
                                    &index,
                                    schema,
                                    identity,
                                    &room,
                                );
                                whole_verdict.set(Some(v));
                                v
                            }
                        };
                        retain_atomic(delta, |_, rec| {
                            // Require-all over the op's governing path set — a Ranged
                            // op's distinct anchor seq paths, one path for every other
                            // op — so a range replays only where both endpoints read.
                            // An op that resolves to no path at all replays only to a
                            // reader denied nothing.
                            op_read_gate(&index, &held, &ranged_anchors, &records, &rec.op).admits(
                                |p| {
                                    recipient_reads_path(
                                        authorizer,
                                        &records,
                                        creator.as_deref(),
                                        &index,
                                        schema,
                                        identity,
                                        &room,
                                        p,
                                    )
                                },
                                reads_whole,
                            )
                        })
                    };
                    // Then keep only the ops in this subscription's authorized zones
                    // — the root partition plus its zones — so a zone-scoped or
                    // partially-authorized whole-room joiner catches up on exactly the
                    // partitions it may read, an unauthorized zone's ops wholly absent.
                    // A no-zones room (`None`) skips the filter, byte-identical to
                    // before zones.
                    let delta = match &zones {
                        Some(_) => retain_atomic(delta, |_, rec| zone_admits(&zones, rec.op.zone)),
                        None => delta,
                    };
                    // Reveal-on-move-in: prepend a shell for every movable node born in
                    // a subtree this reader may not read but whose current position it
                    // may (a node dragged out of a private space into a shared one). The
                    // op stream withholds such a node's create (its birth path is
                    // denied), so without a shell the reader's readable move op could
                    // not materialize it — while the snapshot projection keeps it. The
                    // shells are derived from the same read predicate the snapshot
                    // projection uses, so an op joiner converges with a snapshot joiner.
                    // A whole-document reader (nothing denied) gets none. Shells lead the
                    // delta so the readable move + content ops fold onto them.
                    let delta = if records.is_empty() {
                        delta
                    } else {
                        // A shell is delivered only for a node whose placing move survives
                        // into this delta — so a shell never rides without the move that
                        // would place it (e.g. a move dropped by the zone filter above,
                        // where the node lives in a partition this subscription omits). An
                        // orphan shell would materialize an unplaced node the reader can
                        // never fold, stranding stray registry state; gating on the move's
                        // presence keeps the two in lockstep, at the cost of not revealing
                        // a node whose zone this subscriber cannot read (a doc-ACL × zones
                        // case left out of scope — see DECISIONS).
                        let moved_in_delta: std::collections::HashSet<_> = delta
                            .iter()
                            .filter_map(|rec| match &rec.op.kind {
                                crdtsync_core::OpKind::XmlMove { node, .. } => Some(*node),
                                _ => None,
                            })
                            .collect();
                        let reveals = hub
                            .reveal_ops(
                                &room,
                                crate::acl::recipient_reads_predicate(
                                    authorizer,
                                    &records,
                                    creator.as_deref(),
                                    &index,
                                    schema,
                                    identity,
                                    &room,
                                ),
                            )
                            .into_iter()
                            .filter(|op| match &op.kind {
                                crdtsync_core::OpKind::XmlReveal { node, .. } => {
                                    moved_in_delta.contains(node)
                                }
                                _ => false,
                            });
                        // A reveal shell has no schema-keyed field, so translation is a
                        // no-op on it — tag it relay-style (`None`) so no version rewrite
                        // is attempted.
                        reveals
                            .map(|op| StoredOp::new(op, None))
                            .chain(delta)
                            .collect()
                    };
                    // The owning-element type of each delta op, resolved once over
                    // the room document — a type-scoped migration step narrows to the
                    // ops whose owning element is of its declared type, so the delta
                    // joiner converges with a snapshot joiner. Empty (no narrowing)
                    // when the room binds no schema.
                    let types = schema
                        .map(|s| hub.element_types(&room, s))
                        .unwrap_or_default();
                    Message::Ops {
                        channel,
                        ops: catch_up_ops(registry, governing, session, delta, &types),
                    }
                }
                // A snapshot is the whole materialized replica. A whole-document reader
                // (the creator, a root grant, a deployment read-allow, or any reader of
                // a room with no doc-ACL state) is served it verbatim. A partial reader
                // — read on some subtrees, not the whole document — is served the
                // snapshot *projected* to its authorized subtrees, exactly as the per-op
                // fan-out withholds every op on a subtree it may not read: the two join
                // paths drop the same subtrees, so a snapshot-served joiner converges
                // with an op-served one. Zone scoping then narrows the result further.
                // The subscribe gate already refused a reader with no read grant at all,
                // so a partial reader here holds read on at least one subtree.
                Catchup::Snapshot { seq, state } => {
                    // The replica identity this channel's snapshot is served to — what
                    // the recipient authors under, and so the one author whose ids the
                    // projections keep in the frontier they otherwise scrub.
                    let recipient = session.client.map(|c| c.for_channel(channel.0));
                    // The whole-document gate resolves element-scoped grants against the
                    // tree it is deciding for, and `index` is that tree: this snapshot is
                    // the stream's own state — the live room on `main`, a branch's base
                    // with its divergent tail folded in on a branch — and `index` was
                    // built from that same stream (C32, C60). An element that has left
                    // `main` still resolves on the branch that holds it, so a deny on it
                    // is not inert here.
                    let state = project_served_state(
                        state,
                        authorizer,
                        &records,
                        creator.as_deref(),
                        &index,
                        schema,
                        identity,
                        &room,
                        &zones,
                        recipient,
                    );
                    Message::Snapshot {
                        channel,
                        // The state is the stream's whole head, projected to this
                        // reader's partitions; the sequence it is tagged with is
                        // narrowed to match, so the scalar carries no more about the
                        // partitions withheld than the bytes do.
                        seq: narrowed_watermark(
                            hub,
                            &room,
                            &branch,
                            schema,
                            &zones,
                            seq,
                            last_seen_seq,
                        ),
                        state: catch_up_snapshot(
                            registry, governing, session, high_water, state, schema,
                        ),
                    }
                }
                // The branch owns a base this node cannot decode, so there is no
                // state and no delta over it. Say so and leave the subscriber
                // uncaught-up: an empty delta would tell it it is at the head of a
                // stream it holds none of, and it would then edit from an empty
                // document.
                Catchup::Unavailable => {
                    return Response {
                        replies: vec![Message::Error {
                            code: ErrorCode::Internal,
                            message: "branch state is unreadable".to_string(),
                            details: Vec::new(),
                        }],
                        ..Response::default()
                    }
                }
            };
            // After the catch-up, replay the room's current presence so the
            // joiner sees who is already here without waiting for a republish.
            let mut replies = vec![reply];
            for (actor, key, value) in hub.awareness_entries(&room) {
                replies.push(Message::AwarenessUpdate {
                    channel,
                    actor,
                    key,
                    value,
                });
            }
            session.channels.insert(
                channel,
                Subscription {
                    room,
                    branch,
                    zone,
                    room_read: may_read,
                },
            );
            Response {
                replies,
                ..Response::default()
            }
        }
        Message::Unsubscribe { channel } => {
            if session.actor().is_none() {
                return violation("unsubscribe before auth");
            }
            if session.channels.remove(&channel).is_none() {
                return violation("unsubscribe of an unbound channel");
            }
            Response::default()
        }
        // An ordinary write carries no token, so any cross-zone crossing it
        // contains stays rejected (the token-free path, unchanged).
        Message::Ops { channel, ops } => handle_ops(
            hub, session, authorizer, schema, governing, membership, now, channel, ops, None,
        ),
        // A tokened write redeems a cross-zone capability token: the one cross-zone
        // crossing the token authorizes is admitted, every other check unchanged.
        Message::CrossZoneOps {
            channel,
            ops,
            token,
        } => handle_ops(
            hub,
            session,
            authorizer,
            schema,
            governing,
            membership,
            now,
            channel,
            ops,
            Some(token),
        ),
        Message::CrossZoneToken {
            room,
            element,
            dst_zone,
        } => issue_cross_zone_token(
            hub, session, authorizer, schema, membership, now, room, element, dst_zone,
        ),
        // A token grant only travels server-to-client.
        Message::CrossZoneTokenGrant { .. } => violation("client sent a cross-zone token grant"),
        Message::Snapshot { .. } => violation("client sent a snapshot"),
        Message::Error { .. } => violation("client sent an error"),
        Message::AuthOk { .. } => violation("client sent an authok"),
        Message::SchemaAdvert { .. } => violation("client sent a schema advert"),
        // The codec selection is the server's own answer to an advertisement.
        Message::CodecSelected { .. } => violation("client sent a codec selection"),
        // The client reports its applied sequence; recording it into the
        // per-client GC watermark is the next unit. Until then the report is
        // accepted and ignored rather than treated as a violation — a
        // well-behaved client will send it.
        Message::Ack { .. } => Response::default(),
        // `Accepted` is the server's own reply to an author; a client never sends
        // one.
        Message::Accepted { .. } => violation("client sent an accepted"),
        // `OpsRejected` is the server's own refusal of an author's ops; it only
        // travels server-to-client.
        Message::OpsRejected { .. } => violation("client sent an ops rejected"),
        Message::AwarenessSet {
            channel,
            key,
            value,
        } => {
            let Some(identity) = session.identity() else {
                return violation("awareness before auth");
            };
            let actor = identity.actor().to_vec();
            let Some(client) = session.client else {
                return violation("awareness before hello");
            };
            let Some(room) = session.channels.get(&channel).map(|s| s.room.clone()) else {
                return violation("awareness on an unbound channel");
            };
            // Awareness publish is not yet gated by the doc-ACL tier (the write and
            // read paths are the first cut): the tier abstains, leaving the
            // deployment and schema tiers to decide exactly as before.
            if !authorized(
                authorizer,
                Decision::Abstain,
                schema,
                identity,
                Action::PublishAwareness,
                &Resource::Room(&room),
            ) {
                return forbidden("awareness publish denied");
            }
            // Ephemeral: retained for late-joiner replay and fanned to the room's
            // peers, but never logged or snapshotted. A key dropped at the
            // per-client cap is neither stored nor broadcast; a throttled update
            // arriving inside its window is coalesced — recorded but not fanned out
            // from here (the client SDK's debounce delivers the trailing value).
            let outcome = hub.set_awareness(
                &room,
                client,
                actor.clone(),
                key.clone(),
                value.clone(),
                now,
                throttle,
            );
            if outcome.stored && outcome.broadcast {
                Response {
                    awareness: Some(AwarenessBroadcast {
                        room,
                        actor,
                        key,
                        value,
                    }),
                    ..Response::default()
                }
            } else {
                Response::default()
            }
        }
        // Peer updates and clears only travel server-to-client.
        Message::AwarenessUpdate { .. } => violation("client sent an awareness update"),
        Message::AwarenessClear { .. } => violation("client sent an awareness clear"),
        Message::AwarenessClearKey { .. } => violation("client sent an awareness clear key"),
        // Versioning is a request/response sub-protocol over the channel's room.
        // A mutation replies with the fresh name list — the authoritative
        // post-state — and a list request the same; a fetch that hits replies
        // with the version's state, and one that misses falls back to the list.
        // The whole sub-protocol is served by the room's leader; on a non-leader
        // every frame of it is redirected. A mutation persists to the room, so —
        // like an ops write — a follower that served it would diverge the room's
        // versions. A *read* follows the mutation because a version index is **per
        // node**: replication carries the room's log and never its captures, so a
        // node answers about the captures it took, and a read answered anywhere but
        // where the mutations land reports on a different set from the one the
        // client's own mutations built. Routing it there also puts it on the
        // freshest `acl_records` a fetch redacts by — those tuples ride the log, so
        // a replica's are as old as its last replicated commit — which is a
        // consequence worth having rather than the reason: the live stream that
        // node serves beside it is behind by the same records at the same instant
        // (C33).
        Message::VersionCreate { channel, name } => {
            let Some(room) = bound_room(session, channel) else {
                return channel_request_denied(session, channel, "version");
            };
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            if !channel_authorized(session, authorizer, schema, &room, Action::Write) {
                return channel_request_denied(session, channel, "version");
            }
            match hub.create_version(&room, &name) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionRename { channel, from, to } => {
            let Some(room) = bound_room(session, channel) else {
                return channel_request_denied(session, channel, "version");
            };
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            if !channel_authorized(session, authorizer, schema, &room, Action::Write) {
                return channel_request_denied(session, channel, "version");
            }
            match hub.rename_version(&room, &from, &to) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionDelete { channel, name } => {
            let Some(room) = bound_room(session, channel) else {
                return channel_request_denied(session, channel, "version");
            };
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            if !channel_authorized(session, authorizer, schema, &room, Action::Write) {
                return channel_request_denied(session, channel, "version");
            }
            match hub.delete_version(&room, &name) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionList { channel } => {
            let Some(room) = bound_room(session, channel) else {
                return channel_request_denied(session, channel, "version");
            };
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            if !channel_authorized(session, authorizer, schema, &room, Action::Read) {
                return channel_request_denied(session, channel, "version");
            }
            versions_list(hub, channel, &room)
        }
        Message::VersionFetch { channel, name } => {
            // The identity the redaction below narrows for.
            let Some(identity) = session.identity() else {
                return channel_request_denied(session, channel, "version");
            };
            let Some(room) = bound_room(session, channel) else {
                return channel_request_denied(session, channel, "version");
            };
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            if !channel_authorized(session, authorizer, schema, &room, Action::Read) {
                return channel_request_denied(session, channel, "version");
            }
            match hub.version_state(&room, &name) {
                Some(state) => {
                    let captured_at = hub.version_seq(&room, &name).unwrap_or(0);
                    let state = state.to_vec();
                    // A version is the room's own state at an earlier sequence, so it
                    // carries every partition the room carried — the zones this
                    // channel withholds and the subtrees this reader's doc-ACL denies
                    // included. Narrow it with the same two projections, in the same
                    // order, that a catch-up snapshot runs, so a version read serves
                    // exactly the partitions the live stream does.
                    let records = hub.acl_records(&room);
                    let creator = hub.room_creator(&room);
                    // The channel's zone scope against the acting schema — the same
                    // set the live fan-out filters this channel's ops by, resolved
                    // the same way and at the same moment, so a version read and the
                    // live stream serve the same partitions.
                    let zones = session
                        .channels
                        .get(&channel)
                        .and_then(|sub| acting_zone_scope(authorizer, identity, schema, sub));
                    // A zone-limited channel whose room resolves no schema cannot be
                    // served this state at all: the projection needs the schema to know
                    // where the partitions are, so with none it would hand over the
                    // whole room to the narrowest scope there is. Refused, without
                    // closing — the live stream this channel carries is unaffected,
                    // since an op names its own partition and is filtered on that.
                    if zone_scope_unprojectable(schema, &zones) {
                        return Response {
                            replies: vec![Message::Error {
                                code: ErrorCode::Internal,
                                message: "room schema is unavailable".to_string(),
                                details: Vec::new(),
                            }],
                            ..Response::default()
                        };
                    }
                    // The sequence this capture is tagged with. A version's own
                    // capture point is a room sequence like any other, and a version
                    // *name* is a room-read fact — the `Versions` reply hands them all
                    // over — so an unnarrowed scalar lets a zone-limited reader
                    // enumerate the captures and chart a hidden partition's write
                    // volume across them. Such a reader is told **nothing** instead.
                    //
                    // Not the catch-up seam's answer — the last sequence this reader's
                    // scope admits — because that one is read out of the *retained*
                    // log, and what a compaction has dropped is a function of the whole
                    // room's volume. A capture's answer would then move when hidden
                    // writes alone pushed the floor past this reader's last visible op:
                    // re-reading one fixed capture would flip, which is a signal a
                    // constant does not carry and which the unnarrowed scalar did not
                    // carry either. A catch-up frame has no such option — its scalar is
                    // the client's resume cursor and must be a real sequence — but a
                    // version read feeds no cursor and carries no floor, so refusing
                    // the field outright is available here and is strictly quieter.
                    let seq = if zone_narrowing(schema, &zones).is_some() {
                        0
                    } else {
                        captured_at
                    };
                    // Whether either redaction is configured over these bytes on this
                    // channel at all. A room with no doc-ACL state, read by a channel that
                    // is not zone-limited, is served the captured bytes as it always was,
                    // without paying a decode. This asks what is *configured* — a room
                    // holding any tuple, a channel holding a partial zone set — not what
                    // would actually have been cut: that answer needs the element index,
                    // the index needs the decode, and the decode is what can fail here.
                    let narrowable =
                        !records.is_empty() || zone_narrowing(schema, &zones).is_some();
                    // Element-scoped grants resolve against the *version's* tree: an
                    // element's redaction path is where it stood when the version was
                    // captured, not where it stands in the live room.
                    let index = if narrowable {
                        match Document::decode_state(&state) {
                            Ok(doc) if !records.is_empty() => crate::index::element_paths(&doc),
                            Ok(_) => HashMap::new(),
                            // A version's bytes come back off durable storage, so unlike a
                            // snapshot materialized this instant they can fail to decode.
                            // Undecodable is unprojectable, and the unnarrowed bytes still
                            // carry whatever a redaction would have cut — so say so, without
                            // closing: one archived state is unreadable, the live stream
                            // this channel carries is not.
                            Err(_) => {
                                return Response {
                                    replies: vec![Message::Error {
                                        code: ErrorCode::Internal,
                                        message: "version state is unreadable".to_string(),
                                        details: Vec::new(),
                                    }],
                                    ..Response::default()
                                }
                            }
                        }
                    } else {
                        HashMap::new()
                    };
                    // The version's state is being served — an auditable history read,
                    // distinct from the live subscribe stream. Record it through the audit
                    // seam once it is actually going out (the read was authorized by
                    // `channel_authorized` above, so the verdict is granted).
                    authorizer.observe(identity, Action::VersionRead, &Resource::Room(&room), true);
                    // The replica identity this channel authors under — the one author
                    // whose ids a projection keeps in the frontier it otherwise scrubs,
                    // so a reader that adopts this version does not re-mint into ids
                    // the room's log already holds.
                    let recipient = session.client.map(|c| c.for_channel(channel.0));
                    let state = project_served_state(
                        state,
                        authorizer,
                        &records,
                        creator.as_deref(),
                        &index,
                        schema,
                        identity,
                        &room,
                        &zones,
                        recipient,
                    );
                    Response {
                        replies: vec![Message::VersionState {
                            channel,
                            name,
                            seq,
                            state,
                        }],
                        ..Response::default()
                    }
                }
                None => versions_list(hub, channel, &room),
            }
        }
        // Version responses only travel server-to-client.
        Message::Versions { .. } => violation("client sent a versions list"),
        Message::VersionState { .. } => violation("client sent a version state"),
        // Branch management is a room-keyed request/response sub-protocol. A
        // mutation replies with the fresh branch set — the authoritative
        // post-state — and a list request the same. Like a version mutation, a
        // branch mutation persists to the room, so it is served only by the room's
        // leader; on a non-leader it is redirected rather than persisted, so a
        // follower never diverges the room's branches. A read (list) is served
        // locally from the replicated registry.
        Message::BranchList { room } => {
            if !branch_authorized(session, authorizer, schema, &room, Action::Read) {
                return request_denied(session, "branch");
            }
            branches_list(hub, &room)
        }
        Message::BranchFork {
            room,
            name,
            from_branch,
        } => {
            if !branch_authorized(session, authorizer, schema, &room, Action::Write) {
                return request_denied(session, "branch");
            }
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            // A fork past the source's head is clamped to it, so `u64::MAX` forks at
            // the source branch's current HEAD.
            match hub.fork_branch(&room, &name, &from_branch, u64::MAX) {
                Ok(_) => branches_list(hub, &room),
                Err(_) => internal("failed to persist branch"),
            }
        }
        Message::BranchForkFromVersion {
            room,
            name,
            version,
        } => {
            if !branch_authorized(session, authorizer, schema, &room, Action::Write) {
                return request_denied(session, "branch");
            }
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            match hub.fork_branch_from_version(&room, &name, &version) {
                Ok(_) => branches_list(hub, &room),
                Err(_) => internal("failed to persist branch"),
            }
        }
        Message::BranchRestore {
            room,
            name,
            version,
        } => {
            if !branch_authorized(session, authorizer, schema, &room, Action::Write) {
                return request_denied(session, "branch");
            }
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            match hub.restore_as_branch(&room, &version, &name) {
                Ok(_) => branches_list(hub, &room),
                Err(_) => internal("failed to persist branch"),
            }
        }
        Message::BranchPublish { room, published } => {
            if !branch_authorized(session, authorizer, schema, &room, Action::Write) {
                return request_denied(session, "branch");
            }
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            match hub.publish(&room, &published) {
                Ok(_) => branches_list(hub, &room),
                Err(_) => internal("failed to persist branch"),
            }
        }
        Message::BranchDelete { room, name } => {
            if !branch_authorized(session, authorizer, schema, &room, Action::Write) {
                return request_denied(session, "branch");
            }
            if let Some(redirect) = redirect_response(membership, &room) {
                return redirect;
            }
            match hub.delete_branch(&room, &name) {
                Ok(_) => branches_list(hub, &room),
                Err(_) => internal("failed to persist branch"),
            }
        }
        // A diff query serves a room's own content — a change list carries
        // `core::path`s and the scalar values at them — so it is a state read wearing
        // a different shape, and it is redacted like one. **Channel-keyed** like a
        // version fetch rather than room-keyed like a branch list: the channel is what
        // carries the reader's zone scope, and a room riding the frame leaves a diff
        // nothing to narrow by. Gated by the read tier every channel-keyed room
        // request uses (the doc-ACL tier abstains, deployment and schema decide), then
        // each side is put through `project_served_state` *before* the diff engine
        // sees it — so a change list is the diff of the two states this reader would
        // itself have been served (the causal frontier aside, which the two seams
        // scrub differently and a change list does not carry), and a partition it may
        // not read contributes no change at all rather than a redacted one — with the
        // one exception the projections themselves carry, a *container* the live walk
        // does not reach, which they still serve (C67).
        // A **branch** answer is served locally from the replicated state, with no
        // leader redirect — which is also what makes it a statement about this node's
        // own state (C103). The **version** arm routes; see the gate below.
        // A name the room does not have answers `NotFound`; a branch this node cannot
        // read, and a materialized side that fails to decode, answer `Internal`. None
        // of them closes. A side is archived or reconstructed state — a version's
        // captured bytes, a branch's folded stream, the live replica for `main` — and
        // one of them being unreadable is a server-side fault this channel's live
        // stream survives, which is the reading the version fetch already takes.
        Message::DiffQuery {
            channel,
            kind,
            a,
            b,
        } => {
            // The identity each side's redaction narrows for.
            let Some(identity) = session.identity() else {
                return channel_request_denied(session, channel, "diff");
            };
            let Some(room) = bound_room(session, channel) else {
                return channel_request_denied(session, channel, "diff");
            };
            // A **version** diff names two of this room's captures and puts them
            // through the projection a fetch runs, so it is the leader's on the same
            // terms — the index is per node, so only the node the captures land on
            // can resolve the names at all (C33). A
            // **branch** diff is not routed here: what a replica may answer about a
            // branch, and whether one unservable side refuses the whole query, is
            // C103's question and its own unit's to rule on.
            let routed = match kind {
                DiffKind::Versions => true,
                DiffKind::Branches => false,
            };
            if routed {
                if let Some(redirect) = redirect_response(membership, &room) {
                    return redirect;
                }
            }
            if !channel_authorized(session, authorizer, schema, &room, Action::Read) {
                return channel_request_denied(session, channel, "diff");
            }
            let records = hub.acl_records(&room);
            let creator = hub.room_creator(&room);
            // The channel's zone scope against the acting schema — the same set the
            // live fan-out filters this channel's ops by, and the same one a version
            // fetch on this channel narrows to.
            let zones = session
                .channels
                .get(&channel)
                .and_then(|sub| acting_zone_scope(authorizer, identity, schema, sub));
            // The same refusal the version fetch takes: a zone-limited channel whose
            // room resolves no schema has no projection available, and both sides of a
            // diff are states, so neither can be narrowed to the scope this channel
            // holds.
            if zone_scope_unprojectable(schema, &zones) {
                return Response {
                    replies: vec![Message::Error {
                        code: ErrorCode::Internal,
                        message: "room schema is unavailable".to_string(),
                        details: Vec::new(),
                    }],
                    ..Response::default()
                };
            }
            // Each side is narrowed against its own tree. An element-scoped grant
            // resolves to where that element stood in *that* state, and the two sides
            // of a diff are two different trees — so neither side may be handed the
            // other's index, nor the live room's (C32). Only the read projection's
            // whole-document gate reads the index, so a room holding no doc-ACL state
            // pays no decode to build one.
            let narrow = |state: Vec<u8>| -> Vec<u8> {
                let index = if records.is_empty() {
                    HashMap::new()
                } else {
                    // An empty index where the state does not decode, and no guard for
                    // that case — unlike the version fetch. The projections would hand
                    // such bytes on unnarrowed, but the engine below decodes them again
                    // and refuses the whole query: a diff has no way to serve a side it
                    // cannot read, where a fetch would have served it.
                    Document::decode_state(&state)
                        .map(|doc| crate::index::element_paths(&doc))
                        .unwrap_or_default()
                };
                project_served_state(
                    state,
                    authorizer,
                    &records,
                    creator.as_deref(),
                    &index,
                    schema,
                    identity,
                    &room,
                    &zones,
                    // No recipient. The frontier a projection scrubs is kept back for a
                    // replica that will *author* from the state it is served (C9); a
                    // diff is never adopted as state, and the change list carries no
                    // frontier at all, so the scrub goes whole.
                    None,
                )
            };
            let diff = match kind {
                DiffKind::Versions => hub.diff_versions(&room, &a, &b, narrow),
                DiffKind::Branches => hub.diff_branches(&room, &a, &b, narrow),
            };
            match diff {
                Ok(changes) => {
                    // A change list is captured room content leaving the server — the
                    // same auditable history read a version fetch records, in another
                    // shape — so it goes through the same audit seam, once the content
                    // is actually going out. (Later than the fetch's, which fires
                    // before its projections and so can record a read it then narrows
                    // to nothing; both are on the reply path.) The read was authorized
                    // by `channel_authorized` above, so the verdict is granted.
                    authorizer.observe(identity, Action::VersionRead, &Resource::Room(&room), true);
                    Response {
                        replies: vec![Message::DiffResult {
                            channel,
                            changes: encode_changes(&changes),
                        }],
                        ..Response::default()
                    }
                }
                Err(e) => diff_error(e),
            }
        }
        // A diff result only travels server-to-client.
        Message::DiffResult { .. } => violation("client sent a diff result"),
        // Cloning duplicates the live state of `src` into a fresh room `dst` — a
        // read of the whole source composed with a room create. Two gates compose:
        // the actor must read `src` **whole** ([`reads_source_whole`]) — the clone
        // carries every byte, so anything less launders what the withheld part is
        // redacted by into a room the caller names — and the create is a
        // room-management mutation on `dst`, gated by the write tier a branch
        // mutation uses. A create persists a new room, so like a branch
        // mutation it is served only by `dst`'s leader; on a non-leader it is
        // redirected rather than persisted. The clone reads `src`'s state from the
        // node that persists `dst`, so it is served only where one node leads both:
        // a replica *holds* `src` and would export it happily, but a replica's view
        // of the inputs the gate decides on is a follower's — its governing binding
        // is per-node, so the source's zone declarations read as absent there — and
        // its ACL records are only as fresh as its last replicated commit. A node
        // that does not lead `src` reports `created == false` rather than cloning
        // from a copy it does not own; cross-leader clone is a cross-node state
        // transfer this single-hub primitive does not do. The reply is a
        // `CloneRoomResult` whose `created` is false when the clone was a no-op
        // (`src` absent or not led here, or `dst` already present).
        Message::CloneRoom { src, dst } => {
            // Authentication, then routing, then the gate. A node that will not clone
            // must not decide the gate at all: its records, creator and binding for
            // `src` are a follower's, so the verdict would be computed from state it
            // is not authoritative for — and `Authorizer::observe` would record a read
            // of a room it does not serve.
            if session.identity().is_none() {
                return request_denied(session, "clone");
            }
            if let Some(redirect) = redirect_response(membership, &dst) {
                return redirect;
            }
            if redirect_if_not_leader(membership, &src).is_some() {
                return Response {
                    replies: vec![Message::CloneRoomResult {
                        dst,
                        created: false,
                    }],
                    ..Response::default()
                };
            }
            if !reads_source_whole(hub, session, authorizer, schema, &src)
                || !branch_authorized(session, authorizer, schema, &dst, Action::Write)
            {
                return request_denied(session, "clone");
            }
            match hub.clone_room(&src, &dst) {
                Ok(created) => Response {
                    replies: vec![Message::CloneRoomResult { dst, created }],
                    ..Response::default()
                },
                Err(_) => internal("failed to persist clone"),
            }
        }
        // A clone result only travels server-to-client.
        Message::CloneRoomResult { .. } => violation("client sent a clone result"),
        // A branch set only travels server-to-client.
        Message::Branches { .. } => violation("client sent a branch set"),
        // A redirect is the server's own routing reply; a client never sends one.
        Message::Redirect { .. } => violation("client sent a redirect"),
        // Replication frames travel node-to-node between replicas, on a connection
        // that has presented the cluster secret — the registry handles them there,
        // off the client session path. One arriving here came from a connection with
        // no claim on the peer plane, so it is the protocol violation it looks like.
        Message::Replicate { .. } => violation("client sent a replicate"),
        Message::ReplicaAck { .. } => violation("client sent a replica ack"),
        Message::ReplicateSnapshot { .. } => violation("client sent a replicate snapshot"),
        Message::FollowerHeads { .. } => violation("client sent follower heads"),
        // Gossip is a node-to-node membership advertisement the registry handles
        // off the client session path; a client that sends one violates.
        Message::Gossip { .. } => violation("client sent a gossip"),
        // Ping-req/-ack are node-to-node SWIM indirect-probe frames the transport
        // services off the client session path; a client that sends one violates.
        Message::PingReq { .. } => violation("client sent a ping-req"),
        Message::PingAck { .. } => violation("client sent a ping-ack"),
        // The registry answers a peer-plane admission itself, ahead of the session
        // step, so one reaching a session came from a caller driving `step` directly
        // — never the client data plane, where it is a violation like the rest.
        Message::PeerAuth { .. } => violation("client sent a peer auth"),
    }
}

/// Fold an op batch into `channel`'s room — the shared body of the plain
/// [`Message::Ops`] path (`token` `None`) and the tokened
/// [`Message::CrossZoneOps`] path (`token` `Some`). Every gate but the cross-zone
/// one is identical across the two: a cross-zone crossing is admitted only when a
/// valid capability token authorizes exactly it, so an ordinary write (no token)
/// keeps the crossing rejected, and a tokened write admits precisely the move its
/// token binds.
#[allow(clippy::too_many_arguments)]
fn handle_ops(
    hub: &mut Hub,
    session: &Session,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    governing: Option<(&[u8], u32)>,
    membership: Option<&Membership>,
    now: u64,
    channel: Channel,
    ops: Vec<Op>,
    token: Option<Vec<u8>>,
) -> Response {
    if session.actor().is_none() {
        return violation("ops before auth");
    }
    let Some(client) = session.client else {
        return violation("ops before hello");
    };
    let Some(Subscription { room, branch, .. }) = session.channels.get(&channel).cloned() else {
        return violation("ops on an unbound channel");
    };
    // Every op must carry the replica identity this channel authors under —
    // `for_channel` of the client declared at Hello — so a connection's ops stay
    // self-consistent and each channel keeps its own op-id space. Two channels of
    // one connection can be bound to the same room, and identities shared across
    // them would make one channel's ops dedup away as duplicates of the other's.
    let authoring = client.for_channel(channel.0);
    if ops.iter().any(|op| op.id.client != authoring) {
        return violation("op client mismatch");
    }
    // The node's own replica identity is reserved: no op carrying it may enter a
    // room's log. Channel 0 authors under the declared Hello id unchanged, and a
    // node's id is a fixed, publicly guessable constant rather than the 122 random
    // bits a client's is drawn from — so it is the one identity a client can write
    // under on purpose, into the very replica whose `encode_state` rides every
    // catch-up snapshot the node serves. The reservation sits here rather than at
    // the handshake because a node-to-node link legitimately says Hello under its
    // own node id; it is authorship, not the declaration, that has to be refused.
    // It covers the client write path — every `Ops` and `CrossZoneOps` batch
    // reaches the log through here. A node-to-node `Replicate` frame is a separate
    // ingest seam with its own (absent) authentication, not this gate's to close.
    if authoring == hub.replica_identity() {
        return violation("ops authored under the node's replica identity");
    }
    // An `XmlReveal` is a redaction-time synthesis the server injects into a partial
    // reader's stream — never an authored op. A client that submits one is rejected
    // outright: applied to the authoritative document it would inject an unplaced,
    // arbitrarily-identified node shell, so it must never enter the log. A
    // well-behaved client never authors one, so this is a protocol violation, not a
    // recoverable authz refusal.
    if ops
        .iter()
        .any(|op| matches!(op.kind, OpKind::XmlReveal { .. }))
    {
        return violation("client authored a reveal op");
    }
    // An op no replica can hold is refused here, so the batch is never logged,
    // deduped, fanned out or acknowledged. `Document::apply` refuses it whatever
    // path it arrives by, and the ingest seam drops it; what this gate adds is the
    // *answer to the author* — without it the write is acked `Accepted` (the ack
    // frontier is a max over the whole submitted batch), the client prunes it from
    // its outbox, and the edit is lost with nothing reported anywhere.
    //
    // Recoverable rather than a disconnect: the frame is well-formed and the rest of
    // the connection's traffic is unaffected, so the author keeps its ops and
    // surfaces the rejection, as an authz or schema refusal does. It refuses the
    // whole batch, not the offending op — the ack frontier covers the batch, so
    // acking the admissible half would acknowledge the refused op's sequence with
    // it.
    //
    // The judgement is a pure function of the op, so every replica refuses exactly
    // the same set: rejecting converges the room on the op's absence rather than
    // splitting it, which is what makes refusing safe where dropping state would not
    // be. Nothing this codebase mints is inadmissible, so an honest client never
    // meets this.
    if !ops.iter().all(Op::is_admissible) {
        return ops_rejected(channel, &ops, ErrorCode::MalformedOp);
    }
    // A write is served only by the room's leader. A subscribe to a non-led room is
    // already redirected, so a bound channel here implies leadership; the guard still
    // holds if a write reaches a non-leader — it is redirected, not ingested, so a
    // follower never folds a stray write.
    if let Some(redirect) = redirect_response(membership, &room) {
        return redirect;
    }
    let identity = session.identity().expect("identity set, checked above");
    // A replica identity belongs to the authenticated actor that first wrote under
    // it in this room, and no other actor may author under it afterwards. Without
    // this the `ClientId` is a bare declaration — Hello names it and nothing binds
    // it — and the declaration is enough to reach the one place it does lasting
    // damage: a stamp names its author, so an op admitted under a victim's identity
    // raises *that* replica's id-space high-water, and one op stamped at the top of
    // the space spends it outright, on every replica that folds the op and in the
    // durable snapshot. The mint's refusal is fail-closed rather than a re-issued
    // live id, so the victim does not diverge — it simply can never write again.
    //
    // The claim is per room, because the damage is: an id space is a property of a
    // document, and a replica identity is spent in the room whose replica holds the
    // stamp. It is established by the writer's own first batch below, so an honest
    // client claims what it uses on its way in and nothing has to be provisioned.
    //
    // Refused recoverably, not as a protocol violation. Ownership is server-side
    // state the client cannot observe, and it is decided per node — a batch this
    // node refuses is one another may hold the claim for, and a client whose actor
    // legitimately changed has no rotation to reach for (C94). The two violations
    // above are about the batch's consistency with the client's *own* declaration,
    // which the client can always check; this one is not, so it keeps the connection
    // and leaves the author holding its ops. An empty batch is exempt: it authors no
    // stamp, so it can spend no id space, and it is what an inert edit frames.
    if !ops.is_empty() {
        if let Some(owner) = hub.client_actor(&room, authoring) {
            if Some(owner) != session.actor() {
                return ops_rejected(channel, &ops, ErrorCode::Forbidden);
            }
        }
    }
    // The doc-ACL tuple tier gates the write between the deployment and schema tiers:
    // the room creator owns `/`, and its grants let others in. A first write to a
    // fresh room finds no creator and no tuples, so the tier abstains and the
    // deployment/schema tiers bootstrap it; that authorized first writer then becomes
    // the creator (below). Element scopes resolve through the room's element-context
    // index; a room with no doc-ACL records has none to resolve, so skip the tree walk.
    let records = hub.acl_records(&room);
    let index = if records.is_empty() {
        HashMap::new()
    } else {
        hub.element_paths(&room)
    };
    let doc_acl = doc_acl_tier(
        &records,
        hub.room_creator(&room).as_deref(),
        &index,
        identity,
        Action::Write,
    );
    if !authorized(
        authorizer,
        doc_acl,
        schema,
        identity,
        Action::Write,
        &Resource::Room(&room),
    ) {
        // Authored ops sit in the client's outbox until acknowledged, so a refusal
        // must be recoverable rather than a connection close: name the rejected ops,
        // keep the connection open, ingest and ack nothing. The client drains them
        // from its outbox and surfaces the rejection for the app to show, discard, or
        // export.
        return ops_rejected(channel, &ops, ErrorCode::Forbidden);
    }
    // A published branch is a read-only publish target — its HEAD is advanced only by
    // `publish`, never by a client write. Refuse recoverably, as the authz denial
    // above does: the author keeps its ops and surfaces the rejection rather than
    // losing the connection.
    if hub.is_published(&room, &branch) {
        return ops_rejected(channel, &ops, ErrorCode::Forbidden);
    }
    // A cross-zone tree move is inadmissible by default: the per-zone clocks never
    // order across zones, and the crossing is not detectable from the post-move tree,
    // so it is caught here at the op against the room's pre-move document. The one
    // authorized bypass is a server-sealed capability token that authorizes exactly
    // one crossing — so the crossing is admitted only when the batch carries a
    // token whose sealed binding matches its actual `(actor, element, src, dst)` move
    // and has not expired; an un-tokened, forged, expired, or mismatched crossing
    // stays rejected recoverably, the op never entering the log so every replica
    // converges on its absence. Gated to `main`: the enforcement resolves against the
    // room's materialized document, which a branch's divergent tree is not part of, so
    // branch-scoped move enforcement waits on the per-zone stream work that models
    // branch/zone interaction.
    if branch == MAIN_BRANCH {
        if let Some(schema) = schema {
            // A room whose state cannot be simulated yields no verdict, and the
            // gate is a reject boundary: refuse the batch rather than read the
            // missing verdict as "crosses nothing".
            let Some(crossings) = hub.batch_zone_crossings(&room, &ops, schema) else {
                return ops_rejected(channel, &ops, ErrorCode::Internal);
            };
            if !crossings.is_empty()
                && !cross_zone_move_authorized(
                    hub,
                    schema,
                    identity.actor(),
                    &room,
                    token.as_deref(),
                    &crossings,
                    now,
                )
            {
                return ops_rejected(channel, &ops, ErrorCode::Forbidden);
            }
            // The enforcing tier is the authoritative reject boundary: a batch that
            // would introduce a runtime-kind mismatch at a declared slot — the one
            // unrepairable-and-inadmissible schema violation — is refused at ingress.
            // The op never enters the log, so every replica converges on its absence;
            // the author keeps its ops and surfaces the rejection. A relay connection
            // carries no schema (`None`), so a relay tier never validates — it passes
            // the batch through unvalidated. The repairable violations are not checked
            // here (they are folded away convergently at read), and an undeclared map
            // slot is admissible (a Map is an open container).
            if hub.batch_violates_schema(&room, &ops, schema) {
                return ops_rejected(channel, &ops, ErrorCode::SchemaViolation);
            }
        }
    }
    // The batch's highest per-client op sequence: the frontier the author is
    // acknowledged through once the ops are durably logged, so it can prune its
    // outbox. Computed over the whole submitted batch, not just the fresh ops, so a
    // resent op the hub already holds is still acked and pruned. An empty batch
    // acknowledges nothing.
    let through = ops.iter().map(|op| op.id.seq).max();
    // The op's creation version is recorded only when the writer speaks the room's
    // governing app — its version number lives in that app's space. A foreign-app
    // writer's version is a different space and must never drive this room's chain, so
    // its ops are logged untagged (`None`, relay-like) and pass verbatim on both the
    // live and the catch-up seam, exactly as the fan-out already leaves them.
    let write_version = governing_target(governing, session).map(|(_, _, client)| client);
    // The deduped ops fan out to the `(room, branch)` stream's other subscribed
    // replicas — the authoring channel's own excepted, its connection's other
    // channels included. A `main` write appends to the room's log as
    // today; a branch write appends to that branch's divergent tail, advancing its
    // head, never main's. A hub that cannot durably record the ops rejects the write
    // rather than advertising an unpersisted one.
    let applied = if branch == MAIN_BRANCH {
        let applied = hub.ingest(&room, ops, write_version);
        // The first authenticated actor to write a room establishes it, so it becomes
        // the room's creator — the doc-ACL authority root that owns `/`. Set-once and
        // authenticated-only, both decided by `ensure_creator` so a root arriving over
        // a replication frame is judged by the same rule. A branch write presupposes
        // an already-established (forked) room, so it never bootstraps a creator.
        if applied.is_ok() {
            hub.ensure_creator(&room, identity.actor());
        }
        applied
    } else {
        hub.ingest_branch(&room, &branch, ops, write_version)
    };
    // A written batch claims the replica identity it was authored under, so the gate
    // above has an owner to compare the next actor against. It runs on both branches
    // of the write: a branch has its own log and its own materialised replica, and a
    // stamp planted through one is in the room's id space just the same.
    //
    // Only a batch that actually landed an op claims. An empty one authors no stamp,
    // and one the room deduped away authors nothing *new* — a replay of another
    // replica's historic ops would otherwise let whoever resends them take the
    // identity that wrote them, which is the lockout the claim exists to prevent
    // reached from the other side.
    if matches!(&applied, Ok(landed) if !landed.is_empty()) {
        hub.claim_client(&room, authoring, identity.actor());
    }
    match applied {
        Ok(applied) => Response {
            replies: through
                .map(|through| Message::Accepted { channel, through })
                .into_iter()
                .collect(),
            broadcast: applied,
            broadcast_room: Some(room),
            broadcast_branch: Some(branch),
            broadcast_version: write_version,
            ..Response::default()
        },
        Err(_) => Response {
            replies: vec![Message::Error {
                code: ErrorCode::Internal,
                message: "failed to persist ops".to_string(),
                details: Vec::new(),
            }],
            close: true,
            ..Response::default()
        },
    }
}

/// Whether every cross-zone crossing a batch performs is authorized by `token` — the
/// redemption check. A valid token opens (under the room's zone key) to a binding
/// that matches the crossing's actual `(room, actor, element, src zone, dst zone)`
/// and has not expired. Every crossing must be authorized by the one token: a single
/// token binds exactly one move, so a batch straddling more than one distinct
/// crossing cannot be authorized by it. Fail-closed at every ambiguous case — no
/// token, no zone key, or a forged / expired / mismatched token yields `false`, so
/// the crossing stays rejected exactly as an un-tokened one.
fn cross_zone_move_authorized(
    hub: &Hub,
    schema: &Schema,
    actor: &[u8],
    room: &[u8],
    token: Option<&[u8]>,
    crossings: &[crate::index::ZoneCrossing],
    now: u64,
) -> bool {
    let Some(token) = token else {
        return false;
    };
    let Some(grant) = hub.open_cross_zone_token(token) else {
        return false;
    };
    if !grant.is_live(now) {
        return false;
    }
    crossings.iter().all(|c| {
        grant.authorizes(
            room,
            actor,
            c.node,
            &zone_name(schema, c.from),
            &zone_name(schema, c.to),
        )
    })
}

/// The name bytes of the zone with compact id `id` — the schema's declared zone name
/// for a zoned partition, or the empty string for the unzoned root partition
/// (`None`). An out-of-range id (never produced by the crossings, which resolve
/// under the same schema) maps to the root, so the match fails closed.
fn zone_name(schema: &Schema, id: Option<u32>) -> Vec<u8> {
    match id {
        Some(i) => schema
            .zones()
            .get(i as usize)
            .map(|(name, _)| name.as_bytes().to_vec())
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

/// Issue a cross-zone-move capability token for `room`: ACL-authorize the request
/// and, if allowed, seal the token binding `(room, actor, element, src zone, dst
/// zone, expiry)`. The actor must hold **move authority on the element** (write where
/// the element currently lives) *and* **write authority to the destination zone** —
/// both composed through the same [`authorized`] evaluator the write gate uses, never
/// a parallel check. Fail-closed: an unresolvable element, an undeclared destination
/// zone, a denied authority, or no configured zone key all mint no token and answer
/// `Forbidden`. Served by the room's leader, whose authoritative document the element
/// path and ACL resolve against.
#[allow(clippy::too_many_arguments)]
fn issue_cross_zone_token(
    hub: &Hub,
    session: &Session,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    membership: Option<&Membership>,
    now: u64,
    room: Vec<u8>,
    element: ElementId,
    dst_zone: Vec<u8>,
) -> Response {
    let Some(identity) = session.identity() else {
        return violation("cross-zone token request before auth");
    };
    if let Some(redirect) = redirect_response(membership, &room) {
        return redirect;
    }
    // A relay / zoneless connection declares no zones, so there is nothing to cross —
    // nothing to authorize, fail-closed.
    let Some(schema) = schema else {
        return forbidden("cross-zone token denied");
    };
    // The element's current location fixes its source zone; an element the room's
    // index does not hold (unknown or already deleted) cannot be moved — deny.
    let paths = hub.element_paths(&room);
    let Some(element_segs) = paths.get(&element) else {
        return forbidden("cross-zone token denied");
    };
    let element_path = encode_path(&element_segs.iter().map(Vec::as_slice).collect::<Vec<_>>());
    let src_zone = zone::zone_of(schema, element_segs)
        .map(|name| name.as_bytes().to_vec())
        .unwrap_or_default();
    // The destination is the unzoned root (empty selector) or a declared zone; an
    // undeclared name names no partition, so deny rather than mint a token for a
    // destination that does not exist. The destination's authority path is that
    // partition's subtree root.
    let dst_path = if dst_zone.is_empty() {
        encode_path(&[])
    } else {
        let Ok(dst_name) = std::str::from_utf8(&dst_zone) else {
            return forbidden("cross-zone token denied");
        };
        let Some(keys) = zone::zone_root_keys(schema, dst_name) else {
            return forbidden("cross-zone token denied");
        };
        encode_path(&keys.iter().map(Vec::as_slice).collect::<Vec<_>>())
    };
    let records = hub.acl_records(&room);
    let creator = hub.room_creator(&room);
    // Move authority on the element: the actor may write where the element lives,
    // composed through the deployment / doc-ACL / schema tiers exactly as the write
    // gate composes, at the element's own path.
    let move_ok = authorized(
        authorizer,
        doc_acl_write_at(
            &records,
            creator.as_deref(),
            &paths,
            identity,
            &element_path,
        ),
        Some(schema),
        identity,
        Action::Write,
        &Resource::Room(&room),
    );
    // Write authority to the destination: a named zone gates on `Resource::Zone`, the
    // unzoned root on the room itself; the doc-ACL tier evaluates write at the
    // destination subtree's root path.
    let dst_resource = if dst_zone.is_empty() {
        Resource::Room(&room)
    } else {
        Resource::Zone {
            room: &room,
            zone: &dst_zone,
        }
    };
    let dst_ok = authorized(
        authorizer,
        doc_acl_write_at(&records, creator.as_deref(), &paths, identity, &dst_path),
        Some(schema),
        identity,
        Action::Write,
        &dst_resource,
    );
    if !(move_ok && dst_ok) {
        return forbidden("cross-zone token denied");
    }
    let grant = CrossZoneGrant {
        room: room.clone(),
        actor: identity.actor().to_vec(),
        element,
        src_zone,
        dst_zone,
        expiry: now.saturating_add(CROSS_ZONE_TOKEN_TTL_MILLIS),
    };
    // Seal binds the whole tuple under the zone key; with no key configured no token
    // can be minted (the escape hatch is off) — fail-closed.
    match hub.seal_cross_zone_token(&grant) {
        Some(token) => Response {
            replies: vec![Message::CrossZoneTokenGrant { room, token }],
            ..Response::default()
        },
        None => forbidden("cross-zone token denied"),
    }
}

/// Resolve a subscribe's zone selector into the set of zone partitions the
/// subscription carries, or a refusal. `None` is no filtering — a room that
/// declares no zones (or a relay with no schema): one implicit root partition,
/// byte-identical to a zoneless subscribe. `Some(set)` scopes the stream; an op is
/// carried only if it is unzoned (the root partition) or its zone id is in the set.
///
/// An empty selector is the whole room: every zone the actor may read, collected by
/// gating each declared zone; an unreadable zone is silently omitted, so a
/// partially-authorized whole-room subscriber sees its authorized zones and nothing
/// of the rest. A named selector scopes to one zone, gated the same way — an unknown
/// name and an unauthorized zone are both answered with one generic denial, so a
/// refusal never reveals whether the zone exists.
fn zone_scope(
    authorizer: &dyn Authorizer,
    identity: &Identity,
    schema: Option<&Schema>,
    room: &[u8],
    zone: &[u8],
    room_read: bool,
) -> Result<Option<HashSet<u32>>, ()> {
    let Some(schema) = schema.filter(|s| !s.zones().is_empty()) else {
        // No declared zones: a whole-room subscribe carries the one implicit
        // partition unfiltered; a named-zone subscribe selects a partition that does
        // not exist — refuse, so a zoneless room is indistinguishable from one that
        // hides the named zone.
        return if zone.is_empty() { Ok(None) } else { Err(()) };
    };
    if zone.is_empty() {
        let set = schema
            .zones()
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| {
                zone_readable(authorizer, identity, room, name.as_bytes(), room_read)
            })
            .map(|(i, _)| i as u32)
            .collect();
        Ok(Some(set))
    } else {
        let Some(id) = schema
            .zones()
            .iter()
            .position(|(name, _)| name.as_bytes() == zone)
        else {
            return Err(());
        };
        if !zone_readable(authorizer, identity, room, zone, room_read) {
            return Err(());
        }
        Ok(Some(HashSet::from([id as u32])))
    }
}

/// The zone partitions `sub` admits against the schema acting over its room: its
/// selector resolved to ids, each id gated on the deployment's per-zone read verdict.
/// **The one reading every seam that narrows by zone takes** — the live fan-out, the
/// version fetch, each side of a diff query — so one channel's three answers are one
/// answer, resolved against the schema each is about to narrow with rather than
/// against whichever schema was acting when the channel joined.
///
/// A **named** selector the acting schema does not declare, or whose read the
/// deployment now denies, narrows to the **empty set**: the root partition alone. Not
/// `None`, which is "do not filter" and would serve the whole room on the one input
/// that has become unanswerable. Where the acting schema declares no zones the room
/// has a single implicit partition, so the empty set carries every op that partition
/// holds — a name against a block that does not declare it is narrowed to the only
/// partition there is, not to nothing. Within one registered chain that case cannot
/// arise at all: a `zones` block may only be extended.
///
/// That reading is about the *acting* schema, not about the log. A room whose acting
/// schema declares no zones can still hold ops stamped in a partition an earlier
/// binding declared, and those the empty set drops while a state read of the same room
/// serves them — the same two-seams disagreement this guard closes for a `None` schema,
/// left open for a zero-zone one and filed as **C106**.
///
/// The **whole-room** selector against no acting schema is the one input this cannot
/// answer: it is `None`, and a room genuinely declaring no partitions is
/// indistinguishable here from one whose schema this node cannot resolve (C101, C62).
fn acting_zone_scope(
    authorizer: &dyn Authorizer,
    identity: &Identity,
    schema: Option<&Schema>,
    sub: &Subscription,
) -> Option<HashSet<u32>> {
    zone_scope(
        authorizer,
        identity,
        schema,
        &sub.room,
        &sub.zone,
        sub.room_read,
    )
    .unwrap_or(Some(HashSet::new()))
}

/// Whether a scope names a narrowing that cannot be performed against `schema`. A
/// `Some` scope is the claim that this channel sees a subset of the room's partitions,
/// and cutting a *state* down to that subset needs the schema that defines where they
/// are — so with no schema [`zone_narrowing`] declines, the projection is skipped, and
/// the whole state goes out.
///
/// The op seam has an answer that needs no schema: an op carries its partition in its
/// envelope, so the empty set drops every zoned op and keeps the root. A state carries
/// no such marking — the partitions are positions in a tree the schema names — so the
/// two seams cannot agree here by narrowing, only by the state read refusing. A
/// channel that named a zone against a schema that no longer resolves is exactly that
/// case: [`acting_zone_scope`] hands it the empty set, and serving it a state whole
/// would be the widest possible reading of the narrowest possible scope.
///
/// Reached in three frames on one node, with no registry lag and no restart: a
/// subscribe binds a room name *before* anything materializes under it, so a channel
/// is admitted to a zone against the **connection's** schema and the name binds to it;
/// a clone from an **ungoverned** source then removes that binding outright
/// ([`Hub::clone_room`](crate::Hub::clone_room)'s `None` arm, which exists so a caller
/// cannot pick the schema its own clone is read under); and every later frame on that
/// name resolves no schema, since the connection's-own fallback is a subscribe-only
/// rule. The channel then names a partition of a room that declares none, while the
/// clone has filled it with content that partition never held.
fn zone_scope_unprojectable(schema: Option<&Schema>, zones: &Option<HashSet<u32>>) -> bool {
    schema.is_none() && zones.is_some()
}

/// Whether `identity` may read the zone named `zone` in `room`. A deployment that
/// explicitly allows or denies the [`Resource::Zone`] decides; one that abstains
/// inherits the room read verdict — a zone is visible by default within a readable
/// room, and an explicit per-zone deny is what carves out an isolated partition.
fn zone_readable(
    authorizer: &dyn Authorizer,
    identity: &Identity,
    room: &[u8],
    zone: &[u8],
    room_read: bool,
) -> bool {
    match authorizer.decide(identity, Action::Read, &Resource::Zone { room, zone }) {
        Decision::Allow => true,
        Decision::Deny => false,
        Decision::Abstain => room_read,
    }
}

/// Narrow a whole-replica state to what one recipient may read: the doc-ACL path
/// projection, then the zone projection, in that order. **The one composition every
/// seam that hands a client a state blob runs** — the subscribe catch-up snapshot, the
/// named-version fetch, and each side of a diff query — so a redaction added to it
/// cannot reach one seam and miss the others, which is the failure it exists to answer.
///
/// Read-then-zone is the order the catch-up seam has always used, preserved verbatim
/// by the extraction rather than re-derived. It is not a containment property: running
/// the zone projection first is *more* redactive, not less, since a zone-purged node
/// leaves no placement for the read projection's reveal rule to un-purge. The rationale
/// for keeping it is that the reveal rule's decision belongs against the whole document,
/// as the op fan-out resolves it — but no test today distinguishes the two orders, so
/// treat that as the reason it was chosen, not as an invariant something enforces.
///
/// `index` resolves element-scoped grants to paths for the *gate* — the whole-document
/// verdict that decides whether the read projection runs at all. The projection itself
/// always derives its own index from the state it is about to narrow, so only the gate
/// can be fed an index from a different tree than the bytes. It must not be: an element
/// scope that resolves to no path is inert, an inert deny is no deny, and a gate that
/// finds none serves the state whole. Every caller passes the tree its own bytes are:
/// the version fetch the version's, each diff side its own decoded state, the catch-up
/// snapshot the tree of the stream it is catching up on — the live room on `main`, a
/// branch's base with its divergent tail folded in on a branch (C32, C60).
/// `records.is_empty()` (a room with no doc-ACL state) or a whole-document verdict
/// skips the read projection; [`zone_narrowing`] decides the zone one.
#[allow(clippy::too_many_arguments)]
fn project_served_state(
    state: Vec<u8>,
    authorizer: &dyn Authorizer,
    records: &[crdtsync_core::acl::AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    schema: Option<&Schema>,
    identity: &Identity,
    room: &[u8],
    zones: &Option<HashSet<u32>>,
    recipient: Option<ClientId>,
) -> Vec<u8> {
    let reads_all = records.is_empty()
        || reads_whole_document(authorizer, records, creator, index, schema, identity, room);
    let state = if reads_all {
        state
    } else {
        project_snapshot_reads(
            state, authorizer, records, creator, schema, identity, room, recipient,
        )
    };
    project_snapshot_zones(state, schema, zones, recipient)
}

/// The `(schema, authorized set)` a zone projection would narrow by, or `None` when it
/// is not to run at all — a whole-zone subscriber (its set is exactly the declared id
/// range), a no-zones room, or a relay, each of which takes a state verbatim. Skipping
/// it is not merely an optimization: the projection also scrubs the causal frontier,
/// and a reader entitled to every partition is owed the whole frontier to dedup
/// against. The single home of that rule, so [`project_snapshot_zones`] and a caller
/// that must know whether narrowing is even possible cannot drift apart.
///
/// "Whole-zone" means the set is *exactly* the declared id range, not merely as large
/// as it. A caller may hand this an id the schema does not declare — the callers that
/// resolve their set through [`acting_zone_scope`] cannot, but the rule is a property
/// of the pair rather than of any one caller — so a count is not a claim about *which*
/// partitions a set names. Anything short of the exact range projects, which is the
/// only reading that is never wider than either a count or a containment test alone.
fn zone_narrowing<'a>(
    schema: Option<&'a Schema>,
    zones: &'a Option<HashSet<u32>>,
) -> Option<(&'a Schema, &'a HashSet<u32>)> {
    let (schema, set) = (schema?, zones.as_ref()?);
    let declared = schema.zones().len() as u32;
    let whole = set.len() as u32 == declared && (0..declared).all(|id| set.contains(&id));
    (declared > 0 && !whole).then_some((schema, set))
}

/// Narrow a catch-up snapshot to a zone-limited subscriber's authorized partitions,
/// dropping every hidden zone's state so the snapshot carries no trace of it. Only a
/// genuinely zone-limited subscriber pays the projection ([`zone_narrowing`]).
fn project_snapshot_zones(
    state: Vec<u8>,
    schema: Option<&Schema>,
    zones: &Option<HashSet<u32>>,
    recipient: Option<ClientId>,
) -> Vec<u8> {
    let Some((schema, set)) = zone_narrowing(schema, zones) else {
        return state;
    };
    match Document::decode_state(&state) {
        Ok(mut doc) => {
            doc.project_zones(schema, set, recipient);
            doc.encode_state()
        }
        // An undecodable state is left as-is: it fails downstream on the same footing
        // it would have without zones. The version fetch refuses such bytes before it
        // gets here, but only the bytes it was *handed* — this input can also be the
        // read projection's re-encode, which nothing re-checks.
        Err(_) => state,
    }
}

/// Narrow a catch-up snapshot to a partial reader's readable paths, dropping every
/// element its doc-ACL read tier does not admit so the snapshot carries no trace of it —
/// the state half of the per-op read redaction. The projection gates each element on the
/// same [`recipient_reads_path`] the op fan-out applies, at the same `core::path` the
/// op's [`op_read_gate`](crate::acl::op_read_gate) resolves to, so a snapshot-served
/// joiner drops exactly the elements an op-served joiner never received — the two
/// converge. An unreadable container's whole subtree goes, a leaf-level deny drops its
/// slot, and doc-level ACL/ranged state goes with an unreadable root.
///
/// An undecodable snapshot is left as-is: it fails downstream on the same footing it
/// would have without projection, never silently served narrowed-wrong.
#[allow(clippy::too_many_arguments)]
fn project_snapshot_reads(
    state: Vec<u8>,
    authorizer: &dyn Authorizer,
    records: &[crdtsync_core::acl::AclRecord],
    creator: Option<&[u8]>,
    schema: Option<&Schema>,
    identity: &Identity,
    room: &[u8],
    recipient: Option<ClientId>,
) -> Vec<u8> {
    match Document::decode_state(&state) {
        Ok(mut doc) => {
            // Resolve element-scoped grants against the very tree being projected, so
            // an element's redaction path matches its position in this snapshot — the
            // same element-context index the op fan-out resolves against, derived here
            // from the decoded doc so it cannot drift from what is projected.
            let index = crate::index::element_paths(&doc);
            doc.project_read_paths(
                crate::acl::recipient_reads_predicate(
                    authorizer, records, creator, &index, schema, identity, room,
                ),
                recipient,
            );
            doc.encode_state()
        }
        Err(_) => state,
    }
}

/// The redirect to send when this node does not lead `room` — the leader's
/// advertise address for the client to reconnect to — or `None` when this node
/// serves the room itself: it leads it, or single-node mode (no membership)
/// makes it leader of every room. The leader is `room`'s *effective* leader —
/// its placement primary while that primary is live, else the promoted next-live
/// replica (failover, Unit 6a) — so a client is never redirected at a dead node.
/// When every replica of the room is down, the redirect falls back to the
/// placement primary: a client retrying a dead leader is correct backpressure,
/// and a node that does not hold the room never serves it itself.
fn redirect_if_not_leader(membership: Option<&Membership>, room: &[u8]) -> Option<Message> {
    let membership = membership?;
    // A stranded node holds no room, and "no primary" must not fall through to serving
    // it here: `None` means *this node owns the room*, which is the right reading for a
    // node with no membership and the wrong one for a node whose ring it could not
    // rebuild. It has no leader to name either, so the client is told the room is
    // unavailable rather than silently served by a node the cluster does not place.
    if membership.is_stranded() {
        return Some(Message::Error {
            code: ErrorCode::Internal,
            message: "node holds no ring position and cannot serve this room".to_string(),
            details: room.to_vec(),
        });
    }
    let leader = membership
        .effective_primary_for(room)
        .or_else(|| membership.primary_for(room))?;
    if membership.is_self(&leader) {
        return None;
    }
    Some(Message::Redirect {
        room: room.to_vec(),
        leader_addr: leader.as_bytes().to_vec(),
    })
}

/// The [`Response`] declining to serve `room` here — a lone [`Message::Redirect`]
/// to its leader — or `None` to serve the request as usual. The gate every request
/// the leader alone answers shares: the room-serving *writes* (an ops write, a
/// durable version or branch mutation), so a follower never ingests or persists a
/// room it does not lead; and a **version read**, which follows its mutations
/// because a version index is per node, so a node answers only about the captures
/// it took. A live room read uses [`read_redirect_response`] instead, which lets a
/// caught-up follower serve under a client-named floor.
fn redirect_response(membership: Option<&Membership>, room: &[u8]) -> Option<Response> {
    redirect_if_not_leader(membership, room).map(|redirect| Response {
        replies: vec![redirect],
        ..Response::default()
    })
}

/// The [`Response`] redirecting a READ (a Subscribe) this node cannot serve
/// locally, or `None` to serve it from local state. Unlike a write, a follower
/// MAY serve a read — the transparent-proxy consistency model is bounded-staleness
/// with read-your-writes / monotonicity via `floor` (the client's highest observed
/// server sequence, its `last_seen_seq`):
///
/// - The room's leader always serves (single-node, being primary, or the effective
///   primary on failover) — `None`.
/// - A follower serves the read from its own replica only when it (1) is a replica
///   of the room, (2) holds a materialized copy — it has been caught up, so the
///   state is whole, never torn — and (3) its committed watermark ([`Hub::seq`]) is
///   at least `floor`. The watermark-≥-floor test is the read-your-writes and
///   monotonicity gate: a read whose floor is ahead of the follower would be stale
///   relative to what the client already wrote or saw.
/// - Failing any of those, it redirects to the leader — which is by definition at
///   or ahead of the floor. Every unsafe read (uncaught, non-replica, past the
///   floor) fails safe to the leader, so a follower never serves a torn,
///   missing-a-just-written-op, or backwards-in-time read.
fn read_redirect_response(
    membership: Option<&Membership>,
    hub: &Hub,
    room: &[u8],
    branch: &[u8],
    floor: u64,
) -> Option<Response> {
    // A read resolves the leader exactly as a write does — `None` when this node
    // serves the room as its leader (single-node, or the room's effective primary).
    // Sharing that one resolution is what keeps read routing from ever diverging
    // from write routing as the election rule evolves.
    let redirect = redirect_if_not_leader(membership, room)?;
    // This node is a follower of the room. It serves the read from its own replica
    // — bounded staleness — only when it holds a materialized, caught-up copy whose
    // committed watermark is at least the client's floor (the read-your-writes /
    // monotonicity gate), and only for the `main` stream: a leader mirrors only
    // `main` to its followers (branch replication is a later unit), so a named-branch
    // read must redirect to the leader, which holds every branch. Failing any of
    // those it redirects, so an uncaught, non-replica, past-the-floor, or non-main
    // read fails safe to the leader.
    let main_stream = branch.is_empty() || branch == MAIN_BRANCH;
    if let Some(membership) = membership {
        if main_stream && membership.owns(room) && hub.holds_room(room) && hub.seq(room) >= floor {
            return None;
        }
    }
    Some(Response {
        replies: vec![redirect],
        ..Response::default()
    })
}

/// The room a channel-keyed request names, resolved without deciding anything about
/// it: the connection is authenticated and the channel is bound, and nothing more.
/// The routing half of such a request needs the room and must run *before* the gate:
/// a node that will not answer must not decide it, since [`Authorizer::observe`]
/// records the verdict and would leave a durable audit entry for a read the node
/// then refuses.
fn bound_room(session: &Session, channel: Channel) -> Option<RoomId> {
    session.identity()?;
    Some(session.channels.get(&channel)?.room.clone())
}

/// Whether this connection may take `action` on `room` through a channel-keyed
/// request — a version request, a diff query. Neither is gated by the doc-ACL tier
/// here — it abstains, so the deployment and schema tiers decide. What a partial
/// reader may see of the content behind such a request is the projections' answer,
/// not this gate's.
fn channel_authorized(
    session: &Session,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    room: &[u8],
    action: Action,
) -> bool {
    let Some(identity) = session.identity() else {
        return false;
    };
    authorized(
        authorizer,
        Decision::Abstain,
        schema,
        identity,
        action,
        &Resource::Room(room),
    )
}

/// The refusal for a channel-keyed `what` request that [`bound_room`] or
/// [`channel_authorized`] rejected: a violation if the connection is unauthenticated
/// or the channel is unbound, otherwise a non-closing forbidden. `what` names the
/// request kind (`"version"`, `"diff"`) so the diagnostic points at the surface the
/// client actually used — the channel-bound counterpart of [`request_denied`].
fn channel_request_denied(session: &Session, channel: Channel, what: &str) -> Response {
    if session.actor().is_none() {
        violation(&format!("{what} request before auth"))
    } else if !session.channels.contains_key(&channel) {
        violation(&format!("{what} request on an unbound channel"))
    } else {
        forbidden(&format!("{what} request denied"))
    }
}

/// The reply carrying `room`'s current version names on `channel`.
fn versions_list(hub: &Hub, channel: Channel, room: &[u8]) -> Response {
    Response {
        replies: vec![Message::Versions {
            channel,
            names: hub.version_names(room),
        }],
        ..Response::default()
    }
}

/// Whether this session's actor is authorized for `action` on `room`'s branch
/// management. Branch ops are room-management actions, gated by the same tier as
/// version management: the doc-ACL tier abstains, so the deployment and schema
/// tiers decide. Room-keyed rather than channel-keyed — a client may manage a
/// room's branches without holding a subscription — so the room comes from the
/// frame, checked only that the connection is authenticated.
fn branch_authorized(
    session: &Session,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    room: &[u8],
    action: Action,
) -> bool {
    let Some(identity) = session.identity() else {
        return false;
    };
    authorized(
        authorizer,
        Decision::Abstain,
        schema,
        identity,
        action,
        &Resource::Room(room),
    )
}

/// Whether this session's actor may read `room` **whole** — the gate on cloning it.
/// A clone hands over every byte the source holds, so anything short of a whole read
/// launders the redaction that governs the withheld part: both dimensions a served
/// state is narrowed by are keyed by the room the bytes are leaving, and neither
/// follows them into the destination. The doc-ACL dimension is
/// [`reads_whole_document`] — a reader an effective `Deny(Read)` carves a subtree out
/// of is partial, and its own catch-up would have projected that subtree away. The
/// zone dimension is every declared partition being readable, since a
/// [`Resource::Zone`] verdict names the source room and decides nothing about a
/// destination the caller picks.
///
/// Room-keyed rather than channel-keyed, like [`branch_authorized`] — a client may
/// clone a room without holding a subscription — so the room comes from the frame,
/// checked only that the connection is authenticated.
fn reads_source_whole(
    hub: &Hub,
    session: &Session,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    room: &[u8],
) -> bool {
    let Some(identity) = session.identity() else {
        return false;
    };
    let records = hub.acl_records(room);
    let creator = hub.room_creator(room);
    let index = hub.element_paths(room);
    reads_whole_document(
        authorizer,
        &records,
        creator.as_deref(),
        &index,
        schema,
        identity,
        room,
    ) && reads_every_zone(authorizer, identity, schema, room)
}

/// Whether `identity` may read every zone `schema` declares in `room` — the zone
/// dimension of reading a room whole. Reached only where the room read verdict
/// already holds, so an abstaining deployment admits each zone ([`zone_readable`])
/// and only an explicit per-zone deny carves one out.
///
/// `schema` is the room's *governing* schema, never the caller's declared one: a
/// clone's source is a room the caller is not the incumbent of, so a self-declared
/// app would let it pick a zone block. A room governed by nothing declares no zones
/// and so is whole by this measure — the one implicit root partition, the same
/// reading [`zone_scope`] takes.
///
/// That reading is a *fact* about the room only where the binding resolves. Two ways
/// it does not, and neither is closed here: a room bound to a version this node's
/// registry does not hold — the window between a restart and the app re-registering
/// (C101), registration itself parsing what it stores so an unreadable body is not one
/// of them; and a room the hub holds that nothing ever bound — an import, or a clone
/// of an ungoverned room — which is indistinguishable from a relay room, genuinely
/// partitionless and rightly clonable (C62). Requiring `src`'s leader makes the ACL
/// records and the creator authoritative; it does not conjure a binding.
fn reads_every_zone(
    authorizer: &dyn Authorizer,
    identity: &Identity,
    schema: Option<&Schema>,
    room: &[u8],
) -> bool {
    schema.is_none_or(|s| {
        s.zones()
            .iter()
            .all(|(name, _)| zone_readable(authorizer, identity, room, name.as_bytes(), true))
    })
}

/// The refusal for a room-keyed `what` request [`branch_authorized`] rejected: a
/// violation if the connection is unauthenticated, otherwise a non-closing
/// forbidden. `what` names the request kind (`"branch"`, `"clone"`) in the
/// diagnostic so the message points at the surface the client actually used.
fn request_denied(session: &Session, what: &str) -> Response {
    if session.actor().is_none() {
        violation(&format!("{what} request before auth"))
    } else {
        forbidden(&format!("{what} request denied"))
    }
}

/// Map a [`DiffError`] to the client failure it surfaces. A version or branch the
/// room does not have is a recoverable `NotFound` — a name the client can correct.
/// A branch the room *does* have whose state this node cannot read is an `Internal`
/// fault, as is a materialized state that fails to decode: nothing the client sent
/// caused either, and a branch `BranchList` enumerates is not a name the client can
/// correct (C51) — the code the subscribe seam gives a branch stream it cannot
/// resolve a redaction index for. None of them closes: a diff's sides are archived or
/// reconstructed state, so one of them being unreadable is a server-side fault the
/// channel's live stream survives — the reading the version fetch takes of the
/// version bytes, generalized.
fn diff_error(e: DiffError) -> Response {
    let code = match e {
        DiffError::UnknownVersion(_) | DiffError::UnknownBranch(_) => ErrorCode::NotFound,
        DiffError::UnreadableBranch(_) | DiffError::Decode => ErrorCode::Internal,
    };
    Response {
        replies: vec![Message::Error {
            code,
            message: e.to_string(),
            details: Vec::new(),
        }],
        ..Response::default()
    }
}

/// The reply carrying `room`'s current branch set.
fn branches_list(hub: &Hub, room: &[u8]) -> Response {
    let branches = hub
        .branches(room)
        .into_iter()
        .map(|b| BranchInfo {
            name: b.name,
            fork_point: b.fork_point,
            head: b.head,
            published: b.published,
        })
        .collect();
    Response {
        replies: vec![Message::Branches {
            room: room.to_vec(),
            branches,
        }],
        ..Response::default()
    }
}

/// The `(governing_app, governing_version, client_version)` when this session is
/// the enforcing speaker of the room's governing app — the one connection class
/// whose ops drive the room's chain, whose catch-up is translated, and whose
/// subscribe is range-checked. `None` for a relay, a foreign app, or a
/// versionless session: a different version space, served verbatim, its writes
/// logged untagged, never refused.
fn governing_target<'a>(
    governing: Option<(&'a [u8], u32)>,
    session: &Session,
) -> Option<(&'a [u8], u32, u32)> {
    match (governing, session.schema_version()) {
        (Some((app, governing_version)), Some(client_version)) if session.app_id() == app => {
            Some((app, governing_version, client_version))
        }
        _ => None,
    }
}

/// Translate a catch-up delta to the joining session's version, on the same
/// app-scoping as the live fan-out: only when the room is bound to an app the
/// joiner also speaks, and the joiner declared an enforced version. A relay
/// joiner, an unbound room, or a foreign-app joiner takes the delta verbatim —
/// its version is a different space and must never drive the room's chain.
fn catch_up_ops(
    registry: &Mutex<SchemaRegistry>,
    governing: Option<(&[u8], u32)>,
    session: &Session,
    delta: Vec<StoredOp>,
    types: &crate::index::ElementTypes,
) -> Vec<Op> {
    match governing_target(governing, session) {
        Some((app, _, target)) => {
            let reg = match registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            crate::translate::translate_delta_scoped(&reg, app, delta, target, types)
        }
        None => delta.into_iter().map(|rec| rec.op).collect(),
    }
}

/// Migrate a catch-up snapshot to the joining session's version, on the same
/// app-scoping as the op delta. The snapshot is projected from the room's
/// op-version `high_water` — the version its merged state actually embodies — so
/// an enforcing joiner below it has fields added above its version projected out,
/// and one above it has the state up-migrated. The handshake admits a joiner only
/// when it reaches the high-water, so this projection is always across invertible
/// edges. A relay, unbound, foreign-app, or same-version joiner, or a room with
/// no governing-app content, takes the snapshot verbatim.
fn catch_up_snapshot(
    registry: &Mutex<SchemaRegistry>,
    governing: Option<(&[u8], u32)>,
    session: &Session,
    high_water: Option<u32>,
    state: Vec<u8>,
    schema: Option<&Schema>,
) -> Vec<u8> {
    match (governing_target(governing, session), high_water) {
        (Some((app, _, target)), Some(high_water)) if high_water != target => {
            let reg = match registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            crate::translate::translate_snapshot_scoped(
                &reg, app, &state, high_water, target, schema,
            )
        }
        _ => state,
    }
}

/// Whether a subscriber may be served the room's ops, or must be refused with
/// `onUpdateRequired`. Only an enforcing joiner of the room's governing app is
/// range-checked: it must reach the room's op-version `high_water` — the highest
/// creation version the merged state embodies — across a back-compatible path
/// (forward always, backward only over invertible edges). A joiner admitted at
/// the true high-water can down-reach every op the room holds, so fan-out and the
/// snapshot seam only ever traverse invertible edges. A room with no versioned op
/// (`high_water` is `None`) has nothing to reach and never refuses on this basis.
/// A relay or
/// foreign-app joiner is a different version space and is never refused. A broken
/// chain (a gap the registry cannot bridge) refuses, fail-closed. The same
/// predicate re-checks an already-joined peer when a write lifts the high-water,
/// so admission and stranded-peer eviction agree on reachability.
pub(crate) fn subscriber_reaches_governing(
    registry: &Mutex<SchemaRegistry>,
    governing: Option<(&[u8], u32)>,
    session: &Session,
    high_water: Option<u32>,
) -> bool {
    match (governing_target(governing, session), high_water) {
        (Some((app, _, client_version)), Some(high_water)) => {
            let reg = match registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            matches!(
                crate::translate::reachable(&reg, app, high_water, client_version),
                Ok(true)
            )
        }
        _ => true,
    }
}

/// Accept a peer's protocol version, or refuse it with an Error to send back.
pub fn negotiate(version: u32) -> Result<(), Message> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Message::Error {
            code: ErrorCode::UnsupportedVersion,
            message: "unsupported protocol version".to_string(),
            details: Vec::new(),
        })
    }
}

fn violation(reason: &str) -> Response {
    Response {
        replies: vec![Message::Error {
            code: ErrorCode::ProtocolViolation,
            message: reason.to_string(),
            details: Vec::new(),
        }],
        close: true,
        ..Response::default()
    }
}

/// A server-side failure that could not be completed — the write did not land,
/// so the connection closes rather than advertise a result it cannot back.
fn internal(reason: &str) -> Response {
    Response {
        replies: vec![Message::Error {
            code: ErrorCode::Internal,
            message: reason.to_string(),
            details: Vec::new(),
        }],
        close: true,
        ..Response::default()
    }
}

/// Refuse a batch of authored ops without closing the connection: name the
/// rejected ops by their per-client sequence and why, ingesting and
/// acknowledging nothing. The client drains the named ops from its outbox and
/// surfaces the rejection, so an op the server will not accept never sits
/// acked-forever in the queue.
fn ops_rejected(channel: Channel, ops: &[Op], reason: ErrorCode) -> Response {
    Response {
        replies: vec![Message::OpsRejected {
            channel,
            seqs: ops.iter().map(|op| op.id.seq).collect(),
            reason,
        }],
        ..Response::default()
    }
}

/// A denied-but-well-formed request: the actor lacks permission. Unlike a
/// protocol violation the connection stays open — the client may still act
/// within what it is allowed.
fn forbidden(reason: &str) -> Response {
    Response {
        replies: vec![Message::Error {
            code: ErrorCode::Forbidden,
            message: reason.to_string(),
            details: Vec::new(),
        }],
        ..Response::default()
    }
}
