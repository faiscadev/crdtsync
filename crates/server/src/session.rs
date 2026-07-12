//! The connection's protocol driver.
//!
//! A [`Session`] is one client connection. [`step`] sequences the protocol —
//! Hello, then Subscribe, then a stream of Ops — turning each inbound
//! [`Message`] into hub mutations plus a [`Response`]: messages to reply to
//! this client, ops to broadcast to the room's other subscribers, and whether
//! to close. Anything out of order is a protocol violation. Pure logic; the
//! async transport drives it.

use std::collections::HashMap;
use std::sync::Mutex;

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{Channel, ClientId, ErrorCode, Message, Op};

use crdtsync_core::schema::Schema;

use crate::acl::authorized;
use crate::auth::{Identity, Verifier};
use crate::authz::{Action, Authorizer, Resource};
use crate::schema_registry::{Resolution, SchemaRegistry};
use crate::{Catchup, Hub, RoomId, StoredOp};

/// One client connection's protocol state. The handshake runs Hello → Auth →
/// Subscribe: the client names itself, then presents a credential the server
/// turns into an [`Identity`] (actor plus roles and groups), then joins rooms. A
/// connection multiplexes several room subscriptions, each on its own
/// [`Channel`]; the client assigns the handle at Subscribe and every later frame
/// names it.
pub struct Session {
    client: Option<ClientId>,
    identity: Option<Identity>,
    channels: HashMap<Channel, RoomId>,
    /// The app named at Hello (empty for a relay connection with no app).
    app_id: Vec<u8>,
    /// The registered schema version this connection is enforced at, resolved at
    /// Hello; `None` for a relay connection (no app, or an unregistered app).
    schema_version: Option<u32>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            client: None,
            identity: None,
            channels: HashMap::new(),
            app_id: Vec::new(),
            schema_version: None,
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
        self.channels.get(&channel)
    }

    /// The channels this connection has bound to `room`. A broadcast for the
    /// room is delivered on each — one connection may hold the same room on
    /// more than one channel.
    pub fn channels_for_room(&self, room: &[u8]) -> Vec<Channel> {
        self.channels
            .iter()
            .filter(|(_, r)| r.as_slice() == room)
            .map(|(c, _)| *c)
            .collect()
    }

    /// The rooms this connection currently subscribes, one entry per channel —
    /// the same room recurs if held on several channels, so the caller dedups.
    pub fn subscribed_rooms(&self) -> impl Iterator<Item = &RoomId> {
        self.channels.values()
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
/// other subscribers of `broadcast_room`, an ephemeral awareness entry to fan
/// out, and whether the connection should close. The broadcast fields are
/// `None`/empty when there is nothing to fan out.
#[derive(Default)]
pub struct Response {
    pub replies: Vec<Message>,
    pub broadcast: Vec<Op>,
    pub broadcast_room: Option<RoomId>,
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
    now: u64,
    throttle: Option<u64>,
    msg: Message,
) -> Response {
    match msg {
        Message::Hello {
            client,
            app_id,
            schema_version,
        } => {
            if session.client.is_some() {
                return violation("already said hello");
            }
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
            Response {
                replies: advert.into_iter().collect(),
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
            // A subscription is scoped by `(room, branch)`; an empty branch is
            // `main`. The room binds the subscription; the branch is carried but
            // does not scope the log or rooms.
            branch: _,
            last_seen_seq,
        } => {
            let Some(identity) = session.identity() else {
                return violation("subscribe before auth");
            };
            if session.channels.contains_key(&channel) {
                return violation("channel already subscribed");
            }
            // A subscription reads the room; the server never serves a room the
            // actor may not read.
            if !authorized(
                authorizer,
                schema,
                identity,
                Action::Read,
                &Resource::Room(&room),
            ) {
                return forbidden("read denied");
            }
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
            let reply = match hub.catch_up(&room, last_seen_seq) {
                Catchup::Ops(delta) => Message::Ops {
                    channel,
                    ops: catch_up_ops(registry, governing, session, delta),
                },
                Catchup::Snapshot { seq, state } => Message::Snapshot {
                    channel,
                    seq,
                    state: catch_up_snapshot(registry, governing, session, high_water, state),
                },
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
            session.channels.insert(channel, room);
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
        Message::Ops { channel, ops } => {
            if session.actor().is_none() {
                return violation("ops before auth");
            }
            let Some(client) = session.client else {
                return violation("ops before hello");
            };
            let Some(room) = session.channels.get(&channel).cloned() else {
                return violation("ops on an unbound channel");
            };
            // Every op must carry the client declared at Hello, so a
            // connection's ops stay self-consistent. Authenticating that the
            // client is who it claims is the transport's credential check;
            // this driver only enforces consistency.
            if ops.iter().any(|op| op.id.client != client) {
                return violation("op client mismatch");
            }
            let identity = session.identity().expect("identity set, checked above");
            if !authorized(
                authorizer,
                schema,
                identity,
                Action::Write,
                &Resource::Room(&room),
            ) {
                // Authored ops sit in the client's outbox until acknowledged, so a
                // refusal must be recoverable rather than a connection close: name
                // the rejected ops, keep the connection open, ingest and ack
                // nothing. The client drains them from its outbox and surfaces the
                // rejection for the app to show, discard, or export.
                return ops_rejected(channel, &ops, ErrorCode::Forbidden);
            }
            // The batch's highest per-client op sequence: the frontier the author
            // is acknowledged through once the ops are durably logged, so it can
            // prune its outbox. Computed over the whole submitted batch, not just
            // the fresh ops, so a resent op the hub already holds is still acked
            // and pruned. An empty batch acknowledges nothing.
            let through = ops.iter().map(|op| op.id.seq).max();
            // The op's creation version is recorded only when the writer speaks
            // the room's governing app — its version number lives in that app's
            // space. A foreign-app writer's version is a different space and must
            // never drive this room's chain, so its ops are logged untagged
            // (`None`, relay-like) and pass verbatim on both the live and the
            // catch-up seam, exactly as the fan-out already leaves them.
            let write_version = governing_target(governing, session).map(|(_, _, client)| client);
            // The deduped ops fan out to the room's other subscribers; nothing
            // echoes back to the sender. A hub that cannot durably record the
            // ops rejects the write rather than advertising an unpersisted one.
            match hub.ingest(&room, ops, write_version) {
                Ok(applied) => Response {
                    replies: through
                        .map(|through| Message::Accepted { channel, through })
                        .into_iter()
                        .collect(),
                    broadcast: applied,
                    broadcast_room: Some(room),
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
        Message::Snapshot { .. } => violation("client sent a snapshot"),
        Message::Error { .. } => violation("client sent an error"),
        Message::AuthOk { .. } => violation("client sent an authok"),
        Message::SchemaAdvert { .. } => violation("client sent a schema advert"),
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
            let Some(room) = session.channels.get(&channel).cloned() else {
                return violation("awareness on an unbound channel");
            };
            if !authorized(
                authorizer,
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
        Message::VersionCreate { channel, name } => {
            let Some(room) = version_room(session, channel, authorizer, schema, Action::Write)
            else {
                return version_denied(session, channel);
            };
            match hub.create_version(&room, &name) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionRename { channel, from, to } => {
            let Some(room) = version_room(session, channel, authorizer, schema, Action::Write)
            else {
                return version_denied(session, channel);
            };
            match hub.rename_version(&room, &from, &to) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionDelete { channel, name } => {
            let Some(room) = version_room(session, channel, authorizer, schema, Action::Write)
            else {
                return version_denied(session, channel);
            };
            match hub.delete_version(&room, &name) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionList { channel } => {
            let Some(room) = version_room(session, channel, authorizer, schema, Action::Read)
            else {
                return version_denied(session, channel);
            };
            versions_list(hub, channel, &room)
        }
        Message::VersionFetch { channel, name } => {
            let Some(room) = version_room(session, channel, authorizer, schema, Action::Read)
            else {
                return version_denied(session, channel);
            };
            match hub.version_state(&room, &name) {
                Some(state) => {
                    let seq = hub.version_seq(&room, &name).unwrap_or(0);
                    let state = state.to_vec();
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
    }
}

/// Resolve the room a version request targets, having checked the connection is
/// authenticated, the channel is bound, and the actor is authorized for
/// `action`. `None` means the request cannot proceed — [`version_denied`]
/// distinguishes an unbound channel (a violation) from a denial (forbidden).
fn version_room(
    session: &Session,
    channel: Channel,
    authorizer: &dyn Authorizer,
    schema: Option<&Schema>,
    action: Action,
) -> Option<RoomId> {
    let identity = session.identity()?;
    let room = session.channels.get(&channel)?.clone();
    authorized(authorizer, schema, identity, action, &Resource::Room(&room)).then_some(room)
}

/// The refusal for a version request that [`version_room`] rejected: a violation
/// if the connection is unauthenticated or the channel is unbound, otherwise a
/// non-closing forbidden.
fn version_denied(session: &Session, channel: Channel) -> Response {
    if session.actor().is_none() {
        violation("version request before auth")
    } else if !session.channels.contains_key(&channel) {
        violation("version request on an unbound channel")
    } else {
        forbidden("version request denied")
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
) -> Vec<Op> {
    match governing_target(governing, session) {
        Some((app, _, target)) => {
            let reg = match registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            crate::translate::translate_delta(&reg, app, delta, target)
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
) -> Vec<u8> {
    match (governing_target(governing, session), high_water) {
        (Some((app, _, target)), Some(high_water)) if high_water != target => {
            let reg = match registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            crate::translate::translate_snapshot(&reg, app, &state, high_water, target)
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
