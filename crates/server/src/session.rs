//! The connection's protocol driver.
//!
//! A [`Session`] is one client connection. [`step`] sequences the protocol —
//! Hello, then Subscribe, then a stream of Ops — turning each inbound
//! [`Message`] into hub mutations plus a [`Response`]: messages to reply to
//! this client, ops to broadcast to the room's other subscribers, and whether
//! to close. Anything out of order is a protocol violation. Pure logic; the
//! async transport drives it.

use std::collections::HashMap;

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{Channel, ClientId, ErrorCode, Message, Op};

use crate::auth::{Identity, Verifier};
use crate::authz::{Action, Authorizer, Resource};
use crate::{Catchup, Hub, RoomId};

/// One client connection's protocol state. The handshake runs Hello → Auth →
/// Subscribe: the client names itself, then presents a credential the server
/// turns into an actor, then joins rooms. A connection multiplexes several room
/// subscriptions, each on its own [`Channel`]; the client assigns the handle at
/// Subscribe and every later frame names it.
pub struct Session {
    client: Option<ClientId>,
    identity: Option<Identity>,
    channels: HashMap<Channel, RoomId>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            client: None,
            identity: None,
            channels: HashMap::new(),
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
        }
    }

    /// The client named at Hello, if the handshake is done.
    pub fn client(&self) -> Option<ClientId> {
        self.client
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
    msg: Message,
) -> Response {
    match msg {
        Message::Hello { client } => {
            if session.client.is_some() {
                return violation("already said hello");
            }
            // Hello establishes quietly; the version was negotiated at the
            // connection header before any message.
            session.client = Some(client);
            Response::default()
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
            last_seen_seq,
        } => {
            let Some(actor) = session.actor() else {
                return violation("subscribe before auth");
            };
            if session.channels.contains_key(&channel) {
                return violation("channel already subscribed");
            }
            // A subscription reads the room; the server never serves a room the
            // actor may not read.
            if !authorizer.authorize(actor, Action::Read, &Resource::Room(&room)) {
                return forbidden("read denied");
            }
            let reply = match hub.catch_up(&room, last_seen_seq) {
                Catchup::Ops(ops) => Message::Ops { channel, ops },
                Catchup::Snapshot { seq, state } => Message::Snapshot {
                    channel,
                    seq,
                    state,
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
            let actor = session.actor().expect("actor set, checked above");
            if !authorizer.authorize(actor, Action::Write, &Resource::Room(&room)) {
                return forbidden("write denied");
            }
            // The batch's highest per-client op sequence: the frontier the author
            // is acknowledged through once the ops are durably logged, so it can
            // prune its outbox. Computed over the whole submitted batch, not just
            // the fresh ops, so a resent op the hub already holds is still acked
            // and pruned. An empty batch acknowledges nothing.
            let through = ops.iter().map(|op| op.id.seq).max();
            // The deduped ops fan out to the room's other subscribers; nothing
            // echoes back to the sender. A hub that cannot durably record the
            // ops rejects the write rather than advertising an unpersisted one.
            match hub.ingest(&room, ops) {
                Ok(applied) => Response {
                    replies: through
                        .map(|through| Message::Accepted { channel, through })
                        .into_iter()
                        .collect(),
                    broadcast: applied,
                    broadcast_room: Some(room),
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
        // The client reports its applied sequence; recording it into the
        // per-client GC watermark is the next unit. Until then the report is
        // accepted and ignored rather than treated as a violation — a
        // well-behaved client will send it.
        Message::Ack { .. } => Response::default(),
        // `Accepted` is the server's own reply to an author; a client never sends
        // one.
        Message::Accepted { .. } => violation("client sent an accepted"),
        Message::AwarenessSet {
            channel,
            key,
            value,
        } => {
            let Some(actor) = session.actor().map(<[u8]>::to_vec) else {
                return violation("awareness before auth");
            };
            let Some(client) = session.client else {
                return violation("awareness before hello");
            };
            let Some(room) = session.channels.get(&channel).cloned() else {
                return violation("awareness on an unbound channel");
            };
            if !authorizer.authorize(&actor, Action::PublishAwareness, &Resource::Room(&room)) {
                return forbidden("awareness publish denied");
            }
            // Ephemeral: retained for late-joiner replay and fanned to the room's
            // peers, but never logged or snapshotted. A key dropped at the
            // per-client cap is neither stored nor broadcast.
            if !hub.set_awareness(&room, client, actor.clone(), key.clone(), value.clone()) {
                return Response::default();
            }
            Response {
                awareness: Some(AwarenessBroadcast {
                    room,
                    actor,
                    key,
                    value,
                }),
                ..Response::default()
            }
        }
        // Peer updates and clears only travel server-to-client.
        Message::AwarenessUpdate { .. } => violation("client sent an awareness update"),
        Message::AwarenessClear { .. } => violation("client sent an awareness clear"),
        // Versioning is a request/response sub-protocol over the channel's room.
        // A mutation replies with the fresh name list — the authoritative
        // post-state — and a list request the same; a fetch that hits replies
        // with the version's state, and one that misses falls back to the list.
        Message::VersionCreate { channel, name } => {
            let Some(room) = version_room(session, channel, authorizer, Action::Write) else {
                return version_denied(session, channel);
            };
            match hub.create_version(&room, &name) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionRename { channel, from, to } => {
            let Some(room) = version_room(session, channel, authorizer, Action::Write) else {
                return version_denied(session, channel);
            };
            match hub.rename_version(&room, &from, &to) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionDelete { channel, name } => {
            let Some(room) = version_room(session, channel, authorizer, Action::Write) else {
                return version_denied(session, channel);
            };
            match hub.delete_version(&room, &name) {
                Ok(_) => versions_list(hub, channel, &room),
                Err(_) => internal("failed to persist version"),
            }
        }
        Message::VersionList { channel } => {
            let Some(room) = version_room(session, channel, authorizer, Action::Read) else {
                return version_denied(session, channel);
            };
            versions_list(hub, channel, &room)
        }
        Message::VersionFetch { channel, name } => {
            let Some(room) = version_room(session, channel, authorizer, Action::Read) else {
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
    action: Action,
) -> Option<RoomId> {
    let actor = session.actor()?;
    let room = session.channels.get(&channel)?.clone();
    authorizer
        .authorize(actor, action, &Resource::Room(&room))
        .then_some(room)
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
