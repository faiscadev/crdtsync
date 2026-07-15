//! The replica's side of the wire protocol.
//!
//! A [`ClientSession`] is the client-side mirror of the server's session
//! driver. It opens with Hello, then holds several room subscriptions at once —
//! each on its own [`Channel`], with its own local [`Document`] and caught-up
//! sequence. Subscribe assigns the next channel and draws the server's catch-up
//! — an op delta or a whole-replica [`Message::Snapshot`]. Inbound frames route
//! to a room by their channel; a reconnect resumes each room from where it left
//! off instead of replaying from zero. Pure logic: messages in, local replicas
//! and messages out; the transport moves the bytes.

use std::collections::{HashMap, HashSet};

use crate::diff::{decode_changes, Change};
use crate::doc::MapCursor;
use crate::{BranchInfo, Channel, ClientId, DiffKind, Document, ErrorCode, Message, Op};

/// Why an inbound message could not be folded into a replica.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ClientError {
    /// A frame that only travels client-to-server arrived from the server.
    UnexpectedMessage(&'static str),
    /// A snapshot's state bytes did not decode into a replica.
    BadSnapshot,
    /// A diff result's encoded change list did not decode.
    BadDiff,
    /// A routed frame named a channel this session does not hold.
    UnknownChannel(Channel),
    /// The server reported a failure.
    Server { code: ErrorCode, message: String },
}

/// The server's redirect of a room to its leader: this node does not lead the
/// room, so the client must reconnect to `leader_addr` and subscribe there. The
/// core session holds no socket, so it cannot reconnect itself — it surfaces the
/// target through [`take_redirects`](ClientSession::take_redirects) for the
/// transport layer to act on, the same split as the `onOpsRejected` signal.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Redirect {
    pub room: Vec<u8>,
    pub leader_addr: Vec<u8>,
}

/// A batch of authored ops the server refused, surfaced for the app to show,
/// discard, or export. The server rejected these — auth revoked, or an enforcing
/// server's validation failed — so they left the outbox (a `resend` will never
/// replay them) but are kept here, with the `reason`, until the app drains them
/// through [`take_rejected`](ClientSession::take_rejected).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Rejected {
    pub channel: Channel,
    pub reason: ErrorCode,
    pub ops: Vec<Op>,
}

/// One subscribed room: its local replica, the room name and the branch within
/// it (empty is the default `main`), how far it has caught up, the outbox of
/// authored-but-unacknowledged ops, the peers' ephemeral awareness entries keyed
/// by `(actor, key)`, and the version view — the last name list the server
/// reported and any fetched version states keyed by name.
struct Room {
    room: Vec<u8>,
    branch: Vec<u8>,
    zone: Vec<u8>,
    doc: Document,
    last_seen_seq: u64,
    outbox: Vec<Op>,
    awareness: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    version_names: Vec<Vec<u8>>,
    version_states: HashMap<Vec<u8>, (u64, Vec<u8>)>,
}

/// A replica's connection carrying several room subscriptions, each keyed by the
/// channel the client assigned it.
pub struct ClientSession {
    client: ClientId,
    actor: Option<Vec<u8>>,
    app_id: Vec<u8>,
    schema_version: u32,
    /// The schema the server advertised it serves this connection — its active
    /// version and bytes — learned from a `SchemaAdvert`. Distinct from the
    /// declared `schema_version`: a dynamic client declares 0 and learns the
    /// concrete version the server picked here. `None` until an advert arrives.
    active_schema: Option<(u32, Vec<u8>)>,
    rooms: HashMap<Channel, Room>,
    next_channel: u32,
    /// Op batches the server refused, awaiting the app's drain. Held at the
    /// session, not the room, so one [`take_rejected`](Self::take_rejected)
    /// surfaces rejections across every channel; each entry names its channel.
    rejected: Vec<Rejected>,
    /// Room redirects the server sent — a node declining to serve a room it does
    /// not lead, naming the leader to reconnect to. Buffered at the session for
    /// the transport to drain and act on; the core holds no socket to reconnect.
    redirects: Vec<Redirect>,
    /// The last branch set the server reported per room — the view a
    /// [`Message::Branches`] reply updates. Keyed by room rather than channel:
    /// branch management is a room-level operation a client may run before it
    /// holds any subscription. Empty until a list request or a mutation is
    /// answered.
    branches: HashMap<Vec<u8>, Vec<BranchInfo>>,
    /// The last diff result the server returned per room — the change list a
    /// [`Message::DiffResult`] reply carried, decoded. Room-keyed like the branch
    /// view; a fresh query replaces the room's entry. Empty until a diff query is
    /// answered.
    diffs: HashMap<Vec<u8>, Vec<Change>>,
    /// The outcome of each clone-room request, keyed by the destination room — the
    /// `created` flag a [`Message::CloneRoomResult`] reply carried. Keyed by `dst`
    /// so a client reads the result of the clone it issued. Empty until a clone is
    /// answered.
    clone_results: HashMap<Vec<u8>, bool>,
}

impl ClientSession {
    /// A session for `client` holding no rooms yet. It opens as a relay — no app
    /// named, no schema version — until [`declare_app`](Self::declare_app) names one.
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            actor: None,
            app_id: Vec::new(),
            schema_version: 0,
            active_schema: None,
            rooms: HashMap::new(),
            next_channel: 0,
            rejected: Vec::new(),
            redirects: Vec::new(),
            branches: HashMap::new(),
            diffs: HashMap::new(),
            clone_results: HashMap::new(),
        }
    }

    /// Declare the app this replica speaks for and the schema version it targets,
    /// carried in the next [`hello`](Self::hello). An empty `app_id` (or the
    /// default) opens a relay connection; a named app with `schema_version` 0 is a
    /// dynamic client that adopts the server's head. Call before `hello`.
    pub fn declare_app(&mut self, app_id: &[u8], schema_version: u32) {
        self.app_id = app_id.to_vec();
        self.schema_version = schema_version;
    }

    /// The declared app id — empty for a relay connection.
    pub fn app_id(&self) -> &[u8] {
        &self.app_id
    }

    /// The declared schema version — 0 for a relay or a dynamic client.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// The active version the server advertised it serves this connection, or
    /// `None` before any `SchemaAdvert` arrives. A dynamic client (declared
    /// version 0) reads its concrete served version here.
    pub fn active_schema_version(&self) -> Option<u32> {
        self.active_schema.as_ref().map(|(v, _)| *v)
    }

    /// The bytes of the schema the server advertised, or `None` before any
    /// `SchemaAdvert` arrives.
    pub fn active_schema(&self) -> Option<&[u8]> {
        self.active_schema.as_ref().map(|(_, s)| s.as_slice())
    }

    /// The opening frame, naming this replica and the app it speaks for.
    pub fn hello(&self) -> Message {
        Message::Hello {
            client: self.client,
            app_id: self.app_id.clone(),
            schema_version: self.schema_version,
        }
    }

    /// Present an opaque credential for the server to verify. The server derives
    /// the actor and returns it in AuthOk; the client never asserts its own.
    pub fn auth(&self, credential: &[u8]) -> Message {
        Message::Auth {
            credential: credential.to_vec(),
        }
    }

    /// The server-derived actor for this session, once AuthOk has arrived.
    pub fn actor(&self) -> Option<&[u8]> {
        self.actor.as_deref()
    }

    /// Join `room` on a fresh channel, requesting everything from the start.
    /// Returns the assigned channel and the Subscribe frame to send. Scoped to
    /// the default `main` branch and the whole room (every zone the actor may
    /// read); [`subscribe_branch`](Self::subscribe_branch) names another branch,
    /// [`subscribe_zone`](Self::subscribe_zone) narrows to one zone.
    pub fn subscribe(&mut self, room: &[u8]) -> (Channel, Message) {
        self.subscribe_inner(room, b"", b"")
    }

    /// Join `branch` of `room` on a fresh channel, requesting everything from the
    /// start. An empty `branch` is the default `main`. Returns the assigned
    /// channel and the Subscribe frame to send.
    pub fn subscribe_branch(&mut self, room: &[u8], branch: &[u8]) -> (Channel, Message) {
        self.subscribe_inner(room, branch, b"")
    }

    /// Join `room` on a fresh channel scoped to one `zone`, requesting everything
    /// from the start. An empty `zone` is the whole room (every zone the actor may
    /// read); a named `zone` narrows the stream to that partition plus the unzoned
    /// root it is entitled to. Scoped to the default `main` branch. Returns the
    /// assigned channel and the Subscribe frame to send.
    pub fn subscribe_zone(&mut self, room: &[u8], zone: &[u8]) -> (Channel, Message) {
        self.subscribe_inner(room, b"", zone)
    }

    fn subscribe_inner(&mut self, room: &[u8], branch: &[u8], zone: &[u8]) -> (Channel, Message) {
        let channel = Channel(self.next_channel);
        self.next_channel += 1;
        self.rooms.insert(
            channel,
            Room {
                room: room.to_vec(),
                branch: branch.to_vec(),
                zone: zone.to_vec(),
                doc: Document::new(self.client),
                last_seen_seq: 0,
                outbox: Vec::new(),
                awareness: HashMap::new(),
                version_names: Vec::new(),
                version_states: HashMap::new(),
            },
        );
        (
            channel,
            Message::Subscribe {
                channel,
                room: room.to_vec(),
                branch: branch.to_vec(),
                zone: zone.to_vec(),
                last_seen_seq: 0,
            },
        )
    }

    /// Re-issue the Subscribe for a held channel from its caught-up position, so
    /// a reconnect resumes with a delta rather than the whole room. `None` if
    /// the channel isn't held.
    pub fn resume(&self, channel: Channel) -> Option<Message> {
        let room = self.rooms.get(&channel)?;
        Some(Message::Subscribe {
            channel,
            room: room.room.clone(),
            branch: room.branch.clone(),
            zone: room.zone.clone(),
            last_seen_seq: room.last_seen_seq,
        })
    }

    /// Re-emit the authored ops on `channel` the server has not yet acknowledged,
    /// as one `Message::Ops` to replay after a reconnect. `None` if the channel
    /// isn't held or nothing is outstanding. The server deduplicates a replayed
    /// op by its id, so replaying more than it kept is harmless.
    pub fn resend(&self, channel: Channel) -> Option<Message> {
        let room = self.rooms.get(&channel)?;
        if room.outbox.is_empty() {
            return None;
        }
        Some(Message::Ops {
            channel,
            ops: room.outbox.clone(),
        })
    }

    /// How many authored ops on `channel` await acknowledgement — the depth of
    /// the offline queue. `0` if the channel isn't held.
    pub fn outbox_len(&self, channel: Channel) -> usize {
        self.rooms.get(&channel).map_or(0, |r| r.outbox.len())
    }

    /// Drain the op batches the server has refused since the last call — the
    /// `onOpsRejected` observation. Each entry names the channel, the reason, and
    /// the rejected ops (still carrying their bytes) so the app can show, discard,
    /// or export them. Draining, so a second call reports nothing new; empty when
    /// no rejection has arrived.
    pub fn take_rejected(&mut self) -> Vec<Rejected> {
        std::mem::take(&mut self.rejected)
    }

    /// Drain the room redirects the server has sent since the last call — the
    /// signal that a room's leader is elsewhere. Each entry names the room and the
    /// leader's advertise address for the transport to reconnect to. Draining, so
    /// a second call reports nothing new; empty when no redirect has arrived.
    pub fn take_redirects(&mut self) -> Vec<Redirect> {
        std::mem::take(&mut self.redirects)
    }

    /// Apply a local edit to `channel`'s room and return the ops to broadcast.
    /// The seen sequence is the server's, so an unacknowledged local write
    /// leaves it untouched until the ops come back with a sequence assigned.
    /// `None` if the channel isn't held.
    pub fn edit<F>(&mut self, channel: Channel, f: F) -> Option<Message>
    where
        F: FnOnce(&mut MapCursor),
    {
        let ops = self.rooms.get_mut(&channel)?.doc.transact(f);
        self.enqueue_ops(channel, ops)
    }

    /// Like [`edit`](Self::edit), but the emitted ops form one atomic
    /// transaction: a peer folds them in all-or-nothing, never observing a
    /// partial group. The ops travel as an ordinary `Message::Ops` — the
    /// transaction membership rides on the ops themselves. `None` if the channel
    /// isn't held.
    pub fn atomic_edit<F>(&mut self, channel: Channel, f: F) -> Option<Message>
    where
        F: FnOnce(&mut MapCursor),
    {
        let ops = self.rooms.get_mut(&channel)?.doc.atomic_transact(f);
        self.enqueue_ops(channel, ops)
    }

    /// Begin recording an atomic transaction on `channel`'s room: subsequent
    /// [`edit`](Self::edit)s accumulate into one group until
    /// [`commit_atomic`](Self::commit_atomic). For callers that build a group
    /// across several calls rather than in one closure. `None` if the channel
    /// isn't held.
    pub fn begin_atomic(&mut self, channel: Channel) -> Option<()> {
        self.rooms.get_mut(&channel)?.doc.begin_atomic();
        Some(())
    }

    /// Commit the atomic transaction opened by [`begin_atomic`](Self::begin_atomic)
    /// on `channel`, returning the group's ops as one `Message::Ops` to send.
    /// `None` if the channel isn't held.
    pub fn commit_atomic(&mut self, channel: Channel) -> Option<Message> {
        let ops = self.rooms.get_mut(&channel)?.doc.commit_atomic();
        self.enqueue_ops(channel, ops)
    }

    /// Record ops authored through [`document_mut`](Self::document_mut) into
    /// `channel`'s outbox and frame them as the `Message::Ops` to send, so an
    /// edit made through the path façade is acknowledged and resent exactly like
    /// one made through [`edit`](Self::edit). During an atomic transaction the
    /// façade's per-call ops are empty (they accumulate in the replica until
    /// commit), so nothing is enqueued until the group ships. `None` if the
    /// channel isn't held.
    pub fn enqueue_ops(&mut self, channel: Channel, ops: Vec<Op>) -> Option<Message> {
        let room = self.rooms.get_mut(&channel)?;
        room.outbox.extend(ops.iter().cloned());
        Some(Message::Ops { channel, ops })
    }

    /// Publish an ephemeral awareness entry on `channel`'s room, returning the
    /// frame to send. `None` if the channel isn't held. The entry is transient —
    /// it is not stored locally or reflected back.
    pub fn set_awareness(&self, channel: Channel, key: &[u8], value: &[u8]) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::AwarenessSet {
            channel,
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    /// A peer's awareness entry on `channel`, by publishing actor and key.
    pub fn awareness(&self, channel: Channel, actor: &[u8], key: &[u8]) -> Option<&[u8]> {
        self.rooms
            .get(&channel)?
            .awareness
            .get(&(actor.to_vec(), key.to_vec()))
            .map(Vec::as_slice)
    }

    /// How many awareness entries `channel` currently holds.
    pub fn awareness_len(&self, channel: Channel) -> usize {
        self.rooms.get(&channel).map_or(0, |r| r.awareness.len())
    }

    /// Capture the current state of `channel`'s room as version `name`, returning
    /// the request frame. `None` if the channel isn't held. The server's reply
    /// updates the [`versions`](ClientSession::versions) view.
    pub fn create_version(&self, channel: Channel, name: &[u8]) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::VersionCreate {
            channel,
            name: name.to_vec(),
        })
    }

    /// Rename version `from` to `to` on `channel`'s room, returning the request
    /// frame. `None` if the channel isn't held.
    pub fn rename_version(&self, channel: Channel, from: &[u8], to: &[u8]) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::VersionRename {
            channel,
            from: from.to_vec(),
            to: to.to_vec(),
        })
    }

    /// Delete version `name` on `channel`'s room, returning the request frame.
    /// `None` if the channel isn't held.
    pub fn delete_version(&self, channel: Channel, name: &[u8]) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::VersionDelete {
            channel,
            name: name.to_vec(),
        })
    }

    /// Request the version names of `channel`'s room, returning the request
    /// frame. `None` if the channel isn't held. The reply updates the
    /// [`versions`](ClientSession::versions) view.
    pub fn list_versions(&self, channel: Channel) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::VersionList { channel })
    }

    /// Request the captured state of version `name` on `channel`'s room,
    /// returning the request frame. `None` if the channel isn't held. A hit
    /// updates the [`version_state`](ClientSession::version_state) view.
    pub fn fetch_version(&self, channel: Channel, name: &[u8]) -> Option<Message> {
        self.rooms.get(&channel)?;
        Some(Message::VersionFetch {
            channel,
            name: name.to_vec(),
        })
    }

    /// The version names last reported for `channel`'s room, or `None` if the
    /// channel isn't held. Empty until a list request or a mutation is answered.
    pub fn versions(&self, channel: Channel) -> Option<&[Vec<u8>]> {
        self.rooms.get(&channel).map(|r| r.version_names.as_slice())
    }

    /// The captured state of a fetched version of `channel`'s room, by name, once
    /// a fetch has returned it. `None` if the channel isn't held or no such
    /// version state has been fetched.
    pub fn version_state(&self, channel: Channel, name: &[u8]) -> Option<&[u8]> {
        self.rooms
            .get(&channel)?
            .version_states
            .get(name)
            .map(|(_, state)| state.as_slice())
    }

    /// Request the branches of `room`, returning the request frame. The reply
    /// updates the [`branches`](ClientSession::branches) view. Room-keyed: a client
    /// may enumerate a room's branches before it subscribes any of them.
    pub fn list_branches(&self, room: &[u8]) -> Message {
        Message::BranchList {
            room: room.to_vec(),
        }
    }

    /// Fork a fresh branch `name` off `from_branch`'s HEAD in `room`, returning the
    /// request frame. The reply carries the fresh branch set.
    pub fn fork_branch(&self, room: &[u8], name: &[u8], from_branch: &[u8]) -> Message {
        Message::BranchFork {
            room: room.to_vec(),
            name: name.to_vec(),
            from_branch: from_branch.to_vec(),
        }
    }

    /// Fork a fresh branch `name` off the snapshot of version `version` in `room`,
    /// returning the request frame. The reply carries the fresh branch set.
    pub fn fork_branch_from_version(&self, room: &[u8], name: &[u8], version: &[u8]) -> Message {
        Message::BranchForkFromVersion {
            room: room.to_vec(),
            name: name.to_vec(),
            version: version.to_vec(),
        }
    }

    /// Restore `room` to version `version` as a fresh branch `name`, switching the
    /// active HEAD to it. Returns the request frame; the reply carries the fresh
    /// branch set.
    pub fn restore_branch(&self, room: &[u8], name: &[u8], version: &[u8]) -> Message {
        Message::BranchRestore {
            room: room.to_vec(),
            name: name.to_vec(),
            version: version.to_vec(),
        }
    }

    /// Publish `room`'s active editor branch onto the read-only `published` branch,
    /// returning the request frame. The reply carries the fresh branch set.
    pub fn publish_branch(&self, room: &[u8], published: &[u8]) -> Message {
        Message::BranchPublish {
            room: room.to_vec(),
            published: published.to_vec(),
        }
    }

    /// Delete branch `name` of `room`, returning the request frame. The default
    /// `main` is never deletable. The reply carries the fresh branch set.
    pub fn delete_branch(&self, room: &[u8], name: &[u8]) -> Message {
        Message::BranchDelete {
            room: room.to_vec(),
            name: name.to_vec(),
        }
    }

    /// The branch set last reported for `room`, or `None` if none has been
    /// reported. Empty until a list request or a branch mutation is answered.
    pub fn branches(&self, room: &[u8]) -> Option<&[BranchInfo]> {
        self.branches.get(room).map(Vec::as_slice)
    }

    /// Request the structural diff turning state `a` into state `b` in `room`,
    /// returning the request frame. `kind` selects whether `a`/`b` name two saved
    /// versions or two branches. The reply updates the [`diff`](Self::diff) view.
    /// Room-keyed like branch management: a client may diff a room before it
    /// subscribes any of its branches.
    pub fn diff_query(&self, room: &[u8], kind: DiffKind, a: &[u8], b: &[u8]) -> Message {
        Message::DiffQuery {
            room: room.to_vec(),
            kind,
            a: a.to_vec(),
            b: b.to_vec(),
        }
    }

    /// The change list from the last diff query answered for `room`, or `None` if
    /// none has been. An empty diff is an empty slice, not `None`.
    pub fn diff(&self, room: &[u8]) -> Option<&[Change]> {
        self.diffs.get(room).map(Vec::as_slice)
    }

    /// Duplicate room `src`'s live state into a fresh room `dst`, returning the
    /// request frame. Room-keyed like branch management: a client may clone a room
    /// before it subscribes any of it. The reply folds into the
    /// [`clone_result`](Self::clone_result) view keyed by `dst`.
    pub fn clone_room(&self, src: &[u8], dst: &[u8]) -> Message {
        Message::CloneRoom {
            src: src.to_vec(),
            dst: dst.to_vec(),
        }
    }

    /// The outcome of the last clone answered for destination `dst`: `Some(true)`
    /// if `dst` was minted from the source's state, `Some(false)` if the clone was
    /// a no-op (source unknown or `dst` already existed), `None` if no clone into
    /// `dst` has been answered.
    pub fn clone_result(&self, dst: &[u8]) -> Option<bool> {
        self.clone_results.get(dst).copied()
    }

    /// Leave the room on `channel`, dropping its replica. Returns the Unsubscribe
    /// frame to send, or `None` if the channel isn't held.
    pub fn unsubscribe(&mut self, channel: Channel) -> Option<Message> {
        self.rooms.remove(&channel)?;
        Some(Message::Unsubscribe { channel })
    }

    /// Fold one server message into the addressed room. An op delta applies in
    /// place; a snapshot replaces that room's replica with the server's state up
    /// to its tagged sequence. Frames the server never sends, a frame for a
    /// channel this session does not hold, and a snapshot that fails to decode
    /// are refused without touching any replica.
    pub fn receive(&mut self, msg: Message) -> Result<(), ClientError> {
        match msg {
            Message::Ops { channel, ops } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // The delta is a contiguous run of ops at the head, each holding
                // one server sequence, so the seen sequence advances by the batch
                // length — a redelivered op still occupies its slot even though
                // the replica deduplicates it away.
                let count = ops.len() as u64;
                for op in &ops {
                    room.doc.apply(op);
                }
                room.last_seen_seq += count;
                Ok(())
            }
            Message::Snapshot {
                channel,
                seq,
                state,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // Adopt the server's state but keep our own identity and op-seq
                // high-water mark for the ops we author next, so a re-mint can't
                // collide with an op already made durable. A decode failure
                // leaves the room untouched.
                let doc = Document::decode_state_as(self.client, room.doc.next_seq(), &state)
                    .map_err(|_| ClientError::BadSnapshot)?;
                room.doc = doc;
                room.last_seen_seq = seq;
                Ok(())
            }
            Message::AuthOk { actor } => {
                self.actor = Some(actor);
                Ok(())
            }
            // The server advertises the schema it serves this connection; record
            // its active version and bytes, the latest advert winning. It drives no
            // room replica.
            Message::SchemaAdvert {
                schema_version,
                schema,
            } => {
                self.active_schema = Some((schema_version, schema));
                Ok(())
            }
            Message::AwarenessUpdate {
                channel,
                actor,
                key,
                value,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // Last-writer-wins per (actor, key); the peer's latest entry
                // replaces any prior one.
                room.awareness.insert((actor, key), value);
                Ok(())
            }
            Message::AwarenessClear { channel, actor } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // The actor's presence expired; drop all of its entries.
                room.awareness.retain(|(a, _), _| *a != actor);
                Ok(())
            }
            Message::AwarenessClearKey {
                channel,
                actor,
                key,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // A single entry's timed TTL expired; drop just that (actor, key).
                room.awareness.remove(&(actor, key));
                Ok(())
            }
            Message::Error { code, message, .. } => Err(ClientError::Server { code, message }),
            Message::Accepted { channel, through } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // Drop every authored op the server has durably logged. The
                // per-client op sequence is the stable ack key — it is the dedup
                // identity, so a resent op re-acks to the same frontier and the
                // prune is idempotent.
                room.outbox.retain(|op| op.id.seq > through);
                Ok(())
            }
            Message::OpsRejected {
                channel,
                seqs,
                reason,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // The refused ops will never be acknowledged, so they must leave
                // the outbox — else `resend` would replay them forever. Move them
                // to the rejected buffer, keeping the op bytes so the app can
                // export them, and leave the rest queued.
                let refused: HashSet<u64> = seqs.into_iter().collect();
                let (ops, remaining) = room
                    .outbox
                    .drain(..)
                    .partition(|op| refused.contains(&op.id.seq));
                room.outbox = remaining;
                self.rejected.push(Rejected {
                    channel,
                    reason,
                    ops,
                });
                Ok(())
            }
            // The room's leader is elsewhere; buffer the target for the transport
            // to reconnect to. It names a room, not a held channel — the subscribe
            // did not take — so no replica is touched.
            Message::Redirect { room, leader_addr } => {
                self.redirects.push(Redirect { room, leader_addr });
                Ok(())
            }
            // `Ack` reports a client's applied sequence to the server; it never
            // travels the other way.
            Message::Ack { .. } => Err(ClientError::UnexpectedMessage("server sent an ack")),
            Message::Auth { .. } => Err(ClientError::UnexpectedMessage("server sent auth")),
            Message::AwarenessSet { .. } => Err(ClientError::UnexpectedMessage(
                "server sent an awareness set",
            )),
            Message::Hello { .. } => Err(ClientError::UnexpectedMessage("server sent hello")),
            Message::Subscribe { .. } => {
                Err(ClientError::UnexpectedMessage("server sent subscribe"))
            }
            Message::Unsubscribe { .. } => {
                Err(ClientError::UnexpectedMessage("server sent unsubscribe"))
            }
            Message::Versions { channel, names } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // The server's list is authoritative — it replaces the view.
                room.version_names = names;
                Ok(())
            }
            Message::VersionState {
                channel,
                name,
                seq,
                state,
            } => {
                let room = self
                    .rooms
                    .get_mut(&channel)
                    .ok_or(ClientError::UnknownChannel(channel))?;
                // Cache the fetched state under its name for the embedder to read.
                room.version_states.insert(name, (seq, state));
                Ok(())
            }
            // Version requests only travel client-to-server.
            Message::VersionCreate { .. }
            | Message::VersionRename { .. }
            | Message::VersionDelete { .. }
            | Message::VersionList { .. }
            | Message::VersionFetch { .. } => Err(ClientError::UnexpectedMessage(
                "server sent a version request",
            )),
            Message::Branches { room, branches } => {
                // The server's set is authoritative — it replaces the view.
                self.branches.insert(room, branches);
                Ok(())
            }
            Message::DiffResult { room, changes } => {
                // A malformed change list is refused without touching the view.
                let changes = decode_changes(&changes).map_err(|_| ClientError::BadDiff)?;
                self.diffs.insert(room, changes);
                Ok(())
            }
            Message::CloneRoomResult { dst, created } => {
                self.clone_results.insert(dst, created);
                Ok(())
            }
            // Branch and diff requests only travel client-to-server.
            Message::BranchList { .. }
            | Message::BranchFork { .. }
            | Message::BranchForkFromVersion { .. }
            | Message::BranchRestore { .. }
            | Message::BranchPublish { .. }
            | Message::BranchDelete { .. }
            | Message::DiffQuery { .. } => Err(ClientError::UnexpectedMessage(
                "server sent a branch or diff request",
            )),
            // A clone request only travels client-to-server.
            Message::CloneRoom { .. } => Err(ClientError::UnexpectedMessage(
                "server sent a clone request",
            )),
            // Replication frames travel node-to-node; a client never sees one.
            Message::Replicate { .. } => {
                Err(ClientError::UnexpectedMessage("server sent a replicate"))
            }
            Message::ReplicaAck { .. } => {
                Err(ClientError::UnexpectedMessage("server sent a replica ack"))
            }
            Message::Gossip { .. } => Err(ClientError::UnexpectedMessage("server sent a gossip")),
        }
    }

    /// The local replica for `channel`'s room, if held.
    pub fn document(&self, channel: Channel) -> Option<&Document> {
        self.rooms.get(&channel).map(|r| &r.doc)
    }

    /// Mutable access to `channel`'s replica, for an embedder that edits through
    /// the path façade rather than the [`edit`](ClientSession::edit) closure —
    /// the ops it emits still travel to the server as a `Message::Ops` the caller
    /// builds. `None` if the channel isn't held.
    pub fn document_mut(&mut self, channel: Channel) -> Option<&mut Document> {
        self.rooms.get_mut(&channel).map(|r| &mut r.doc)
    }

    /// The highest server sequence `channel`'s room has caught up to.
    pub fn last_seen_seq(&self, channel: Channel) -> Option<u64> {
        self.rooms.get(&channel).map(|r| r.last_seen_seq)
    }

    /// The room name bound to `channel`, if held.
    pub fn room(&self, channel: Channel) -> Option<&[u8]> {
        self.rooms.get(&channel).map(|r| r.room.as_slice())
    }

    /// The branch bound to `channel`, if held — empty for the default `main`.
    pub fn branch(&self, channel: Channel) -> Option<&[u8]> {
        self.rooms.get(&channel).map(|r| r.branch.as_slice())
    }

    /// The zone selector bound to `channel`, if held — empty for the whole room.
    pub fn zone(&self, channel: Channel) -> Option<&[u8]> {
        self.rooms.get(&channel).map(|r| r.zone.as_slice())
    }
}
