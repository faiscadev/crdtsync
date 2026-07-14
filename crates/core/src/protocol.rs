//! Wire protocol — the framed messages two replicas exchange over a connection.
//!
//! A connection opens with an 8-byte header: a 4-byte [`MAGIC`] identifying the
//! protocol and a 4-byte version for codec negotiation. Every frame after that
//! is one [`Message`]: a tag byte and a payload. Op batches reuse the op codec,
//! so the wire and the durable log share one encoding. Decoding is total —
//! malformed bytes yield a [`ProtocolError`], never a panic.

use crate::clientid::ClientId;
use crate::codec::{
    decode_ops, encode_ops, put_bytes, put_u16, put_u32, put_u64, put_u8, Cursor, DecodeError,
};
use crate::op::Op;

/// Identifies a crdtsync stream, so a foreign connection is rejected at once.
pub const MAGIC: u32 = u32::from_le_bytes(*b"CRDT");

/// The protocol version this build speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Why a byte string could not be decoded into a header or message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ProtocolError {
    /// The header did not lead with [`MAGIC`].
    BadMagic,
    /// The input ended before a field was fully read.
    UnexpectedEof,
    /// A tag byte named no known variant.
    BadTag { what: &'static str, tag: u8 },
    /// A text field held bytes that are not valid UTF-8.
    BadUtf8,
    /// Bytes remained after decoding a complete header or message.
    TrailingBytes,
    /// An op batch payload was itself malformed.
    Op(DecodeError),
}

impl From<DecodeError> for ProtocolError {
    fn from(e: DecodeError) -> Self {
        match e {
            DecodeError::UnexpectedEof => ProtocolError::UnexpectedEof,
            DecodeError::BadUtf8 => ProtocolError::BadUtf8,
            DecodeError::TrailingBytes => ProtocolError::TrailingBytes,
            DecodeError::BadTag { what, tag } => ProtocolError::BadTag { what, tag },
        }
    }
}

/// A connection-local handle for one room subscription. The client assigns it
/// at Subscribe; every op batch, snapshot, and unsubscribe on that subscription
/// names it, so several rooms multiplex over one connection. The handle is what
/// stays stable as a subscription later widens to `(room, branch, zone)`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Channel(pub u32);

/// A cluster member's liveness in the SWIM-style gossip failure detector,
/// disseminated in each [`Message::Gossip`] tuple. Variants are ordered least- to
/// most-suspicious, so the derived `Ord` gives `Dead > Suspect > Alive` — the
/// tie-break the anti-entropy merge applies at equal incarnation, letting a
/// detected failure win over stale optimism.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum MemberState {
    /// Reachable and participating — the default a member is learned at.
    Alive,
    /// Missed enough direct gossip probes to be doubted, but not yet declared
    /// dead. Still routed to (optimistically live) until it reaches `Dead`.
    Suspect,
    /// Confirmed unreachable — excluded from room leadership cluster-wide.
    Dead,
}

/// A closed set of failure reasons the server reports to a client.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ErrorCode {
    ProtocolViolation,
    UnsupportedVersion,
    AuthFailed,
    UnknownRoom,
    Internal,
    /// The authenticated actor is not permitted the requested action.
    Forbidden,
    /// The client's version cannot reach the room's governing version across a
    /// back-compatible path — a breaking gap lies between them. Surfaced at
    /// subscribe as the `onUpdateRequired` signal, before the client joins; the
    /// app prompts an update or falls back to read-only.
    UpdateRequired,
}

/// One branch of a room as a client observes it over the wire: its `name`, the
/// `fork_point` it shares history up to, its own `head` position, and whether it
/// is a read-only `published` target. Marshaled in a [`Message::Branches`] reply
/// so a client can enumerate a room's branches and decide which to subscribe or
/// act on.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BranchInfo {
    pub name: Vec<u8>,
    pub fork_point: u64,
    pub head: u64,
    pub published: bool,
}

/// One framed message on the wire.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message {
    /// Opens a connection, naming the client and the app it speaks for. `app_id`
    /// selects the tenant whose registered schema (if any) the server enforces —
    /// empty means no app, a relay connection. `schema_version` is the single
    /// version the client speaks; `0` declares none (a dynamic client that adopts
    /// whatever the server serves). The server resolves `{app_id, schema_version}`
    /// against its registry — an unknown version for a registered app is refused.
    Hello {
        client: ClientId,
        app_id: Vec<u8>,
        schema_version: u32,
    },
    /// Presents an opaque credential for the server to verify. The core does not
    /// parse it; a deployment-configured verifier interprets the bytes.
    Auth { credential: Vec<u8> },
    /// Reports a verified credential, carrying the server-derived actor id. The
    /// client never asserts its own actor — it learns it here.
    AuthOk { actor: Vec<u8> },
    /// The enforcing server's advertisement of the schema it is serving this
    /// connection: `schema_version` is the active version, `schema` its bytes (a
    /// dynamic client that did not bundle adopts them; a client that already holds
    /// the version can ignore the body). A relay connection is never sent one.
    SchemaAdvert {
        schema_version: u32,
        schema: Vec<u8>,
    },
    /// Joins a room on `channel`, requesting every op past `last_seen_seq`. A
    /// subscription names its room and the `branch` within it — an empty `branch`
    /// is the default `main`. The replication unit is `(room, branch)`.
    Subscribe {
        channel: Channel,
        room: Vec<u8>,
        branch: Vec<u8>,
        last_seen_seq: u64,
    },
    /// Leaves the room bound to `channel`, freeing the handle.
    Unsubscribe { channel: Channel },
    /// A batch of ops to fold into `channel`'s room.
    Ops { channel: Channel, ops: Vec<Op> },
    /// A whole-replica state snapshot the server sends a subscriber that fell
    /// below a room's compaction floor, tagged with the channel it answers and
    /// the sequence it lands at.
    Snapshot {
        channel: Channel,
        seq: u64,
        state: Vec<u8>,
    },
    /// Acknowledges an author's durably-logged ops on `channel`: `through` is the
    /// highest per-client op sequence (`OpId.seq`) the server has committed for
    /// this client, so the author drains its outbox up to it. Sent to the author
    /// only — never fanned out to the room.
    Accepted { channel: Channel, through: u64 },
    /// Reports the server sequence the client has applied on `channel`, so the
    /// server can advance this client's tombstone-GC watermark.
    Ack { channel: Channel, seq: u64 },
    /// Publishes this client's ephemeral awareness entry `key` on `channel`'s
    /// room, replacing any prior value. Not durable — never logged or snapshotted.
    AwarenessSet {
        channel: Channel,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// A peer's awareness entry, fanned out to the room, tagged with the
    /// publishing actor so receivers know whose it is.
    AwarenessUpdate {
        channel: Channel,
        actor: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Drops all of `actor`'s awareness on `channel` — sent when that actor's
    /// presence expires (disconnect past the grace window, or a session-lifetime
    /// entry going away).
    AwarenessClear { channel: Channel, actor: Vec<u8> },
    /// Drops a single one of `actor`'s awareness entries — `key` — on `channel`,
    /// sent when that entry's timed TTL expires while the actor's other entries
    /// (and connection) live on.
    AwarenessClearKey {
        channel: Channel,
        actor: Vec<u8>,
        key: Vec<u8>,
    },
    /// Captures the current state of `channel`'s room as a named version.
    VersionCreate { channel: Channel, name: Vec<u8> },
    /// Renames a version of `channel`'s room.
    VersionRename {
        channel: Channel,
        from: Vec<u8>,
        to: Vec<u8>,
    },
    /// Deletes a named version of `channel`'s room.
    VersionDelete { channel: Channel, name: Vec<u8> },
    /// Requests the names of the versions of `channel`'s room.
    VersionList { channel: Channel },
    /// Requests the captured state of a named version of `channel`'s room.
    VersionFetch { channel: Channel, name: Vec<u8> },
    /// The current version names of `channel`'s room — the server's reply to a
    /// list request and the authoritative post-state after any version mutation.
    Versions {
        channel: Channel,
        names: Vec<Vec<u8>>,
    },
    /// A named version's captured state, the server's reply to a fetch of a
    /// version that exists, tagged with the sequence it covered.
    VersionState {
        channel: Channel,
        name: Vec<u8>,
        seq: u64,
        state: Vec<u8>,
    },
    /// A failure the server reports to the client: a closed-enum `code`, a
    /// human-readable `message`, and opaque `details` the core never parses —
    /// machine-readable specifics a client interprets by code. `details` is
    /// empty until a producer populates it.
    Error {
        code: ErrorCode,
        message: String,
        details: Vec<u8>,
    },
    /// The server's refusal of a batch of authored ops on `channel` — the actor's
    /// write was denied (auth revoked while offline), or the ops failed the
    /// enforcing server's validation. Sender-directed and non-fatal: the
    /// connection stays open, the named ops are neither ingested nor
    /// acknowledged, and the client drains them from its outbox to surface as the
    /// `onOpsRejected` signal for the app to show, discard, or export. `seqs` are
    /// the rejected ops' per-client sequences (`OpId.seq`), which the client
    /// resolves against its own outbox — it still holds the op bytes; `reason` is
    /// the closed-enum code for why (`Forbidden` for a revoked write).
    OpsRejected {
        channel: Channel,
        seqs: Vec<u64>,
        reason: ErrorCode,
    },
    /// This room's leader is elsewhere: `leader_addr` is the advertise address of
    /// the node that leads `room`, and the client must reconnect there to
    /// subscribe or write. A node that is not `room`'s leader sends this instead
    /// of serving the room — a follower does not serve it directly. Server-
    /// directed; a client that sends one commits a protocol violation.
    Redirect { room: Vec<u8>, leader_addr: Vec<u8> },
    /// A room's leader fans its freshly committed ops out to a follower replica:
    /// `ops` is the batch (the op codec, as an `Ops` write), `base_seq` the
    /// leader's compaction floor when it sent them, so the follower places them in
    /// the same server-sequence space. `epoch` is the leader's monotonic
    /// leadership generation for the room (a Raft term): a promotion bumps it
    /// strictly above any epoch the promoting node has seen, and a follower fences
    /// a frame whose `epoch` is below the highest it has seen, so a demoted-then-
    /// recovered stale leader cannot replicate. Node-to-node — never a client
    /// frame; a client that sends one commits a protocol violation.
    Replicate {
        room: Vec<u8>,
        branch: Vec<u8>,
        ops: Vec<Op>,
        base_seq: u64,
        epoch: u64,
    },
    /// A follower's acknowledgement of replicated ops: `through_seq` is the
    /// server sequence the follower's replica of `room` has now reached, the
    /// watermark the leader records per follower. Node-to-node — never a client
    /// frame; a client that sends one commits a protocol violation.
    ReplicaAck { room: Vec<u8>, through_seq: u64 },
    /// A node's advertisement of the cluster members it knows, for gossip
    /// membership discovery and SWIM-style failure detection: `members` is a set of
    /// `(node_id, advertise_addr, incarnation, state)` tuples — the node id a peer
    /// places with, the address it dials to reach that member, a monotonic per-node
    /// refutation counter, and the member's [`MemberState`]. A receiver merges each
    /// tuple into its own liveness view (anti-entropy: a higher incarnation wins,
    /// and at equal incarnation the more-suspicious state wins), so a node that
    /// boots knowing only a seed peer learns the whole cluster — and a node's
    /// failure propagates to every node — within a few gossip rounds. Node-to-node
    /// — never a client frame; a client that sends one commits a protocol
    /// violation.
    Gossip {
        members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState)>,
    },
    /// Requests the branches of `room` — the app-facing enumeration a client runs
    /// to discover a room's forks and published targets before subscribing one.
    /// Room-keyed rather than channel-keyed: branch management is a room-level
    /// operation a client may run before it holds any subscription.
    BranchList { room: Vec<u8> },
    /// The branches of `room`, in deterministic name order — the server's reply to
    /// a [`BranchList`](Message::BranchList) and the authoritative post-state after
    /// any branch mutation.
    Branches {
        room: Vec<u8>,
        branches: Vec<BranchInfo>,
    },
    /// Forks a fresh branch `name` off `from_branch`'s current HEAD in `room` — the
    /// wire form of a live-log fork. Replies with the fresh branch list.
    BranchFork {
        room: Vec<u8>,
        name: Vec<u8>,
        from_branch: Vec<u8>,
    },
    /// Forks a fresh branch `name` off the snapshot of named version `version` in
    /// `room` — the wire form of a snapshot fork. Replies with the fresh list.
    BranchForkFromVersion {
        room: Vec<u8>,
        name: Vec<u8>,
        version: Vec<u8>,
    },
    /// Restores `room` to named version `version` as a fresh branch `name`,
    /// switching the active HEAD to it. Replies with the fresh branch list.
    BranchRestore {
        room: Vec<u8>,
        name: Vec<u8>,
        version: Vec<u8>,
    },
    /// Publishes `room`'s active editor branch onto the read-only `published`
    /// branch. Replies with the fresh branch list.
    BranchPublish { room: Vec<u8>, published: Vec<u8> },
    /// Deletes branch `name` of `room`. The default `main` is never deletable.
    /// Replies with the fresh branch list.
    BranchDelete { room: Vec<u8>, name: Vec<u8> },
}

/// Encode the 8-byte connection header: [`MAGIC`] then the version.
pub fn encode_header(version: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&MAGIC.to_le_bytes());
    out[4..].copy_from_slice(&version.to_le_bytes());
    out
}

/// Decode the connection header, returning the peer's negotiated version.
pub fn decode_header(bytes: &[u8]) -> Result<u32, ProtocolError> {
    if bytes.len() < 8 {
        return Err(ProtocolError::UnexpectedEof);
    }
    if bytes.len() > 8 {
        return Err(ProtocolError::TrailingBytes);
    }
    let magic = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    Ok(u32::from_le_bytes(bytes[4..].try_into().unwrap()))
}

/// Encode one message to its byte string.
pub fn encode_message(m: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    match m {
        Message::Hello {
            client,
            app_id,
            schema_version,
        } => {
            put_u8(&mut out, 0);
            out.extend_from_slice(&client.as_bytes());
            put_bytes(&mut out, app_id);
            put_u32(&mut out, *schema_version);
        }
        Message::Subscribe {
            channel,
            room,
            branch,
            last_seen_seq,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, room);
            put_bytes(&mut out, branch);
            put_u64(&mut out, *last_seen_seq);
        }
        Message::Ops { channel, ops } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, channel.0);
            out.extend_from_slice(&encode_ops(ops));
        }
        Message::Snapshot {
            channel,
            seq,
            state,
        } => {
            put_u8(&mut out, 4);
            put_u32(&mut out, channel.0);
            put_u64(&mut out, *seq);
            put_bytes(&mut out, state);
        }
        Message::Unsubscribe { channel } => {
            put_u8(&mut out, 5);
            put_u32(&mut out, channel.0);
        }
        Message::Auth { credential } => {
            put_u8(&mut out, 6);
            put_bytes(&mut out, credential);
        }
        Message::AuthOk { actor } => {
            put_u8(&mut out, 7);
            put_bytes(&mut out, actor);
        }
        Message::SchemaAdvert {
            schema_version,
            schema,
        } => {
            put_u8(&mut out, 21);
            put_u32(&mut out, *schema_version);
            put_bytes(&mut out, schema);
        }
        Message::Accepted { channel, through } => {
            put_u8(&mut out, 18);
            put_u32(&mut out, channel.0);
            put_u64(&mut out, *through);
        }
        Message::Ack { channel, seq } => {
            put_u8(&mut out, 19);
            put_u32(&mut out, channel.0);
            put_u64(&mut out, *seq);
        }
        Message::AwarenessSet {
            channel,
            key,
            value,
        } => {
            put_u8(&mut out, 8);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        Message::AwarenessUpdate {
            channel,
            actor,
            key,
            value,
        } => {
            put_u8(&mut out, 9);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, actor);
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        Message::AwarenessClear { channel, actor } => {
            put_u8(&mut out, 10);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, actor);
        }
        Message::AwarenessClearKey {
            channel,
            actor,
            key,
        } => {
            put_u8(&mut out, 20);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, actor);
            put_bytes(&mut out, key);
        }
        Message::VersionCreate { channel, name } => {
            put_u8(&mut out, 11);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
        }
        Message::VersionRename { channel, from, to } => {
            put_u8(&mut out, 12);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, from);
            put_bytes(&mut out, to);
        }
        Message::VersionDelete { channel, name } => {
            put_u8(&mut out, 13);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
        }
        Message::VersionList { channel } => {
            put_u8(&mut out, 14);
            put_u32(&mut out, channel.0);
        }
        Message::VersionFetch { channel, name } => {
            put_u8(&mut out, 15);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
        }
        Message::Versions { channel, names } => {
            put_u8(&mut out, 16);
            put_u32(&mut out, channel.0);
            put_u32(
                &mut out,
                u32::try_from(names.len()).expect("version count exceeds u32"),
            );
            for name in names {
                put_bytes(&mut out, name);
            }
        }
        Message::VersionState {
            channel,
            name,
            seq,
            state,
        } => {
            put_u8(&mut out, 17);
            put_u32(&mut out, channel.0);
            put_bytes(&mut out, name);
            put_u64(&mut out, *seq);
            put_bytes(&mut out, state);
        }
        Message::Error {
            code,
            message,
            details,
        } => {
            put_u8(&mut out, 3);
            put_u16(&mut out, error_code_tag(*code));
            put_bytes(&mut out, message.as_bytes());
            put_bytes(&mut out, details);
        }
        Message::OpsRejected {
            channel,
            seqs,
            reason,
        } => {
            put_u8(&mut out, 22);
            put_u32(&mut out, channel.0);
            put_u16(&mut out, error_code_tag(*reason));
            put_u32(
                &mut out,
                u32::try_from(seqs.len()).expect("rejected op count exceeds u32"),
            );
            for seq in seqs {
                put_u64(&mut out, *seq);
            }
        }
        Message::Redirect { room, leader_addr } => {
            put_u8(&mut out, 23);
            put_bytes(&mut out, room);
            put_bytes(&mut out, leader_addr);
        }
        Message::Replicate {
            room,
            branch,
            ops,
            base_seq,
            epoch,
        } => {
            put_u8(&mut out, 24);
            put_bytes(&mut out, room);
            put_bytes(&mut out, branch);
            put_u64(&mut out, *base_seq);
            put_u64(&mut out, *epoch);
            out.extend_from_slice(&encode_ops(ops));
        }
        Message::ReplicaAck { room, through_seq } => {
            put_u8(&mut out, 25);
            put_bytes(&mut out, room);
            put_u64(&mut out, *through_seq);
        }
        Message::Gossip { members } => {
            put_u8(&mut out, 26);
            put_u32(
                &mut out,
                u32::try_from(members.len()).expect("member count exceeds u32"),
            );
            for (node, addr, incarnation, state) in members {
                put_bytes(&mut out, node);
                put_bytes(&mut out, addr);
                put_u64(&mut out, *incarnation);
                put_u8(&mut out, member_state_tag(*state));
            }
        }
        Message::BranchList { room } => {
            put_u8(&mut out, 27);
            put_bytes(&mut out, room);
        }
        Message::Branches { room, branches } => {
            put_u8(&mut out, 28);
            put_bytes(&mut out, room);
            put_u32(
                &mut out,
                u32::try_from(branches.len()).expect("branch count exceeds u32"),
            );
            for b in branches {
                put_bytes(&mut out, &b.name);
                put_u64(&mut out, b.fork_point);
                put_u64(&mut out, b.head);
                put_u8(&mut out, u8::from(b.published));
            }
        }
        Message::BranchFork {
            room,
            name,
            from_branch,
        } => {
            put_u8(&mut out, 29);
            put_bytes(&mut out, room);
            put_bytes(&mut out, name);
            put_bytes(&mut out, from_branch);
        }
        Message::BranchForkFromVersion {
            room,
            name,
            version,
        } => {
            put_u8(&mut out, 30);
            put_bytes(&mut out, room);
            put_bytes(&mut out, name);
            put_bytes(&mut out, version);
        }
        Message::BranchRestore {
            room,
            name,
            version,
        } => {
            put_u8(&mut out, 31);
            put_bytes(&mut out, room);
            put_bytes(&mut out, name);
            put_bytes(&mut out, version);
        }
        Message::BranchPublish { room, published } => {
            put_u8(&mut out, 32);
            put_bytes(&mut out, room);
            put_bytes(&mut out, published);
        }
        Message::BranchDelete { room, name } => {
            put_u8(&mut out, 33);
            put_bytes(&mut out, room);
            put_bytes(&mut out, name);
        }
    }
    out
}

/// Decode exactly one message; surplus bytes are an error.
pub fn decode_message(bytes: &[u8]) -> Result<Message, ProtocolError> {
    let mut cur = Cursor::new(bytes);
    let msg = match cur.u8()? {
        0 => Message::Hello {
            client: cur.client()?,
            app_id: cur.bytes()?,
            schema_version: cur.u32()?,
        },
        1 => {
            let channel = Channel(cur.u32()?);
            let room = cur.bytes()?;
            let branch = cur.bytes()?;
            let last_seen_seq = cur.u64()?;
            Message::Subscribe {
                channel,
                room,
                branch,
                last_seen_seq,
            }
        }
        // An op batch is length-framed and consumes the remainder after the
        // channel, so decoding it is already total.
        2 => {
            let channel = Channel(cur.u32()?);
            return Ok(Message::Ops {
                channel,
                ops: decode_ops(cur.rest()).map_err(ProtocolError::Op)?,
            });
        }
        3 => {
            let code = error_code(cur.u16()?)?;
            let message = cur.string()?;
            let details = cur.bytes()?;
            Message::Error {
                code,
                message,
                details,
            }
        }
        4 => {
            let channel = Channel(cur.u32()?);
            let seq = cur.u64()?;
            let state = cur.bytes()?;
            Message::Snapshot {
                channel,
                seq,
                state,
            }
        }
        5 => Message::Unsubscribe {
            channel: Channel(cur.u32()?),
        },
        6 => Message::Auth {
            credential: cur.bytes()?,
        },
        7 => Message::AuthOk {
            actor: cur.bytes()?,
        },
        21 => {
            let schema_version = cur.u32()?;
            let schema = cur.bytes()?;
            Message::SchemaAdvert {
                schema_version,
                schema,
            }
        }
        8 => {
            let channel = Channel(cur.u32()?);
            let key = cur.bytes()?;
            let value = cur.bytes()?;
            Message::AwarenessSet {
                channel,
                key,
                value,
            }
        }
        9 => {
            let channel = Channel(cur.u32()?);
            let actor = cur.bytes()?;
            let key = cur.bytes()?;
            let value = cur.bytes()?;
            Message::AwarenessUpdate {
                channel,
                actor,
                key,
                value,
            }
        }
        10 => {
            let channel = Channel(cur.u32()?);
            let actor = cur.bytes()?;
            Message::AwarenessClear { channel, actor }
        }
        20 => {
            let channel = Channel(cur.u32()?);
            let actor = cur.bytes()?;
            let key = cur.bytes()?;
            Message::AwarenessClearKey {
                channel,
                actor,
                key,
            }
        }
        11 => Message::VersionCreate {
            channel: Channel(cur.u32()?),
            name: cur.bytes()?,
        },
        12 => {
            let channel = Channel(cur.u32()?);
            let from = cur.bytes()?;
            let to = cur.bytes()?;
            Message::VersionRename { channel, from, to }
        }
        13 => Message::VersionDelete {
            channel: Channel(cur.u32()?),
            name: cur.bytes()?,
        },
        14 => Message::VersionList {
            channel: Channel(cur.u32()?),
        },
        15 => Message::VersionFetch {
            channel: Channel(cur.u32()?),
            name: cur.bytes()?,
        },
        16 => {
            let channel = Channel(cur.u32()?);
            let count = cur.u32()?;
            // Grow as records are read rather than trusting `count` to size the
            // allocation — a bogus count then fails on the missing bytes, not on
            // a giant up-front reservation.
            let mut names = Vec::new();
            for _ in 0..count {
                names.push(cur.bytes()?);
            }
            Message::Versions { channel, names }
        }
        17 => {
            let channel = Channel(cur.u32()?);
            let name = cur.bytes()?;
            let seq = cur.u64()?;
            let state = cur.bytes()?;
            Message::VersionState {
                channel,
                name,
                seq,
                state,
            }
        }
        18 => {
            let channel = Channel(cur.u32()?);
            let through = cur.u64()?;
            Message::Accepted { channel, through }
        }
        19 => {
            let channel = Channel(cur.u32()?);
            let seq = cur.u64()?;
            Message::Ack { channel, seq }
        }
        22 => {
            let channel = Channel(cur.u32()?);
            let reason = error_code(cur.u16()?)?;
            let count = cur.u32()?;
            // Grow as sequences are read rather than trusting `count` to size the
            // allocation — a bogus count then fails on the missing bytes, not on a
            // giant up-front reservation.
            let mut seqs = Vec::new();
            for _ in 0..count {
                seqs.push(cur.u64()?);
            }
            Message::OpsRejected {
                channel,
                seqs,
                reason,
            }
        }
        23 => {
            let room = cur.bytes()?;
            let leader_addr = cur.bytes()?;
            Message::Redirect { room, leader_addr }
        }
        // An op batch is length-framed and consumes the remainder after the
        // leading fields, so decoding it is already total.
        24 => {
            let room = cur.bytes()?;
            let branch = cur.bytes()?;
            let base_seq = cur.u64()?;
            let epoch = cur.u64()?;
            return Ok(Message::Replicate {
                room,
                branch,
                base_seq,
                epoch,
                ops: decode_ops(cur.rest()).map_err(ProtocolError::Op)?,
            });
        }
        25 => {
            let room = cur.bytes()?;
            let through_seq = cur.u64()?;
            Message::ReplicaAck { room, through_seq }
        }
        26 => {
            let count = cur.u32()?;
            // Grow as tuples are read rather than trusting `count` to size the
            // allocation — a bogus count then fails on the missing bytes, not on a
            // giant up-front reservation.
            let mut members = Vec::new();
            for _ in 0..count {
                let node = cur.bytes()?;
                let addr = cur.bytes()?;
                let incarnation = cur.u64()?;
                let state = member_state(cur.u8()?)?;
                members.push((node, addr, incarnation, state));
            }
            Message::Gossip { members }
        }
        27 => Message::BranchList { room: cur.bytes()? },
        28 => {
            let room = cur.bytes()?;
            let count = cur.u32()?;
            // Grow as records are read rather than trusting `count` to size the
            // allocation — a bogus count then fails on the missing bytes, not on a
            // giant up-front reservation.
            let mut branches = Vec::new();
            for _ in 0..count {
                let name = cur.bytes()?;
                let fork_point = cur.u64()?;
                let head = cur.u64()?;
                let published = cur.u8()? != 0;
                branches.push(BranchInfo {
                    name,
                    fork_point,
                    head,
                    published,
                });
            }
            Message::Branches { room, branches }
        }
        29 => {
            let room = cur.bytes()?;
            let name = cur.bytes()?;
            let from_branch = cur.bytes()?;
            Message::BranchFork {
                room,
                name,
                from_branch,
            }
        }
        30 => {
            let room = cur.bytes()?;
            let name = cur.bytes()?;
            let version = cur.bytes()?;
            Message::BranchForkFromVersion {
                room,
                name,
                version,
            }
        }
        31 => {
            let room = cur.bytes()?;
            let name = cur.bytes()?;
            let version = cur.bytes()?;
            Message::BranchRestore {
                room,
                name,
                version,
            }
        }
        32 => {
            let room = cur.bytes()?;
            let published = cur.bytes()?;
            Message::BranchPublish { room, published }
        }
        33 => {
            let room = cur.bytes()?;
            let name = cur.bytes()?;
            Message::BranchDelete { room, name }
        }
        tag => {
            return Err(ProtocolError::BadTag {
                what: "message",
                tag,
            })
        }
    };
    if !cur.at_end() {
        return Err(ProtocolError::TrailingBytes);
    }
    Ok(msg)
}

fn member_state_tag(state: MemberState) -> u8 {
    match state {
        MemberState::Alive => 0,
        MemberState::Suspect => 1,
        MemberState::Dead => 2,
    }
}

fn member_state(tag: u8) -> Result<MemberState, ProtocolError> {
    match tag {
        0 => Ok(MemberState::Alive),
        1 => Ok(MemberState::Suspect),
        2 => Ok(MemberState::Dead),
        tag => Err(ProtocolError::BadTag {
            what: "gossip member state",
            tag,
        }),
    }
}

fn error_code_tag(code: ErrorCode) -> u16 {
    match code {
        ErrorCode::ProtocolViolation => 0,
        ErrorCode::UnsupportedVersion => 1,
        ErrorCode::AuthFailed => 2,
        ErrorCode::UnknownRoom => 3,
        ErrorCode::Internal => 4,
        ErrorCode::Forbidden => 5,
        ErrorCode::UpdateRequired => 6,
    }
}

fn error_code(tag: u16) -> Result<ErrorCode, ProtocolError> {
    match tag {
        0 => Ok(ErrorCode::ProtocolViolation),
        1 => Ok(ErrorCode::UnsupportedVersion),
        2 => Ok(ErrorCode::AuthFailed),
        3 => Ok(ErrorCode::UnknownRoom),
        4 => Ok(ErrorCode::Internal),
        5 => Ok(ErrorCode::Forbidden),
        6 => Ok(ErrorCode::UpdateRequired),
        tag => Err(ProtocolError::BadTag {
            what: "error code",
            tag: tag as u8,
        }),
    }
}
