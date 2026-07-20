//! The access log — a record of every authorization decision.
//!
//! Authorization is the single seam every enforcement point consults, so it is
//! also the one place every access decision passes through. [`Audited`] wraps an
//! inner [`Authorizer`], forwarding its verdict unchanged while handing each
//! decision to a pluggable [`AccessLog`] sink. This captures read-only accesses
//! (subscribe) that generate no op, alongside the writes the op log already
//! records with their actor and timestamp — together the authoritative audit
//! trail.
//!
//! A record carries the actor, the action, the resource, and the verdict. It
//! never carries the credential that authenticated the actor, nor an awareness
//! entry's key or value: an awareness publish is logged as *that a publish was
//! decided*, never as the ephemeral presence it carried.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::auth::Identity;
use crate::authz::{Action, Authorizer, Decision as Verdict, Resource};
use crate::clock::Clock;

/// How an access was decided.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decision {
    Permitted,
    Denied,
}

/// One authorization decision, as handed to an [`AccessLog`]. Borrows the check's
/// inputs; a sink that retains records copies what it needs.
pub struct AccessRecord<'a> {
    pub actor: &'a [u8],
    pub action: Action,
    pub resource: &'a Resource<'a>,
    pub decision: Decision,
}

/// A sink for access decisions. A deployment plugs in its own — a file, a
/// database, a metrics pipeline; the engine only emits.
pub trait AccessLog {
    fn record(&self, record: &AccessRecord);
}

/// An access log from a plain closure, so a deployment (or a test) can supply the
/// sink inline.
impl<F> AccessLog for F
where
    F: Fn(&AccessRecord),
{
    fn record(&self, record: &AccessRecord) {
        self(record)
    }
}

/// An [`Authorizer`] that records every decision of an inner one. Compose it
/// around a real policy and set it as the registry's authorizer; the decision is
/// logged at the same instant it is enforced, so the log cannot drift from what
/// the server actually allowed.
pub struct Audited {
    inner: Box<dyn Authorizer>,
    log: Box<dyn AccessLog>,
}

impl Audited {
    pub fn new(inner: Box<dyn Authorizer>, log: Box<dyn AccessLog>) -> Self {
        Self { inner, log }
    }
}

impl Audited {
    fn record(&self, identity: &Identity, action: Action, resource: &Resource, granted: bool) {
        let decision = if granted {
            Decision::Permitted
        } else {
            Decision::Denied
        };
        self.log.record(&AccessRecord {
            actor: identity.actor(),
            action,
            resource,
            decision,
        });
    }
}

impl Authorizer for Audited {
    /// A direct (non-composed) caller — the control plane's `RegisterSchema`
    /// check — gets the inner verdict logged here as the final decision.
    fn authorize(&self, identity: &Identity, action: Action, resource: &Resource) -> bool {
        let granted = self.inner.authorize(identity, action, resource);
        self.record(identity, action, resource, granted);
        granted
    }

    /// Forward the inner verdict *unchanged*, preserving an
    /// [`Abstain`](Verdict::Abstain) so a wrapped [`Acl`](crate::acl::Acl) still
    /// defers to the schema `@auth` tier — flattening it to a deny here would
    /// silently disable schema grants for every audited deployment. The final
    /// decision is logged by [`observe`](Audited::observe), once the composition
    /// has resolved this abstain.
    fn decide(&self, identity: &Identity, action: Action, resource: &Resource) -> Verdict {
        self.inner.decide(identity, action, resource)
    }

    /// The data-plane composition reports the final verdict here — schema grants
    /// included — so the log reflects what was actually enforced.
    fn observe(&self, identity: &Identity, action: Action, resource: &Resource, granted: bool) {
        self.record(identity, action, resource, granted);
    }
}

// --- the durable audit trail ---------------------------------------------

/// The resource an audited event names, owned so a record outlives the borrow it
/// was captured from — the durable counterpart of [`Resource`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AuditResource {
    Room(Vec<u8>),
    App(Vec<u8>),
    Zone { room: Vec<u8>, zone: Vec<u8> },
}

impl AuditResource {
    /// The owned copy of a borrowed [`Resource`].
    pub fn from_resource(resource: &Resource) -> Self {
        match *resource {
            Resource::Room(room) => AuditResource::Room(room.to_vec()),
            Resource::App(app) => AuditResource::App(app.to_vec()),
            Resource::Zone { room, zone } => AuditResource::Zone {
                room: room.to_vec(),
                zone: zone.to_vec(),
            },
        }
    }

    /// The room this resource names — a [`Room`](AuditResource::Room) or the room a
    /// [`Zone`](AuditResource::Zone) partitions — so a room filter matches both. An
    /// [`App`](AuditResource::App) names no room.
    pub fn room(&self) -> Option<&[u8]> {
        match self {
            AuditResource::Room(room) | AuditResource::Zone { room, .. } => Some(room),
            AuditResource::App(_) => None,
        }
    }
}

/// One durable audit record: the security-relevant fact that `actor` took `action`
/// on `resource` at `timestamp` (wall-clock millis), decided `decision`. Immutable
/// and time-ordered — the log only ever appends one.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AuditRecord {
    pub timestamp: u64,
    pub actor: Vec<u8>,
    pub action: Action,
    pub resource: AuditResource,
    pub decision: Decision,
}

/// A read-only filter over the audit trail. Every set field must match for a record
/// to be returned; an all-`None` filter returns every record. The time window is
/// half-open `[since, until)` so adjacent windows compose without overlap.
#[derive(Clone, Default, Debug)]
pub struct AuditQuery {
    /// Only records whose actor equals this.
    pub actor: Option<Vec<u8>>,
    /// Only records of this action.
    pub action: Option<Action>,
    /// Only records whose resource names this room (a room or a zone within it).
    pub room: Option<Vec<u8>>,
    /// Only records at or after this wall-clock millisecond (inclusive).
    pub since: Option<u64>,
    /// Only records strictly before this wall-clock millisecond (exclusive).
    pub until: Option<u64>,
}

impl AuditQuery {
    fn matches(&self, record: &AuditRecord) -> bool {
        if let Some(actor) = &self.actor {
            if record.actor != *actor {
                return false;
            }
        }
        if let Some(action) = self.action {
            if record.action != action {
                return false;
            }
        }
        if let Some(room) = &self.room {
            if record.resource.room() != Some(room.as_slice()) {
                return false;
            }
        }
        if let Some(since) = self.since {
            if record.timestamp < since {
                return false;
            }
        }
        if let Some(until) = self.until {
            if record.timestamp >= until {
                return false;
            }
        }
        true
    }
}

/// The append-only audit trail — a structured file-log, the same durability shape
/// the op store uses: one length-framed record per event, flushed before the append
/// returns so a crash or a second handle sees it at once. It is **append-only**: the
/// only write is [`record`](AuditLog::record); nothing mutates or removes a stored
/// event, and the query path ([`query`](AuditLog::query) / [`read_all`](AuditLog::read_all))
/// is read-only.
///
/// A failed append is **not** swallowed — it returns the [`io::Error`] to a direct
/// caller and latches a failure flag [`healthy`](AuditLog::healthy) exposes, so a
/// dropped security event surfaces rather than passing silently.
///
/// Query is a straight scan over the whole log (v1): the security-event stream is
/// low-volume relative to the op stream, so no in-memory index is built yet — a
/// rebuildable index over actor/room/time is a scale follow-on if the trail grows
/// large.
pub struct AuditLog {
    path: PathBuf,
    /// The append handle, opened once. Interior-mutable so [`record`](AuditLog::record)
    /// takes `&self` — the sink and the query surface share one `Arc<AuditLog>`.
    writer: Mutex<File>,
    clock: Arc<dyn Clock>,
    /// Latched on the first append that failed — a dropped security event the
    /// operator surface reports rather than hiding.
    failed: AtomicBool,
}

impl AuditLog {
    /// Open (creating if absent) the append-only log at `path`, stamping each record
    /// with `clock`. Existing records are left untouched — a new append lands after
    /// them.
    pub fn open(path: impl AsRef<Path>, clock: Arc<dyn Clock>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => {
                std::fs::create_dir_all(parent)?;
                Some(parent.to_path_buf())
            }
            _ => None,
        };
        let writer = OpenOptions::new().create(true).append(true).open(&path)?;
        // Flush the parent directory so a freshly-created log's directory entry is
        // itself crash-durable — else a power loss before the OS flushes the
        // directory loses the whole trail even though the first append fsync'd the
        // file (the same crash-durability the op store's atomic writes take).
        if let Some(parent) = parent {
            File::open(&parent)?.sync_all()?;
        }
        Ok(Self {
            path,
            writer: Mutex::new(writer),
            clock,
            failed: AtomicBool::new(false),
        })
    }

    /// Append one event, stamped with the clock's current time. Flushes before it
    /// returns so the record survives a crash. On an IO failure the error is
    /// returned *and* the failure flag latched — a security event is never dropped
    /// silently.
    ///
    /// Once an append has failed the log is refused further writes: a partial
    /// `write_all` can strand a fraction of a frame on disk, and a later append
    /// landing after it would make its bogus length prefix consume the following
    /// real records — corrupting the whole readable trail, not just a torn tail. So
    /// the readable prefix is frozen (and the latched flag keeps surfacing the
    /// failure) rather than compounded.
    pub fn record(
        &self,
        actor: &[u8],
        action: Action,
        resource: AuditResource,
        decision: Decision,
    ) -> io::Result<()> {
        let record = AuditRecord {
            timestamp: self.clock.now_millis(),
            actor: actor.to_vec(),
            action,
            resource,
            decision,
        };
        let body = encode_record(&record);
        let len = u32::try_from(body.len()).expect("audit record length exceeds u32");
        let mut framed = Vec::with_capacity(4 + body.len());
        framed.extend_from_slice(&len.to_le_bytes());
        framed.extend_from_slice(&body);

        let result = {
            let mut file = self.writer.lock().unwrap_or_else(|p| p.into_inner());
            if self.failed.load(Ordering::SeqCst) {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "audit log is in a failed state; refusing further appends",
                ))
            } else {
                file.write_all(&framed).and_then(|()| file.sync_all())
            }
        };
        if result.is_err() {
            self.failed.store(true, Ordering::SeqCst);
        }
        result
    }

    /// Every stored record, in append (time) order. Reopens the file so it sees
    /// every flushed append. A torn trailing record (a crash mid-append) is dropped;
    /// a complete but undecodable record body is [`io::ErrorKind::InvalidData`].
    /// (A corrupted *length* prefix that overruns the file is indistinguishable from
    /// a crash-torn tail and is likewise dropped — the append log is not
    /// tamper-evident; a hash-chained record is the follow-on for that.)
    ///
    /// The read holds the append lock, so it never observes a frame mid-write: a
    /// query racing an in-flight append sees the trail either before or after that
    /// record, never a half-written one it would mistake for a torn tail.
    pub fn read_all(&self) -> io::Result<Vec<AuditRecord>> {
        let _writing = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let mut bytes = Vec::new();
        File::open(&self.path)?.read_to_end(&mut bytes)?;
        let mut records = Vec::new();
        let mut at = 0;
        while at + 4 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            let start = at + 4;
            let Some(body) = bytes.get(start..).and_then(|rest| rest.get(..len)) else {
                break; // torn tail: the record outruns the bytes on disk
            };
            let record = decode_record(body).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "undecodable audit record")
            })?;
            records.push(record);
            at = start + len;
        }
        Ok(records)
    }

    /// The stored records matching `query`, in time order — the read-only operator
    /// query. An all-`None` query returns every record.
    pub fn query(&self, query: &AuditQuery) -> io::Result<Vec<AuditRecord>> {
        Ok(self
            .read_all()?
            .into_iter()
            .filter(|record| query.matches(record))
            .collect())
    }

    /// Whether every append so far has succeeded. `false` once any append failed —
    /// a security event was dropped, which the operator surface reports rather than
    /// hiding.
    pub fn healthy(&self) -> bool {
        !self.failed.load(Ordering::SeqCst)
    }
}

/// An [`AccessLog`] sink that persists the **security-relevant** authorization
/// decisions to a shared [`AuditLog`]: every denial, and every write or schema
/// registration (which carry the ACL grant/revoke mutations). Routine permitted
/// reads and awareness publishes are not persisted — the audit trail records
/// refusals, mutations, and the explicit connect/export/version-read events, not the
/// ongoing read stream, so it stays signal, not noise.
///
/// A failed persist is surfaced, not swallowed: the underlying [`AuditLog`] latches
/// its failure flag (which the operator surface reports) and the error is logged to
/// stderr — the [`AccessLog`] contract returns no result to propagate.
pub struct DurableAccessLog {
    log: Arc<AuditLog>,
}

impl DurableAccessLog {
    pub fn new(log: Arc<AuditLog>) -> Self {
        Self { log }
    }

    /// Whether an access record is security-relevant enough to persist.
    fn security_relevant(record: &AccessRecord) -> bool {
        record.decision == Decision::Denied
            || matches!(
                record.action,
                Action::Write
                    | Action::RegisterSchema
                    | Action::Connect
                    | Action::Export
                    | Action::VersionRead
            )
    }
}

impl AccessLog for DurableAccessLog {
    fn record(&self, record: &AccessRecord) {
        if !Self::security_relevant(record) {
            return;
        }
        let resource = AuditResource::from_resource(record.resource);
        if let Err(err) = self
            .log
            .record(record.actor, record.action, resource, record.decision)
        {
            eprintln!("audit: failed to persist a security event: {err}");
        }
    }
}

/// A stable byte tag per [`Action`], the on-disk encoding. Exhaustive by design: a
/// new `Action` variant fails this to compile (fail-loud) until it is given a tag.
fn action_tag(action: Action) -> u8 {
    match action {
        Action::Read => 0,
        Action::Write => 1,
        Action::PublishAwareness => 2,
        Action::RegisterSchema => 3,
        Action::Connect => 4,
        Action::Export => 5,
        Action::VersionRead => 6,
    }
}

fn action_from_tag(tag: u8) -> Option<Action> {
    Some(match tag {
        0 => Action::Read,
        1 => Action::Write,
        2 => Action::PublishAwareness,
        3 => Action::RegisterSchema,
        4 => Action::Connect,
        5 => Action::Export,
        6 => Action::VersionRead,
        _ => return None,
    })
}

fn decision_tag(decision: Decision) -> u8 {
    match decision {
        Decision::Permitted => 0,
        Decision::Denied => 1,
    }
}

fn decision_from_tag(tag: u8) -> Option<Decision> {
    Some(match tag {
        0 => Decision::Permitted,
        1 => Decision::Denied,
        _ => return None,
    })
}

/// Encode one record's body (the bytes inside the length frame): the timestamp, the
/// action and decision tags, the length-prefixed actor, then the tagged resource.
fn encode_record(record: &AuditRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&record.timestamp.to_le_bytes());
    buf.push(action_tag(record.action));
    buf.push(decision_tag(record.decision));
    put_bytes(&mut buf, &record.actor);
    match &record.resource {
        AuditResource::Room(room) => {
            buf.push(0);
            put_bytes(&mut buf, room);
        }
        AuditResource::App(app) => {
            buf.push(1);
            put_bytes(&mut buf, app);
        }
        AuditResource::Zone { room, zone } => {
            buf.push(2);
            put_bytes(&mut buf, room);
            put_bytes(&mut buf, zone);
        }
    }
    buf
}

/// Decode a complete record body, or `None` if it is malformed (a bad tag, a framing
/// shortfall, or trailing garbage) — the read path treats that as corruption.
fn decode_record(bytes: &[u8]) -> Option<AuditRecord> {
    let mut at = 0;
    let timestamp = take_u64(bytes, &mut at)?;
    let action = action_from_tag(take_u8(bytes, &mut at)?)?;
    let decision = decision_from_tag(take_u8(bytes, &mut at)?)?;
    let actor = take_bytes(bytes, &mut at)?;
    let resource = match take_u8(bytes, &mut at)? {
        0 => AuditResource::Room(take_bytes(bytes, &mut at)?),
        1 => AuditResource::App(take_bytes(bytes, &mut at)?),
        2 => AuditResource::Zone {
            room: take_bytes(bytes, &mut at)?,
            zone: take_bytes(bytes, &mut at)?,
        },
        _ => return None,
    };
    if at != bytes.len() {
        return None;
    }
    Some(AuditRecord {
        timestamp,
        actor,
        action,
        resource,
        decision,
    })
}

/// Append `bytes` as a `u32` little-endian length prefix then the bytes.
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("length exceeds u32");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Read a `u32`-length-prefixed byte string at `at`, advancing it.
fn take_bytes(bytes: &[u8], at: &mut usize) -> Option<Vec<u8>> {
    let len = take_u32(bytes, at)? as usize;
    let start = *at;
    let body = bytes.get(start..).and_then(|rest| rest.get(..len))?;
    *at = start + len;
    Some(body.to_vec())
}

fn take_u8(bytes: &[u8], at: &mut usize) -> Option<u8> {
    let byte = bytes.get(*at).copied()?;
    *at += 1;
    Some(byte)
}

fn take_u32(bytes: &[u8], at: &mut usize) -> Option<u32> {
    let end = *at + 4;
    let field = bytes.get(*at..end)?;
    *at = end;
    Some(u32::from_le_bytes(field.try_into().unwrap()))
}

fn take_u64(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let end = *at + 8;
    let field = bytes.get(*at..end)?;
    *at = end;
    Some(u64::from_le_bytes(field.try_into().unwrap()))
}
