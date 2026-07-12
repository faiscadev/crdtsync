//! The durable store behind the hub: a per-room op log plus an optional
//! compaction snapshot.
//!
//! A [`Store`] persists each room's ops to a `<room>.log` file — one record per
//! op: a `u32` little-endian creation schema version (`0` for a relay op with no
//! schema), then a `u32` little-endian length prefix followed by its `encode_op`
//! bytes. The version rides on the record so a heterogeneous log — ops created
//! under different schema versions — round-trips a restart, and per-recipient
//! translation can rewrite each op from its own creation version.
//! An [`append`](Store::append) is durable — it flushes before it returns, so a
//! restart or a second handle sees the ops immediately. [`compact`](Store::compact)
//! folds a room's log prefix into a `<room>.snap` snapshot (an `8`-byte
//! little-endian base sequence then the encoded document state) and drops the
//! covered log records, so a restart replays a bounded tail. The snapshot lands
//! atomically — a temp file, flushed, then renamed — and only then is the log
//! truncated, so a crash between the two leaves the snapshot beside a still-full
//! log; [`load`](Store::load) hands both back and the hub dedups the overlap on
//! replay. [`load`](Store::load) tolerates a torn log tail but rejects a
//! complete, undecodable record — or a truncated snapshot header — as
//! corruption.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crdtsync_core::{decode_op, encode_op, Op};

use crate::RoomId;

/// One logged op with the schema version it was created under — the unit the
/// heterogeneous log stores and persists. A relay op, created with no schema in
/// force, carries `None`; an enforced op carries the writing client's version
/// (always `>= 1`), which per-recipient translation rewrites from.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StoredOp {
    pub op: Op,
    pub schema_version: Option<u32>,
}

impl StoredOp {
    /// A logged op tagged with its creation schema version.
    pub fn new(op: Op, schema_version: Option<u32>) -> Self {
        Self { op, schema_version }
    }
}

/// A room's compaction snapshot: the sequence its state covers and the encoded
/// document state.
pub struct Snapshot {
    pub base_seq: u64,
    pub state: Vec<u8>,
}

/// A room's durable governing metadata: the app that governs it — `{app_id,
/// version}`, `None` for a relay/unbound room — and the highest governing-app op
/// version ever folded into its merged state. Persisted beside the room's log
/// and snapshot so a restart or a dormant-room sweep restores the binding and
/// the op-version high-water, rather than rebinding a populated room's first
/// subscriber untranslated or under-counting a compacted room's high-water from
/// its post-compaction log tail. Absent or undecodable metadata falls back to
/// the rebuild-from-log / bind-on-subscribe reconstruction, so it is a durability
/// cache, not a source of truth the load path may fail on.
pub struct RoomMeta {
    pub governing: Option<(Vec<u8>, u32)>,
    pub max_op_version: Option<u32>,
}

/// One branch of a room: a named pointer into the op log. `fork_point` is the
/// history position up to which it shares the log with the branch it forked from;
/// `head` is its own high-water position (the room's server sequence, the
/// single-node log's monotonic history counter). The default `main` (fork_point
/// 0) is synthesized rather than stored, so the persisted set holds only the forks
/// that diverge from it.
///
/// `published` marks a read-only publish target — a branch whose HEAD is advanced
/// only by [`publish`](crate::Hub::publish), never by a client write. A client
/// `Ops` write to one is refused, so the published state stays a snapshot of the
/// editor branch at each publish, not a live-editable stream.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Branch {
    pub name: Vec<u8>,
    pub fork_point: u64,
    pub head: u64,
    pub published: bool,
}

/// What a store holds for one room: an optional compaction snapshot, the op
/// records still in its log, and its named versions. The log may overlap the
/// snapshot — a crash between writing the snapshot and truncating the log leaves
/// the prefix behind — so the hub replays it through its dedup.
#[derive(Default)]
pub struct RoomLog {
    pub snapshot: Option<Snapshot>,
    pub ops: Vec<StoredOp>,
    /// The room's named versions as `(name, covered seq, auto-version origin,
    /// capture ordinal, state)`. `origin` is `None` for a manual version.
    pub versions: Vec<(Vec<u8>, u64, Option<Vec<u8>>, u64, Vec<u8>)>,
    /// The room's durable governing metadata, or `None` when the store holds none
    /// (an unbound relay room, or one whose record is absent or undecodable — the
    /// rebuild-from-log fallback then stands).
    pub meta: Option<RoomMeta>,
    /// The room's persisted branches — the forks past the default `main`. Empty
    /// when the store holds none (a never-forked room, or a record that is absent
    /// or undecodable), which the hub restores as the default `{main}`.
    pub branches: Vec<Branch>,
    /// Each non-`main` branch's divergent op tail as `(branch name, ops)` — the
    /// ops appended past its fork point. The shared base rides `main`'s log, so it
    /// is never stored here. Empty for a room with no diverged branch.
    pub branch_ops: Vec<(Vec<u8>, Vec<StoredOp>)>,
    /// Each snapshot-forked branch's owned base as `(branch name, encoded state)`
    /// — the materialized state of the version it forked from, at that version's
    /// covered sequence. A live-log fork shares `main`'s log and has no entry here;
    /// only a fork whose base is a snapshot owns a copy. Empty for a room with no
    /// snapshot fork.
    pub branch_bases: Vec<(Vec<u8>, Vec<u8>)>,
    /// The room's active-HEAD branch — the branch a default (unnamed) subscribe
    /// follows after a restore-as-branch switched it. `None` when the store holds
    /// none (the room has never been restored, so the default `main` is served).
    pub active_branch: Option<Vec<u8>>,
}

/// A directory of per-room logs and snapshots.
pub struct Store {
    root: PathBuf,
}

impl Store {
    /// Open a store rooted at `root`, creating the directory if it is absent.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Store> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Store { root })
    }

    /// Append `ops` to `room`'s log, each as a version-tagged, length-prefixed
    /// record. Flushes before returning so the records survive a crash and are
    /// visible to any handle opened afterward. An empty batch writes nothing.
    pub fn append(&mut self, room: &[u8], ops: &[StoredOp]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for stored in ops {
            let body = encode_op(&stored.op);
            let len = u32::try_from(body.len()).expect("op length exceeds u32");
            // A relay op (no schema) is version 0; an enforced op is >= 1.
            buf.extend_from_slice(&stored.schema_version.unwrap_or(0).to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&body);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.room_path(room))?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    /// Append `ops` to `branch`'s divergent tail in `room`, each a version-tagged,
    /// length-prefixed record — the same framing as the main log. Flushes before
    /// returning so the tail survives a crash. An empty batch writes nothing.
    pub fn append_branch(
        &mut self,
        room: &[u8],
        branch: &[u8],
        ops: &[StoredOp],
    ) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for stored in ops {
            let body = encode_op(&stored.op);
            let len = u32::try_from(body.len()).expect("op length exceeds u32");
            buf.extend_from_slice(&stored.schema_version.unwrap_or(0).to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&body);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.branch_log_path(room, branch))?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    /// Drop `branch`'s divergent-tail file in `room`, if any — the durable
    /// counterpart to deleting the branch, treating an already-absent file as
    /// success.
    pub fn remove_branch_log(&mut self, room: &[u8], branch: &[u8]) -> io::Result<()> {
        self.remove_if_present(&self.branch_log_path(room, branch))
    }

    /// Persist a snapshot fork's owned base `state` for `(room, branch)`, replacing
    /// whatever it held. The base is the version's encoded state, stored verbatim;
    /// it lands atomically — temp, flushed, renamed, directory flushed — so a crash
    /// never leaves a torn base beside a branch pointer.
    pub fn write_branch_base(
        &mut self,
        room: &[u8],
        branch: &[u8],
        state: &[u8],
    ) -> io::Result<()> {
        self.atomic_write(
            &self.branch_base_path(room, branch),
            &self.branch_base_tmp_path(room, branch),
            state,
        )
    }

    /// Drop a snapshot fork's base file for `(room, branch)`, if any — the durable
    /// counterpart to deleting the branch, treating an already-absent file as
    /// success.
    pub fn remove_branch_base(&mut self, room: &[u8], branch: &[u8]) -> io::Result<()> {
        self.remove_if_present(&self.branch_base_path(room, branch))
    }

    /// Persist `room`'s active-HEAD `branch` — the branch a default subscribe
    /// follows — replacing whatever it held. The name is stored verbatim and lands
    /// atomically. The default `main` is never stored: an absent file *is* `main`,
    /// so passing `main` (or the empty name) removes the file, restoring the
    /// default.
    pub fn write_active_branch(&mut self, room: &[u8], branch: &[u8]) -> io::Result<()> {
        if branch.is_empty() || branch == crate::MAIN_BRANCH {
            return self.remove_if_present(&self.active_path(room));
        }
        self.atomic_write(&self.active_path(room), &self.active_tmp_path(room), branch)
    }

    /// Fold `room`'s log prefix into a snapshot and drop the covered records.
    /// The snapshot — its base sequence then `state` — is written to a temp
    /// file, flushed, and atomically renamed into place before the log is
    /// removed, so a crash never leaves a torn snapshot and never drops the log
    /// while the snapshot is missing.
    pub fn compact(&mut self, room: &[u8], base_seq: u64, state: &[u8]) -> io::Result<()> {
        let mut buf = Vec::with_capacity(8 + state.len());
        buf.extend_from_slice(&base_seq.to_le_bytes());
        buf.extend_from_slice(state);

        // The snapshot lands durably — temp, flushed, renamed, directory flushed —
        // before the log it replaces is removed: until the directory is flushed the
        // snapshot entry is not crash-durable, so a power loss could otherwise drop
        // it while the log removal below survives.
        self.atomic_write(&self.snap_path(room), &self.snap_tmp_path(room), &buf)?;

        // The snapshot is durable; the log prefix it covers can go. A compaction
        // folds up to the head, so the whole log is dropped and later appends
        // form the tail after `base_seq`.
        match fs::remove_file(self.room_path(room)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Rewrite `room`'s named versions to their own file, replacing whatever it
    /// held. Each record is the name (length-prefixed), the covered sequence
    /// (8-byte little-endian), a 1-byte origin present-flag then the origin
    /// (length-prefixed, only when present), the capture ordinal (8-byte
    /// little-endian), then the state (length-prefixed). The file lands atomically
    /// — temp, flushed, renamed, directory flushed — so a crash never leaves a torn
    /// index; an empty set removes the file. A version is immutable but the *index*
    /// is not, so the whole file is rewritten on every change.
    pub fn write_versions(
        &mut self,
        room: &[u8],
        versions: &[(&[u8], u64, Option<&[u8]>, u64, &[u8])],
    ) -> io::Result<()> {
        if versions.is_empty() {
            return self.remove_if_present(&self.versions_path(room));
        }
        let mut buf = Vec::new();
        for (name, seq, origin, ordinal, state) in versions {
            put_bytes(&mut buf, name);
            buf.extend_from_slice(&seq.to_le_bytes());
            match origin {
                Some(origin) => {
                    buf.push(1);
                    put_bytes(&mut buf, origin);
                }
                None => buf.push(0),
            }
            buf.extend_from_slice(&ordinal.to_le_bytes());
            put_bytes(&mut buf, state);
        }
        self.atomic_write(
            &self.versions_path(room),
            &self.versions_tmp_path(room),
            &buf,
        )
    }

    /// Rewrite `room`'s governing metadata to its own file, replacing whatever it
    /// held. The record is a 1-byte governing present-flag then — when present —
    /// the length-prefixed app id and the 4-byte little-endian version, then a
    /// 1-byte high-water present-flag then — when present — the 4-byte
    /// little-endian op version. The file lands atomically — temp, flushed,
    /// renamed, directory flushed — so a crash never leaves a torn record; a
    /// record with neither field removes the file. The binding is derived state
    /// (re-derivable from live subscribers) rather than the authoritative op log,
    /// so a caller treats a write failure as a durability-cache miss, not a data
    /// loss.
    pub fn write_meta(&mut self, room: &[u8], meta: &RoomMeta) -> io::Result<()> {
        if meta.governing.is_none() && meta.max_op_version.is_none() {
            return self.remove_if_present(&self.meta_path(room));
        }
        let mut buf = Vec::new();
        match &meta.governing {
            Some((app, version)) => {
                buf.push(1);
                put_bytes(&mut buf, app);
                buf.extend_from_slice(&version.to_le_bytes());
            }
            None => buf.push(0),
        }
        match meta.max_op_version {
            Some(version) => {
                buf.push(1);
                buf.extend_from_slice(&version.to_le_bytes());
            }
            None => buf.push(0),
        }
        self.atomic_write(&self.meta_path(room), &self.meta_tmp_path(room), &buf)
    }

    /// Rewrite `room`'s branches to their own file, replacing whatever it held.
    /// Only the forks past the default `main` are stored — `main` is synthesized
    /// on load — so an empty slice removes the file, restoring the room to the
    /// default `{main}`. Each record is the name (length-prefixed), the fork-point
    /// position (8-byte little-endian), the head position (8-byte little-endian),
    /// then a 1-byte read-only-publish-target flag. The file lands atomically — temp, flushed, renamed,
    /// directory flushed — so a crash never leaves a torn set; the whole file is
    /// rewritten on every change, as the *set* of branches is mutable though a
    /// branch's history is not.
    pub fn write_branches(&mut self, room: &[u8], branches: &[Branch]) -> io::Result<()> {
        if branches.is_empty() {
            return self.remove_if_present(&self.branches_path(room));
        }
        let mut buf = Vec::new();
        for branch in branches {
            put_bytes(&mut buf, &branch.name);
            buf.extend_from_slice(&branch.fork_point.to_le_bytes());
            buf.extend_from_slice(&branch.head.to_le_bytes());
            buf.push(branch.published as u8);
        }
        self.atomic_write(
            &self.branches_path(room),
            &self.branches_tmp_path(room),
            &buf,
        )
    }

    /// Write `buf` to `path` atomically: fill `tmp`, flush it, rename it into
    /// place, then flush the directory so the rename itself is crash-durable. A
    /// reader sees either the whole prior file or the whole new one, never a torn
    /// mix.
    fn atomic_write(&self, path: &Path, tmp: &Path, buf: &[u8]) -> io::Result<()> {
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(tmp)?;
            file.write_all(buf)?;
            file.sync_all()?;
        }
        fs::rename(tmp, path)?;
        self.sync_dir()
    }

    /// Remove `path` and flush the directory so the removal is durable, treating
    /// an already-absent file as success. A no-op leaves the directory unflushed —
    /// nothing changed to persist.
    fn remove_if_present(&self, path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => self.sync_dir(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Flush the root directory so a rename or removal within it is durable.
    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.root)?.sync_all()
    }

    /// Every room's snapshot, log, and governing metadata, keyed by room id. Room
    /// order is unspecified. A torn tail log record is dropped; a complete but
    /// undecodable record, or a snapshot missing its header, is an
    /// [`io::ErrorKind::InvalidData`] error. An undecodable metadata or branches
    /// record loads as absent (a durability cache / the default `{main}`, never
    /// fatal). An uncommitted snapshot temp is ignored.
    pub fn load(&self) -> io::Result<Vec<(RoomId, RoomLog)>> {
        let mut rooms: HashMap<RoomId, RoomLog> = HashMap::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            let Some((room, kind)) = classify(&path) else {
                continue;
            };
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            let slot = rooms.entry(room).or_default();
            match kind {
                FileKind::Log => slot.ops = decode_records(&bytes)?,
                FileKind::Snapshot => slot.snapshot = Some(parse_snapshot(&bytes)?),
                FileKind::Versions => slot.versions = parse_versions(&bytes)?,
                // Metadata is a durability cache, not authoritative: an undecodable
                // record loads as absent, leaving the rebuild-from-log /
                // bind-on-subscribe fallback rather than failing the whole load.
                FileKind::Meta => slot.meta = parse_meta(&bytes),
                // A malformed branches record loads as no forks, restoring the
                // room to the default `{main}` rather than failing the whole load.
                FileKind::Branches => slot.branches = parse_branches(&bytes),
                // A branch tail is framed as the main log; a torn tail record is
                // dropped, a complete-but-undecodable one is corruption.
                FileKind::BranchLog(branch) => {
                    slot.branch_ops.push((branch, decode_records(&bytes)?))
                }
                // A snapshot fork's base is the version's encoded state, stored
                // verbatim; it is restored as the branch's owned base.
                FileKind::BranchBase(branch) => slot.branch_bases.push((branch, bytes)),
                // The active-HEAD branch name, stored verbatim.
                FileKind::Active => slot.active_branch = Some(bytes),
            }
        }
        Ok(rooms.into_iter().collect())
    }

    /// The hex of a room id, so any id — separators, non-utf8, and all — maps to
    /// one safe name inside the root.
    fn hex_name(room: &[u8]) -> String {
        let mut name = String::with_capacity(room.len() * 2);
        for byte in room {
            name.push(HEX[(byte >> 4) as usize] as char);
            name.push(HEX[(byte & 0x0f) as usize] as char);
        }
        name
    }

    /// The log file backing `room`.
    fn room_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.log", Self::hex_name(room)))
    }

    /// The committed snapshot file backing `room`.
    fn snap_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.snap", Self::hex_name(room)))
    }

    /// The in-progress snapshot temp for `room`, renamed onto `snap_path` once
    /// durable.
    fn snap_tmp_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.snap.tmp", Self::hex_name(room)))
    }

    /// The named-versions file backing `room`.
    fn versions_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.versions", Self::hex_name(room)))
    }

    /// The in-progress versions temp for `room`, renamed onto `versions_path`
    /// once durable.
    fn versions_tmp_path(&self, room: &[u8]) -> PathBuf {
        self.root
            .join(format!("{}.versions.tmp", Self::hex_name(room)))
    }

    /// The governing-metadata file backing `room`.
    fn meta_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.meta", Self::hex_name(room)))
    }

    /// The in-progress metadata temp for `room`, renamed onto `meta_path` once
    /// durable.
    fn meta_tmp_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.meta.tmp", Self::hex_name(room)))
    }

    /// The branches file backing `room`.
    fn branches_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.branches", Self::hex_name(room)))
    }

    /// The in-progress branches temp for `room`, renamed onto `branches_path`
    /// once durable.
    fn branches_tmp_path(&self, room: &[u8]) -> PathBuf {
        self.root
            .join(format!("{}.branches.tmp", Self::hex_name(room)))
    }

    /// The divergent-tail log file backing `(room, branch)`. Room and branch are
    /// each hex-encoded and joined by a non-hex separator, so any bytes — the
    /// branch name included — map to one safe, unambiguous name.
    fn branch_log_path(&self, room: &[u8], branch: &[u8]) -> PathBuf {
        self.root.join(format!(
            "{}_{}.blog",
            Self::hex_name(room),
            Self::hex_name(branch)
        ))
    }

    /// The snapshot-fork base file backing `(room, branch)`. Room and branch are
    /// each hex-encoded and joined by a non-hex separator, matching `.blog`.
    fn branch_base_path(&self, room: &[u8], branch: &[u8]) -> PathBuf {
        self.root.join(format!(
            "{}_{}.bbase",
            Self::hex_name(room),
            Self::hex_name(branch)
        ))
    }

    /// The in-progress base temp for `(room, branch)`, renamed onto
    /// `branch_base_path` once durable.
    fn branch_base_tmp_path(&self, room: &[u8], branch: &[u8]) -> PathBuf {
        self.root.join(format!(
            "{}_{}.bbase.tmp",
            Self::hex_name(room),
            Self::hex_name(branch)
        ))
    }

    /// The active-HEAD file backing `room`.
    fn active_path(&self, room: &[u8]) -> PathBuf {
        self.root.join(format!("{}.active", Self::hex_name(room)))
    }

    /// The in-progress active-HEAD temp for `room`, renamed onto `active_path`
    /// once durable.
    fn active_tmp_path(&self, room: &[u8]) -> PathBuf {
        self.root
            .join(format!("{}.active.tmp", Self::hex_name(room)))
    }
}

/// Append `bytes` to `buf` as a `u32` little-endian length prefix then the bytes.
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("length exceeds u32");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Which kind of per-room file a path is. A branch tail carries its branch name,
/// the second dimension of its `(room, branch)` key.
enum FileKind {
    Log,
    Snapshot,
    Versions,
    Meta,
    Branches,
    BranchLog(Vec<u8>),
    BranchBase(Vec<u8>),
    Active,
}

/// Recover a room id and file kind from a path, or `None` if it is not one of
/// ours (including an uncommitted `.snap.tmp` / `.versions.tmp` / `.meta.tmp` /
/// `.branches.tmp`). A `.blog` names its branch too: `<hex room>_<hex branch>`.
fn classify(path: &Path) -> Option<(RoomId, FileKind)> {
    let ext = path.extension()?.to_str()?;
    let stem = path.file_stem()?.to_str()?;
    if ext == "blog" {
        let (room, branch) = stem.split_once('_')?;
        return Some((decode_hex(room)?, FileKind::BranchLog(decode_hex(branch)?)));
    }
    if ext == "bbase" {
        let (room, branch) = stem.split_once('_')?;
        return Some((decode_hex(room)?, FileKind::BranchBase(decode_hex(branch)?)));
    }
    let kind = match ext {
        "log" => FileKind::Log,
        "snap" => FileKind::Snapshot,
        "versions" => FileKind::Versions,
        "meta" => FileKind::Meta,
        "branches" => FileKind::Branches,
        "active" => FileKind::Active,
        _ => return None,
    };
    Some((decode_hex(stem)?, kind))
}

/// Decode a hex file-name component back to its bytes, or `None` if it is not an
/// even-length run of hex digits.
fn decode_hex(hex: &str) -> Option<RoomId> {
    let hex = hex.as_bytes();
    if hex.len() % 2 != 0 {
        return None;
    }
    hex.chunks(2)
        .map(|pair| Some(unhex(pair[0])? << 4 | unhex(pair[1])?))
        .collect()
}

/// Parse a snapshot record: an 8-byte little-endian base sequence, then the
/// document state to end of file.
fn parse_snapshot(bytes: &[u8]) -> io::Result<Snapshot> {
    if bytes.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot shorter than its header",
        ));
    }
    let base_seq = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    Ok(Snapshot {
        base_seq,
        state: bytes[8..].to_vec(),
    })
}

/// Parse a versions file: a sequence of `(name, seq, state)` records, each a
/// length-prefixed name, an 8-byte little-endian sequence, and a length-prefixed
/// state. The file is rewritten atomically, so it is never torn — any framing
/// shortfall or trailing garbage is corruption, not a tolerable tail.
fn parse_versions(bytes: &[u8]) -> io::Result<Vec<(Vec<u8>, u64, Option<Vec<u8>>, u64, Vec<u8>)>> {
    let mut versions = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let name = take_bytes(bytes, &mut at)?;
        let seq = take_u64(bytes, &mut at)?;
        let origin = match take_u8(bytes, &mut at)? {
            0 => None,
            1 => Some(take_bytes(bytes, &mut at)?),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "versions record has a bad origin flag",
                ))
            }
        };
        let ordinal = take_u64(bytes, &mut at)?;
        let state = take_bytes(bytes, &mut at)?;
        versions.push((name, seq, origin, ordinal, state));
    }
    Ok(versions)
}

/// Parse a governing-metadata record, or `None` if it is absent or malformed —
/// the load path treats metadata as a durability cache and never fails on it, so
/// any framing shortfall or bad flag reconstructs from the log instead. A record
/// with neither field present decodes to `None`, matching the file's removal when
/// there is nothing to persist.
fn parse_meta(bytes: &[u8]) -> Option<RoomMeta> {
    let mut at = 0;
    let governing = match take_u8(bytes, &mut at).ok()? {
        0 => None,
        1 => {
            let app = take_bytes(bytes, &mut at).ok()?;
            let version = take_u32(bytes, &mut at).ok()?;
            Some((app, version))
        }
        _ => return None,
    };
    let max_op_version = match take_u8(bytes, &mut at).ok()? {
        0 => None,
        1 => Some(take_u32(bytes, &mut at).ok()?),
        _ => return None,
    };
    if at != bytes.len() {
        return None;
    }
    if governing.is_none() && max_op_version.is_none() {
        return None;
    }
    Some(RoomMeta {
        governing,
        max_op_version,
    })
}

/// Parse a branches file: a sequence of `(name, fork_point, head)` records, each
/// a length-prefixed name and two 8-byte little-endian history positions.
/// Branches are a durability cache over the default `{main}`, so any framing
/// shortfall discards the whole set (loading as no forks) rather than failing the
/// load — the hub then re-synthesizes `{main}`.
fn parse_branches(bytes: &[u8]) -> Vec<Branch> {
    let mut branches = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let Ok(name) = take_bytes(bytes, &mut at) else {
            return Vec::new();
        };
        let Ok(fork_point) = take_u64(bytes, &mut at) else {
            return Vec::new();
        };
        let Ok(head) = take_u64(bytes, &mut at) else {
            return Vec::new();
        };
        let Ok(published) = take_u8(bytes, &mut at) else {
            return Vec::new();
        };
        branches.push(Branch {
            name,
            fork_point,
            head,
            published: published != 0,
        });
    }
    branches
}

/// Read a single byte at `at`, advancing it.
fn take_u8(bytes: &[u8], at: &mut usize) -> io::Result<u8> {
    let byte = bytes.get(*at).copied().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "versions record is truncated")
    })?;
    *at += 1;
    Ok(byte)
}

/// Read a `u32`-length-prefixed byte string at `at`, advancing it.
fn take_bytes(bytes: &[u8], at: &mut usize) -> io::Result<Vec<u8>> {
    let len = take_u32(bytes, at)? as usize;
    let start = *at;
    let body = bytes
        .get(start..)
        .and_then(|rest| rest.get(..len))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "versions record is truncated")
        })?;
    *at = start + len;
    Ok(body.to_vec())
}

/// Read a little-endian `u32` at `at`, advancing it.
fn take_u32(bytes: &[u8], at: &mut usize) -> io::Result<u32> {
    let end = *at + 4;
    let field = bytes.get(*at..end).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "versions record is truncated")
    })?;
    *at = end;
    Ok(u32::from_le_bytes(field.try_into().unwrap()))
}

/// Read a little-endian `u64` at `at`, advancing it.
fn take_u64(bytes: &[u8], at: &mut usize) -> io::Result<u64> {
    let end = *at + 8;
    let field = bytes.get(*at..end).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "versions record is truncated")
    })?;
    *at = end;
    Ok(u64::from_le_bytes(field.try_into().unwrap()))
}

fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// Decode a file's records in order. Each record is a `u32` creation version
/// then a `u32`-length-prefixed op body. A trailing record that runs past the
/// end of the bytes is a torn write and is dropped; a fully-present record that
/// fails to decode is corruption.
fn decode_records(bytes: &[u8]) -> io::Result<Vec<StoredOp>> {
    let mut ops = Vec::new();
    let mut at = 0;
    // Each record needs a 4-byte version plus a 4-byte length before its body.
    while at + 8 <= bytes.len() {
        let version = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
        let start = at + 8;
        let Some(body) = bytes.get(start..).and_then(|rest| rest.get(..len)) else {
            break; // torn tail: the record outruns the bytes on disk
        };
        let op = decode_op(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;
        // Version 0 is a relay op with no schema; >= 1 is an enforced version.
        ops.push(StoredOp::new(op, (version != 0).then_some(version)));
        at = start + len;
    }
    Ok(ops)
}
