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

/// What a store holds for one room: an optional compaction snapshot, the op
/// records still in its log, and its named versions. The log may overlap the
/// snapshot — a crash between writing the snapshot and truncating the log leaves
/// the prefix behind — so the hub replays it through its dedup.
#[derive(Default)]
pub struct RoomLog {
    pub snapshot: Option<Snapshot>,
    pub ops: Vec<StoredOp>,
    /// The room's named versions as `(name, covered seq, state)`.
    pub versions: Vec<(Vec<u8>, u64, Vec<u8>)>,
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

    /// Fold `room`'s log prefix into a snapshot and drop the covered records.
    /// The snapshot — its base sequence then `state` — is written to a temp
    /// file, flushed, and atomically renamed into place before the log is
    /// removed, so a crash never leaves a torn snapshot and never drops the log
    /// while the snapshot is missing.
    pub fn compact(&mut self, room: &[u8], base_seq: u64, state: &[u8]) -> io::Result<()> {
        let mut buf = Vec::with_capacity(8 + state.len());
        buf.extend_from_slice(&base_seq.to_le_bytes());
        buf.extend_from_slice(state);

        let tmp = self.snap_tmp_path(room);
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, self.snap_path(room))?;
        // Persist the rename itself: until the directory is flushed the snapshot
        // entry is not crash-durable, so a power loss could drop it while the
        // log removal below survives. Flushing here keeps the snapshot present
        // before the log it replaces can disappear.
        self.sync_dir()?;

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
    /// (8-byte little-endian), then the state (length-prefixed). The file lands
    /// atomically — temp, flushed, renamed, directory flushed — so a crash never
    /// leaves a torn index; an empty set removes the file. A version is immutable
    /// but the *index* is not, so the whole file is rewritten on every change.
    pub fn write_versions(
        &mut self,
        room: &[u8],
        versions: &[(&[u8], u64, &[u8])],
    ) -> io::Result<()> {
        if versions.is_empty() {
            match fs::remove_file(self.versions_path(room)) {
                Ok(()) => return self.sync_dir(),
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        let mut buf = Vec::new();
        for (name, seq, state) in versions {
            put_bytes(&mut buf, name);
            buf.extend_from_slice(&seq.to_le_bytes());
            put_bytes(&mut buf, state);
        }
        let tmp = self.versions_tmp_path(room);
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, self.versions_path(room))?;
        self.sync_dir()
    }

    /// Flush the root directory so a rename or removal within it is durable.
    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.root)?.sync_all()
    }

    /// Every room's snapshot and log, keyed by room id. Room order is
    /// unspecified. A torn tail log record is dropped; a complete but
    /// undecodable record, or a snapshot missing its header, is an
    /// [`io::ErrorKind::InvalidData`] error. An uncommitted snapshot temp is
    /// ignored.
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
}

/// Append `bytes` to `buf` as a `u32` little-endian length prefix then the bytes.
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("length exceeds u32");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Which kind of per-room file a path is.
enum FileKind {
    Log,
    Snapshot,
    Versions,
}

/// Recover a room id and file kind from a path, or `None` if it is not one of
/// ours (including an uncommitted `.snap.tmp` / `.versions.tmp`).
fn classify(path: &Path) -> Option<(RoomId, FileKind)> {
    let kind = match path.extension()?.to_str()? {
        "log" => FileKind::Log,
        "snap" => FileKind::Snapshot,
        "versions" => FileKind::Versions,
        _ => return None,
    };
    let stem = path.file_stem()?.to_str()?.as_bytes();
    if stem.len() % 2 != 0 {
        return None;
    }
    let room: RoomId = stem
        .chunks(2)
        .map(|pair| Some(unhex(pair[0])? << 4 | unhex(pair[1])?))
        .collect::<Option<_>>()?;
    Some((room, kind))
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
fn parse_versions(bytes: &[u8]) -> io::Result<Vec<(Vec<u8>, u64, Vec<u8>)>> {
    let mut versions = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let name = take_bytes(bytes, &mut at)?;
        let seq = take_u64(bytes, &mut at)?;
        let state = take_bytes(bytes, &mut at)?;
        versions.push((name, seq, state));
    }
    Ok(versions)
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
