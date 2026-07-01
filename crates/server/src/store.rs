//! The durable store behind the hub: a per-room op log plus an optional
//! compaction snapshot.
//!
//! A [`Store`] persists each room's ops to a `<room>.log` file — one record per
//! op, a `u32` little-endian length prefix followed by its `encode_op` bytes.
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

/// A room's compaction snapshot: the sequence its state covers and the encoded
/// document state.
pub struct Snapshot {
    pub base_seq: u64,
    pub state: Vec<u8>,
}

/// What a store holds for one room: an optional compaction snapshot and the op
/// records still in its log. The log may overlap the snapshot — a crash between
/// writing the snapshot and truncating the log leaves the prefix behind — so
/// the hub replays it through its dedup.
#[derive(Default)]
pub struct RoomLog {
    pub snapshot: Option<Snapshot>,
    pub ops: Vec<Op>,
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

    /// Append `ops` to `room`'s log, each as a length-prefixed record. Flushes
    /// before returning so the records survive a crash and are visible to any
    /// handle opened afterward. An empty batch writes nothing.
    pub fn append(&mut self, room: &[u8], ops: &[Op]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for op in ops {
            let body = encode_op(op);
            let len = u32::try_from(body.len()).expect("op length exceeds u32");
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

        // The snapshot is durable; the log prefix it covers can go. A compaction
        // folds up to the head, so the whole log is dropped and later appends
        // form the tail after `base_seq`.
        match fs::remove_file(self.room_path(room)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
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
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Which kind of per-room file a path is.
enum FileKind {
    Log,
    Snapshot,
}

/// Recover a room id and file kind from a path, or `None` if it is not one of
/// ours (including an uncommitted `.snap.tmp`).
fn classify(path: &Path) -> Option<(RoomId, FileKind)> {
    let kind = match path.extension()?.to_str()? {
        "log" => FileKind::Log,
        "snap" => FileKind::Snapshot,
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

fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// Decode a file's records in order. A trailing record that runs past the end
/// of the bytes is a torn write and is dropped; a fully-present record that
/// fails to decode is corruption.
fn decode_records(bytes: &[u8]) -> io::Result<Vec<Op>> {
    let mut ops = Vec::new();
    let mut at = 0;
    while at + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let start = at + 4;
        let Some(body) = bytes.get(start..).and_then(|rest| rest.get(..len)) else {
            break; // torn tail: the record outruns the bytes on disk
        };
        let op = decode_op(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;
        ops.push(op);
        at = start + len;
    }
    Ok(ops)
}
