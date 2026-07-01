//! The durable, append-only op log behind the hub.
//!
//! A [`Store`] persists each room's ops to disk so a restarted node replays
//! back to the same state. Each room is one append-only file; each op is one
//! record, a `u32` little-endian length prefix followed by its `encode_op`
//! bytes. An [`append`](Store::append) is durable — it flushes before it
//! returns, so a restart or a second handle sees the ops immediately.
//! [`load`](Store::load) tolerates a torn tail (a record half-written when the
//! process died) but rejects a complete, undecodable record as corruption.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crdtsync_core::{decode_op, encode_op, Op};

use crate::RoomId;

/// A directory of per-room op logs.
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

    /// Every room's log, decoded in append order. Room order is unspecified. A
    /// torn tail record is dropped; a complete but undecodable record is an
    /// [`io::ErrorKind::InvalidData`] error.
    pub fn load(&self) -> io::Result<Vec<(RoomId, Vec<Op>)>> {
        let mut rooms = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            let Some(room) = room_of(&path) else {
                continue;
            };
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            rooms.push((room, decode_records(&bytes)?));
        }
        Ok(rooms)
    }

    /// The file backing `room`: its bytes as lowercase hex, so any room id —
    /// separators, non-utf8, and all — maps to one safe name inside the root.
    fn room_path(&self, room: &[u8]) -> PathBuf {
        let mut name = String::with_capacity(room.len() * 2 + 4);
        for byte in room {
            name.push(HEX[(byte >> 4) as usize] as char);
            name.push(HEX[(byte & 0x0f) as usize] as char);
        }
        name.push_str(".log");
        self.root.join(name)
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Recover a room id from a log file path, or `None` if it is not one of ours.
fn room_of(path: &Path) -> Option<RoomId> {
    if path.extension()?.to_str()? != "log" {
        return None;
    }
    let stem = path.file_stem()?.to_str()?.as_bytes();
    if stem.len() % 2 != 0 {
        return None;
    }
    stem.chunks(2)
        .map(|pair| Some(unhex(pair[0])? << 4 | unhex(pair[1])?))
        .collect()
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
