//! A content-addressable local-filesystem blob store.
//!
//! Blob bytes live here, out of the op stream: an op carries only a [`BlobRef`],
//! whose `id` is an unguessable public UUID. A blob at or below [`INLINE_MAX`]
//! rides inside the ref and never reaches the store; a larger one is persisted
//! and addressed internally by the sha256 of its bytes, so two puts of identical
//! content share one on-disk object (natural dedup). The public UUID never
//! encodes the hash, so holding a ref cannot probe whether the store already
//! has the bytes. A durable UUID → sha256 mapping resolves a fetch after a
//! reopen.
//!
//! This slice is the bytes-at-rest layer only. The op/wire ref producer, the
//! signed HTTP upload/fetch route, and per-reference ACL build on it in later
//! slices. An S3 backend, dedup ref-counting / GC, and range requests are
//! deferred to v0.5.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crdtsync_core::BlobRef;
use sha2::{Digest, Sha256};

/// Blobs at or below this size ride inline in the [`BlobRef`] and never touch
/// the store; larger ones are persisted and fetched by id.
pub const INLINE_MAX: usize = 4096;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// A directory of content-addressed blob objects plus the public-handle index.
///
/// Layout under the root: `objects/<sha256-hex>` holds each distinct blob's
/// bytes; `refs/<uuid-hex>` holds the 32-byte sha256 its handle resolves to. The
/// index is mirrored in memory, rebuilt from `refs/` on open.
pub struct BlobStore {
    objects: PathBuf,
    refs: PathBuf,
    /// Public UUID handle → content sha256, mirrored on disk under `refs/`.
    index: HashMap<[u8; 16], [u8; 32]>,
}

impl BlobStore {
    /// Open a store rooted at `root`, creating the object and ref directories if
    /// absent and rebuilding the handle index from the persisted refs.
    pub fn open(root: impl AsRef<Path>) -> io::Result<BlobStore> {
        let root = root.as_ref();
        let objects = root.join("objects");
        let refs = root.join("refs");
        fs::create_dir_all(&objects)?;
        fs::create_dir_all(&refs)?;

        let mut index = HashMap::new();
        for entry in fs::read_dir(&refs)? {
            let path = entry?.path();
            let Some(id) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(parse_uuid)
            else {
                continue;
            };
            let mut sha = [0u8; 32];
            File::open(&path)?.read_exact(&mut sha)?;
            index.insert(id, sha);
        }
        Ok(BlobStore {
            objects,
            refs,
            index,
        })
    }

    /// Store `bytes` and return a [`BlobRef`] handle for them. A blob at or below
    /// [`INLINE_MAX`] is returned inline and never written to disk. A larger one
    /// is content-addressed by sha256 — identical bytes reuse the existing object
    /// — and a fresh UUID → sha256 mapping is persisted so the returned handle
    /// resolves after a reopen.
    pub fn put(&mut self, bytes: &[u8], mime: &str) -> io::Result<BlobRef> {
        let id = new_uuid();
        let size = bytes.len() as u64;

        if bytes.len() <= INLINE_MAX {
            return Ok(BlobRef {
                id,
                mime: mime.to_string(),
                size,
                inline: Some(bytes.to_vec()),
            });
        }

        let sha = content_sha(bytes);
        // Content-addressed: write the object only when this content is new, so a
        // repeat put of identical bytes leaves the single shared object in place.
        let object = self.object_path(&sha);
        if !object.exists() {
            atomic_write(&object, bytes)?;
        }
        // Persist the public handle → content mapping before mirroring it.
        atomic_write(&self.ref_path(&id), &sha)?;
        self.index.insert(id, sha);

        Ok(BlobRef {
            id,
            mime: mime.to_string(),
            size,
            inline: None,
        })
    }

    /// Store `bytes` under a fresh handle that [`get`](BlobStore::get) always
    /// resolves — the HTTP fetch channel, where even a small blob must be
    /// retrievable by handle rather than riding an op's inline ref. Unlike
    /// [`put`](BlobStore::put), no blob is left inline-only: the bytes are
    /// content-addressed and the UUID → sha256 mapping persisted regardless of
    /// size. The returned ref still reports `inline` truthfully, so a caller may
    /// skip the fetch for a small blob it just uploaded.
    pub fn put_fetchable(&mut self, bytes: &[u8], mime: &str) -> io::Result<BlobRef> {
        let id = new_uuid();
        let sha = content_sha(bytes);
        let object = self.object_path(&sha);
        if !object.exists() {
            atomic_write(&object, bytes)?;
        }
        atomic_write(&self.ref_path(&id), &sha)?;
        self.index.insert(id, sha);
        Ok(BlobRef {
            id,
            mime: mime.to_string(),
            size: bytes.len() as u64,
            inline: (bytes.len() <= INLINE_MAX).then(|| bytes.to_vec()),
        })
    }

    /// Resolve a public handle to its stored bytes, or `None` if the id is
    /// unknown. Inline blobs are not held here — they ride their ref — so this
    /// serves stored (non-inline) blobs.
    pub fn get(&self, id: &[u8; 16]) -> io::Result<Option<Vec<u8>>> {
        let Some(sha) = self.index.get(id) else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        File::open(self.object_path(sha))?.read_to_end(&mut bytes)?;
        Ok(Some(bytes))
    }

    /// The object file backing the content with sha256 `sha`.
    fn object_path(&self, sha: &[u8; 32]) -> PathBuf {
        self.objects.join(hex(sha))
    }

    /// The ref file backing the public handle `id`.
    fn ref_path(&self, id: &[u8; 16]) -> PathBuf {
        self.refs.join(hex(id))
    }
}

/// The sha256 of a blob's bytes — its internal content address.
fn content_sha(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().into()
}

/// A fresh 16-byte public handle drawn from system entropy — unguessable and
/// independent of the content, so it never reveals whether the store holds the
/// bytes.
fn new_uuid() -> [u8; 16] {
    let mut id = [0u8; 16];
    getrandom::getrandom(&mut id).expect("system entropy is available");
    id
}

/// Write `buf` to `path` atomically: fill a sibling temp, flush it, then rename
/// it into place so a reader sees either the whole prior file or the whole new
/// one. The temp name carries a fresh UUID so concurrent writers never collide.
fn atomic_write(path: &Path, buf: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension(hex(&new_uuid()));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(buf)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Lowercase hex of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// Parse a 32-char lowercase-hex ref filename back to its 16-byte handle,
/// rejecting any name that is not exactly one.
pub(crate) fn parse_uuid(name: &str) -> Option<[u8; 16]> {
    let bytes = name.as_bytes();
    if bytes.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (i, chunk) in bytes.chunks_exact(2).enumerate() {
        id[i] = (unhex(chunk[0])? << 4) | unhex(chunk[1])?;
    }
    Some(id)
}

/// One lowercase-hex digit to its nibble.
fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_sha_is_deterministic_and_content_sensitive() {
        assert_eq!(content_sha(b"hello"), content_sha(b"hello"));
        assert_ne!(content_sha(b"hello"), content_sha(b"world"));
    }

    #[test]
    fn hex_round_trips_a_handle() {
        let id = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        assert_eq!(parse_uuid(&hex(&id)), Some(id));
    }

    #[test]
    fn parse_uuid_rejects_non_handles() {
        assert_eq!(parse_uuid("short"), None);
        assert_eq!(parse_uuid(&"zz".repeat(16)), None);
        assert_eq!(parse_uuid(&"a".repeat(31)), None);
    }

    use std::sync::atomic::{AtomicU64, Ordering};

    static NONCE: AtomicU64 = AtomicU64::new(0);

    fn temp_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "crdtsync-blobstore-{}-{}",
            std::process::id(),
            NONCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    // Touches the filesystem — skip under Miri, which cannot execute the syscalls.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn put_fetchable_round_trips_inline_and_stored() {
        let root = temp_root();
        let mut store = BlobStore::open(&root).unwrap();

        // A small blob reports `inline` yet is still fetchable by handle.
        let small = store.put_fetchable(b"hi", "text/plain").unwrap();
        assert!(small.inline.is_some());
        assert_eq!(store.get(&small.id).unwrap().as_deref(), Some(&b"hi"[..]));

        // A large blob is stored and fetched by handle.
        let big = vec![9u8; INLINE_MAX + 1];
        let large = store
            .put_fetchable(&big, "application/octet-stream")
            .unwrap();
        assert!(large.inline.is_none());
        assert_eq!(store.get(&large.id).unwrap(), Some(big));

        std::fs::remove_dir_all(&root).ok();
    }
}
