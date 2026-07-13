//! The content-addressable local-filesystem blob store: small blobs ride inline
//! in the ref and never touch disk; larger ones are content-addressed by sha256
//! (identical bytes dedupe to one object) and fetched by an opaque public UUID
//! that survives a reopen.

use crdtsync_server::blobs::{BlobStore, INLINE_MAX};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-blobs-{pid}-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// The number of on-disk content objects — one per distinct sha256.
fn object_count(root: &std::path::Path) -> usize {
    std::fs::read_dir(root.join("objects"))
        .map(|d| d.count())
        .unwrap_or(0)
}

/// The number of persisted public-handle → content mappings.
fn ref_count(root: &std::path::Path) -> usize {
    std::fs::read_dir(root.join("refs"))
        .map(|d| d.count())
        .unwrap_or(0)
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn a_small_blob_inlines_and_never_touches_the_fs() {
    let tmp = tempdir();
    let mut store = BlobStore::open(tmp.path()).unwrap();
    let bytes = b"a small note that rides in the ref";

    let r = store.put(bytes, "text/plain").unwrap();

    assert_eq!(r.inline.as_deref(), Some(&bytes[..]));
    assert_eq!(r.size, bytes.len() as u64);
    assert_eq!(r.mime, "text/plain");
    // Small blobs live in the ref, not the store: nothing is written.
    assert_eq!(object_count(tmp.path()), 0);
    assert_eq!(ref_count(tmp.path()), 0);
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn a_large_blob_stores_and_round_trips_by_id() {
    let tmp = tempdir();
    let mut store = BlobStore::open(tmp.path()).unwrap();
    let bytes: Vec<u8> = (0..INLINE_MAX as u32 + 1).map(|i| i as u8).collect();

    let r = store.put(&bytes, "application/octet-stream").unwrap();

    assert!(r.inline.is_none(), "a stored blob leaves inline empty");
    assert_eq!(r.size, bytes.len() as u64);
    assert_eq!(store.get(&r.id).unwrap().unwrap(), bytes);
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn identical_large_blobs_dedupe_to_one_object_under_distinct_handles() {
    let tmp = tempdir();
    let mut store = BlobStore::open(tmp.path()).unwrap();
    let bytes = vec![0x5Au8; INLINE_MAX + 500];

    let a = store.put(&bytes, "x").unwrap();
    let b = store.put(&bytes, "x").unwrap();

    // Distinct public handles — the UUID never leaks the content.
    assert_ne!(a.id, b.id);
    // Content-addressed: identical bytes are stored exactly once.
    assert_eq!(object_count(tmp.path()), 1);
    // Both handles resolve the same bytes.
    assert_eq!(store.get(&a.id).unwrap().unwrap(), bytes);
    assert_eq!(store.get(&b.id).unwrap().unwrap(), bytes);
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn distinct_large_blobs_store_distinct_objects() {
    let tmp = tempdir();
    let mut store = BlobStore::open(tmp.path()).unwrap();

    let a = store.put(&vec![1u8; INLINE_MAX + 1], "x").unwrap();
    let b = store.put(&vec![2u8; INLINE_MAX + 1], "x").unwrap();

    assert_eq!(object_count(tmp.path()), 2);
    assert_eq!(
        store.get(&a.id).unwrap().unwrap(),
        vec![1u8; INLINE_MAX + 1]
    );
    assert_eq!(
        store.get(&b.id).unwrap().unwrap(),
        vec![2u8; INLINE_MAX + 1]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn get_of_an_unknown_id_is_none_not_a_panic() {
    let tmp = tempdir();
    let store = BlobStore::open(tmp.path()).unwrap();
    assert!(store.get(&[0u8; 16]).unwrap().is_none());
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn a_stored_blob_survives_a_drop_and_reopen() {
    let tmp = tempdir();
    let bytes = vec![0x3Cu8; INLINE_MAX + 777];

    let id = {
        let mut store = BlobStore::open(tmp.path()).unwrap();
        store.put(&bytes, "x").unwrap().id
    };

    // A fresh handle over the same root resolves the id — mapping and object
    // both survived.
    let store = BlobStore::open(tmp.path()).unwrap();
    assert_eq!(store.get(&id).unwrap().unwrap(), bytes);
}

#[test]
#[cfg_attr(miri, ignore)] // drives the blob store on the filesystem
fn the_inline_boundary_is_4096_in_4097_out() {
    let tmp = tempdir();
    let mut store = BlobStore::open(tmp.path()).unwrap();

    let at = store.put(&vec![1u8; 4096], "x").unwrap();
    assert!(at.inline.is_some(), "exactly 4096 bytes inlines");
    assert_eq!(object_count(tmp.path()), 0);

    let over = store.put(&vec![1u8; 4097], "x").unwrap();
    assert!(over.inline.is_none(), "4097 bytes is stored");
    assert_eq!(object_count(tmp.path()), 1);
    assert_eq!(store.get(&over.id).unwrap().unwrap(), vec![1u8; 4097]);
}
