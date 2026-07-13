//! Inline blob producer — the pure-core path façade for small blobs.
//!
//! `set_blob` mints a stable public handle from injected entropy and inlines the
//! bytes; `get_blob` reads the ref back. Inline only: bytes over `INLINE_MAX`
//! belong to the store-backed large-blob path and are rejected here.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::{path, Host, INLINE_MAX};

mod common;
use common::cid;

// Deterministic host: entropy is a fixed fill byte so a minted handle is
// reproducible across a test's replicas.
struct TestHost {
    fill: u8,
}
impl Host for TestHost {
    fn entropy(&self, buf: &mut [u8]) {
        buf.fill(self.fill);
    }
    fn now_unix_millis(&self) -> u64 {
        0
    }
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn host(fill: u8) -> TestHost {
    TestHost { fill }
}

fn p(keys: &[&str]) -> Vec<u8> {
    let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    path::encode_path(&keys)
}

fn replay(b: &mut Document, ops: &[Op]) {
    for op in ops {
        b.apply(op);
    }
}

#[test]
fn set_blob_reads_back() {
    let mut d = doc(1);
    let bytes = vec![0x89, b'P', b'N', b'G', 0x00, 0xFF];
    path::set_blob(&mut d, &p(&["avatar"]), &host(0xAB), "image/png", &bytes).expect("inlines");

    let blob = path::get_blob(&d, &p(&["avatar"])).expect("reads a ref");
    assert_eq!(blob.mime, "image/png");
    assert_eq!(blob.inline.as_deref(), Some(bytes.as_slice()));
    assert_eq!(blob.size, bytes.len() as u64);
    assert_eq!(blob.id, [0xAB; 16]);
}

#[test]
fn a_nested_path_reads_back() {
    let mut d = doc(1);
    let path = p(&["profile", "media", "cover"]);
    let bytes = vec![1, 2, 3, 4];
    path::set_blob(&mut d, &path, &host(7), "image/jpeg", &bytes).expect("inlines");

    let blob = path::get_blob(&d, &path).expect("reads a ref");
    assert_eq!(blob.mime, "image/jpeg");
    assert_eq!(blob.inline.as_deref(), Some(bytes.as_slice()));
}

#[test]
fn empty_bytes_inline_as_an_empty_slice() {
    let mut d = doc(1);
    path::set_blob(
        &mut d,
        &p(&["empty"]),
        &host(1),
        "application/octet-stream",
        &[],
    )
    .expect("ok");

    let blob = path::get_blob(&d, &p(&["empty"])).expect("reads a ref");
    assert_eq!(blob.size, 0);
    assert_eq!(blob.inline.as_deref(), Some(&[][..]));
}

#[test]
fn exactly_inline_max_inlines() {
    let mut d = doc(1);
    let bytes = vec![0x5A; INLINE_MAX];
    let ops = path::set_blob(
        &mut d,
        &p(&["big"]),
        &host(2),
        "application/octet-stream",
        &bytes,
    )
    .expect("at the bound, still inline");
    assert!(!ops.is_empty());

    let blob = path::get_blob(&d, &p(&["big"])).expect("reads a ref");
    assert_eq!(blob.size, INLINE_MAX as u64);
    assert_eq!(blob.inline.as_deref(), Some(bytes.as_slice()));
}

#[test]
fn one_over_inline_max_is_rejected() {
    let mut d = doc(1);
    let bytes = vec![0x5A; INLINE_MAX + 1];
    let out = path::set_blob(
        &mut d,
        &p(&["huge"]),
        &host(2),
        "application/octet-stream",
        &bytes,
    );
    assert!(
        out.is_none(),
        "over the bound belongs to the large-blob path"
    );
    // The rejected write lands nothing.
    assert_eq!(path::get_blob(&d, &p(&["huge"])), None);
}

#[test]
fn the_emitted_ops_converge_on_a_second_replica() {
    let mut a = doc(1);
    let mut b = doc(2);
    let bytes = vec![10, 20, 30];
    let ops =
        path::set_blob(&mut a, &p(&["logo"]), &host(0xCD), "image/svg+xml", &bytes).expect("ok");
    replay(&mut b, &ops);

    let ra = path::get_blob(&a, &p(&["logo"])).expect("author reads");
    let rb = path::get_blob(&b, &p(&["logo"])).expect("peer reads");
    assert_eq!(ra, rb);
    assert_eq!(rb.mime, "image/svg+xml");
    assert_eq!(rb.inline.as_deref(), Some(bytes.as_slice()));
}

#[test]
fn the_handle_is_deterministic_for_a_given_host() {
    let mut d = doc(1);
    path::set_blob(&mut d, &p(&["a"]), &host(0x11), "text/plain", b"x").expect("ok");
    path::set_blob(&mut d, &p(&["b"]), &host(0x11), "text/plain", b"y").expect("ok");
    assert_eq!(
        path::get_blob(&d, &p(&["a"])).unwrap().id,
        path::get_blob(&d, &p(&["b"])).unwrap().id
    );
}

// A blob ref replaces LWW like any other scalar assignment.
#[test]
fn a_later_blob_replaces_the_earlier_one() {
    let mut d = doc(1);
    path::set_blob(&mut d, &p(&["pic"]), &host(1), "image/png", b"first").expect("ok");
    path::set_blob(&mut d, &p(&["pic"]), &host(2), "image/jpeg", b"second").expect("ok");

    let blob = path::get_blob(&d, &p(&["pic"])).expect("reads the latest");
    assert_eq!(blob.mime, "image/jpeg");
    assert_eq!(blob.inline.as_deref(), Some(&b"second"[..]));
    assert_eq!(blob.id, [2; 16]);
}

// get_blob is typed: a non-blob slot reads None, and a blob slot is not a
// bytes/register slot.
#[test]
fn get_blob_ignores_a_non_blob_slot() {
    let mut d = doc(1);
    path::set_bytes(&mut d, &p(&["raw"]), b"not a blob");
    path::register_int(&mut d, &p(&["n"]), 5);
    assert_eq!(path::get_blob(&d, &p(&["raw"])), None);
    assert_eq!(path::get_blob(&d, &p(&["n"])), None);
    assert_eq!(path::get_blob(&d, &p(&["missing"])), None);
}

// A blob slot is a distinct value: it is neither a bytes slot nor a register.
#[test]
fn a_blob_slot_is_not_a_bytes_slot() {
    let mut d = doc(1);
    path::set_blob(&mut d, &p(&["pic"]), &host(1), "image/png", b"data").expect("ok");
    assert_eq!(path::get_bytes(&d, &p(&["pic"])), None);
    assert_eq!(path::get_register(&d, &p(&["pic"])), None);
}

// Regression: a normal bytes slot still round-trips unchanged next to the new
// blob surface.
#[test]
fn a_bytes_slot_still_reads_back() {
    let mut d = doc(1);
    path::set_bytes(&mut d, &p(&["data"]), b"hello");
    assert_eq!(path::get_bytes(&d, &p(&["data"])), Some(b"hello".to_vec()));
    assert_eq!(path::get_blob(&d, &p(&["data"])), None);
}

// The ref-only producer: a handle the caller already holds, no bytes inline.
#[test]
fn set_blob_ref_reads_back_a_ref() {
    let mut d = doc(1);
    let id = [0x42; 16];
    path::set_blob_ref(&mut d, &p(&["video"]), id, "video/mp4", 10_000_000);

    let blob = path::get_blob(&d, &p(&["video"])).expect("reads a ref");
    assert_eq!(blob.id, id);
    assert_eq!(blob.mime, "video/mp4");
    assert_eq!(blob.size, 10_000_000);
    assert_eq!(blob.inline, None);
}

// A ref may name a blob far larger than the inline bound — the bytes never ride
// the op, so there is no size guard.
#[test]
fn a_ref_names_a_blob_over_the_inline_bound() {
    let mut d = doc(1);
    let ops = path::set_blob_ref(
        &mut d,
        &p(&["huge"]),
        [1; 16],
        "application/octet-stream",
        (INLINE_MAX as u64) * 1024,
    );
    assert!(!ops.is_empty());

    let blob = path::get_blob(&d, &p(&["huge"])).expect("reads a ref");
    assert_eq!(blob.size, (INLINE_MAX as u64) * 1024);
    assert_eq!(blob.inline, None);
}

#[test]
fn the_emitted_ref_op_converges_on_a_second_replica() {
    let mut a = doc(1);
    let mut b = doc(2);
    let id = [0x77; 16];
    let ops = path::set_blob_ref(&mut a, &p(&["doc"]), id, "application/pdf", 500_000);
    replay(&mut b, &ops);

    let ra = path::get_blob(&a, &p(&["doc"])).expect("author reads");
    let rb = path::get_blob(&b, &p(&["doc"])).expect("peer reads");
    assert_eq!(ra, rb);
    assert_eq!(rb.id, id);
    assert_eq!(rb.mime, "application/pdf");
    assert_eq!(rb.size, 500_000);
    assert_eq!(rb.inline, None);
}

// An inline blob and a ref blob at different paths coexist and read back
// distinctly: inline carries bytes, the ref does not.
#[test]
fn an_inline_blob_and_a_ref_blob_coexist() {
    let mut d = doc(1);
    path::set_blob(&mut d, &p(&["small"]), &host(3), "image/png", b"tiny").expect("inlines");
    path::set_blob_ref(&mut d, &p(&["large"]), [9; 16], "video/mp4", 9_000_000);

    let inline = path::get_blob(&d, &p(&["small"])).expect("reads inline");
    assert_eq!(inline.inline.as_deref(), Some(&b"tiny"[..]));

    let refonly = path::get_blob(&d, &p(&["large"])).expect("reads ref");
    assert_eq!(refonly.inline, None);
    assert_eq!(refonly.id, [9; 16]);
}

// Overwriting an inline slot with a ref (and vice-versa) is an ordinary LWW
// scalar replace — the later write wins.
#[test]
fn overwriting_an_inline_blob_with_a_ref_is_lww() {
    let mut d = doc(1);
    path::set_blob(&mut d, &p(&["slot"]), &host(1), "image/png", b"inline").expect("ok");
    path::set_blob_ref(&mut d, &p(&["slot"]), [5; 16], "video/mp4", 1_000);

    let blob = path::get_blob(&d, &p(&["slot"])).expect("reads the winner");
    assert_eq!(blob.mime, "video/mp4");
    assert_eq!(blob.id, [5; 16]);
    assert_eq!(blob.inline, None);
}

#[test]
fn overwriting_a_ref_with_an_inline_blob_is_lww() {
    let mut d = doc(1);
    path::set_blob_ref(&mut d, &p(&["slot"]), [5; 16], "video/mp4", 1_000);
    path::set_blob(&mut d, &p(&["slot"]), &host(2), "image/png", b"inline").expect("ok");

    let blob = path::get_blob(&d, &p(&["slot"])).expect("reads the winner");
    assert_eq!(blob.mime, "image/png");
    assert_eq!(blob.inline.as_deref(), Some(&b"inline"[..]));
    assert_eq!(blob.id, [2; 16]);
}
