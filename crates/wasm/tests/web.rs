//! WebAssembly bindings — the JS SDK end to end, run under wasm.
//!
//! Two documents that exchange ops converge. A slot is addressed by a path;
//! edits return the ops to broadcast and `apply` folds a peer's ops back in.
//! Run with `wasm-pack test --node crates/wasm`.

use crdtsync_core::path::encode_path;
use crdtsync_wasm::WasmDocument;
use wasm_bindgen_test::*;

fn cid(first: u8) -> Vec<u8> {
    let mut b = vec![0u8; 16];
    b[0] = first;
    b
}

fn doc(first: u8) -> WasmDocument {
    WasmDocument::new(&cid(first)).ok().unwrap()
}

fn path(keys: &[&str]) -> Vec<u8> {
    let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    encode_path(&keys)
}

#[wasm_bindgen_test]
fn a_bad_client_id_is_rejected() {
    assert!(WasmDocument::new(&[0u8; 4]).is_err());
}

#[wasm_bindgen_test]
fn register_reads_back_and_converges() {
    let mut a = doc(1);
    let mut b = doc(2);
    let p = path(&["age"]);
    let ops = a.register_int(&p, 30);
    assert_eq!(a.get_int(&p), Some(30));
    assert_eq!(b.apply(&ops), 1);
    assert_eq!(b.get_int(&p), Some(30));
}

#[wasm_bindgen_test]
fn a_missing_slot_is_absent() {
    let a = doc(1);
    assert_eq!(a.get_int(&path(&["nope"])), None);
}

#[wasm_bindgen_test]
fn a_counter_accumulates_across_replicas() {
    let mut a = doc(1);
    let mut b = doc(2);
    let p = path(&["n"]);
    let oa = a.inc(&p, 3);
    let ob = b.inc(&p, 4);
    b.apply(&oa);
    a.apply(&ob);
    assert_eq!(a.get_counter(&p), Some(7));
    assert_eq!(b.get_counter(&p), Some(7));
}

#[wasm_bindgen_test]
fn a_snapshot_round_trips_through_a_decode() {
    let mut a = doc(1);
    let p = path(&["age"]);
    a.register_int(&p, 30);
    a.inc(&path(&["hits"]), 5);

    let snap = a.encode_state();
    let back = WasmDocument::decode_state(&snap).ok().unwrap();
    assert_eq!(back.get_int(&p), Some(30));
    assert_eq!(back.get_counter(&path(&["hits"])), Some(5));
}

#[wasm_bindgen_test]
fn a_decoded_document_dedups_and_converges() {
    let mut a = doc(1);
    let reg = a.register_int(&path(&["age"]), 30);

    let mut back = WasmDocument::decode_state(&a.encode_state()).ok().unwrap();
    // A replay of a covered op is a no-op; a later peer op still lands.
    assert_eq!(back.apply(&reg), 0);
    let mut b = doc(2);
    b.apply(&reg);
    let hit = b.inc(&path(&["hits"]), 4);
    assert_eq!(back.apply(&hit), 1);
    assert_eq!(back.get_counter(&path(&["hits"])), Some(4));
}

#[wasm_bindgen_test]
fn decoding_garbage_state_is_an_error() {
    assert!(WasmDocument::decode_state(&[0xFF; 8]).is_err());
}

#[wasm_bindgen_test]
fn a_nested_path_converges() {
    let mut a = doc(1);
    let mut b = doc(2);
    let p = path(&["profile", "stats", "score"]);
    b.apply(&a.register_int(&p, 7));
    assert_eq!(b.get_int(&p), Some(7));
}

#[wasm_bindgen_test]
fn bytes_round_trip() {
    let mut a = doc(1);
    let p = path(&["blob"]);
    let want = vec![0u8, 1, 255, 0];
    a.set_bytes(&p, &want);
    assert_eq!(a.get_bytes(&p), Some(want));
}

#[wasm_bindgen_test]
fn delete_removes_a_slot() {
    let mut a = doc(1);
    let p = path(&["age"]);
    a.register_int(&p, 30);
    a.delete(&p);
    assert_eq!(a.get_int(&p), None);
}

#[wasm_bindgen_test]
fn a_list_converges() {
    let mut a = doc(1);
    let mut b = doc(2);
    let p = path(&["board", "cards"]);
    b.apply(&a.list_insert(&p, 0, b"x"));
    b.apply(&a.list_insert(&p, 1, b"y"));
    assert_eq!(b.list_len(&p), Some(2));
    assert_eq!(b.list_get(&p, 0), Some(b"x".to_vec()));
    // A delete of an absent list is inert.
    assert!(a.list_delete(&path(&["ghost"]), 0).is_empty());
    assert_eq!(a.list_len(&path(&["ghost"])), None);
}

#[wasm_bindgen_test]
fn a_text_converges_and_deletes() {
    let mut a = doc(1);
    let mut b = doc(2);
    let p = path(&["doc", "title"]);
    b.apply(&a.text_insert(&p, 0, "héllo"));
    assert_eq!(b.text_len(&p), Some(5));
    assert_eq!(b.text_get(&p), Some("héllo".to_string()));
    b.apply(&a.text_delete(&p, 1, 3));
    assert_eq!(b.text_get(&p), Some("ho".to_string()));
}

#[wasm_bindgen_test]
fn apply_rejects_garbage() {
    let mut a = doc(1);
    assert_eq!(a.apply(&[0xff; 8]), -1);
}

#[wasm_bindgen_test]
fn encode_path_frames_keys() {
    let k1 = js_sys::Uint8Array::from(&b"ab"[..]);
    let k2 = js_sys::Uint8Array::from(&b"c"[..]);
    let got = WasmDocument::encode_path(vec![k1, k2]);
    assert_eq!(got, vec![2, 0, 0, 0, b'a', b'b', 1, 0, 0, 0, b'c']);
}

use crdtsync_wasm::WasmUndo;

#[wasm_bindgen_test]
fn undo_and_redo_a_register() {
    let mut d = doc(1);
    let mut u = WasmUndo::new();
    let p = path(&["title"]);
    u.register_int(&mut d, &p, 1);
    u.register_int(&mut d, &p, 2);
    assert_eq!(d.get_int(&p), Some(2));
    assert!(u.can_undo());

    u.undo(&mut d);
    assert_eq!(d.get_int(&p), Some(1));
    u.redo(&mut d);
    assert_eq!(d.get_int(&p), Some(2));
    assert!(!u.can_redo());
}

#[wasm_bindgen_test]
fn undo_covers_list_and_text() {
    let mut d = doc(1);
    let mut u = WasmUndo::new();

    let items = path(&["items"]);
    u.list_insert(&mut d, &items, 0, b"a");
    assert_eq!(d.list_len(&items), Some(1));
    u.undo(&mut d);
    assert_eq!(d.list_len(&items), Some(0));

    let body = path(&["body"]);
    u.text_insert(&mut d, &body, 0, "hi");
    assert_eq!(d.text_get(&body), Some("hi".to_string()));
    u.undo(&mut d);
    assert_eq!(d.text_get(&body), Some(String::new()));
}

#[wasm_bindgen_test]
fn a_wasm_undo_converges_on_a_peer() {
    let mut a = doc(1);
    let mut b = doc(2);
    let mut u = WasmUndo::new();
    let p = path(&["votes"]);
    b.apply(&u.inc(&mut a, &p, 5));
    assert_eq!(b.get_counter(&p), Some(5));
    b.apply(&u.undo(&mut a));
    assert_eq!(b.get_counter(&p), Some(0));
}

use crdtsync_wasm::WasmClient;

fn wasm_client(first: u8) -> WasmClient {
    WasmClient::new(&cid(first)).ok().unwrap()
}

#[wasm_bindgen_test]
fn a_client_edit_travels_to_a_peer() {
    let mut a = wasm_client(1);
    let mut b = wasm_client(2);
    // Both fresh sessions assign channel 0 to their first subscription.
    let sa = a.subscribe(b"room-1");
    let sb = b.subscribe(b"room-1");
    assert_eq!(sa.channel(), 0);
    assert_eq!(sb.channel(), 0);

    let p = path(&["age"]);
    let ops = a.register_int(sa.channel(), &p, 30);
    assert_eq!(a.get_int(sa.channel(), &p), Some(30));
    assert!(b.receive(&ops));
    assert_eq!(b.get_int(sb.channel(), &p), Some(30));
    assert_eq!(b.last_seen_seq(sb.channel()), Some(1));
}

#[wasm_bindgen_test]
fn a_client_handshake_and_awareness_marshal() {
    let mut c = wasm_client(1);
    assert!(!c.hello().is_empty());
    assert!(!c.auth(b"token").is_empty());
    assert_eq!(c.actor(), None);

    let sub = c.subscribe(b"room-1");
    assert!(c.set_awareness(sub.channel(), b"cursor", b"x").is_some());
    assert_eq!(c.awareness_len(sub.channel()), 0);
    assert!(c.unsubscribe(sub.channel()).is_some());
    assert_eq!(c.last_seen_seq(sub.channel()), None);
    assert!(c.resume(sub.channel()).is_none());
}

#[wasm_bindgen_test]
fn a_client_version_requests_marshal() {
    let mut c = wasm_client(1);
    let sub = c.subscribe(b"room-1");
    let ch = sub.channel();
    assert!(c.create_version(ch, b"v1").is_some());
    assert!(c.rename_version(ch, b"v1", b"v2").is_some());
    assert!(c.delete_version(ch, b"v1").is_some());
    assert!(c.list_versions(ch).is_some());
    assert!(c.fetch_version(ch, b"v1").is_some());
    // Nothing reported until a server reply is folded in.
    assert!(c.versions(ch).is_empty());
    assert!(c.version_state(ch, b"v1").is_none());
}

#[wasm_bindgen_test]
fn a_client_rejects_garbage_frames() {
    let mut c = wasm_client(1);
    assert!(!c.receive(&[0xff, 0xff, 0xff, 0xff]));
}
