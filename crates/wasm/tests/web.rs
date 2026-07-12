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
fn a_relative_position_tracks_edits_and_round_trips() {
    let mut a = doc(1);
    let p = path(&["board", "cards"]);
    a.list_insert(&p, 0, b"a");
    a.list_insert(&p, 1, b"b");
    a.list_insert(&p, 2, b"c");
    // Anchor left of index 2, then insert ahead of it.
    let pos = a.relative_position(&p, 2, 0).expect("captured");
    assert_eq!(a.resolve_position(&p, &pos), Some(2));
    a.list_insert(&p, 0, b"z");
    assert_eq!(a.resolve_position(&p, &pos), Some(3));
    // A non-sequence slot, an unknown side, and malformed bytes are all absent.
    a.register_int(&path(&["age"]), 30);
    assert_eq!(a.relative_position(&path(&["age"]), 0, 0), None);
    assert_eq!(a.relative_position(&p, 0, 9), None);
    assert_eq!(a.resolve_position(&p, &[0xff, 0xff]), None);
}

#[wasm_bindgen_test]
fn a_text_relative_position_round_trips() {
    let mut a = doc(1);
    let t = path(&["doc", "title"]);
    a.text_insert(&t, 0, "hello");
    let pos = a.relative_position(&t, 5, 0).expect("captured");
    assert_eq!(a.resolve_position(&t, &pos), Some(5));
    a.text_insert(&t, 0, ">>");
    assert_eq!(a.resolve_position(&t, &pos), Some(7));
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

#[wasm_bindgen_test]
fn an_atomic_transaction_groups_edits_and_converges() {
    let mut a = doc(1);
    let mut b = doc(2);
    a.begin_atomic();
    // Edits accumulate while recording; each returns no ops of its own.
    assert!(a.register_int(&path(&["x"]), 1).is_empty());
    assert!(a.register_int(&path(&["y"]), 2).is_empty());
    let group = a.commit_atomic();
    assert!(!group.is_empty());

    assert_eq!(a.get_int(&path(&["x"])), Some(1));
    b.apply(&group);
    assert_eq!(b.get_int(&path(&["x"])), Some(1));
    assert_eq!(b.get_int(&path(&["y"])), Some(2));
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
    assert!(b.receive(&ops).unwrap());
    assert_eq!(b.get_int(sb.channel(), &p), Some(30));
    assert_eq!(b.last_seen_seq(sb.channel()), Some(1));
}

#[wasm_bindgen_test]
fn subscribe_branch_carries_the_named_branch() {
    use crdtsync_core::{decode_message, Message};

    let branch_of = |frame: &[u8]| match decode_message(frame).unwrap() {
        Message::Subscribe { branch, .. } => branch,
        other => panic!("expected Subscribe, got {other:?}"),
    };

    let mut a = wasm_client(1);
    // A named branch rides along in the Subscribe frame.
    let sub = a.subscribe_branch(b"room-1", b"feature-x");
    assert_eq!(sub.channel(), 0);
    assert_eq!(branch_of(&sub.frame()), b"feature-x");
    // An empty branch is the default/active branch, as the plain subscribe.
    let sub = a.subscribe_branch(b"room-1", b"");
    assert!(branch_of(&sub.frame()).is_empty());
    let sub = a.subscribe(b"room-1");
    assert!(branch_of(&sub.frame()).is_empty());
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
fn a_declared_app_rides_along_in_the_hello_frame() {
    let mut c = wasm_client(1);
    // A bare client opens as a relay — no app named in the frame.
    assert!(!c.hello().windows(5).any(|w| w == b"app-x"));
    // Declaring an app names it in the next Hello.
    c.declare_app(b"app-x", 3);
    assert!(c.hello().windows(5).any(|w| w == b"app-x"));
}

#[wasm_bindgen_test]
fn the_server_advertised_schema_is_recorded_and_readable() {
    use crdtsync_core::protocol::{encode_message, Message};
    let mut c = wasm_client(1);
    // Nothing advertised yet.
    assert_eq!(c.active_schema_version(), None);
    assert_eq!(c.active_schema(), None);

    // Folding a SchemaAdvert records the served version and its bytes.
    let advert = encode_message(&Message::SchemaAdvert {
        schema_version: 4,
        schema: b"schema-body".to_vec(),
    });
    assert!(c.receive(&advert).unwrap());
    assert_eq!(c.active_schema_version(), Some(4));
    assert_eq!(c.active_schema().as_deref(), Some(&b"schema-body"[..]));

    // A later advert supersedes it.
    let advert = encode_message(&Message::SchemaAdvert {
        schema_version: 5,
        schema: b"next-body".to_vec(),
    });
    assert!(c.receive(&advert).unwrap());
    assert_eq!(c.active_schema_version(), Some(5));
    assert_eq!(c.active_schema().as_deref(), Some(&b"next-body"[..]));

    // An empty body is still an advertisement, not `None`.
    let advert = encode_message(&Message::SchemaAdvert {
        schema_version: 6,
        schema: Vec::new(),
    });
    assert!(c.receive(&advert).unwrap());
    assert_eq!(c.active_schema_version(), Some(6));
    assert_eq!(c.active_schema().as_deref(), Some(&[][..]));
}

#[wasm_bindgen_test]
fn a_client_outbox_drains_on_ack() {
    use crdtsync_core::protocol::{encode_message, Channel, Message};
    let mut a = wasm_client(1);
    let sa = a.subscribe(b"room-1");
    let ch = sa.channel();

    a.register_int(ch, &path(&["age"]), 30);
    assert_eq!(a.outbox_len(ch), 1);
    a.register_int(ch, &path(&["age"]), 31);
    assert_eq!(a.outbox_len(ch), 2);
    // The unacknowledged tail replays as one Ops frame.
    assert!(a.resend(ch).is_some());

    // An Accepted through u64::MAX drains the outbox.
    let accepted = encode_message(&Message::Accepted {
        channel: Channel(ch),
        through: u64::MAX,
    });
    assert!(a.receive(&accepted).unwrap());
    assert_eq!(a.outbox_len(ch), 0);
    assert!(a.resend(ch).is_none());
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
    assert!(!c.receive(&[0xff, 0xff, 0xff, 0xff]).unwrap());
}

#[wasm_bindgen_test]
fn a_server_error_frame_throws_its_code() {
    use crdtsync_core::protocol::{encode_message, ErrorCode as CoreErrorCode, Message};
    use crdtsync_wasm::ErrorCode;
    let mut c = wasm_client(1);
    let err = encode_message(&Message::Error {
        code: CoreErrorCode::UpdateRequired,
        message: "please update".to_string(),
        details: Vec::new(),
    });
    // A server Error throws its code — UpdateRequired is the onUpdateRequired
    // signal; a normal frame still applies.
    let thrown = c.receive(&err).unwrap_err();
    assert_eq!(
        thrown.as_f64(),
        Some(ErrorCode::UpdateRequired as i32 as f64)
    );
    let sa = c.subscribe(b"room-1");
    let ops = c.register_int(sa.channel(), &path(&["age"]), 30);
    assert!(c.receive(&ops).unwrap());
}

#[wasm_bindgen_test]
fn a_server_ops_rejection_surfaces_the_refused_batch() {
    use crdtsync_core::protocol::{encode_message, Channel, ErrorCode as CoreErrorCode, Message};
    use crdtsync_wasm::ErrorCode;
    let mut c = wasm_client(1);
    let sub = c.subscribe(b"room-1");
    let ch = sub.channel();

    // Author an edit; its ops enter the outbox with per-client sequences 0..n.
    c.register_int(ch, &path(&["age"]), 30);
    let n = c.outbox_len(ch);
    assert!(n >= 1);
    let seqs: Vec<u64> = (0..n as u64).collect();

    // The server refuses that batch — Forbidden, the auth-revoked rejection.
    let rejection = encode_message(&Message::OpsRejected {
        channel: Channel(ch),
        seqs,
        reason: CoreErrorCode::Forbidden,
    });
    assert!(c.receive(&rejection).unwrap());

    // The drain yields one { channel, reason, ops } batch.
    let batches = js_sys::Array::from(&c.take_rejected());
    assert_eq!(batches.length(), 1);
    let entry = batches.get(0);
    let channel = js_sys::Reflect::get(&entry, &"channel".into())
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(channel, ch as f64);
    let reason = js_sys::Reflect::get(&entry, &"reason".into())
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(reason, ErrorCode::Forbidden as i32 as f64);
    let ops = js_sys::Array::from(&js_sys::Reflect::get(&entry, &"ops".into()).unwrap());
    assert_eq!(ops.length(), n as u32);
    // Each refused op carries its bytes.
    assert!(js_sys::Uint8Array::from(ops.get(0)).length() > 0);

    // The refused ops left the outbox; draining, a second call is empty.
    assert_eq!(c.outbox_len(ch), 0);
    assert_eq!(js_sys::Array::from(&c.take_rejected()).length(), 0);
}

#[wasm_bindgen_test]
fn a_client_atomic_transaction_travels_to_a_peer() {
    let mut a = wasm_client(1);
    let mut b = wasm_client(2);
    let sa = a.subscribe(b"room-1");
    let sb = b.subscribe(b"room-1");

    a.begin_atomic(sa.channel());
    // Edits accumulate while recording; only the commit frame is sent.
    a.register_int(sa.channel(), &path(&["x"]), 1);
    a.register_int(sa.channel(), &path(&["y"]), 2);
    let frame = a.commit_atomic(sa.channel());
    assert!(!frame.is_empty());
    assert_eq!(a.get_int(sa.channel(), &path(&["x"])), Some(1));

    assert!(b.receive(&frame).unwrap());
    assert_eq!(b.get_int(sb.channel(), &path(&["x"])), Some(1));
    assert_eq!(b.get_int(sb.channel(), &path(&["y"])), Some(2));
}

// --- schema-aware diff ---

fn get_str(obj: &wasm_bindgen::JsValue, key: &str) -> String {
    js_sys::Reflect::get(obj, &wasm_bindgen::JsValue::from_str(key))
        .unwrap()
        .as_string()
        .unwrap()
}

#[wasm_bindgen_test]
fn diff_reports_a_value_change_as_a_tagged_object() {
    let mut a = doc(1);
    let p = path(&["age"]);
    a.register_int(&p, 30);
    let old = a.encode_state();
    a.register_int(&p, 31);
    let new = a.encode_state();

    let changes = WasmDocument::diff(&old, &new).unwrap();
    assert_eq!(changes.len(), 1);
    let c = &changes[0];
    assert_eq!(get_str(c, "op"), "value");
    let newv = js_sys::Reflect::get(c, &"new".into()).unwrap();
    assert_eq!(get_str(&newv, "t"), "int");
}

#[wasm_bindgen_test]
fn diff_reports_a_list_insert_with_an_items_array() {
    let mut a = doc(1);
    let p = path(&["xs"]);
    a.list_insert(&p, 0, &[1, 0, 0, 0, 0, 0, 0, 0]); // one scalar item
    let old = a.encode_state();
    a.list_insert(&p, 1, &[2, 0, 0, 0, 0, 0, 0, 0]);
    let new = a.encode_state();

    let changes = WasmDocument::diff(&old, &new).unwrap();
    assert_eq!(changes.len(), 1);
    let c = &changes[0];
    assert_eq!(get_str(c, "op"), "listInsert");
    let items = js_sys::Reflect::get(c, &"items".into()).unwrap();
    let items = js_sys::Array::from(&items);
    assert_eq!(items.length(), 1);
}

#[wasm_bindgen_test]
fn diff_of_a_malformed_snapshot_throws() {
    assert!(WasmDocument::diff(&[1, 2, 3], &[1, 2, 3]).is_err());
}

#[wasm_bindgen_test]
fn an_encoded_diff_round_trips_through_decode() {
    let mut a = doc(1);
    let p = path(&["age"]);
    a.register_int(&p, 30);
    let old = a.encode_state();
    a.register_int(&p, 31);
    let new = a.encode_state();

    // The opaque buffer decodes to the same tagged changes the structural diff shapes.
    let bytes = WasmDocument::diff_encoded(&old, &new).unwrap();
    let changes = WasmDocument::decode_changes(&bytes).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(get_str(&changes[0], "op"), "value");
    // A truncated buffer is a clean throw, never a panic.
    assert!(WasmDocument::decode_changes(&[0xFF, 0xFF, 0xFF]).is_err());
}

// --- xml ---

#[wasm_bindgen_test]
fn an_xml_element_reads_its_tag() {
    let mut a = doc(1);
    let p = path(&["body"]);
    a.xml_element(&p, b"section");
    assert_eq!(a.xml_tag(&p), Some(b"section".to_vec()));
    // A fragment is tagless, and a plain register is not an element.
    let f = path(&["frag"]);
    a.xml_fragment(&f);
    assert_eq!(a.xml_tag(&f), None);
    a.register_int(&path(&["n"]), 1);
    assert_eq!(a.xml_tag(&path(&["n"])), None);
    assert_eq!(a.xml_tag(&path(&["absent"])), None);
}

#[wasm_bindgen_test]
fn xml_children_insert_count_delete_and_converge() {
    let mut a = doc(1);
    let mut b = doc(2);
    let p = path(&["body"]);
    b.apply(&a.xml_element(&p, b"div"));
    b.apply(&a.xml_insert_element(&p, 0, b"p"));
    b.apply(&a.xml_insert_text(&p, 1, "hello"));
    assert_eq!(a.xml_children_len(&p), Some(2));
    assert_eq!(b.xml_children_len(&p), Some(2));

    b.apply(&a.xml_child_delete(&p, 0));
    assert_eq!(a.xml_children_len(&p), Some(1));
    assert_eq!(b.xml_children_len(&p), Some(1));

    // A non-node path has no child count and its edits are inert.
    assert!(a.xml_insert_element(&path(&["ghost"]), 0, b"x").is_empty());
    assert_eq!(a.xml_children_len(&path(&["ghost"])), None);
}

#[wasm_bindgen_test]
fn an_xml_child_moves_to_a_new_parent_and_converges() {
    let mut a = doc(1);
    let mut b = doc(2);
    // Two path-addressable fragments; the mover is a child of `src`.
    let src = path(&["src"]);
    let dst = path(&["dst"]);
    b.apply(&a.xml_fragment(&src));
    b.apply(&a.xml_fragment(&dst));
    b.apply(&a.xml_insert_element(&src, 0, b"leaf"));
    assert_eq!(a.xml_children_len(&src), Some(1));
    assert_eq!(a.xml_children_len(&dst), Some(0));

    // Relocate src's only child under dst — its identity and subtree ride along.
    b.apply(&a.xml_move(&src, 0, &dst, 0));
    assert_eq!(a.xml_children_len(&src), Some(0));
    assert_eq!(a.xml_children_len(&dst), Some(1));
    assert_eq!(b.xml_children_len(&src), Some(0));
    assert_eq!(b.xml_children_len(&dst), Some(1));

    // A move naming no live child is inert.
    assert!(a.xml_move(&src, 5, &dst, 0).is_empty());
}

// --- marks ---

use crdtsync_core::Scalar;

fn get_bool(obj: &wasm_bindgen::JsValue, key: &str) -> bool {
    js_sys::Reflect::get(obj, &wasm_bindgen::JsValue::from_str(key))
        .unwrap()
        .as_bool()
        .unwrap()
}

fn get_bytes_field(obj: &wasm_bindgen::JsValue, key: &str) -> Vec<u8> {
    let v = js_sys::Reflect::get(obj, &wasm_bindgen::JsValue::from_str(key)).unwrap();
    js_sys::Uint8Array::new(&v).to_vec()
}

// A schema declaring the mark flavors over a top-level "body" text, so the read
// model resolves boolean/value marks (an undeclared name defaults to object).
const MARK_SCHEMA: &[u8] = br#"{
    "schema": "doc", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map", "children": { "body": "Body" } }, "Body": { "kind": "text" } },
    "marks": { "bold": { "flavor": "boolean" }, "link": { "flavor": "value" } }
}"#;

#[wasm_bindgen_test]
fn a_mark_is_authored_read_changed_and_deleted() {
    let mut a = doc(1);
    assert!(a.set_schema(MARK_SCHEMA));
    let t = path(&["body"]);
    a.text_insert(&t, 0, "hello world");
    // A boolean mark over [0,5) — the mark id is the handle.
    let on = Scalar::Bool(true).encode_state();
    let id = a.mark(&t, 0, 0, 5, 1, b"bold", &on).expect("mark authored");
    assert_eq!(id.len(), 16);

    let marks = js_sys::Array::from(&a.marks_at(&t, 0));
    assert_eq!(marks.length(), 1);
    let m = marks.get(0);
    assert_eq!(get_bytes_field(&m, "name"), b"bold".to_vec());
    assert_eq!(get_str(&m, "kind"), "boolean");
    assert!(get_bool(&m, "value"));

    // The payload change and the delete each emit broadcastable ops.
    let off = Scalar::Bool(false).encode_state();
    assert!(!a.mark_set_value(&id, &off).is_empty());
    assert!(!a.mark_delete(&id).is_empty());
    assert_eq!(js_sys::Array::from(&a.marks_at(&t, 0)).length(), 0);

    // A non-sequence path yields no handle, an unknown side is rejected, and a
    // malformed value is rejected.
    a.register_int(&path(&["n"]), 1);
    assert!(a.mark(&path(&["n"]), 0, 0, 1, 1, b"x", &on).is_none());
    assert!(a.mark(&t, 0, 9, 5, 1, b"x", &on).is_none());
    assert!(a.mark(&t, 0, 0, 5, 1, b"x", &[0xFF, 0xFF]).is_none());
    assert_eq!(
        js_sys::Array::from(&a.marks_at(&path(&["n"]), 0)).length(),
        0
    );
}

#[wasm_bindgen_test]
fn a_value_mark_reads_as_a_tagged_scalar() {
    let mut a = doc(1);
    assert!(a.set_schema(MARK_SCHEMA));
    let t = path(&["body"]);
    a.text_insert(&t, 0, "abc");
    let payload = Scalar::Int(7).encode_state();
    a.mark(&t, 0, 0, 3, 1, b"link", &payload).expect("authored");

    let marks = js_sys::Array::from(&a.marks_at(&t, 1));
    assert_eq!(marks.length(), 1);
    let m = marks.get(0);
    assert_eq!(get_str(&m, "kind"), "value");
    let v = js_sys::Reflect::get(&m, &"value".into()).unwrap();
    // The scalar rides the same tagged `{ t, v }` shape as change_to_js.
    assert_eq!(get_str(&v, "t"), "int");
}

// --- schema / repair ---

#[wasm_bindgen_test]
fn a_schema_binds_or_is_rejected_and_repairs_drain() {
    let mut a = doc(1);
    // A malformed schema binds nothing.
    assert!(!a.set_schema(b"not json"));
    // A well-formed schema binds.
    assert!(a.set_schema(MARK_SCHEMA));
    // With the current state as baseline, nothing newly needs repair.
    assert_eq!(js_sys::Array::from(&a.take_repairs()).length(), 0);
}

// --- client-routed xml / marks (outbox / resend) ---

#[wasm_bindgen_test]
fn a_client_xml_edit_rides_the_outbox_and_travels_to_a_peer() {
    let mut a = wasm_client(1);
    let mut b = wasm_client(2);
    let sa = a.subscribe(b"room-1");
    let sb = b.subscribe(b"room-1");
    let p = path(&["body"]);
    // Each edit enters the outbox — resent / acknowledged, not framed and forgotten.
    let frame = a.xml_element(sa.channel(), &p, b"div");
    assert!(!frame.is_empty());
    assert_eq!(a.outbox_len(sa.channel()), 1);
    assert!(a.resend(sa.channel()).is_some());
    assert!(b.receive(&frame).unwrap());

    assert!(b
        .receive(&a.xml_insert_element(sa.channel(), &p, 0, b"p"))
        .unwrap());
    assert_eq!(a.xml_tag(sa.channel(), &p), Some(b"div".to_vec()));
    assert_eq!(b.xml_children_len(sb.channel(), &p), Some(1));
}

#[wasm_bindgen_test]
fn a_client_mark_over_a_non_sequence_is_inert() {
    let mut a = wasm_client(1);
    let sa = a.subscribe(b"room-1");
    // A fragment is not a sequence, so this author enqueues nothing and hands back
    // no handle.
    let t = path(&["body"]);
    a.xml_fragment(sa.channel(), &t);
    let outbox = a.outbox_len(sa.channel());
    let on = Scalar::Bool(true).encode_state();
    assert!(a.mark(sa.channel(), &t, 0, 0, 0, 1, b"c", &on).is_none());
    assert_eq!(a.outbox_len(sa.channel()), outbox);
    // An unheld channel is likewise inert.
    assert!(a.mark(9, &t, 0, 0, 0, 1, b"c", &on).is_none());
}
