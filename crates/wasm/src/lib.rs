//! WebAssembly bindings for the CRDT core, for JavaScript.
//!
//! A [`WasmDocument`] is a local replica. A slot is addressed by a path — a
//! length-framed sequence of `Uint8Array` keys, the last the slot, the rest
//! nested maps (build one with [`WasmDocument::encode_path`]). An edit applies
//! locally and returns the ops to broadcast; `apply` folds a peer's ops back
//! in. Navigation lives in `crdtsync_core::path`; this layer only marshals
//! JS values.

use crdtsync_core::diff::{diff as core_diff, Change, SeqItem};
use crdtsync_core::element::ElementKind;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::list::Side;
use crdtsync_core::op::Op;
use crdtsync_core::{
    decode_message, decode_ops, encode_message, encode_ops, path, Channel, ClientId, ClientSession,
    Document, Message, RelativePosition, Scalar, UndoManager,
};
use wasm_bindgen::prelude::*;

/// A CRDT replica for one 16-byte client id.
#[wasm_bindgen]
pub struct WasmDocument {
    inner: Document,
}

#[wasm_bindgen]
impl WasmDocument {
    /// Open a document for the given 16-byte client id.
    #[wasm_bindgen(constructor)]
    pub fn new(client_id: &[u8]) -> Result<WasmDocument, JsError> {
        let bytes: [u8; 16] = client_id
            .try_into()
            .map_err(|_| JsError::new("client id must be 16 bytes"))?;
        Ok(WasmDocument {
            inner: Document::new(ClientId::from_bytes(bytes)),
        })
    }

    /// Serialize the whole replica to a canonical snapshot.
    #[wasm_bindgen(js_name = encodeState)]
    pub fn encode_state(&self) -> Vec<u8> {
        self.inner.encode_state()
    }

    /// Open a document from a snapshot produced by [`WasmDocument::encode_state`].
    #[wasm_bindgen(js_name = decodeState)]
    pub fn decode_state(state: &[u8]) -> Result<WasmDocument, JsError> {
        Document::decode_state(state)
            .map(|inner| WasmDocument { inner })
            .map_err(|e| JsError::new(&format!("{e:?}")))
    }

    /// Encode a path from its keys.
    #[wasm_bindgen(js_name = encodePath)]
    pub fn encode_path(keys: Vec<js_sys::Uint8Array>) -> Vec<u8> {
        let owned: Vec<Vec<u8>> = keys.iter().map(js_sys::Uint8Array::to_vec).collect();
        path::encode_path(&owned.iter().map(Vec::as_slice).collect::<Vec<_>>())
    }

    /// Diff two snapshots — each a state buffer from [`WasmDocument::encode_state`],
    /// a named version, or an exported room — into an array of structural change
    /// objects turning the old state into the new. Each change has an `op` tag, a
    /// `path` (Uint8Array), and its variant's fields; a scalar is a tagged
    /// `{ t, v }` object so it is read unambiguously. Throws on a malformed
    /// snapshot.
    #[wasm_bindgen(js_name = diff)]
    pub fn diff(old_state: &[u8], new_state: &[u8]) -> Result<Vec<JsValue>, JsError> {
        let old = Document::decode_state(old_state).map_err(|e| JsError::new(&format!("{e:?}")))?;
        let new = Document::decode_state(new_state).map_err(|e| JsError::new(&format!("{e:?}")))?;
        Ok(core_diff(&old, &new).iter().map(change_to_js).collect())
    }

    /// Install-or-set an integer Register at a path. Returns the ops to broadcast.
    #[wasm_bindgen(js_name = registerInt)]
    pub fn register_int(&mut self, path: &[u8], value: i64) -> Vec<u8> {
        encode_ops(&path::register_int(&mut self.inner, path, value))
    }

    /// Install-or-increment a Counter at a path.
    pub fn inc(&mut self, path: &[u8], amount: u32) -> Vec<u8> {
        encode_ops(&path::inc(&mut self.inner, path, amount))
    }

    /// Install-or-decrement a Counter at a path.
    pub fn dec(&mut self, path: &[u8], amount: u32) -> Vec<u8> {
        encode_ops(&path::dec(&mut self.inner, path, amount))
    }

    /// Set a bytes scalar at a path.
    #[wasm_bindgen(js_name = setBytes)]
    pub fn set_bytes(&mut self, path: &[u8], value: &[u8]) -> Vec<u8> {
        encode_ops(&path::set_bytes(&mut self.inner, path, value))
    }

    /// Tombstone the slot at a path.
    pub fn delete(&mut self, path: &[u8]) -> Vec<u8> {
        encode_ops(&path::delete(&mut self.inner, path))
    }

    /// Read an integer Register at a path.
    #[wasm_bindgen(js_name = getInt)]
    pub fn get_int(&self, path: &[u8]) -> Option<i64> {
        path::get_int(&self.inner, path)
    }

    /// Read a Counter's value at a path.
    #[wasm_bindgen(js_name = getCounter)]
    pub fn get_counter(&self, path: &[u8]) -> Option<i64> {
        path::get_counter(&self.inner, path)
    }

    /// Read a bytes scalar at a path.
    #[wasm_bindgen(js_name = getBytes)]
    pub fn get_bytes(&self, path: &[u8]) -> Option<Vec<u8>> {
        path::get_bytes(&self.inner, path)
    }

    /// Insert a bytes item at a live index in the List at a path.
    #[wasm_bindgen(js_name = listInsert)]
    pub fn list_insert(&mut self, path: &[u8], index: usize, value: &[u8]) -> Vec<u8> {
        encode_ops(&path::list_insert(&mut self.inner, path, index, value))
    }

    /// Tombstone the live item at an index in the List at a path.
    #[wasm_bindgen(js_name = listDelete)]
    pub fn list_delete(&mut self, path: &[u8], index: usize) -> Vec<u8> {
        encode_ops(&path::list_delete(&mut self.inner, path, index))
    }

    /// Read the live length of the List at a path.
    #[wasm_bindgen(js_name = listLen)]
    pub fn list_len(&self, path: &[u8]) -> Option<usize> {
        path::list_len(&self.inner, path)
    }

    /// Read the bytes item at a live index in the List at a path.
    #[wasm_bindgen(js_name = listGet)]
    pub fn list_get(&self, path: &[u8], index: usize) -> Option<Vec<u8>> {
        path::list_get(&self.inner, path, index)
    }

    /// Insert text at a codepoint index in the Text at a path.
    #[wasm_bindgen(js_name = textInsert)]
    pub fn text_insert(&mut self, path: &[u8], index: usize, s: &str) -> Vec<u8> {
        encode_ops(&path::text_insert(&mut self.inner, path, index, s))
    }

    /// Tombstone `count` codepoints from an index in the Text at a path.
    #[wasm_bindgen(js_name = textDelete)]
    pub fn text_delete(&mut self, path: &[u8], index: usize, count: usize) -> Vec<u8> {
        encode_ops(&path::text_delete(&mut self.inner, path, index, count))
    }

    /// Read the codepoint length of the Text at a path.
    #[wasm_bindgen(js_name = textLen)]
    pub fn text_len(&self, path: &[u8]) -> Option<usize> {
        path::text_len(&self.inner, path)
    }

    /// Read the Text at a path as a string.
    #[wasm_bindgen(js_name = textGet)]
    pub fn text_get(&self, path: &[u8]) -> Option<String> {
        path::text_get(&self.inner, path)
    }

    /// Capture a stable position in the List or Text at a path — the encoded
    /// `RelativePosition` bytes, resolved later with `resolvePosition`. `side` is
    /// 0 (left of `index`) or 1 (right). Returns `undefined` for a non-sequence
    /// slot or an unknown `side`.
    #[wasm_bindgen(js_name = relativePosition)]
    pub fn relative_position(&self, path: &[u8], index: usize, side: u32) -> Option<Vec<u8>> {
        let side = match side {
            0 => Side::Left,
            1 => Side::Right,
            _ => return None,
        };
        path::relative_position(&self.inner, path, index, side).map(|p| p.encode())
    }

    /// Resolve a captured position (bytes from `relativePosition`) back to a live
    /// index in the List or Text at a path. Returns `undefined` for a non-sequence
    /// slot or malformed position bytes.
    #[wasm_bindgen(js_name = resolvePosition)]
    pub fn resolve_position(&self, path: &[u8], pos: &[u8]) -> Option<usize> {
        let position = RelativePosition::decode(pos).ok()?;
        path::resolve_position(&self.inner, path, &position)
    }

    /// Fold a peer's encoded ops in. Returns the number applied, -1 on error.
    pub fn apply(&mut self, ops: &[u8]) -> i32 {
        match decode_ops(ops) {
            Ok(ops) => ops.iter().filter(|op| self.inner.apply(op)).count() as i32,
            Err(_) => -1,
        }
    }

    /// Begin recording an atomic transaction; edits accumulate until commit.
    #[wasm_bindgen(js_name = beginAtomic)]
    pub fn begin_atomic(&mut self) {
        self.inner.begin_atomic();
    }

    /// Commit the atomic transaction, returning the group's ops to broadcast.
    #[wasm_bindgen(js_name = commitAtomic)]
    pub fn commit_atomic(&mut self) -> Vec<u8> {
        encode_ops(&self.inner.commit_atomic())
    }
}

/// A per-user undo/redo manager over a [`WasmDocument`]. Each edit made through
/// it records its inverse; `undo`/`redo` emit ordinary ops that converge on peers
/// like any edit. The manager is separate from the document it drives, so every
/// call names the document.
#[wasm_bindgen]
pub struct WasmUndo {
    inner: UndoManager,
}

#[wasm_bindgen]
impl WasmUndo {
    /// Open an undo manager.
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmUndo {
        WasmUndo {
            inner: UndoManager::new(),
        }
    }

    /// Set an integer Register at a path as one undo step. Returns the ops.
    #[wasm_bindgen(js_name = registerInt)]
    pub fn register_int(&mut self, doc: &mut WasmDocument, path: &[u8], value: i64) -> Vec<u8> {
        encode_ops(
            &self
                .inner
                .register(&mut doc.inner, path, Scalar::Int(value)),
        )
    }

    /// Increment a Counter at a path as one undo step.
    pub fn inc(&mut self, doc: &mut WasmDocument, path: &[u8], amount: u32) -> Vec<u8> {
        encode_ops(&self.inner.inc(&mut doc.inner, path, amount))
    }

    /// Decrement a Counter at a path as one undo step.
    pub fn dec(&mut self, doc: &mut WasmDocument, path: &[u8], amount: u32) -> Vec<u8> {
        encode_ops(&self.inner.dec(&mut doc.inner, path, amount))
    }

    /// Tombstone the Register slot at a path as one undo step.
    pub fn delete(&mut self, doc: &mut WasmDocument, path: &[u8]) -> Vec<u8> {
        encode_ops(&self.inner.delete(&mut doc.inner, path))
    }

    /// Insert a bytes item at a live index in the List at a path as one undo step.
    #[wasm_bindgen(js_name = listInsert)]
    pub fn list_insert(
        &mut self,
        doc: &mut WasmDocument,
        path: &[u8],
        index: usize,
        value: &[u8],
    ) -> Vec<u8> {
        encode_ops(&self.inner.list_insert(&mut doc.inner, path, index, value))
    }

    /// Tombstone the live item at an index in the List at a path as one undo step.
    #[wasm_bindgen(js_name = listDelete)]
    pub fn list_delete(&mut self, doc: &mut WasmDocument, path: &[u8], index: usize) -> Vec<u8> {
        encode_ops(&self.inner.list_delete(&mut doc.inner, path, index))
    }

    /// Insert text at a codepoint index in the Text at a path as one undo step.
    #[wasm_bindgen(js_name = textInsert)]
    pub fn text_insert(
        &mut self,
        doc: &mut WasmDocument,
        path: &[u8],
        index: usize,
        s: &str,
    ) -> Vec<u8> {
        encode_ops(&self.inner.text_insert(&mut doc.inner, path, index, s))
    }

    /// Tombstone `count` codepoints from an index in the Text at a path as one
    /// undo step.
    #[wasm_bindgen(js_name = textDelete)]
    pub fn text_delete(
        &mut self,
        doc: &mut WasmDocument,
        path: &[u8],
        index: usize,
        count: usize,
    ) -> Vec<u8> {
        encode_ops(&self.inner.text_delete(&mut doc.inner, path, index, count))
    }

    /// Revert the most recent intention; returns the ops (empty if none).
    pub fn undo(&mut self, doc: &mut WasmDocument) -> Vec<u8> {
        encode_ops(&self.inner.undo(&mut doc.inner).unwrap_or_default())
    }

    /// Replay the most recently undone intention; returns the ops (empty if none).
    pub fn redo(&mut self, doc: &mut WasmDocument) -> Vec<u8> {
        encode_ops(&self.inner.redo(&mut doc.inner).unwrap_or_default())
    }

    /// Whether there is a recorded intention to undo.
    #[wasm_bindgen(js_name = canUndo)]
    pub fn can_undo(&self) -> bool {
        self.inner.can_undo()
    }

    /// Whether there is an undone intention to redo.
    #[wasm_bindgen(js_name = canRedo)]
    pub fn can_redo(&self) -> bool {
        self.inner.can_redo()
    }
}

impl Default for WasmUndo {
    fn default() -> Self {
        Self::new()
    }
}

/// The channel a [`WasmClient::subscribe`] assigned, and the Subscribe frame to
/// send for it.
#[wasm_bindgen]
pub struct WasmSubscription {
    channel: u32,
    frame: Vec<u8>,
}

#[wasm_bindgen]
impl WasmSubscription {
    /// The connection-local channel the room was assigned.
    #[wasm_bindgen(getter)]
    pub fn channel(&self) -> u32 {
        self.channel
    }

    /// The Subscribe frame to send to the server.
    #[wasm_bindgen(getter)]
    pub fn frame(&self) -> Vec<u8> {
        self.frame.clone()
    }
}

/// A wire client session for one 16-byte client id. It holds a replica per
/// subscribed room and turns local edits into wire frames to send; [`receive`]
/// folds a peer's frame back in. A room is addressed by the channel
/// [`subscribe`] returns.
#[wasm_bindgen]
pub struct WasmClient {
    inner: ClientSession,
}

#[wasm_bindgen]
impl WasmClient {
    /// Open a wire client for the given 16-byte client id.
    #[wasm_bindgen(constructor)]
    pub fn new(client_id: &[u8]) -> Result<WasmClient, JsError> {
        let bytes: [u8; 16] = client_id
            .try_into()
            .map_err(|_| JsError::new("client id must be 16 bytes"))?;
        Ok(WasmClient {
            inner: ClientSession::new(ClientId::from_bytes(bytes)),
        })
    }

    /// Declare the app this client speaks for and the schema version it targets,
    /// carried in the next `hello`. An empty `app_id` opens a relay connection; a
    /// named app with `schema_version` 0 is a dynamic client that adopts the
    /// server's head. Call before `hello`.
    pub fn declare_app(&mut self, app_id: &[u8], schema_version: u32) {
        self.inner.declare_app(app_id, schema_version);
    }

    /// The concrete schema version the enforcing server advertised for this
    /// session, or `None` before any advertisement. Distinct from the version
    /// declared in `declare_app`: a dynamic client (declared 0) learns the served
    /// version here. The app persists it across restart itself; the SDK caches,
    /// owns no storage.
    pub fn active_schema_version(&self) -> Option<u32> {
        self.inner.active_schema_version()
    }

    /// The bytes of the schema the enforcing server advertised for this session,
    /// or `None` before any advertisement. Pairs with `active_schema_version`.
    pub fn active_schema(&self) -> Option<Vec<u8>> {
        self.inner.active_schema().map(<[u8]>::to_vec)
    }

    /// The opening Hello frame to send, naming this client.
    pub fn hello(&self) -> Vec<u8> {
        encode_message(&self.inner.hello())
    }

    /// The Auth frame asking the server to verify `credential`.
    pub fn auth(&self, credential: &[u8]) -> Vec<u8> {
        encode_message(&self.inner.auth(credential))
    }

    /// The server-derived actor, present once AuthOk has been received.
    pub fn actor(&self) -> Option<Vec<u8>> {
        self.inner.actor().map(<[u8]>::to_vec)
    }

    /// Join `room` on a fresh channel; returns the channel and Subscribe frame.
    pub fn subscribe(&mut self, room: &[u8]) -> WasmSubscription {
        let (channel, msg) = self.inner.subscribe(room);
        WasmSubscription {
            channel: channel.0,
            frame: encode_message(&msg),
        }
    }

    /// Re-issue Subscribe for a held channel from its caught-up position; `None`
    /// if the channel isn't held.
    pub fn resume(&self, channel: u32) -> Option<Vec<u8>> {
        self.inner
            .resume(Channel(channel))
            .map(|msg| encode_message(&msg))
    }

    /// Re-emit the unacknowledged authored ops on `channel` as one Ops frame to
    /// replay after a reconnect; `None` when nothing is outstanding.
    pub fn resend(&self, channel: u32) -> Option<Vec<u8>> {
        self.inner
            .resend(Channel(channel))
            .map(|msg| encode_message(&msg))
    }

    /// How many authored ops on `channel` await acknowledgement.
    #[wasm_bindgen(js_name = outboxLen)]
    pub fn outbox_len(&self, channel: u32) -> usize {
        self.inner.outbox_len(Channel(channel))
    }

    /// Leave the room on `channel`, dropping its replica; `None` if not held.
    pub fn unsubscribe(&mut self, channel: u32) -> Option<Vec<u8>> {
        self.inner
            .unsubscribe(Channel(channel))
            .map(|msg| encode_message(&msg))
    }

    /// Fold one received wire frame in. `true` when applied, `false` when the
    /// frame is undecodable or the session refuses it.
    pub fn receive(&mut self, msg: &[u8]) -> bool {
        match decode_message(msg) {
            Ok(message) => self.inner.receive(message).is_ok(),
            Err(_) => false,
        }
    }

    /// The highest server sequence `channel` has caught up to.
    #[wasm_bindgen(js_name = lastSeenSeq)]
    pub fn last_seen_seq(&self, channel: u32) -> Option<u64> {
        self.inner.last_seen_seq(Channel(channel))
    }

    /// Install-or-set an integer Register at a path in `channel`'s room. Returns
    /// the Ops frame to send; empty if the channel isn't held.
    #[wasm_bindgen(js_name = registerInt)]
    pub fn register_int(&mut self, channel: u32, path: &[u8], value: i64) -> Vec<u8> {
        self.ops_frame(channel, |d| path::register_int(d, path, value))
    }

    /// Install-or-increment a Counter at a path in `channel`'s room.
    pub fn inc(&mut self, channel: u32, path: &[u8], amount: u32) -> Vec<u8> {
        self.ops_frame(channel, |d| path::inc(d, path, amount))
    }

    /// Install-or-decrement a Counter at a path in `channel`'s room.
    pub fn dec(&mut self, channel: u32, path: &[u8], amount: u32) -> Vec<u8> {
        self.ops_frame(channel, |d| path::dec(d, path, amount))
    }

    /// Set a bytes scalar at a path in `channel`'s room.
    #[wasm_bindgen(js_name = setBytes)]
    pub fn set_bytes(&mut self, channel: u32, path: &[u8], value: &[u8]) -> Vec<u8> {
        self.ops_frame(channel, |d| path::set_bytes(d, path, value))
    }

    /// Tombstone the slot at a path in `channel`'s room.
    pub fn delete(&mut self, channel: u32, path: &[u8]) -> Vec<u8> {
        self.ops_frame(channel, |d| path::delete(d, path))
    }

    /// Begin an atomic transaction on `channel`'s room; edits accumulate until
    /// commit.
    #[wasm_bindgen(js_name = beginAtomic)]
    pub fn begin_atomic(&mut self, channel: u32) {
        if let Some(doc) = self.inner.document_mut(Channel(channel)) {
            doc.begin_atomic();
        }
    }

    /// Commit the atomic transaction on `channel`, returning the Ops frame to
    /// send.
    #[wasm_bindgen(js_name = commitAtomic)]
    pub fn commit_atomic(&mut self, channel: u32) -> Vec<u8> {
        self.ops_frame(channel, |d| d.commit_atomic())
    }

    /// Read an integer Register at a path in `channel`'s room.
    #[wasm_bindgen(js_name = getInt)]
    pub fn get_int(&self, channel: u32, path: &[u8]) -> Option<i64> {
        self.inner
            .document(Channel(channel))
            .and_then(|d| path::get_int(d, path))
    }

    /// Read a bytes scalar at a path in `channel`'s room.
    #[wasm_bindgen(js_name = getBytes)]
    pub fn get_bytes(&self, channel: u32, path: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .document(Channel(channel))
            .and_then(|d| path::get_bytes(d, path))
    }

    /// Publish an ephemeral awareness entry `key` on `channel`'s room; returns
    /// the frame to send. `None` if the channel isn't held.
    #[wasm_bindgen(js_name = setAwareness)]
    pub fn set_awareness(&self, channel: u32, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .set_awareness(Channel(channel), key, value)
            .map(|msg| encode_message(&msg))
    }

    /// A peer's awareness entry on `channel` by publishing `actor` and `key`.
    pub fn awareness(&self, channel: u32, actor: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .awareness(Channel(channel), actor, key)
            .map(<[u8]>::to_vec)
    }

    /// How many awareness entries `channel` currently holds.
    #[wasm_bindgen(js_name = awarenessLen)]
    pub fn awareness_len(&self, channel: u32) -> usize {
        self.inner.awareness_len(Channel(channel))
    }

    /// Frame a request to capture `channel`'s room as version `name`. `None` if
    /// the channel isn't held.
    #[wasm_bindgen(js_name = createVersion)]
    pub fn create_version(&self, channel: u32, name: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .create_version(Channel(channel), name)
            .map(|m| encode_message(&m))
    }

    /// Frame a request to rename version `from` to `to`. `None` if the channel
    /// isn't held.
    #[wasm_bindgen(js_name = renameVersion)]
    pub fn rename_version(&self, channel: u32, from: &[u8], to: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .rename_version(Channel(channel), from, to)
            .map(|m| encode_message(&m))
    }

    /// Frame a request to delete version `name`. `None` if the channel isn't held.
    #[wasm_bindgen(js_name = deleteVersion)]
    pub fn delete_version(&self, channel: u32, name: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .delete_version(Channel(channel), name)
            .map(|m| encode_message(&m))
    }

    /// Frame a request for `channel`'s room's version names. `None` if the channel
    /// isn't held.
    #[wasm_bindgen(js_name = listVersions)]
    pub fn list_versions(&self, channel: u32) -> Option<Vec<u8>> {
        self.inner
            .list_versions(Channel(channel))
            .map(|m| encode_message(&m))
    }

    /// Frame a request for the captured state of version `name`. `None` if the
    /// channel isn't held.
    #[wasm_bindgen(js_name = fetchVersion)]
    pub fn fetch_version(&self, channel: u32, name: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .fetch_version(Channel(channel), name)
            .map(|m| encode_message(&m))
    }

    /// The version names last reported for `channel`'s room, in order. Empty if
    /// the channel isn't held or none have been reported.
    pub fn versions(&self, channel: u32) -> Vec<js_sys::Uint8Array> {
        self.inner
            .versions(Channel(channel))
            .unwrap_or(&[])
            .iter()
            .map(|name| js_sys::Uint8Array::from(name.as_slice()))
            .collect()
    }

    /// The captured state of a fetched version `name`, once it has arrived.
    #[wasm_bindgen(js_name = versionState)]
    pub fn version_state(&self, channel: u32, name: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .version_state(Channel(channel), name)
            .map(<[u8]>::to_vec)
    }
}

impl WasmClient {
    /// Run a path edit against `channel`'s replica and wrap the ops in the Ops
    /// frame to send; empty when the channel isn't held.
    fn ops_frame(&mut self, channel: u32, run: impl FnOnce(&mut Document) -> Vec<Op>) -> Vec<u8> {
        let Some(doc) = self.inner.document_mut(Channel(channel)) else {
            return Vec::new();
        };
        let ops = run(doc);
        // Route through the session so the ops enter the outbox and are resent /
        // acknowledged like a closure edit, not just framed and forgotten.
        match self.inner.enqueue_ops(Channel(channel), ops) {
            Some(msg) => encode_message(&msg),
            None => Vec::new(),
        }
    }
}

/// Set an own property on a plain object; infallible for a fresh `Object`.
fn set(obj: &js_sys::Object, key: &str, val: &JsValue) {
    js_sys::Reflect::set(obj, &JsValue::from_str(key), val).unwrap();
}

fn kind_name(k: ElementKind) -> &'static str {
    match k {
        ElementKind::Scalar => "scalar",
        ElementKind::Register => "register",
        ElementKind::Counter => "counter",
        ElementKind::Map => "map",
        ElementKind::List => "list",
        ElementKind::Text => "text",
        ElementKind::XmlElement => "xmlElement",
        ElementKind::XmlFragment => "xmlFragment",
    }
}

/// A scalar as a tagged `{ t, v }` object, so `Bytes` and a `BlobRef` (both
/// binary) are told apart and an `Int` keeps full 64-bit range as a BigInt.
fn scalar_to_js(s: &Scalar) -> JsValue {
    let obj = js_sys::Object::new();
    match s {
        Scalar::Null => set(&obj, "t", &JsValue::from_str("null")),
        Scalar::Bool(b) => {
            set(&obj, "t", &JsValue::from_str("bool"));
            set(&obj, "v", &JsValue::from_bool(*b));
        }
        Scalar::Int(n) => {
            set(&obj, "t", &JsValue::from_str("int"));
            set(&obj, "v", &js_sys::BigInt::from(*n).into());
        }
        Scalar::Bytes(b) => {
            set(&obj, "t", &JsValue::from_str("bytes"));
            set(&obj, "v", &js_sys::Uint8Array::from(b.as_slice()).into());
        }
        Scalar::BlobRef(_) => {
            set(&obj, "t", &JsValue::from_str("blobref"));
            set(
                &obj,
                "v",
                &js_sys::Uint8Array::from(s.encode_state().as_slice()).into(),
            );
        }
        Scalar::ElementRef(id) => {
            set(&obj, "t", &JsValue::from_str("elementref"));
            set(
                &obj,
                "v",
                &js_sys::Uint8Array::from(&id.as_bytes()[..]).into(),
            );
        }
    }
    obj.into()
}

fn item_to_js(item: &SeqItem) -> JsValue {
    let obj = js_sys::Object::new();
    match item {
        SeqItem::Scalar(s) => set(&obj, "scalar", &scalar_to_js(s)),
        SeqItem::Composite(k) => set(&obj, "kind", &JsValue::from_str(kind_name(*k))),
    }
    obj.into()
}

fn items_to_js(items: &[SeqItem]) -> JsValue {
    items
        .iter()
        .map(item_to_js)
        .collect::<js_sys::Array>()
        .into()
}

fn path_to_js(path: &[u8]) -> JsValue {
    js_sys::Uint8Array::from(path).into()
}

/// One structural change as a plain JS object: an `op` tag, a `path`, and the
/// variant's fields.
fn change_to_js(change: &Change) -> JsValue {
    let obj = js_sys::Object::new();
    match change {
        Change::Added { path, kind } => {
            set(&obj, "op", &JsValue::from_str("add"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "kind", &JsValue::from_str(kind_name(*kind)));
        }
        Change::Removed { path, kind } => {
            set(&obj, "op", &JsValue::from_str("remove"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "kind", &JsValue::from_str(kind_name(*kind)));
        }
        Change::Value { path, old, new } => {
            set(&obj, "op", &JsValue::from_str("value"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "old", &scalar_to_js(old));
            set(&obj, "new", &scalar_to_js(new));
        }
        Change::Counter { path, old, new } => {
            set(&obj, "op", &JsValue::from_str("counter"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "old", &js_sys::BigInt::from(*old).into());
            set(&obj, "new", &js_sys::BigInt::from(*new).into());
        }
        Change::ListInsert { path, index, items } => {
            set(&obj, "op", &JsValue::from_str("listInsert"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "index", &JsValue::from_f64(*index as f64));
            set(&obj, "items", &items_to_js(items));
        }
        Change::ListDelete { path, index, items } => {
            set(&obj, "op", &JsValue::from_str("listDelete"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "index", &JsValue::from_f64(*index as f64));
            set(&obj, "items", &items_to_js(items));
        }
        Change::TextInsert { path, index, text } => {
            set(&obj, "op", &JsValue::from_str("textInsert"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "index", &JsValue::from_f64(*index as f64));
            set(&obj, "text", &JsValue::from_str(text));
        }
        Change::TextDelete { path, index, text } => {
            set(&obj, "op", &JsValue::from_str("textDelete"));
            set(&obj, "path", &path_to_js(path));
            set(&obj, "index", &JsValue::from_f64(*index as f64));
            set(&obj, "text", &JsValue::from_str(text));
        }
        Change::MarkAdded {
            id,
            seq,
            name,
            value,
        } => {
            set(&obj, "op", &JsValue::from_str("markAdd"));
            set_mark_head(&obj, id, seq, name);
            set(&obj, "value", &scalar_to_js(value));
        }
        Change::MarkRemoved {
            id,
            seq,
            name,
            value,
        } => {
            set(&obj, "op", &JsValue::from_str("markRemove"));
            set_mark_head(&obj, id, seq, name);
            set(&obj, "value", &scalar_to_js(value));
        }
        Change::MarkChanged {
            id,
            seq,
            name,
            old,
            new,
        } => {
            set(&obj, "op", &JsValue::from_str("markChange"));
            set_mark_head(&obj, id, seq, name);
            set(&obj, "old", &scalar_to_js(old));
            set(&obj, "new", &scalar_to_js(new));
        }
    }
    obj.into()
}

/// The common head of a mark change as JS fields: the mark id, its target
/// sequence, and its name (each a `Uint8Array`).
fn set_mark_head(obj: &js_sys::Object, id: &ElementId, seq: &ElementId, name: &[u8]) {
    set(obj, "id", &js_sys::Uint8Array::from(&id.as_bytes()[..]).into());
    set(obj, "seq", &js_sys::Uint8Array::from(&seq.as_bytes()[..]).into());
    set(obj, "name", &js_sys::Uint8Array::from(name).into());
}
