//! WebAssembly bindings for the CRDT core, for JavaScript.
//!
//! A [`WasmDocument`] is a local replica. A slot is addressed by a path — a
//! length-framed sequence of `Uint8Array` keys, the last the slot, the rest
//! nested maps (build one with [`WasmDocument::encode_path`]). An edit applies
//! locally and returns the ops to broadcast; `apply` folds a peer's ops back
//! in. Navigation lives in `crdtsync_core::path`; this layer only marshals
//! JS values.

use crdtsync_core::op::Op;
use crdtsync_core::{
    decode_message, decode_ops, encode_message, encode_ops, path, Channel, ClientId, ClientSession,
    Document, Message, Scalar, UndoManager,
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

    /// Fold a peer's encoded ops in. Returns the number applied, -1 on error.
    pub fn apply(&mut self, ops: &[u8]) -> i32 {
        match decode_ops(ops) {
            Ok(ops) => ops.iter().filter(|op| self.inner.apply(op)).count() as i32,
            Err(_) => -1,
        }
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
        match self.inner.document_mut(Channel(channel)) {
            Some(doc) => encode_message(&Message::Ops {
                channel: Channel(channel),
                ops: run(doc),
            }),
            None => Vec::new(),
        }
    }
}
