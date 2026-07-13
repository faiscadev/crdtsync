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
use crdtsync_core::marks::{MarkState, ResolvedMark};
use crdtsync_core::op::Op;
use crdtsync_core::{
    decode_message, decode_ops, encode_message, encode_op, encode_ops, path, BlobRef, Channel,
    ClientError, ClientId, ClientSession, Document, ErrorCode as CoreErrorCode, Host, Redirect,
    Rejected, RelativePosition, Scalar, UndoManager,
};
use wasm_bindgen::prelude::*;

/// The browser's crypto RNG for the inline blob producer, which mints a blob's
/// handle from it. The blob path never reads the clock.
struct WasmHost;

impl Host for WasmHost {
    fn entropy(&self, buf: &mut [u8]) {
        getrandom::getrandom(buf).expect("crypto.getRandomValues is available");
    }

    fn now_unix_millis(&self) -> u64 {
        0
    }
}

/// Marshal a [`BlobRef`] into a plain JS object: `{ id: Uint8Array, mime: string,
/// size: number, inline: Uint8Array | null }`.
fn blob_ref_to_js(blob: &BlobRef) -> JsValue {
    let obj = js_sys::Object::new();
    set(&obj, "id", &js_sys::Uint8Array::from(&blob.id[..]).into());
    set(&obj, "mime", &JsValue::from_str(&blob.mime));
    set(&obj, "size", &JsValue::from_f64(blob.size as f64));
    let inline = match &blob.inline {
        Some(bytes) => js_sys::Uint8Array::from(bytes.as_slice()).into(),
        None => JsValue::NULL,
    };
    set(&obj, "inline", &inline);
    obj.into()
}

/// A failure the server reports to the client, surfaced by [`WasmClient::receive`].
/// `UpdateRequired` is the `onUpdateRequired` signal: the client's version can't
/// bridge the room's across a breaking gap, so the app prompts an update or falls
/// back read-only.
#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ErrorCode {
    ProtocolViolation = 0,
    UnsupportedVersion = 1,
    AuthFailed = 2,
    UnknownRoom = 3,
    Internal = 4,
    Forbidden = 5,
    UpdateRequired = 6,
}

impl From<CoreErrorCode> for ErrorCode {
    fn from(code: CoreErrorCode) -> Self {
        match code {
            CoreErrorCode::ProtocolViolation => ErrorCode::ProtocolViolation,
            CoreErrorCode::UnsupportedVersion => ErrorCode::UnsupportedVersion,
            CoreErrorCode::AuthFailed => ErrorCode::AuthFailed,
            CoreErrorCode::UnknownRoom => ErrorCode::UnknownRoom,
            CoreErrorCode::Internal => ErrorCode::Internal,
            CoreErrorCode::Forbidden => ErrorCode::Forbidden,
            CoreErrorCode::UpdateRequired => ErrorCode::UpdateRequired,
        }
    }
}

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

    /// Set an inline blob at a path from `mime` and `bytes`, minting the blob's
    /// public handle. Returns the ops to broadcast, or `undefined` when `bytes`
    /// exceeds the inline ceiling — a large blob is uploaded out of band and set
    /// with [`setBlobRef`](WasmDocument::set_blob_ref).
    #[wasm_bindgen(js_name = setBlob)]
    pub fn set_blob(&mut self, path: &[u8], mime: &str, bytes: &[u8]) -> Option<Vec<u8>> {
        path::set_blob(&mut self.inner, path, &WasmHost, mime, bytes).map(|ops| encode_ops(&ops))
    }

    /// Set a store-backed blob ref at a path from a 16-byte `id` handle, `mime`,
    /// and `size`. Carries no bytes; the content is fetched by id. Returns the ops
    /// to broadcast (throws if `id` is not 16 bytes).
    #[wasm_bindgen(js_name = setBlobRef)]
    pub fn set_blob_ref(
        &mut self,
        path: &[u8],
        id: &[u8],
        mime: &str,
        size: u64,
    ) -> Result<Vec<u8>, JsError> {
        let handle: [u8; 16] = id
            .try_into()
            .map_err(|_| JsError::new("blob id must be 16 bytes"))?;
        Ok(encode_ops(&path::set_blob_ref(
            &mut self.inner,
            path,
            handle,
            mime,
            size,
        )))
    }

    /// Read the blob ref at a path as `{ id, mime, size, inline }`, or `null` when
    /// the slot holds no blob ref.
    #[wasm_bindgen(js_name = getBlob)]
    pub fn get_blob(&self, path: &[u8]) -> JsValue {
        match path::get_blob(&self.inner, path) {
            Some(blob) => blob_ref_to_js(&blob),
            None => JsValue::NULL,
        }
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

    /// Install an `XmlElement` with `tag` at a map-slot path. Returns the ops to
    /// broadcast.
    #[wasm_bindgen(js_name = xmlElement)]
    pub fn xml_element(&mut self, path: &[u8], tag: &[u8]) -> Vec<u8> {
        encode_ops(&path::xml_element(&mut self.inner, path, tag))
    }

    /// Install a tagless `XmlFragment` at a map-slot path.
    #[wasm_bindgen(js_name = xmlFragment)]
    pub fn xml_fragment(&mut self, path: &[u8]) -> Vec<u8> {
        encode_ops(&path::xml_fragment(&mut self.inner, path))
    }

    /// The tag of the live `XmlElement` at a path, or `undefined` if the path is
    /// not a live element (a fragment is tagless, so it too reads `undefined`).
    #[wasm_bindgen(js_name = xmlTag)]
    pub fn xml_tag(&self, path: &[u8]) -> Option<Vec<u8>> {
        path::xml_tag(&self.inner, path)
    }

    /// Insert a nested `XmlElement` child with `tag` at live `index` in the children
    /// of the element/fragment at `elem_path`. Inert if the target is not a live
    /// XmlElement or XmlFragment.
    #[wasm_bindgen(js_name = xmlInsertElement)]
    pub fn xml_insert_element(&mut self, elem_path: &[u8], index: usize, tag: &[u8]) -> Vec<u8> {
        encode_ops(&path::xml_insert_element(
            &mut self.inner,
            elem_path,
            index,
            tag,
        ))
    }

    /// Insert a `Text`-run child initialised with `s` at live `index` in the
    /// children of the element/fragment at `elem_path`. Inert if the target is not
    /// a live XmlElement or XmlFragment.
    #[wasm_bindgen(js_name = xmlInsertText)]
    pub fn xml_insert_text(&mut self, elem_path: &[u8], index: usize, s: &str) -> Vec<u8> {
        encode_ops(&path::xml_insert_text(&mut self.inner, elem_path, index, s))
    }

    /// Tombstone the child at live `index` in the children of the element/fragment
    /// at `elem_path`. Inert if the target is not a live XmlElement or XmlFragment,
    /// or `index` names no live child.
    #[wasm_bindgen(js_name = xmlChildDelete)]
    pub fn xml_child_delete(&mut self, elem_path: &[u8], index: usize) -> Vec<u8> {
        encode_ops(&path::xml_child_delete(&mut self.inner, elem_path, index))
    }

    /// The count of live children of the element/fragment at `elem_path`, or
    /// `undefined` if the path is not a live XmlElement or XmlFragment.
    #[wasm_bindgen(js_name = xmlChildrenLen)]
    pub fn xml_children_len(&self, elem_path: &[u8]) -> Option<usize> {
        path::xml_children_len(&self.inner, elem_path)
    }

    /// Relocate the live child at `child_index` under the XML node at `parent_path`
    /// to `dest_index` in the children of the XML node at `new_parent_path` — a
    /// Kleppmann tree move that keeps the child's identity and subtree. Inert if
    /// either path is not a live XML node or `child_index` names no live child.
    #[wasm_bindgen(js_name = xmlMove)]
    pub fn xml_move(
        &mut self,
        parent_path: &[u8],
        child_index: usize,
        new_parent_path: &[u8],
        dest_index: usize,
    ) -> Vec<u8> {
        encode_ops(&path::xml_move_child(
            &mut self.inner,
            parent_path,
            child_index,
            new_parent_path,
            dest_index,
        ))
    }

    /// Author a mark named `name` over `[start_index, end_index)` of the sequence
    /// (Text or List) at `seq_path`, each endpoint captured with the given gravity
    /// `side` (0 left of the index, 1 right). `value` is an encoded [`Scalar`]
    /// payload. Returns the mark's id as bytes — the handle a later
    /// `markSetValue`/`markDelete` names it by — or `undefined` if `seq_path` is
    /// not a live sequence, a side is unknown, or the value is malformed.
    #[wasm_bindgen(js_name = mark)]
    #[allow(clippy::too_many_arguments)]
    pub fn mark(
        &mut self,
        seq_path: &[u8],
        start_index: usize,
        start_side: u32,
        end_index: usize,
        end_side: u32,
        name: &[u8],
        value: &[u8],
    ) -> Option<Vec<u8>> {
        let start = side_from_u32(start_side)?;
        let end = side_from_u32(end_side)?;
        let scalar = Scalar::decode_state(value).ok()?;
        let (_ops, id) = path::mark(
            &mut self.inner,
            seq_path,
            start_index,
            start,
            end_index,
            end,
            name,
            scalar,
        );
        id
    }

    /// Change the scalar payload of the mark handle `mark_id` to the encoded
    /// [`Scalar`] `value`. Returns the ops to broadcast; empty on a malformed value
    /// or a handle that names no live mark.
    #[wasm_bindgen(js_name = markSetValue)]
    pub fn mark_set_value(&mut self, mark_id: &[u8], value: &[u8]) -> Vec<u8> {
        let Ok(scalar) = Scalar::decode_state(value) else {
            return Vec::new();
        };
        encode_ops(&path::mark_set_value(&mut self.inner, mark_id, scalar))
    }

    /// Tombstone the mark handle `mark_id`. Returns the ops to broadcast; empty on
    /// a handle that names no live mark.
    #[wasm_bindgen(js_name = markDelete)]
    pub fn mark_delete(&mut self, mark_id: &[u8]) -> Vec<u8> {
        encode_ops(&path::mark_delete(&mut self.inner, mark_id))
    }

    /// The active marks covering character `index` of the sequence at `seq_path`,
    /// as an array of `{ name, kind, value }` objects (a boolean's value is a bool,
    /// a value mark's a tagged `{ t, v }` scalar, an object mark's an array of
    /// `Uint8Array` instance ids). Empty if `seq_path` is not a live sequence.
    #[wasm_bindgen(js_name = marksAt)]
    pub fn marks_at(&self, seq_path: &[u8], index: usize) -> JsValue {
        marks_to_js(&path::marks_at(&self.inner, seq_path, index))
    }

    /// Parse schema JSON bytes and bind the schema for `onRepaired` observation,
    /// returning whether it bound. Non-UTF-8 or JSON that is not a valid schema
    /// fails cleanly (`false`), binding nothing.
    #[wasm_bindgen(js_name = setSchema)]
    pub fn set_schema(&mut self, schema_bytes: &[u8]) -> bool {
        path::set_schema(&mut self.inner, schema_bytes)
    }

    /// The located paths whose repaired reading has newly changed against the bound
    /// schema since the last call — the `onRepaired` signal, as an array of
    /// `Uint8Array` (each an encoded repair path). Empty when no schema is bound or
    /// nothing newly needs repair.
    #[wasm_bindgen(js_name = takeRepairs)]
    pub fn take_repairs(&mut self) -> JsValue {
        path::take_repairs(&mut self.inner)
            .iter()
            .map(|p| js_sys::Uint8Array::from(p.as_slice()))
            .collect::<js_sys::Array>()
            .into()
    }

    /// Diff two snapshots into one opaque buffer — the encoded [`Change`]s a
    /// binding forwards across the SDK boundary and later reads with
    /// [`decodeChanges`](WasmDocument::decode_changes), mirroring how a captured
    /// position crosses as bytes. Throws on a malformed snapshot.
    #[wasm_bindgen(js_name = diffEncoded)]
    pub fn diff_encoded(old_state: &[u8], new_state: &[u8]) -> Result<Vec<u8>, JsError> {
        let old = Document::decode_state(old_state).map_err(|e| JsError::new(&format!("{e:?}")))?;
        let new = Document::decode_state(new_state).map_err(|e| JsError::new(&format!("{e:?}")))?;
        Ok(path::diff_encoded(&old, &new))
    }

    /// Decode a diff buffer — the encoded [`Change`]s a peer computed and forwarded
    /// as opaque bytes — back into the same array of tagged change objects
    /// [`diff`](WasmDocument::diff) returns. Throws on a truncated or malformed
    /// buffer.
    #[wasm_bindgen(js_name = decodeChanges)]
    pub fn decode_changes(bytes: &[u8]) -> Result<Vec<JsValue>, JsError> {
        let changes = path::decode_changes(bytes).map_err(|e| JsError::new(&format!("{e:?}")))?;
        Ok(changes.iter().map(change_to_js).collect())
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

    /// Join `branch` of `room` on a fresh channel; returns the channel and
    /// Subscribe frame. An empty `branch` is the default/active branch, as
    /// `subscribe`.
    #[wasm_bindgen(js_name = subscribeBranch)]
    pub fn subscribe_branch(&mut self, room: &[u8], branch: &[u8]) -> WasmSubscription {
        let (channel, msg) = self.inner.subscribe_branch(room, branch);
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
    /// frame is undecodable or the session refuses it. Throws the server
    /// [`ErrorCode`] when the frame is a server `Error` — `UpdateRequired` is the
    /// `onUpdateRequired` signal.
    pub fn receive(&mut self, msg: &[u8]) -> Result<bool, JsValue> {
        let Ok(message) = decode_message(msg) else {
            return Ok(false);
        };
        match self.inner.receive(message) {
            Ok(()) => Ok(true),
            Err(ClientError::Server { code, .. }) => {
                Err(JsValue::from(ErrorCode::from(code) as i32))
            }
            Err(_) => Ok(false),
        }
    }

    /// Drain the op batches the server refused since the last call — the
    /// `onOpsRejected` observation — as an array of `{ channel, reason, ops }`:
    /// `channel` a number, `reason` the server [`ErrorCode`] (`5` is `Forbidden`,
    /// the auth-revoked rejection), `ops` an array of `Uint8Array`, each a refused
    /// op's bytes to show, discard, or export. Empty when nothing was refused;
    /// draining, so a second call is empty.
    #[wasm_bindgen(js_name = takeRejected)]
    pub fn take_rejected(&mut self) -> JsValue {
        self.inner
            .take_rejected()
            .iter()
            .map(rejected_to_js)
            .collect::<js_sys::Array>()
            .into()
    }

    /// Drain the room redirects the server has sent since the last call — a node
    /// that does not lead a room telling the client to reconnect to its leader —
    /// as an array of `{ room, leaderAddr }`, each a `Uint8Array`: `room` the
    /// room name, `leaderAddr` the leader's advertise address. The core holds no
    /// socket, so reconnecting is the app's job. Empty when none arrived;
    /// draining, so a second call is empty.
    #[wasm_bindgen(js_name = takeRedirects)]
    pub fn take_redirects(&mut self) -> JsValue {
        self.inner
            .take_redirects()
            .iter()
            .map(redirect_to_js)
            .collect::<js_sys::Array>()
            .into()
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

    /// Set an inline blob at a path in `channel`'s room, routed through the outbox.
    /// Returns the Ops frame to send; a `bytes` length over the inline ceiling
    /// enqueues no op (use [`setBlobRef`](WasmClient::set_blob_ref) for a large
    /// blob).
    #[wasm_bindgen(js_name = setBlob)]
    pub fn set_blob(&mut self, channel: u32, path: &[u8], mime: &str, bytes: &[u8]) -> Vec<u8> {
        self.ops_frame(channel, |d| {
            path::set_blob(d, path, &WasmHost, mime, bytes).unwrap_or_default()
        })
    }

    /// Set a store-backed blob ref at a path in `channel`'s room from a 16-byte
    /// `id` handle, `mime`, and `size`, routed through the outbox. Returns the Ops
    /// frame to send (throws if `id` is not 16 bytes).
    #[wasm_bindgen(js_name = setBlobRef)]
    pub fn set_blob_ref(
        &mut self,
        channel: u32,
        path: &[u8],
        id: &[u8],
        mime: &str,
        size: u64,
    ) -> Result<Vec<u8>, JsError> {
        let handle: [u8; 16] = id
            .try_into()
            .map_err(|_| JsError::new("blob id must be 16 bytes"))?;
        Ok(self.ops_frame(channel, |d| path::set_blob_ref(d, path, handle, mime, size)))
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

    /// Install an `XmlElement` with `tag` at a path in `channel`'s room. Returns
    /// the Ops frame to send; empty if the channel isn't held.
    #[wasm_bindgen(js_name = xmlElement)]
    pub fn xml_element(&mut self, channel: u32, path: &[u8], tag: &[u8]) -> Vec<u8> {
        self.ops_frame(channel, |d| path::xml_element(d, path, tag))
    }

    /// Install a tagless `XmlFragment` at a path in `channel`'s room.
    #[wasm_bindgen(js_name = xmlFragment)]
    pub fn xml_fragment(&mut self, channel: u32, path: &[u8]) -> Vec<u8> {
        self.ops_frame(channel, |d| path::xml_fragment(d, path))
    }

    /// Insert a nested `XmlElement` child with `tag` at live `index` in the children
    /// of the element/fragment at `elem_path` in `channel`'s room.
    #[wasm_bindgen(js_name = xmlInsertElement)]
    pub fn xml_insert_element(
        &mut self,
        channel: u32,
        elem_path: &[u8],
        index: usize,
        tag: &[u8],
    ) -> Vec<u8> {
        self.ops_frame(channel, |d| {
            path::xml_insert_element(d, elem_path, index, tag)
        })
    }

    /// Insert a `Text`-run child initialised with `s` at live `index` in the
    /// children of the element/fragment at `elem_path` in `channel`'s room.
    #[wasm_bindgen(js_name = xmlInsertText)]
    pub fn xml_insert_text(
        &mut self,
        channel: u32,
        elem_path: &[u8],
        index: usize,
        s: &str,
    ) -> Vec<u8> {
        self.ops_frame(channel, |d| path::xml_insert_text(d, elem_path, index, s))
    }

    /// Tombstone the child at live `index` in the children of the element/fragment
    /// at `elem_path` in `channel`'s room.
    #[wasm_bindgen(js_name = xmlChildDelete)]
    pub fn xml_child_delete(&mut self, channel: u32, elem_path: &[u8], index: usize) -> Vec<u8> {
        self.ops_frame(channel, |d| path::xml_child_delete(d, elem_path, index))
    }

    /// The count of live children of the element/fragment at `elem_path` in
    /// `channel`'s room, or `undefined` if the path is not a live XML node.
    #[wasm_bindgen(js_name = xmlChildrenLen)]
    pub fn xml_children_len(&self, channel: u32, elem_path: &[u8]) -> Option<usize> {
        self.inner
            .document(Channel(channel))
            .and_then(|d| path::xml_children_len(d, elem_path))
    }

    /// The tag of the live `XmlElement` at a path in `channel`'s room, or
    /// `undefined` if the path is not a live element.
    #[wasm_bindgen(js_name = xmlTag)]
    pub fn xml_tag(&self, channel: u32, path: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .document(Channel(channel))
            .and_then(|d| path::xml_tag(d, path))
    }

    /// Relocate the live child at `child_index` under `parent_path` to `dest_index`
    /// in the children of `new_parent_path`, both in `channel`'s room — a Kleppmann
    /// tree move. Returns the Ops frame to send.
    #[wasm_bindgen(js_name = xmlMove)]
    pub fn xml_move(
        &mut self,
        channel: u32,
        parent_path: &[u8],
        child_index: usize,
        new_parent_path: &[u8],
        dest_index: usize,
    ) -> Vec<u8> {
        self.ops_frame(channel, |d| {
            path::xml_move_child(d, parent_path, child_index, new_parent_path, dest_index)
        })
    }

    /// Author a mark named `name` over `[start_index, end_index)` of the sequence at
    /// `seq_path` in `channel`'s room, routed through the outbox so it is resent /
    /// acknowledged like every other client edit. Each endpoint carries the given
    /// gravity `side` (0 left, 1 right); `value` is an encoded [`Scalar`]. Returns
    /// the mark's id — its handle — while the authoring ops ride the outbox (send
    /// them with [`resend`](WasmClient::resend)). `undefined` on an unheld channel,
    /// a non-sequence path, an unknown side, or a malformed value.
    #[wasm_bindgen(js_name = mark)]
    #[allow(clippy::too_many_arguments)]
    pub fn mark(
        &mut self,
        channel: u32,
        seq_path: &[u8],
        start_index: usize,
        start_side: u32,
        end_index: usize,
        end_side: u32,
        name: &[u8],
        value: &[u8],
    ) -> Option<Vec<u8>> {
        let start = side_from_u32(start_side)?;
        let end = side_from_u32(end_side)?;
        let scalar = Scalar::decode_state(value).ok()?;
        let doc = self.inner.document_mut(Channel(channel))?;
        let (ops, id) = path::mark(
            doc,
            seq_path,
            start_index,
            start,
            end_index,
            end,
            name,
            scalar,
        );
        self.inner.enqueue_ops(Channel(channel), ops);
        id
    }

    /// Change the payload of the mark handle `mark_id` in `channel`'s room, routed
    /// through the outbox. Returns the Ops frame to send; empty on a malformed
    /// value or a handle that names no live mark.
    #[wasm_bindgen(js_name = markSetValue)]
    pub fn mark_set_value(&mut self, channel: u32, mark_id: &[u8], value: &[u8]) -> Vec<u8> {
        let Ok(scalar) = Scalar::decode_state(value) else {
            return Vec::new();
        };
        self.ops_frame(channel, |d| path::mark_set_value(d, mark_id, scalar))
    }

    /// Tombstone the mark handle `mark_id` in `channel`'s room, routed through the
    /// outbox. Returns the Ops frame to send; empty on a handle that names no live
    /// mark.
    #[wasm_bindgen(js_name = markDelete)]
    pub fn mark_delete(&mut self, channel: u32, mark_id: &[u8]) -> Vec<u8> {
        self.ops_frame(channel, |d| path::mark_delete(d, mark_id))
    }

    /// The active marks covering character `index` of the sequence at `seq_path` in
    /// `channel`'s room, as an array of `{ name, kind, value }` objects (shaped as
    /// on [`WasmDocument::marks_at`]). Empty if the channel isn't held or the path
    /// is not a live sequence.
    #[wasm_bindgen(js_name = marksAt)]
    pub fn marks_at(&self, channel: u32, seq_path: &[u8], index: usize) -> JsValue {
        match self.inner.document(Channel(channel)) {
            Some(d) => marks_to_js(&path::marks_at(d, seq_path, index)),
            None => js_sys::Array::new().into(),
        }
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

/// The gravity `Side` a `0`/`1` endpoint code names — `None` for any other code,
/// matching how `relativePosition` reads a side.
fn side_from_u32(side: u32) -> Option<Side> {
    match side {
        0 => Some(Side::Left),
        1 => Some(Side::Right),
        _ => None,
    }
}

/// The resolved marks on a character as a JS array of `{ name, kind, value }`
/// objects: a boolean's value is a bool, a value mark's a tagged `{ t, v }`
/// scalar, an object mark's an array of `Uint8Array` instance ids.
fn marks_to_js(marks: &[ResolvedMark]) -> JsValue {
    marks
        .iter()
        .map(resolved_mark_to_js)
        .collect::<js_sys::Array>()
        .into()
}

fn resolved_mark_to_js(mark: &ResolvedMark) -> JsValue {
    let obj = js_sys::Object::new();
    set(
        &obj,
        "name",
        &js_sys::Uint8Array::from(mark.name.as_slice()).into(),
    );
    match &mark.state {
        MarkState::Boolean(b) => {
            set(&obj, "kind", &JsValue::from_str("boolean"));
            set(&obj, "value", &JsValue::from_bool(*b));
        }
        MarkState::Value(s) => {
            set(&obj, "kind", &JsValue::from_str("value"));
            set(&obj, "value", &scalar_to_js(s));
        }
        MarkState::Object(ids) => {
            set(&obj, "kind", &JsValue::from_str("object"));
            let arr: js_sys::Array = ids
                .iter()
                .map(|id| js_sys::Uint8Array::from(&id.as_bytes()[..]))
                .collect();
            set(&obj, "value", &arr.into());
        }
    }
    obj.into()
}

/// One refused op batch as a `{ channel, reason, ops }` object: `channel` a
/// number, `reason` the server `ErrorCode` discriminant, `ops` an array of
/// `Uint8Array` (each a refused op's bytes).
fn rejected_to_js(r: &Rejected) -> JsValue {
    let obj = js_sys::Object::new();
    set(&obj, "channel", &JsValue::from_f64(r.channel.0 as f64));
    set(&obj, "reason", &JsValue::from(ErrorCode::from(r.reason) as i32));
    let ops: js_sys::Array = r
        .ops
        .iter()
        .map(|op| js_sys::Uint8Array::from(encode_op(op).as_slice()))
        .collect();
    set(&obj, "ops", &ops.into());
    obj.into()
}

/// One redirect as a `{ room, leaderAddr }` object, each a `Uint8Array`: `room`
/// the redirected room, `leaderAddr` the leader's advertise address.
fn redirect_to_js(r: &Redirect) -> JsValue {
    let obj = js_sys::Object::new();
    set(
        &obj,
        "room",
        &js_sys::Uint8Array::from(r.room.as_slice()).into(),
    );
    set(
        &obj,
        "leaderAddr",
        &js_sys::Uint8Array::from(r.leader_addr.as_slice()).into(),
    );
    obj.into()
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
    set(
        obj,
        "id",
        &js_sys::Uint8Array::from(&id.as_bytes()[..]).into(),
    );
    set(
        obj,
        "seq",
        &js_sys::Uint8Array::from(&seq.as_bytes()[..]).into(),
    );
    set(obj, "name", &js_sys::Uint8Array::from(name).into());
}
