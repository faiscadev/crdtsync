//! WebAssembly bindings for the CRDT core, for JavaScript.
//!
//! A [`WasmDocument`] is a local replica. A slot is addressed by a path — a
//! length-framed sequence of `Uint8Array` keys, the last the slot, the rest
//! nested maps (build one with [`WasmDocument::encode_path`]). An edit applies
//! locally and returns the ops to broadcast; `apply` folds a peer's ops back
//! in. Navigation lives in `crdtsync_core::path`; this layer only marshals
//! JS values.

use crdtsync_core::{decode_ops, encode_ops, path, ClientId, Document};
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
