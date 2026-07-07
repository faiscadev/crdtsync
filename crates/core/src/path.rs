//! Path addressing — the stateless navigation the language bindings share.
//!
//! A slot is named by a path: a length-framed sequence of keys (each a
//! little-endian `u32` length then its bytes), the last the slot, the rest
//! nested maps. Edits apply locally and return the ops to broadcast; reads
//! resolve the whole path or yield nothing. A path that doesn't resolve is
//! inert — it neither panics nor materialises a container. Every binding wraps
//! this one implementation.

use std::cell::RefCell;
use std::rc::Rc;

use crate::anchor::RelativePosition;
use crate::doc::{Document, MapCursor};
use crate::list::Side;
use crate::map::Map;
use crate::op::Op;
use crate::stamp::Stamp;
use crate::{Element, Scalar};

/// Encode a path from its keys.
pub fn encode_path(keys: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for key in keys {
        // A key longer than a u32 can't be framed; fail loudly rather than
        // truncate the length into a corrupt path.
        let len = u32::try_from(key.len()).expect("path: key length exceeds u32");
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(key);
    }
    out
}

/// Parse a path into its keys, or `None` if a length header runs past the end.
pub fn parse_path(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let hdr = bytes.get(i..i.checked_add(4)?)?;
        let klen = u32::from_le_bytes(hdr.try_into().unwrap()) as usize;
        i += 4;
        let key = bytes.get(i..i.checked_add(klen)?)?;
        keys.push(key.to_vec());
        i += klen;
    }
    Some(keys)
}

/// Install-or-set a Register holding any scalar at a path.
pub fn register(doc: &mut Document, path: &[u8], value: Scalar) -> Vec<Op> {
    emit(doc, path, move |cur, key| cur.register(key, value))
}

/// Install-or-set an integer Register at a path.
pub fn register_int(doc: &mut Document, path: &[u8], value: i64) -> Vec<Op> {
    emit(doc, path, |cur, key| cur.register(key, Scalar::Int(value)))
}

/// Install-or-increment a Counter at a path.
pub fn inc(doc: &mut Document, path: &[u8], amount: u32) -> Vec<Op> {
    emit(doc, path, |cur, key| cur.inc(key, amount))
}

/// Install-or-decrement a Counter at a path.
pub fn dec(doc: &mut Document, path: &[u8], amount: u32) -> Vec<Op> {
    emit(doc, path, |cur, key| cur.dec(key, amount))
}

/// Set a bytes scalar at a path.
pub fn set_bytes(doc: &mut Document, path: &[u8], value: &[u8]) -> Vec<Op> {
    let value = value.to_vec();
    emit(doc, path, move |cur, key| {
        cur.set(key, Scalar::Bytes(value))
    })
}

/// Tombstone the slot at a path.
pub fn delete(doc: &mut Document, path: &[u8]) -> Vec<Op> {
    emit(doc, path, |cur, key| cur.delete(key))
}

/// Install an `XmlElement` with `tag` at a map-slot path. Its attrs are then
/// addressed by extending the path with the attr key (they descend as an
/// ordinary keyed Map); its children are a later slice.
pub fn xml_element(doc: &mut Document, path: &[u8], tag: &[u8]) -> Vec<Op> {
    let tag = tag.to_vec();
    emit(doc, path, move |cur, key| {
        cur.xml_element(key, &tag);
    })
}

/// Install a tagless `XmlFragment` at a map-slot path. A fragment has no attrs.
pub fn xml_fragment(doc: &mut Document, path: &[u8]) -> Vec<Op> {
    emit(doc, path, |cur, key| {
        cur.xml_fragment(key);
    })
}

/// The tag of the live `XmlElement` at a path, or `None` if the path is not a
/// live element (a fragment is tagless, so it too reads `None`).
pub fn xml_tag(doc: &Document, path: &[u8]) -> Option<Vec<u8>> {
    match slot(doc, path)? {
        Element::XmlElement(x) => Some(x.borrow().tag().to_vec()),
        _ => None,
    }
}

/// Insert a bytes item at a live index in the List at a path.
pub fn list_insert(doc: &mut Document, path: &[u8], index: usize, value: &[u8]) -> Vec<Op> {
    let value = value.to_vec();
    emit(doc, path, move |cur, key| {
        cur.list(key).insert(index, Scalar::Bytes(value))
    })
}

/// Tombstone the live item at an index in the List at a path.
pub fn list_delete(doc: &mut Document, path: &[u8], index: usize) -> Vec<Op> {
    // A delete that targets no live item must not create or re-stamp a list.
    if !slot_ok(
        doc,
        path,
        |e| matches!(e, Element::List(l) if index < l.borrow().len()),
    ) {
        return Vec::new();
    }
    emit(doc, path, move |cur, key| cur.list(key).delete(index))
}

/// Tombstone the list node `id` at a path, addressing it by stable id rather
/// than a shifting index. Inert if the list or node is absent.
pub fn list_delete_id(doc: &mut Document, path: &[u8], id: Stamp) -> Vec<Op> {
    if !slot_ok(
        doc,
        path,
        |e| matches!(e, Element::List(l) if l.borrow().contains(id)),
    ) {
        return Vec::new();
    }
    emit(doc, path, move |cur, key| cur.list(key).delete_id(id))
}

/// The live index of list node `id` at a path, if it is present and live.
pub fn list_live_index(doc: &Document, path: &[u8], id: Stamp) -> Option<usize> {
    slot(doc, path).and_then(|e| match e {
        Element::List(l) => l.borrow().live_index(id),
        _ => None,
    })
}

/// Insert text at a codepoint index in the Text at a path.
pub fn text_insert(doc: &mut Document, path: &[u8], index: usize, s: &str) -> Vec<Op> {
    let s = s.to_owned();
    emit(doc, path, move |cur, key| cur.text(key).insert(index, &s))
}

/// Tombstone `count` codepoints from an index in the Text at a path.
pub fn text_delete(doc: &mut Document, path: &[u8], index: usize, count: usize) -> Vec<Op> {
    // A delete that removes no codepoint must not create or re-stamp a text.
    if !slot_ok(
        doc,
        path,
        |e| matches!(e, Element::Text(t) if count > 0 && index < t.borrow().len()),
    ) {
        return Vec::new();
    }
    emit(doc, path, move |cur, key| {
        cur.text(key).delete(index, count)
    })
}

/// Tombstone the codepoints with these char_ids in the Text at a path,
/// addressing them by stable id rather than a shifting index. Inert if the text
/// or every id is absent.
pub fn text_delete_ids(doc: &mut Document, path: &[u8], ids: &[Stamp]) -> Vec<Op> {
    if !slot_ok(
        doc,
        path,
        |e| matches!(e, Element::Text(t) if ids.iter().any(|id| t.borrow().contains(*id))),
    ) {
        return Vec::new();
    }
    let ids = ids.to_vec();
    emit(doc, path, move |cur, key| cur.text(key).delete_ids(&ids))
}

/// The char_ids of up to `count` live codepoints from `index` in the Text at a
/// path — the run just inserted there, so an inverse can delete exactly it.
pub fn text_run_ids(doc: &Document, path: &[u8], index: usize, count: usize) -> Vec<Stamp> {
    slot(doc, path)
        .and_then(|e| match e {
            Element::Text(t) => Some(t.borrow().node_ids(index, count)),
            _ => None,
        })
        .unwrap_or_default()
}

/// The live codepoint index of char_id `id` in the Text at a path, if live.
pub fn text_live_index(doc: &Document, path: &[u8], id: Stamp) -> Option<usize> {
    slot(doc, path).and_then(|e| match e {
        Element::Text(t) => t.borrow().live_index(id),
        _ => None,
    })
}

/// Read an integer Register at a path.
pub fn get_int(doc: &Document, path: &[u8]) -> Option<i64> {
    slot(doc, path).and_then(|e| match e {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => None,
        },
        _ => None,
    })
}

/// Read a Register's scalar at a path, whatever its type.
pub fn get_register(doc: &Document, path: &[u8]) -> Option<Scalar> {
    slot(doc, path).and_then(|e| match e {
        Element::Register(r) => Some(r.borrow().read().clone()),
        _ => None,
    })
}

/// Read a Counter's value at a path.
pub fn get_counter(doc: &Document, path: &[u8]) -> Option<i64> {
    slot(doc, path).and_then(|e| match e {
        Element::Counter(c) => Some(c.borrow().read()),
        _ => None,
    })
}

/// Read a bytes scalar at a path.
pub fn get_bytes(doc: &Document, path: &[u8]) -> Option<Vec<u8>> {
    slot(doc, path).and_then(|e| match e {
        Element::Scalar(Scalar::Bytes(b)) => Some(b.clone()),
        _ => None,
    })
}

/// Read the live length of the List at a path.
pub fn list_len(doc: &Document, path: &[u8]) -> Option<usize> {
    slot(doc, path).and_then(|e| match e {
        Element::List(l) => Some(l.borrow().len()),
        _ => None,
    })
}

/// Read the bytes item at a live index in the List at a path.
pub fn list_get(doc: &Document, path: &[u8], index: usize) -> Option<Vec<u8>> {
    slot(doc, path).and_then(|e| match e {
        Element::List(l) => match l.borrow().get(index) {
            Some(Element::Scalar(Scalar::Bytes(b))) => Some(b),
            _ => None,
        },
        _ => None,
    })
}

/// Read the codepoint length of the Text at a path.
pub fn text_len(doc: &Document, path: &[u8]) -> Option<usize> {
    slot(doc, path).and_then(|e| match e {
        Element::Text(t) => Some(t.borrow().len()),
        _ => None,
    })
}

/// Read the Text at a path as a string.
pub fn text_get(doc: &Document, path: &[u8]) -> Option<String> {
    slot(doc, path).and_then(|e| match e {
        Element::Text(t) => Some(t.borrow().as_string()),
        _ => None,
    })
}

/// Capture a stable position in the List or Text at a path — an anchor that
/// tracks the same gap between items as they shift under concurrent edits.
/// A path that is not a sequence yields nothing.
pub fn relative_position(
    doc: &Document,
    path: &[u8],
    index: usize,
    side: Side,
) -> Option<RelativePosition> {
    slot(doc, path).and_then(|e| match e {
        Element::List(l) => Some(l.borrow().relative_position(index, side)),
        Element::Text(t) => Some(t.borrow().relative_position(index, side)),
        _ => None,
    })
}

/// Resolve a captured position back to a live index in the List or Text at a
/// path. A path that is not a sequence yields nothing.
pub fn resolve_position(doc: &Document, path: &[u8], pos: &RelativePosition) -> Option<usize> {
    slot(doc, path).and_then(|e| match e {
        Element::List(l) => Some(l.borrow().resolve_position(pos)),
        Element::Text(t) => Some(t.borrow().resolve_position(pos)),
        _ => None,
    })
}

/// Run a path-addressed edit, apply it locally, and return its emitted ops.
/// A malformed or leaf-less path emits nothing.
fn emit<F>(doc: &mut Document, path: &[u8], leaf: F) -> Vec<Op>
where
    F: FnOnce(&mut MapCursor, &[u8]),
{
    let Some(keys) = parse_path(path) else {
        return Vec::new();
    };
    let Some((leaf_key, parents)) = keys.split_last() else {
        return Vec::new();
    };
    doc.transact(|tx| {
        descend(tx, parents, |cur| leaf(cur, leaf_key));
    })
}

/// Descend `parents` from `cur`, creating maps as needed and descending into an
/// element's attrs, then run `f` on the map that holds the leaf. A parent that
/// is a dead end (an `XmlFragment`, which has no attrs) stops the walk before
/// `f` runs, so the write leaves nothing at an unreachable path. Iterative —
/// path depth is caller-supplied, so a recursive walk could overflow the stack.
fn descend<F>(cur: &mut MapCursor, parents: &[Vec<u8>], f: F)
where
    F: FnOnce(&mut MapCursor),
{
    let Some((first, rest)) = parents.split_first() else {
        f(cur);
        return;
    };
    let Some(mut child) = cur.child(first) else {
        return;
    };
    for key in rest {
        let Some(next) = child.into_child(key) else {
            return;
        };
        child = next;
    }
    f(&mut child);
}

/// The live element at a path, if the whole path resolves.
fn slot(doc: &Document, path: &[u8]) -> Option<Element> {
    let keys = parse_path(path)?;
    let (leaf_key, parents) = keys.split_last()?;
    let map = resolve_map(doc, parents)?;
    let value = map.borrow().get(leaf_key);
    value
}

/// Whether the element at a path satisfies `ok` — keeps a no-op delete from
/// materialising a container.
fn slot_ok<F>(doc: &Document, path: &[u8], ok: F) -> bool
where
    F: FnOnce(&Element) -> bool,
{
    slot(doc, path).as_ref().map(ok).unwrap_or(false)
}

/// Walk `parents` from the root, following nested maps and descending into an
/// `XmlElement`'s attrs Map when a parent key holds one — so an attr is
/// addressed by naming its element then the attr key. A fragment carries no
/// attrs, so a key past it is unresolved.
fn resolve_map(doc: &Document, parents: &[Vec<u8>]) -> Option<Rc<RefCell<Map>>> {
    let mut cur = doc.root();
    for key in parents {
        let next = match cur.borrow().get(key) {
            Some(Element::Map(m)) => m,
            Some(Element::XmlElement(x)) => x.borrow().attrs(),
            _ => return None,
        };
        cur = next;
    }
    Some(cur)
}
