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
use crate::codec::DecodeError;
use crate::diff::{self, Change};
use crate::doc::{Document, MapCursor, XmlChildrenCursor};
use crate::elementid::ElementId;
use crate::list::{List, Side};
use crate::map::Map;
use crate::marks::ResolvedMark;
use crate::op::Op;
use crate::ranged::RangeAnchor;
use crate::schema::Schema;
use crate::stamp::Stamp;
use crate::validate::Step;
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

/// Insert a nested `XmlElement` child with `tag` at live `index` in the children
/// of the element/fragment at `elem_path`. Inert if `elem_path` is not a live
/// XmlElement or XmlFragment. The new child is index-addressed and its index
/// shifts under sibling edits, so it has no stable path key of its own — this
/// slice addresses the children sequence, not a child's contents.
pub fn xml_insert_element(
    doc: &mut Document,
    elem_path: &[u8],
    index: usize,
    tag: &[u8],
) -> Vec<Op> {
    if !slot_ok(doc, elem_path, is_xml_node) {
        return Vec::new();
    }
    let tag = tag.to_vec();
    xml_children_emit(doc, elem_path, move |kids| {
        kids.insert_element(index, &tag);
    })
}

/// Insert a `Text`-run child initialised with `s` at live `index` in the children
/// of the element/fragment at `elem_path`. Inert if the target is not a live
/// XmlElement or XmlFragment. The text child is born empty then filled in the same
/// transaction.
pub fn xml_insert_text(doc: &mut Document, elem_path: &[u8], index: usize, s: &str) -> Vec<Op> {
    if !slot_ok(doc, elem_path, is_xml_node) {
        return Vec::new();
    }
    let s = s.to_owned();
    xml_children_emit(doc, elem_path, move |kids| {
        let mut text = kids.insert_text(index);
        if !s.is_empty() {
            text.insert(0, &s);
        }
    })
}

/// Tombstone the child at live `index` in the children of the element/fragment at
/// `elem_path`. Inert if the target is not a live XmlElement or XmlFragment, or
/// `index` names no live child — a no-op delete must not create or re-stamp a
/// container.
pub fn xml_child_delete(doc: &mut Document, elem_path: &[u8], index: usize) -> Vec<Op> {
    if !slot_ok(doc, elem_path, |e| {
        xml_children_of(e).is_some_and(|l| index < l.borrow().len())
    }) {
        return Vec::new();
    }
    xml_children_emit(doc, elem_path, move |kids| kids.delete(index))
}

/// The count of live children of the element/fragment at `elem_path`, or `None`
/// if the path is not a live XmlElement or XmlFragment.
pub fn xml_children_len(doc: &Document, elem_path: &[u8]) -> Option<usize> {
    xml_children_of(&slot(doc, elem_path)?).map(|l| l.borrow().len())
}

/// Relocate the live child at `child_index` under the XML node at `parent_path`
/// to `dest_index` in the children of the XML node at `new_parent_path` — a
/// Kleppmann tree move that keeps the child's identity and subtree, converging to
/// one parent under concurrent moves. Inert if either path is not a live
/// XmlElement/XmlFragment or `child_index` names no live child.
///
/// A child is index-addressed, so the mover is named by its parent path and its
/// live index. A map-slot root — an element or fragment installed straight into a
/// map — is never a child and has no placement, so it is unaddressable as a mover
/// and thus never moved; a fragment is a valid destination (it owns children).
/// The destination is always a map-slot-addressable node, never inside the
/// mover's subtree, so this surface cannot express a cycle.
pub fn xml_move_child(
    doc: &mut Document,
    parent_path: &[u8],
    child_index: usize,
    new_parent_path: &[u8],
    dest_index: usize,
) -> Vec<Op> {
    let Some(node) = xml_child_id(doc, parent_path, child_index) else {
        return Vec::new();
    };
    let Some(new_parent) = xml_node_id(doc, new_parent_path) else {
        return Vec::new();
    };
    doc.transact(|tx| tx.move_xml(node, new_parent, dest_index))
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

/// Author a mark named `name` over the span `[start_index, end_index)` of the
/// sequence (Text or List) at `seq_path`, capturing each endpoint as a
/// RelativePosition (with the given gravity `Side`) so the span tracks its
/// characters under concurrent edits. Returns the emitted ops and the mark's id
/// as bytes — the handle a later [`mark_set_value`]/[`mark_delete`] names it by.
/// Inert (empty ops, `None` handle) if `seq_path` is not a live sequence.
///
/// Both endpoints anchor into the *same* sequence: a single-sequence span is the
/// in-scope case. A cross-element span (endpoints on different sequences), which
/// the underlying RangeAnchor supports, is a deferred surface (Unit 4b-ii).
#[allow(clippy::too_many_arguments)]
pub fn mark(
    doc: &mut Document,
    seq_path: &[u8],
    start_index: usize,
    start_side: Side,
    end_index: usize,
    end_side: Side,
    name: &[u8],
    value: Scalar,
) -> (Vec<Op>, Option<Vec<u8>>) {
    let Some(start) = range_anchor(doc, seq_path, start_index, start_side) else {
        return (Vec::new(), None);
    };
    let Some(end) = range_anchor(doc, seq_path, end_index, end_side) else {
        return (Vec::new(), None);
    };
    let name = name.to_vec();
    let mut id = None;
    let ops = doc.transact(|tx| {
        id = Some(tx.ranged().mark(&name, start, end, value));
    });
    (ops, id.map(|id| id.as_bytes().to_vec()))
}

/// Change the scalar payload of the mark handle `mark_id`. Inert (empty ops) if
/// the handle does not decode to a live scalar mark.
pub fn mark_set_value(doc: &mut Document, mark_id: &[u8], value: Scalar) -> Vec<Op> {
    let Some(id) = element_id(mark_id) else {
        return Vec::new();
    };
    doc.transact(|tx| tx.ranged().set_payload(id, value))
}

/// Tombstone the mark handle `mark_id`. Inert (empty ops) if the handle does not
/// decode to a live mark.
pub fn mark_delete(doc: &mut Document, mark_id: &[u8]) -> Vec<Op> {
    let Some(id) = element_id(mark_id) else {
        return Vec::new();
    };
    doc.transact(|tx| tx.ranged().delete(id))
}

/// The active marks covering character `index` of the sequence at `seq_path` —
/// the [`Document::marks_at`](crate::doc::Document::marks_at) read model, each a
/// [`ResolvedMark`] (name + resolved state). Empty if `seq_path` is not a live
/// sequence.
pub fn marks_at(doc: &Document, seq_path: &[u8], index: usize) -> Vec<ResolvedMark> {
    match seq_id(doc, seq_path) {
        Some(seq) => doc.marks_at(seq, index),
        None => Vec::new(),
    }
}

/// The structural diff of two snapshots as an ordered [`Change`] list — the
/// façade's read model of what turned `old` into `new`. A binding that renders
/// changes structurally, building its own per-change value, reads this; one that
/// forwards an opaque buffer across the SDK boundary uses [`diff_encoded`].
pub fn diff(old: &Document, new: &Document) -> Vec<Change> {
    diff::diff(old, new)
}

/// The structural diff of two snapshots serialized to one buffer, so a diff
/// computed in the core crosses the language-SDK boundary as opaque bytes the
/// binding forwards and later reads with [`decode_changes`] — mirroring how a
/// captured sequence position crosses as bytes. Runs the diff engine then encodes
/// in one call.
pub fn diff_encoded(old: &Document, new: &Document) -> Vec<u8> {
    diff::encode_changes(&diff::diff(old, new))
}

/// Decode a diff buffer from [`diff_encoded`] back into its [`Change`]s. Total: a
/// truncated or malformed buffer yields a `DecodeError`, never a panic — a diff
/// crosses an untrusted boundary, so the decode never trusts its length headers.
pub fn decode_changes(bytes: &[u8]) -> Result<Vec<Change>, DecodeError> {
    diff::decode_changes(bytes)
}

/// Parse schema JSON bytes and bind the schema to the document for `onRepaired`
/// observation, returning whether it bound. A binding crosses schema JSON as
/// bytes, so the façade parses them here; parsing is total — non-UTF-8 or JSON
/// that is not a valid schema fails cleanly (returns `false`, binding nothing)
/// rather than panicking, mirroring the other total-decode boundaries. On success
/// the current state is taken as the baseline, so a later [`take_repairs`] surfaces
/// only a repair the state comes to need after binding.
pub fn set_schema(doc: &mut Document, schema_bytes: &[u8]) -> bool {
    let Ok(src) = std::str::from_utf8(schema_bytes) else {
        return false;
    };
    let Ok(schema) = Schema::parse(src) else {
        return false;
    };
    doc.set_schema(schema);
    true
}

/// The located paths whose repaired reading has newly changed against the bound
/// schema since the last call — the `onRepaired` signal, each encoded with
/// [`encode_repair_path`] so a binding forwards it as bytes. Empty when no schema
/// is bound or nothing newly needs repair (and while an atomic transaction is
/// open); a drain reseeds the baseline, so a standing repair reports once.
///
/// A reported path names a *location*, not a value: the repaired value is produced
/// by a read ([`repairs`](crate::repair::repairs)), while a normal `get_*` at the
/// location returns the raw stored value. A consumer reacts to the signal by
/// reading the fresh repaired reading, never caching a stale one.
pub fn take_repairs(doc: &mut Document) -> Vec<Vec<u8>> {
    doc.take_repairs()
        .iter()
        .map(|steps| encode_repair_path(steps))
        .collect()
}

/// The step-path tag for a map-slot key.
const STEP_KEY: u8 = 0x00;
/// The step-path tag for a sequence index.
const STEP_INDEX: u8 = 0x01;

/// Encode a located repair path — a sequence of steps, each a map-slot key or a
/// sequence index — as framed bytes a binding carries. Each step is a one-byte
/// tag then its payload: a key is `u32` little-endian length then its bytes, an
/// index is a `u64` little-endian. Distinct from [`encode_path`], which frames
/// keys only: a repair location can descend a sequence index (a bounded list item,
/// an xml child), so its path carries indices a bare key path cannot express.
pub fn encode_repair_path(steps: &[Step]) -> Vec<u8> {
    let mut out = Vec::new();
    for step in steps {
        match step {
            Step::Key(k) => {
                out.push(STEP_KEY);
                let len = u32::try_from(k.len()).expect("path: key length exceeds u32");
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(k);
            }
            Step::Index(i) => {
                out.push(STEP_INDEX);
                out.extend_from_slice(&(*i as u64).to_le_bytes());
            }
        }
    }
    out
}

/// Decode a repair path from [`encode_repair_path`] back into its [`Step`]s, or
/// `None` if a tag is unknown or a length / field runs past the end — total over
/// any bytes, since a repair path crosses an untrusted boundary and the decode
/// never trusts its framing.
pub fn parse_repair_path(bytes: &[u8]) -> Option<Vec<Step>> {
    let mut steps = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let tag = bytes[i];
        i += 1;
        match tag {
            STEP_KEY => {
                let hdr = bytes.get(i..i.checked_add(4)?)?;
                let klen = u32::from_le_bytes(hdr.try_into().unwrap()) as usize;
                i += 4;
                let key = bytes.get(i..i.checked_add(klen)?)?;
                steps.push(Step::Key(key.to_vec()));
                i += klen;
            }
            STEP_INDEX => {
                let hdr = bytes.get(i..i.checked_add(8)?)?;
                let n = u64::from_le_bytes(hdr.try_into().unwrap());
                steps.push(Step::Index(usize::try_from(n).ok()?));
                i += 8;
            }
            _ => return None,
        }
    }
    Some(steps)
}

/// A range endpoint into the sequence at `path` at `index` with gravity `side`,
/// or `None` if the path is not a live Text or List. Resolves the path once,
/// pairing the sequence's stable id with a RelativePosition at the index.
fn range_anchor(doc: &Document, path: &[u8], index: usize, side: Side) -> Option<RangeAnchor> {
    match slot(doc, path)? {
        Element::List(l) => {
            let l = l.borrow();
            Some(RangeAnchor {
                seq: l.id(),
                pos: l.relative_position(index, side),
            })
        }
        Element::Text(t) => {
            let t = t.borrow();
            Some(RangeAnchor {
                seq: t.id(),
                pos: t.relative_position(index, side),
            })
        }
        _ => None,
    }
}

/// The stable id of the sequence (Text or List) at a path — the sequence a mark
/// annotates. `None` if the path is not a live sequence.
fn seq_id(doc: &Document, path: &[u8]) -> Option<ElementId> {
    match slot(doc, path)? {
        Element::List(l) => Some(l.borrow().id()),
        Element::Text(t) => Some(t.borrow().id()),
        _ => None,
    }
}

/// Decode a mark handle back to an ElementId, or `None` if it is not the width of
/// an id — a malformed handle names no mark.
fn element_id(bytes: &[u8]) -> Option<ElementId> {
    let arr: [u8; 16] = bytes.try_into().ok()?;
    Some(ElementId::from_bytes(arr))
}

/// Whether an element is an XML node that owns a children sequence.
fn is_xml_node(e: &Element) -> bool {
    matches!(e, Element::XmlElement(_) | Element::XmlFragment(_))
}

/// The children List of an XML node, or `None` if the element is neither an
/// XmlElement nor an XmlFragment.
fn xml_children_of(e: &Element) -> Option<Rc<RefCell<List>>> {
    match e {
        Element::XmlElement(x) => Some(x.borrow().children()),
        Element::XmlFragment(f) => Some(f.borrow().children()),
        _ => None,
    }
}

/// The stable id of the live child at `index` under the XML node at `path`, or
/// `None` if the path is not a live XML node or `index` names no live child. A
/// child (element or text run) holds a placement, so it is the movable target of
/// a tree move.
fn xml_child_id(doc: &Document, path: &[u8], index: usize) -> Option<ElementId> {
    let kids = xml_children_of(&slot(doc, path)?)?;
    let child = kids.borrow().get(index)?;
    xml_child_element_id(&child)
}

/// The stable id of the live XmlElement/XmlFragment at `path` — a move's
/// destination parent. A fragment is a valid parent: it owns a children sequence.
fn xml_node_id(doc: &Document, path: &[u8]) -> Option<ElementId> {
    match slot(doc, path)? {
        Element::XmlElement(x) => Some(x.borrow().id()),
        Element::XmlFragment(f) => Some(f.borrow().id()),
        _ => None,
    }
}

/// The stable id of an XmlElement or Text child, or `None` for any other value.
fn xml_child_element_id(e: &Element) -> Option<ElementId> {
    match e {
        Element::XmlElement(x) => Some(x.borrow().id()),
        Element::Text(t) => Some(t.borrow().id()),
        _ => None,
    }
}

/// Descend to the map holding the element/fragment named by `path`, reach its
/// children cursor, and run `f`. The caller has already confirmed the leaf is a
/// live XML node, so the descent creates no phantom map and the children cursor
/// resolves.
fn xml_children_emit<F>(doc: &mut Document, path: &[u8], f: F) -> Vec<Op>
where
    F: FnOnce(&mut XmlChildrenCursor),
{
    let Some(keys) = parse_path(path) else {
        return Vec::new();
    };
    let Some((leaf_key, parents)) = keys.split_last() else {
        return Vec::new();
    };
    doc.transact(|tx| {
        descend(tx, parents, |cur| {
            if let Some(mut kids) = cur.xml_children(leaf_key) {
                f(&mut kids);
            }
        });
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
    // A dead-end path (a parent is a fragment, which has no attrs) emits nothing
    // — the emptiness of the returned ops is the "did this write land" signal a
    // caller broadcasts off. The check is read-only and precedes any emit, so
    // create-through never materialises a map above an unreachable leaf. It must
    // stay a pre-check: descending-then-refusing would emit the ancestor maps
    // before discovering a deeper dead end.
    if !writable(doc, parents) {
        return Vec::new();
    }
    doc.transact(|tx| descend(tx, parents, |cur| leaf(cur, leaf_key)))
}

/// Descend `parents` from `cur`, creating maps as needed and descending into an
/// element's attrs, then run `f` on the map that holds the leaf. `writable` has
/// already ruled out a dead end, so every step resolves. Iterative — path depth
/// is caller-supplied, so a recursive walk could overflow the stack.
fn descend<F>(cur: &mut MapCursor, parents: &[Vec<u8>], f: F)
where
    F: FnOnce(&mut MapCursor),
{
    let Some((first, rest)) = parents.split_first() else {
        f(cur);
        return;
    };
    let mut child = cur.child(first);
    for key in rest {
        child = child.into_child(key);
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

/// Whether the write path `parents` can be descended: a key holding an
/// `XmlFragment` is a dead end (a fragment has no attrs), so a write past it
/// emits nothing rather than materialising a phantom Map above the unreachable
/// leaf. A Map or `XmlElement` is descendable; an absent slot is create-through
/// (nothing live can lurk below it, so no deeper dead end is hidden).
fn writable(doc: &Document, parents: &[Vec<u8>]) -> bool {
    let mut cur = doc.root();
    for key in parents {
        let next = match cur.borrow().get(key) {
            Some(Element::Map(m)) => m,
            Some(Element::XmlElement(x)) => x.borrow().attrs(),
            Some(Element::XmlFragment(_)) => return false,
            _ => return true,
        };
        cur = next;
    }
    true
}
