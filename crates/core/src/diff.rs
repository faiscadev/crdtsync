//! Structural diff between two replica snapshots.
//!
//! A document is a structured Element tree, so the change from one snapshot to
//! another is a list of structural changes rather than an opaque byte delta.
//! [`diff`] walks the two trees in lockstep and reports each slot that was
//! added, removed, or changed, addressed by its path. Scalar / register /
//! counter values report their old and new value; a nested map is walked so a
//! deep edit surfaces at its own path. Sequences diff to runs by stable id — a
//! List to item inserts/deletes, a Text to codepoint inserts/deletes. The
//! change list is ordered by path, so diffing the same pair is deterministic.
//! An XmlElement diffs as its children (a sequence, structural inserts/deletes at
//! the element's own path) then its attrs (a keyed Map, value diffs at the deeper
//! attr-key paths) — that order keeps the change list path-sorted; a fragment as
//! its children alone. A tag is part of an element's identity, so a changed tag at
//! a slot reads as a replace.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::rc::Rc;

use crate::codec::{put_bytes, put_scalar, put_u32, put_u64, put_u8, Cursor, DecodeError};
use crate::doc::Document;
use crate::element::{Element, ElementKind};
use crate::list::List;
use crate::map::Map;
use crate::path::{encode_path, parse_path};
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use crate::text::Text;
use crate::xml::XmlElement;

/// One structural change between two snapshots, addressed by its path.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Change {
    /// A slot present in the new snapshot, absent in the old.
    Added { path: Vec<u8>, kind: ElementKind },
    /// A slot present in the old snapshot, absent in the new.
    Removed { path: Vec<u8>, kind: ElementKind },
    /// A scalar leaf — an inline Scalar or a Register — whose value changed.
    Value {
        path: Vec<u8>,
        old: Scalar,
        new: Scalar,
    },
    /// A Counter whose value changed.
    Counter { path: Vec<u8>, old: i64, new: i64 },
    /// A run of items inserted into a List, at its index in the new list.
    ListInsert {
        path: Vec<u8>,
        index: usize,
        items: Vec<SeqItem>,
    },
    /// A run of items deleted from a List, at its index in the old list.
    ListDelete {
        path: Vec<u8>,
        index: usize,
        items: Vec<SeqItem>,
    },
    /// A run of codepoints inserted into a Text, at its index in the new text.
    TextInsert {
        path: Vec<u8>,
        index: usize,
        text: String,
    },
    /// A run of codepoints deleted from a Text, at its index in the old text.
    TextDelete {
        path: Vec<u8>,
        index: usize,
        text: String,
    },
}

/// A List item in a diff: an inline scalar value, or the kind of a composite
/// (a composite's own contents diff at its own path once it is reachable).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SeqItem {
    Scalar(Scalar),
    Composite(ElementKind),
}

/// The structural changes turning `old` into `new`, ordered by path.
pub fn diff(old: &Document, new: &Document) -> Vec<Change> {
    let mut out = Vec::new();
    let mut prefix = Vec::new();
    diff_map(
        &old.root().borrow(),
        &new.root().borrow(),
        &mut prefix,
        &mut out,
    );
    out
}

/// Walk two maps in lockstep over the union of their live keys, sorted.
fn diff_map(old: &Map, new: &Map, prefix: &mut Vec<Vec<u8>>, out: &mut Vec<Change>) {
    let keys: BTreeSet<Vec<u8>> = old.keys().into_iter().chain(new.keys()).collect();
    for key in keys {
        prefix.push(key.clone());
        match (old.get(&key), new.get(&key)) {
            (None, Some(e)) => out.push(Change::Added {
                path: path_of(prefix),
                kind: e.kind(),
            }),
            (Some(e), None) => out.push(Change::Removed {
                path: path_of(prefix),
                kind: e.kind(),
            }),
            (Some(a), Some(b)) => diff_elem(&a, &b, prefix, out),
            (None, None) => {}
        }
        prefix.pop();
    }
}

/// Diff two elements at the same live slot.
fn diff_elem(a: &Element, b: &Element, prefix: &mut Vec<Vec<u8>>, out: &mut Vec<Change>) {
    if a.kind() != b.kind() {
        // The slot holds a different kind of thing: a structural replace.
        out.push(Change::Removed {
            path: path_of(prefix),
            kind: a.kind(),
        });
        out.push(Change::Added {
            path: path_of(prefix),
            kind: b.kind(),
        });
        return;
    }
    match (a, b) {
        (Element::Map(a), Element::Map(b)) => {
            diff_map(&a.borrow(), &b.borrow(), prefix, out);
        }
        (Element::Counter(a), Element::Counter(b)) => {
            let (old, new) = (a.borrow().read(), b.borrow().read());
            if old != new {
                out.push(Change::Counter {
                    path: path_of(prefix),
                    old,
                    new,
                });
            }
        }
        (Element::List(a), Element::List(b)) => {
            diff_list(&a.borrow(), &b.borrow(), prefix, out);
        }
        (Element::Text(a), Element::Text(b)) => {
            diff_text(&a.borrow(), &b.borrow(), prefix, out);
        }
        (Element::XmlElement(a), Element::XmlElement(b)) => {
            diff_xml_element(a, b, prefix, out);
        }
        (Element::XmlFragment(a), Element::XmlFragment(b)) => {
            let (a_children, b_children) = (a.borrow().children(), b.borrow().children());
            diff_list(&a_children.borrow(), &b_children.borrow(), prefix, out);
        }
        _ => {
            // Both are the same scalar-valued kind (inline Scalar or Register).
            let (old, new) = (scalar_of(a), scalar_of(b));
            if old != new {
                out.push(Change::Value {
                    path: path_of(prefix),
                    old,
                    new,
                });
            }
        }
    }
}

/// Diff two XmlElement snapshots at the same slot: children (an ordered sequence
/// → structural inserts/deletes at the element's own path) then attrs (a keyed
/// Map → value diffs at the deeper attr-key paths), so the change list stays
/// ordered by path. A tag is part of an element's identity, so a different tag at
/// the same slot is a different element — a structural replace, not a field diff.
fn diff_xml_element(
    a: &Rc<RefCell<XmlElement>>,
    b: &Rc<RefCell<XmlElement>>,
    prefix: &mut Vec<Vec<u8>>,
    out: &mut Vec<Change>,
) {
    if a.borrow().tag() != b.borrow().tag() {
        out.push(Change::Removed {
            path: path_of(prefix),
            kind: ElementKind::XmlElement,
        });
        out.push(Change::Added {
            path: path_of(prefix),
            kind: ElementKind::XmlElement,
        });
        return;
    }
    let (a_attrs, a_children) = {
        let x = a.borrow();
        (x.attrs(), x.children())
    };
    let (b_attrs, b_children) = {
        let x = b.borrow();
        (x.attrs(), x.children())
    };
    diff_list(&a_children.borrow(), &b_children.borrow(), prefix, out);
    diff_map(&a_attrs.borrow(), &b_attrs.borrow(), prefix, out);
}

/// The scalar a leaf slot reads — inline or through a Register.
fn scalar_of(e: &Element) -> Scalar {
    match e {
        Element::Scalar(s) => s.clone(),
        Element::Register(r) => r.borrow().read().clone(),
        _ => unreachable!("scalar_of is only called on a scalar-valued element"),
    }
}

/// Element-level diff of two List snapshots by node id. A node identified by
/// its stable id (a `Stamp`) that is live in one snapshot and not the other is
/// an exact insert or delete; consecutive same-op items coalesce into a run,
/// deletes (at their index in the old list) before inserts (at their index in
/// the new list). A node still live in both is unchanged in position; a change
/// to a composite item's own contents surfaces at that item's path.
fn diff_list(old: &List, new: &List, prefix: &[Vec<u8>], out: &mut Vec<Change>) {
    let old_seq = list_seq(old);
    let new_seq = list_seq(new);
    let old_ids: HashSet<Stamp> = old_seq.iter().map(|(id, _)| *id).collect();
    let new_ids: HashSet<Stamp> = new_seq.iter().map(|(id, _)| *id).collect();

    for (index, items) in list_runs(&old_seq, &new_ids) {
        out.push(Change::ListDelete {
            path: path_of(prefix),
            index,
            items,
        });
    }
    for (index, items) in list_runs(&new_seq, &old_ids) {
        out.push(Change::ListInsert {
            path: path_of(prefix),
            index,
            items,
        });
    }
}

/// A List's live items in order, each with its stable node id.
fn list_seq(l: &List) -> Vec<(Stamp, SeqItem)> {
    (0..l.len())
        .filter_map(|i| Some((l.node_at(i)?, seq_item(&l.get(i)?))))
        .collect()
}

/// A List item as it appears in a diff: an inline scalar (or a register's
/// scalar), else the composite's kind.
fn seq_item(e: &Element) -> SeqItem {
    match e {
        Element::Scalar(s) => SeqItem::Scalar(s.clone()),
        Element::Register(r) => SeqItem::Scalar(r.borrow().read().clone()),
        other => SeqItem::Composite(other.kind()),
    }
}

/// Coalesce the items of `seq` whose id is absent from `present` into runs, each
/// tagged with the run's start index within `seq`.
fn list_runs(seq: &[(Stamp, SeqItem)], present: &HashSet<Stamp>) -> Vec<(usize, Vec<SeqItem>)> {
    let mut out: Vec<(usize, Vec<SeqItem>)> = Vec::new();
    let mut run: Option<(usize, Vec<SeqItem>)> = None;
    for (index, (id, item)) in seq.iter().enumerate() {
        if present.contains(id) {
            if let Some(r) = run.take() {
                out.push(r);
            }
        } else {
            run.get_or_insert((index, Vec::new())).1.push(item.clone());
        }
    }
    if let Some(r) = run.take() {
        out.push(r);
    }
    out
}

/// Char-level diff of two Text snapshots by char id. A codepoint is identified
/// by its stable char id, so a codepoint live in one snapshot and not the other
/// is an exact insert or delete — no heuristic alignment. Consecutive same-op
/// codepoints coalesce into one run: deletes at their index in the old text
/// (ascending) first, then inserts at their index in the new text.
fn diff_text(old: &Text, new: &Text, prefix: &[Vec<u8>], out: &mut Vec<Change>) {
    let old_seq = char_seq(old);
    let new_seq = char_seq(new);
    let old_ids: HashSet<Stamp> = old_seq.iter().map(|(id, _)| *id).collect();
    let new_ids: HashSet<Stamp> = new_seq.iter().map(|(id, _)| *id).collect();

    for (index, run) in runs(&old_seq, &new_ids) {
        out.push(Change::TextDelete {
            path: path_of(prefix),
            index,
            text: run,
        });
    }
    for (index, run) in runs(&new_seq, &old_ids) {
        out.push(Change::TextInsert {
            path: path_of(prefix),
            index,
            text: run,
        });
    }
}

/// A Text's live codepoints in order, each with its stable char id.
fn char_seq(t: &Text) -> Vec<(Stamp, char)> {
    t.node_ids(0, t.len())
        .into_iter()
        .zip(t.as_string().chars())
        .collect()
}

/// Coalesce the codepoints of `seq` whose id is absent from `present` into runs,
/// each tagged with the run's start index within `seq`.
fn runs(seq: &[(Stamp, char)], present: &HashSet<Stamp>) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = Vec::new();
    let mut run: Option<(usize, String)> = None;
    for (index, (id, ch)) in seq.iter().enumerate() {
        if present.contains(id) {
            if let Some(r) = run.take() {
                out.push(r);
            }
        } else {
            run.get_or_insert((index, String::new())).1.push(*ch);
        }
    }
    if let Some(r) = run.take() {
        out.push(r);
    }
    out
}

fn path_of(prefix: &[Vec<u8>]) -> Vec<u8> {
    let keys: Vec<&[u8]> = prefix.iter().map(Vec::as_slice).collect();
    encode_path(&keys)
}

/// The engine's default text rendering of a change list — one human-readable
/// line per change, for a debug dump, an audit view, or a CLI. An app that wants
/// its own presentation reads the structured [`Change`]s directly; this is the
/// sensible default. `+` adds, `-` removes, `~` changes a value; paths print
/// slash-joined and sequence runs print their index and contents.
pub fn render(changes: &[Change]) -> Vec<String> {
    changes.iter().map(render_change).collect()
}

fn render_change(change: &Change) -> String {
    match change {
        Change::Added { path, kind } => format!("+ {} ({})", show_path(path), kind_name(*kind)),
        Change::Removed { path, kind } => format!("- {} ({})", show_path(path), kind_name(*kind)),
        Change::Value { path, old, new } => {
            format!(
                "~ {}: {} -> {}",
                show_path(path),
                show_scalar(old),
                show_scalar(new)
            )
        }
        Change::Counter { path, old, new } => format!("~ {}: {old} -> {new}", show_path(path)),
        Change::ListInsert { path, index, items } => {
            format!("+ {}[{index}]: {}", show_path(path), show_items(items))
        }
        Change::ListDelete { path, index, items } => {
            format!("- {}[{index}]: {}", show_path(path), show_items(items))
        }
        Change::TextInsert { path, index, text } => {
            format!("+ {}[{index}]: {text:?}", show_path(path))
        }
        Change::TextDelete { path, index, text } => {
            format!("- {}[{index}]: {text:?}", show_path(path))
        }
    }
}

/// A path as `/key/key`, each key shown as UTF-8 (lossy for non-text keys).
fn show_path(path: &[u8]) -> String {
    match parse_path(path) {
        Some(keys) => {
            let mut out = String::new();
            for key in keys {
                out.push('/');
                out.push_str(&String::from_utf8_lossy(&key));
            }
            out
        }
        None => String::from("<bad path>"),
    }
}

fn show_scalar(s: &Scalar) -> String {
    match s {
        Scalar::Null => String::from("null"),
        Scalar::Bool(b) => b.to_string(),
        Scalar::Int(n) => n.to_string(),
        Scalar::Bytes(b) => format!("<{} bytes>", b.len()),
        Scalar::BlobRef(_) => String::from("<blobref>"),
        Scalar::ElementRef(_) => String::from("<elementref>"),
    }
}

fn show_items(items: &[SeqItem]) -> String {
    items
        .iter()
        .map(|item| match item {
            SeqItem::Scalar(s) => show_scalar(s),
            SeqItem::Composite(kind) => format!("<{}>", kind_name(*kind)),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn kind_name(kind: ElementKind) -> &'static str {
    match kind {
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

/// Serialize a change list to bytes, so a diff computed in the core crosses the
/// language SDK boundary as one buffer the binding decodes. The encoding is a
/// `u32` count then each change, tag-led; it is not a durable format — a diff is
/// a transient computed result, not stored.
pub fn encode_changes(changes: &[Change]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, changes.len() as u32);
    for change in changes {
        put_change(&mut out, change);
    }
    out
}

/// Decode a change list encoded by [`encode_changes`], rejecting trailing bytes.
pub fn decode_changes(bytes: &[u8]) -> Result<Vec<Change>, DecodeError> {
    let mut cur = Cursor::new(bytes);
    let count = cur.u32()?;
    let mut changes = Vec::new();
    for _ in 0..count {
        changes.push(read_change(&mut cur)?);
    }
    if cur.at_end() {
        Ok(changes)
    } else {
        Err(DecodeError::TrailingBytes)
    }
}

fn put_change(out: &mut Vec<u8>, change: &Change) {
    match change {
        Change::Added { path, kind } => {
            put_u8(out, 0);
            put_bytes(out, path);
            put_u8(out, *kind as u8);
        }
        Change::Removed { path, kind } => {
            put_u8(out, 1);
            put_bytes(out, path);
            put_u8(out, *kind as u8);
        }
        Change::Value { path, old, new } => {
            put_u8(out, 2);
            put_bytes(out, path);
            put_scalar(out, old);
            put_scalar(out, new);
        }
        Change::Counter { path, old, new } => {
            put_u8(out, 3);
            put_bytes(out, path);
            put_u64(out, *old as u64);
            put_u64(out, *new as u64);
        }
        Change::ListInsert { path, index, items } => {
            put_u8(out, 4);
            put_bytes(out, path);
            put_u64(out, *index as u64);
            put_items(out, items);
        }
        Change::ListDelete { path, index, items } => {
            put_u8(out, 5);
            put_bytes(out, path);
            put_u64(out, *index as u64);
            put_items(out, items);
        }
        Change::TextInsert { path, index, text } => {
            put_u8(out, 6);
            put_bytes(out, path);
            put_u64(out, *index as u64);
            put_bytes(out, text.as_bytes());
        }
        Change::TextDelete { path, index, text } => {
            put_u8(out, 7);
            put_bytes(out, path);
            put_u64(out, *index as u64);
            put_bytes(out, text.as_bytes());
        }
    }
}

fn put_items(out: &mut Vec<u8>, items: &[SeqItem]) {
    put_u32(out, items.len() as u32);
    for item in items {
        match item {
            SeqItem::Scalar(s) => {
                put_u8(out, 0);
                put_scalar(out, s);
            }
            SeqItem::Composite(kind) => {
                put_u8(out, 1);
                put_u8(out, *kind as u8);
            }
        }
    }
}

fn read_change(cur: &mut Cursor) -> Result<Change, DecodeError> {
    Ok(match cur.u8()? {
        0 => Change::Added {
            path: cur.bytes()?,
            kind: read_kind(cur)?,
        },
        1 => Change::Removed {
            path: cur.bytes()?,
            kind: read_kind(cur)?,
        },
        2 => Change::Value {
            path: cur.bytes()?,
            old: cur.scalar()?,
            new: cur.scalar()?,
        },
        3 => Change::Counter {
            path: cur.bytes()?,
            old: cur.u64()? as i64,
            new: cur.u64()? as i64,
        },
        4 => Change::ListInsert {
            path: cur.bytes()?,
            index: cur.u64()? as usize,
            items: read_items(cur)?,
        },
        5 => Change::ListDelete {
            path: cur.bytes()?,
            index: cur.u64()? as usize,
            items: read_items(cur)?,
        },
        6 => Change::TextInsert {
            path: cur.bytes()?,
            index: cur.u64()? as usize,
            text: cur.string()?,
        },
        7 => Change::TextDelete {
            path: cur.bytes()?,
            index: cur.u64()? as usize,
            text: cur.string()?,
        },
        tag => {
            return Err(DecodeError::BadTag {
                what: "diff change",
                tag,
            })
        }
    })
}

fn read_kind(cur: &mut Cursor) -> Result<ElementKind, DecodeError> {
    let tag = cur.u8()?;
    ElementKind::from_tag(tag).ok_or(DecodeError::BadTag {
        what: "element kind",
        tag,
    })
}

fn read_items(cur: &mut Cursor) -> Result<Vec<SeqItem>, DecodeError> {
    let count = cur.u32()?;
    let mut items = Vec::new();
    for _ in 0..count {
        items.push(match cur.u8()? {
            0 => SeqItem::Scalar(cur.scalar()?),
            1 => SeqItem::Composite(read_kind(cur)?),
            tag => {
                return Err(DecodeError::BadTag {
                    what: "diff item",
                    tag,
                })
            }
        });
    }
    Ok(items)
}
