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

use std::collections::{BTreeSet, HashSet};

use crate::doc::Document;
use crate::element::{Element, ElementKind};
use crate::list::List;
use crate::map::Map;
use crate::path::encode_path;
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use crate::text::Text;

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
