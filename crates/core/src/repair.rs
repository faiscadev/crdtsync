//! Read-time invariant repair.
//!
//! [`repairs`] turns the [`validate`] violation set into the normalization a
//! schema-conformant read applies over merged state: a register/counter integer
//! clamped into its declared bounds, a list/text truncated to its `max` by
//! dropping the lamport-newest excess, or a disallowed / mistyped attr or
//! disallowed xml child dropped from the element. It is a pure read — the stored
//! ops are never touched — and every input is a value already in state, so a
//! repair mints nothing and needs no clock.
//!
//! The drop-newest order comes from the stamps in state, total-ordered by
//! `(lamport, client)`, so replicas that merged the same ops truncate to the same
//! surviving items. Only a violation with a defined normalization is repaired; a
//! kind mismatch or an unknown slot has no value to read repaired, so [`repairs`]
//! omits it.

use crate::doc::Document;
use crate::element::Element;
use crate::schema::Schema;
use crate::stamp::Stamp;
use crate::validate::{validate, Step, ViolationKind};

/// How a non-conformant element reads once repaired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairKind {
    /// A register/counter integer read clamped to this in-bounds value.
    Clamped { value: i64 },
    /// A list/text read as only these sequence indices — the survivors, in
    /// sequence order, after dropping the lamport-newest items over `max`.
    Truncated { keep: Vec<usize> },
    /// A node read as absent — a disallowed / mistyped attr, or an xml child whose
    /// tag no allowed type matches, drops from a conformant read of the element.
    Dropped,
}

/// The repaired reading of one located element.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Repair {
    pub path: Vec<Step>,
    pub kind: RepairKind,
}

/// A location's repaired *reading*, independent of where it currently sits — what
/// the `onRepaired` observer diffs across settles to decide a location's repair
/// status changed. A truncation is identified by its surviving node *stamps*, not
/// their sequence indices: an unrelated edit that shifts those indices while the
/// same items survive is not a reading change (the consumer already observes the
/// sequence edit through normal reads), whereas a different surviving item, or a
/// re-clamp to the other bound, is.
///
/// The `path` completes the identity. It is all `Step::Key` — and so fully
/// index-stable — for a repair under map slots (the common case). A repair under
/// a *sequence* position carries a `Step::Index` (a bounded register list item, or
/// now an xml child of a bounded type): such a repair's identity shifts if a
/// preceding item is inserted or removed, which can churn an `onRepaired` report
/// even though the reading is unchanged. Keying a sequence-positioned repair by a
/// stable node stamp instead is a follow-up (it needs the same node-stamp handle
/// the truncation survivors use, extended to a leaf register item).
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum RepairId {
    Clamp {
        path: Vec<Step>,
        value: i64,
    },
    Keep {
        path: Vec<Step>,
        survivors: Vec<Stamp>,
    },
    /// A dropped attribute, identified by its location — the reading (absent) is
    /// the same whenever the location is in violation, so the path is the identity.
    Drop {
        path: Vec<Step>,
    },
}

/// How every non-conformant element in `doc` reads repaired against `schema`, in
/// the same deterministic tree order as [`validate`]. An empty result is a
/// document that reads conformant as-is.
pub fn repairs(doc: &Document, schema: &Schema) -> Vec<Repair> {
    keyed_repairs(doc, schema)
        .into_iter()
        .map(|(repair, _)| repair)
        .collect()
}

/// Each repair paired with its reading-stable [`RepairId`]. A read consumes the
/// [`Repair`] (index projection); the observer diffs on the id. One walk builds
/// both.
pub(crate) fn keyed_repairs(doc: &Document, schema: &Schema) -> Vec<(Repair, RepairId)> {
    validate(doc, schema)
        .into_iter()
        .filter_map(|v| {
            let path = v.path;
            let (kind, id) = match v.kind {
                ViolationKind::BelowMin { min, .. } => (
                    RepairKind::Clamped { value: min },
                    RepairId::Clamp {
                        path: path.clone(),
                        value: min,
                    },
                ),
                ViolationKind::AboveMax { max, .. } => (
                    RepairKind::Clamped { value: max },
                    RepairId::Clamp {
                        path: path.clone(),
                        value: max,
                    },
                ),
                ViolationKind::TooLong { max, .. } => {
                    let ids = sequence_node_ids(element_at(doc, &path)?)?;
                    let keep = survivors(&ids, max);
                    let survivors = keep.iter().map(|&i| ids[i]).collect();
                    (
                        RepairKind::Truncated { keep },
                        RepairId::Keep {
                            path: path.clone(),
                            survivors,
                        },
                    )
                }
                ViolationKind::DisallowedAttr
                | ViolationKind::MistypedAttr { .. }
                | ViolationKind::DisallowedChild => {
                    (RepairKind::Dropped, RepairId::Drop { path: path.clone() })
                }
                ViolationKind::KindMismatch { .. } | ViolationKind::UnknownSlot => return None,
            };
            Some((Repair { path, kind }, id))
        })
        .collect()
}

/// Walk `path` from the root to the element it locates. Intermediate steps are
/// map keys and live list indices; a step that does not resolve (a since-changed
/// tree) yields `None`.
fn element_at(doc: &Document, path: &[Step]) -> Option<Element> {
    let mut cur = Element::Map(doc.root());
    for step in path {
        cur = match (step, &cur) {
            (Step::Key(k), Element::Map(m)) => m.borrow().get(k)?,
            (Step::Index(i), Element::List(l)) => l.borrow().get(*i)?,
            // An xml element's attrs are keyed (its attrs Map), its children
            // indexed (its children List); a fragment has only children.
            (Step::Key(k), Element::XmlElement(x)) => x.borrow().attrs().borrow().get(k)?,
            (Step::Index(i), Element::XmlElement(x)) => x.borrow().children().borrow().get(*i)?,
            (Step::Index(i), Element::XmlFragment(f)) => f.borrow().children().borrow().get(*i)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// The live node ids of a list or text, in sequence order (index-aligned with
/// the values a read returns). `None` for any other element.
fn sequence_node_ids(el: Element) -> Option<Vec<Stamp>> {
    match el {
        Element::List(l) => {
            let l = l.borrow();
            Some(l.node_ids(0, l.len()))
        }
        Element::Text(t) => {
            let t = t.borrow();
            Some(t.node_ids(0, t.len()))
        }
        _ => None,
    }
}

/// The sequence indices that survive truncating to `max` — every index except
/// the `len - max` whose stamp is lamport-newest. Returned in ascending order.
fn survivors(ids: &[Stamp], max: u64) -> Vec<usize> {
    let len = ids.len();
    let max = max as usize;
    if len <= max {
        return (0..len).collect();
    }
    // Order by stamp, newest first; the `len - max` newest are dropped, so the
    // rest of the order are the survivors. Return them in sequence order.
    let mut by_recency: Vec<usize> = (0..len).collect();
    by_recency.sort_by(|&a, &b| ids[b].cmp(&ids[a]));
    let mut keep: Vec<usize> = by_recency.split_off(len - max);
    keep.sort_unstable();
    keep
}
