//! Schema state validation.
//!
//! [`validate`] walks a document's materialized element tree against a parsed
//! [`Schema`] and produces the set of [`Violation`]s — a pure read that never
//! mutates. It is the state half of the schema's closure guarantee: every
//! constraint it reports has a repair, so no valid schema can describe a runtime
//! state that cannot be normalized.
//!
//! Only the constraints the schema model expresses are checked, per built
//! primitive: a map slot outside its type's declared allowlist, an element whose
//! runtime kind does not match its declared type, a register/counter integer
//! outside its numeric bounds, and a list/text longer than its `max`. The walk is
//! deterministic — map keys sorted, list items in sequence order, depth-first —
//! so replicas that merged the same ops produce identical violation sets.
//!
//! In a well-formed tree every element has one parent, so each is reached once
//! and every violation is reported. The walk is nonetheless total on any decoded
//! document, including one whose slots were crafted to share or cycle: an
//! element's own constraint is checked each time it is reached (a shared leaf is
//! flagged under both slots), but it is descended into only once — so the work is
//! bounded by the number of slot edges and the walk can neither loop on a cycle
//! nor blow up on a shared subtree.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::doc::Document;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::List;
use crate::map::Map;
use crate::scalar::Scalar;
use crate::schema::{Schema, TypeDef};

/// One step down the element tree: a map slot key, or a list item index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Key(Vec<u8>),
    Index(usize),
}

/// What is wrong with an element, and the data a repair needs to fix it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViolationKind {
    /// The element's runtime kind is not the one its declared type calls for.
    KindMismatch {
        expected: ElementKind,
        found: ElementKind,
    },
    /// A map slot the governing type does not declare.
    UnknownSlot,
    /// A register/counter integer below its declared minimum.
    BelowMin { value: i64, min: i64 },
    /// A register/counter integer above its declared maximum.
    AboveMax { value: i64, max: i64 },
    /// A list or text longer than its declared maximum.
    TooLong { len: u64, max: u64 },
    /// An attribute key an xml element's declared type does not list in `attrs`.
    DisallowedAttr,
    /// An attribute whose value is the wrong kind for its declared attr type — a
    /// mistyped attr is dropped, not clamped, so it is distinct from an
    /// out-of-range (right-kind) attr value.
    MistypedAttr {
        expected: ElementKind,
        found: ElementKind,
    },
}

/// A single constraint violation at a located element.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub path: Vec<Step>,
    pub kind: ViolationKind,
}

/// A location in the tree as a shared path from the root — each node points at
/// its parent, so extending a path is one allocation and a queued work item holds
/// a cheap handle rather than a full owned path. The `Vec<Step>` a violation
/// carries is materialized only when one is actually recorded.
enum PathNode {
    Root,
    Step(Step, Option<Rc<PathNode>>),
}

impl PathNode {
    /// The root-to-here steps, in order.
    fn steps(&self) -> Vec<Step> {
        let mut steps = Vec::new();
        let mut node = self;
        while let PathNode::Step(step, Some(parent)) = node {
            steps.push(step.clone());
            node = parent.as_ref();
        }
        steps.reverse();
        steps
    }
}

impl Drop for PathNode {
    /// Dismantle the parent chain iteratively. A deep path is a long single-owner
    /// chain, and the derived drop would recurse one stack frame per level and
    /// overflow — so unlink each uniquely-owned link in a loop instead (the parent
    /// is an `Option` so a link is taken out without allocating a placeholder). A
    /// link a live path still shares is left for its last owner to drop.
    fn drop(&mut self) {
        let PathNode::Step(_, parent) = self else {
            return;
        };
        let mut next = parent.take();
        while let Some(rc) = next {
            match Rc::try_unwrap(rc) {
                Ok(mut node) => {
                    let PathNode::Step(_, parent) = &mut node else {
                        break;
                    };
                    next = parent.take();
                }
                Err(_) => break,
            }
        }
    }
}

/// One unit of pending work: an element to check against a named type, or a
/// known-bad slot to report. Slots of a map are queued (not walked eagerly) so
/// unknown-slot reports and child descents stay interleaved in sorted key order.
enum Work<'a> {
    Check {
        element: Element,
        type_name: &'a str,
        path: Rc<PathNode>,
    },
    UnknownSlot {
        path: Rc<PathNode>,
    },
    /// A violation to record when this work item is reached, so a violation found
    /// while queueing children (an attr key, a mistyped attr) still emits in tree
    /// order rather than ahead of the descent.
    Report {
        path: Rc<PathNode>,
        kind: ViolationKind,
    },
}

/// Every way `doc`'s current state violates `schema`, in deterministic tree
/// order. An empty result is a conforming document.
pub fn validate(doc: &Document, schema: &Schema) -> Vec<Violation> {
    Validator {
        schema,
        visited: HashSet::new(),
        out: Vec::new(),
        stack: vec![Work::Check {
            element: Element::Map(doc.root()),
            type_name: schema.root(),
            path: Rc::new(PathNode::Root),
        }],
        allowlists: HashMap::new(),
    }
    .run()
}

/// The state of one validation walk over an explicit work stack.
struct Validator<'a> {
    schema: &'a Schema,
    visited: HashSet<ElementId>,
    out: Vec<Violation>,
    stack: Vec<Work<'a>>,
    /// A map type's `slot → child type` allowlist, built once per type and reused
    /// across every instance of it — a recursive schema visits one type's maps
    /// many times.
    allowlists: HashMap<&'a str, HashMap<&'a [u8], &'a str>>,
}

/// The runtime kind an element of type `td` must have.
fn expected_kind(td: &TypeDef) -> ElementKind {
    match td {
        TypeDef::Map { .. } => ElementKind::Map,
        TypeDef::List { .. } => ElementKind::List,
        TypeDef::Text { .. } => ElementKind::Text,
        TypeDef::Register { .. } => ElementKind::Register,
        TypeDef::Counter { .. } => ElementKind::Counter,
        TypeDef::Xml { tag: Some(_), .. } => ElementKind::XmlElement,
        TypeDef::Xml { tag: None, .. } => ElementKind::XmlFragment,
    }
}

/// The declared type of an xml child, resolved against the parent type's
/// `children` allowlist: an element child matches the allowed xml type whose tag
/// equals its own, a text child matches the allowed text type. `None` if no
/// allowed type fits (a disallowed child — structural repair's concern, 5c).
fn resolve_child_type<'a>(
    schema: &'a Schema,
    child: &Element,
    allowed: &'a [String],
) -> Option<&'a str> {
    let child_kind = child.kind();
    let child_tag = match child {
        Element::XmlElement(x) => Some(x.borrow().tag().to_vec()),
        _ => None,
    };
    allowed.iter().find_map(|name| {
        let td = schema.type_def(name)?;
        let fits = match td {
            TypeDef::Xml { tag: Some(t), .. } => {
                child_kind == ElementKind::XmlElement && child_tag.as_deref() == Some(t.as_bytes())
            }
            TypeDef::Xml { tag: None, .. } => child_kind == ElementKind::XmlFragment,
            other => expected_kind(other) == child_kind,
        };
        fits.then_some(name.as_str())
    })
}

/// The `marks` allowlist of the XmlElement that directly contains sequence
/// `target`, resolved by the same contextual, tag-matched descent [`validate`]
/// walks from the root. `None` when `target` is not directly held by a
/// schema-typed XmlElement — it sits in a map slot, a plain list, or a tagless
/// fragment — in which case no per-type restriction applies and every mark is
/// kept. Only an XmlElement (`tag: Some`) declares a marks allowlist; a fragment
/// (`tag: None`) does not restrict the marks on its inline text.
pub(crate) fn marks_allowlist<'a>(
    doc: &Document,
    schema: &'a Schema,
    target: ElementId,
) -> Option<&'a [String]> {
    let mut visited = HashSet::new();
    let mut stack: Vec<(Element, &'a str)> = vec![(Element::Map(doc.root()), schema.root())];
    while let Some((element, type_name)) = stack.pop() {
        let Some(td) = schema.type_def(type_name) else {
            continue;
        };
        if matches!(element, Element::Scalar(_))
            || expected_kind(td) != element.kind()
            || !visited.insert(element.id())
        {
            continue;
        }
        match (td, &element) {
            (TypeDef::Map { children }, Element::Map(m)) => {
                let allow: HashMap<&[u8], &str> = children
                    .iter()
                    .map(|(s, t)| (s.as_bytes(), t.as_str()))
                    .collect();
                for (key, child) in m.borrow().entries() {
                    if is_target(&child, target) {
                        return None;
                    }
                    if let Some(&child_type) = allow.get(key.as_slice()) {
                        stack.push((child, child_type));
                    }
                }
            }
            (TypeDef::List { items, .. }, Element::List(l)) => {
                for item in l.borrow().values() {
                    if is_target(&item, target) {
                        return None;
                    }
                    stack.push((item, items));
                }
            }
            (
                TypeDef::Xml {
                    children, marks, ..
                },
                Element::XmlElement(x),
            ) => {
                let list = x.borrow().children();
                for child in list.borrow().values() {
                    if is_target(&child, target) {
                        return Some(marks);
                    }
                    if let Some(child_type) = resolve_child_type(schema, &child, children) {
                        stack.push((child, child_type));
                    }
                }
            }
            (TypeDef::Xml { children, .. }, Element::XmlFragment(f)) => {
                let list = f.borrow().children();
                for child in list.borrow().values() {
                    if is_target(&child, target) {
                        return None;
                    }
                    if let Some(child_type) = resolve_child_type(schema, &child, children) {
                        stack.push((child, child_type));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether `element` is the sequence `target`. A scalar has no id, so it is never
/// a mark's target sequence.
fn is_target(element: &Element, target: ElementId) -> bool {
    !matches!(element, Element::Scalar(_)) && element.id() == target
}

impl<'a> Validator<'a> {
    fn run(mut self) -> Vec<Violation> {
        while let Some(work) = self.stack.pop() {
            match work {
                Work::UnknownSlot { path } => self.out.push(Violation {
                    path: path.steps(),
                    kind: ViolationKind::UnknownSlot,
                }),
                Work::Report { path, kind } => self.out.push(Violation {
                    path: path.steps(),
                    kind,
                }),
                Work::Check {
                    element,
                    type_name,
                    path,
                } => self.check(element, type_name, path),
            }
        }
        self.out
    }

    /// Check one element against `type_name`: its own constraint every time it is
    /// reached, then — the first time only — queue its children (reversed, so they
    /// pop in tree order). A wrong-kind element is terminal.
    fn check(&mut self, element: Element, type_name: &'a str, path: Rc<PathNode>) {
        // A parse-validated schema resolves every type reference, so an unknown
        // name cannot occur; if it somehow did, there is nothing to check against.
        let Some(td) = self.schema.type_def(type_name) else {
            return;
        };
        let found = element.kind();
        let expected = expected_kind(td);
        if found != expected {
            self.out.push(Violation {
                path: path.steps(),
                kind: ViolationKind::KindMismatch { expected, found },
            });
            return;
        }
        // The element's own constraint — reported on every visit, so a slot that
        // shares a handle with another is still flagged under its own path.
        match (td, &element) {
            (TypeDef::List { max, .. }, Element::List(l)) => {
                self.check_max_len(l.borrow().len() as u64, *max, &path)
            }
            (TypeDef::Text { max }, Element::Text(t)) => {
                self.check_max_len(t.borrow().len() as u64, *max, &path)
            }
            (TypeDef::Register { min, max }, Element::Register(r)) => {
                if let Scalar::Int(v) = r.borrow().read() {
                    self.check_bounds(*v, *min, *max, &path);
                }
            }
            (TypeDef::Counter { min, max }, Element::Counter(c)) => {
                self.check_bounds(c.borrow().read(), *min, *max, &path)
            }
            _ => {}
        }
        // Descend once. A scalar is a leaf; a composite already entered means the
        // slot graph shares or loops back, so its subtree is not re-walked.
        if matches!(element, Element::Scalar(_)) || !self.visited.insert(element.id()) {
            return;
        }
        match (td, &element) {
            (TypeDef::Map { children }, Element::Map(m)) => {
                let allow = self.allowlists.entry(type_name).or_insert_with(|| {
                    children
                        .iter()
                        .map(|(s, t)| (s.as_bytes(), t.as_str()))
                        .collect()
                });
                let mut queued = Vec::new();
                for (key, child) in m.borrow().entries().into_iter().rev() {
                    let child_type = allow.get(key.as_slice()).copied();
                    let child_path = Rc::new(PathNode::Step(Step::Key(key), Some(path.clone())));
                    queued.push(match child_type {
                        Some(child_type) => Work::Check {
                            element: child,
                            type_name: child_type,
                            path: child_path,
                        },
                        None => Work::UnknownSlot { path: child_path },
                    });
                }
                self.stack.extend(queued);
            }
            (TypeDef::List { items, .. }, Element::List(l)) => {
                for (i, item) in l.borrow().values().into_iter().enumerate().rev() {
                    let item_path = Rc::new(PathNode::Step(Step::Index(i), Some(path.clone())));
                    self.stack.push(Work::Check {
                        element: item,
                        type_name: items,
                        path: item_path,
                    });
                }
            }
            (
                TypeDef::Xml {
                    attrs, children, ..
                },
                Element::XmlElement(x),
            ) => {
                let (attrs_map, children_list) = {
                    let x = x.borrow();
                    (x.attrs(), x.children())
                };
                // Queue children first, then attrs on top, so attrs (sorted keys)
                // emit before children (sequence order) in the popped tree order.
                self.queue_xml_children(children, &children_list, &path);
                self.check_xml_attrs(attrs, &attrs_map, &path);
            }
            (TypeDef::Xml { children, .. }, Element::XmlFragment(f)) => {
                let children_list = f.borrow().children();
                self.queue_xml_children(children, &children_list, &path);
            }
            _ => {}
        }
    }

    /// Validate an xml element's attrs Map against its type's `attrs` allowlist: an
    /// undeclared key is a `DisallowedAttr`, a declared key whose value is the
    /// wrong kind is a `MistypedAttr`, and a right-kind value recurses to its
    /// declared attr type (so an out-of-range value falls into the bounds rule).
    fn check_xml_attrs(
        &mut self,
        attrs: &'a [(String, String)],
        map: &Rc<RefCell<Map>>,
        path: &Rc<PathNode>,
    ) {
        let mut queued = Vec::new();
        for (key, child) in map.borrow().entries().into_iter().rev() {
            let child_path = Rc::new(PathNode::Step(Step::Key(key.clone()), Some(path.clone())));
            match attrs.iter().find(|(k, _)| k.as_bytes() == key.as_slice()) {
                None => queued.push(Work::Report {
                    path: child_path,
                    kind: ViolationKind::DisallowedAttr,
                }),
                Some((_, ty)) => {
                    let found = child.kind();
                    match self.schema.type_def(ty).map(expected_kind) {
                        Some(expected) if expected != found => queued.push(Work::Report {
                            path: child_path,
                            kind: ViolationKind::MistypedAttr { expected, found },
                        }),
                        _ => queued.push(Work::Check {
                            element: child,
                            type_name: ty.as_str(),
                            path: child_path,
                        }),
                    }
                }
            }
        }
        self.stack.extend(queued);
    }

    /// Queue each child of an xml element / fragment that resolves to an allowed
    /// child type (its tag matched against `children`). A child that resolves to no
    /// allowed type is a disallowed child — left to structural repair (5c); here it
    /// is simply not descended, so a nested element's own attrs are still reached
    /// through the children that do resolve.
    fn queue_xml_children(
        &mut self,
        children: &'a [String],
        list: &Rc<RefCell<List>>,
        path: &Rc<PathNode>,
    ) {
        let mut queued = Vec::new();
        for (i, child) in list.borrow().values().into_iter().enumerate().rev() {
            if let Some(child_type) = resolve_child_type(self.schema, &child, children) {
                let child_path = Rc::new(PathNode::Step(Step::Index(i), Some(path.clone())));
                queued.push(Work::Check {
                    element: child,
                    type_name: child_type,
                    path: child_path,
                });
            }
        }
        self.stack.extend(queued);
    }

    /// Record a `TooLong` violation for a sequence of `len` over its `max`.
    fn check_max_len(&mut self, len: u64, max: Option<u64>, path: &PathNode) {
        if let Some(max) = max {
            if len > max {
                self.out.push(Violation {
                    path: path.steps(),
                    kind: ViolationKind::TooLong { len, max },
                });
            }
        }
    }

    /// Record a below-min or above-max violation for integer `v`. A well-formed
    /// schema keeps `min <= max`, so at most one bound is ever crossed.
    fn check_bounds(&mut self, v: i64, min: Option<i64>, max: Option<i64>, path: &PathNode) {
        if let Some(min) = min {
            if v < min {
                self.out.push(Violation {
                    path: path.steps(),
                    kind: ViolationKind::BelowMin { value: v, min },
                });
                return;
            }
        }
        if let Some(max) = max {
            if v > max {
                self.out.push(Violation {
                    path: path.steps(),
                    kind: ViolationKind::AboveMax { value: v, max },
                });
            }
        }
    }
}
