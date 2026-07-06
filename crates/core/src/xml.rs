//! XmlElement / XmlFragment — the tree primitives.
//!
//! An [`XmlElement`] is a tagged tree node: an immutable `tag`, an `attrs` [`Map`]
//! (holding any CRDT values), and an ordered `children` sequence of nested
//! `XmlElement`s and `Text` runs. An [`XmlFragment`] is the same children sequence
//! without a tag or attrs — a bare document body.
//!
//! Both reuse the built composites rather than reimplement them: attrs are a
//! [`Map`] and children a [`List`] (the Fugue sequence CRDT, whose nodes already
//! hold any [`Element`](crate::Element), so a child may itself be an `XmlElement`
//! or a `Text`). Merge, tombstones, LWW, and placement therefore come from those
//! engines unchanged — this type only pairs them under one id and a tag. The
//! attrs Map and children List take ids derived from the element's own id, so
//! every replica agrees on them (the same convergence the [`ElementId`] derivation
//! gives Map slots). The `tag` is fixed at creation — retagging a node is a
//! replace, not a mutation — so a merge of the same element never reconciles it.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::elementid::{ElementId, ElementKind};
use crate::list::List;
use crate::map::Map;

/// The derivation key for an element's attrs Map id.
const ATTRS_KEY: &[u8] = b"attrs";
/// The derivation key for an element's (or fragment's) children List id.
const CHILDREN_KEY: &[u8] = b"children";

/// A tagged tree node — `tag` + `attrs` Map + `children` sequence.
pub struct XmlElement {
    id: ElementId,
    tag: Vec<u8>,
    attrs: Rc<RefCell<Map>>,
    children: Rc<RefCell<List>>,
    displaced: Cell<bool>,
}

impl XmlElement {
    /// A fresh element with the given `tag`. Its attrs Map and children List take
    /// ids derived from `id`, so two replicas building the same element agree on
    /// them.
    pub fn new(id: ElementId, tag: Vec<u8>) -> Self {
        Self {
            id,
            tag,
            attrs: Rc::new(RefCell::new(Map::new(ElementId::derive(
                id,
                ATTRS_KEY,
                ElementKind::Map,
            )))),
            children: Rc::new(RefCell::new(List::new(ElementId::derive(
                id,
                CHILDREN_KEY,
                ElementKind::List,
            )))),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    /// The node's tag — fixed at creation.
    pub fn tag(&self) -> &[u8] {
        &self.tag
    }

    /// The attrs Map handle, shared with the document registry.
    pub fn attrs(&self) -> Rc<RefCell<Map>> {
        Rc::clone(&self.attrs)
    }

    /// The children sequence handle, shared with the document registry.
    pub fn children(&self) -> Rc<RefCell<List>> {
        Rc::clone(&self.children)
    }

    /// Merge `src` (the same element) into this one — attrs and children each
    /// reconcile through their own engine. The `tag` is identity, not state, so
    /// it is left untouched.
    pub fn merge(&self, src: &Self) {
        self.attrs.borrow_mut().merge(&src.attrs.borrow());
        self.children.borrow_mut().merge(&src.children.borrow());
    }

    /// An independent deep copy — fresh attrs/children handles, not shared.
    pub fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            tag: self.tag.clone(),
            attrs: Rc::new(RefCell::new(self.attrs.borrow().deep_clone())),
            children: Rc::new(RefCell::new(self.children.borrow().deep_clone())),
            displaced: Cell::new(false),
        }
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    pub fn reinstate(&self) {
        self.displaced.set(false);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }
}

/// A tagless children sequence — a document body of `XmlElement`s and `Text` runs.
pub struct XmlFragment {
    id: ElementId,
    children: Rc<RefCell<List>>,
    displaced: Cell<bool>,
}

impl XmlFragment {
    /// A fresh, empty fragment. Its children List id derives from `id`.
    pub fn new(id: ElementId) -> Self {
        Self {
            id,
            children: Rc::new(RefCell::new(List::new(ElementId::derive(
                id,
                CHILDREN_KEY,
                ElementKind::List,
            )))),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    /// The children sequence handle, shared with the document registry.
    pub fn children(&self) -> Rc<RefCell<List>> {
        Rc::clone(&self.children)
    }

    pub fn merge(&self, src: &Self) {
        self.children.borrow_mut().merge(&src.children.borrow());
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            children: Rc::new(RefCell::new(self.children.borrow().deep_clone())),
            displaced: Cell::new(false),
        }
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    pub fn reinstate(&self) {
        self.displaced.set(false);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }
}
