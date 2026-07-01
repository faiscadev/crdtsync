//! List — an ordered sequence CRDT (Fugue).
//!
//! Items live in a tree: each insert attaches to a neighbour and the sequence
//! is the tree's in-order traversal. A run typed left-to-right forms a spine,
//! so two concurrent runs at the same gap stay whole and ordered by their
//! first item's stamp instead of interleaving. Deletes tombstone — a position
//! must survive to anchor inserts placed against it. The same algorithm backs
//! Text.

use crate::element::Element;
use crate::elementid::ElementId;
use crate::stamp::Stamp;
use std::cell::Cell;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Side {
    Left,
    Right,
}

struct Node {
    id: Stamp,
    value: Element,
    parent: Option<Stamp>,
    side: Side,
    tombstone: bool,
}

impl Node {
    fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            value: self.value.deep_clone(),
            parent: self.parent,
            side: self.side,
            tombstone: self.tombstone,
        }
    }
}

pub struct List {
    id: ElementId,
    nodes: HashMap<Stamp, Node>,
    displaced: Cell<bool>,
}

impl List {
    pub fn new(id: ElementId) -> Self {
        Self {
            id,
            nodes: HashMap::new(),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    pub fn len(&self) -> usize {
        self.nodes.values().filter(|n| !n.tombstone).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The live item at `index`, if any.
    pub fn get(&self, index: usize) -> Option<Element> {
        self.live_order()
            .get(index)
            .map(|s| self.nodes[s].value.clone())
    }

    /// The live items in sequence order.
    pub fn values(&self) -> Vec<Element> {
        self.live_order()
            .iter()
            .map(|s| self.nodes[s].value.clone())
            .collect()
    }

    /// Insert `value` at live `index`, identified by `stamp`.
    pub fn insert(&mut self, index: usize, value: Element, stamp: Stamp) {
        let order = self.tree_order();
        let (left, right) = self.gap(&order, index);
        let (parent, side) = self.placement(left, right);
        self.nodes.insert(
            stamp,
            Node {
                id: stamp,
                value,
                parent,
                side,
                tombstone: false,
            },
        );
    }

    /// Tombstone the live item at `index`.
    pub fn delete(&mut self, index: usize) {
        if let Some(&s) = self.live_order().get(index) {
            self.nodes.get_mut(&s).unwrap().tombstone = true;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (id, on) in &other.nodes {
            match self.nodes.get_mut(id) {
                // Same node: deletion is monotonic, so a tombstone anywhere wins.
                Some(sn) => sn.tombstone |= on.tombstone,
                None => {
                    self.nodes.insert(*id, on.deep_clone());
                }
            }
        }
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            nodes: self
                .nodes
                .iter()
                .map(|(k, n)| (*k, n.deep_clone()))
                .collect(),
            displaced: Cell::new(false),
        }
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }

    // --- Fugue internals ---

    /// Child stamps grouped by `(parent, side)`, each bucket sorted by stamp.
    fn children(&self) -> HashMap<(Option<Stamp>, Side), Vec<Stamp>> {
        let mut map: HashMap<(Option<Stamp>, Side), Vec<Stamp>> = HashMap::new();
        for n in self.nodes.values() {
            map.entry((n.parent, n.side)).or_default().push(n.id);
        }
        for bucket in map.values_mut() {
            bucket.sort();
        }
        map
    }

    /// Every node in sequence order (tombstones included).
    fn tree_order(&self) -> Vec<Stamp> {
        let children = self.children();
        let bucket = |p: Option<Stamp>, side: Side| -> Vec<Stamp> {
            children.get(&(p, side)).cloned().unwrap_or_default()
        };

        enum Step {
            Emit(Stamp),
            Expand(Option<Stamp>),
        }
        let mut out = Vec::with_capacity(self.nodes.len());
        let mut stack = vec![Step::Expand(None)];
        while let Some(step) = stack.pop() {
            match step {
                Step::Emit(s) => out.push(s),
                Step::Expand(p) => {
                    // Reverse push so execution is: left children, self, right children.
                    for r in bucket(p, Side::Right).into_iter().rev() {
                        stack.push(Step::Expand(Some(r)));
                    }
                    if let Some(s) = p {
                        stack.push(Step::Emit(s));
                    }
                    for l in bucket(p, Side::Left).into_iter().rev() {
                        stack.push(Step::Expand(Some(l)));
                    }
                }
            }
        }
        out
    }

    /// Live nodes in sequence order.
    fn live_order(&self) -> Vec<Stamp> {
        self.tree_order()
            .into_iter()
            .filter(|s| !self.nodes[s].tombstone)
            .collect()
    }

    /// The nodes bracketing the gap before live position `index`.
    fn gap(&self, order: &[Stamp], index: usize) -> (Option<Stamp>, Option<Stamp>) {
        let mut live = 0;
        let mut boundary = order.len();
        for (k, s) in order.iter().enumerate() {
            if live == index {
                boundary = k;
                break;
            }
            if !self.nodes[s].tombstone {
                live += 1;
            }
        }
        let left = (boundary > 0).then(|| order[boundary - 1]);
        let right = order.get(boundary).copied();
        (left, right)
    }

    /// Attach after the left origin when it has no right subtree yet, else as
    /// the left child of the right origin — the rule that keeps concurrent runs
    /// from interleaving.
    fn placement(&self, left: Option<Stamp>, right: Option<Stamp>) -> (Option<Stamp>, Side) {
        match (left, right) {
            (Some(l), _) if !self.has_right_child(l) => (Some(l), Side::Right),
            (_, Some(r)) => (Some(r), Side::Left),
            (Some(l), None) => (Some(l), Side::Right),
            (None, None) => (None, Side::Right),
        }
    }

    fn has_right_child(&self, parent: Stamp) -> bool {
        self.nodes
            .values()
            .any(|n| n.parent == Some(parent) && n.side == Side::Right)
    }
}
