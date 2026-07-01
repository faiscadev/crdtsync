//! Counter — PN-counter: per-client (inc, dec) tallies, merge takes per-client
//! max in each direction.
//!
//! `displaced` marks a handle no longer installed in any slot, so the Doc
//! layer can skip op emission for it.

use crate::clientid::ClientId;
use crate::elementid::ElementId;
use std::cell::Cell;
use std::collections::HashMap;

#[derive(Clone, Copy, Default)]
struct Tally {
    inc: u32,
    dec: u32,
}

pub struct Counter {
    id: ElementId,
    entries: HashMap<ClientId, Tally>,
    displaced: Cell<bool>,
}

impl Counter {
    pub fn new(id: ElementId) -> Self {
        Self {
            id,
            entries: HashMap::new(),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    pub fn inc(&mut self, client: ClientId, amount: u32) {
        self.entries
            .entry(client)
            .and_modify(|tally| tally.inc += amount)
            .or_insert(Tally {
                inc: amount,
                dec: 0,
            });
    }

    pub fn dec(&mut self, client: ClientId, amount: u32) {
        self.entries
            .entry(client)
            .and_modify(|tally| tally.dec += amount)
            .or_insert(Tally {
                inc: 0,
                dec: amount,
            });
    }

    pub fn read(&self) -> i64 {
        self.entries
            .values()
            .map(|tally| tally.inc as i64 - tally.dec as i64)
            .sum()
    }

    pub fn merge(&mut self, other: &Self) {
        for (client, other_tally) in &other.entries {
            let entry = self
                .entries
                .entry(*client)
                .or_insert(Tally { inc: 0, dec: 0 });
            entry.inc = entry.inc.max(other_tally.inc);
            entry.dec = entry.dec.max(other_tally.dec);
        }
    }

    /// Deep copy into a fresh, non-displaced Counter (displacement is a
    /// per-instance signal, not part of the value).
    pub fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            entries: self.entries.clone(),
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
