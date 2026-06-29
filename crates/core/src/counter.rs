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
        let _ = id;
        todo!()
    }

    pub fn id(&self) -> ElementId {
        todo!()
    }

    pub fn inc(&mut self, client: ClientId, amount: u32) {
        let _ = (client, amount);
        todo!()
    }

    pub fn dec(&mut self, client: ClientId, amount: u32) {
        let _ = (client, amount);
        todo!()
    }

    pub fn read(&self) -> i64 {
        todo!()
    }

    pub fn merge(&mut self, other: &Counter) {
        let _ = other;
        todo!()
    }

    /// Deep copy into a fresh, non-displaced Counter (displacement is a
    /// per-instance signal, not part of the value).
    pub fn deep_clone(&self) -> Counter {
        todo!()
    }

    pub fn displace(&self) {
        todo!()
    }

    pub fn is_displaced(&self) -> bool {
        todo!()
    }
}
