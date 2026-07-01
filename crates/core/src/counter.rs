//! Counter — PN-counter: per-client (inc, dec) tallies, merge takes per-client
//! max in each direction.
//!
//! `displaced` marks a handle no longer installed in any slot, so the Doc
//! layer can skip op emission for it.

use crate::clientid::ClientId;
use crate::codec::{len_u32, put_u32, Cursor, DecodeError};
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

    /// Append this counter's state — id and every per-client tally — to `out`.
    /// Entries are ordered by client so equal states encode identically.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.as_bytes());
        let mut entries: Vec<(&ClientId, &Tally)> = self.entries.iter().collect();
        entries.sort_by_key(|(client, _)| client.as_bytes());
        put_u32(out, len_u32(entries.len()));
        for (client, tally) in entries {
            out.extend_from_slice(&client.as_bytes());
            put_u32(out, tally.inc);
            put_u32(out, tally.dec);
        }
    }

    /// Read a counter from `cur`, advancing it.
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<Counter, DecodeError> {
        let id = cur.element_id()?;
        let count = cur.u32()?;
        let mut entries = HashMap::with_capacity(count as usize);
        for _ in 0..count {
            let client = cur.client()?;
            let inc = cur.u32()?;
            let dec = cur.u32()?;
            // A client must appear once; a duplicate would silently drop a tally
            // and let a non-canonical state decode.
            if entries.insert(client, Tally { inc, dec }).is_some() {
                return Err(DecodeError::BadTag {
                    what: "counter: duplicate client",
                    tag: 0,
                });
            }
        }
        Ok(Counter {
            id,
            entries,
            displaced: Cell::new(false),
        })
    }

    /// Serialize this counter's state to self-contained bytes.
    pub fn encode_state(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_state_into(&mut out);
        out
    }

    /// Read a counter from a complete byte slice, rejecting trailing bytes.
    pub fn decode_state(bytes: &[u8]) -> Result<Counter, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let counter = Counter::decode_state_from(&mut cur)?;
        if cur.at_end() {
            Ok(counter)
        } else {
            Err(DecodeError::TrailingBytes)
        }
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
