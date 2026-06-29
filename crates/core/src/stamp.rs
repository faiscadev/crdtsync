//! Stamp — (lamport, client_id) with a strict-greater total order.

use crate::clientid::ClientId;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Stamp {
    pub lamport: u64,
    pub client: ClientId,
}

impl Stamp {
    /// Strictly greater: larger lamport, or equal lamport and larger client id.
    pub fn gt(&self, other: &Stamp) -> bool {
        let _ = other;
        todo!()
    }
}
