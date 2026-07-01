//! Stamp — (lamport, client_id) with a strict-greater total order.

use crate::clientid::ClientId;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Stamp {
    pub lamport: u64,
    pub client: ClientId,
}

impl Ord for Stamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.lamport
            .cmp(&other.lamport)
            .then_with(|| self.client.cmp(&other.client))
    }
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
