//! Stamp — (lamport, client_id) with a strict-greater total order.

use crate::clientid::ClientId;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Stamp {
    pub lamport: u64,
    pub client: ClientId,
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.lamport == other.lamport {
            Some(self.client.cmp(&other.client))
        } else {
            Some(self.lamport.cmp(&other.lamport))
        }
    }
}
