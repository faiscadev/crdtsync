//! ClientId — a per-replica UUIDv7 identity.

use crate::host::Host;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ClientId(Uuid);

impl ClientId {
    /// Generate a fresh v7 id from injected host entropy + clock. Built
    /// manually (not the uuid crate's `v7` feature) to avoid a getrandom dep.
    pub fn generate(host: &dyn Host) -> Self {
        let _ = host;
        todo!()
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let _ = bytes;
        todo!()
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        todo!()
    }
}
