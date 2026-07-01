//! ClientId — a per-replica UUIDv7 identity.

use crate::host::Host;
use uuid::{Builder, Uuid};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Ord, PartialOrd)]
pub struct ClientId(Uuid);

impl ClientId {
    pub fn generate(host: &dyn Host) -> Self {
        let mut rand = [0u8; 10];
        host.entropy(&mut rand);
        let uuid = Builder::from_unix_timestamp_millis(host.now_unix_millis(), &rand).into_uuid();
        Self(uuid)
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Builder::from_bytes(bytes).into_uuid())
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        self.0.into_bytes()
    }
}
