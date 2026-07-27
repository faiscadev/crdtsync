//! ClientId — a per-replica UUIDv7 identity.
//!
//! One connection holds a replica per subscribed channel, and every replica is
//! its own author: op ids, stamps, and per-client counter tallies all key off
//! this id. [`for_channel`](ClientId::for_channel) derives a channel's replica
//! identity from the connection's, so those authorship spaces stay disjoint
//! across the channels of one session.

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

    /// The replica identity a connection's `channel` authors under.
    ///
    /// Each channel of a session holds its own replica, and two channels can be
    /// bound to the same room — a whole-room subscription beside a zone-scoped
    /// one, two zones of one room, a branch beside the default. Sharing an
    /// identity across them makes both replicas mint the same `OpId` and the same
    /// [`Stamp`](crate::Stamp) for unrelated edits, so a peer (and the server's
    /// log) drops one channel's ops as duplicates of the other's. An identity per
    /// channel gives each its own op-id, stamp, transaction-group, and
    /// counter-tally space.
    ///
    /// Channel 0 is the replica the connection declared itself to be, so it
    /// authors under this id unchanged: a session's first subscription carries the
    /// id its embedder chose, and keeps it across a reconnect (a resume reuses the
    /// channel). Channel numbers are assigned in subscribe order and never
    /// recycled, so a *later* subscription is channel 0 only if it is the
    /// session's first — that is deliberate, since reusing a freed number would
    /// hand a fresh replica, minting from seq 0, the identity of the ops the old
    /// one still has in flight.
    ///
    /// Every further channel takes a UUIDv5 over this id as namespace and the
    /// channel number's four big-endian bytes as name — distinct from the declared
    /// id and from each other. Distinctness from *another connection's* id rests
    /// on the 122 random bits an embedder mints its own id from, not on the UUID
    /// version field: ids arrive through [`from_bytes`](Self::from_bytes), which
    /// stamps no version.
    ///
    /// The derivation is protocol, not a local convenience: it is pure — no
    /// [`Host`] entropy — so both ends of the wire compute it, and a server that
    /// knows the connection's Hello id and the channel an op batch names
    /// re-derives the identity that batch must carry. Its namespace, byte width,
    /// and endianness are therefore fixed; changing any of them rebinds every
    /// deployed client's identity.
    pub fn for_channel(&self, channel: u32) -> Self {
        match channel {
            0 => *self,
            n => Self(Uuid::new_v5(&self.0, &n.to_be_bytes())),
        }
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        self.0.into_bytes()
    }
}
