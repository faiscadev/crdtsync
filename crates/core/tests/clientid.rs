use crdtsync_core::{ClientId, Host};

mod common;
use common::cid;

// Deterministic host: entropy is a fixed fill byte, clock a fixed instant.
struct TestHost {
    fill: u8,
    millis: u64,
}
impl Host for TestHost {
    fn entropy(&self, buf: &mut [u8]) {
        buf.fill(self.fill);
    }
    fn now_unix_millis(&self) -> u64 {
        self.millis
    }
}

#[test]
fn from_bytes_roundtrips() {
    let mut b = [0u8; 16];
    for (i, x) in b.iter_mut().enumerate() {
        *x = i as u8;
    }
    assert_eq!(ClientId::from_bytes(b).as_bytes(), b);
}

// Distinguished by the FULL 16 bytes, not a prefix.
#[test]
fn distinguished_by_full_bytes() {
    let mut a = [7u8; 16];
    let mut b = [7u8; 16];
    a[15] = 1;
    b[15] = 2;
    assert_ne!(ClientId::from_bytes(a), ClientId::from_bytes(b));
}

#[test]
fn single_byte_helper_distinguishes() {
    assert_ne!(cid(1), cid(2));
    assert_eq!(cid(3), cid(3));
}

#[test]
fn generate_is_v7() {
    let host = TestHost {
        fill: 0xAB,
        millis: 0x0123_4567_89AB,
    };
    let bytes = ClientId::generate(&host).as_bytes();
    // version nibble (high nibble of byte 6) == 7
    assert_eq!(bytes[6] >> 4, 0x7);
    // variant bits (top two of byte 8) == 0b10
    assert_eq!(bytes[8] >> 6, 0b10);
}

#[test]
fn generate_timestamp_is_big_endian_prefix() {
    let host = TestHost {
        fill: 0,
        millis: 0x0000_0102_0304_0506 & 0xFFFF_FFFF_FFFF, // 48-bit ms
    };
    let bytes = ClientId::generate(&host).as_bytes();
    // first 48 bits are the big-endian millisecond timestamp
    let ts = ((host.millis) & 0xFFFF_FFFF_FFFF).to_be_bytes();
    assert_eq!(&bytes[0..6], &ts[2..8]);
}

// --- per-channel replica identities ---

#[test]
fn for_channel_distinguishes_channels() {
    let ids: Vec<ClientId> = (0..64u32).map(|c| cid(1).for_channel(c)).collect();
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn for_channel_distinguishes_sessions() {
    assert_ne!(cid(1).for_channel(1), cid(2).for_channel(1));
}

// Channel 0 is the replica the connection declared itself to be, so a session's
// first subscription authors under the id its embedder chose.
#[test]
fn channel_zero_keeps_the_declared_id() {
    assert_eq!(cid(1).for_channel(0), cid(1));
}

// Every further channel is its own replica, never the declared one.
#[test]
fn a_further_channel_is_never_the_declared_id() {
    for channel in 1..64u32 {
        assert_ne!(cid(1).for_channel(channel), cid(1));
    }
}

// A channel beyond the first is v5, so it never equals a `generate`d (v7) id.
// Channel 0 is excluded by construction — it *is* the declared id.
#[test]
fn a_derived_channel_id_is_v5() {
    for channel in 1..64u32 {
        let bytes = cid(1).for_channel(channel).as_bytes();
        assert_eq!(bytes[6] >> 4, 0x5);
        assert_eq!(bytes[8] >> 6, 0b10);
    }
}

// Every byte of the channel number is distinguishing, not just the low one a
// narrower hash would see. Byte *order* is not pinned here — these inputs stay
// pairwise distinct under either endianness; `the_derivation_is_a_fixed_wire_constant`
// is what fixes that.
#[test]
fn for_channel_distinguishes_every_byte_of_the_channel_number() {
    let ids: Vec<ClientId> = [1u32, 1 << 8, 1 << 16, 1 << 24, u32::MAX, u32::MAX - 1]
        .iter()
        .map(|c| cid(1).for_channel(*c))
        .collect();
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

// The derivation is protocol: a server re-derives what a client mints, so its
// namespace, byte width and endianness are fixed. Changing any of them rebinds
// every deployed client's identity, which this vector is here to make loud.
#[test]
fn the_derivation_is_a_fixed_wire_constant() {
    assert_eq!(
        cid(1).for_channel(1).as_bytes(),
        [236, 78, 246, 112, 183, 111, 95, 121, 140, 97, 77, 149, 29, 141, 248, 4]
    );
    assert_eq!(
        cid(1).for_channel(u32::MAX).as_bytes(),
        [96, 188, 86, 20, 49, 11, 90, 169, 186, 193, 57, 40, 3, 125, 5, 155]
    );
}

#[test]
fn generate_distinct_entropy_distinct_id() {
    let a = ClientId::generate(&TestHost {
        fill: 0x11,
        millis: 1,
    });
    let b = ClientId::generate(&TestHost {
        fill: 0x22,
        millis: 1,
    });
    assert_ne!(a, b);
}
