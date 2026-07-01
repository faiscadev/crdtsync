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
