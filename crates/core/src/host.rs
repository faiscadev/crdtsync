//! Host effect seam — the only platform dependency the core has.
//!
//! Covers the effects wasm cannot get from the OS directly: cryptographic
//! randomness and a wall clock (for UUIDv7 ClientIds).

pub trait Host {
    /// Fill `buf` with cryptographically suitable randomness.
    fn entropy(&self, buf: &mut [u8]);

    /// Current Unix time in milliseconds (for UUIDv7 timestamp ordering).
    fn now_unix_millis(&self) -> u64;
}
