//! Cross-zone capability tokens — the AEAD-sealed escape hatch for an authorized
//! cross-zone tree move.
//!
//! A cross-zone move is rejected at op-ingress by default (Zones 1b-ii-b): the
//! per-zone lamport clocks never order across zones. Zones-4 is the one authorized
//! bypass — a server-sealed capability token that authorizes exactly **one**
//! cross-zone move and cannot be forged, reused for a different move, or read by a
//! client.
//!
//! The token seals a [`CrossZoneGrant`] — the binding tuple
//! `(room, actor, element, src_zone, dst_zone, expiry)` — under the server's
//! zone-master key with **ChaCha20-Poly1305** (RustCrypto, constant-time in
//! software, no AES-NI dependency). The whole binding is the AEAD plaintext, so the
//! Poly1305 tag authenticates every field: a client cannot forge a token, and a
//! token minted for one `(actor, element, src, dst)` in one room cannot redeem a
//! different move. A fresh random 96-bit nonce is drawn per seal — never a fixed
//! nonce — and prepended to the ciphertext, so two seals of the same binding differ
//! and never reuse a `(key, nonce)` pair. [`open`](ZoneSealer::open) is total and
//! **fail-closed**: a bad tag, a truncated token, or trailing bytes yield `None`, so
//! a garbage or tampered token is rejected exactly as an absent one.
//!
//! The zone key is server config (like the TLS cert), never leaving the server; the
//! token is opaque bytes to the client, which only relays it back to redeem the move.

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

use crdtsync_core::ElementId;

/// The AEAD nonce width for ChaCha20-Poly1305 — 96 bits.
const NONCE_LEN: usize = 12;

/// The binding a cross-zone token authorizes: exactly one move of `element` by
/// `actor`, out of `src_zone` into `dst_zone`, in `room`, valid until `expiry`. A
/// zone name is the schema-declared zone name; the empty string is the unzoned root
/// partition. `expiry` is an absolute wall-clock time in the same millisecond unit
/// the session's `now` carries.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CrossZoneGrant {
    pub room: Vec<u8>,
    pub actor: Vec<u8>,
    pub element: ElementId,
    pub src_zone: Vec<u8>,
    pub dst_zone: Vec<u8>,
    pub expiry: u64,
}

impl CrossZoneGrant {
    /// Whether this grant is still valid at `now` — its expiry has not passed.
    pub fn is_live(&self, now: u64) -> bool {
        now <= self.expiry
    }

    /// Whether this grant authorizes exactly the move `(room, actor, element,
    /// src_zone, dst_zone)` — every bound field matching. The redemption check: a
    /// token authorizes a crossing only when all five bindings match the op's actual
    /// crossing (expiry checked separately by [`is_live`](Self::is_live)).
    pub fn authorizes(
        &self,
        room: &[u8],
        actor: &[u8],
        element: ElementId,
        src_zone: &[u8],
        dst_zone: &[u8],
    ) -> bool {
        self.room == room
            && self.actor == actor
            && self.element == element
            && self.src_zone == src_zone
            && self.dst_zone == dst_zone
    }
}

/// The server's cross-zone token sealer, holding the 32-byte zone-master key. One
/// per server, built from config; the key never leaves it.
#[derive(Clone)]
pub struct ZoneSealer {
    cipher: ChaCha20Poly1305,
}

impl ZoneSealer {
    /// A sealer over the 32-byte zone-master key.
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(Key::from_slice(&key)),
        }
    }

    /// Seal `grant` into an opaque token: a fresh random nonce prepended to the
    /// AEAD ciphertext of the encoded binding. The whole binding is authenticated,
    /// so the token is unforgeable and non-transferable to a different move. Two
    /// seals of the same grant differ (fresh nonce), never reusing a `(key, nonce)`
    /// pair.
    pub fn seal(&self, grant: &CrossZoneGrant) -> Vec<u8> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        getrandom::getrandom(&mut nonce_bytes).expect("system entropy is available");
        let nonce = Nonce::from_slice(&nonce_bytes);
        let plaintext = encode_grant(grant);
        // ChaCha20-Poly1305 with a fresh 96-bit nonce cannot fail on a well-formed
        // plaintext, so a seal error is a programming fault, not a runtime one.
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext.as_slice())
            .expect("chacha20poly1305 seal");
        let mut token = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        token.extend_from_slice(&nonce_bytes);
        token.extend_from_slice(&ciphertext);
        token
    }

    /// Open and authenticate an opaque `token`, recovering its sealed
    /// [`CrossZoneGrant`]. Total and fail-closed: a token too short to hold a nonce
    /// and a tag, a bad authentication tag (forged or tampered), or a payload that
    /// does not decode back into a binding all yield `None` — an unforgeable,
    /// unambiguous reject.
    pub fn open(&self, token: &[u8]) -> Option<CrossZoneGrant> {
        if token.len() < NONCE_LEN {
            return None;
        }
        let (nonce_bytes, ciphertext) = token.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = self.cipher.decrypt(nonce, ciphertext).ok()?;
        decode_grant(&plaintext)
    }
}

/// Encode a grant to its deterministic byte string: length-prefixed byte fields for
/// the variable-length `room`/`actor`/zone names, the element's 16 raw bytes, and
/// the `expiry` as 8 little-endian bytes. Only ever the AEAD plaintext, never a wire
/// or stored form, so it needs no version tag.
fn encode_grant(grant: &CrossZoneGrant) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, &grant.room);
    put_bytes(&mut out, &grant.actor);
    out.extend_from_slice(&grant.element.as_bytes());
    put_bytes(&mut out, &grant.src_zone);
    put_bytes(&mut out, &grant.dst_zone);
    out.extend_from_slice(&grant.expiry.to_le_bytes());
    out
}

/// Decode a grant from the plaintext [`encode_grant`] produced. Total — any
/// truncation or trailing byte yields `None`, so a tag that authenticated but did
/// not encode a well-formed binding (impossible under this one sealer, but never
/// assumed) is rejected rather than trusted.
fn decode_grant(bytes: &[u8]) -> Option<CrossZoneGrant> {
    let mut cur = Reader { buf: bytes, pos: 0 };
    let room = cur.bytes()?;
    let actor = cur.bytes()?;
    let element = ElementId::from_bytes(cur.array16()?);
    let src_zone = cur.bytes()?;
    let dst_zone = cur.bytes()?;
    let expiry = u64::from_le_bytes(cur.array8()?);
    if cur.pos != bytes.len() {
        return None;
    }
    Some(CrossZoneGrant {
        room,
        actor,
        element,
        src_zone,
        dst_zone,
        expiry,
    })
}

/// Append a `u32` length prefix and then the bytes.
fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// A minimal cursor over the AEAD plaintext, total on truncation.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = u32::from_le_bytes(self.array4()?) as usize;
        Some(self.take(len)?.to_vec())
    }

    fn array4(&mut self) -> Option<[u8; 4]> {
        self.take(4)?.try_into().ok()
    }

    fn array8(&mut self) -> Option<[u8; 8]> {
        self.take(8)?.try_into().ok()
    }

    fn array16(&mut self) -> Option<[u8; 16]> {
        self.take(16)?.try_into().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant() -> CrossZoneGrant {
        CrossZoneGrant {
            room: b"room-1".to_vec(),
            actor: b"alice".to_vec(),
            element: ElementId::from_bytes([7u8; 16]),
            src_zone: b"za".to_vec(),
            dst_zone: b"zb".to_vec(),
            expiry: 30_000,
        }
    }

    #[test]
    fn a_sealed_token_opens_to_its_grant() {
        let sealer = ZoneSealer::new([1u8; 32]);
        let token = sealer.seal(&grant());
        assert_eq!(sealer.open(&token), Some(grant()));
    }

    #[test]
    fn two_seals_of_one_grant_differ_but_both_open() {
        // A fresh nonce per seal — the tokens differ, never reusing (key, nonce).
        let sealer = ZoneSealer::new([2u8; 32]);
        let a = sealer.seal(&grant());
        let b = sealer.seal(&grant());
        assert_ne!(a, b, "a fresh nonce makes each token distinct");
        assert_eq!(sealer.open(&a), Some(grant()));
        assert_eq!(sealer.open(&b), Some(grant()));
    }

    #[test]
    fn a_token_from_a_different_key_fails_to_open() {
        let token = ZoneSealer::new([3u8; 32]).seal(&grant());
        assert_eq!(ZoneSealer::new([4u8; 32]).open(&token), None);
    }

    #[test]
    fn a_tampered_token_fails_to_open() {
        let sealer = ZoneSealer::new([5u8; 32]);
        let mut token = sealer.seal(&grant());
        let last = token.len() - 1;
        token[last] ^= 0xff;
        assert_eq!(sealer.open(&token), None);
    }

    #[test]
    fn a_truncated_or_garbage_token_fails_to_open_without_panicking() {
        let sealer = ZoneSealer::new([6u8; 32]);
        for len in 0..NONCE_LEN + 4 {
            assert_eq!(sealer.open(&vec![0u8; len]), None);
        }
        assert_eq!(sealer.open(b"garbage"), None);
    }

    #[test]
    fn authorizes_and_expiry_gate_the_binding() {
        let g = grant();
        assert!(g.authorizes(b"room-1", b"alice", g.element, b"za", b"zb"));
        assert!(!g.authorizes(b"room-2", b"alice", g.element, b"za", b"zb"));
        assert!(!g.authorizes(b"room-1", b"bob", g.element, b"za", b"zb"));
        assert!(!g.authorizes(
            b"room-1",
            b"alice",
            ElementId::from_bytes([9u8; 16]),
            b"za",
            b"zb"
        ));
        assert!(!g.authorizes(b"room-1", b"alice", g.element, b"zc", b"zb"));
        assert!(!g.authorizes(b"room-1", b"alice", g.element, b"za", b"zc"));
        assert!(g.is_live(30_000));
        assert!(!g.is_live(30_001));
    }
}
