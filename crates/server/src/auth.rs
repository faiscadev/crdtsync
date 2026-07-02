//! Credential verification at the handshake.
//!
//! The engine ships no identity provider. A [`Verifier`] is the deployment's
//! seam: it turns the opaque credential a client presents at Auth into the
//! actor id the server trusts for that connection, or rejects it. Apps bring
//! their own (JWT, OIDC, custom); the engine only calls `verify` and never lets
//! the client name its own actor.

/// Turns a presented credential into a server-trusted actor id, or rejects it.
pub trait Verifier {
    /// The actor id for `credential`, or `None` to refuse the connection. The
    /// bytes are opaque; an implementation interprets them (a signed token, an
    /// API key) and derives the actor itself — it must not echo attacker-chosen
    /// bytes back as identity in production.
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>>;
}

/// A verifier from a plain closure, so a deployment (or a test) can supply the
/// mapping inline.
impl<F> Verifier for F
where
    F: Fn(&[u8]) -> Option<Vec<u8>>,
{
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>> {
        self(credential)
    }
}

/// Dev-mode verifier: accepts any credential and adopts it verbatim as the
/// actor id. It performs no real validation and lets the client pick its own
/// actor — for local development and tests only, never production.
pub struct AllowAll;

impl Verifier for AllowAll {
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>> {
        Some(credential.to_vec())
    }
}
