//! The schema registry — the control-plane store an app's schema and migration
//! chain is registered into, and the handshake resolves a client's
//! `{app_id, version}` against.
//!
//! A schema is an app-developer artifact, never carried in a document: the app
//! owner's CI registers `{app_id, version, schema, migration}` here on release,
//! and a connecting client names only its `app_id` + version, which the server
//! resolves to the schema it holds. Each link is **hash-locked** by SHA-256 over
//! its content, so the chain is tamper-evident and immutable: the registry
//! appends the next contiguous version, no-ops an identical retry, and refuses a
//! gap, a backward version, or a content change under an already-registered
//! version. The crypto lives here in the server, not core — core stays
//! dependency-minimal and a client never hash-verifies the server it trusts.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

/// One registered link in an app's chain: the schema body, the migration edge
/// that reaches it (empty at version 1, which has no predecessor), and the
/// SHA-256 that locks both.
struct Link {
    schema: Vec<u8>,
    migration: Vec<u8>,
    hash: [u8; 32],
}

/// An app's schema chain. Version `n` is `links[n - 1]`; the chain is contiguous
/// from version 1, so its length is the head version.
#[derive(Default)]
struct Chain {
    links: Vec<Link>,
}

/// A per-`app_id` registry of hash-locked schema chains.
#[derive(Default)]
pub struct SchemaRegistry {
    apps: HashMap<Vec<u8>, Chain>,
}

/// The effect of a successful registration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Registered {
    /// A new version appended at the chain head.
    Appended,
    /// The head re-registered with identical content — an idempotent retry that
    /// left the chain unchanged.
    Unchanged,
}

/// Why a registration was refused. The chain stays hash-locked: contiguous from
/// version 1, every link immutable once registered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegisterError {
    /// The next version must be `expected`, but `got` skips ahead — a gap would
    /// leave the chain incomplete.
    Gap { expected: u32, got: u32 },
    /// `got` is behind the head (or zero) — a chain only moves forward; a
    /// superseded version cannot be re-registered.
    OutOfSequence { expected: u32, got: u32 },
    /// The head version was re-registered with different content — a link is
    /// immutable once locked.
    HashMismatch { version: u32 },
}

impl SchemaRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `version` of `app_id` with its `schema` body and the `migration`
    /// edge that reaches it (empty for version 1). Appends the next contiguous
    /// version; a re-push of the current head with identical content is an
    /// idempotent [`Unchanged`](Registered::Unchanged). Refuses a gap, a
    /// backward or zero version, or a content change under the head.
    pub fn register(
        &mut self,
        app_id: &[u8],
        version: u32,
        schema: &[u8],
        migration: &[u8],
    ) -> Result<Registered, RegisterError> {
        let head = self.apps.get(app_id).map_or(0, |c| c.links.len() as u32);
        let expected = head + 1;
        let hash = content_hash(version, schema, migration);
        if version == expected {
            self.apps
                .entry(app_id.to_vec())
                .or_default()
                .links
                .push(Link {
                    schema: schema.to_vec(),
                    migration: migration.to_vec(),
                    hash,
                });
            Ok(Registered::Appended)
        } else if version == head && head >= 1 {
            // A retry of the head: honoured only if it reproduces the lock.
            if self.apps[app_id].links[(version - 1) as usize].hash == hash {
                Ok(Registered::Unchanged)
            } else {
                Err(RegisterError::HashMismatch { version })
            }
        } else if version > expected {
            Err(RegisterError::Gap {
                expected,
                got: version,
            })
        } else {
            Err(RegisterError::OutOfSequence {
                expected,
                got: version,
            })
        }
    }

    /// The schema body registered under `app_id` at `version`, or `None` for an
    /// unknown app or a version outside its chain — the handshake's lookup, where
    /// an unknown version is a rejection, never a fabrication.
    pub fn resolve(&self, app_id: &[u8], version: u32) -> Option<&[u8]> {
        self.link(app_id, version).map(|l| l.schema.as_slice())
    }

    /// The migration edge that reaches `version` (empty at version 1), or `None`
    /// for an unknown app or version.
    pub fn migration(&self, app_id: &[u8], version: u32) -> Option<&[u8]> {
        self.link(app_id, version).map(|l| l.migration.as_slice())
    }

    /// The SHA-256 lock over `version`'s content, or `None` for an unknown app or
    /// version — the content hash a boot-time chain verification checks against.
    pub fn hash(&self, app_id: &[u8], version: u32) -> Option<[u8; 32]> {
        self.link(app_id, version).map(|l| l.hash)
    }

    /// The highest version registered for `app_id`, or `None` if it has none.
    pub fn head_version(&self, app_id: &[u8]) -> Option<u32> {
        match self.apps.get(app_id) {
            Some(c) if !c.links.is_empty() => Some(c.links.len() as u32),
            _ => None,
        }
    }

    fn link(&self, app_id: &[u8], version: u32) -> Option<&Link> {
        let chain = self.apps.get(app_id)?;
        let index = (version as usize).checked_sub(1)?;
        chain.links.get(index)
    }
}

/// The SHA-256 content lock for a link. The version, schema, and migration are
/// each length-framed so no boundary shift can collide two distinct links, and
/// the version is bound so identical bytes at two positions lock differently.
fn content_hash(version: u32, schema: &[u8], migration: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(version.to_be_bytes());
    h.update((schema.len() as u64).to_be_bytes());
    h.update(schema);
    h.update((migration.len() as u64).to_be_bytes());
    h.update(migration);
    h.finalize().into()
}
