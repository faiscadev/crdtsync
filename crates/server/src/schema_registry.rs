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
//!
//! A registered body is also **parsed** before it is stored, and its `zones` block
//! is held to an append-only rule against its predecessor's. Both are about what a
//! stored version means downstream rather than about the chain's shape: an
//! unparseable body resolves to no schema at all, which reads at every zone seam as
//! "this room declares no partitions" and serves a zone-limited reader the whole
//! room; and a zone id is a *position* in the declaration order, so reordering or
//! removing a zone re-points every id already stamped into the log — and every
//! subscription resolved against the block — at a different partition. The registry
//! is where both are cheap to refuse and expensive to detect anywhere else.

use std::collections::HashMap;

use crdtsync_core::schema::Schema;
use sha2::{Digest, Sha256};

/// One registered link in an app's chain: the schema body, the migration edge
/// that reaches it (none at version 1 — it has no predecessor to migrate from),
/// the SHA-256 that locks both, and the zone block the body declares.
struct Link {
    schema: Vec<u8>,
    migration: Vec<u8>,
    hash: [u8; 32],
    /// The zones this version declares, in declaration order — the position→region
    /// map every reader of an op's `zone` resolves through, taken from the parse that
    /// admitted the body and read by the next version's append-only check.
    zones: Vec<(String, String)>,
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

/// The tier a connection resolves to when a client names its `{app_id, version}`
/// at the handshake.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Resolution {
    /// No app, or an app that never registered a schema: served with no schema
    /// enforcement.
    Relay,
    /// A registered app pinned to `version` — a declared known version, or the
    /// head adopted by a version-0 dynamic client — carrying `schema`, its
    /// registered bytes, so the caller advertises them without a second lookup.
    Enforcing { version: u32, schema: Vec<u8> },
    /// A registered app for which the client declared a version the registry does
    /// not hold: refused, not fabricated.
    Reject,
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
    /// The body is not a schema this server can parse. Storing it would leave the
    /// version resolving to *no* schema, which every zone seam reads as a room with
    /// no partitions and serves whole.
    Unparseable { version: u32 },
    /// The `zones` block is not an append-only extension of its predecessor's — a
    /// zone was reordered, renamed, re-rooted, or removed. A zone id is the zone's
    /// position in that block, so any of those re-points ids already stamped into
    /// the room's log, and every scope resolved against the old block, at a
    /// different partition. Retiring a zone means leaving its declaration in place.
    ZonesNotAppendOnly { version: u32 },
}

impl SchemaRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `version` of `app_id` with its `schema` body and the `migration`
    /// edge that reaches it (there is none to supply at version 1). Appends the next contiguous
    /// version; a re-push of the current head with identical content is an
    /// idempotent [`Unchanged`](Registered::Unchanged). Refuses a gap, a
    /// backward or zero version, or a content change under the head.
    ///
    /// A body that does not parse is refused, and so is one whose `zones` block is
    /// not an append-only extension of the predecessor's: both would be stored
    /// facts the rest of the server cannot read the way the version's author meant
    /// them. An idempotent retry of the head reproduces content that already passed
    /// both, so it is checked on the lock alone.
    pub fn register(
        &mut self,
        app_id: &[u8],
        version: u32,
        schema: &[u8],
        migration: &[u8],
    ) -> Result<Registered, RegisterError> {
        let head = self.apps.get(app_id).map_or(0, |c| c.links.len() as u32);
        let expected = head + 1;
        // The hash is computed only where a version is accepted — a gap or a
        // backward version is rejected without hashing a payload it discards.
        if version == expected {
            let Some(parsed) = parse_schema(schema) else {
                return Err(RegisterError::Unparseable { version });
            };
            let zones: Vec<(String, String)> = parsed.zones().to_vec();
            // The predecessor's block is a prefix of this one, or the ids the log
            // already carries change meaning. Version 1 has no predecessor and so
            // declares whatever it likes.
            let extends = self
                .link(app_id, version - 1)
                .is_none_or(|prev| zones.get(..prev.zones.len()) == Some(&prev.zones));
            if !extends {
                return Err(RegisterError::ZonesNotAppendOnly { version });
            }
            let hash = content_hash(version, schema, migration);
            self.apps
                .entry(app_id.to_vec())
                .or_default()
                .links
                .push(Link {
                    schema: schema.to_vec(),
                    migration: migration.to_vec(),
                    hash,
                    zones,
                });
            Ok(Registered::Appended)
        } else if version == head && head >= 1 {
            // A retry of the head: honoured only if it reproduces the lock.
            let hash = content_hash(version, schema, migration);
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

    /// Resolve a client's handshake declaration to its tier. An empty `app_id`,
    /// or an app that never registered, is a [`Relay`](Resolution::Relay). A
    /// registered app pins the connection to a version: `version` `0` is a
    /// dynamic client that adopts the chain head, a declared version the registry
    /// holds is [`Enforcing`](Resolution::Enforcing) at that version, and a
    /// declared version it does not hold is a [`Reject`](Resolution::Reject).
    pub fn resolve_handshake(&self, app_id: &[u8], version: u32) -> Resolution {
        if app_id.is_empty() {
            return Resolution::Relay;
        }
        let Some(head) = self.head_version(app_id) else {
            return Resolution::Relay;
        };
        let version = if version == 0 { head } else { version };
        match self.resolve(app_id, version) {
            Some(schema) => Resolution::Enforcing {
                version,
                schema: schema.to_vec(),
            },
            None => Resolution::Reject,
        }
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

/// The schema a registered body denotes, or `None` for one this server cannot read
/// as a schema at all — invalid UTF-8 or text the parser refuses.
fn parse_schema(body: &[u8]) -> Option<Schema> {
    Schema::parse(std::str::from_utf8(body).ok()?).ok()
}

/// The SHA-256 content lock for a link. The schema and migration are each
/// length-framed so no boundary shift can collide two distinct links, and the
/// fixed-width version prefix binds the position so identical bytes at two
/// versions lock differently.
fn content_hash(version: u32, schema: &[u8], migration: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(version.to_be_bytes());
    h.update((schema.len() as u64).to_be_bytes());
    h.update(schema);
    h.update((migration.len() as u64).to_be_bytes());
    h.update(migration);
    h.finalize().into()
}
