//! The engine event bus — a record of the engine's lifecycle moments.
//!
//! Several server concerns want to observe *what the engine did* rather than
//! *what an actor was allowed to do* (the access log's axis): auto-version fires
//! on a matching lifecycle event, a webhook relays one outward, a debugger
//! replays the sequence. Rather than each grow its own hook, the engine emits one
//! [`EngineEvent`] stream to every registered [`EventSink`]. A sink is a passive
//! observer — it never alters the engine's behavior, only watches — so emission
//! is a pure fan-out the engine performs after the moment has committed.
//!
//! An event borrows its context; a sink that retains records copies what it
//! needs. The variants a later layer will emit — a branch publish, a migration —
//! are declared here now and left unfired, so that layer routes through the one
//! seam without an enum break.

use crate::registry::ConnId;

/// A lifecycle moment the engine emits to every registered [`EventSink`], after
/// the moment has committed. Borrows its context.
pub enum EngineEvent<'a> {
    /// A connection was opened.
    Connected { conn: ConnId },
    /// A connection was closed — the counterpart to [`Connected`](Self::Connected),
    /// so a sink pairing the two stays balanced.
    Disconnected { conn: ConnId },
    /// A connection's subscribe to `room` was accepted (the room was not already
    /// subscribed on this connection).
    Subscribed { conn: ConnId, room: &'a [u8] },
    /// A named version of `room` was captured.
    VersionCreated { room: &'a [u8], name: &'a [u8] },
    /// A named version of `room` was renamed from `from` to `to`.
    VersionRenamed {
        room: &'a [u8],
        from: &'a [u8],
        to: &'a [u8],
    },
    /// A named version of `room` was removed.
    VersionDeleted { room: &'a [u8], name: &'a [u8] },
    /// `room` was compacted, advancing its retained-log floor to `floor`.
    Compacted { room: &'a [u8], floor: u64 },

    // Reserved — declared for the layers that will emit them (branches,
    // migration), never fired by this unit. Routing a new lifecycle point through
    // the one seam then needs no enum break at its call sites.
    /// Reserved: a branch is about to publish onto its base.
    BeforePublish { room: &'a [u8], branch: &'a [u8] },
    /// Reserved: a version was restored as a branch.
    AfterRestore { room: &'a [u8], branch: &'a [u8] },
    /// Reserved: a room is about to migrate to a new schema version.
    BeforeMigration { room: &'a [u8], to_version: u32 },
}

/// A sink for engine events. A deployment plugs in its own — an auto-version
/// trigger, a webhook relay, a metrics pipeline; the engine only emits. Several
/// may be registered; each sees every event.
pub trait EventSink {
    fn on_event(&self, event: &EngineEvent);
}

/// An event sink from a plain closure, so a deployment (or a test) can supply the
/// sink inline.
impl<F> EventSink for F
where
    F: Fn(&EngineEvent),
{
    fn on_event(&self, event: &EngineEvent) {
        self(event)
    }
}
