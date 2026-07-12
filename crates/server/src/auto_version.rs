//! Declarative auto-versioning — the first built-in [`EventSink`](crate::EventSink).
//!
//! A room's governing schema may declare `autoVersion` triggers (§Auto-Version
//! Triggers). On a matching lifecycle event the engine captures a named version of
//! that room. The capture cannot happen inside `on_event`: a sink observes after a
//! moment commits, and capturing a version mutates the hub the emit is running
//! over. So the sink is a passive recorder — it copies each room-bearing event
//! into a shared queue — and the registry drains that queue after the delivery,
//! resolving each room's schema, matching its triggers, and driving the capture.
//!
//! Recording is gated on an `armed` latch the registry raises the first time a
//! bound schema declares any trigger, so a deployment that uses no auto-versioning
//! pays nothing per event. Draining a capture itself emits `VersionCreated`; a
//! `draining` flag suppresses recording that, so an auto-created version never
//! cascades into another.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crdtsync_core::schema::TriggerEvent;

use crate::{EngineEvent, EventSink, RoomId};

/// A room-bearing lifecycle event, copied out for the registry to act on after the
/// delivery — the room it names and the trigger kind it matches.
pub(crate) type Signal = (RoomId, TriggerEvent);

/// Shared between the recording [`AutoVersionSink`] (in the hub's sink list) and
/// the registry that drains it. Single-threaded — the registry actor owns both
/// ends on one thread.
#[derive(Default)]
pub(crate) struct AutoVersionState {
    queue: RefCell<Vec<Signal>>,
    /// Raised once a bound schema declares any trigger; until then the sink records
    /// nothing, so a deployment with no `autoVersion` pays no per-event cost.
    armed: Cell<bool>,
    /// Set while the registry drains: the captures it drives re-emit version
    /// events, which the sink must not record, or they would cascade.
    draining: Cell<bool>,
}

impl AutoVersionState {
    /// Take the recorded signals, leaving the queue empty.
    pub(crate) fn take(&self) -> Vec<Signal> {
        std::mem::take(&mut self.queue.borrow_mut())
    }

    /// Whether any signals are queued.
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.borrow().is_empty()
    }

    /// Start recording room-bearing events — a bound schema declares a trigger.
    pub(crate) fn arm(&self) {
        self.armed.set(true);
    }

    /// Whether recording is armed.
    pub(crate) fn is_armed(&self) -> bool {
        self.armed.get()
    }

    /// Suppress or resume recording — set while draining so a capture's own events
    /// do not re-enter the queue.
    pub(crate) fn set_draining(&self, draining: bool) {
        self.draining.set(draining);
    }
}

/// The recording sink the hub holds: maps each room-bearing event to its trigger
/// kind and queues it. Roomless events (connect/disconnect) and the still-reserved
/// `before-migration` carry no room to version here, so they are ignored.
pub(crate) struct AutoVersionSink(pub(crate) Rc<AutoVersionState>);

impl EventSink for AutoVersionSink {
    fn on_event(&self, event: &EngineEvent) {
        if !self.0.armed.get() || self.0.draining.get() {
            return;
        }
        let signal = match *event {
            EngineEvent::Subscribed { room, .. } => (room.to_vec(), TriggerEvent::Subscribe),
            EngineEvent::VersionCreated { room, .. } => {
                (room.to_vec(), TriggerEvent::VersionCreated)
            }
            EngineEvent::VersionRenamed { room, .. } => {
                (room.to_vec(), TriggerEvent::VersionRenamed)
            }
            EngineEvent::VersionDeleted { room, .. } => {
                (room.to_vec(), TriggerEvent::VersionDeleted)
            }
            EngineEvent::Compacted { room, .. } => (room.to_vec(), TriggerEvent::Compaction),
            EngineEvent::AfterRestore { room, .. } => (room.to_vec(), TriggerEvent::AfterRestore),
            EngineEvent::BeforePublish { room, .. } => (room.to_vec(), TriggerEvent::BeforePublish),
            _ => return,
        };
        self.0.queue.borrow_mut().push(signal);
    }
}

/// Expand a version-name template at fire time: `${timestamp}` to a fixed-width
/// zero-padded millis stamp (so names sort chronologically) and `${event}` to the
/// event's kebab name. An unrecognized `${…}` is left verbatim.
pub(crate) fn expand_name(template: &str, now_millis: u64, event: TriggerEvent) -> String {
    template
        .replace("${timestamp}", &format!("{now_millis:020}"))
        .replace("${event}", event.as_kebab())
}

/// Expand a schedule trigger's name template. A schedule carries no event, so only
/// `${timestamp}` resolves; an `${event}` (or any other `${…}`) is left verbatim.
pub(crate) fn expand_schedule_name(template: &str, now_millis: u64) -> String {
    template.replace("${timestamp}", &format!("{now_millis:020}"))
}

/// A trigger's stable identity — the retention provenance tag stamped on every
/// version it captures. Its `(event, template)` pair, so two triggers that render
/// the same name under different events keep independent retention windows, while
/// two genuinely identical triggers share one. A NUL separates the fields, which
/// neither an event kebab nor (in practice) a template contains.
pub(crate) fn trigger_origin(event: TriggerEvent, template: &str) -> Vec<u8> {
    let mut origin = event.as_kebab().as_bytes().to_vec();
    origin.push(0);
    origin.extend_from_slice(template.as_bytes());
    origin
}

/// A schedule trigger's stable identity, keyed on `(interval, template)` so two
/// schedules that render the same name at different periods keep independent
/// retention windows. Its leading `every` field is no event kebab, so it never
/// collides with an [`trigger_origin`] identity even for the same template.
pub(crate) fn schedule_origin(interval_millis: u64, template: &str) -> Vec<u8> {
    let mut origin = b"every".to_vec();
    origin.push(0);
    origin.extend_from_slice(interval_millis.to_string().as_bytes());
    origin.push(0);
    origin.extend_from_slice(template.as_bytes());
    origin
}
