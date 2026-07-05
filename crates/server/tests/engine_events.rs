//! The engine event bus — lifecycle moments fanned out to registered sinks.
//!
//! The engine emits one [`EngineEvent`] stream: connections and subscribes from
//! the registry, version create/delete and compaction from the hub. A sink is a
//! passive observer — it never alters behavior — so a no-sink engine runs exactly
//! as before, and several sinks each see every event, in order. The reserved
//! variants (branch/migration) are declarable now and fired by a later layer.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Scalar};
use crdtsync_server::{ConnId, EngineEvent, EventSink, Hub, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-1";

/// An owned snapshot of an [`EngineEvent`], so a recording sink can retain the
/// stream for order assertions.
#[derive(Clone, PartialEq, Debug)]
enum Ev {
    Connected(ConnId),
    Subscribed(ConnId, Vec<u8>),
    VersionCreated(Vec<u8>, Vec<u8>),
    VersionDeleted(Vec<u8>, Vec<u8>),
    Compacted(Vec<u8>, u64),
    Reserved,
}

fn snapshot(e: &EngineEvent) -> Ev {
    match e {
        EngineEvent::Connected { conn } => Ev::Connected(*conn),
        EngineEvent::Subscribed { conn, room } => Ev::Subscribed(*conn, room.to_vec()),
        EngineEvent::VersionCreated { room, name } => {
            Ev::VersionCreated(room.to_vec(), name.to_vec())
        }
        EngineEvent::VersionDeleted { room, name } => {
            Ev::VersionDeleted(room.to_vec(), name.to_vec())
        }
        EngineEvent::Compacted { room, floor } => Ev::Compacted(room.to_vec(), *floor),
        _ => Ev::Reserved,
    }
}

/// A shared log a [`Recorder`] appends every event to.
type Log = Arc<Mutex<Vec<Ev>>>;

struct Recorder(Log);

impl EventSink for Recorder {
    fn on_event(&self, event: &EngineEvent) {
        self.0.lock().unwrap().push(snapshot(event));
    }
}

/// A recording sink plus a handle to the log it writes.
fn recording() -> (Log, Box<dyn EventSink>) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    (Arc::clone(&log), Box::new(Recorder(log)))
}

/// Create `ROOM` with a single register op.
fn populate(h: &mut Hub) {
    let mut a = doc(1);
    h.ingest(
        ROOM,
        a.transact(|tx| tx.register(b"a", Scalar::Int(1))),
        None,
    )
    .unwrap();
}

// --- hub lifecycle ---

#[test]
fn the_hub_emits_version_and_compaction_events_in_order() {
    let mut h = hub();
    let (log, sink) = recording();
    h.add_event_sink(sink);
    populate(&mut h);
    h.create_version(ROOM, b"v1").unwrap();
    h.delete_version(ROOM, b"v1").unwrap();
    h.compact(ROOM).unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            Ev::VersionCreated(ROOM.to_vec(), b"v1".to_vec()),
            Ev::VersionDeleted(ROOM.to_vec(), b"v1".to_vec()),
            // The one ingested op folds into the snapshot: the floor advances to 1.
            Ev::Compacted(ROOM.to_vec(), 1),
        ]
    );
}

#[test]
fn a_no_op_version_change_emits_nothing() {
    let mut h = hub();
    let (log, sink) = recording();
    h.add_event_sink(sink);
    // Unknown room → captured nothing → no event.
    assert!(!h.create_version(b"ghost", b"v1").unwrap());
    populate(&mut h);
    assert!(h.create_version(ROOM, b"v1").unwrap());
    // A duplicate name → removed/created nothing → no second event.
    assert!(!h.create_version(ROOM, b"v1").unwrap());
    // Deleting an absent version → nothing removed → no event.
    assert!(!h.delete_version(ROOM, b"absent").unwrap());
    assert_eq!(
        *log.lock().unwrap(),
        vec![Ev::VersionCreated(ROOM.to_vec(), b"v1".to_vec())]
    );
}

#[test]
fn a_hub_with_no_sink_behaves_normally() {
    let mut h = hub();
    populate(&mut h);
    assert!(h.create_version(ROOM, b"v1").unwrap());
    assert!(h.delete_version(ROOM, b"v1").unwrap());
    h.compact(ROOM).unwrap();
    // Emission with no sink is a no-op; every operation still takes effect.
    assert_eq!(h.seq(ROOM), 1);
}

#[test]
fn every_sink_receives_every_event() {
    let mut h = hub();
    let (log1, sink1) = recording();
    let (log2, sink2) = recording();
    h.add_event_sink(sink1);
    h.add_event_sink(sink2);
    populate(&mut h);
    h.create_version(ROOM, b"v1").unwrap();
    let expected = vec![Ev::VersionCreated(ROOM.to_vec(), b"v1".to_vec())];
    assert_eq!(*log1.lock().unwrap(), expected);
    assert_eq!(*log2.lock().unwrap(), expected);
}

// --- registry lifecycle ---

#[test]
fn the_registry_emits_connect_then_subscribe() {
    let mut r = Registry::new(cid(0xFF));
    let (log, sink) = recording();
    r.add_event_sink(sink);
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: b"app".to_vec(),
            schema_version: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"actor".to_vec(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        }
    ));
    // Connect emits at accept; a Hello/Auth is silent; an accepted Subscribe emits.
    assert_eq!(
        *log.lock().unwrap(),
        vec![Ev::Connected(id), Ev::Subscribed(id, ROOM.to_vec())]
    );
}

// --- reserved variants ---

#[test]
fn reserved_events_are_declarable_but_never_emitted() {
    // A reserved variant exists in the enum for a later layer to emit — it can be
    // constructed and handed to a sink now.
    let (log, sink) = recording();
    sink.on_event(&EngineEvent::BeforePublish {
        room: ROOM,
        branch: b"feature",
    });
    sink.on_event(&EngineEvent::AfterRestore {
        room: ROOM,
        branch: b"feature",
    });
    sink.on_event(&EngineEvent::BeforeMigration {
        room: ROOM,
        to_version: 2,
    });
    assert_eq!(
        *log.lock().unwrap(),
        vec![Ev::Reserved, Ev::Reserved, Ev::Reserved]
    );

    // But the engine's own lifecycle paths never emit one.
    let mut h = hub();
    let (log2, sink2) = recording();
    h.add_event_sink(sink2);
    populate(&mut h);
    h.create_version(ROOM, b"v1").unwrap();
    h.compact(ROOM).unwrap();
    assert!(!log2.lock().unwrap().iter().any(|e| *e == Ev::Reserved));
}
