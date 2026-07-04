//! The access log — every authorization decision is recorded at the seam every
//! enforcement point consults.
//!
//! An [`Audited`] authorizer decorates an inner policy: it forwards the verdict
//! unchanged and hands each decision to a pluggable [`AccessLog`] sink. The
//! record carries the actor, the attempted action, the resource, and the
//! verdict — never the credential that authenticated the actor, and never an
//! awareness entry's key/value (an awareness publish logs only that a publish
//! was attempted and how it was decided).

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Message};
use crdtsync_server::acl::{Acl, ResourceMatch, Subject};
use crdtsync_server::audit::{AccessLog, AccessRecord, Audited, Decision};
use crdtsync_server::{Action, Authorizer, Registry, Resource};

const ROOM: &[u8] = b"room-a";

/// One recorded decision, owned so a sink can retain it past the borrow.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Entry {
    actor: Vec<u8>,
    action: Action,
    room: Vec<u8>,
    decision: Decision,
}

/// A sink that retains every record, for assertions.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Entry>>>);

impl Recorder {
    fn entries(&self) -> Vec<Entry> {
        self.0.lock().unwrap().clone()
    }
}

impl AccessLog for Recorder {
    fn record(&self, record: &AccessRecord) {
        // This recorder only tracks room decisions; app-scoped ones are skipped.
        let Resource::Room(room) = *record.resource else {
            return;
        };
        self.0.lock().unwrap().push(Entry {
            actor: record.actor.to_vec(),
            action: record.action,
            room: room.to_vec(),
            decision: record.decision,
        });
    }
}

/// An authorizer that permits only reads, to drive both verdicts.
fn read_only() -> Box<dyn Authorizer> {
    Box::new(|_actor: &[u8], action: Action, _res: &Resource| action == Action::Read)
}

#[test]
fn a_permitted_decision_is_recorded() {
    let rec = Recorder::default();
    let audited = Audited::new(read_only(), Box::new(rec.clone()));
    assert!(audited.authorize(b"alice", Action::Read, &Resource::Room(ROOM)));
    assert_eq!(
        rec.entries(),
        vec![Entry {
            actor: b"alice".to_vec(),
            action: Action::Read,
            room: ROOM.to_vec(),
            decision: Decision::Permitted,
        }]
    );
}

#[test]
fn a_denied_decision_is_recorded() {
    let rec = Recorder::default();
    let audited = Audited::new(read_only(), Box::new(rec.clone()));
    assert!(!audited.authorize(b"alice", Action::Write, &Resource::Room(ROOM)));
    assert_eq!(
        rec.entries(),
        vec![Entry {
            actor: b"alice".to_vec(),
            action: Action::Write,
            room: ROOM.to_vec(),
            decision: Decision::Denied,
        }]
    );
}

#[test]
fn the_inner_verdict_is_forwarded_unchanged() {
    let rec = Recorder::default();
    let audited = Audited::new(read_only(), Box::new(rec.clone()));
    assert!(audited.authorize(b"alice", Action::Read, &Resource::Room(ROOM)));
    assert!(!audited.authorize(b"alice", Action::PublishAwareness, &Resource::Room(ROOM)));
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// Wrapping the live authorizer records the real enforcement points: a permitted
/// subscribe and a denied one both land in the log, verdicts intact.
#[test]
fn wrapping_the_registry_authorizer_logs_enforcement() {
    let rec = Recorder::default();
    // Only "open" is readable.
    let policy: Box<dyn Authorizer> = Box::new(Acl::new().allow(
        Subject::Anyone,
        Some(Action::Read),
        ResourceMatch::Room(b"open".to_vec()),
    ));
    let mut r = Registry::new(cid(0xFF));
    r.set_authorizer(Box::new(Audited::new(policy, Box::new(rec.clone()))));

    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"actor-1".to_vec(),
        }
    ));
    r.take_outbox(id);

    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: b"open".to_vec(),
            last_seen_seq: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(1),
            room: b"secret".to_vec(),
            last_seen_seq: 0,
        }
    ));

    assert_eq!(
        rec.entries(),
        vec![
            Entry {
                actor: b"actor-1".to_vec(),
                action: Action::Read,
                room: b"open".to_vec(),
                decision: Decision::Permitted,
            },
            Entry {
                actor: b"actor-1".to_vec(),
                action: Action::Read,
                room: b"secret".to_vec(),
                decision: Decision::Denied,
            },
        ]
    );
}

/// An awareness publish records the decision — actor, action, room, verdict —
/// and nothing of the entry's content (the sink never sees a key or value).
#[test]
fn an_awareness_publish_logs_the_decision_not_the_entry() {
    let rec = Recorder::default();
    let policy: Box<dyn Authorizer> =
        Box::new(Acl::new().allow(Subject::Anyone, None, ResourceMatch::AnyRoom));
    let mut r = Registry::new(cid(0xFF));
    r.set_authorizer(Box::new(Audited::new(policy, Box::new(rec.clone()))));

    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"actor-1".to_vec(),
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
    assert!(r.deliver(
        id,
        Message::AwarenessSet {
            channel: Channel(0),
            key: b"cursor".to_vec(),
            value: vec![1, 2, 3],
        }
    ));

    let publishes: Vec<Entry> = rec
        .entries()
        .into_iter()
        .filter(|e| e.action == Action::PublishAwareness)
        .collect();
    assert_eq!(
        publishes,
        vec![Entry {
            actor: b"actor-1".to_vec(),
            action: Action::PublishAwareness,
            room: ROOM.to_vec(),
            decision: Decision::Permitted,
        }],
        "the publish decision is logged, carrying no key or value"
    );
}
