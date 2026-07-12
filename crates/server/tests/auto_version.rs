//! Declarative auto-versioning — the first built-in engine-event sink.
//!
//! A room's governing schema may declare `autoVersion` triggers: on a matching
//! lifecycle event ([`EngineEvent`](crdtsync_server::EngineEvent)), the engine
//! captures a named version of that room, expanding the name template
//! (`${timestamp}`, `${event}`) at fire time. Only room-bearing events drive a
//! capture (subscribe, version create/rename/delete, compaction); a relay room
//! with no governing schema never auto-versions; an `every:` schedule trigger is
//! the scheduler's job, not an event's. A trigger's `keep` prunes its own oldest
//! captures (by provenance, never a manual version or another trigger's) past the
//! window. A [`ManualClock`] drives `${timestamp}` deterministically.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::{ConnId, ManualClock, NoTimedTtl, Registry, SchemaRegistry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-a";
const APP: &[u8] = b"collab";
const CH: Channel = Channel(0);

fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

/// A schema of `APP` version 1 declaring `body` as its `autoVersion` array.
fn schema(body: &str) -> String {
    format!(
        r#"{{ "schema": "collab", "version": 1, "root": "R",
            "types": {{ "R": {{ "kind": "map" }} }},
            "autoVersion": {body} }}"#
    )
}

/// A registry whose shared schema registry holds `APP` version 1 with the given
/// `autoVersion` body, driven by a manual clock starting at 0.
fn registry_with(auto_version: &str) -> (Registry, Arc<ManualClock>) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, schema(auto_version).as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    (r, clock)
}

/// Hello + Auth a connection declaring `{APP, version}` — enforcing for a
/// registered app, relay for an empty id.
fn hello_auth(r: &mut Registry, client: u8, app_id: &[u8], version: u32) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app_id.to_vec(),
            schema_version: version,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: actor_of(client),
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId, room: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id);
}

/// Ingest a register write, so the room exists with state to capture.
fn write(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

fn a_write() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)))
}

fn version_names(r: &Registry) -> Vec<Vec<u8>> {
    r.hub().version_names(ROOM)
}

/// A 20-digit zero-padded millis stamp — how a name template's `${timestamp}`
/// renders, so the names sort chronologically.
fn stamp(millis: u64) -> String {
    format!("{millis:020}")
}

/// Bring `ROOM` into existence bound to `APP`: an enforcing subscribe (empty room,
/// captures nothing) then a write, so later subscribes have state to version.
fn seed_room(r: &mut Registry) -> ConnId {
    let a = hello_auth(r, 1, APP, 1);
    subscribe(r, a, ROOM);
    write(r, a, a_write());
    a
}

#[test]
fn a_subscribe_trigger_captures_a_version_on_join() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    seed_room(&mut r);

    clock.advance(1000);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(
        version_names(&r),
        vec![format!("auto/join/{}", stamp(1000)).into_bytes()],
        "the join captures exactly one version, named at the clock",
    );
}

#[test]
fn the_event_token_expands_to_the_kebab_event_name() {
    let (mut r, _clock) = registry_with(r#"[{ "on": "subscribe", "name": "auto/${event}" }]"#);
    seed_room(&mut r);

    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(version_names(&r), vec![b"auto/subscribe".to_vec()]);
}

#[test]
fn a_capture_on_an_empty_room_makes_no_version() {
    let (mut r, _clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    // The first subscriber joins a room with no state — the trigger fires but has
    // nothing to capture.
    let a = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, a, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn the_first_enforcing_subscriber_to_a_populated_room_captures() {
    // A relay client populates the room (no schema governs it yet), then the first
    // enforcing subscriber joins. Recording arms as its subscribe is authorized —
    // before its `Subscribed` fires — so that very first join captures, not only
    // the second. (A latch armed after the event would miss it.)
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    let relay = hello_auth(&mut r, 1, b"", 0);
    subscribe(&mut r, relay, ROOM);
    write(&mut r, relay, a_write());

    clock.advance(500);
    let enforcing = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, enforcing, ROOM);

    assert_eq!(
        version_names(&r),
        vec![format!("auto/join/{}", stamp(500)).into_bytes()],
    );
}

#[test]
fn a_relay_room_never_auto_versions() {
    let (mut r, _clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    // Both connect under an empty app id — no schema governs the room, so its
    // autoVersion triggers are never resolved.
    let a = hello_auth(&mut r, 1, b"", 0);
    subscribe(&mut r, a, ROOM);
    write(&mut r, a, a_write());
    let b = hello_auth(&mut r, 2, b"", 0);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn a_non_matching_event_does_not_fire() {
    let (mut r, _clock) =
        registry_with(r#"[{ "on": "compaction", "name": "auto/c/${timestamp}" }]"#);
    // Only compaction is declared; a subscribe must not capture.
    seed_room(&mut r);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn an_after_restore_trigger_captures_the_restored_state() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "after-restore", "name": "auto/restore/${timestamp}" }]"#);
    let a = seed_room(&mut r);
    // A version to restore from, past the seed write.
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());
    write(
        &mut r,
        a,
        doc(1).transact(|tx| tx.register(b"k2", Scalar::Int(2))),
    );

    clock.advance(1000);
    assert!(r.restore_as_branch(ROOM, b"v1", b"restored").unwrap());

    // The after-restore trigger fired once, capturing a version named at the clock,
    // beside `v1` and the restore's own audit version.
    let restore_capture = format!("auto/restore/{}", stamp(1000)).into_bytes();
    assert!(
        version_names(&r).contains(&restore_capture),
        "after-restore trigger captured a version: {:?}",
        version_names(&r),
    );
}

#[test]
fn a_compaction_event_captures_a_version() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "compaction", "name": "auto/c/${timestamp}" }]"#);
    r.set_compaction_threshold(1);
    let a = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, a, ROOM);

    clock.advance(500);
    // The write's ingest crosses the threshold and compacts, emitting Compacted.
    write(&mut r, a, a_write());

    assert_eq!(
        version_names(&r),
        vec![format!("auto/c/{}", stamp(500)).into_bytes()],
    );
}

#[test]
fn an_every_schedule_trigger_does_not_fire_on_an_event() {
    let (mut r, _clock) =
        registry_with(r#"[{ "every": "1h", "name": "auto/hourly/${timestamp}" }]"#);
    // A schedule trigger is the scheduler's concern; no lifecycle event fires it.
    seed_room(&mut r);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn a_schedule_does_not_capture_on_the_binding_sweep() {
    // The first sweep that sees a room's schedule arms it to `now`; it captures one
    // interval later, not the instant the room binds.
    let (mut r, _clock) = registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}" }]"#);
    seed_room(&mut r);
    r.sweep();
    assert!(version_names(&r).is_empty());
}

#[test]
fn a_schedule_trigger_captures_after_its_interval() {
    let (mut r, clock) = registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}" }]"#);
    seed_room(&mut r);
    r.sweep(); // arm at 0

    clock.advance(500);
    r.sweep(); // 500ms elapsed — under the window
    assert!(version_names(&r).is_empty());

    clock.advance(500);
    r.sweep(); // 1000ms elapsed — fires
    assert_eq!(
        version_names(&r),
        vec![format!("auto/tick/{}", stamp(1000)).into_bytes()],
        "the schedule captures once its interval has elapsed",
    );
}

#[test]
fn a_schedule_fires_at_most_once_per_sweep() {
    // A long gap (a paused or slow server) captures once on the next sweep, not one
    // per missed interval — no catch-up burst.
    let (mut r, clock) = registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}" }]"#);
    seed_room(&mut r);
    r.sweep(); // arm at 0

    clock.advance(5_000); // five intervals pass between sweeps
    r.sweep();

    assert_eq!(
        version_names(&r),
        vec![format!("auto/tick/{}", stamp(5000)).into_bytes()],
        "one capture for the whole gap, not five",
    );
}

#[test]
fn a_schedule_keep_prunes_its_captures() {
    let (mut r, clock) =
        registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}", "keep": 2 }]"#);
    seed_room(&mut r);
    r.sweep(); // arm at 0

    for _ in 0..3 {
        clock.advance(1_000);
        r.sweep();
    }

    assert_eq!(
        version_names(&r),
        vec![
            format!("auto/tick/{}", stamp(2000)).into_bytes(),
            format!("auto/tick/{}", stamp(3000)).into_bytes(),
        ],
        "keep:2 retains the two newest scheduled captures",
    );
}

#[test]
fn a_scheduled_capture_does_not_cascade_to_version_created() {
    // A schedule fires a version create, whose VersionCreated event a
    // version-created trigger would capture — the drain latch suppresses it, so a
    // sweep produces exactly the scheduled version.
    let (mut r, clock) = registry_with(
        r#"[{ "every": "1s", "name": "auto/tick/x" },
             { "on": "version-created", "name": "auto/vc/${timestamp}" }]"#,
    );
    seed_room(&mut r);
    r.sweep(); // arm at 0

    clock.advance(1_000);
    r.sweep();

    assert_eq!(
        version_names(&r),
        vec![b"auto/tick/x".to_vec()],
        "the scheduled capture does not re-fire the version-created trigger",
    );
}

#[test]
fn two_schedules_sharing_an_interval_and_name_apply_the_tighter_keep() {
    // Two schedules with the same interval and name render one version per sweep and
    // share a provenance group; the first stamps the fire time, but the second must
    // still be considered (not shadowed by that stamp), so the tighter `keep` wins.
    let (mut r, clock) = registry_with(
        r#"[{ "every": "1s", "name": "auto/tick/${timestamp}", "keep": 3 },
             { "every": "1s", "name": "auto/tick/${timestamp}", "keep": 1 }]"#,
    );
    seed_room(&mut r);
    r.sweep(); // arm at 0

    for _ in 0..3 {
        clock.advance(1_000);
        r.sweep();
    }

    assert_eq!(
        version_names(&r),
        vec![format!("auto/tick/{}", stamp(3000)).into_bytes()],
        "the keep:1 schedule prunes despite sharing the key with keep:3",
    );
}

#[test]
fn a_schedule_fires_under_an_injected_awareness_policy() {
    // An injected awareness policy makes the sweep skip resolving schemas for TTL
    // (`resolve_schema_policy`). The schedule pass must still resolve each bound
    // room's schema and fire on its own — it does not depend on the TTL path having
    // parsed the schema.
    let (mut r, clock) = registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}" }]"#);
    r.set_awareness_policy(Arc::new(NoTimedTtl));
    seed_room(&mut r);

    r.sweep(); // arm at 0
    clock.advance(1_000);
    r.sweep(); // fires

    assert_eq!(
        version_names(&r),
        vec![format!("auto/tick/{}", stamp(1000)).into_bytes()],
        "the schedule fires regardless of the awareness policy in force",
    );
}

#[test]
fn a_backward_clock_step_rearms_a_schedule() {
    // A backward wall-clock step (NTP) must not strand the schedule: it re-arms to
    // the new time and fires an interval later, rather than stalling for the whole
    // regression.
    let (mut r, clock) = registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}" }]"#);
    seed_room(&mut r);
    r.sweep(); // arm at 0

    clock.advance(1_000);
    r.sweep(); // fires at 1000
    assert_eq!(version_names(&r).len(), 1);

    // The clock steps back to 500 — below the last fire at 1000.
    r.set_clock(Arc::new(ManualClock::new(500)));
    r.sweep(); // re-arm, no capture
    assert_eq!(
        version_names(&r).len(),
        1,
        "no capture on the backward step"
    );

    r.set_clock(Arc::new(ManualClock::new(1_500))); // one interval past the re-arm
    r.sweep();
    assert_eq!(
        version_names(&r).len(),
        2,
        "the schedule resumes at its cadence after the correction",
    );
}

#[test]
fn a_schedule_rearms_after_the_room_goes_dormant() {
    // A room that empties drops its binding, so its schedule state is pruned;
    // rebinding re-arms it — it does not immediately fire on the strength of a
    // pre-dormancy timer.
    let (mut r, clock) = registry_with(r#"[{ "every": "1s", "name": "auto/tick/${timestamp}" }]"#);
    r.set_grace_millis(0);
    let a = seed_room(&mut r);
    r.sweep(); // arm at 0

    clock.advance(1_000);
    r.sweep(); // fires at 1000
    assert_eq!(version_names(&r).len(), 1);

    // The only subscriber departs; a sweep clears its presence and unbinds the room.
    r.disconnect(a);
    r.sweep();

    // A new subscriber rebinds the room; this sweep arms afresh — no capture.
    let b = seed_room(&mut r);
    let _ = b;
    clock.advance(500);
    r.sweep();
    assert_eq!(
        version_names(&r).len(),
        1,
        "the rebound schedule arms, it does not fire on the old timer",
    );
}

#[test]
fn two_triggers_on_the_same_event_both_fire() {
    let (mut r, clock) = registry_with(
        r#"[{ "on": "subscribe", "name": "a/${timestamp}" },
             { "on": "subscribe", "name": "b/${timestamp}" }]"#,
    );
    seed_room(&mut r);

    clock.advance(7);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(
        version_names(&r),
        vec![
            format!("a/{}", stamp(7)).into_bytes(),
            format!("b/{}", stamp(7)).into_bytes(),
        ],
    );
}

#[test]
fn keep_prunes_the_oldest_captures_of_the_trigger() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 2 }]"#);
    seed_room(&mut r);

    for client in [2u8, 3, 4] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }

    assert_eq!(
        version_names(&r),
        vec![
            format!("auto/join/{}", stamp(2000)).into_bytes(),
            format!("auto/join/{}", stamp(3000)).into_bytes(),
        ],
        "keep:2 retains the two newest, prunes the oldest",
    );
}

#[test]
fn a_lowered_window_prunes_the_whole_backlog_at_once() {
    // Five captures accumulate, then keep:1 must evict four in a single batch —
    // exercising the multi-eviction path, not just a one-off trim.
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 1 }]"#);
    seed_room(&mut r);

    for client in [2u8, 3, 4, 5, 6] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }

    assert_eq!(
        version_names(&r),
        vec![format!("auto/join/{}", stamp(5000)).into_bytes()],
        "keep:1 evicts the four older captures in one batch, leaving the newest",
    );
}

#[test]
fn keep_never_prunes_a_manual_version() {
    // A manually created version whose name fits the trigger's naming is not the
    // trigger's capture — retention keys on provenance, not the name — so it is
    // never pruned by the trigger's window.
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 1 }]"#);
    let a = seed_room(&mut r);
    // A manual version named exactly as a capture would be — but authored by the
    // operator, not the trigger.
    let manual = format!("auto/join/{}", stamp(0));
    assert!(r.deliver(
        a,
        Message::VersionCreate {
            channel: CH,
            name: manual.clone().into_bytes(),
        }
    ));
    r.take_outbox(a);

    for client in [2u8, 3] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }

    // keep:1 pruned the trigger's own older capture, but the manual version stays.
    assert_eq!(
        version_names(&r),
        vec![
            manual.into_bytes(),
            format!("auto/join/{}", stamp(2000)).into_bytes(),
        ],
        "retention prunes only the trigger's captures, never the manual version",
    );
}

#[test]
fn two_triggers_sharing_a_template_keep_independent_windows() {
    // Two triggers on different events render the same name pattern. Provenance is
    // per (event, template), so each keeps its own window — a shared name does not
    // collapse them into one destructive group.
    let (mut r, clock) = registry_with(
        r#"[{ "on": "subscribe", "name": "snap/${timestamp}", "keep": 3 },
             { "on": "version-created", "name": "snap/${timestamp}", "keep": 1 }]"#,
    );
    let a = seed_room(&mut r);

    // Three subscribes → three subscribe-captures (room already populated).
    for client in [2u8, 3, 4] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }
    // A manual version create fires the version-created trigger → one capture.
    clock.advance(1000);
    assert!(r.deliver(
        a,
        Message::VersionCreate {
            channel: CH,
            name: b"manual".to_vec(),
        }
    ));
    r.take_outbox(a);

    let names = version_names(&r);
    // The subscribe trigger keeps its newest 3; the version-created trigger keeps
    // its 1 — its single capture did not prune the subscribe group.
    let snaps = names.iter().filter(|n| n.starts_with(b"snap/")).count();
    assert_eq!(
        snaps, 4,
        "3 (subscribe window) + 1 (version-created window) survive despite the shared name",
    );
}

#[test]
fn keep_orders_by_capture_not_wall_clock() {
    // Retention orders by the monotonic capture ordinal, not the timestamp in the
    // name, so a backward clock step does not make the newest capture look oldest
    // and get pruned.
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 1 }]"#);
    seed_room(&mut r);

    clock.advance(1000);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM); // capture at 1000

    // The wall clock steps backward (an NTP correction) to 500.
    r.set_clock(Arc::new(ManualClock::new(500)));
    let c = hello_auth(&mut r, 3, APP, 1);
    subscribe(&mut r, c, ROOM); // capture at 500 — but a later ordinal

    assert_eq!(
        version_names(&r),
        vec![format!("auto/join/{}", stamp(500)).into_bytes()],
        "keep:1 retains the latest capture (by ordinal), even with a smaller stamp",
    );
}

#[test]
fn a_renamed_capture_survives_its_triggers_retention() {
    // Renaming an auto-capture is a deliberate operator act — it detaches the
    // version from its trigger's window, so a later capture's `keep` must never
    // prune the curated snapshot.
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 1 }]"#);
    let a = seed_room(&mut r);

    clock.advance(1000);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM); // capture "auto/join/{1000}"

    // The operator renames it — now curated, exempt from the window.
    assert!(r.deliver(
        a,
        Message::VersionRename {
            channel: CH,
            from: format!("auto/join/{}", stamp(1000)).into_bytes(),
            to: b"keeper".to_vec(),
        }
    ));
    r.take_outbox(a);

    clock.advance(1000);
    let c = hello_auth(&mut r, 3, APP, 1);
    subscribe(&mut r, c, ROOM); // capture "auto/join/{2000}"; keep:1 prunes the group

    assert_eq!(
        version_names(&r),
        vec![
            format!("auto/join/{}", stamp(2000)).into_bytes(),
            b"keeper".to_vec(),
        ],
        "the renamed version is detached from the window and survives",
    );
}

#[test]
fn a_name_collision_still_applies_the_tighter_window() {
    // Two triggers on the same event render the same name — the second's capture
    // collides and no-ops, but retention still runs on that path, so the tighter
    // `keep` governs the shared provenance group.
    let (mut r, clock) = registry_with(
        r#"[{ "on": "subscribe", "name": "dup/${timestamp}", "keep": 2 },
             { "on": "subscribe", "name": "dup/${timestamp}", "keep": 1 }]"#,
    );
    seed_room(&mut r);

    for client in [2u8, 3] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }

    assert_eq!(
        version_names(&r),
        vec![format!("dup/{}", stamp(2000)).into_bytes()],
        "the keep:1 trigger prunes on the collision path, leaving only the newest",
    );
}

#[test]
fn keep_zero_captures_nothing() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 0 }]"#);
    seed_room(&mut r);
    clock.advance(1000);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn keep_none_retains_all() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    seed_room(&mut r);
    for client in [2u8, 3] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }
    assert_eq!(version_names(&r).len(), 2);
}

#[test]
fn an_auto_created_version_does_not_cascade() {
    // A subscribe trigger captures a version, whose VersionCreated event a
    // version-created trigger would in turn capture — an unbounded cascade. The
    // engine suppresses recording while it drains, so the auto-created version
    // never re-fires: exactly the subscribe capture, not a version-created one.
    let (mut r, _clock) = registry_with(
        r#"[{ "on": "subscribe", "name": "auto/join/x" },
             { "on": "version-created", "name": "auto/vc/x" }]"#,
    );
    seed_room(&mut r);

    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(
        version_names(&r),
        vec![b"auto/join/x".to_vec()],
        "the join capture does not cascade into a version-created capture",
    );
}

/// Retention is durable — a trigger's provenance and capture order persist, so a
/// reopened server prunes the pre-restart captures rather than orphaning them (the
/// failure the abandoned in-memory tally had). Real filesystem I/O, not under Miri.
#[test]
#[cfg(not(miri))]
fn retention_survives_a_store_restart() {
    use crdtsync_server::Store;

    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("crdtsync-autover-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    let _guard = TempDir(path.clone());

    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, schema(SUBSCRIBE_KEEP_2).as_bytes(), b"")
        .unwrap();
    let sr = Arc::new(Mutex::new(sr));

    // First run: seed the room and take two captures (filling keep:2).
    {
        let mut r = Registry::with_store(cid(0xFF), Store::open(&path).unwrap()).unwrap();
        r.set_schema_registry(sr.clone());
        let clock = Arc::new(ManualClock::new(0));
        r.set_clock(clock.clone());
        seed_room(&mut r);
        for client in [2u8, 3] {
            clock.advance(1000);
            let c = hello_auth(&mut r, client, APP, 1);
            subscribe(&mut r, c, ROOM);
        }
        assert_eq!(r.hub().version_names(ROOM).len(), 2);
    }

    // Reopen the store into a fresh registry — a restart.
    let mut r = Registry::with_store(cid(0xFF), Store::open(&path).unwrap()).unwrap();
    r.set_schema_registry(sr);
    r.set_clock(Arc::new(ManualClock::new(3000)));
    let c = hello_auth(&mut r, 4, APP, 1);
    subscribe(&mut r, c, ROOM); // a third capture, at 3000

    assert_eq!(
        r.hub().version_names(ROOM),
        vec![
            format!("auto/join/{}", stamp(2000)).into_bytes(),
            format!("auto/join/{}", stamp(3000)).into_bytes(),
        ],
        "keep:2 pruned the pre-restart oldest across the reopen, not orphaned it",
    );
}

const SUBSCRIBE_KEEP_2: &str =
    r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 2 }]"#;
