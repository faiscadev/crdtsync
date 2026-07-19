// Real filesystem I/O (the durable epoch store), which Miri does not model.
#![cfg(not(miri))]

//! Cluster hardening 3 — leadership-epoch persistence across a restart.
//!
//! The split-brain fence (Unit 6b) is a per-room monotone leadership epoch: a
//! follower rejects a `Replicate` whose epoch is below the highest it has seen, so a
//! demoted-then-recovered stale leader cannot replicate writes it missed. That fence
//! lived only in memory — a restart forgot it, so a restarted follower would
//! re-accept a stale leader's low-epoch frames it had previously fenced. Persisting
//! the epoch closes that: the highest epoch seen per room is written to the durable
//! store as it advances and reloaded on startup, so the fence survives a restart and
//! is monotone across it.
//!
//! These drive a store-backed `Registry` across a simulated restart (drop, reopen
//! over the same store) with injected epoch-stamped frames — no sockets — so they
//! are deterministic. Real filesystem I/O, so Miri-excluded; the fence logic itself
//! is covered under Miri by `epoch_fence.rs`.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::store::Store;
use crdtsync_server::{ManualClock, Registry};

const CH: Channel = Channel(0);
const N: usize = 3;
const SELF_ADDR: &str = "10.0.0.6:9000";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn members_str() -> String {
    (0..7)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership() -> Membership {
    Membership::from_static_config(None, Some(SELF_ADDR), &members_str(), N).unwrap()
}

/// A room self holds as a *follower* — it applies replicated frames and observes
/// their epochs.
fn room_self_follows(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let r = m.replicas_for(&room);
        if r.len() >= 2 && !m.is_self(&r[0]) && r.iter().skip(1).any(|n| m.is_self(n)) {
            return room;
        }
    }
    panic!("no room places self as a follower");
}

/// A room self is the placement primary of — it leads and claims epochs.
fn room_self_leads(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        if m.is_primary_for(&room) {
            return room;
        }
    }
    panic!("no room places self as primary");
}

/// A store-backed registry over the shared cluster, its clock pinned.
fn node(store: Store) -> Registry {
    let mut r = Registry::with_store(cid(0xFF), store).unwrap();
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership());
    r
}

/// A leader's `Replicate` for `room`'s main stream at `epoch`, one register write.
fn replicate(writer: &mut Document, room: &[u8], epoch: u64, key: &[u8], value: i64) -> Message {
    let ops = writer.transact(|tx| tx.register(key, Scalar::Int(value)));
    Message::Replicate {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        ops,
        base_seq: 0,
        epoch,
    }
}

/// An authenticated client on `r` declaring device `client`, handshake drained. The
/// declared client must match the author of any ops it writes (the server fences an
/// "op client mismatch"), so a write authored by `doc(n)` uses `client_as(r, n)`.
fn client_as(r: &mut Registry, client: u8) -> crdtsync_server::ConnId {
    let id = r.connect();
    r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
    r.take_outbox(id);
    id
}

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

// --- a follower's observed epoch survives a restart and keeps fencing ---

#[test]
fn an_observed_epoch_survives_a_restart() {
    let tmp = tempdir();
    let m = membership();
    let room = room_self_follows(&m);
    let mut w = doc(9);

    {
        let mut r = node(Store::open(tmp.path()).unwrap());
        let peer = r.connect();
        // The room advances to epoch 5 under a leader — the follower observes it.
        assert!(r.deliver(peer, replicate(&mut w, &room, 5, b"a", 1)));
        assert_eq!(r.highest_epoch(&room), 5, "the follower observed epoch 5");
    }

    // A restart over the same store reloads the fence at epoch 5, not 0.
    let mut r = node(Store::open(tmp.path()).unwrap());
    assert_eq!(
        r.highest_epoch(&room),
        5,
        "the epoch fence is reloaded, not forgotten"
    );

    // A stale leader replays at epoch 3 — fenced (dropped, not applied), because the
    // reloaded node still remembers epoch 5. Without persistence it would have
    // re-accepted this.
    let seq_before = r.hub().seq(&room);
    let stale = r.connect();
    let kept = r.deliver(stale, replicate(&mut w, &room, 3, b"resurrected", 999));
    assert!(kept, "a fenced frame is dropped, not a violation");
    assert_eq!(
        r.hub().seq(&room),
        seq_before,
        "the stale-epoch write is fenced after the restart"
    );
    let out = r.take_outbox(stale);
    assert!(out.is_empty(), "a fenced frame is not acked, got {out:?}");
}

#[test]
fn a_fresh_higher_epoch_still_applies_after_a_restart() {
    let tmp = tempdir();
    let m = membership();
    let room = room_self_follows(&m);
    let mut w = doc(9);

    {
        let mut r = node(Store::open(tmp.path()).unwrap());
        let peer = r.connect();
        assert!(r.deliver(peer, replicate(&mut w, &room, 5, b"a", 1)));
    }

    let mut r = node(Store::open(tmp.path()).unwrap());
    assert_eq!(r.highest_epoch(&room), 5);
    // A genuine promotion past the reloaded fence is applied and acked.
    let peer = r.connect();
    let seq_before = r.hub().seq(&room);
    assert!(r.deliver(peer, replicate(&mut w, &room, 6, b"b", 2)));
    assert_eq!(
        r.hub().seq(&room),
        seq_before + 1,
        "the higher epoch applied"
    );
    assert_eq!(r.highest_epoch(&room), 6, "the fence advanced to 6");
}

// --- a leader's claimed epoch survives a restart and never regresses ---

#[test]
fn a_leaders_claimed_epoch_survives_a_restart_and_advances() {
    let tmp = tempdir();
    let m = membership();
    let room = room_self_leads(&m);

    {
        let mut r = node(Store::open(tmp.path()).unwrap());
        let c = client_as(&mut r, 1);
        r.deliver(c, sub(&room));
        r.take_outbox(c);
        // A client write makes self originate replication, claiming epoch 1.
        let ops = doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)));
        r.deliver(c, Message::Ops { channel: CH, ops });
        assert_eq!(r.highest_epoch(&room), 1, "the leader claimed epoch 1");
    }

    // A restart reloads the claimed epoch, so re-leading does not regress to 1. The
    // second write is authored by a fresh device (cid 2) so its op id is new — a
    // reused cid-1 op would dedup against the persisted first write and never
    // broadcast — and the declaring client matches the author, avoiding an op-client
    // mismatch.
    let mut r = node(Store::open(tmp.path()).unwrap());
    assert_eq!(r.highest_epoch(&room), 1, "the claimed epoch reloaded");
    let c = client_as(&mut r, 2);
    r.deliver(c, sub(&room));
    r.take_outbox(c);
    let ops = doc(2).transact(|tx| tx.register(b"k2", Scalar::Int(2)));
    r.deliver(c, Message::Ops { channel: CH, ops });
    let claimed = r.take_replication().into_iter().find_map(|(_, f)| match f {
        Message::Replicate { epoch, .. } => Some(epoch),
        _ => None,
    });
    assert_eq!(
        claimed,
        Some(2),
        "the re-led epoch advances past the reloaded one, never regressing to 1"
    );
    assert_eq!(r.highest_epoch(&room), 2);
}

// --- a tempdir without pulling in a dev-dependency ---

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-epoch-{pid}-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
