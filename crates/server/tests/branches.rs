//! The per-room branch registry: the default `main`, fork/list/rename/delete
//! pointer semantics, and durable persistence beside the room.
//!
//! A branch is a named pointer into the op log. Every room has the default
//! `main` (fork_point 0) with zero ceremony; forks share immutable history up to
//! their fork point and are the only branches persisted, `main` being
//! re-synthesized on load.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Scalar};
use crdtsync_server::{Hub, Store};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";

/// The branch names a room lists, in the order the registry reports them.
fn names(hub: &Hub, room: &[u8]) -> Vec<Vec<u8>> {
    hub.branches(room).into_iter().map(|b| b.name).collect()
}

/// Ingest a register write from `d` into `ROOM`, advancing the room's log head.
fn write_age(hub: &mut Hub, d: &mut Document, value: i64) {
    hub.ingest(
        ROOM,
        d.transact(|tx| tx.register(b"age", Scalar::Int(value))),
        None,
    )
    .unwrap();
}

/// Grow the room's log to head `n`, so a fork point up to `n` names history that
/// exists (the fork point is clamped to the source's head).
fn advance(hub: &mut Hub, d: &mut Document, n: u64) {
    for i in 0..n {
        write_age(hub, d, i as i64);
    }
}

#[test]
fn a_fresh_room_has_only_the_default_main() {
    let hub = Hub::new(cid(0xFF));
    assert_eq!(names(&hub, ROOM), vec![b"main".to_vec()]);
    let main = hub.branch(ROOM, b"main").expect("main always resolves");
    assert_eq!(main.fork_point, 0);
    assert!(hub.branch(ROOM, b"draft").is_none());
}

#[test]
fn main_head_tracks_the_room_log_head() {
    let mut hub = Hub::new(cid(0xFF));
    let mut d = Document::new(cid(1));
    write_age(&mut hub, &mut d, 30);
    write_age(&mut hub, &mut d, 40);
    let main = hub.branch(ROOM, b"main").unwrap();
    assert_eq!(main.head, hub.seq(ROOM));
    assert!(main.head > 0);
}

#[test]
fn fork_creates_a_branch_sharing_history_to_the_fork_point() {
    let mut hub = Hub::new(cid(0xFF));
    advance(&mut hub, &mut Document::new(cid(1)), 5);
    assert!(hub.fork_branch(ROOM, b"draft", b"main", 5).unwrap());
    assert_eq!(names(&hub, ROOM), vec![b"draft".to_vec(), b"main".to_vec()]);
    let draft = hub.branch(ROOM, b"draft").unwrap();
    assert_eq!(draft.fork_point, 5);
    assert_eq!(draft.head, 5);
}

#[test]
fn fork_refuses_a_duplicate_name_or_an_unknown_source() {
    let mut hub = Hub::new(cid(0xFF));
    advance(&mut hub, &mut Document::new(cid(1)), 7);
    assert!(hub.fork_branch(ROOM, b"draft", b"main", 3).unwrap());
    // A duplicate name changes nothing.
    assert!(!hub.fork_branch(ROOM, b"draft", b"main", 7).unwrap());
    assert_eq!(hub.branch(ROOM, b"draft").unwrap().fork_point, 3);
    // An unknown source branch is refused.
    assert!(!hub.fork_branch(ROOM, b"other", b"ghost", 1).unwrap());
    assert!(hub.branch(ROOM, b"other").is_none());
}

#[test]
fn main_is_never_deletable() {
    let mut hub = Hub::new(cid(0xFF));
    assert!(!hub.delete_branch(ROOM, b"main").unwrap());
    assert_eq!(names(&hub, ROOM), vec![b"main".to_vec()]);
}

#[test]
fn rename_moves_a_non_main_branch() {
    let mut hub = Hub::new(cid(0xFF));
    advance(&mut hub, &mut Document::new(cid(1)), 4);
    hub.fork_branch(ROOM, b"draft", b"main", 4).unwrap();
    assert!(hub.rename_branch(ROOM, b"draft", b"final").unwrap());
    assert!(hub.branch(ROOM, b"draft").is_none());
    let final_ = hub.branch(ROOM, b"final").unwrap();
    assert_eq!(final_.fork_point, 4);
    // main is not renamable; a rename onto a taken name changes nothing.
    assert!(!hub.rename_branch(ROOM, b"main", b"trunk").unwrap());
    hub.fork_branch(ROOM, b"draft2", b"main", 2).unwrap();
    assert!(!hub.rename_branch(ROOM, b"draft2", b"final").unwrap());
}

#[test]
fn delete_removes_a_non_main_branch() {
    let mut hub = Hub::new(cid(0xFF));
    hub.fork_branch(ROOM, b"draft", b"main", 1).unwrap();
    assert!(hub.delete_branch(ROOM, b"draft").unwrap());
    assert_eq!(names(&hub, ROOM), vec![b"main".to_vec()]);
    // Deleting an absent branch changes nothing.
    assert!(!hub.delete_branch(ROOM, b"draft").unwrap());
}

#[test]
fn branches_list_in_deterministic_name_order() {
    let mut hub = Hub::new(cid(0xFF));
    hub.fork_branch(ROOM, b"zeta", b"main", 1).unwrap();
    hub.fork_branch(ROOM, b"alpha", b"main", 2).unwrap();
    hub.fork_branch(ROOM, b"mid", b"main", 3).unwrap();
    assert_eq!(
        names(&hub, ROOM),
        vec![
            b"alpha".to_vec(),
            b"main".to_vec(),
            b"mid".to_vec(),
            b"zeta".to_vec(),
        ],
    );
}

// --- durability: the branch set survives a store reopen (real filesystem I/O,
// which Miri does not model) ---
#[cfg(not(miri))]
mod durable {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn open_hub(path: &Path) -> Hub {
        let store = Store::open(path).unwrap();
        let rooms = store.load().unwrap();
        let mut hub = Hub::from_rooms(cid(0xFF), rooms).unwrap();
        hub.attach_store(store);
        hub
    }

    #[test]
    fn a_fork_and_rename_survive_a_reopen() {
        let tmp = tempdir();
        {
            let mut hub = open_hub(tmp.path());
            advance(&mut hub, &mut Document::new(cid(1)), 9);
            hub.fork_branch(ROOM, b"draft", b"main", 5).unwrap();
            hub.fork_branch(ROOM, b"spike", b"main", 9).unwrap();
            hub.rename_branch(ROOM, b"draft", b"final").unwrap();
        }

        let hub = open_hub(tmp.path());
        assert_eq!(
            names(&hub, ROOM),
            vec![b"final".to_vec(), b"main".to_vec(), b"spike".to_vec()],
        );
        assert_eq!(hub.branch(ROOM, b"final").unwrap().fork_point, 5);
        assert_eq!(hub.branch(ROOM, b"spike").unwrap().head, 9);
    }

    #[test]
    fn deleting_the_last_fork_leaves_only_main_after_reopen() {
        let tmp = tempdir();
        {
            let mut hub = open_hub(tmp.path());
            hub.fork_branch(ROOM, b"draft", b"main", 3).unwrap();
            hub.delete_branch(ROOM, b"draft").unwrap();
        }

        let hub = open_hub(tmp.path());
        assert_eq!(names(&hub, ROOM), vec![b"main".to_vec()]);
    }

    #[test]
    fn an_absent_branches_record_loads_as_main() {
        let tmp = tempdir();
        let hub = open_hub(tmp.path());
        assert_eq!(names(&hub, ROOM), vec![b"main".to_vec()]);
    }

    #[test]
    fn a_malformed_branches_record_loads_as_main() {
        let tmp = tempdir();
        {
            let mut hub = open_hub(tmp.path());
            hub.fork_branch(ROOM, b"draft", b"main", 5).unwrap();
        }
        // Corrupt the branches file: a truncated record is undecodable.
        let branches_file = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("branches"))
            .expect("a branches file was written for the fork");
        fs::write(&branches_file, b"\x07\x00\x00\x00tru").unwrap();

        let hub = open_hub(tmp.path());
        assert_eq!(names(&hub, ROOM), vec![b"main".to_vec()]);
    }

    // --- a tempdir without pulling in a dev-dependency ---

    struct TempDir(PathBuf);

    impl TempDir {
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn tempdir() -> TempDir {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("crdtsync-branches-{pid}-{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}
