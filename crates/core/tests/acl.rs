//! Doc-level ACL — the authorization tuple CRDT set (Doc-level ACL slice 1).
//!
//! An ACL tuple is a doc-level CRDT entry keyed by its own id: a `{subject,
//! grant, effect, path, grantor}` grant an owner emits. The set is storage only —
//! any tuple that arrives is stored and merged; who *may* emit one, and how the
//! grants evaluate, are later (server-side) slices. Semantics mirror the
//! RangedElement set: concurrent grants union to distinct ids, a tuple is
//! immutable once created, and a revoke tombstones it (retained, delete-wins).

use crdtsync_core::acl::{AclEffect, AclGrant, AclScope, AclSubject, AclTuple, Capability};
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Op};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

fn read_cap() -> AclGrant {
    AclGrant::Capability(Capability::Read)
}

#[test]
fn a_grant_records_its_subject_grant_effect_path_and_grantor() {
    let mut d = Document::new(cid(1));
    let path = encode_path(&[b"doc", b"content"]);
    let grantor = cid(9);

    let mut id = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Actor(cid(2)),
            AclGrant::Capability(Capability::Own),
            AclEffect::Allow,
            path.clone(),
            grantor,
        );
    });

    let t = d.acl_tuple(id).expect("tuple present");
    assert_eq!(t.id, id);
    assert_eq!(t.subject, AclSubject::Actor(cid(2)));
    assert_eq!(t.grant, AclGrant::Capability(Capability::Own));
    assert_eq!(t.effect, AclEffect::Allow);
    assert_eq!(t.scope, AclScope::Path(path));
    assert_eq!(t.grantor, grantor);
    assert_eq!(d.acl_tuples().len(), 1);
}

#[test]
fn acl_on_filters_by_exact_path() {
    let mut d = Document::new(cid(1));
    let a = encode_path(&[b"a"]);
    let b = encode_path(&[b"b"]);
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Anyone,
            read_cap(),
            AclEffect::Allow,
            a.clone(),
            cid(1),
        );
        tx.acl().grant(
            AclSubject::Anonymous,
            read_cap(),
            AclEffect::Deny,
            a.clone(),
            cid(1),
        );
        tx.acl().grant(
            AclSubject::Anyone,
            read_cap(),
            AclEffect::Allow,
            b.clone(),
            cid(1),
        );
    });
    assert_eq!(d.acl_on(&a).len(), 2, "two tuples on path a");
    assert_eq!(d.acl_on(&b).len(), 1, "one tuple on path b");
    assert!(d.acl_on(&encode_path(&[b"c"])).is_empty());
}

#[test]
fn concurrent_grants_union_to_distinct_ids() {
    let path = encode_path(&[b"x"]);
    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));

    let mut a = ElementId::from_bytes([0u8; 16]);
    let mut b = ElementId::from_bytes([0u8; 16]);
    let c1 = r1.transact(|tx| {
        a = tx.acl().grant(
            AclSubject::Actor(cid(5)),
            read_cap(),
            AclEffect::Allow,
            path.clone(),
            cid(2),
        );
    });
    let c2 = r2.transact(|tx| {
        b = tx.acl().grant(
            AclSubject::Actor(cid(6)),
            read_cap(),
            AclEffect::Allow,
            path.clone(),
            cid(3),
        );
    });
    apply_all(&mut r1, &c2);
    apply_all(&mut r2, &c1);

    assert_ne!(a, b, "concurrent grants get distinct ids");
    assert_eq!(r1.acl_tuples().len(), 2);
    assert_eq!(r2.acl_tuples().len(), 2);
    assert!(r1.acl_tuple(a).is_some() && r1.acl_tuple(b).is_some());
    assert!(r2.acl_tuple(a).is_some() && r2.acl_tuple(b).is_some());
    assert_eq!(r1.acl_tuples(), r2.acl_tuples(), "replicas converge");
}

#[test]
fn a_revoke_tombstones_the_tuple() {
    let mut d = Document::new(cid(1));
    let path = encode_path(&[b"p"]);
    let mut id = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Anyone,
            read_cap(),
            AclEffect::Allow,
            path,
            cid(1),
        );
    });
    d.transact(|tx| tx.acl().revoke(id));

    assert!(d.acl_tuple(id).is_none(), "read-filtered after revoke");
    assert_eq!(d.acl_tuples().len(), 0);

    // The tombstone is retained in state — it survives a snapshot reload.
    let back = Document::decode_state(&d.encode_state()).unwrap();
    assert!(back.acl_tuple(id).is_none(), "revoked stays revoked");
}

#[test]
fn a_revoke_waits_for_its_grant() {
    // A revoke that arrives before the grant it tombstones must buffer: applied
    // against a missing entry it would be silently lost, diverging from a replica
    // that saw them in order.
    let mut src = Document::new(cid(1));
    let path = encode_path(&[b"p"]);
    let mut id = ElementId::from_bytes([0u8; 16]);
    let grant = src.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Anyone,
            read_cap(),
            AclEffect::Allow,
            path,
            cid(1),
        );
    });
    let revoke = src.transact(|tx| tx.acl().revoke(id));

    let mut dst = Document::new(cid(2));
    apply_all(&mut dst, &revoke); // revoke first — the tuple is absent
    assert!(
        dst.acl_tuple(id).is_none(),
        "nothing to read before the grant"
    );
    apply_all(&mut dst, &grant); // grant lands; the buffered revoke replays after it
    assert!(
        dst.acl_tuple(id).is_none(),
        "the buffered revoke is not lost",
    );
    // Both replicas hold the tombstone.
    assert_eq!(dst.acl_tuples().len(), 0);
    assert_eq!(src.acl_tuples().len(), 0);
}

#[test]
fn a_local_revoke_of_an_unseen_tuple_emits_nothing() {
    // A revoke for an id whose grant this replica has not applied must emit no op:
    // a local apply would no-op while still broadcasting, so the author keeps the
    // old reading while a peer that applied it against the present entry moves on.
    let mut src = Document::new(cid(1));
    let path = encode_path(&[b"p"]);
    let mut id = ElementId::from_bytes([0u8; 16]);
    let grant = src.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Anyone,
            read_cap(),
            AclEffect::Allow,
            path,
            cid(1),
        );
    });

    let mut other = Document::new(cid(2));
    let revoke = other.transact(|tx| tx.acl().revoke(id));
    assert!(revoke.is_empty(), "revoke on an unseen tuple emits nothing");

    // Once the grant is applied, a revoke is a real op again and converges.
    apply_all(&mut other, &grant);
    let revoke2 = other.transact(|tx| tx.acl().revoke(id));
    assert!(!revoke2.is_empty(), "revoke on a materialised tuple emits");

    let mut peer = Document::new(cid(3));
    apply_all(&mut peer, &grant);
    apply_all(&mut peer, &revoke2);
    assert!(peer.acl_tuple(id).is_none());
    assert_eq!(peer.acl_tuples(), other.acl_tuples());
}

#[test]
fn grant_and_revoke_converge_regardless_of_order() {
    let mut base = Document::new(cid(1));
    let path = encode_path(&[b"p"]);
    let mut id = ElementId::from_bytes([0u8; 16]);
    let grant = base.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Actor(cid(7)),
            read_cap(),
            AclEffect::Allow,
            path,
            cid(1),
        );
    });

    let mut r1 = Document::new(cid(2));
    let mut r2 = Document::new(cid(3));
    apply_all(&mut r1, &grant);
    apply_all(&mut r2, &grant);

    let revoke = r1.transact(|tx| tx.acl().revoke(id));
    // r1 applied grant then revoke; r2 applies revoke then re-sees grant order.
    apply_all(&mut r2, &revoke);

    assert!(r1.acl_tuple(id).is_none());
    assert!(r2.acl_tuple(id).is_none(), "revoke converges on r2");
    assert_eq!(r1.acl_tuples(), r2.acl_tuples());
}

#[test]
fn the_set_round_trips_through_state() {
    let mut d = Document::new(cid(1));
    let path = encode_path(&[b"doc"]);

    // Every subject variant, both grant flavors, both effects.
    let grants: Vec<(AclSubject, AclGrant, AclEffect)> = vec![
        (
            AclSubject::Actor(cid(2)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
        ),
        (
            AclSubject::Group(b"designers".to_vec()),
            AclGrant::Capability(Capability::Write),
            AclEffect::Deny,
        ),
        (
            AclSubject::Authenticated,
            AclGrant::Capability(Capability::PublishAwareness),
            AclEffect::Allow,
        ),
        (
            AclSubject::Anonymous,
            AclGrant::Capability(Capability::Own),
            AclEffect::Deny,
        ),
        (
            AclSubject::Anyone,
            AclGrant::Role(b"editor".to_vec()),
            AclEffect::Allow,
        ),
    ];

    let mut ids = Vec::new();
    d.transact(|tx| {
        for (s, g, e) in grants.iter().cloned() {
            ids.push(tx.acl().grant(s, g, e, path.clone(), cid(1)));
        }
    });
    // Revoke one so a tombstone is exercised across the round-trip.
    let gone = ids[1];
    d.transact(|tx| tx.acl().revoke(gone));

    let bytes = d.encode_state();
    let restored = Document::decode_state(&bytes).unwrap();

    for &id in &ids {
        assert_eq!(restored.acl_tuple(id), d.acl_tuple(id));
    }
    assert!(restored.acl_tuple(gone).is_none(), "deleted stays deleted");
    assert_eq!(restored.acl_tuples(), d.acl_tuples());
    assert_eq!(restored.encode_state(), bytes, "re-encode diverged");
}

#[test]
fn acl_tuples_are_id_sorted_and_deterministic() {
    // The same grants authored by the same replica produce the same set order.
    fn build() -> Vec<AclTuple> {
        let mut d = Document::new(cid(1));
        let path = encode_path(&[b"p"]);
        d.transact(|tx| {
            for k in 0..8u8 {
                tx.acl().grant(
                    AclSubject::Actor(cid(k + 10)),
                    read_cap(),
                    AclEffect::Allow,
                    path.clone(),
                    cid(1),
                );
            }
        });
        d.acl_tuples()
    }
    let a = build();
    let b = build();
    assert_eq!(a, b, "same ops → same order");
    let mut sorted = a.clone();
    sorted.sort_by_key(|t| t.id.as_bytes());
    assert_eq!(a, sorted, "acl_tuples() is id-sorted");
}

#[test]
fn a_truncated_acl_record_is_a_decode_error_not_a_panic() {
    let mut d = Document::new(cid(1));
    let path = encode_path(&[b"doc", b"content"]);
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Group(b"team".to_vec()),
            AclGrant::Role(b"editor".to_vec()),
            AclEffect::Deny,
            path,
            cid(4),
        );
    });
    let bytes = d.encode_state();

    // Every truncation of a stream carrying an ACL record decodes to an error,
    // never a panic.
    for n in 0..bytes.len() {
        assert!(
            Document::decode_state(&bytes[..n]).is_err(),
            "truncation to {n} bytes should error",
        );
    }
    // A trailing byte past a valid stream is rejected too.
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(Document::decode_state(&extra).is_err());
}

#[test]
fn random_orderings_converge() {
    // A fixed script of grant/revoke ops authored across three replicas, delivered
    // shuffled to a fourth: every ordering converges to the same set.
    let path = encode_path(&[b"p"]);
    let mut r = [
        Document::new(cid(2)),
        Document::new(cid(3)),
        Document::new(cid(4)),
    ];

    let mut ids = [ElementId::from_bytes([0u8; 16]); 3];
    let mut ops: Vec<Op> = Vec::new();
    for (i, d) in r.iter_mut().enumerate() {
        ops.extend(d.transact(|tx| {
            ids[i] = tx.acl().grant(
                AclSubject::Actor(cid(i as u8 + 20)),
                read_cap(),
                AclEffect::Allow,
                path.clone(),
                cid(1),
            );
        }));
    }
    // Replica 1 sees every grant, then revokes replica 2's — so the revoke is a
    // real op. Shuffled toward a fourth replica it may land before that grant,
    // exercising the buffer.
    apply_all(&mut r[1], &ops);
    ops.extend(r[1].transact(|tx| tx.acl().revoke(ids[2])));

    let mut reference = Document::new(cid(9));
    apply_all(&mut reference, &ops);
    let expect = reference.acl_tuples();

    let seeds: u64 = if cfg!(miri) { 8 } else { 64 };
    for seed in 0..seeds {
        let mut shuffled = ops.clone();
        let mut rng = Rng::new(seed);
        for i in (1..shuffled.len()).rev() {
            let j = (rng.next() as usize) % (i + 1);
            shuffled.swap(i, j);
        }
        let mut d = Document::new(cid(10));
        apply_all(&mut d, &shuffled);
        assert_eq!(d.acl_tuples(), expect, "seed {seed} diverged");
    }
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 17
    }
}
