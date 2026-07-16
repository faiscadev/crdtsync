//! Element-scoped ACL — grants keyed to a stable element id that resolve to the
//! element's *current* path at evaluation, so a grant follows the element across a
//! tree-move (Kleppmann `XmlMove`).
//!
//! The load-bearing security properties (the whole point of the unit):
//!
//! 1. **Move-safe grant** — a `Deny(Read)` on an element by id still denies after the
//!    element is moved to a new parent/path (the grant followed the element). A path
//!    grant on the element's OLD path would have failed this.
//! 2. **No stranded restriction** — after the element moves away, a different element
//!    now at the old path is not governed by the moved element's grant.
//! 3. **No exfil-by-move** — a restricted element cannot be freed by relocating it (the
//!    reverse of (1)).
//!
//! The pure evaluator stays path-based: an element scope resolves to a path first,
//! then composes in the existing deny-overrides / inheritance / provenance lattice
//! exactly as a path tuple. An unresolvable element id is inert (fail-closed).

use std::collections::HashMap;

use crdtsync_core::acl::{
    decide_capability, decide_capability_with_authority, evaluate_with_authority, AclActor,
    AclDecision, AclEffect, AclGrant, AclRecord, AclScope, AclSubject, AclTuple, Capability,
};
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::path::{self, encode_path};
use crdtsync_core::{decode_op, encode_op, ClientId, Element};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn actor(id: u8) -> AclActor {
    AclActor::new(cid(id))
}

fn cap(c: Capability) -> AclGrant {
    AclGrant::Capability(c)
}

fn col_a() -> Vec<u8> {
    encode_path(&[b"colA"])
}

fn col_b() -> Vec<u8> {
    encode_path(&[b"colB"])
}

/// The columns the fixtures use as top-level fragment slots. The element index the
/// resolver reads is derived by walking these — the test analog of the server's
/// element-context index.
const SLOTS: &[&[u8]] = &[b"colA", b"colB"];

/// A board `doc` = two `XmlFragment` columns `colA`/`colB`, with a single `card`
/// element in `colA`. Returns the card's stable element id.
fn board(d: &mut Document) -> ElementId {
    path::xml_fragment(d, &col_a());
    path::xml_fragment(d, &col_b());
    path::xml_insert_element(d, &col_a(), 0, b"card");
    child_id(d, b"colA", 0)
}

/// The element id of the child at `idx` under the top-level fragment slot `col`.
fn child_id(d: &Document, col: &[u8], idx: usize) -> ElementId {
    let Some(Element::XmlFragment(f)) = d.get(col) else {
        panic!("slot {col:?} is not a fragment");
    };
    let child = f
        .borrow()
        .children()
        .borrow()
        .get(idx)
        .expect("child at index");
    child.id()
}

/// The element-context index for the fixture: every element under a column fragment
/// mapped to that column's encoded `core::path`. An XML descendant inherits its
/// column's path, exactly as core/server `element_paths` resolves it — so this is the
/// faithful id→current-path map the resolver reads, and a move to another column
/// re-derives the moved element's path to the new column.
fn element_paths(d: &Document) -> HashMap<ElementId, Vec<u8>> {
    fn collect(e: &Element, path: &[u8], out: &mut HashMap<ElementId, Vec<u8>>) {
        out.insert(e.id(), path.to_vec());
        let kids = match e {
            Element::XmlFragment(f) => f.borrow().children(),
            Element::XmlElement(x) => x.borrow().children(),
            _ => return,
        };
        for child in kids.borrow().values() {
            collect(&child, path, out);
        }
    }
    let mut out = HashMap::new();
    for col in SLOTS {
        if let Some(e) = d.get(col) {
            collect(&e, &encode_path(&[col]), &mut out);
        }
    }
    out
}

/// A live record around a directly-built tuple (throwaway id — the evaluator reads
/// scope/subject/grant/effect/grantor, never the id).
fn live(subject: AclSubject, grant: AclGrant, effect: AclEffect, scope: AclScope) -> AclRecord {
    AclRecord {
        tuple: AclTuple {
            id: ElementId::from_bytes([0u8; 16]),
            subject,
            grant,
            effect,
            scope,
            grantor: cid(1),
        },
        revoked_by: Vec::new(),
    }
}

// ---- the load-bearing security properties ---------------------------------

#[test]
fn a_deny_read_element_grant_follows_the_element_across_a_move() {
    // creator=1 owns `/`. A=cid(2) has a whole-document read allow, minus a deny on
    // one card (by element id).
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            encode_path(&[]),
            cid(1),
        );
        tx.acl().grant_element(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            card,
            cid(1),
        );
    });

    // Before the move: the card is in colA, and A is denied at the card's path.
    let records = d.acl_records();
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    assert_eq!(idx.get(&card).cloned(), Some(col_a()));
    assert!(
        !evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &resolve
        ),
        "A cannot read the card at its original location"
    );

    // Move the card colA -> colB via a real XmlMove.
    path::xml_move_child(&mut d, &col_a(), 0, &col_b(), 0);
    assert_eq!(
        child_id(&d, b"colB", 0),
        card,
        "the card moved, keeping its id"
    );

    // Property 1 / 3: the deny FOLLOWED the element — A still cannot read it at its new
    // location, so relocating it did not free it (no exfil-by-move).
    let records = d.acl_records();
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    assert_eq!(idx.get(&card).cloned(), Some(col_b()));
    assert!(
        !evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_b(),
            Capability::Read,
            &resolve
        ),
        "A still cannot read the card after it was dragged to colB"
    );

    // Property 2: the restriction did not strand on colA — A now reads colA freely.
    assert!(
        evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &resolve
        ),
        "the deny moved with the card; colA is no longer restricted"
    );
}

#[test]
fn a_path_deny_would_strand_and_exfil_where_an_element_deny_does_not() {
    // The contrast that justifies the unit: a PATH deny on the card's ORIGINAL path
    // stays put across the move — it strands on colA and frees the card at colB.
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            encode_path(&[]),
            cid(1),
        );
        // A path deny pinned to colA (where the card sits today).
        tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            col_a(),
            cid(1),
        );
    });
    path::xml_move_child(&mut d, &col_a(), 0, &col_b(), 0);
    let records = d.acl_records();
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    assert_eq!(idx.get(&card).cloned(), Some(col_b()));
    // The hole a path grant leaves: the card is now readable at colB (deny stranded),
    // and colA — now holding nothing of the card — is what stays denied.
    assert!(
        evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_b(),
            Capability::Read,
            &resolve
        ),
        "a path deny strands: the card is exfiltrated by moving it out of colA"
    );
    assert!(
        !evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &resolve
        ),
        "the path deny governs whatever now occupies colA — a stranded restriction"
    );
}

#[test]
fn an_element_deny_does_not_govern_a_different_element_at_the_old_path() {
    // No stranded restriction, shown directly: card1 (denied to A by id) moves out of
    // colA; card2 lands in colA; A may read card2 — the deny went with card1.
    let mut d = Document::new(cid(1));
    let card1 = board(&mut d);
    path::xml_insert_element(&mut d, &col_a(), 1, b"card"); // a second card in colA
    let card2 = child_id(&d, b"colA", 1);
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            encode_path(&[]),
            cid(1),
        );
        tx.acl().grant_element(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            card1,
            cid(1),
        );
    });
    // Move card1 out to colB; card2 stays in colA.
    path::xml_move_child(&mut d, &col_a(), 0, &col_b(), 0);
    let records = d.acl_records();
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    assert_eq!(idx.get(&card1).cloned(), Some(col_b()));
    assert_eq!(idx.get(&card2).cloned(), Some(col_a()));
    // card1's deny resolves to colB now, so it does not govern colA where card2 sits.
    assert!(
        evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &resolve
        ),
        "card2 at the old path is unrestricted — the restriction was not stranded"
    );
}

// ---- resolution: element scope -> current path ----------------------------

#[test]
fn an_element_scope_resolves_to_the_element_current_path() {
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    // A deny-read element grant, evaluated at the resolved path, denies exactly as a
    // path grant on that same path would.
    let recs = vec![
        live(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            AclScope::Path(encode_path(&[])),
        ),
        live(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            AclScope::Element(card),
        ),
    ];
    assert_eq!(
        decide_capability_with_authority(
            &recs,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &resolve
        ),
        AclDecision::Deny
    );
    // Off the card's path, the element deny does not govern.
    assert!(evaluate_with_authority(
        &recs,
        cid(1),
        &actor(2),
        &col_b(),
        Capability::Read,
        &resolve
    ));
}

#[test]
fn an_element_grant_composes_in_the_deny_overrides_lattice() {
    // Pure tier (no authority): an element-scoped deny overrides a path allow at the
    // same resolved path — the same deny-overrides rule a path deny obeys.
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    let tuples = vec![
        AclTuple {
            id: ElementId::from_bytes([1u8; 16]),
            subject: AclSubject::Actor(cid(2)),
            grant: cap(Capability::Read),
            effect: AclEffect::Allow,
            scope: AclScope::Path(col_a()),
            grantor: cid(1),
        },
        AclTuple {
            id: ElementId::from_bytes([2u8; 16]),
            subject: AclSubject::Actor(cid(2)),
            grant: cap(Capability::Read),
            effect: AclEffect::Deny,
            scope: AclScope::Element(card),
            grantor: cid(1),
        },
    ];
    assert_eq!(
        decide_capability(&tuples, &actor(2), &col_a(), Capability::Read, &resolve),
        AclDecision::Deny,
        "an element deny overrides a path allow at the resolved path"
    );
}

// ---- unresolvable id -> inert (fail-closed) -------------------------------

#[test]
fn an_unresolvable_element_grant_is_inert() {
    // A resolver that resolves nothing: an element allow grants nothing, and an
    // element deny blocks nothing — the grant is inert, never failing open.
    let ghost = ElementId::from_bytes([9u8; 16]);
    let none = |_: ElementId| None;

    // An element ALLOW to A that cannot resolve confers no read (fail-closed).
    let allow_only = vec![live(
        AclSubject::Actor(cid(2)),
        cap(Capability::Read),
        AclEffect::Allow,
        AclScope::Element(ghost),
    )];
    assert!(
        !evaluate_with_authority(
            &allow_only,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &none
        ),
        "an unresolvable element allow grants nothing"
    );

    // An element DENY that cannot resolve suppresses nothing: A's standing path allow
    // survives (the deny is inert, not a hard floor).
    let allow_plus_ghost_deny = vec![
        live(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            AclScope::Path(encode_path(&[])),
        ),
        live(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            AclScope::Element(ghost),
        ),
    ];
    assert!(
        evaluate_with_authority(
            &allow_plus_ghost_deny,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &none
        ),
        "an unresolvable element deny is inert — it does not block a standing allow"
    );
}

// ---- element scope in the rooting / revocation / bounded-deny walk --------

#[test]
fn an_element_grant_roots_via_its_resolved_path_and_revokes() {
    // creator=1. B=cid(3) owns colB (a path Own from the creator). B grants A=cid(2)
    // an element Allow(Read) on a card that lives in colB. The element grant ROOTS
    // because B owns the card's resolved path (colB). Then B revokes it and the read
    // is withdrawn.
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    path::xml_move_child(&mut d, &col_a(), 0, &col_b(), 0); // card now in colB
    let element_grant = std::cell::Cell::new(ElementId::from_bytes([0u8; 16]));
    d.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            AclEffect::Allow,
            col_b(),
            cid(1),
        );
        let id = tx.acl().grant_element(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            card,
            cid(3), // granted by B, who owns colB
        );
        element_grant.set(id);
    });

    let records = d.acl_records();
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    assert!(
        evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_b(),
            Capability::Read,
            &resolve
        ),
        "A reads the card: B's element grant rooted via B's ownership of the resolved path"
    );

    // A bounded deny by B (at/above the grantor — B is the grantor) suppresses it.
    let mut with_deny = records.clone();
    with_deny.push(AclRecord {
        tuple: AclTuple {
            id: ElementId::from_bytes([7u8; 16]),
            subject: AclSubject::Actor(cid(2)),
            grant: cap(Capability::Read),
            effect: AclEffect::Deny,
            scope: AclScope::Element(card),
            grantor: cid(3),
        },
        revoked_by: Vec::new(),
    });
    assert!(
        !evaluate_with_authority(
            &with_deny,
            cid(1),
            &actor(2),
            &col_b(),
            Capability::Read,
            &resolve
        ),
        "B's element deny is at/above the grantor and suppresses the element allow"
    );

    // Revocation: B revokes its own element grant → the read is withdrawn.
    d.transact(|tx| tx.acl().revoke(element_grant.get()));
    let records = d.acl_records();
    assert!(
        !evaluate_with_authority(
            &records,
            cid(1),
            &actor(2),
            &col_b(),
            Capability::Read,
            &resolve
        ),
        "after B revokes the element grant, A can no longer read the card"
    );
}

#[test]
fn an_unrooted_element_grant_confers_nothing() {
    // B=cid(3) does NOT own the card's path, yet purports to grant A an element read.
    // The grant does not root at the creator, so it is inert.
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    let recs = vec![live(
        AclSubject::Actor(cid(2)),
        cap(Capability::Read),
        AclEffect::Allow,
        AclScope::Element(card),
    )
    .with_grantor(cid(3))];
    let idx = element_paths(&d);
    let resolve = |id| idx.get(&id).cloned();
    assert!(
        !evaluate_with_authority(
            &recs,
            cid(1),
            &actor(2),
            &col_a(),
            Capability::Read,
            &resolve
        ),
        "a self-granted element allow that roots at no owner confers nothing"
    );
}

// ---- codec: element scope survives a snapshot; scope framing is total -----

#[test]
fn an_element_scope_survives_a_state_snapshot_round_trip() {
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    let id = std::cell::Cell::new(ElementId::from_bytes([0u8; 16]));
    d.transact(|tx| {
        let g = tx.acl().grant_element(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            card,
            cid(1),
        );
        id.set(g);
    });
    let back = Document::decode_state(&d.encode_state()).expect("round-trip");
    let tuple = back
        .acl_tuple(id.get())
        .expect("tuple present after reload");
    assert_eq!(tuple.scope, AclScope::Element(card));
}

#[test]
fn the_scope_codec_is_total_over_truncation_and_a_bad_tag() {
    // Build two element-scoped grant ops differing only in the element id payload, so
    // the byte just before the first difference is the scope tag.
    let mut d = Document::new(cid(1));
    let a = grant_op_bytes(&mut d, AclScope::Element(ElementId::from_bytes([0xAB; 16])));
    let mut d2 = Document::new(cid(1));
    let b = grant_op_bytes(
        &mut d2,
        AclScope::Element(ElementId::from_bytes([0xCD; 16])),
    );
    let diff = a
        .iter()
        .zip(&b)
        .position(|(x, y)| x != y)
        .expect("ops differ in the element id");
    let tag_pos = diff - 1;
    assert_eq!(a[tag_pos], 1, "the scope tag is the element variant");

    // A bad scope tag is rejected, not misread.
    let mut bad = a.clone();
    bad[tag_pos] = 0x7f;
    assert!(decode_op(&bad).is_err(), "an unknown scope tag is rejected");

    // Truncation partway through the element id is rejected.
    let truncated = &a[..diff + 4];
    assert!(
        decode_op(truncated).is_err(),
        "a scope cut short of its element id is rejected"
    );

    // The full frame still round-trips.
    assert_eq!(
        decode_op(&a).expect("round-trip").kind,
        decode_op(&a).unwrap().kind
    );
}

#[test]
fn a_projection_redacts_an_unresolvable_element_tuple_by_root_read() {
    // Regression: a snapshot projection must redact an unresolvable-element ACL tuple by
    // ROOT read — the same fallback the server op-stream takes (`op_read_path` gates it
    // at root) — so a snapshot-served reader and an op-served reader converge on it.
    // Dropping it here (fail-closed to nothing) diverged the two catch-up seams: an
    // op-served root reader kept the tuple while a snapshot-served one lost it.
    let mut d = Document::new(cid(1));
    let card = board(&mut d);
    let gid = std::cell::Cell::new(ElementId::from_bytes([0u8; 16]));
    d.transact(|tx| {
        let g = tx.acl().grant_element(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            card,
            cid(1),
        );
        gid.set(g);
    });
    // Delete the card so its id no longer resolves in the live tree.
    path::xml_child_delete(&mut d, &col_a(), 0);
    assert!(
        element_paths(&d).get(&card).is_none(),
        "the card is unresolvable after deletion"
    );

    // A root-reading projection keeps the tuple (root fallback) — as the op-stream does.
    let mut keep = Document::decode_state(&d.encode_state()).expect("round-trip");
    keep.project_read_paths(|_path| true);
    assert!(
        keep.acl_tuple(gid.get()).is_some(),
        "a root reader keeps the unresolvable-element tuple, matching the op-stream"
    );

    // A projection that cannot read root drops it — as the op-stream withholds a
    // root-gated op from a non-root reader.
    let mut cut = Document::decode_state(&d.encode_state()).expect("round-trip");
    cut.project_read_paths(|path| !path.is_empty());
    assert!(
        cut.acl_tuple(gid.get()).is_none(),
        "a reader that cannot read root drops the unresolvable-element tuple"
    );
}

/// Emit one element-scoped `AclGrant` op against `d` and return its wire bytes.
fn grant_op_bytes(d: &mut Document, scope: AclScope) -> Vec<u8> {
    let subject = AclSubject::Actor(cid(2));
    let ops = d.transact(|tx| {
        tx.acl().grant_scoped(
            subject.clone(),
            cap(Capability::Read),
            AclEffect::Allow,
            scope.clone(),
            cid(1),
        );
    });
    let op = ops
        .iter()
        .find(|o| matches!(&o.kind, crdtsync_core::OpKind::AclGrant { .. }))
        .expect("an AclGrant op");
    encode_op(op)
}

trait WithGrantor {
    fn with_grantor(self, g: ClientId) -> Self;
}
impl WithGrantor for AclRecord {
    fn with_grantor(mut self, g: ClientId) -> Self {
        self.tuple.grantor = g;
        self
    }
}

/// A small linear-congruential PRNG — deterministic, seedable, reproducible.
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
    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

/// A Fisher-Yates permutation of `0..len` under the PRNG.
fn shuffle(len: usize, rng: &mut Rng) -> Vec<usize> {
    let mut out: Vec<usize> = (0..len).collect();
    for i in (1..out.len()).rev() {
        out.swap(i, rng.below(i + 1));
    }
    out
}

#[test]
fn element_scoped_acl_and_moves_settle_the_same_read_decision_under_reorder() {
    // The convergence property behind element-scoped ACL: a whole-doc read allow, an
    // element-scoped Deny(Read) on a card, an XmlMove that relocates the card, and a
    // revoke of the deny — delivered in any order to a fresh replica — must fold to one
    // read decision at the card's converged position. An element scope resolves against
    // the *current* tree, so the move and the grant/revoke set must both converge for the
    // decision to; this shuffles them together and pins that they do.
    let mut src = Document::new(cid(1));
    let mut pool: Vec<crdtsync_core::Op> = Vec::new();
    pool.extend(path::xml_fragment(&mut src, &col_a()));
    pool.extend(path::xml_fragment(&mut src, &col_b()));
    pool.extend(path::xml_insert_element(&mut src, &col_a(), 0, b"card"));
    let card = child_id(&src, b"colA", 0);

    let mut deny_id = ElementId::from_bytes([0u8; 16]);
    pool.extend(src.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            encode_path(&[]),
            cid(1),
        );
        deny_id = tx.acl().grant_element(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Deny,
            card,
            cid(1),
        );
    }));
    pool.extend(path::xml_move_child(&mut src, &col_a(), 0, &col_b(), 0));
    pool.extend(src.transact(|tx| tx.acl().revoke(deny_id)));

    // Replay one delivery order into a fresh replica, to a fixpoint so buffered
    // (out-of-order) ops all land, then read A's decision at the card's current path.
    let decide = |order: &[usize]| -> (Option<Vec<u8>>, bool) {
        let mut d = Document::new(cid(9));
        loop {
            let mut progressed = false;
            for &i in order {
                if d.apply(&pool[i]) {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        let records = d.acl_records();
        let idx = element_paths(&d);
        let path = idx.get(&card).cloned();
        let resolve = |id| idx.get(&id).cloned();
        let decision = match &path {
            Some(p) => {
                evaluate_with_authority(&records, cid(1), &actor(2), p, Capability::Read, &resolve)
            }
            None => false,
        };
        (path, decision)
    };

    let forward: Vec<usize> = (0..pool.len()).collect();
    let reference = decide(&forward);
    // The card converges under colB, and A's read of it is decided one way.
    assert_eq!(reference.0.as_deref(), Some(col_b().as_slice()));

    let reverse: Vec<usize> = (0..pool.len()).rev().collect();
    assert_eq!(decide(&reverse), reference, "reversed order diverged");

    let rounds = if cfg!(miri) { 3 } else { 40 };
    let mut rng = Rng::new(0xE1E1);
    for round in 0..rounds {
        let order = shuffle(pool.len(), &mut rng);
        assert_eq!(decide(&order), reference, "shuffle {round} diverged");
    }
}
