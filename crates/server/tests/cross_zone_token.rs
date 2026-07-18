//! Cross-zone move via an AEAD capability token (Zones Unit 4) — the authorized
//! escape hatch for the cross-zone tree move the op-submit gate otherwise rejects.
//!
//! A cross-zone move is rejected by default (Zones 1b-ii-b): the per-zone lamport
//! clocks never order across zones. Zones-4 admits **one** specifically-authorized
//! crossing through a server-sealed capability token that binds
//! `(room, actor, element, src zone, dst zone, expiry)`. The server issues the token
//! only to an actor with move authority on the element and write authority to the
//! destination zone; at ingress it decrypts and authenticates the token under its
//! zone key and admits the move only when the sealed binding matches the op's actual
//! crossing and has not expired. Every other cross-zone move — un-tokened, forged,
//! expired, or bound to a different move — stays rejected, the op never entering the
//! log so replicas converge on its absence.

use std::sync::Mutex;

use crdtsync_core::protocol::Channel;
use crdtsync_core::xml::XmlFragment;
use crdtsync_core::{zone, ClientId, Document, ElementId, ErrorCode, Message, Op, Schema};
use crdtsync_server::auth::{AllowAll, Identity};
use crdtsync_server::authz::{Action, Authorizer, Resource};
use crdtsync_server::index::element_paths;
use crdtsync_server::{
    step, CrossZoneGrant, Hub, PermitAll, Response, SchemaRegistry, Session, ZoneSealer,
};

const ROOM: &[u8] = b"room-1";
const CH: Channel = Channel(0);
const KEY: [u8; 32] = [0x5a; 32];
/// The credential the handshake presents; `AllowAll` derives the actor from it.
const CRED: &[u8] = b"alice";

/// `za` roots at `/board`, `zb` at `/notes`; `/loose` is unzoned.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Frag", "notes": "Frag", "loose": "Frag" } },
        "Frag": { "kind": "fragment", "children": { "a": {} } },
        "a": { "kind": "xml", "tag": "a", "children": {} }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn zoned() -> Schema {
    Schema::parse(ZONED).expect("schema parses")
}

fn frag_id(d: &Document, key: &[u8]) -> ElementId {
    XmlFragment::node_id(d.root_id(), key)
}

/// A doc with `board`, `notes`, `loose` fragments and one child `a` in the board
/// fragment; returns the doc, the setup ops, and the child id.
fn doc_with_child_in_board() -> (Document, Vec<Op>, ElementId) {
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let ops = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        child = board.children().insert_element(0, b"a").id();
        tx.xml_fragment(b"notes");
        tx.xml_fragment(b"loose");
    });
    (d, ops, child)
}

/// Drive one message with a real zone key installed on the hub, the given deployment
/// authorizer, and `now` — so the cross-zone-token path is exercised end to end.
fn drive(
    h: &mut Hub,
    s: &mut Session,
    authorizer: &dyn Authorizer,
    now: u64,
    msg: Message,
) -> Response {
    step(
        h,
        s,
        &AllowAll,
        authorizer,
        Some(&zoned()),
        &Mutex::new(SchemaRegistry::new()),
        None,
        None,
        now,
        None,
        msg,
    )
}

/// Handshake + subscribe as `CRED`.
fn handshake(h: &mut Hub, s: &mut Session, authorizer: &dyn Authorizer) {
    drive(
        h,
        s,
        authorizer,
        0,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    drive(
        h,
        s,
        authorizer,
        0,
        Message::Auth {
            credential: CRED.to_vec(),
        },
    );
    let r = drive(
        h,
        s,
        authorizer,
        0,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    );
    assert!(!r.close, "subscribe establishes the channel");
}

fn is_forbidden(r: &Response) -> bool {
    r.replies.iter().any(|m| {
        matches!(
            m,
            Message::OpsRejected {
                reason: ErrorCode::Forbidden,
                ..
            }
        )
    })
}

fn granted_token(r: &Response) -> Option<Vec<u8>> {
    r.replies.iter().find_map(|m| match m {
        Message::CrossZoneTokenGrant { token, .. } => Some(token.clone()),
        _ => None,
    })
}

/// A hub seeded with the board/notes/loose setup and a zone key installed.
fn seeded_hub() -> (Hub, Document, ElementId) {
    let (d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    h.set_zone_key(KEY);
    h.ingest(ROOM, setup, None).expect("in-memory ingest");
    (h, d, child)
}

/// The move ops relocating `child` from board into notes (za → zb).
fn move_to_notes(d: &mut Document, child: ElementId) -> Vec<Op> {
    let notes = frag_id(d, b"notes");
    d.transact(|tx| tx.move_xml(child, notes, 0))
}

fn correct_grant(child: ElementId, expiry: u64) -> CrossZoneGrant {
    CrossZoneGrant {
        room: ROOM.to_vec(),
        actor: CRED.to_vec(),
        element: child,
        src_zone: b"za".to_vec(),
        dst_zone: b"zb".to_vec(),
        expiry,
    }
}

// --- issuance ---

#[test]
fn an_authorized_actor_is_granted_a_token() {
    let (mut h, _d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"zb".to_vec(),
        },
    );
    let token = granted_token(&r).expect("an authorized request is granted a token");
    // The token opens under the server's key to exactly the requested binding, with
    // the src zone derived from the element's current position.
    let grant = ZoneSealer::new(KEY).open(&token).expect("token opens");
    assert_eq!(grant.room, ROOM);
    assert_eq!(grant.actor, CRED);
    assert_eq!(grant.element, child);
    assert_eq!(grant.src_zone, b"za");
    assert_eq!(grant.dst_zone, b"zb");
    assert!(grant.expiry > 0, "an expiry is stamped");
}

/// Deny write to the destination zone `zb`; every other action allowed.
struct DenyDstZone;
impl Authorizer for DenyDstZone {
    fn authorize(&self, _id: &Identity, action: Action, res: &Resource) -> bool {
        !matches!(
            (action, res),
            (Action::Write, Resource::Zone { zone, .. }) if *zone == b"zb"
        )
    }
}

#[test]
fn issuance_is_denied_when_the_actor_lacks_dst_zone_write() {
    let (mut h, _d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &DenyDstZone);

    let r = drive(
        &mut h,
        &mut s,
        &DenyDstZone,
        0,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"zb".to_vec(),
        },
    );
    assert!(granted_token(&r).is_none(), "no token is minted");
    assert!(
        r.replies.iter().any(|m| matches!(
            m,
            Message::Error {
                code: ErrorCode::Forbidden,
                ..
            }
        )),
        "the denial is a recoverable Forbidden"
    );
}

#[test]
fn issuance_is_denied_for_an_unknown_element() {
    let (mut h, _d, _child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: ElementId::from_bytes([0x99; 16]),
            dst_zone: b"zb".to_vec(),
        },
    );
    assert!(
        granted_token(&r).is_none(),
        "no token for an unresolved element"
    );
}

#[test]
fn issuance_is_denied_for_an_undeclared_destination_zone() {
    let (mut h, _d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"nope".to_vec(),
        },
    );
    assert!(
        granted_token(&r).is_none(),
        "no token for a nonexistent zone"
    );
}

#[test]
fn issuance_is_denied_when_no_zone_key_is_configured() {
    let (_d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    // No set_zone_key — the escape hatch is off.
    h.ingest(ROOM, setup, None).expect("ingest");
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"zb".to_vec(),
        },
    );
    assert!(granted_token(&r).is_none(), "no key, no token");
}

// --- redemption: the accepted move + convergence ---

#[test]
fn a_server_issued_token_accepts_the_cross_zone_move_and_converges() {
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    // Request the token, then submit the move redeeming it.
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"zb".to_vec(),
        },
    );
    let token = granted_token(&r).expect("token granted");

    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneOps {
            channel: CH,
            ops: mv.clone(),
            token,
        },
    );
    assert!(!is_forbidden(&r), "the tokened cross-zone move is accepted");
    assert_eq!(h.seq(ROOM), before + 1, "the move entered the log");

    // Convergence: a fresh replica folding the setup + the fanned-out move op places
    // the child in the destination zone, exactly as the leader committed it.
    let (_d2, setup2, child2) = doc_with_child_in_board();
    let mut replica = Document::new(cid(2));
    for op in &setup2 {
        replica.apply(op);
    }
    for op in &r.broadcast {
        replica.apply(op);
    }
    let paths = element_paths(&replica);
    let landed = paths.get(&child2).expect("child resolves in the replica");
    assert_eq!(
        zone::zone_of(&zoned(), landed),
        Some("zb"),
        "the moved child converges in the destination zone on the replica"
    );
}

// --- redemption: the gate holds for everything else ---

#[test]
fn an_untokened_cross_zone_move_is_still_rejected_even_with_a_key() {
    // The escape hatch does not weaken the default: a plain `Ops` cross-zone move is
    // refused exactly as before, key or no key.
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::Ops {
            channel: CH,
            ops: mv,
        },
    );
    assert!(
        is_forbidden(&r),
        "an un-tokened cross-zone move is rejected"
    );
    assert_eq!(h.seq(ROOM), before, "no op was logged");
}

#[test]
fn a_forged_garbage_token_is_rejected() {
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneOps {
            channel: CH,
            ops: mv,
            token: b"not a real sealed token".to_vec(),
        },
    );
    assert!(
        is_forbidden(&r),
        "a garbage token fails AEAD auth and is rejected"
    );
    assert_eq!(h.seq(ROOM), before, "no op was logged");
}

#[test]
fn a_token_from_a_foreign_key_is_rejected() {
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    // A correctly-bound grant sealed under a *different* key — the tag fails to
    // authenticate under the server's key.
    let token = ZoneSealer::new([0x11; 32]).seal(&correct_grant(child, 1_000_000));
    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneOps {
            channel: CH,
            ops: mv,
            token,
        },
    );
    assert!(is_forbidden(&r), "a foreign-key token is rejected");
    assert_eq!(h.seq(ROOM), before, "no op was logged");
}

#[test]
fn an_expired_token_is_rejected() {
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    // A correctly-bound token that expired at t=10; redeem at t=11.
    let token = ZoneSealer::new(KEY).seal(&correct_grant(child, 10));
    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        11,
        Message::CrossZoneOps {
            channel: CH,
            ops: mv,
            token,
        },
    );
    assert!(is_forbidden(&r), "an expired token is rejected");
    assert_eq!(h.seq(ROOM), before, "no op was logged");
}

/// Redeem a token whose binding differs from the actual move in exactly one field —
/// the redemption must reject each, proving the binding is enforced dimension by
/// dimension. The token is validly sealed under the server key, so only the binding
/// mismatch (not the AEAD tag) rejects it.
fn assert_mismatched_binding_rejected(mutate: impl Fn(&mut CrossZoneGrant)) {
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let mut grant = correct_grant(child, 1_000_000);
    mutate(&mut grant);
    let token = ZoneSealer::new(KEY).seal(&grant);

    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneOps {
            channel: CH,
            ops: mv,
            token,
        },
    );
    assert!(is_forbidden(&r), "a mismatched binding is rejected");
    assert_eq!(h.seq(ROOM), before, "no op was logged");
}

#[test]
fn a_token_bound_to_a_different_element_is_rejected() {
    assert_mismatched_binding_rejected(|g| g.element = ElementId::from_bytes([0x22; 16]));
}

#[test]
fn a_token_bound_to_a_different_actor_is_rejected() {
    assert_mismatched_binding_rejected(|g| g.actor = b"mallory".to_vec());
}

#[test]
fn a_token_bound_to_a_different_src_zone_is_rejected() {
    assert_mismatched_binding_rejected(|g| g.src_zone = b"zb".to_vec());
}

#[test]
fn a_token_bound_to_a_different_dst_zone_is_rejected() {
    // The actual move lands in zb; a token authorizing a move into the unzoned root
    // (empty dst) does not authorize it.
    assert_mismatched_binding_rejected(|g| g.dst_zone = Vec::new());
}

#[test]
fn a_token_bound_to_a_different_room_is_rejected() {
    assert_mismatched_binding_rejected(|g| g.room = b"other-room".to_vec());
}

// --- a correctly test-sealed token is accepted (isolates the one-field mismatches) ---

#[test]
fn a_correctly_sealed_token_accepts_the_move() {
    let (mut h, mut d, child) = seeded_hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);

    let token = ZoneSealer::new(KEY).seal(&correct_grant(child, 1_000_000));
    let before = h.seq(ROOM);
    let mv = move_to_notes(&mut d, child);
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::CrossZoneOps {
            channel: CH,
            ops: mv,
            token,
        },
    );
    assert!(
        !is_forbidden(&r),
        "a correctly-bound token accepts the move"
    );
    assert_eq!(h.seq(ROOM), before + 1, "the move entered the log");
}

// --- a same-zone move needs no token (unchanged) ---

#[test]
fn a_same_zone_move_needs_no_token() {
    let schema = zoned();
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let setup = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        let mut kids = board.children();
        child = kids.insert_element(0, b"a").id();
        kids.insert_element(1, b"a");
    });
    let mut h = Hub::new(cid(0xFF));
    h.set_zone_key(KEY);
    h.ingest(ROOM, setup, None).expect("ingest");
    let mut s = Session::new();
    handshake(&mut h, &mut s, &PermitAll);
    let before = h.seq(ROOM);

    // A reorder within the board zone commits through the plain `Ops` path.
    let board = frag_id(&d, b"board");
    let mv = d.transact(|tx| tx.move_xml(child, board, 1));
    let _ = &schema;
    let r = drive(
        &mut h,
        &mut s,
        &PermitAll,
        0,
        Message::Ops {
            channel: CH,
            ops: mv,
        },
    );
    assert!(
        !is_forbidden(&r),
        "a same-zone move is accepted without a token"
    );
    assert_eq!(h.seq(ROOM), before + 1, "the move was logged");
}
