//! Codec negotiation at the server's Hello gate.
//!
//! The client advertises the codecs it speaks; the server selects one, records it
//! on the session, and echoes the selection back before any schema advertisement.
//! A client that advertises nothing settles on the codec that predates
//! negotiation and is answered no selection frame at all — the connection is
//! byte-for-byte what it was before the seam existed. A client that shares no
//! codec with this build is closed with a clean `UnsupportedVersion` error rather
//! than served frames it would misdecode.

use std::sync::Mutex;

use crdtsync_core::{ClientId, ErrorCode, Message, CODEC_V1, SUPPORTED_CODECS};
use crdtsync_server::{step, AllowAll, Hub, PermitAll, SchemaRegistry, Session};

const APP: &[u8] = b"app-x";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hello(app_id: &[u8], codecs: Vec<u32>) -> Message {
    Message::Hello {
        client: cid(1),
        app_id: app_id.to_vec(),
        schema_version: 0,
        codecs,
    }
}

fn resolve(reg: &Mutex<SchemaRegistry>, msg: Message) -> (Session, crdtsync_server::Response) {
    let mut hub = Hub::new(cid(0xFF));
    let mut session = Session::new();
    let resp = step(
        &mut hub,
        &mut session,
        &AllowAll,
        &PermitAll,
        None,
        reg,
        None,
        None,
        0,
        None,
        msg,
    );
    (session, resp)
}

fn empty() -> Mutex<SchemaRegistry> {
    Mutex::new(SchemaRegistry::new())
}

fn registered() -> Mutex<SchemaRegistry> {
    let mut r = SchemaRegistry::new();
    r.register(APP, 1, br#"{"v":1}"#, b"").unwrap();
    Mutex::new(r)
}

#[test]
fn an_advertising_client_is_answered_the_selected_codec() {
    let (session, resp) = resolve(&empty(), hello(b"", SUPPORTED_CODECS.to_vec()));
    assert!(!resp.close);
    assert_eq!(session.codec(), CODEC_V1);
    assert!(
        matches!(
            resp.replies.as_slice(),
            [Message::CodecSelected { codec }] if *codec == CODEC_V1,
        ),
        "an advertisement is answered with the selection, got {:?}",
        resp.replies
    );
}

#[test]
fn versions_this_build_does_not_speak_are_ignored_in_the_selection() {
    // A newer client offering a codec this build never heard of still settles on
    // the shared one — that is the whole point of the reservation.
    let (session, resp) = resolve(&empty(), hello(b"", vec![CODEC_V1, 900]));
    assert!(!resp.close);
    assert_eq!(session.codec(), CODEC_V1);
    assert!(matches!(
        resp.replies.as_slice(),
        [Message::CodecSelected { codec: 1 }]
    ));
}

#[test]
fn a_client_that_advertises_nothing_defaults_to_the_current_codec() {
    // The peer that predates negotiation: no advertisement, no selection frame,
    // and the session on the codec every build holds.
    let (session, resp) = resolve(&empty(), hello(b"", Vec::new()));
    assert!(!resp.close);
    assert_eq!(session.client(), Some(cid(1)));
    assert_eq!(session.codec(), CODEC_V1);
    assert!(
        resp.replies.is_empty(),
        "an unadvertised handshake is answered exactly as before, got {:?}",
        resp.replies
    );
}

#[test]
fn a_client_sharing_no_codec_is_closed_with_unsupported_version() {
    let (session, resp) = resolve(&empty(), hello(b"", vec![800, 900]));
    assert!(resp.close, "the connection cannot be served, so it closes");
    assert!(
        matches!(
            resp.replies.as_slice(),
            [Message::Error {
                code: ErrorCode::UnsupportedVersion,
                ..
            }]
        ),
        "expected a clean protocol error, got {:?}",
        resp.replies
    );
    assert_eq!(
        session.client(),
        None,
        "a refused handshake binds no client to the session"
    );
}

#[test]
fn the_codec_is_settled_before_the_schema_is_resolved() {
    // A client the registry would refuse anyway is refused on the codec first —
    // there is no point resolving a schema for a peer that cannot read the answer.
    let (session, resp) = resolve(&registered(), hello(APP, vec![900]));
    assert!(resp.close);
    assert!(matches!(
        resp.replies.as_slice(),
        [Message::Error {
            code: ErrorCode::UnsupportedVersion,
            ..
        }]
    ));
    assert_eq!(session.schema_version(), None);
}

#[test]
fn the_selection_precedes_the_schema_advert() {
    let (session, resp) = resolve(&registered(), hello(APP, SUPPORTED_CODECS.to_vec()));
    assert!(!resp.close);
    assert_eq!(session.codec(), CODEC_V1);
    assert_eq!(session.schema_version(), Some(1));
    assert!(
        matches!(
            resp.replies.as_slice(),
            [
                Message::CodecSelected { codec: 1 },
                Message::SchemaAdvert {
                    schema_version: 1,
                    ..
                }
            ]
        ),
        "the codec settles before the schema is advertised, got {:?}",
        resp.replies
    );
}

#[test]
fn an_enforcing_client_that_advertises_nothing_still_gets_its_schema_advert() {
    let (session, resp) = resolve(&registered(), hello(APP, Vec::new()));
    assert!(!resp.close);
    assert_eq!(session.codec(), CODEC_V1);
    assert!(matches!(
        resp.replies.as_slice(),
        [Message::SchemaAdvert {
            schema_version: 1,
            ..
        }]
    ));
}

#[test]
fn a_client_cannot_send_a_codec_selection() {
    // The selection is the server's own answer; a client that sends one is out of
    // protocol.
    let (_, resp) = resolve(&empty(), Message::CodecSelected { codec: CODEC_V1 });
    assert!(resp.close);
    assert!(matches!(
        resp.replies.as_slice(),
        [Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }]
    ));
}
