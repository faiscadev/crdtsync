//! Codec negotiation at the server's Hello gate.
//!
//! The client advertises the codecs it speaks and the server selects one, records
//! it on the session, and answers with a `CodecSelected` — but only when the
//! selection moves off the default, since both ends read silence as `CODEC_V1`.
//! With one codec to select, every handshake settles on it, so the reply stream is
//! exactly what it was before the seam existed. A client that shares no codec with
//! this build is closed with a clean `UnsupportedVersion` error rather than served
//! frames it would misdecode.

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
fn an_advertising_client_settles_on_the_selected_codec() {
    let (session, resp) = resolve(&empty(), hello(b"", SUPPORTED_CODECS.to_vec()));
    assert!(!resp.close);
    assert_eq!(session.codec(), Some(CODEC_V1));
    assert!(
        resp.replies.is_empty(),
        "the selection is the default, which silence already carries, got {:?}",
        resp.replies
    );
}

#[test]
fn versions_this_build_does_not_speak_are_ignored_in_the_selection() {
    // A newer client offering a codec this build never heard of still settles on
    // the shared one — that is the whole point of the reservation.
    let (session, resp) = resolve(&empty(), hello(b"", vec![CODEC_V1, 900]));
    assert!(!resp.close);
    assert_eq!(session.codec(), Some(CODEC_V1));
    assert!(resp.replies.is_empty());
}

#[test]
fn a_client_that_advertises_nothing_defaults_to_the_current_codec() {
    // The peer that predates negotiation: no advertisement, no selection frame,
    // and the session on the codec every build holds.
    let (session, resp) = resolve(&empty(), hello(b"", Vec::new()));
    assert!(!resp.close);
    assert_eq!(session.client(), Some(cid(1)));
    assert_eq!(session.codec(), Some(CODEC_V1));
    assert!(
        resp.replies.is_empty(),
        "an unadvertised handshake is answered exactly as before, got {:?}",
        resp.replies
    );
}

#[test]
fn a_fresh_session_has_settled_on_no_codec_yet() {
    assert_eq!(Session::new().codec(), None);
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
    assert_eq!(
        session.codec(),
        None,
        "a refused handshake settles on no codec"
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
    assert_eq!(session.codec(), None);
}

#[test]
fn an_enforcing_handshake_is_answered_the_schema_advert_alone() {
    // The reply stream an enforcing client sees is unchanged by negotiation: the
    // schema advert and nothing ahead of it, advertised or not.
    for codecs in [Vec::new(), SUPPORTED_CODECS.to_vec()] {
        let (session, resp) = resolve(&registered(), hello(APP, codecs));
        assert!(!resp.close);
        assert_eq!(session.codec(), Some(CODEC_V1));
        assert_eq!(session.schema_version(), Some(1));
        assert!(
            matches!(
                resp.replies.as_slice(),
                [Message::SchemaAdvert {
                    schema_version: 1,
                    ..
                }]
            ),
            "got {:?}",
            resp.replies
        );
    }
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
