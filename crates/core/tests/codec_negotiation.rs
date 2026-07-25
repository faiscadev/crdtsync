//! Codec negotiation over the Hello handshake.
//!
//! The client advertises the binary codecs it speaks in its `Hello`; the server
//! picks one and answers with a `CodecSelected` when the pick moves off the
//! default. Exactly one codec exists today, so the point of the exchange is the
//! reservation: a later release adds a version and still settles with a peer that
//! only holds the older one.
//!
//! Silence carries `CODEC_V1` in both directions — an omitted advertisement and an
//! absent selection both settle there — so a peer that predates negotiation writes
//! and reads exactly the frames it always did. Selection is total: a disjoint
//! advertisement yields no selection (the server refuses the connection) and a
//! malformed one decodes to a `ProtocolError`, never a panic and never a silent
//! wrong-codec decode.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::protocol::{
    decode_message, encode_message, select_codec, Message, ProtocolError, CODEC_V1,
    SUPPORTED_CODECS,
};

mod common;
use common::cid;

fn hello(codecs: Vec<u32>) -> Message {
    Message::Hello {
        client: cid(9),
        app_id: b"acme-notes".to_vec(),
        schema_version: 7,
        codecs,
    }
}

// --- selection ---

#[test]
fn an_unadvertised_peer_settles_on_the_codec_that_predates_negotiation() {
    // No advertisement names no codec — the peer speaks the one every build
    // holds, so it keeps connecting exactly as before negotiation existed.
    assert_eq!(select_codec(&[]), Some(CODEC_V1));
}

#[test]
fn the_highest_shared_version_wins() {
    assert_eq!(select_codec(SUPPORTED_CODECS), Some(CODEC_V1));
    // Versions this build does not hold are ignored; the best shared one is taken.
    assert_eq!(select_codec(&[CODEC_V1, 800, 900]), Some(CODEC_V1));
    // Order of the advertisement does not decide the outcome.
    assert_eq!(select_codec(&[900, CODEC_V1]), Some(CODEC_V1));
}

#[test]
fn a_disjoint_advertisement_selects_nothing() {
    // A client that names only codecs this build cannot speak shares no ground:
    // there is nothing to decode its frames with, so no selection exists.
    assert_eq!(select_codec(&[800]), None);
    assert_eq!(select_codec(&[800, 900, u32::MAX]), None);
    // Zero is not a codec version — the numbering starts at CODEC_V1.
    assert_eq!(select_codec(&[0]), None);
}

#[test]
fn a_duplicated_advertisement_still_selects_once() {
    assert_eq!(select_codec(&[CODEC_V1, CODEC_V1]), Some(CODEC_V1));
}

// --- wire ---

#[test]
fn an_advertisement_round_trips() {
    for codecs in [
        Vec::new(),
        vec![CODEC_V1],
        SUPPORTED_CODECS.to_vec(),
        vec![CODEC_V1, 800, 900],
    ] {
        let msg = hello(codecs);
        let bytes = encode_message(&msg);
        assert_eq!(decode_message(&bytes), Ok(msg));
    }
}

#[test]
fn the_codec_selection_round_trips() {
    let msg = Message::CodecSelected { codec: CODEC_V1 };
    let bytes = encode_message(&msg);
    assert_eq!(decode_message(&bytes), Ok(msg));
}

#[test]
fn an_unadvertised_hello_is_byte_identical_to_the_pre_negotiation_frame() {
    // The advertisement is trailing and omitted when empty, so the frame a peer
    // that names no codec writes is exactly the one both ends exchanged before
    // negotiation: tag, client, app id, schema version, and nothing more.
    let mut expected = vec![0u8];
    expected.extend_from_slice(&cid(9).as_bytes());
    expected.extend_from_slice(&10u32.to_le_bytes());
    expected.extend_from_slice(b"acme-notes");
    expected.extend_from_slice(&7u32.to_le_bytes());
    assert_eq!(encode_message(&hello(Vec::new())), expected);

    // And the same bytes read back as an absent advertisement.
    assert_eq!(decode_message(&expected), Ok(hello(Vec::new())));
}

#[test]
fn an_advertisement_is_counted_and_appended() {
    let mut expected = encode_message(&hello(Vec::new()));
    expected.extend_from_slice(&2u32.to_le_bytes());
    expected.extend_from_slice(&CODEC_V1.to_le_bytes());
    expected.extend_from_slice(&900u32.to_le_bytes());
    assert_eq!(encode_message(&hello(vec![CODEC_V1, 900])), expected);
}

// --- total decode ---

#[test]
fn a_partially_present_advertisement_is_an_error() {
    // Two versions plus the count is 12 trailing bytes; every truncation that
    // leaves some of them fails rather than reading past the frame.
    let bytes = encode_message(&hello(vec![CODEC_V1, 900]));
    for cut in 1..=11 {
        assert_eq!(
            decode_message(&bytes[..bytes.len() - cut]),
            Err(ProtocolError::UnexpectedEof),
            "a Hello cut {cut} bytes short of its advertisement decodes to an error"
        );
    }
    // Losing the advertisement whole is not a truncation — it is the frame a peer
    // that names no codec writes, so it reads as an absent advertisement.
    assert_eq!(
        decode_message(&bytes[..bytes.len() - 12]),
        Ok(hello(Vec::new()))
    );
}

#[test]
fn an_advertisement_naming_no_codec_is_an_error() {
    // The empty set is the omitted field. Spelling it as a present count of zero
    // is a second encoding of one message, so it is refused.
    let mut bytes = encode_message(&hello(Vec::new()));
    bytes.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn a_count_larger_than_the_advertisement_is_an_error() {
    // A count the frame cannot back fails on the missing bytes rather than
    // reserving for versions that were never sent.
    let mut bytes = encode_message(&hello(Vec::new()));
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.extend_from_slice(&CODEC_V1.to_le_bytes());
    assert_eq!(decode_message(&bytes), Err(ProtocolError::UnexpectedEof));
}

#[test]
fn a_partial_count_is_an_error() {
    // Trailing bytes too short to even be a count.
    let mut bytes = encode_message(&hello(Vec::new()));
    bytes.extend_from_slice(&[0xFF, 0xFF]);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::UnexpectedEof));
}

#[test]
fn bytes_after_a_complete_advertisement_are_an_error() {
    let mut bytes = encode_message(&hello(vec![CODEC_V1]));
    bytes.push(0);
    assert_eq!(decode_message(&bytes), Err(ProtocolError::TrailingBytes));
}

#[test]
fn a_truncated_codec_selection_is_an_error() {
    let bytes = encode_message(&Message::CodecSelected { codec: CODEC_V1 });
    assert_eq!(
        decode_message(&bytes[..bytes.len() - 1]),
        Err(ProtocolError::UnexpectedEof)
    );
}

// --- the client side ---

#[test]
fn the_client_hello_advertises_every_codec_this_build_speaks() {
    let session = ClientSession::new(cid(1));
    assert_eq!(
        session.codec(),
        CODEC_V1,
        "a fresh session speaks the default"
    );
    match session.hello() {
        Message::Hello { codecs, .. } => assert_eq!(codecs, SUPPORTED_CODECS),
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[test]
fn the_client_adopts_the_selection_the_server_answers_with() {
    let mut session = ClientSession::new(cid(1));
    assert_eq!(
        session.receive(Message::CodecSelected { codec: CODEC_V1 }),
        Ok(())
    );
    assert_eq!(session.codec(), CODEC_V1);
}

#[test]
fn a_second_selection_is_a_violation() {
    // Neither end could agree on which frame a mid-stream switch takes effect
    // from, so the codec settles once — the mirror of the server refusing a
    // second Hello.
    let mut session = ClientSession::new(cid(1));
    assert_eq!(
        session.receive(Message::CodecSelected { codec: CODEC_V1 }),
        Ok(())
    );
    assert_eq!(
        session.receive(Message::CodecSelected { codec: CODEC_V1 }),
        Err(ClientError::UnexpectedMessage("codec already selected"))
    );
    assert_eq!(session.codec(), CODEC_V1);
}

#[test]
fn the_client_refuses_a_selection_it_cannot_speak() {
    // A server naming a codec this build does not hold would have every later
    // frame misread, so the refusal is permanent: the session reports the same
    // failure for every frame after it rather than reading on.
    let mut session = ClientSession::new(cid(1));
    assert_eq!(
        session.receive(Message::CodecSelected { codec: 900 }),
        Err(ClientError::UnsupportedCodec(900))
    );
    assert_eq!(
        session.receive(Message::AuthOk {
            actor: b"alice".to_vec()
        }),
        Err(ClientError::UnsupportedCodec(900)),
        "a stranded session refuses every later frame, not just the selection"
    );
    assert_eq!(session.actor(), None, "and folds none of them");
}
