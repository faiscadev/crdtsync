//! The minimal HTTP/1.1 request-head parser for the admin control plane.
//!
//! It reads only what schema registration needs and rejects everything else,
//! fail-loud: a request line (`METHOD SP TARGET SP HTTP/1.1`), header lines
//! terminated by a blank line, and a `Content-Length` framing the body the
//! transport then reads. `Ok(None)` means the head is not yet complete (read
//! more); a malformed line, an unsupported version, chunked transfer-encoding, a
//! bad or duplicate `Content-Length`, or an over-large head is an error — never a
//! panic, over any byte input.

use crdtsync_server::http::{parse_head, HeadError};

fn parse(bytes: &[u8]) -> Result<Option<()>, HeadError> {
    parse_head(bytes).map(|o| o.map(|_| ()))
}

#[test]
fn a_well_formed_post_head_parses() {
    let raw = b"POST /apps/app-x/schemas/1 HTTP/1.1\r\nHost: admin\r\nContent-Length: 12\r\n\r\n";
    let head = parse_head(raw).unwrap().expect("head complete");
    assert_eq!(head.method(), "POST");
    assert_eq!(head.target(), "/apps/app-x/schemas/1");
    assert_eq!(head.content_length(), 12);
    assert_eq!(
        head.head_len(),
        raw.len(),
        "head spans up to and incl the blank line"
    );
}

#[test]
fn a_head_without_the_blank_line_terminator_needs_more() {
    // No CRLFCRLF yet: not malformed, just incomplete.
    let raw = b"POST /x HTTP/1.1\r\nContent-Length: 3\r\n";
    assert_eq!(parse(raw), Ok(None));
    // Even an empty buffer is merely incomplete.
    assert_eq!(parse(b""), Ok(None));
}

#[test]
fn header_lookup_is_case_insensitive() {
    let raw = b"POST /x HTTP/1.1\r\nContent-Length: 5\r\nAuthorization: Bearer sekret\r\n\r\n";
    let head = parse_head(raw).unwrap().unwrap();
    assert_eq!(head.header("authorization"), Some("Bearer sekret"));
    assert_eq!(head.header("AUTHORIZATION"), Some("Bearer sekret"));
    assert_eq!(head.header("content-length"), Some("5"));
    assert_eq!(head.header("x-absent"), None);
}

#[test]
fn an_absent_content_length_is_zero() {
    let raw = b"POST /x HTTP/1.1\r\nHost: admin\r\n\r\n";
    let head = parse_head(raw).unwrap().unwrap();
    assert_eq!(head.content_length(), 0);
}

#[test]
fn the_body_after_the_head_is_not_consumed_by_the_parser() {
    // Trailing body bytes past the blank line are left for the transport; the
    // head still reports its own length and the declared content length.
    let raw = b"POST /x HTTP/1.1\r\nContent-Length: 4\r\n\r\nbody-and-more";
    let head = parse_head(raw).unwrap().unwrap();
    assert_eq!(head.content_length(), 4);
    assert_eq!(&raw[head.head_len()..head.head_len() + 4], b"body");
}

#[test]
fn a_malformed_request_line_is_rejected() {
    // Two tokens, not three.
    assert_eq!(parse(b"POST /x\r\n\r\n"), Err(HeadError::BadRequestLine));
    // Empty method / target.
    assert_eq!(
        parse(b" /x HTTP/1.1\r\n\r\n"),
        Err(HeadError::BadRequestLine)
    );
    assert_eq!(
        parse(b"POST  HTTP/1.1\r\n\r\n"),
        Err(HeadError::BadRequestLine)
    );
}

#[test]
fn a_non_http_11_version_is_rejected() {
    assert_eq!(
        parse(b"POST /x HTTP/1.0\r\n\r\n"),
        Err(HeadError::UnsupportedVersion)
    );
    assert_eq!(
        parse(b"POST /x GARBAGE\r\n\r\n"),
        Err(HeadError::UnsupportedVersion)
    );
}

#[test]
fn a_header_without_a_colon_is_rejected() {
    assert_eq!(
        parse(b"POST /x HTTP/1.1\r\nBadHeaderNoColon\r\n\r\n"),
        Err(HeadError::BadHeader)
    );
}

#[test]
fn a_non_numeric_or_duplicate_content_length_is_rejected() {
    assert_eq!(
        parse(b"POST /x HTTP/1.1\r\nContent-Length: abc\r\n\r\n"),
        Err(HeadError::BadContentLength)
    );
    assert_eq!(
        parse(b"POST /x HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n"),
        Err(HeadError::DuplicateContentLength)
    );
}

#[test]
fn a_transfer_encoding_header_is_rejected() {
    // Only Content-Length framing is supported; chunked opens request smuggling.
    assert_eq!(
        parse(b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
        Err(HeadError::UnsupportedTransferEncoding)
    );
}

#[test]
fn an_over_large_head_is_rejected_not_buffered_forever() {
    // A head that grows past the cap without terminating is an error, not an
    // endless "need more" — a memory-exhaustion guard mirroring the codec's.
    let mut raw = Vec::from(&b"POST /x HTTP/1.1\r\n"[..]);
    while raw.len() < 64 * 1024 {
        raw.extend_from_slice(b"X-Pad: paddingpaddingpadding\r\n");
    }
    assert_eq!(parse(&raw), Err(HeadError::HeadTooLarge));
}

#[test]
fn bare_lf_line_endings_are_rejected() {
    // Line endings must be CRLF: accepting bare LF as well is a request-smuggling
    // ambiguity, so a bare-LF request line or header line is malformed even when
    // the head still terminates with a proper blank line.
    assert_eq!(
        parse(b"POST /x HTTP/1.1\nContent-Length: 3\r\n\r\n"),
        Err(HeadError::BadRequestLine),
        "a bare-LF request line"
    );
    assert_eq!(
        parse(b"POST /x HTTP/1.1\r\nContent-Length: 3\nHost: a\r\n\r\n"),
        Err(HeadError::BadHeader),
        "a bare-LF-separated header line"
    );
}

#[test]
fn arbitrary_bytes_never_panic() {
    let hostile: &[&[u8]] = &[
        b"\x00\x01\x02",
        b"POST",
        b"\r\n\r\n",
        b"GET / HTTP/1.1\r\n\r\n",
        b"POST /x HTTP/1.1\r\nContent-Length: 99999999999999999999\r\n\r\n",
        b"\xff\xfe HTTP/1.1\r\n\r\n",
        b"POST /x HTTP/1.1\r\n: novalue-name\r\n\r\n",
    ];
    for input in hostile {
        // Any result is fine; a panic is not.
        let _ = parse(input);
    }
}
