//! A minimal, strict HTTP/1.1 request-head parser for the admin control plane.
//!
//! The schema-registration endpoint speaks just enough HTTP to accept a `POST`
//! from an app owner's CI, so this reads only that shape and rejects everything
//! else fail-loud, in the total-decode style of the wire codec: a request line
//! (`METHOD SP TARGET SP HTTP/1.1`), `CRLF`-terminated header lines, and a blank
//! line ending the head. Framing is `Content-Length` only — chunked
//! transfer-encoding is refused, since supporting both is a request-smuggling
//! surface — and the head is size-capped so a client cannot buffer it without
//! bound. The body the length frames is read by the transport, not here.

/// A parsed request head: the request line, the headers, and the `Content-Length`
/// framing the body that follows. Header names are stored lowercased for
/// case-insensitive lookup; values keep their original casing.
pub struct Head {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    content_length: usize,
    head_len: usize,
}

impl Head {
    /// The request method (e.g. `POST`).
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The request target — the path and any query, verbatim.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The declared body length; `0` when no `Content-Length` was sent.
    pub fn content_length(&self) -> usize {
        self.content_length
    }

    /// The byte length of the head itself, up to and including the blank line —
    /// so the body occupies `bytes[head_len()..head_len() + content_length()]`.
    pub fn head_len(&self) -> usize {
        self.head_len
    }

    /// The value of `name`, matched case-insensitively; `None` if absent.
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Why a request head was rejected. An incomplete head is not an error —
/// [`parse_head`] returns `Ok(None)` for that — so every variant here is a
/// malformed or unsupported request.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HeadError {
    /// The head grew past the size cap without terminating.
    HeadTooLarge,
    /// The request line is not exactly `METHOD SP TARGET SP VERSION`.
    BadRequestLine,
    /// The version token is present but not `HTTP/1.1`.
    UnsupportedVersion,
    /// A header line has no colon, an empty name, or non-UTF-8 bytes.
    BadHeader,
    /// A `Content-Length` value is not a non-negative integer that fits.
    BadContentLength,
    /// More than one `Content-Length` header was sent.
    DuplicateContentLength,
    /// A `Transfer-Encoding` header was sent; only `Content-Length` is supported.
    UnsupportedTransferEncoding,
}

/// The largest request head accepted, headers and terminator included.
const MAX_HEAD_LEN: usize = 16 * 1024;

/// Parse the head of an HTTP/1.1 request from `buf`. Returns `Ok(Some(head))`
/// once the blank line terminating the headers is present, `Ok(None)` if it is
/// not yet (the transport should read more, up to the size cap), and an error
/// for a malformed or unsupported head. Never panics, over any bytes.
pub fn parse_head(buf: &[u8]) -> Result<Option<Head>, HeadError> {
    let Some(term) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        // Not terminated yet: keep reading, unless it has already grown too big.
        return if buf.len() > MAX_HEAD_LEN {
            Err(HeadError::HeadTooLarge)
        } else {
            Ok(None)
        };
    };
    let head_len = term + 4;
    if head_len > MAX_HEAD_LEN {
        return Err(HeadError::HeadTooLarge);
    }

    let mut lines = split_lines(&buf[..term]);
    let request_line = lines.next().ok_or(HeadError::BadRequestLine)?;
    let (method, target) = parse_request_line(request_line)?;

    let mut headers = Vec::new();
    let mut content_length: Option<usize> = None;
    for line in lines {
        let (name, value) = parse_header_line(line)?;
        if name == "transfer-encoding" {
            return Err(HeadError::UnsupportedTransferEncoding);
        }
        if name == "content-length" {
            if content_length.is_some() {
                return Err(HeadError::DuplicateContentLength);
            }
            content_length = Some(value.parse().map_err(|_| HeadError::BadContentLength)?);
        }
        headers.push((name, value));
    }

    Ok(Some(Head {
        method,
        target,
        headers,
        content_length: content_length.unwrap_or(0),
        head_len,
    }))
}

/// Split header bytes into lines on `LF`, dropping a trailing `CR` from each so a
/// `CRLF`-delimited head yields clean lines.
fn split_lines(head: &[u8]) -> impl Iterator<Item = &[u8]> {
    head.split(|&b| b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
}

/// Parse `METHOD SP TARGET SP HTTP/1.1` into its method and target. Exactly three
/// non-empty single-space-separated tokens; a missing third token is malformed,
/// a present-but-wrong one is an unsupported version.
fn parse_request_line(line: &[u8]) -> Result<(String, String), HeadError> {
    let line = std::str::from_utf8(line).map_err(|_| HeadError::BadRequestLine)?;
    let mut parts = line.split(' ');
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let version = parts.next().unwrap_or("");
    if parts.next().is_some() || method.is_empty() || target.is_empty() || version.is_empty() {
        return Err(HeadError::BadRequestLine);
    }
    if version != "HTTP/1.1" {
        return Err(HeadError::UnsupportedVersion);
    }
    Ok((method.to_string(), target.to_string()))
}

/// Parse a `Name: Value` header line, returning the name lowercased and the value
/// trimmed. An empty name, an internal-whitespace name, or no colon is rejected.
fn parse_header_line(line: &[u8]) -> Result<(String, String), HeadError> {
    let line = std::str::from_utf8(line).map_err(|_| HeadError::BadHeader)?;
    let colon = line.find(':').ok_or(HeadError::BadHeader)?;
    let name = &line[..colon];
    if name.is_empty() || name.contains(char::is_whitespace) {
        return Err(HeadError::BadHeader);
    }
    Ok((
        name.to_ascii_lowercase(),
        line[colon + 1..].trim().to_string(),
    ))
}
