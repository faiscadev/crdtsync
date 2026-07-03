//! A minimal JSON value parser.
//!
//! The schema file is JSON, and core is its sole validator, so core reads JSON
//! itself rather than pulling in a serialization crate — the same hand-rolled,
//! total-decode discipline as the binary codec. [`Json::parse`] turns a string
//! into a [`Json`] value or a [`JsonError`]; it never panics, and it bounds
//! nesting depth so a hostile document cannot overflow the stack.

/// A parsed JSON value. Integers keep full `i64` range distinct from floats,
/// since a schema's versions and bounds are integers; objects keep declaration
/// order.
#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// The string, if this is a `Str`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    /// The integer, if this is an `Int`. A `Float` is not coerced — a bound or
    /// version written with a decimal point is a distinct value.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// The boolean, if this is a `Bool`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// The items, if this is an `Array`.
    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(a) => Some(a),
            _ => None,
        }
    }

    /// The key/value pairs in declaration order, if this is an `Object`.
    pub fn as_object(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Object(o) => Some(o),
            _ => None,
        }
    }

    /// The value for `key`, if this is an `Object` that has it. Keys are unique
    /// (a duplicate is a parse error), so the first match is the only one.
    pub fn get(&self, key: &str) -> Option<&Json> {
        self.as_object()?
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }
}

/// Why a JSON string failed to parse.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonErrorKind {
    /// The input ended while a value was still expected.
    UnexpectedEof,
    /// A byte appeared where the grammar did not allow it.
    Unexpected,
    /// A string escape was malformed.
    BadEscape,
    /// A number was not a valid JSON number.
    BadNumber,
    /// A `\u` escape named no valid Unicode scalar (e.g. a lone surrogate).
    BadUnicode,
    /// An object repeated a key.
    DuplicateKey,
    /// Non-whitespace bytes remained after the top-level value.
    TrailingBytes,
    /// Nesting ran deeper than the parser admits.
    DepthLimit,
}

/// A parse failure, pinned to the byte offset it occurred at.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct JsonError {
    pub at: usize,
    pub kind: JsonErrorKind,
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            JsonErrorKind::UnexpectedEof => "unexpected end of input",
            JsonErrorKind::Unexpected => "unexpected byte",
            JsonErrorKind::BadEscape => "invalid string escape",
            JsonErrorKind::BadNumber => "invalid number",
            JsonErrorKind::BadUnicode => "invalid unicode escape",
            JsonErrorKind::DuplicateKey => "duplicate object key",
            JsonErrorKind::TrailingBytes => "trailing bytes after value",
            JsonErrorKind::DepthLimit => "nesting too deep",
        };
        write!(f, "{what} at byte {}", self.at)
    }
}

impl std::error::Error for JsonError {}

/// The most deeply an array/object may nest before parsing gives up — a
/// stack-overflow guard against a hostile document, not a real-schema limit.
const MAX_DEPTH: usize = 128;

impl Json {
    /// Parse one JSON value. Total — any input yields a value or a
    /// [`JsonError`], never a panic. Bytes after the value (other than
    /// whitespace) are a [`TrailingBytes`](JsonErrorKind::TrailingBytes) error.
    pub fn parse(input: &str) -> Result<Json, JsonError> {
        let mut p = Parser {
            bytes: input.as_bytes(),
            at: 0,
        };
        p.skip_ws();
        let value = p.value(0)?;
        p.skip_ws();
        if p.at != p.bytes.len() {
            return Err(p.err(JsonErrorKind::TrailingBytes));
        }
        Ok(value)
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn err(&self, kind: JsonErrorKind) -> JsonError {
        JsonError { at: self.at, kind }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
                self.at += 1;
            } else {
                break;
            }
        }
    }

    /// Consume `lit` if it is next, else an `Unexpected` error.
    fn literal(&mut self, lit: &[u8], value: Json) -> Result<Json, JsonError> {
        if self.bytes[self.at..].starts_with(lit) {
            self.at += lit.len();
            Ok(value)
        } else {
            Err(self.err(JsonErrorKind::Unexpected))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, JsonError> {
        match self
            .peek()
            .ok_or_else(|| self.err(JsonErrorKind::UnexpectedEof))?
        {
            b'n' => self.literal(b"null", Json::Null),
            b't' => self.literal(b"true", Json::Bool(true)),
            b'f' => self.literal(b"false", Json::Bool(false)),
            b'"' => Ok(Json::Str(self.string()?)),
            b'[' => self.array(depth),
            b'{' => self.object(depth),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(self.err(JsonErrorKind::Unexpected)),
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, JsonError> {
        if depth + 1 > MAX_DEPTH {
            return Err(self.err(JsonErrorKind::DepthLimit));
        }
        self.at += 1; // consume '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value(depth + 1)?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b']') => {
                    self.at += 1;
                    return Ok(Json::Array(items));
                }
                Some(_) => return Err(self.err(JsonErrorKind::Unexpected)),
                None => return Err(self.err(JsonErrorKind::UnexpectedEof)),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, JsonError> {
        if depth + 1 > MAX_DEPTH {
            return Err(self.err(JsonErrorKind::DepthLimit));
        }
        self.at += 1; // consume '{'
        let mut pairs: Vec<(String, Json)> = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(Json::Object(pairs));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(self.err(match self.peek() {
                    None => JsonErrorKind::UnexpectedEof,
                    Some(_) => JsonErrorKind::Unexpected,
                }));
            }
            let key_at = self.at;
            let key = self.string()?;
            if pairs.iter().any(|(k, _)| *k == key) {
                return Err(JsonError {
                    at: key_at,
                    kind: JsonErrorKind::DuplicateKey,
                });
            }
            self.skip_ws();
            if self.peek() != Some(b':') {
                return Err(self.err(match self.peek() {
                    None => JsonErrorKind::UnexpectedEof,
                    Some(_) => JsonErrorKind::Unexpected,
                }));
            }
            self.at += 1; // consume ':'
            self.skip_ws();
            let value = self.value(depth + 1)?;
            pairs.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    return Ok(Json::Object(pairs));
                }
                Some(_) => return Err(self.err(JsonErrorKind::Unexpected)),
                None => return Err(self.err(JsonErrorKind::UnexpectedEof)),
            }
        }
    }

    /// Parse a string literal, the opening quote next.
    fn string(&mut self) -> Result<String, JsonError> {
        self.at += 1; // consume opening '"'
        let mut out = String::new();
        loop {
            let b = self
                .peek()
                .ok_or_else(|| self.err(JsonErrorKind::UnexpectedEof))?;
            match b {
                b'"' => {
                    self.at += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.at += 1;
                    self.escape(&mut out)?;
                }
                // A control byte must be escaped.
                0x00..=0x1F => return Err(self.err(JsonErrorKind::Unexpected)),
                _ => {
                    // Copy one whole UTF-8 scalar. The input is a `&str`, so a
                    // byte >= 0x80 begins a valid multi-byte sequence.
                    let start = self.at;
                    self.at += utf8_len(b);
                    let slice = self
                        .bytes
                        .get(start..self.at)
                        .ok_or_else(|| self.err(JsonErrorKind::UnexpectedEof))?;
                    // Safe: `bytes` came from a `&str` and we stepped a whole scalar.
                    out.push_str(std::str::from_utf8(slice).map_err(|_| JsonError {
                        at: start,
                        kind: JsonErrorKind::Unexpected,
                    })?);
                }
            }
        }
    }

    /// Handle one escape sequence, the backslash already consumed.
    fn escape(&mut self, out: &mut String) -> Result<(), JsonError> {
        let b = self
            .peek()
            .ok_or_else(|| self.err(JsonErrorKind::UnexpectedEof))?;
        let simple = match b {
            b'"' => Some('"'),
            b'\\' => Some('\\'),
            b'/' => Some('/'),
            b'b' => Some('\u{0008}'),
            b'f' => Some('\u{000C}'),
            b'n' => Some('\n'),
            b'r' => Some('\r'),
            b't' => Some('\t'),
            _ => None,
        };
        if let Some(c) = simple {
            self.at += 1;
            out.push(c);
            return Ok(());
        }
        if b == b'u' {
            self.at += 1; // consume 'u'
            let c = self.unicode_escape()?;
            out.push(c);
            return Ok(());
        }
        Err(self.err(JsonErrorKind::BadEscape))
    }

    /// Parse the four hex digits of a `\uXXXX` escape (the `\u` already
    /// consumed), combining a surrogate pair into one scalar. A lone surrogate is
    /// [`BadUnicode`](JsonErrorKind::BadUnicode).
    fn unicode_escape(&mut self) -> Result<char, JsonError> {
        let first = self.hex4()?;
        // A high surrogate must be followed by a `\u` low surrogate.
        if (0xD800..=0xDBFF).contains(&first) {
            let pair_at = self.at;
            if self.peek() != Some(b'\\') || self.bytes.get(self.at + 1) != Some(&b'u') {
                return Err(JsonError {
                    at: pair_at,
                    kind: JsonErrorKind::BadUnicode,
                });
            }
            self.at += 2; // consume "\u"
            let low = self.hex4()?;
            if !(0xDC00..=0xDFFF).contains(&low) {
                return Err(JsonError {
                    at: pair_at,
                    kind: JsonErrorKind::BadUnicode,
                });
            }
            let combined = 0x10000 + (((first - 0xD800) as u32) << 10) + (low - 0xDC00) as u32;
            return char::from_u32(combined).ok_or(JsonError {
                at: pair_at,
                kind: JsonErrorKind::BadUnicode,
            });
        }
        // A lone low surrogate is invalid.
        char::from_u32(first as u32).ok_or_else(|| self.err(JsonErrorKind::BadUnicode))
    }

    /// Read exactly four hex digits into a `u16`, the digits positioned next.
    fn hex4(&mut self) -> Result<u16, JsonError> {
        let mut value: u16 = 0;
        for _ in 0..4 {
            let d = self
                .peek()
                .ok_or_else(|| self.err(JsonErrorKind::UnexpectedEof))?;
            let nibble = match d {
                b'0'..=b'9' => d - b'0',
                b'a'..=b'f' => d - b'a' + 10,
                b'A'..=b'F' => d - b'A' + 10,
                _ => return Err(self.err(JsonErrorKind::BadUnicode)),
            };
            value = (value << 4) | nibble as u16;
            self.at += 1;
        }
        Ok(value)
    }

    fn number(&mut self) -> Result<Json, JsonError> {
        let start = self.at;
        let mut is_float = false;
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        // Integer part: `0` alone, or a non-zero digit then any digits. A leading
        // zero (`01`, `00`) is not a JSON number.
        match self.peek() {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => self.digits()?,
            _ => return Err(self.err(JsonErrorKind::BadNumber)),
        }
        if matches!(self.peek(), Some(b'0'..=b'9')) {
            return Err(JsonError {
                at: start,
                kind: JsonErrorKind::BadNumber,
            });
        }
        if self.peek() == Some(b'.') {
            is_float = true;
            self.at += 1;
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            self.digits()?;
        }
        let text = std::str::from_utf8(&self.bytes[start..self.at]).map_err(|_| JsonError {
            at: start,
            kind: JsonErrorKind::BadNumber,
        })?;
        if !is_float {
            if let Ok(n) = text.parse::<i64>() {
                return Ok(Json::Int(n));
            }
            // An integer past i64 range degrades to a float rather than failing.
        }
        text.parse::<f64>().map(Json::Float).map_err(|_| JsonError {
            at: start,
            kind: JsonErrorKind::BadNumber,
        })
    }

    /// Consume one or more decimal digits, else a `BadNumber` error.
    fn digits(&mut self) -> Result<(), JsonError> {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        if self.at == start {
            Err(self.err(JsonErrorKind::BadNumber))
        } else {
            Ok(())
        }
    }
}

/// The length in bytes of the UTF-8 scalar that starts with `b`.
fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}
