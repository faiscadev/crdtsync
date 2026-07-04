//! Differential oracle for the hand-rolled JSON parser.
//!
//! `serde_json` is a dev-only dependency here (it never enters the shipped
//! core/wasm/ffi graph). This suite cross-checks `Json::parse` against it over
//! many generated inputs: for each input the two parsers must agree on whether
//! it is valid JSON, and when both accept, on the value — modulo three
//! deliberate divergences the core documents:
//!
//!   * a duplicate object key is a hard error here, "keep last" in serde;
//!   * a number whose magnitude overflows `f64` is `Float(inf)` here, a range
//!     error in serde;
//!   * an integer past `i64` degrades to a float — a value difference the
//!     f64-normalised comparison already tolerates.
//!
//! Inputs are capped at 120 bytes, below the 128-deep nesting bound, so the
//! depth limit never fires and never counts as a divergence. The generator is a
//! seeded xorshift so a failure reproduces exactly.

use crdtsync_core::json::{Json, JsonErrorKind};

/// Deterministic xorshift64* — reproducible so any counterexample is stable.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// A number-collapsing normal form so the two value models can be compared:
/// int and float both become `f64`, and objects are keyed as a sorted map (our
/// parser preserves order, serde's default `Value` sorts — order is not what we
/// are checking here).
#[derive(PartialEq, Debug)]
enum Norm {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<Norm>),
    Obj(Vec<(String, Norm)>),
}

fn norm_ours(j: &Json) -> Norm {
    match j {
        Json::Null => Norm::Null,
        Json::Bool(b) => Norm::Bool(*b),
        Json::Int(n) => Norm::Num(*n as f64),
        Json::Float(f) => Norm::Num(*f),
        Json::Str(s) => Norm::Str(s.clone()),
        Json::Array(a) => Norm::Arr(a.iter().map(norm_ours).collect()),
        Json::Object(o) => {
            let mut pairs: Vec<(String, Norm)> =
                o.iter().map(|(k, v)| (k.clone(), norm_ours(v))).collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Norm::Obj(pairs)
        }
    }
}

fn norm_serde(v: &serde_json::Value) -> Norm {
    match v {
        serde_json::Value::Null => Norm::Null,
        serde_json::Value::Bool(b) => Norm::Bool(*b),
        serde_json::Value::Number(n) => Norm::Num(n.as_f64().expect("finite json number")),
        serde_json::Value::String(s) => Norm::Str(s.clone()),
        serde_json::Value::Array(a) => Norm::Arr(a.iter().map(norm_serde).collect()),
        serde_json::Value::Object(m) => {
            let mut pairs: Vec<(String, Norm)> =
                m.iter().map(|(k, v)| (k.clone(), norm_serde(v))).collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Norm::Obj(pairs)
        }
    }
}

/// True if any number in the value we parsed is non-finite — the documented
/// `Float(inf)` divergence, which serde reports as a range error instead.
fn has_non_finite(j: &Json) -> bool {
    match j {
        Json::Float(f) => !f.is_finite(),
        Json::Array(a) => a.iter().any(has_non_finite),
        Json::Object(o) => o.iter().any(|(_, v)| has_non_finite(v)),
        _ => false,
    }
}

/// Compare the two parsers on one input, accounting for the documented
/// divergences. Returns `Err(reason)` on a genuine disagreement.
fn check(input: &str) -> Result<(), String> {
    let ours = Json::parse(input);
    let theirs = serde_json::from_str::<serde_json::Value>(input);

    match (&ours, &theirs) {
        (Ok(o), Ok(t)) => {
            // A "keep last" object in serde cannot match our reject-on-dup, but
            // when both accept there were no dup keys, so values must agree.
            let (no, nt) = (norm_ours(o), norm_serde(t));
            if no != nt {
                return Err(format!("value mismatch: ours={no:?} theirs={nt:?}"));
            }
            Ok(())
        }
        (Err(e), Ok(_)) => {
            // Only a duplicate key legitimately splits us from serde here.
            if e.kind == JsonErrorKind::DuplicateKey {
                Ok(())
            } else {
                Err(format!("we reject ({:?}) but serde accepts", e.kind))
            }
        }
        (Ok(o), Err(_)) => {
            // The only value we accept that serde rejects is a non-finite float.
            if has_non_finite(o) {
                Ok(())
            } else {
                Err("we accept but serde rejects".to_string())
            }
        }
        (Err(_), Err(_)) => Ok(()),
    }
}

const ALPHABET: &[u8] = b"{}[]\":,0123456789.-+eEtruefalsn \\/ux \t\n\"\"abcDEF\0";

/// A random string over a JSON-flavoured alphabet — mostly invalid, which is
/// exactly where a hand-rolled parser is most likely to diverge or panic.
fn random_bytes(rng: &mut Rng) -> String {
    let len = rng.below(120);
    let mut buf = Vec::with_capacity(len);
    for _ in 0..len {
        buf.push(ALPHABET[rng.below(ALPHABET.len())]);
    }
    // The parser takes `&str`; keep inputs valid UTF-8 (control bytes included).
    String::from_utf8_lossy(&buf).into_owned()
}

/// A structurally valid JSON document, bounded well under the depth limit. This
/// side of the fuzzer exercises value-level agreement rather than error paths.
fn random_json(rng: &mut Rng, depth: usize) -> String {
    if depth >= 4 || rng.below(3) == 0 {
        return match rng.below(6) {
            0 => "null".to_string(),
            1 => "true".to_string(),
            2 => "false".to_string(),
            3 => format!("{}", rng.next_u64() as i64),
            4 => format!("{}.{}", rng.below(1000), rng.below(1000)),
            _ => random_string_literal(rng),
        };
    }
    if rng.below(2) == 0 {
        let n = rng.below(4);
        let items: Vec<String> = (0..n).map(|_| random_json(rng, depth + 1)).collect();
        format!("[{}]", items.join(","))
    } else {
        let n = rng.below(4);
        // Distinct keys by index so a generated object never trips dup-key.
        let pairs: Vec<String> = (0..n)
            .map(|i| format!("\"k{i}\":{}", random_json(rng, depth + 1)))
            .collect();
        format!("{{{}}}", pairs.join(","))
    }
}

fn random_string_literal(rng: &mut Rng) -> String {
    let choices = [
        r#""""#,
        r#""abc""#,
        r#""a\nb\t\"c""#,
        r#""Aé""#,
        r#""😀""#, // a surrogate pair (😀)
        r#""\/\\\b\f\r""#,
        r#""unicode ★ é ✓""#,
    ];
    choices[rng.below(choices.len())].to_string()
}

#[test]
fn random_bytes_agree_with_serde() {
    let mut rng = Rng(0x1234_5678_9abc_def0);
    for i in 0..200_000 {
        let input = random_bytes(&mut rng);
        if let Err(why) = check(&input) {
            panic!("iter {i}: {why}\ninput = {input:?}");
        }
    }
}

#[test]
fn structured_json_agrees_with_serde() {
    let mut rng = Rng(0x0fed_cba9_8765_4321);
    for i in 0..100_000 {
        let input = random_json(&mut rng, 0);
        if let Err(why) = check(&input) {
            panic!("iter {i}: {why}\ninput = {input:?}");
        }
    }
}

/// A few explicit corners the random streams are unlikely to hit densely, kept
/// as named regressions.
#[test]
fn known_corners_agree_with_serde() {
    let corners = [
        "1e999",                    // overflow -> Float(inf) here, range error in serde
        "-1e999",                   // same, negative
        "1e-999",                   // underflows to a finite 0.0 both sides
        "18446744073709551616",     // 2^64, past i64 -> float
        "9223372036854775807",      // i64::MAX
        "-9223372036854775808",     // i64::MIN
        "[]",
        "{}",
        "\"\\ud83d\\ude00\"",       // valid surrogate pair
        "\"\\ud83d\"",              // lone high surrogate -> both reject
        "\"\\udc00\"",              // lone low surrogate -> both reject
        "  \n\t 42 \r\n ",          // surrounding whitespace
        "01",                       // leading zero -> both reject
        "1.",                       // dangling point -> both reject
        ".5",                       // no integer part -> both reject
        "+1",                       // leading plus -> both reject
        "nan",
        "Infinity",
        "",                         // empty -> both reject
        "1 2",                      // trailing value -> both reject
        "\"unterminated",
    ];
    for c in corners {
        check(c).unwrap_or_else(|why| panic!("corner {c:?}: {why}"));
    }
}
