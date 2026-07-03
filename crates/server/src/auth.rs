//! Credential verification at the handshake.
//!
//! The engine ships no identity provider. A [`Verifier`] is the deployment's
//! seam: it turns the opaque credential a client presents at Auth into the
//! actor id the server trusts for that connection, or rejects it. Apps bring
//! their own (JWT, OIDC, custom); the engine only calls `verify` and never lets
//! the client name its own actor.
//!
//! [`StaticTokens`] is the engine's minimal built-in verifier: a fixed table of
//! secret credential → actor mappings an operator declares in a file. It is real
//! validation — an unknown credential is refused and the actor comes from the
//! table, not the client — enough to make a declared policy enforceable without
//! embedding, while staying short of an identity provider (no issuance, login,
//! or user management).

use std::collections::HashMap;

/// Turns a presented credential into a server-trusted actor id, or rejects it.
pub trait Verifier {
    /// The actor id for `credential`, or `None` to refuse the connection. The
    /// bytes are opaque; an implementation interprets them (a signed token, an
    /// API key) and derives the actor itself — it must not echo attacker-chosen
    /// bytes back as identity in production.
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>>;
}

/// A verifier from a plain closure, so a deployment (or a test) can supply the
/// mapping inline.
impl<F> Verifier for F
where
    F: Fn(&[u8]) -> Option<Vec<u8>>,
{
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>> {
        self(credential)
    }
}

/// Dev-mode verifier: accepts any credential and adopts it verbatim as the
/// actor id. It performs no real validation and lets the client pick its own
/// actor — for local development and tests only, never production.
pub struct AllowAll;

impl Verifier for AllowAll {
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>> {
        Some(credential.to_vec())
    }
}

/// A verifier over a fixed table of secret credential → actor id. A credential in
/// the table authenticates as its mapped actor; one not in it is refused. The
/// actor is server-side, so a policy's `actor:` and subject-class rules become
/// real boundaries — a client cannot pick its own actor as it can under
/// [`AllowAll`].
#[derive(Clone, Default, Debug)]
pub struct StaticTokens {
    tokens: HashMap<Vec<u8>, Vec<u8>>,
}

impl StaticTokens {
    /// An empty table — refuses every credential until entries are added.
    pub fn new() -> Self {
        Self::default()
    }

    /// Map a secret `credential` to the `actor` it authenticates as, replacing any
    /// existing mapping for that credential.
    pub fn insert(&mut self, credential: impl Into<Vec<u8>>, actor: impl Into<Vec<u8>>) {
        self.tokens.insert(credential.into(), actor.into());
    }

    /// Parse a credentials table from text. One entry per line,
    /// `<credential> <actor>` (whitespace-separated literal tokens); blank lines
    /// and `#` comment lines are ignored. Both tokens are taken as raw UTF-8
    /// bytes — the `actor` bytes are what a policy references as `actor:<hex>`.
    /// Parsing is total: a line that is not exactly two fields, or that repeats a
    /// credential, yields a [`CredentialsError`] naming its physical line, never a
    /// panic.
    pub fn from_credentials(text: &str) -> Result<Self, CredentialsError> {
        let mut table = StaticTokens::new();
        for (i, raw) in text.lines().enumerate() {
            let line = i + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = trimmed.split_whitespace().collect();
            if fields.len() != 2 {
                return Err(CredentialsError {
                    line,
                    kind: CredentialsErrorKind::Arity(fields.len()),
                });
            }
            let credential = fields[0].as_bytes().to_vec();
            if table.tokens.contains_key(&credential) {
                return Err(CredentialsError {
                    line,
                    kind: CredentialsErrorKind::DuplicateCredential(fields[0].into()),
                });
            }
            table
                .tokens
                .insert(credential, fields[1].as_bytes().to_vec());
        }
        Ok(table)
    }

    /// Load a credentials table from a file at `path` — read it, then
    /// [`from_credentials`](StaticTokens::from_credentials) its contents. The file
    /// being unreadable and its contents being malformed are distinct
    /// [`CredentialsFileError`] arms.
    pub fn from_credentials_file(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, CredentialsFileError> {
        let text = std::fs::read_to_string(path)?;
        Ok(Self::from_credentials(&text)?)
    }
}

impl Verifier for StaticTokens {
    fn verify(&self, credential: &[u8]) -> Option<Vec<u8>> {
        self.tokens.get(credential).cloned()
    }
}

/// Why a credentials line failed to parse. `Arity` carries the field count found;
/// `DuplicateCredential` carries the repeated credential.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CredentialsErrorKind {
    /// A line held this many whitespace-separated fields, not the two an entry
    /// requires.
    Arity(usize),
    /// A credential was mapped on more than one line — an ambiguous config.
    DuplicateCredential(String),
}

impl std::fmt::Display for CredentialsErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialsErrorKind::Arity(n) => {
                write!(f, "expected 2 fields (credential actor), found {n}")
            }
            CredentialsErrorKind::DuplicateCredential(c) => {
                write!(f, "credential \"{c}\" is mapped more than once")
            }
        }
    }
}

/// A failure to parse a credentials table, pinned to the 1-based physical line it
/// occurred on.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CredentialsError {
    pub line: usize,
    pub kind: CredentialsErrorKind,
}

impl std::fmt::Display for CredentialsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.kind)
    }
}

impl std::error::Error for CredentialsError {}

/// Why loading a credentials file failed: the file could not be read, or its
/// contents did not parse.
#[derive(Debug)]
pub enum CredentialsFileError {
    /// The file could not be read.
    Io(std::io::Error),
    /// The file was read but a line did not parse.
    Parse(CredentialsError),
}

impl std::fmt::Display for CredentialsFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialsFileError::Io(e) => write!(f, "reading credentials file: {e}"),
            CredentialsFileError::Parse(e) => write!(f, "parsing credentials file: {e}"),
        }
    }
}

impl std::error::Error for CredentialsFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CredentialsFileError::Io(e) => Some(e),
            CredentialsFileError::Parse(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for CredentialsFileError {
    fn from(e: std::io::Error) -> Self {
        CredentialsFileError::Io(e)
    }
}

impl From<CredentialsError> for CredentialsFileError {
    fn from(e: CredentialsError) -> Self {
        CredentialsFileError::Parse(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_known_credential_authenticates_as_its_actor() {
        let table = StaticTokens::from_credentials(
            "# team credentials\n\
             secret-alice alice\n\
             secret-bob   bob\n",
        )
        .unwrap();
        assert_eq!(table.verify(b"secret-alice"), Some(b"alice".to_vec()));
        assert_eq!(table.verify(b"secret-bob"), Some(b"bob".to_vec()));
    }

    #[test]
    fn an_unknown_credential_is_refused() {
        let table = StaticTokens::from_credentials("secret-alice alice").unwrap();
        assert_eq!(table.verify(b"secret-bob"), None);
        assert_eq!(table.verify(b""), None);
    }

    #[test]
    fn an_empty_table_refuses_everything() {
        let table = StaticTokens::from_credentials("# nothing here\n").unwrap();
        assert_eq!(table.verify(b"anything"), None);
    }

    #[test]
    fn blank_lines_comments_and_whitespace_are_ignored() {
        let table =
            StaticTokens::from_credentials("\n   # a note\n   secret-alice    alice   \n\n")
                .unwrap();
        assert_eq!(table.verify(b"secret-alice"), Some(b"alice".to_vec()));
    }

    #[test]
    fn a_line_without_two_fields_is_an_arity_error() {
        let one = StaticTokens::from_credentials("secret-alice").unwrap_err();
        assert_eq!(one.line, 1);
        assert!(matches!(one.kind, CredentialsErrorKind::Arity(1)));
        let three = StaticTokens::from_credentials("cred actor extra").unwrap_err();
        assert!(matches!(three.kind, CredentialsErrorKind::Arity(3)));
    }

    #[test]
    fn a_repeated_credential_is_a_duplicate_error_on_its_line() {
        let err = StaticTokens::from_credentials(
            "secret alice\n\
             secret bob",
        )
        .unwrap_err();
        assert_eq!(err.line, 2, "the second mapping is the conflict");
        assert!(matches!(
            err.kind,
            CredentialsErrorKind::DuplicateCredential(_)
        ));
    }

    #[test]
    fn the_same_actor_may_hold_several_credentials() {
        // Distinct credentials mapping to one actor is fine — two devices, say.
        let table = StaticTokens::from_credentials(
            "laptop-token alice\n\
             phone-token  alice",
        )
        .unwrap();
        assert_eq!(table.verify(b"laptop-token"), Some(b"alice".to_vec()));
        assert_eq!(table.verify(b"phone-token"), Some(b"alice".to_vec()));
    }

    #[test]
    fn insert_builds_a_table_programmatically() {
        let mut table = StaticTokens::new();
        table.insert(b"tok".to_vec(), b"carol".to_vec());
        assert_eq!(table.verify(b"tok"), Some(b"carol".to_vec()));
    }
}
