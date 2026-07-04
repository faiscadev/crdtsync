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

/// The identity a credential resolves to: the actor id the server trusts for the
/// connection, plus the roles and groups the credential asserts. Membership is
/// read from the credential — the engine never decides it — and authorization
/// matches a subject against this identity (an actor id against `actor`, a group
/// against `groups`, a role against `roles`).
///
/// Roles and groups are captured here at the handshake; the policy evaluator
/// begins consuming them with the role-grant tier.
///
/// There is deliberately no `Default`: an identity always names an actor, so a
/// caller constructs it via [`Identity::new`] or [`Identity::with_claims`] —
/// this rules out an *accidental* empty-actor identity (a stray
/// `Identity::default()` marking a session authenticated). The actor bytes are
/// whatever the verifier derives; a verifier that yields an empty actor (e.g.
/// dev-mode [`AllowAll`] on an empty credential) is the verifier's concern.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Identity {
    actor: Vec<u8>,
    roles: Vec<String>,
    groups: Vec<String>,
}

impl Identity {
    /// An identity that asserts only an actor id — no roles, no groups.
    pub fn new(actor: impl Into<Vec<u8>>) -> Self {
        Identity {
            actor: actor.into(),
            roles: Vec::new(),
            groups: Vec::new(),
        }
    }

    /// An identity asserting `actor` with the given role and group membership.
    pub fn with_claims(actor: impl Into<Vec<u8>>, roles: Vec<String>, groups: Vec<String>) -> Self {
        Identity {
            actor: actor.into(),
            roles,
            groups,
        }
    }

    /// The server-trusted actor id.
    pub fn actor(&self) -> &[u8] {
        &self.actor
    }

    /// The roles the credential asserts, in declaration order.
    pub fn roles(&self) -> &[String] {
        &self.roles
    }

    /// The groups the credential asserts, in declaration order.
    pub fn groups(&self) -> &[String] {
        &self.groups
    }
}

/// Turns a presented credential into a server-trusted [`Identity`], or rejects
/// it.
pub trait Verifier {
    /// The identity for `credential`, or `None` to refuse the connection. The
    /// bytes are opaque; an implementation interprets them (a signed token, an
    /// API key) and derives the actor and its claims itself — it must not echo
    /// attacker-chosen bytes back as identity in production.
    fn verify(&self, credential: &[u8]) -> Option<Identity>;
}

/// A verifier from a plain closure, so a deployment (or a test) can supply the
/// mapping inline.
impl<F> Verifier for F
where
    F: Fn(&[u8]) -> Option<Identity>,
{
    fn verify(&self, credential: &[u8]) -> Option<Identity> {
        self(credential)
    }
}

/// Dev-mode verifier: accepts any credential and adopts it verbatim as the
/// actor id (no roles or groups). It performs no real validation and lets the
/// client pick its own actor — for local development and tests only, never
/// production.
pub struct AllowAll;

impl Verifier for AllowAll {
    fn verify(&self, credential: &[u8]) -> Option<Identity> {
        Some(Identity::new(credential.to_vec()))
    }
}

/// A verifier over a fixed table of secret credential → actor id. A credential in
/// the table authenticates as its mapped actor; one not in it is refused. The
/// actor is server-side, so a policy's `actor:` and subject-class rules become
/// real boundaries — a client cannot pick its own actor as it can under
/// [`AllowAll`].
#[derive(Clone, Default)]
pub struct StaticTokens {
    tokens: HashMap<Vec<u8>, Identity>,
}

/// Redacted — the table's keys are secrets, so a debug print names only how many
/// entries there are, never a credential or an actor.
impl std::fmt::Debug for StaticTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokens")
            .field("entries", &self.tokens.len())
            .finish()
    }
}

impl StaticTokens {
    /// An empty table — refuses every credential until entries are added.
    pub fn new() -> Self {
        Self::default()
    }

    /// Map a secret `credential` to the `actor` it authenticates as (with no
    /// roles or groups), replacing any existing mapping for that credential.
    pub fn insert(&mut self, credential: impl Into<Vec<u8>>, actor: impl Into<Vec<u8>>) {
        self.tokens.insert(credential.into(), Identity::new(actor));
    }

    /// Map a secret `credential` to a full [`Identity`] (actor plus roles and
    /// groups), replacing any existing mapping for that credential.
    pub fn insert_identity(&mut self, credential: impl Into<Vec<u8>>, identity: Identity) {
        self.tokens.insert(credential.into(), identity);
    }

    /// Parse a credentials table from text. One entry per line,
    /// `<credential> <actor> [roles] [groups]` (whitespace-separated tokens);
    /// blank lines and `#` comment lines are ignored. `roles` and `groups` are
    /// comma-separated name lists (`editor,viewer`); a lone `-` is an empty
    /// list, used to give groups while asserting no roles. The credential and
    /// actor are raw UTF-8 bytes — the `actor` bytes are what a policy
    /// references as `actor:<hex>`.
    ///
    /// Parsing is total: a line outside two-to-four fields, one repeating a
    /// credential, or one with an empty role/group name yields a
    /// [`CredentialsError`] naming its physical line, never a panic.
    pub fn from_credentials(text: &str) -> Result<Self, CredentialsError> {
        let mut table = StaticTokens::new();
        for (i, raw) in text.lines().enumerate() {
            let line = i + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = trimmed.split_whitespace().collect();
            if fields.len() < 2 || fields.len() > 4 {
                return Err(CredentialsError {
                    line,
                    kind: CredentialsErrorKind::Arity(fields.len()),
                });
            }
            let credential = fields[0].as_bytes().to_vec();
            if table.tokens.contains_key(&credential) {
                return Err(CredentialsError {
                    line,
                    kind: CredentialsErrorKind::DuplicateCredential,
                });
            }
            let roles = parse_name_list(fields.get(2).copied(), line)?;
            let groups = parse_name_list(fields.get(3).copied(), line)?;
            let identity = Identity::with_claims(fields[1].as_bytes().to_vec(), roles, groups);
            table.tokens.insert(credential, identity);
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
    fn verify(&self, credential: &[u8]) -> Option<Identity> {
        self.tokens.get(credential).cloned()
    }
}

/// Parse a comma-separated role/group field into names. An absent field or a
/// lone `-` is an empty list; an empty name (a stray comma) is rejected.
fn parse_name_list(field: Option<&str>, line: usize) -> Result<Vec<String>, CredentialsError> {
    let names = match field {
        None | Some("-") => return Ok(Vec::new()),
        Some(f) => f,
    };
    let mut out = Vec::new();
    for name in names.split(',') {
        if name.is_empty() {
            return Err(CredentialsError {
                line,
                kind: CredentialsErrorKind::EmptyName,
            });
        }
        out.push(name.to_string());
    }
    Ok(out)
}

/// Why a credentials line failed to parse. `Arity` carries the field count found.
/// The credential itself is a secret, so it is never carried in the error — the
/// line number localizes the offending entry.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CredentialsErrorKind {
    /// A line held this many whitespace-separated fields, outside the two-to-four
    /// an entry allows (`credential actor [roles] [groups]`).
    Arity(usize),
    /// A credential was mapped on more than one line — an ambiguous config.
    DuplicateCredential,
    /// A role or group list held an empty name (a stray or leading/trailing
    /// comma).
    EmptyName,
}

impl std::fmt::Display for CredentialsErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialsErrorKind::Arity(n) => {
                write!(
                    f,
                    "expected 2 to 4 fields (credential actor [roles] [groups]), found {n}"
                )
            }
            // The credential is a secret — name the fault, not the token.
            CredentialsErrorKind::DuplicateCredential => {
                write!(f, "a credential is mapped more than once")
            }
            CredentialsErrorKind::EmptyName => {
                write!(f, "a role or group list has an empty name")
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
        assert_eq!(
            table.verify(b"secret-alice"),
            Some(Identity::new(b"alice".to_vec()))
        );
        assert_eq!(
            table.verify(b"secret-bob"),
            Some(Identity::new(b"bob".to_vec()))
        );
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
        assert_eq!(
            table.verify(b"secret-alice"),
            Some(Identity::new(b"alice".to_vec()))
        );
    }

    #[test]
    fn a_two_field_row_carries_no_roles_or_groups() {
        let table = StaticTokens::from_credentials("secret-alice alice").unwrap();
        let id = table.verify(b"secret-alice").unwrap();
        assert_eq!(id.actor(), b"alice");
        assert!(id.roles().is_empty());
        assert!(id.groups().is_empty());
    }

    #[test]
    fn roles_and_groups_parse_from_the_row() {
        let table =
            StaticTokens::from_credentials("secret-alice alice editor,viewer eng,design").unwrap();
        let id = table.verify(b"secret-alice").unwrap();
        assert_eq!(id.actor(), b"alice");
        assert_eq!(id.roles(), ["editor", "viewer"]);
        assert_eq!(id.groups(), ["eng", "design"]);
    }

    #[test]
    fn a_three_field_row_carries_roles_and_no_groups() {
        let table = StaticTokens::from_credentials("secret-alice alice editor,viewer").unwrap();
        let id = table.verify(b"secret-alice").unwrap();
        assert_eq!(id.actor(), b"alice");
        assert_eq!(id.roles(), ["editor", "viewer"]);
        assert!(id.groups().is_empty());
    }

    #[test]
    fn a_dash_is_an_empty_list() {
        // A `-` in the roles slot lets a row assert groups but no roles.
        let table = StaticTokens::from_credentials("secret-alice alice - eng").unwrap();
        let id = table.verify(b"secret-alice").unwrap();
        assert!(id.roles().is_empty());
        assert_eq!(id.groups(), ["eng"]);
    }

    #[test]
    fn an_empty_name_in_a_list_is_rejected() {
        let e = StaticTokens::from_credentials("secret-alice alice a,,b").unwrap_err();
        assert_eq!(e.line, 1);
        assert!(matches!(e.kind, CredentialsErrorKind::EmptyName));
    }

    #[test]
    fn a_row_outside_two_to_four_fields_is_an_arity_error() {
        let one = StaticTokens::from_credentials("secret-alice").unwrap_err();
        assert_eq!(one.line, 1);
        assert!(matches!(one.kind, CredentialsErrorKind::Arity(1)));
        let five = StaticTokens::from_credentials("cred actor roles groups extra").unwrap_err();
        assert!(matches!(five.kind, CredentialsErrorKind::Arity(5)));
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
            CredentialsErrorKind::DuplicateCredential
        ));
        assert!(
            !err.to_string().contains("secret"),
            "the error must not leak the credential: {err}"
        );
    }

    #[test]
    fn debug_does_not_leak_the_credentials() {
        let table = StaticTokens::from_credentials("secret-alice alice").unwrap();
        let shown = format!("{table:?}");
        assert!(
            !shown.contains("secret-alice"),
            "credential leaked: {shown}"
        );
        assert!(!shown.contains("alice"), "actor leaked: {shown}");
    }

    #[test]
    fn the_same_actor_may_hold_several_credentials() {
        // Distinct credentials mapping to one actor is fine — two devices, say.
        let table = StaticTokens::from_credentials(
            "laptop-token alice\n\
             phone-token  alice",
        )
        .unwrap();
        assert_eq!(
            table.verify(b"laptop-token"),
            Some(Identity::new(b"alice".to_vec()))
        );
        assert_eq!(
            table.verify(b"phone-token"),
            Some(Identity::new(b"alice".to_vec()))
        );
    }

    #[test]
    fn insert_builds_a_table_programmatically() {
        let mut table = StaticTokens::new();
        table.insert(b"tok".to_vec(), b"carol".to_vec());
        assert_eq!(table.verify(b"tok"), Some(Identity::new(b"carol".to_vec())));

        table.insert_identity(
            b"tok2".to_vec(),
            Identity::with_claims(b"dave".to_vec(), vec!["admin".into()], vec![]),
        );
        let id = table.verify(b"tok2").unwrap();
        assert_eq!(id.actor(), b"dave");
        assert_eq!(id.roles(), ["admin"]);
    }
}
