//! The schema model and its parse-time validation.
//!
//! A schema is an app-authored JSON file describing a document's shape over the
//! built primitives: a `root` map type and a set of named `types`, each one of
//! map / list / text / register / counter with its constraints. Core is the
//! sole validator, so it reads the file with its own [`json`](crate::json)
//! parser and turns it into a [`Schema`].
//!
//! Parsing is total — any input yields a [`Schema`] or a [`SchemaError`], never
//! a panic — and it enforces the *closure* property at parse time: every type a
//! schema names is declared, `root` is a map, and every numeric bound is
//! well-formed. A schema that passes cannot describe a runtime state the engine
//! has no rule to repair, so invariant repair (a later unit) always has a rule
//! for every violation the accepted schema can produce.
//!
//! This unit is the model and its validation only. Validating a live document's
//! state against the schema lives with invariant repair, where the violation
//! set it produces is consumed — both operate over the materialized element
//! tree, not over JSON.

use crate::json::{Json, JsonError, JsonErrorKind};
use std::collections::HashSet;

/// A parsed, validated schema.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    name: String,
    version: i64,
    root: String,
    types: Vec<(String, TypeDef)>,
    awareness: Vec<(String, AwarenessEntry)>,
    auth: Auth,
}

/// The schema-level `@auth`: the static, role-based access defaults that ship
/// with the app code. It holds *only* a role vocabulary and role/subject-keyed
/// grants; per-instance ownership and per-actor grants are dynamic doc-level ACL
/// state, never declared here.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Auth {
    roles: Vec<String>,
    grants: Vec<Grant>,
}

impl Auth {
    /// The static role vocabulary, in declaration order.
    pub fn roles(&self) -> &[String] {
        &self.roles
    }

    /// The static grants, in declaration order.
    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }
}

/// One static grant: an `effect` on an `action`, for a `subject`, over a `path`
/// (which inherits downward at check time). The concrete JSON is
/// `{ "allow"|"deny": "<action>", "to": "<subject>", "on": "<path>" }` — the
/// effect and the action are fused into the `allow`/`deny` key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub effect: Effect,
    pub action: Action,
    pub subject: Subject,
    pub path: String,
}

/// Whether a grant opens or closes access. An explicit deny wins at check time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// A schema-grantable capability. Ownership (`own`) and the meta-auth actions
/// are deliberately absent: they are dynamic doc-level ACL state, never a static
/// schema default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    PublishAwareness,
}

/// Who a grant applies to: a declared role, a subject class, or an ownership
/// template resolved against the acting identity at check time. A bare actor id
/// is not a schema subject — per-actor grants are dynamic doc-level ACL state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subject {
    /// A role declared in `auth.roles`.
    Role(String),
    /// A subject class (`authenticated` / `anonymous` / `anyone`).
    Class(SubjectClass),
    /// An ownership template (`${actor_id}` etc.) bound at check time.
    Template(TemplateVar),
}

/// A well-known subject class every deployment understands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubjectClass {
    Authenticated,
    Anonymous,
    Anyone,
}

/// A `${…}` template variable a grant subject (the `to` field) resolves
/// against the acting identity / resource at check time. Grant paths are raw
/// strings (structural validation only), so templates appear only in `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateVar {
    ActorId,
    AuthorId,
    RoomId,
    BranchId,
}

/// The declared timing of one kind of ephemeral awareness entry (a cursor, a
/// selection, a presence marker). Both bounds are milliseconds and optional.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AwarenessEntry {
    /// Auto-expire an entry this many milliseconds after its last update
    /// (timed presence). `None` means the entry lives until its owner clears
    /// it or disconnects.
    pub ttl: Option<u64>,
    /// Coalesce inbound updates arriving within this many milliseconds,
    /// keeping only the latest. `None` means no server-side throttle.
    pub throttle: Option<u64>,
}

/// One named type: a built primitive with its declared constraints.
#[derive(Clone, Debug, PartialEq)]
pub enum TypeDef {
    /// A map with named, typed slots (each slot name → the type it holds).
    /// The slot set is the allowlist; declaration order is preserved.
    Map { children: Vec<(String, String)> },
    /// An ordered list of `items`-typed elements, with an optional maximum
    /// count. A minimum count is not expressible: an over-max list is repaired by
    /// dropping its lamport-newest items, but a below-min list cannot be — items
    /// cannot be invented — so a `min` would admit an unrepairable state and
    /// breaks the schema's closure guarantee. Min-cardinality is an app-layer
    /// concern (fixed map slots, or a publish gate).
    List { items: String, max: Option<u64> },
    /// Collaborative text, with an optional maximum length.
    Text { max: Option<u64> },
    /// A last-writer-wins scalar with optional numeric bounds.
    Register { min: Option<i64>, max: Option<i64> },
    /// A counter with optional numeric bounds.
    Counter { min: Option<i64>, max: Option<i64> },
}

/// Why a schema failed to parse or validate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SchemaErrorKind {
    /// The JSON itself did not parse.
    Json(JsonErrorKind),
    /// An object was required (the document, `types`, a type def, `children`)
    /// but the value was not one.
    NotAnObject,
    /// A required field was absent.
    MissingField,
    /// A field was present but had the wrong JSON type.
    WrongType,
    /// A type declared a `kind` that is not a built primitive.
    UnknownKind,
    /// A name (`root`, a map child, a list's items) referenced a type that is
    /// not declared.
    UnknownTypeRef,
    /// `root` named a type that is not a map.
    RootNotMap,
    /// A numeric bound was ill-formed: `min` above `max`, or a negative count.
    BadRange,
    /// A key the schema language does not define at its position (a top-level
    /// key, a type-def field, or an awareness-entry field) — likely a typo,
    /// rejected rather than silently ignored, so a schema is code that fails
    /// loud.
    UnknownField,
    /// A grant did not carry exactly one of `allow` / `deny`.
    BadGrant,
    /// A grant named an action that is not a schema-grantable capability
    /// (`own` and the meta-auth actions are dynamic doc-level ACL state).
    UnknownAction,
    /// A grant subject was a malformed or unknown `${…}` template.
    BadSubject,
    /// A grant `to` named a role that is not declared in `auth.roles`.
    UnknownRoleRef,
    /// A grant `on` was not a well-formed absolute path.
    BadPath,
    /// `auth.roles` declared the same role twice.
    DuplicateRole,
    /// `auth.roles` declared a role with a reserved subject-class name
    /// (`authenticated` / `anonymous` / `anyone`), which would shadow the class.
    ReservedRole,
}

/// A schema failure: a [`SchemaErrorKind`] plus the field or type name it
/// occurred at, for a readable message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SchemaError {
    pub kind: SchemaErrorKind,
    pub at: String,
}

impl SchemaError {
    fn new(kind: SchemaErrorKind, at: impl Into<String>) -> Self {
        SchemaError {
            kind,
            at: at.into(),
        }
    }
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            SchemaErrorKind::Json(k) => {
                return write!(f, "invalid schema JSON ({k:?}) at {}", self.at)
            }
            SchemaErrorKind::NotAnObject => "expected an object",
            SchemaErrorKind::MissingField => "missing required field",
            SchemaErrorKind::WrongType => "field has the wrong type",
            SchemaErrorKind::UnknownKind => "unknown type kind",
            SchemaErrorKind::UnknownTypeRef => "reference to an undeclared type",
            SchemaErrorKind::RootNotMap => "root type is not a map",
            SchemaErrorKind::BadRange => "ill-formed numeric bound",
            SchemaErrorKind::UnknownField => "unknown key",
            SchemaErrorKind::BadGrant => "grant needs exactly one of allow/deny",
            SchemaErrorKind::UnknownAction => "unknown or non-schema-grantable action",
            SchemaErrorKind::BadSubject => "malformed grant subject template",
            SchemaErrorKind::UnknownRoleRef => "grant references an undeclared role",
            SchemaErrorKind::BadPath => "grant path is not a well-formed absolute path",
            SchemaErrorKind::DuplicateRole => "duplicate role",
            SchemaErrorKind::ReservedRole => "role name is a reserved subject class",
        };
        write!(f, "{what}: {}", self.at)
    }
}

impl std::error::Error for SchemaError {}

impl Schema {
    /// The schema name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared version.
    pub fn version(&self) -> i64 {
        self.version
    }

    /// The name of the root map type.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The named types, in declaration order.
    pub fn types(&self) -> &[(String, TypeDef)] {
        &self.types
    }

    /// The definition of `name`, if declared.
    pub fn type_def(&self, name: &str) -> Option<&TypeDef> {
        self.types
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, def)| def)
    }

    /// The declared awareness entry kinds, in declaration order.
    pub fn awareness(&self) -> &[(String, AwarenessEntry)] {
        &self.awareness
    }

    /// The awareness timing declared for `kind`, if any.
    pub fn awareness_entry(&self, kind: &str) -> Option<&AwarenessEntry> {
        self.awareness
            .iter()
            .find(|(k, _)| k == kind)
            .map(|(_, e)| e)
    }

    /// The static role-based access defaults (`@auth`).
    pub fn auth(&self) -> &Auth {
        &self.auth
    }

    /// Parse a schema from its JSON source. Total — any input yields a `Schema`
    /// or a [`SchemaError`], never a panic.
    pub fn parse(src: &str) -> Result<Schema, SchemaError> {
        let json = Json::parse(src).map_err(|e: JsonError| {
            SchemaError::new(SchemaErrorKind::Json(e.kind), format!("byte {}", e.at))
        })?;
        Schema::from_json(&json)
    }

    /// The top-level keys the schema language defines. Any other key is a typo
    /// and is rejected. `marks` is declared by the language but not yet modelled
    /// here; it is accepted structurally so a spec-valid schema parses, and
    /// modelled by its own unit.
    const TOP_LEVEL_KEYS: [&'static str; 7] = [
        "schema",
        "version",
        "root",
        "types",
        "marks",
        "awareness",
        "auth",
    ];

    /// Build a schema from an already-parsed JSON value.
    pub fn from_json(json: &Json) -> Result<Schema, SchemaError> {
        let obj = json
            .as_object()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "document"))?;

        for (key, _) in obj {
            if !Schema::TOP_LEVEL_KEYS.contains(&key.as_str()) {
                return Err(SchemaError::new(SchemaErrorKind::UnknownField, key.clone()));
            }
        }

        let name = required_str(json, "schema", "")?.to_string();
        let version = required_int(json, "version", "")?;
        let root = required_str(json, "root", "")?.to_string();

        let types_json = json
            .get("types")
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::MissingField, "types"))?;
        let types_obj = types_json
            .as_object()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "types"))?;

        let mut types = Vec::with_capacity(types_obj.len());
        for (type_name, def) in types_obj {
            types.push((type_name.clone(), parse_type_def(def, type_name)?));
        }

        let awareness = match json.get("awareness") {
            None => Vec::new(),
            Some(a) => parse_awareness(a)?,
        };

        let auth = match json.get("auth") {
            None => Auth::default(),
            Some(a) => parse_auth(a)?,
        };

        let schema = Schema {
            name,
            version,
            root,
            types,
            awareness,
            auth,
        };
        schema.validate_references()?;
        Ok(schema)
    }

    /// Every type a schema names must be declared, and `root` must be a map —
    /// the closure guarantee that makes runtime repair total. The declared set
    /// is hashed once so membership stays O(1) per reference (a schema may carry
    /// many types) while `self.types` keeps its declaration order.
    fn validate_references(&self) -> Result<(), SchemaError> {
        let declared: HashSet<&str> = self.types.iter().map(|(n, _)| n.as_str()).collect();
        let require = |name: &str| -> Result<(), SchemaError> {
            if declared.contains(name) {
                Ok(())
            } else {
                Err(SchemaError::new(
                    SchemaErrorKind::UnknownTypeRef,
                    name.to_string(),
                ))
            }
        };
        match self.type_def(&self.root) {
            None => {
                return Err(SchemaError::new(
                    SchemaErrorKind::UnknownTypeRef,
                    self.root.clone(),
                ))
            }
            Some(TypeDef::Map { .. }) => {}
            Some(_) => {
                return Err(SchemaError::new(
                    SchemaErrorKind::RootNotMap,
                    self.root.clone(),
                ))
            }
        }
        for (_, def) in &self.types {
            match def {
                TypeDef::Map { children } => {
                    for (_, ty) in children {
                        require(ty)?;
                    }
                }
                TypeDef::List { items, .. } => require(items)?,
                TypeDef::Text { .. } | TypeDef::Register { .. } | TypeDef::Counter { .. } => {}
            }
        }
        Ok(())
    }
}

/// A readable error location: a field key joined onto its enclosing context
/// (a type name, or a `Type.children` path), or a bare key at the top level.
fn at(ctx: &str, key: &str) -> String {
    if ctx.is_empty() {
        key.to_string()
    } else {
        format!("{ctx}.{key}")
    }
}

/// Reject any key of `obj` outside `allowed`, so a typo'd field fails loud
/// rather than being silently dropped.
fn reject_unknown_fields(
    obj: &[(String, Json)],
    allowed: &[&str],
    ctx: &str,
) -> Result<(), SchemaError> {
    for (key, _) in obj {
        if !allowed.contains(&key.as_str()) {
            return Err(SchemaError::new(
                SchemaErrorKind::UnknownField,
                at(ctx, key),
            ));
        }
    }
    Ok(())
}

pub(crate) fn parse_type_def(json: &Json, type_name: &str) -> Result<TypeDef, SchemaError> {
    let obj = json
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, type_name.to_string()))?;
    let kind = required_str(json, "kind", type_name)?;
    match kind {
        "map" => {
            reject_unknown_fields(obj, &["kind", "children"], type_name)?;
            Ok(TypeDef::Map {
                children: parse_children(json, type_name)?,
            })
        }
        "list" => {
            // `min` is deliberately not accepted — a below-min list is
            // unrepairable, so it is rejected here as an unknown field.
            reject_unknown_fields(obj, &["kind", "items", "max"], type_name)?;
            let items = required_str(json, "items", type_name)?.to_string();
            let max = count_field(json, "max", type_name)?;
            Ok(TypeDef::List { items, max })
        }
        "text" => {
            reject_unknown_fields(obj, &["kind", "max"], type_name)?;
            Ok(TypeDef::Text {
                max: count_field(json, "max", type_name)?,
            })
        }
        "register" => {
            reject_unknown_fields(obj, &["kind", "min", "max"], type_name)?;
            let (min, max) = bounds(json, type_name)?;
            Ok(TypeDef::Register { min, max })
        }
        "counter" => {
            reject_unknown_fields(obj, &["kind", "min", "max"], type_name)?;
            let (min, max) = bounds(json, type_name)?;
            Ok(TypeDef::Counter { min, max })
        }
        _ => Err(SchemaError::new(
            SchemaErrorKind::UnknownKind,
            at(type_name, "kind"),
        )),
    }
}

fn parse_awareness(json: &Json) -> Result<Vec<(String, AwarenessEntry)>, SchemaError> {
    let obj = json
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "awareness"))?;
    let mut out = Vec::with_capacity(obj.len());
    for (kind, entry) in obj {
        let ctx = at("awareness", kind);
        let entry_obj = entry
            .as_object()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
        reject_unknown_fields(entry_obj, &["ttl", "throttle"], &ctx)?;
        let ttl = count_field(entry, "ttl", &ctx)?;
        let throttle = count_field(entry, "throttle", &ctx)?;
        out.push((kind.clone(), AwarenessEntry { ttl, throttle }));
    }
    Ok(out)
}

/// Parse the `@auth` block: a role vocabulary and static grants. `roles` is
/// validated for duplicates; each grant's `to` role reference is closure-checked
/// against that vocabulary, so an accepted `@auth` names no role it did not
/// declare.
fn parse_auth(json: &Json) -> Result<Auth, SchemaError> {
    let obj = json
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "auth"))?;
    reject_unknown_fields(obj, &["roles", "grants"], "auth")?;

    let roles = parse_roles(json)?;
    let declared: HashSet<&str> = roles.iter().map(String::as_str).collect();

    let grants = match json.get("grants") {
        None => Vec::new(),
        Some(g) => {
            let arr = g
                .as_array()
                .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, "auth.grants"))?;
            let mut out = Vec::with_capacity(arr.len());
            for (i, grant) in arr.iter().enumerate() {
                out.push(parse_grant(grant, i, &declared)?);
            }
            out
        }
    };

    Ok(Auth { roles, grants })
}

/// The subject-class keywords. A role may not be declared with one of these
/// names, so a grant's `to` token resolves unambiguously (always the class,
/// never a shadowed role).
fn is_reserved_subject(name: &str) -> bool {
    matches!(name, "authenticated" | "anonymous" | "anyone")
}

fn parse_roles(json: &Json) -> Result<Vec<String>, SchemaError> {
    let Some(roles) = json.get("roles") else {
        return Ok(Vec::new());
    };
    let arr = roles
        .as_array()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, "auth.roles"))?;
    let mut out: Vec<String> = Vec::with_capacity(arr.len());
    let mut seen: HashSet<&str> = HashSet::with_capacity(arr.len());
    for (i, role) in arr.iter().enumerate() {
        let name = role.as_str().ok_or_else(|| {
            SchemaError::new(SchemaErrorKind::WrongType, format!("auth.roles[{i}]"))
        })?;
        if is_reserved_subject(name) {
            return Err(SchemaError::new(
                SchemaErrorKind::ReservedRole,
                format!("auth.roles[{i}]"),
            ));
        }
        if !seen.insert(name) {
            return Err(SchemaError::new(
                SchemaErrorKind::DuplicateRole,
                format!("auth.roles[{i}]"),
            ));
        }
        out.push(name.to_string());
    }
    Ok(out)
}

fn parse_grant(json: &Json, index: usize, roles: &HashSet<&str>) -> Result<Grant, SchemaError> {
    let ctx = format!("auth.grants[{index}]");
    let obj = json
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
    reject_unknown_fields(obj, &["allow", "deny", "to", "on"], &ctx)?;

    // The effect and the action are fused: exactly one of `allow` / `deny`
    // carries the action as its value.
    let (effect, effect_key, action_val) = match (json.get("allow"), json.get("deny")) {
        (Some(a), None) => (Effect::Allow, "allow", a),
        (None, Some(d)) => (Effect::Deny, "deny", d),
        _ => return Err(SchemaError::new(SchemaErrorKind::BadGrant, ctx)),
    };
    let action_name = action_val
        .as_str()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, effect_key)))?;
    let action = parse_action(action_name, &ctx, effect_key)?;

    let subject = parse_subject(required_str(json, "to", &ctx)?, roles, &ctx)?;
    let path = required_str(json, "on", &ctx)?.to_string();
    validate_path(&path, &ctx)?;

    Ok(Grant {
        effect,
        action,
        subject,
        path,
    })
}

/// The action is carried by the `allow` / `deny` key, so a bad value reports
/// that key's location, not a pseudo-key named after the value.
fn parse_action(name: &str, ctx: &str, effect_key: &str) -> Result<Action, SchemaError> {
    match name {
        "read" => Ok(Action::Read),
        "write" => Ok(Action::Write),
        "publish_awareness" => Ok(Action::PublishAwareness),
        _ => Err(SchemaError::new(
            SchemaErrorKind::UnknownAction,
            at(ctx, effect_key),
        )),
    }
}

/// The subject always comes from the `to` field, so every subject error reports
/// `…grants[i].to`, not a pseudo-key named after the offending value.
fn parse_subject(raw: &str, roles: &HashSet<&str>, ctx: &str) -> Result<Subject, SchemaError> {
    let to = || at(ctx, "to");
    if let Some(rest) = raw.strip_prefix("${") {
        let var = rest
            .strip_suffix('}')
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::BadSubject, to()))?;
        return match var {
            "actor_id" => Ok(Subject::Template(TemplateVar::ActorId)),
            "author_id" => Ok(Subject::Template(TemplateVar::AuthorId)),
            "room_id" => Ok(Subject::Template(TemplateVar::RoomId)),
            "branch_id" => Ok(Subject::Template(TemplateVar::BranchId)),
            _ => Err(SchemaError::new(SchemaErrorKind::BadSubject, to())),
        };
    }
    match raw {
        "authenticated" => Ok(Subject::Class(SubjectClass::Authenticated)),
        "anonymous" => Ok(Subject::Class(SubjectClass::Anonymous)),
        "anyone" => Ok(Subject::Class(SubjectClass::Anyone)),
        name if roles.contains(name) => Ok(Subject::Role(name.to_string())),
        _ => Err(SchemaError::new(SchemaErrorKind::UnknownRoleRef, to())),
    }
}

/// A grant path is absolute (`/` or `/seg/seg…`) with no empty segment. Path
/// inheritance is by segment at check time, so a malformed path is rejected here
/// rather than mis-matching later.
fn validate_path(path: &str, ctx: &str) -> Result<(), SchemaError> {
    if path == "/" {
        return Ok(());
    }
    let Some(rest) = path.strip_prefix('/') else {
        return Err(SchemaError::new(SchemaErrorKind::BadPath, at(ctx, "on")));
    };
    if rest.split('/').any(str::is_empty) {
        return Err(SchemaError::new(SchemaErrorKind::BadPath, at(ctx, "on")));
    }
    Ok(())
}

fn parse_children(json: &Json, ctx: &str) -> Result<Vec<(String, String)>, SchemaError> {
    let Some(children) = json.get("children") else {
        return Ok(Vec::new());
    };
    let ctx = at(ctx, "children");
    let obj = children
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
    let mut out = Vec::with_capacity(obj.len());
    for (slot, ty) in obj {
        let type_name = ty
            .as_str()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, slot)))?;
        out.push((slot.clone(), type_name.to_string()));
    }
    Ok(out)
}

/// The `min`/`max` of a register or counter — full-range `i64` bounds.
fn bounds(json: &Json, ctx: &str) -> Result<(Option<i64>, Option<i64>), SchemaError> {
    let min = int_field(json, "min", ctx)?;
    let max = int_field(json, "max", ctx)?;
    if let (Some(a), Some(b)) = (min, max) {
        if a > b {
            return Err(SchemaError::new(
                SchemaErrorKind::BadRange,
                at(ctx, "min_gt_max"),
            ));
        }
    }
    Ok((min, max))
}

fn int_field(json: &Json, key: &str, ctx: &str) -> Result<Option<i64>, SchemaError> {
    match json.get(key) {
        None => Ok(None),
        Some(v) => Ok(Some(v.as_i64().ok_or_else(|| {
            SchemaError::new(SchemaErrorKind::WrongType, at(ctx, key))
        })?)),
    }
}

fn count_field(json: &Json, key: &str, ctx: &str) -> Result<Option<u64>, SchemaError> {
    match int_field(json, key, ctx)? {
        None => Ok(None),
        Some(n) if n < 0 => Err(SchemaError::new(SchemaErrorKind::BadRange, at(ctx, key))),
        Some(n) => Ok(Some(n as u64)),
    }
}

fn required_str<'a>(json: &'a Json, key: &str, ctx: &str) -> Result<&'a str, SchemaError> {
    match json.get(key) {
        None => Err(SchemaError::new(
            SchemaErrorKind::MissingField,
            at(ctx, key),
        )),
        Some(v) => v
            .as_str()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(ctx, key))),
    }
}

fn required_int(json: &Json, key: &str, ctx: &str) -> Result<i64, SchemaError> {
    match json.get(key) {
        None => Err(SchemaError::new(
            SchemaErrorKind::MissingField,
            at(ctx, key),
        )),
        Some(v) => v
            .as_i64()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(ctx, key))),
    }
}
