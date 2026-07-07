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
    marks: Vec<(String, MarkDef)>,
    awareness: Vec<(String, AwarenessEntry)>,
    auth: Auth,
    auto_version: Vec<AutoVersion>,
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

/// The declared merge semantics of one mark name. A mark is a convention over a
/// `RangedElement`; this declaration tells the read model how concurrent marks of
/// this name combine over a character, and whether the mark grows when text is
/// inserted at its boundary (the read model applies `expand` as the gravity of the
/// range's anchors). The value shape a `Value` / `Object` mark carries is a later
/// check-time concern, not modelled here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkDef {
    pub flavor: MarkFlavor,
    pub expand: MarkExpand,
}

/// How concurrent marks of one name merge over a character.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkFlavor {
    /// Presence only: concurrent add + add is present; add + remove on the same
    /// span resolves last-writer-wins by stamp (bold, italic).
    Boolean,
    /// A carried value, last-writer-wins on conflict (a link's href).
    Value,
    /// Each instance independent — no merging across instances, overlapping marks
    /// all coexist (comments).
    Object,
}

/// Whether a mark grows to include text inserted at its boundary — the gravity of
/// its start / end anchor. `Both` grows either side (bold), `None` neither (link),
/// `Before` / `After` one side only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkExpand {
    None,
    Before,
    After,
    Both,
}

/// One declarative version trigger: an event or a schedule that fires a version
/// capture, a name template for the captured version, and an optional retention
/// count. The engine sink (a later unit) reads these off the registered schema
/// and drives `create_version`; this unit is the parse only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoVersion {
    /// What fires the capture — an engine event or a periodic schedule.
    pub trigger: Trigger,
    /// The captured version's name, a template the sink expands (`${timestamp}`,
    /// `${event}`, …) at fire time. Validated non-empty; the template variables
    /// are the sink's concern, an open set here.
    pub name: String,
    /// Retain only this many of the trigger's own auto-versions, pruning the
    /// oldest past it. `None` retains all.
    pub keep: Option<u64>,
}

/// What fires an [`AutoVersion`] capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// On an engine lifecycle event.
    On(TriggerEvent),
    /// On a periodic schedule, every this many milliseconds.
    Every(u64),
}

/// A lifecycle event an [`AutoVersion`] may trigger on. The vocabulary is staged:
/// the first group fires today; the branch/migration events are declarable now
/// and fire once those layers exist — a trigger on one parses and waits, never
/// errors. An event name outside this vocabulary is a typo and is rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerEvent {
    Connect,
    Disconnect,
    Subscribe,
    VersionCreated,
    VersionRenamed,
    VersionDeleted,
    Compaction,
    /// Reserved: fires once the branch layer publishes.
    BeforePublish,
    /// Reserved: fires once a version is restored as a branch.
    AfterRestore,
    /// Reserved: fires once the migration layer runs.
    BeforeMigration,
}

impl TriggerEvent {
    /// The event's kebab-case name — the vocabulary a schema declares and the
    /// value a name template's `${event}` expands to.
    pub fn as_kebab(self) -> &'static str {
        match self {
            TriggerEvent::Connect => "connect",
            TriggerEvent::Disconnect => "disconnect",
            TriggerEvent::Subscribe => "subscribe",
            TriggerEvent::VersionCreated => "version-created",
            TriggerEvent::VersionRenamed => "version-renamed",
            TriggerEvent::VersionDeleted => "version-deleted",
            TriggerEvent::Compaction => "compaction",
            TriggerEvent::BeforePublish => "before-publish",
            TriggerEvent::AfterRestore => "after-restore",
            TriggerEvent::BeforeMigration => "before-migration",
        }
    }
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
    /// An XmlElement (`tag: Some`) or a tagless XmlFragment (`tag: None`, the
    /// document tree's root container). `children` maps each allowed child type
    /// name (element or text) to its optional per-type cardinality cap (`max` — the
    /// exclusivity constraint, e.g. one heading); `attrs` maps each declared
    /// attribute key to its value type; `marks` is the allowlist of mark names this
    /// element may carry; `orphan_inline` names the default block type that repair
    /// wraps loose inline content in. A fragment carries no `tag`, `attrs`, `marks`.
    Xml {
        tag: Option<String>,
        children: Vec<(String, Option<u64>)>,
        attrs: Vec<(String, String)>,
        marks: Vec<String>,
        orphan_inline: Option<String>,
    },
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
    /// An `autoVersion` trigger did not carry exactly one of `on` / `every`.
    BadTrigger,
    /// An `autoVersion` `on` named an event outside the staged vocabulary.
    UnknownEvent,
    /// An `autoVersion` `every` was not a well-formed duration (`<n><s|m|h|d>`).
    BadDuration,
    /// An `autoVersion` `name` template was empty.
    EmptyName,
    /// A mark declared a `flavor` outside `boolean` / `value` / `object`.
    UnknownFlavor,
    /// A mark declared an `expand` outside `none` / `before` / `after` / `both`.
    UnknownExpand,
    /// An xml type's `marks` allowlist named a mark not declared in the top-level
    /// `marks` block.
    UnknownMarkRef,
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
            SchemaErrorKind::BadTrigger => "autoVersion trigger needs exactly one of on/every",
            SchemaErrorKind::UnknownEvent => "unknown autoVersion trigger event",
            SchemaErrorKind::BadDuration => "malformed autoVersion schedule duration",
            SchemaErrorKind::EmptyName => "empty autoVersion name template",
            SchemaErrorKind::UnknownFlavor => "unknown mark flavor",
            SchemaErrorKind::UnknownExpand => "unknown mark expand direction",
            SchemaErrorKind::UnknownMarkRef => "reference to an undeclared mark",
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

    /// The declared mark names and their merge semantics, in declaration order.
    pub fn marks(&self) -> &[(String, MarkDef)] {
        &self.marks
    }

    /// The merge semantics declared for mark `name`, if any.
    pub fn mark(&self, name: &str) -> Option<&MarkDef> {
        self.marks.iter().find(|(n, _)| n == name).map(|(_, d)| d)
    }

    /// The static role-based access defaults (`@auth`).
    pub fn auth(&self) -> &Auth {
        &self.auth
    }

    /// The declarative version triggers (`autoVersion`), in declaration order.
    pub fn auto_version(&self) -> &[AutoVersion] {
        &self.auto_version
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
    const TOP_LEVEL_KEYS: [&'static str; 8] = [
        "schema",
        "version",
        "root",
        "types",
        "marks",
        "awareness",
        "auth",
        "autoVersion",
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

        let marks = match json.get("marks") {
            None => Vec::new(),
            Some(m) => parse_marks(m)?,
        };

        let awareness = match json.get("awareness") {
            None => Vec::new(),
            Some(a) => parse_awareness(a)?,
        };

        let auth = match json.get("auth") {
            None => Auth::default(),
            Some(a) => parse_auth(a)?,
        };

        let auto_version = match json.get("autoVersion") {
            None => Vec::new(),
            Some(a) => parse_auto_version(a)?,
        };

        let schema = Schema {
            name,
            version,
            root,
            types,
            marks,
            awareness,
            auth,
            auto_version,
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
        let declared_marks: HashSet<&str> = self.marks.iter().map(|(n, _)| n.as_str()).collect();
        let require_mark = |name: &str| -> Result<(), SchemaError> {
            if declared_marks.contains(name) {
                Ok(())
            } else {
                Err(SchemaError::new(
                    SchemaErrorKind::UnknownMarkRef,
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
                TypeDef::Xml {
                    children,
                    attrs,
                    marks,
                    orphan_inline,
                    ..
                } => {
                    for (ty, _) in children {
                        require(ty)?;
                    }
                    for (_, ty) in attrs {
                        require(ty)?;
                    }
                    for name in marks {
                        require_mark(name)?;
                    }
                    if let Some(block) = orphan_inline {
                        require(block)?;
                    }
                }
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
        "xml" => {
            reject_unknown_fields(
                obj,
                &["kind", "tag", "children", "attrs", "marks", "repair"],
                type_name,
            )?;
            Ok(TypeDef::Xml {
                tag: Some(required_str(json, "tag", type_name)?.to_string()),
                children: parse_xml_children(json, type_name)?,
                attrs: parse_attrs(json, type_name)?,
                marks: parse_type_name_array(json, "marks", type_name)?,
                orphan_inline: parse_orphan_inline(json, type_name)?,
            })
        }
        "fragment" => {
            reject_unknown_fields(obj, &["kind", "children", "repair"], type_name)?;
            Ok(TypeDef::Xml {
                tag: None,
                children: parse_xml_children(json, type_name)?,
                attrs: Vec::new(),
                marks: Vec::new(),
                orphan_inline: parse_orphan_inline(json, type_name)?,
            })
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

/// Parse the `marks` block: an object mapping each mark name to its merge
/// `flavor` (required) and anchor `expand` (optional, `none` by default). Order is
/// preserved. A mark's value shape is a later check-time concern, not parsed here.
fn parse_marks(json: &Json) -> Result<Vec<(String, MarkDef)>, SchemaError> {
    let obj = json
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "marks"))?;
    let mut out = Vec::with_capacity(obj.len());
    for (name, def) in obj {
        let ctx = at("marks", name);
        let def_obj = def
            .as_object()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
        reject_unknown_fields(def_obj, &["flavor", "expand"], &ctx)?;

        let flavor = match required_str(def, "flavor", &ctx)? {
            "boolean" => MarkFlavor::Boolean,
            "value" => MarkFlavor::Value,
            "object" => MarkFlavor::Object,
            _ => {
                return Err(SchemaError::new(
                    SchemaErrorKind::UnknownFlavor,
                    at(&ctx, "flavor"),
                ))
            }
        };

        let expand = match def.get("expand") {
            None => MarkExpand::None,
            Some(e) => {
                let s = e.as_str().ok_or_else(|| {
                    SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, "expand"))
                })?;
                match s {
                    "none" => MarkExpand::None,
                    "before" => MarkExpand::Before,
                    "after" => MarkExpand::After,
                    "both" => MarkExpand::Both,
                    _ => {
                        return Err(SchemaError::new(
                            SchemaErrorKind::UnknownExpand,
                            at(&ctx, "expand"),
                        ))
                    }
                }
            }
        };

        out.push((name.clone(), MarkDef { flavor, expand }));
    }
    Ok(out)
}

/// Parse the `autoVersion` block: an array of triggers, each an event or a
/// schedule with a name template and an optional retention count. Order is
/// preserved.
fn parse_auto_version(json: &Json) -> Result<Vec<AutoVersion>, SchemaError> {
    let arr = json
        .as_array()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, "autoVersion"))?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, trigger) in arr.iter().enumerate() {
        out.push(parse_trigger(trigger, i)?);
    }
    Ok(out)
}

fn parse_trigger(json: &Json, index: usize) -> Result<AutoVersion, SchemaError> {
    let ctx = format!("autoVersion[{index}]");
    let obj = json
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
    reject_unknown_fields(obj, &["on", "every", "name", "keep"], &ctx)?;

    // Exactly one of `on` / `every`: an event or a schedule, never both or neither.
    let trigger = match (json.get("on"), json.get("every")) {
        (Some(on), None) => {
            let name = on
                .as_str()
                .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, "on")))?;
            Trigger::On(parse_trigger_event(name, &ctx)?)
        }
        (None, Some(every)) => {
            let spec = every
                .as_str()
                .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, "every")))?;
            Trigger::Every(parse_duration_millis(spec, &ctx)?)
        }
        _ => return Err(SchemaError::new(SchemaErrorKind::BadTrigger, ctx)),
    };

    let name = required_str(json, "name", &ctx)?.to_string();
    if name.is_empty() {
        return Err(SchemaError::new(
            SchemaErrorKind::EmptyName,
            at(&ctx, "name"),
        ));
    }
    let keep = count_field(json, "keep", &ctx)?;

    Ok(AutoVersion {
        trigger,
        name,
        keep,
    })
}

/// Map an `on` event name to its [`TriggerEvent`]. The vocabulary spans the
/// events that fire today and the reserved branch/migration events (declarable,
/// waiting); a name outside it is a typo and is rejected.
fn parse_trigger_event(name: &str, ctx: &str) -> Result<TriggerEvent, SchemaError> {
    let event = match name {
        "connect" => TriggerEvent::Connect,
        "disconnect" => TriggerEvent::Disconnect,
        "subscribe" => TriggerEvent::Subscribe,
        "version-created" => TriggerEvent::VersionCreated,
        "version-renamed" => TriggerEvent::VersionRenamed,
        "version-deleted" => TriggerEvent::VersionDeleted,
        "compaction" => TriggerEvent::Compaction,
        "before-publish" => TriggerEvent::BeforePublish,
        "after-restore" => TriggerEvent::AfterRestore,
        "before-migration" => TriggerEvent::BeforeMigration,
        _ => {
            return Err(SchemaError::new(
                SchemaErrorKind::UnknownEvent,
                at(ctx, "on"),
            ))
        }
    };
    Ok(event)
}

/// Parse a schedule duration `<n><unit>` (`s`/`m`/`h`/`d`) into milliseconds.
/// Rejects an empty or non-numeric count, a missing or unknown unit, and an
/// overflowing product.
fn parse_duration_millis(spec: &str, ctx: &str) -> Result<u64, SchemaError> {
    let bad = || SchemaError::new(SchemaErrorKind::BadDuration, at(ctx, "every"));
    let unit = spec.chars().last().ok_or_else(&bad)?;
    let factor: u64 = match unit {
        's' => 1_000,
        'm' => 60_000,
        'h' => 3_600_000,
        'd' => 86_400_000,
        _ => return Err(bad()),
    };
    let digits = &spec[..spec.len() - unit.len_utf8()];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let n: u64 = digits.parse().map_err(|_| bad())?;
    // A zero interval is no schedule — it would fire on every sweep. Reject it at
    // parse rather than let it flood versions at runtime.
    if n == 0 {
        return Err(bad());
    }
    n.checked_mul(factor).ok_or_else(&bad)
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

/// An xml type's `children` allowlist: each allowed child type name → its
/// optional per-type cardinality cap (`max`, the exclusivity constraint). Absent
/// → empty; a non-object, a non-object child value, an unknown field under a
/// child, or a negative `max` is rejected.
fn parse_xml_children(json: &Json, ctx: &str) -> Result<Vec<(String, Option<u64>)>, SchemaError> {
    let Some(children) = json.get("children") else {
        return Ok(Vec::new());
    };
    let ctx = at(ctx, "children");
    let obj = children
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
    let mut out = Vec::with_capacity(obj.len());
    for (name, constraints) in obj {
        let child_ctx = at(&ctx, name);
        let cobj = constraints
            .as_object()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, child_ctx.clone()))?;
        reject_unknown_fields(cobj, &["max"], &child_ctx)?;
        let max = match int_field(constraints, "max", &child_ctx)? {
            None => None,
            Some(m) if m >= 0 => Some(m as u64),
            Some(_) => {
                return Err(SchemaError::new(
                    SchemaErrorKind::BadRange,
                    at(&child_ctx, "max"),
                ))
            }
        };
        out.push((name.clone(), max));
    }
    Ok(out)
}

/// An optional array-of-type-names allowlist (an xml type's `marks`). Absent →
/// empty; a non-array or a non-string element is rejected.
fn parse_type_name_array(json: &Json, key: &str, ctx: &str) -> Result<Vec<String>, SchemaError> {
    let Some(v) = json.get(key) else {
        return Ok(Vec::new());
    };
    let ctx = at(ctx, key);
    let arr = v
        .as_array()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, ctx.clone()))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let name = item
            .as_str()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, ctx.clone()))?;
        out.push(name.to_string());
    }
    Ok(out)
}

/// An xml type's `attrs` allowlist: each attribute key → the type its value
/// holds. Absent → empty; a non-object, or a non-string type name, is rejected.
fn parse_attrs(json: &Json, ctx: &str) -> Result<Vec<(String, String)>, SchemaError> {
    let Some(attrs) = json.get("attrs") else {
        return Ok(Vec::new());
    };
    let ctx = at(ctx, "attrs");
    let obj = attrs
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
    let mut out = Vec::with_capacity(obj.len());
    for (key, ty) in obj {
        let type_name = ty
            .as_str()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, key)))?;
        out.push((key.clone(), type_name.to_string()));
    }
    Ok(out)
}

/// An xml type's `repair.orphanInline` — the default block type that repair
/// wraps loose inline content in. Absent `repair` or absent key → `None`; an
/// unknown key under `repair` fails loud.
fn parse_orphan_inline(json: &Json, ctx: &str) -> Result<Option<String>, SchemaError> {
    let Some(repair) = json.get("repair") else {
        return Ok(None);
    };
    let ctx = at(ctx, "repair");
    let obj = repair
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, ctx.clone()))?;
    reject_unknown_fields(obj, &["orphanInline"], &ctx)?;
    match repair.get("orphanInline") {
        None => Ok(None),
        Some(v) => Ok(Some(
            v.as_str()
                .ok_or_else(|| {
                    SchemaError::new(SchemaErrorKind::WrongType, at(&ctx, "orphanInline"))
                })?
                .to_string(),
        )),
    }
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
