//! The migration model — one edge of an app's schema chain, parsed from JSON.
//!
//! A migration is the transform that reaches schema version `to` from `from`, a
//! single contiguous step (`to == from + 1`) since the registry chain is
//! contiguous from version 1. It carries an ordered list of structural [`Step`]s
//! over the built primitives — add / remove / rename a named type or a map
//! field. Like the schema, a migration is app code: authored as JSON, versioned,
//! registered per `app_id`; core is the sole parser so every SDK forwards bytes
//! rather than reimplementing it.
//!
//! Parsing is total — every input yields a [`Migration`] or a [`MigrationError`],
//! never a panic — and validates the envelope at parse time (contiguous
//! versions, well-formed step params, non-empty names, no unknown keys). Each
//! step and the composed edge classify as back-compatible or breaking
//! ([`Compat`]) — whether an inverse exists, the guard mixed-version fan-out
//! consults — and each step rewrites one op forward ([`Step::rewrite_up`]) and,
//! when back-compatible, backward ([`Step::rewrite_down`]), the structural
//! surgery the fan-out applies per recipient. The step set is the structural
//! transforms over the built primitives; the value-transform kinds (wrap /
//! setAttr / mapValues) belong to the marks / XML layer and are not part of it.

use crate::json::{Json, JsonError, JsonErrorKind};
use crate::op::{Op, OpKind};
use crate::schema::{self, SchemaErrorKind, TypeDef};

/// A parsed, validated migration edge.
#[derive(Clone, Debug, PartialEq)]
pub struct Migration {
    /// The predecessor version this edge migrates from (at least 1).
    pub from: u32,
    /// The version this edge produces (`from + 1`).
    pub to: u32,
    steps: Vec<Step>,
}

/// One structural transform in a migration, over the built-primitive type model
/// (`schema::TypeDef`). Each names the type or field it acts on; its
/// compatibility class and per-op rewrite follow from the kind.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// Introduce a new named type.
    AddType { name: String, def: TypeDef },
    /// Remove a named type.
    RemoveType { name: String },
    /// Rename a named type (references to it move with it).
    RenameType { from: String, to: String },
    /// Add a child slot to a map type, holding `field_type`.
    AddField {
        ty: String,
        field: String,
        field_type: String,
    },
    /// Remove a child slot from a map type.
    RemoveField { ty: String, field: String },
    /// Rename a child slot of a map type.
    RenameField {
        ty: String,
        from: String,
        to: String,
    },
}

/// The compatibility class of a step or an edge — whether an inverse
/// (down-migration) exists, so mixed-version fleets can coexist across it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compat {
    /// Bidirectional: a down-migration exists, so a client on the older version
    /// is served by inverting the edge. The additive steps — down is dropping the
    /// addition.
    BackCompatible,
    /// Forward-only: the down-migration is lossy or impossible, so a client that
    /// cannot reach the newer version is stranded across this edge. Removals lose
    /// state; a bare rename leaves the old construct unreachable without an
    /// expand/contract data copy.
    Breaking,
}

/// The image of one op across an edge: it survives (possibly with a rewritten
/// slot key), or it has no image at the target version and is dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpRewrite {
    Keep(Op),
    Drop,
}

impl Step {
    /// Whether this step has an inverse. The additive steps drop cleanly on the
    /// way down; removals and bare renames do not (a rename needs the
    /// expand/contract data copy to stay reachable at the old version).
    pub fn compat(&self) -> Compat {
        match self {
            Step::AddType { .. } | Step::AddField { .. } => Compat::BackCompatible,
            Step::RemoveType { .. }
            | Step::RemoveField { .. }
            | Step::RenameType { .. }
            | Step::RenameField { .. } => Compat::Breaking,
        }
    }

    /// Rewrite one op forward, to the version this step produces. Field steps act
    /// on the op's slot key; type steps leave every op untouched, since an op
    /// names a slot key, never a schema type. The op is assumed already scoped to
    /// this step's type — narrowing a rewrite to the elements of a given type is
    /// the fan-out's concern, not this transform's.
    pub fn rewrite_up(&self, op: &Op) -> OpRewrite {
        match self {
            Step::RenameField { from, to, .. } => rename_key(op, from, to),
            Step::RemoveField { field, .. } => drop_on_key(op, field),
            // The additive and type steps introduce nothing an existing op
            // already references, so nothing to rewrite on the way up.
            Step::AddField { .. }
            | Step::AddType { .. }
            | Step::RemoveType { .. }
            | Step::RenameType { .. } => OpRewrite::Keep(op.clone()),
        }
    }

    /// Rewrite one op backward, to the version this step migrates from — `None`
    /// when the step is breaking (no inverse). Inverting an additive step drops
    /// the op that references the addition; inverting an added type is a no-op on
    /// the op stream. Kept consistent with [`compat`](Step::compat): a step has a
    /// down-rewrite exactly when it is back-compatible.
    pub fn rewrite_down(&self, op: &Op) -> Option<OpRewrite> {
        match self {
            Step::AddField { field, .. } => Some(drop_on_key(op, field)),
            Step::AddType { .. } => Some(OpRewrite::Keep(op.clone())),
            Step::RemoveType { .. }
            | Step::RemoveField { .. }
            | Step::RenameType { .. }
            | Step::RenameField { .. } => None,
        }
    }
}

/// The slot key an op addresses, for the map-level kinds that carry one. The
/// sequence-internal kinds (list / text insert and delete) address by anchor or
/// id, not a map key, and have none.
fn op_key(op: &Op) -> Option<&[u8]> {
    match &op.kind {
        OpKind::RegisterSet { key, .. }
        | OpKind::CounterInc { key, .. }
        | OpKind::CounterDec { key, .. }
        | OpKind::MapSet { key, .. }
        | OpKind::MapDelete { key }
        | OpKind::MapCreate { key }
        | OpKind::ListCreate { key }
        | OpKind::TextCreate { key }
        | OpKind::XmlElementCreate { key, .. }
        | OpKind::XmlFragmentCreate { key } => Some(key),
        OpKind::ListInsert { .. }
        | OpKind::ListDelete { .. }
        | OpKind::TextInsert { .. }
        | OpKind::TextDelete { .. }
        | OpKind::XmlInsertChild { .. }
        | OpKind::XmlMove { .. }
        | OpKind::XmlReveal { .. }
        | OpKind::RangedCreate { .. }
        | OpKind::RangedSetPayload { .. }
        | OpKind::RangedDelete { .. }
        | OpKind::AclGrant { .. }
        | OpKind::AclRevoke { .. } => None,
    }
}

/// Keep the op, rewriting its key from `from` to `to` when it matches; a key
/// name is the UTF-8 of the schema field name.
fn rename_key(op: &Op, from: &str, to: &str) -> OpRewrite {
    if op_key(op) == Some(from.as_bytes()) {
        OpRewrite::Keep(with_key(op, to.as_bytes().to_vec()))
    } else {
        OpRewrite::Keep(op.clone())
    }
}

/// Drop the op when it addresses `field`, else keep it unchanged.
fn drop_on_key(op: &Op, field: &str) -> OpRewrite {
    if op_key(op) == Some(field.as_bytes()) {
        OpRewrite::Drop
    } else {
        OpRewrite::Keep(op.clone())
    }
}

/// A copy of `op` with its slot key replaced. Called only for a key-bearing op,
/// so the sequence-internal kinds fall through unchanged.
fn with_key(op: &Op, new_key: Vec<u8>) -> Op {
    let kind = match &op.kind {
        OpKind::RegisterSet { value, .. } => OpKind::RegisterSet {
            key: new_key,
            value: value.clone(),
        },
        OpKind::CounterInc { amount, .. } => OpKind::CounterInc {
            key: new_key,
            amount: *amount,
        },
        OpKind::CounterDec { amount, .. } => OpKind::CounterDec {
            key: new_key,
            amount: *amount,
        },
        OpKind::MapSet { value, .. } => OpKind::MapSet {
            key: new_key,
            value: value.clone(),
        },
        OpKind::MapDelete { .. } => OpKind::MapDelete { key: new_key },
        OpKind::MapCreate { .. } => OpKind::MapCreate { key: new_key },
        OpKind::ListCreate { .. } => OpKind::ListCreate { key: new_key },
        OpKind::TextCreate { .. } => OpKind::TextCreate { key: new_key },
        OpKind::XmlElementCreate { tag, .. } => OpKind::XmlElementCreate {
            key: new_key,
            tag: tag.clone(),
        },
        OpKind::XmlFragmentCreate { .. } => OpKind::XmlFragmentCreate { key: new_key },
        other => other.clone(),
    };
    Op { kind, ..op.clone() }
}

/// Rewrite one op forward along a contiguous ascending chain of edges — from the
/// op's creation version up to the target — folding each edge's forward rewrite,
/// a drop short-circuiting the rest. Forward translation is always defined.
///
/// The caller supplies the chain segment between the two versions, ascending and
/// contiguous (`edges[i].to == edges[i + 1].from`); an empty slice is identity.
pub fn rewrite_up_along(edges: &[Migration], op: &Op) -> OpRewrite {
    let mut current = op.clone();
    for edge in edges {
        match edge.rewrite_up(&current) {
            OpRewrite::Keep(next) => current = next,
            OpRewrite::Drop => return OpRewrite::Drop,
        }
    }
    OpRewrite::Keep(current)
}

/// Rewrite one op backward along a contiguous ascending chain — from the top
/// version down to the target — `None` when any edge on the path is breaking, so
/// the target is unreachable and the recipient is refused at the handshake. The
/// chain inverts top-first, so it is walked in reverse.
///
/// The caller supplies the same ascending, contiguous segment as
/// [`rewrite_up_along`]; an empty slice is identity.
pub fn rewrite_down_along(edges: &[Migration], op: &Op) -> Option<OpRewrite> {
    if !reachable_down(edges) {
        return None;
    }
    let mut current = op.clone();
    for edge in edges.iter().rev() {
        match edge.rewrite_down(&current)? {
            OpRewrite::Keep(next) => current = next,
            OpRewrite::Drop => return Some(OpRewrite::Drop),
        }
    }
    Some(OpRewrite::Keep(current))
}

/// Whether the bottom of a contiguous ascending chain is reachable from the top
/// by down-migration — true iff every edge is back-compatible (has an inverse).
/// A single breaking edge strands the older version; forward (up) is always
/// reachable, so there is no `reachable_up`. Consistent with
/// [`rewrite_down_along`], which is `Some` exactly when this holds.
pub fn reachable_down(edges: &[Migration]) -> bool {
    edges.iter().all(|e| e.compat() == Compat::BackCompatible)
}

/// Why a migration failed to parse or validate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MigrationErrorKind {
    /// The JSON itself did not parse.
    Json(JsonErrorKind),
    /// An object was required (the document, a step) but the value was not one.
    NotAnObject,
    /// A required field was absent.
    MissingField,
    /// A field was present but had the wrong JSON type.
    WrongType,
    /// A key the migration language does not define at its position — a typo,
    /// rejected rather than silently ignored, so a migration is code that fails
    /// loud.
    UnknownField,
    /// A step named a `kind` that is not a built-in structural step.
    UnknownStepKind,
    /// The versions are not a single forward step (`to != from + 1`).
    NonContiguous,
    /// A version was negative, zero where a predecessor is required, or beyond
    /// the `u32` version space.
    BadVersion,
    /// A type or field name was the empty string.
    EmptyName,
    /// An `addType` step's type-def body was itself invalid (as judged by the
    /// shared schema type-def parser).
    TypeDef(SchemaErrorKind),
}

/// A migration failure: a [`MigrationErrorKind`] plus the field or step location
/// it occurred at, for a readable message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MigrationError {
    pub kind: MigrationErrorKind,
    pub at: String,
}

impl MigrationError {
    fn new(kind: MigrationErrorKind, at: impl Into<String>) -> Self {
        MigrationError {
            kind,
            at: at.into(),
        }
    }
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            MigrationErrorKind::Json(k) => {
                return write!(f, "invalid migration JSON ({k:?}) at {}", self.at)
            }
            MigrationErrorKind::TypeDef(k) => {
                return write!(f, "invalid type def ({k:?}) at {}", self.at)
            }
            MigrationErrorKind::NotAnObject => "expected an object",
            MigrationErrorKind::MissingField => "missing field",
            MigrationErrorKind::WrongType => "field has the wrong type",
            MigrationErrorKind::UnknownField => "unknown field",
            MigrationErrorKind::UnknownStepKind => "unknown step kind",
            MigrationErrorKind::NonContiguous => "versions are not a single forward step",
            MigrationErrorKind::BadVersion => "version out of range",
            MigrationErrorKind::EmptyName => "empty name",
        };
        write!(f, "{what} at {}", self.at)
    }
}

impl Migration {
    /// The steps of this edge, in declaration order.
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// The compatibility class of the whole edge: back-compatible only when every
    /// step is, since a single forward-only step leaves no inverse for the edge.
    /// An edge with no steps changes nothing, so it inverts trivially.
    pub fn compat(&self) -> Compat {
        if self
            .steps
            .iter()
            .all(|s| s.compat() == Compat::BackCompatible)
        {
            Compat::BackCompatible
        } else {
            Compat::Breaking
        }
    }

    /// Rewrite one op forward across the whole edge — its steps threaded in
    /// declaration order, a drop short-circuiting the rest.
    pub fn rewrite_up(&self, op: &Op) -> OpRewrite {
        let mut current = op.clone();
        for step in &self.steps {
            match step.rewrite_up(&current) {
                OpRewrite::Keep(next) => current = next,
                OpRewrite::Drop => return OpRewrite::Drop,
            }
        }
        OpRewrite::Keep(current)
    }

    /// Rewrite one op backward across the whole edge — `None` when the edge is
    /// breaking (no inverse). Inverting an edge inverts its steps last-to-first,
    /// so the steps are threaded in reverse; every step of a back-compatible edge
    /// is itself back-compatible, so each has a down-rewrite.
    pub fn rewrite_down(&self, op: &Op) -> Option<OpRewrite> {
        if self.compat() == Compat::Breaking {
            return None;
        }
        let mut current = op.clone();
        for step in self.steps.iter().rev() {
            match step.rewrite_down(&current)? {
                OpRewrite::Keep(next) => current = next,
                OpRewrite::Drop => return Some(OpRewrite::Drop),
            }
        }
        Some(OpRewrite::Keep(current))
    }

    /// Parse and validate a migration from its JSON source.
    pub fn parse(src: &str) -> Result<Migration, MigrationError> {
        let json = Json::parse(src).map_err(|e: JsonError| {
            MigrationError::new(MigrationErrorKind::Json(e.kind), format!("byte {}", e.at))
        })?;
        Migration::from_json(&json)
    }

    /// The top-level keys the migration language defines.
    const TOP_LEVEL_KEYS: [&'static str; 3] = ["from", "to", "steps"];

    /// Build a migration from an already-parsed JSON value.
    pub fn from_json(json: &Json) -> Result<Migration, MigrationError> {
        let obj = json
            .as_object()
            .ok_or_else(|| MigrationError::new(MigrationErrorKind::NotAnObject, "document"))?;
        for (key, _) in obj {
            if !Migration::TOP_LEVEL_KEYS.contains(&key.as_str()) {
                return Err(MigrationError::new(
                    MigrationErrorKind::UnknownField,
                    key.clone(),
                ));
            }
        }

        let from = version(json, "from")?;
        let to = version(json, "to")?;
        // The chain starts at version 1, so version 1 has no predecessor: the
        // lowest edge is 1 -> 2.
        if from < 1 {
            return Err(MigrationError::new(MigrationErrorKind::BadVersion, "from"));
        }
        // A `from` at the top of the version space has no successor version, so
        // it is never contiguous with any `to`.
        if from.checked_add(1) != Some(to) {
            return Err(MigrationError::new(MigrationErrorKind::NonContiguous, "to"));
        }

        let steps_json = json
            .get("steps")
            .ok_or_else(|| MigrationError::new(MigrationErrorKind::MissingField, "steps"))?;
        let steps_arr = steps_json
            .as_array()
            .ok_or_else(|| MigrationError::new(MigrationErrorKind::WrongType, "steps"))?;

        let mut steps = Vec::with_capacity(steps_arr.len());
        for (i, step) in steps_arr.iter().enumerate() {
            steps.push(parse_step(step, i)?);
        }

        Ok(Migration { from, to, steps })
    }
}

/// A readable error location: a field key joined onto its enclosing context.
fn at(ctx: &str, key: &str) -> String {
    if ctx.is_empty() {
        key.to_string()
    } else {
        format!("{ctx}.{key}")
    }
}

/// A version field parsed as a `u32`. A non-integer is a `WrongType`; a negative
/// or out-of-`u32`-range value is a `BadVersion`.
fn version(json: &Json, key: &str) -> Result<u32, MigrationError> {
    let n = required_int(json, key, "")?;
    u32::try_from(n).map_err(|_| MigrationError::new(MigrationErrorKind::BadVersion, key))
}

fn parse_step(json: &Json, index: usize) -> Result<Step, MigrationError> {
    let ctx = format!("steps[{index}]");
    let obj = json
        .as_object()
        .ok_or_else(|| MigrationError::new(MigrationErrorKind::NotAnObject, ctx.clone()))?;
    let kind = required_str(json, "kind", &ctx)?;
    match kind {
        "addType" => {
            reject_unknown(obj, &["kind", "name", "def"], &ctx)?;
            let name = required_name(json, "name", &ctx)?;
            let def_json = json.get("def").ok_or_else(|| {
                MigrationError::new(MigrationErrorKind::MissingField, at(&ctx, "def"))
            })?;
            // Root the type-def parser's error location at this step's `def`, so a
            // bad def reports `steps[i].def…` like every other step-level error.
            let def = schema::parse_type_def(def_json, &at(&ctx, "def"))
                .map_err(|e| MigrationError::new(MigrationErrorKind::TypeDef(e.kind), e.at))?;
            Ok(Step::AddType { name, def })
        }
        "removeType" => {
            reject_unknown(obj, &["kind", "name"], &ctx)?;
            Ok(Step::RemoveType {
                name: required_name(json, "name", &ctx)?,
            })
        }
        "renameType" => {
            reject_unknown(obj, &["kind", "from", "to"], &ctx)?;
            Ok(Step::RenameType {
                from: required_name(json, "from", &ctx)?,
                to: required_name(json, "to", &ctx)?,
            })
        }
        "addField" => {
            reject_unknown(obj, &["kind", "type", "field", "fieldType"], &ctx)?;
            Ok(Step::AddField {
                ty: required_name(json, "type", &ctx)?,
                field: required_name(json, "field", &ctx)?,
                field_type: required_name(json, "fieldType", &ctx)?,
            })
        }
        "removeField" => {
            reject_unknown(obj, &["kind", "type", "field"], &ctx)?;
            Ok(Step::RemoveField {
                ty: required_name(json, "type", &ctx)?,
                field: required_name(json, "field", &ctx)?,
            })
        }
        "renameField" => {
            reject_unknown(obj, &["kind", "type", "from", "to"], &ctx)?;
            Ok(Step::RenameField {
                ty: required_name(json, "type", &ctx)?,
                from: required_name(json, "from", &ctx)?,
                to: required_name(json, "to", &ctx)?,
            })
        }
        _ => Err(MigrationError::new(
            MigrationErrorKind::UnknownStepKind,
            at(&ctx, "kind"),
        )),
    }
}

/// Reject any key of `obj` outside `allowed`, so a typo'd field fails loud.
fn reject_unknown(
    obj: &[(String, Json)],
    allowed: &[&str],
    ctx: &str,
) -> Result<(), MigrationError> {
    for (key, _) in obj {
        if !allowed.contains(&key.as_str()) {
            return Err(MigrationError::new(
                MigrationErrorKind::UnknownField,
                at(ctx, key),
            ));
        }
    }
    Ok(())
}

/// A required string field that must be non-empty — every type / field name a
/// step carries identifies something, so the empty string is never meaningful.
fn required_name(json: &Json, key: &str, ctx: &str) -> Result<String, MigrationError> {
    let s = required_str(json, key, ctx)?;
    if s.is_empty() {
        return Err(MigrationError::new(
            MigrationErrorKind::EmptyName,
            at(ctx, key),
        ));
    }
    Ok(s.to_string())
}

fn required_str<'a>(json: &'a Json, key: &str, ctx: &str) -> Result<&'a str, MigrationError> {
    match json.get(key) {
        None => Err(MigrationError::new(
            MigrationErrorKind::MissingField,
            at(ctx, key),
        )),
        Some(v) => v
            .as_str()
            .ok_or_else(|| MigrationError::new(MigrationErrorKind::WrongType, at(ctx, key))),
    }
}

fn required_int(json: &Json, key: &str, ctx: &str) -> Result<i64, MigrationError> {
    match json.get(key) {
        None => Err(MigrationError::new(
            MigrationErrorKind::MissingField,
            at(ctx, key),
        )),
        Some(v) => v
            .as_i64()
            .ok_or_else(|| MigrationError::new(MigrationErrorKind::WrongType, at(ctx, key))),
    }
}
