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
//! versions, well-formed step params, non-empty names, no unknown keys). The
//! op-rewrite each step defines and the back-compatible-vs-breaking edge
//! classification are later slices; this is the model + parser only. The
//! value-transform kinds (wrap / setAttr / mapValues) are deferred with the
//! marks / XML layer they operate over.

use crate::json::{Json, JsonError, JsonErrorKind};
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
/// (`schema::TypeDef`). Each names the type or field it acts on; the op-rewrite
/// it implies is a later slice.
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
