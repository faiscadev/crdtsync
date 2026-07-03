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

/// A parsed, validated schema.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    name: String,
    version: i64,
    root: String,
    types: Vec<(String, TypeDef)>,
}

/// One named type: a built primitive with its declared constraints.
#[derive(Clone, Debug, PartialEq)]
pub enum TypeDef {
    /// A map with named, typed slots (each slot name → the type it holds).
    /// The slot set is the allowlist; declaration order is preserved.
    Map { children: Vec<(String, String)> },
    /// An ordered list of `items`-typed elements, with an optional count range.
    List {
        items: String,
        min: Option<u64>,
        max: Option<u64>,
    },
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
            SchemaErrorKind::Json(k) => return write!(f, "invalid schema JSON: {k:?}"),
            SchemaErrorKind::NotAnObject => "expected an object",
            SchemaErrorKind::MissingField => "missing required field",
            SchemaErrorKind::WrongType => "field has the wrong type",
            SchemaErrorKind::UnknownKind => "unknown type kind",
            SchemaErrorKind::UnknownTypeRef => "reference to an undeclared type",
            SchemaErrorKind::RootNotMap => "root type is not a map",
            SchemaErrorKind::BadRange => "ill-formed numeric bound",
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

    /// Parse a schema from its JSON source. Total — any input yields a `Schema`
    /// or a [`SchemaError`], never a panic.
    pub fn parse(src: &str) -> Result<Schema, SchemaError> {
        let json = Json::parse(src)
            .map_err(|e: JsonError| SchemaError::new(SchemaErrorKind::Json(e.kind), "document"))?;
        Schema::from_json(&json)
    }

    /// Build a schema from an already-parsed JSON value.
    pub fn from_json(json: &Json) -> Result<Schema, SchemaError> {
        json.as_object()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "document"))?;

        let name = required_str(json, "schema")?.to_string();
        let version = required_int(json, "version")?;
        let root = required_str(json, "root")?.to_string();

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

        let schema = Schema {
            name,
            version,
            root,
            types,
        };
        schema.validate_references()?;
        Ok(schema)
    }

    /// Every type a schema names must be declared, and `root` must be a map —
    /// the closure guarantee that makes runtime repair total.
    fn validate_references(&self) -> Result<(), SchemaError> {
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
                        self.require_declared(ty)?;
                    }
                }
                TypeDef::List { items, .. } => self.require_declared(items)?,
                TypeDef::Text { .. } | TypeDef::Register { .. } | TypeDef::Counter { .. } => {}
            }
        }
        Ok(())
    }

    fn require_declared(&self, name: &str) -> Result<(), SchemaError> {
        if self.type_def(name).is_none() {
            return Err(SchemaError::new(
                SchemaErrorKind::UnknownTypeRef,
                name.to_string(),
            ));
        }
        Ok(())
    }
}

fn parse_type_def(json: &Json, type_name: &str) -> Result<TypeDef, SchemaError> {
    json.as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, type_name.to_string()))?;
    let kind = required_str(json, "kind")?;
    match kind {
        "map" => Ok(TypeDef::Map {
            children: parse_children(json)?,
        }),
        "list" => {
            let items = required_str(json, "items")?.to_string();
            let (min, max) = counts(json)?;
            Ok(TypeDef::List { items, min, max })
        }
        "text" => Ok(TypeDef::Text {
            max: count_field(json, "max")?,
        }),
        "register" => {
            let (min, max) = bounds(json)?;
            Ok(TypeDef::Register { min, max })
        }
        "counter" => {
            let (min, max) = bounds(json)?;
            Ok(TypeDef::Counter { min, max })
        }
        _ => Err(SchemaError::new(
            SchemaErrorKind::UnknownKind,
            type_name.to_string(),
        )),
    }
}

fn parse_children(json: &Json) -> Result<Vec<(String, String)>, SchemaError> {
    let Some(children) = json.get("children") else {
        return Ok(Vec::new());
    };
    let obj = children
        .as_object()
        .ok_or_else(|| SchemaError::new(SchemaErrorKind::NotAnObject, "children"))?;
    let mut out = Vec::with_capacity(obj.len());
    for (slot, ty) in obj {
        let type_name = ty
            .as_str()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, slot.clone()))?;
        out.push((slot.clone(), type_name.to_string()));
    }
    Ok(out)
}

/// The `min`/`max` of a register or counter — full-range `i64` bounds.
fn bounds(json: &Json) -> Result<(Option<i64>, Option<i64>), SchemaError> {
    let min = int_field(json, "min")?;
    let max = int_field(json, "max")?;
    if let (Some(a), Some(b)) = (min, max) {
        if a > b {
            return Err(SchemaError::new(SchemaErrorKind::BadRange, "min > max"));
        }
    }
    Ok((min, max))
}

/// The `min`/`max` of a list — non-negative element counts.
fn counts(json: &Json) -> Result<(Option<u64>, Option<u64>), SchemaError> {
    let min = count_field(json, "min")?;
    let max = count_field(json, "max")?;
    if let (Some(a), Some(b)) = (min, max) {
        if a > b {
            return Err(SchemaError::new(SchemaErrorKind::BadRange, "min > max"));
        }
    }
    Ok((min, max))
}

fn int_field(json: &Json, key: &str) -> Result<Option<i64>, SchemaError> {
    match json.get(key) {
        None => Ok(None),
        Some(v) => Ok(Some(v.as_i64().ok_or_else(|| {
            SchemaError::new(SchemaErrorKind::WrongType, key.to_string())
        })?)),
    }
}

fn count_field(json: &Json, key: &str) -> Result<Option<u64>, SchemaError> {
    match int_field(json, key)? {
        None => Ok(None),
        Some(n) if n < 0 => Err(SchemaError::new(SchemaErrorKind::BadRange, key.to_string())),
        Some(n) => Ok(Some(n as u64)),
    }
}

fn required_str<'a>(json: &'a Json, key: &str) -> Result<&'a str, SchemaError> {
    match json.get(key) {
        None => Err(SchemaError::new(
            SchemaErrorKind::MissingField,
            key.to_string(),
        )),
        Some(v) => v
            .as_str()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, key.to_string())),
    }
}

fn required_int(json: &Json, key: &str) -> Result<i64, SchemaError> {
    match json.get(key) {
        None => Err(SchemaError::new(
            SchemaErrorKind::MissingField,
            key.to_string(),
        )),
        Some(v) => v
            .as_i64()
            .ok_or_else(|| SchemaError::new(SchemaErrorKind::WrongType, key.to_string())),
    }
}
