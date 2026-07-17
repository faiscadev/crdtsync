//! `crdtsync-codegen` — emit typed SDK accessors from a schema JSON file.
//!
//! Usage: `crdtsync-codegen <schema.json> [--lang python] [-o <out>]`
//!
//! Reads the schema file, validates it through the core [`Schema`] parser (the
//! sole validator), and writes the generated accessor source to `-o` or stdout.

use crdtsync_core::schema::Schema;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut input: Option<String> = None;
    let mut lang = String::from("python");
    let mut output: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lang" => match args.next() {
                Some(v) => lang = v,
                None => return fail("--lang needs a value"),
            },
            "-o" | "--out" => match args.next() {
                Some(v) => output = Some(v),
                None => return fail("-o needs a value"),
            },
            "-h" | "--help" => {
                eprintln!("usage: crdtsync-codegen <schema.json> [--lang python] [-o <out>]");
                return ExitCode::SUCCESS;
            }
            other if input.is_none() => input = Some(other.to_string()),
            other => return fail(&format!("unexpected argument {other:?}")),
        }
    }

    let Some(input) = input else {
        return fail("a schema JSON path is required");
    };
    let src = match std::fs::read_to_string(&input) {
        Ok(s) => s,
        Err(e) => return fail(&format!("read {input}: {e}")),
    };
    let schema = match Schema::parse(&src) {
        Ok(s) => s,
        Err(e) => return fail(&format!("parse {input}: {e:?}")),
    };

    let generated = match lang.as_str() {
        "python" | "py" => crdtsync_codegen::generate_python(&schema),
        other => return fail(&format!("unknown --lang {other:?} (supported: python)")),
    };

    match output {
        Some(path) => {
            if let Err(e) = std::fs::write(&path, generated) {
                return fail(&format!("write {path}: {e}"));
            }
        }
        None => print!("{generated}"),
    }
    ExitCode::SUCCESS
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("crdtsync-codegen: {msg}");
    ExitCode::FAILURE
}
