//! Regenerate the C header from the ABI so `include/crdtsync.h` always matches
//! the exported functions. Consumers (Go/Python) include the committed header.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out = Path::new(&crate_dir).join("include/crdtsync.h");
    if let Some(dir) = out.parent() {
        let _ = std::fs::create_dir_all(dir);
    }

    match cbindgen::generate(&crate_dir) {
        Ok(bindings) => {
            bindings.write_to_file(&out);
        }
        // A header-generation hiccup must not fail the crate build; the drift
        // test guards that the committed header stays in sync.
        Err(e) => println!("cargo:warning=cbindgen: {e}"),
    }
}
