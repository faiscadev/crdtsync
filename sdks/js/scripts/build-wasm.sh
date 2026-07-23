#!/usr/bin/env bash
# Build the wasm-bindgen bindings for the CRDT core into src/wasm/, hidden
# behind the ergonomic package. The generated bundler-target module is imported
# by the handle layer; consumers never see it.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
wasm_crate="$here/../../crates/wasm"
out="$here/src/wasm"

rustup target add wasm32-unknown-unknown >/dev/null 2>&1 || true
wasm-pack build "$wasm_crate" --target bundler --release --out-dir "$out" --out-name crdtsync_wasm
# The generated package.json/.gitignore in the out dir are noise for our build.
rm -f "$out/package.json" "$out/.gitignore" "$out/README.md" "$out/LICENSE"
echo "wasm bindings written to $out"
