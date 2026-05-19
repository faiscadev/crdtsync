# crdtsync — top-level build orchestration
# Each subdir keeps its native build system. This file only coordinates.

# Default: list available recipes
default:
    @just --list

# === Core (OCaml) ===
# dune-project lives at the repo root; the OCaml workspace covers
# `core/` and (later) `bindings/wasm`, `bindings/cabi`.

core-build:
    dune build

core-test:
    dune runtest

core-clean:
    dune clean

# === Bindings ===

wasm-build: core-build
    cd bindings/wasm && dune build

cabi-build: core-build
    cd bindings/cabi && dune build

# === SDKs ===

# OCaml SDK is part of the same dune workspace — no FFI, no separate
# toolchain. Builds when `dune build` runs.
sdk-ocaml-build: core-build
    dune build sdks/ocaml/

sdk-ocaml-test:
    dune runtest sdks/ocaml/

sdk-ts-build: wasm-build
    cd sdks/typescript && pnpm install && pnpm build

sdk-ts-test:
    cd sdks/typescript && pnpm test

sdk-py-build: cabi-build
    cd sdks/python && pip install -e .

sdk-py-test:
    cd sdks/python && pytest

sdk-go-build: cabi-build
    cd sdks/go && go build ./...

sdk-go-test:
    cd sdks/go && go test ./...

sdk-rust-build: cabi-build
    cd sdks/rust && cargo build

sdk-rust-test:
    cd sdks/rust && cargo test

sdk-jvm-build: cabi-build
    cd sdks/jvm && ./gradlew build

# === CLI ===
# The `crdtsync` binary builds as part of the OCaml workspace.

cli-build: core-build
    dune build core/bin/crdtsync/

cli-install:
    dune install crdtsync

# === Adapters ===

adapter-prosemirror-build: sdk-ts-build
    cd adapters/sync-prosemirror && pnpm install && pnpm build

adapter-codemirror-build: sdk-ts-build
    cd adapters/sync-codemirror && pnpm install && pnpm build

adapter-monaco-build: sdk-ts-build
    cd adapters/sync-monaco && pnpm install && pnpm build

adapter-lexical-build: sdk-ts-build
    cd adapters/sync-lexical && pnpm install && pnpm build

# === Examples ===

example-prosemirror-run: adapter-prosemirror-build
    cd examples/prosemirror-collab && pnpm dev

example-kanban-run: sdk-ts-build
    cd examples/kanban && pnpm dev

# === Lint / Format ===

lint: lint-ocaml lint-ts

lint-fix: lint-ocaml-fix lint-ts-fix

lint-ocaml:
    dune build @fmt

lint-ocaml-fix:
    dune build @fmt --auto-promote

lint-ts:
    cd sdks/typescript && pnpm lint && pnpm format:check

lint-ts-fix:
    cd sdks/typescript && pnpm lint:fix && pnpm format

# === Docs / website ===

docs-build:
    cd docs && # TODO

website-dev:
    cd website && pnpm dev

# === Aggregate ===

all: core-build sdk-ocaml-build wasm-build cabi-build cli-build sdk-ts-build sdk-py-build sdk-go-build sdk-rust-build

test-all: core-test sdk-ocaml-test sdk-ts-test sdk-py-test sdk-go-test sdk-rust-test

clean:
    cd core && dune clean
    rm -rf sdks/*/dist sdks/*/build sdks/*/target sdks/*/node_modules

# === Release ===

# Usage: just release 0.2.0
release VERSION:
    echo "{{VERSION}}" > VERSION
    git add VERSION
    git commit -m "release v{{VERSION}}"
    git tag "v{{VERSION}}"
    @echo "Tag created. Push with: git push origin main --tags"
    @echo "CI will publish all SDKs to their registries on tag push."
