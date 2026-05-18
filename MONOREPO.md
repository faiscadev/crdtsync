# Monorepo Layout

`crdtsync` is developed as a single monorepo: core engine, language SDKs, editor adapters, CLI, examples, website, and docs all live together.

## Why monorepo

Atomic changes across core + SDKs are essential. The wire format, schema language, ACL/auth model, and adapter contracts all touch multiple components at once — a change in the core's op envelope must land alongside the matching change in every SDK and adapter. Multi-repo layouts (Yjs style) make this coordination manual and slow; SDKs lag the core, broken pairs ship in the wild, and the rough edges leak into user code.

This is a **permanent commitment**. Core, SDKs, adapters, CLI, examples, and website live together forever. No package is extracted "once it stabilizes." Lockstep release of every component under a single version is the intentional model, not a temporary measure.

fakecloud and modern dev-tool stacks (Notion, Linear, Vercel) all ship as monorepos for the same reason.

## Layout

```
crdtsync/
├── core/                       OCaml CRDT engine + sync server
│   ├── dune-project
│   ├── lib/                    primitives, schema, repair, migrations, ACL, awareness
│   ├── server/                 sync server (websocket, persistence, cluster)
│   └── tests/
├── bindings/
│   ├── wasm/                   wasm_of_ocaml / js_of_ocaml build of core
│   └── cabi/                   C header + shared lib (.so / .dylib / .dll) build
├── sdks/
│   ├── typescript/             consumes WASM
│   ├── python/                 consumes C ABI via cffi
│   ├── go/                     consumes C ABI via cgo
│   ├── rust/                   consumes C ABI via bindgen
│   └── jvm/                    consumes C ABI via JNI / Panama
├── adapters/
│   ├── sync-prosemirror/       ProseMirror / Tiptap / BlockNote
│   ├── sync-codemirror/        CodeMirror
│   ├── sync-monaco/            Monaco
│   └── sync-lexical/           Lexical
├── cli/                        crdtsync binary (serve, migrate, snapshot, audit, compact)
├── examples/
│   ├── prosemirror-collab/
│   ├── code-editor/
│   ├── kanban/
│   └── notebook/
├── website/                    crdtsync.com source
├── docs/                       generated docs, references
├── scripts/                    release / dev / CI helpers
├── .github/workflows/          CI pipelines
├── Justfile                    top-level orchestration
├── VERSION                     single version source for all packages
├── README.md
├── MONOREPO.md
├── ARCHITECTURE.md
├── CONTRIBUTING.md
└── LICENSE
```

This mirrors fakecloud's `crates/` + `sdks/{lang}/` + `website/` + `examples/` shape. Differences: core is OCaml (dune) instead of Rust (cargo workspace); native bindings live in `bindings/` rather than as Rust crates.

## Versioning

**Lockstep**: every package (core, all SDKs, all adapters, CLI) ships under a single version sourced from the root `VERSION` file. One tag covers everything.

```text
$ cat VERSION
0.1.0
```

Release flow:

1. Bump `VERSION`
2. Tag the monorepo: `git tag v0.1.0`
3. CI publishes each SDK to its native registry under that version:
   - `npm publish crdtsync@0.1.0`
   - `pip publish crdtsync==0.1.0`
   - `cargo publish crdtsync@0.1.0`
   - Go: tag-based, `go get github.com/faiscadev/crdtsync/sdks/go@v0.1.0`
   - Maven: `dev.faisca:crdtsync:0.1.0`

Lockstep is intentional and permanent. Components ship together because they're designed together; independent semver across packages would re-introduce the coordination drift that monorepos exist to prevent.

## Build orchestration

Top-level `Justfile` exposes recipes per component. Each subdir keeps its native build system (dune, npm/pnpm, pyproject, go.mod, Cargo.toml, Maven) — the Justfile only coordinates.

Examples:

```bash
just core-build              # dune build the OCaml core
just core-test               # dune runtest

just wasm-build              # build core to WASM
just cabi-build              # build core to shared lib

just sdk-ts-build            # build TS SDK (depends on WASM)
just sdk-py-build            # build Python SDK (depends on C ABI)
just sdk-go-build            # build Go SDK (depends on C ABI)
just sdk-rust-build          # build Rust SDK (depends on C ABI)

just cli-build               # build crdtsync CLI binary
just cli-install             # install to ~/bin

just adapter-prosemirror-build
just adapter-codemirror-build

just example-prosemirror-run
just example-kanban-run

just docs-build
just website-dev

just all                     # build everything
just test-all                # run all test suites
just release VERSION=0.1.0   # full release pipeline
```

## CI matrix

GitHub Actions runs jobs per concern:

| Job | When |
|-----|------|
| `core-build-test` | every push |
| `wasm-build` | every push touching `core/`, `bindings/wasm/` |
| `cabi-build` | every push touching `core/`, `bindings/cabi/`, any `sdks/{python,go,rust,jvm}/` |
| `sdk-{lang}-test` | every push touching `sdks/{lang}/` or its build deps |
| `adapter-{name}-test` | every push touching adapter dirs |
| `cli-build-test` | every push touching `cli/` |
| `release` | on tag push (`v*`) |

Built artifacts (WASM module, shared lib) are cached between jobs so each SDK build pulls from a known path instead of rebuilding core.

## Contributing scope

Contributors can work on a single component without needing to understand the whole stack:

- **Just want to fix the Python SDK?** Read `sdks/python/README.md` and the relevant Justfile recipes. You need OCaml installed only if you're changing the C ABI surface.
- **Just want to write a new editor adapter?** Read `adapters/sync-prosemirror/` as reference. You consume the TS SDK; you don't need to touch OCaml.
- **Want to add a new primitive or schema feature?** That's a core change. Touches `core/`, then ripples through bindings, SDKs, and adapters. Larger scope, full stack.

Each subdir ships a focused `README.md` describing what it is and how to work on it in isolation.

## Why not Bazel / Buck2

Considered. Rejected for now because:

- Multi-language coordination is needed only at "produce artifact A, consume artifact A" boundaries — a Justfile handles that fine
- Each ecosystem already has a mature native build tool; layering a meta-build-tool on top costs more than it saves at this scale
- Onboarding cost: contributors know dune / npm / pip / cargo / go; learning Bazel for our project is friction we don't need

Revisit if the project grows to where parallel incremental builds across all SDKs become a bottleneck.

## Trade-offs acknowledged

| Concern | Mitigation |
|---------|-----------|
| Repo size grows (multiple lockfiles, vendored deps) | `.gitignore` aggressive; deps not vendored except where required |
| CI matrix is wide | Path-filtered jobs run only when relevant subdirs change |
| New contributors see a big repo and bounce | Per-subdir READMEs scoped tightly; CONTRIBUTING.md explains the "you can ignore most of this" path |
| Lockstep versioning loses per-SDK semver autonomy | Intentional and permanent — coupled releases are the point |
| Cross-language atomic changes can land breaking everything at once | CI gates per-component build; reviewers required to think across affected components |

## Reference

Org sibling fakecloud uses the same shape — see [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud) for a working monorepo with Rust core + multi-language SDKs in `sdks/{go,java,php,python,typescript}/`.
