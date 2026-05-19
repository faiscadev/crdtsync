# crdtsync TypeScript SDK

Client-side SDK for browser and Node apps connecting to a [crdtsync](https://crdtsync.com) server. Sibling of the OCaml / Python / Go / Rust / JVM SDKs.

## Status

Pre-implementation. See [`../../KANBAN.md`](../../KANBAN.md), `SDK-1` through `SDK-11`.

## Develop

```bash
pnpm install
pnpm test         # vitest
pnpm build        # tsup (esm + cjs + types)
pnpm typecheck    # tsc --noEmit
```

Or from the repo root: `just sdk-ts-build`, `just sdk-ts-test`.

## Tooling

- Build: [tsup](https://tsup.egoist.dev/)
- Tests: [vitest](https://vitest.dev/)
- Types: TypeScript strict

## License

AGPL-3.0-or-later. See repo [`LICENSE`](../../LICENSE).
