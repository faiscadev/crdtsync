# @crdtsync/client

The ergonomic JavaScript/TypeScript SDK for [crdtsync](../../ARCHITECTURE.md) — a
handle-graph over the CRDT core. Edit a document through live typed handles with
native-value marshaling; the byte-path core stays hidden.

```ts
import { Doc } from "@crdtsync/client";

const doc = new Doc();
const root = doc.getMap("root");
root.set("title", "Hello");
root.getList("todos").push("write docs");
root.getText("body").insert(0, "collaborative text");

// Observe applied changes (local edits and remote updates).
doc.on("update", ({ origin, ops }) => {
  /* broadcast `ops` when origin === "local"; apply peers' with doc.applyUpdate(ops) */
});
```

## Model

- **Handles.** `Doc.getMap/getList/getText(key)` return live `CrdtMap` / `CrdtList` /
  `CrdtText` handles addressed by ergonomic keys, never byte-paths. Handles own their
  path and re-resolve on every op, and compose (`map.getMap("child").set(...)`).
- **Marshaling.** `string` / `number` (integer) / `bigint` / `boolean` / `null` /
  `Uint8Array` map to CRDT scalars. Containers are created with the explicit
  `getMap`/`getList`/`getText` accessors — a plain object/array passed to `set` is a
  type error (the explicit leaf-vs-container boundary; no implicit deep-seed).

## Development

The package wraps generated wasm bindings; the build script produces them with
`wasm-pack` from `crates/wasm`.

```sh
npm install
npm run build:wasm   # generate src/wasm from crates/wasm
npm run check        # biome lint + format
npm run typecheck
npm test             # vitest
npm run build        # tsc + copy wasm into dist
```
