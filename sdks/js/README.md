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
doc.on("update", ({ origin, ops, changes }) => {
  /* broadcast `ops` when origin === "local"; apply peers' with doc.applyUpdate(ops).
     `changes` are diff-derived: { kind, path (ergonomic keys), old/new native values }. */
});

// Or observe just one handle's subtree:
const off = root.observe(({ origin, changes }) => {
  /* fires only for changes under `root` */
});
off(); // unsubscribe
```

## Sync

Bind a document to a crdtsync server over a WebSocket with `connect`:

```ts
import { connect } from "@crdtsync/client";

const provider = await connect("ws://localhost:9000", "my-room");
provider.doc.getMap("root").set("title", "shared"); // sent to peers
provider.onState((s) => console.log(s)); // "connecting" | "connected" | "disconnected"
provider.setAwareness("cursor", "42"); // ephemeral presence
```

The provider owns the WebSocket + wire session and keeps `provider.doc` in sync:
local edits are framed and sent, inbound updates fold in and fire reactivity, and
a dropped socket reconnects — resuming the channel and resending unacked edits, so
edits made offline converge once the link returns. In a browser the global
`WebSocket` is used; on Node before v22 pass one: `connect(url, room, { WebSocket })`
from the `ws` package.

## Model

- **Handles.** `Doc.getMap/getList/getText(key)` return live `CrdtMap` / `CrdtList` /
  `CrdtText` handles addressed by ergonomic keys, never byte-paths. Handles own their
  path and re-resolve on every op, and compose (`map.getMap("child").set(...)`).
- **Marshaling.** `string` / `number` (integer) / `bigint` / `boolean` / `null` /
  `Uint8Array` map to CRDT scalars. Containers are created with the explicit
  `getMap`/`getList`/`getText` accessors — a plain object/array passed to `set` is a
  type error (the explicit leaf-vs-container boundary; no implicit deep-seed).
- **Reactivity.** `Doc.on("update", cb)` fires on every applied change; `handle.observe(cb)`
  fires only for a subtree. Change events are re-marshaled from the core diff into
  ergonomic key/index targets and native before/after values, with a `local`/`remote`
  origin. (Creating a container reports one `add`; subsequent edits are fine-grained.)

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
