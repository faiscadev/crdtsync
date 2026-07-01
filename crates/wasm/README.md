# crdtsync (WebAssembly / JavaScript)

WebAssembly bindings for the crdtsync CRDT core, for the browser, Node.js, and
Electron. Build the package with `wasm-pack`, then use `WasmDocument`:

```sh
wasm-pack build --target web crates/wasm   # or --target nodejs / bundler
wasm-pack test --node crates/wasm          # run the tests
```

```js
import init, { WasmDocument } from "crdtsync-wasm";
await init();

const a = new WasmDocument(new Uint8Array([1, ...Array(15).fill(0)]));
const b = new WasmDocument(new Uint8Array([2, ...Array(15).fill(0)]));

const path = WasmDocument.encodePath([
  new TextEncoder().encode("user"),
  new TextEncoder().encode("age"),
]);
const ops = a.registerInt(path, 30n); // nested path; returns ops to broadcast
b.apply(ops); // peer folds them in
b.getInt(path); // 30n — converged
```

A slot is addressed by a **path** — build it with `WasmDocument.encodePath(keys)`
from an array of `Uint8Array` keys naming nested maps, the last the slot. Edit
methods (`registerInt`, `inc`, `setBytes`, `delete`, `listInsert`, `listDelete`,
`textInsert`, `textDelete`) apply locally and return the encoded ops to send to
peers; `apply` folds a peer's ops back in. Read methods (`getInt`, `getCounter`,
`getBytes`, `listLen`, `listGet`, `textLen`, `textGet`) return the value or
`undefined`.

The bindings wrap the same `crdtsync_core::path` navigation the native C ABI
uses, so every SDK converges on one implementation.
