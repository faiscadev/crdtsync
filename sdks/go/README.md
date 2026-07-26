# crdtsync (Go)

Go bindings for the crdtsync CRDT core over its C ABI, linked with cgo. Build the
static library first, then use `Document`:

```sh
cargo build -p crdtsync-ffi --release   # produces target/release/libcrdtsync_ffi.a
cd sdks/go && go test ./...
```

```go
import "github.com/faiscadev/crdtsync/sdks/go/crdtsync"

a, _ := crdtsync.New(append([]byte{1}, make([]byte, 15)...))
b, _ := crdtsync.New(append([]byte{2}, make([]byte, 15)...))
defer a.Close()
defer b.Close()

path := [][]byte{[]byte("user"), []byte("age")}
ops := a.RegisterInt(path, 30) // nested path; returns ops to broadcast
b.Apply(ops)                   // peer folds them in
v, _ := b.GetInt(path)         // 30 — converged
```

A slot is addressed by a **path** — a slice of `[]byte` keys naming nested maps,
the last the slot. Edit methods (`RegisterInt`, `Inc`, `SetBytes`, `Delete`,
`ListInsert`, `ListDelete`, `TextInsert`, `TextDelete`) apply locally and return
the encoded ops to send to peers; `Apply` folds a peer's ops back in. Read methods
(`GetInt`, `GetCounter`, `GetBytes`, `ListLen`, `ListGet`, `TextLen`, `TextGet`)
return the value and an `ok` bool.

cgo links `target/release/libcrdtsync_ffi.a` via `${SRCDIR}`-relative flags, so
build the release library before `go test`. Close a document to free it.

## Collaborating over a server

`Connect` opens a socket to a crdtsync server, joins a room, and returns once the
room's state has synced. The `Doc` it carries is the ergonomic handle graph —
edits frame and send, inbound frames fold into the same replica, and a dropped
socket resumes and resends what the server never acknowledged.

```go
p, err := crdtsync.Connect(ctx, "ws://localhost:6060", "my-room", crdtsync.ProviderOptions{})
if err != nil {
	return err
}
defer p.Close()

p.Doc().GetMap("root").Set("title", "Hello")
p.Doc().GetText("body").Insert(0, "hi")
p.SetAwareness("cursor", []byte("10"))
```

The WebSocket transport is built in, so the SDK stays dependency-free; supply
`ProviderOptions.Dial` to plug in another. `ProviderOptions` also carries the
credential, the app/schema declaration, the reconnect policy, and the server's
signals (`OnError`, `OnOpsRejected`, `OnRedirect`).

The transport-agnostic `Provider` remains for syncing over something crdtsync
knows nothing about — a peer mesh, a message bus, a test harness.

A networked `Doc` is the full handle graph: values, blobs, XML, cursors, marks,
change events, and `Observe` all answer off the room's replica. The one
exception is `SetSchema` — binding a schema is replica-local runtime state with
no per-channel seat, so a networked `Doc` reports no repairs.

The integration tests spin up the real server; build it first with
`cargo build -p crdtsync-server` (they skip when it is absent).
