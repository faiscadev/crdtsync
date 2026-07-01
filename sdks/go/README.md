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
