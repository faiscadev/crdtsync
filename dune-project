(lang dune 3.16)

(name crdtsync_core)
(version 0.0.0)

(generate_opam_files true)

(license AGPL-3.0-or-later)
(homepage "https://crdtsync.com")
(bug_reports "https://github.com/faiscadev/crdtsync/issues")
(source (github faiscadev/crdtsync))
(authors "Lucas Vieira")
(maintainers "Lucas Vieira <vieiralucas4@gmail.com>")

(package
 (name crdtsync_core)
 (synopsis "Self-hosted collaborative sync engine: portable CRDT core")
 (description
  "Core CRDT engine + sync server for crdtsync.

Sublibraries:
- crdtsync_core.crdt    : primitives (Map, List, Text, Register, Counter, anchors)
- crdtsync_core.wire    : op envelope, codec, version negotiation
- crdtsync_core.persist : SQLite, op log, snapshots
- crdtsync_core.auth    : token validation, actor binding
- crdtsync_core.blob    : local FS backend, presigned URLs
- crdtsync_core.server  : WebSocket server, handshake, channel routing

See ARCHITECTURE.md and ROADMAP.md.")
 (depends
  (ocaml (>= 5.1))
  (dune (>= 3.16))
  (logs (>= 0.7.0))
  (alcotest :with-test)
  (qcheck-alcotest :with-test)))

(package
 (name crdtsync)
 (synopsis "crdtsync CLI binary")
 (description
  "Server CLI for crdtsync: serve, snapshot, migrate, audit subcommands.
See ARCHITECTURE.md.")
 (depends
  (ocaml (>= 5.1))
  (dune (>= 3.16))
  crdtsync_core))

(package
 (name crdtsync_sdk)
 (synopsis "crdtsync OCaml client SDK")
 (description
  "Client-side SDK for OCaml apps: WebSocket client, handshake, reconnect,
ergonomic primitives wrappers, anchors, doc.transact(), observers, blob
helpers. Sibling of the TS / Python / Go / Rust / JVM SDKs.

Lives in the same dune workspace; sources under sdks/ocaml/.")
 (depends
  (ocaml (>= 5.1))
  (dune (>= 3.16))
  (crdtsync_core (= :version))
  (logs (>= 0.7.0))
  (alcotest :with-test)))
