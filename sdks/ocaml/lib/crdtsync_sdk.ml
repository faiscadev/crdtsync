(** crdtsync OCaml client SDK.

    Client-side connection + ergonomic API for OCaml apps that want to
    speak the crdtsync protocol as a peer of TS / Python / Go / Rust /
    JVM clients.

    Surface (lands per SDK-OCAML-1 through SDK-OCAML-7 — see KANBAN.md):
    - Document.open / connect      — establish session, run handshake
    - Document.transact            — client-side observer batching
    - reconnect with last_seen_seq tracking per channel
    - Token / auth helpers (bearer, in-band Auth)
    - Primitives wrappers (Map / List / Text / Register / Counter)
    - Anchors / RelativePosition surface
    - Live / observe API
    - Blob upload / fetch helpers

    Depends on:
    - crdtsync_core.crdt  for the local CRDT types
    - crdtsync_core.wire  for op envelope + codec + handshake messages

    Does NOT depend on crdtsync_core.{persist,server,auth,blob} — those
    are server-side concerns. *)

let version = "0.0.0"
