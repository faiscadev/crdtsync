(** Wire protocol.

    Op envelope, codec (CBOR for v0.1), version-negotiation header, handshake (Hello / Auth /
    Subscribe), Error envelope.

    Lands per WIRE-1 through WIRE-7 (see KANBAN.md). Design: see ARCHITECTURE.md, sections "Internal
    Data Model", "Networking Layer". *)

(* Wrapper module for the [crdtsync_wire] library. Submodules from sibling files must be
   re-exported here (e.g. [module Envelope = Envelope]) to be reachable as
   [Crdtsync_wire.<Submodule>] from outside. *)

let version = "0.0.0"
