(** Wire protocol.

    Op envelope, codec (CBOR for v0.1), version-negotiation header, handshake (Hello / Auth /
    Subscribe), Error envelope.

    Lands per WIRE-1 through WIRE-7 (see KANBAN.md). Design: see ARCHITECTURE.md, sections "Internal
    Data Model", "Networking Layer". *)

let version = "0.0.0"
