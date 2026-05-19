(** Authentication and authorization.

    Token validation (JWT bearer for v0.1), actor_id binding to session, basic room-level read/write
    enforcement.

    Lands per SERVER-5, SERVER-6, WIRE-4 (see KANBAN.md). Design: see ARCHITECTURE.md, sections
    "Authentication", "Authorization". *)

let version = "0.0.0"
