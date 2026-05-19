(** Authentication and authorization.

    Token validation (JWT bearer for v0.1), actor_id binding to session, basic room-level read/write
    enforcement.

    Lands per SERVER-5, SERVER-6, WIRE-4 (see KANBAN.md). Design: see ARCHITECTURE.md, sections
    "Authentication", "Authorization". *)

(* Wrapper module for the [crdtsync_auth] library. Submodules from sibling files must be
   re-exported here to be reachable as [Crdtsync_auth.<Submodule>] from outside. *)

let version = "0.0.0"
