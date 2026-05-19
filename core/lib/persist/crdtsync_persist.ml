(** Persistence.

    SQLite-backed op log + snapshot store, per-client last_seen_seq tracking, cold-start
    delivery.

    Lands per PERSIST-1 through PERSIST-5 (see KANBAN.md). Design: see ARCHITECTURE.md,
    sections "Persistence Architecture", "Snapshots". *)

(* Wrapper module for the [crdtsync_persist] library. Submodules from sibling files must be
   re-exported here to be reachable as [Crdtsync_persist.<Submodule>] from outside. *)

let version = "0.0.0"
