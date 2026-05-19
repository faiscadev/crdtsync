(** Persistence.

    SQLite-backed op log + snapshot store, per-client last_seen_seq tracking,
    cold-start delivery.

    Lands per PERSIST-1 through PERSIST-5 (see KANBAN.md).
    Design: see ARCHITECTURE.md, sections "Persistence Architecture", "Snapshots". *)

let version = "0.0.0"
