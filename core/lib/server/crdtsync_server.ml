(** Sync server.

    WebSocket server, three-phase handshake (Hello / Auth / Subscribe), multiplexed channels per
    (room, branch, zone), op apply pipeline, reconnect resume from last_seen_seq.

    Lands per SERVER-1 through SERVER-7 (see KANBAN.md). Design: see ARCHITECTURE.md, sections
    "Networking Layer", "Realtime Synchronization", "Idempotency". *)

(* Wrapper module for the [crdtsync_server] library. Submodules from sibling files must be
   re-exported here to be reachable as [Crdtsync_server.<Submodule>] from outside. *)

let version = "0.0.0"
let run () = print_endline "crdtsync server: not yet implemented (see KANBAN.md SERVER-1)"
