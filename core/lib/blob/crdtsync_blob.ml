(** Binary blobs.

    Local FS backend, HMAC-signed presigned URLs, co-located HTTP route for PUT/GET,
    requestUpload/confirmUpload/requestFetch flows, inline-under-4KB.

    Lands per BLOB-1 through BLOB-4 (see KANBAN.md). Design: see ARCHITECTURE.md, section
    "Binary Blobs". *)

(* Wrapper module for the [crdtsync_blob] library. Submodules from sibling files must be
   re-exported here to be reachable as [Crdtsync_blob.<Submodule>] from outside. *)

let version = "0.0.0"
