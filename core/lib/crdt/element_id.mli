(** Element identity: UUID v5 derived from [(parent_id, key)].

    Two clients computing [(same parent, same key)] arrive at the same [element_id] without
    coordination. This is the convergence mechanism for Map slot init: concurrent ergonomic helpers
    like [Map.text key] emit a [Set] op carrying the deterministic id, so LWW collapses to a single
    winning value with identical id on both sides — no orphan. See ARCHITECTURE.md, sections
    "Anchors and Element IDs" and "Map Slot Safety". *)

type t
(** Abstract. Construct via {!root} / {!derive} / {!of_bytes} / {!of_string}. *)

val root : t
(** Universal document-root id. The Nil UUID ([00000000-0000-0000-0000-000000000000]). All other
    element_ids are derived from a path rooted here. *)

val derive : parent:t -> key:string -> t
(** Deterministic child element_id under [parent]. Implemented via [Uuidm.v5 parent key]. *)

val to_string : t -> string
(** Standard dashed UUID format. *)

val of_string : string -> t option
(** Parses standard dashed UUID. Validates 36-character shape; does NOT enforce version (v5
    expected, but the wire may also carry {!root} = Nil = v0). *)

val to_bytes : t -> bytes
(** 16-byte binary representation. *)

val of_bytes : bytes -> t option
(** Validates length = 16; does NOT enforce version. *)

val compare : t -> t -> int
(** Total order on element_ids. Stable, suitable for [Map.Make] / [Set.Make] keys. *)

val equal : t -> t -> bool
val pp : Format.formatter -> t -> unit
