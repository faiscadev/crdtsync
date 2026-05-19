(** Map CRDT primitive.

    Single-Element keyed container with LWW per slot. Wire-level ops are minimal: {!Set} and
    {!Delete}. Higher-level ergonomic getOrCreate surfaces ([Map.text key], [Map.list key],
    [Map.map key]) live in the SDK; they derive a child [Element_id] from [(parent_id, key)] and
    emit a [Set] op with that derived id so concurrent calls converge on the same child Element by
    construction.

    See ARCHITECTURE.md, sections "Map" and "Map Slot Safety". *)

type t
(** Immutable Map CRDT state. Construct via {!empty}; advance via {!apply}. *)

type op =
  | Set of { key : string; value : Value.t }
  | Delete of { key : string }  (** Closed wire-level op set. SDK helpers expand to these. *)

val empty : element_id:Element_id.t -> t
(** A fresh empty Map identified by [element_id]. *)

val element_id : t -> Element_id.t
(** The Map's own element_id; stable across {!apply}. *)

val apply : t -> op:op -> op_id:Op_id.t -> lamport:Lamport.t -> t * Element_id.t list
(** [apply m ~op ~op_id ~lamport] returns [(m', released)] where:

    - [m'] is the new Map state after applying [op].
    - [released] is the list of [Element_id]s whose slot ref was displaced by this op (typically 0
      or 1 ids). The caller (doc layer) decides which displaced refs are actually orphans by
      cross-checking reachability against other slots.

    LWW tiebreak: higher [lamport] wins; on equal [lamport], higher [op_id] (via {!Op_id.compare})
    wins. Applying the same [op_id] twice is idempotent: same final state, no extra releases on the
    second call.

    Dedup of already-seen [op_id]s is the persist layer's responsibility; [apply] assumes each call
    carries a fresh op. *)

val get : t -> string -> Value.t option
(** Current value at [key], if any. *)

val keys : t -> string list
(** Currently-present keys, lexicographically sorted. *)

val entries : t -> (string * Value.t) list
(** Current [(key, value)] pairs, sorted by key. *)

val cardinal : t -> int
(** Number of currently-present keys. *)

val equal : t -> t -> bool
(** Structural equality across [element_id] and slot contents. *)

val pp : Format.formatter -> t -> unit
(** Multi-line human-readable dump. Format is debug-only, not stable. *)
