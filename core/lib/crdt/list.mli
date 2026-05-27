(** List CRDT primitive.

    Sequence with stable per-entry identity. Algorithm: Fugue
    (Weidner & Kleppmann 2023, "The Art of the Fugue") — tree-based,
    formally proven to avoid the interleaving anomaly on concurrent
    inserts at the same point. Same algorithm reused for Text in CORE-4.

    Identity model: every entry has a stable [entry_id : Element_id.t]
    derived from [(list_element_id, insert_op_id)] at insert time.
    Stable across {!apply} of [Move] or [Delete]. Tombstoned entries
    keep their record so other entries can keep referencing them as
    origin neighbors and so cursors anchored on them remain valid.

    Wire-level ops: {!Insert}, {!Delete}, {!Move}. The ergonomic SDK
    surface ([List.insert ~index], [List.move ~from_ ~to_], [List.text
    ~index]) translates user-facing indices into the right neighbor
    pairs via {!neighbors_at} before emitting these ops.

    See ARCHITECTURE.md, sections "List", "Anchors and Element IDs",
    "CRDT Model". *)

type t
(** Immutable list CRDT state. Construct via {!empty}; advance via
    {!apply}. *)

type op =
  | Insert of {
      entry_id : Element_id.t;
          (** Stable entry id; caller derives via
              [Element_id.derive ~parent:list_id ~key:(Op_id.to_string op_id)]. *)
      value : Value.t;
      origin_left : Element_id.t option;  (** [None] = head *)
      origin_right : Element_id.t option;  (** [None] = tail *)
    }
  | Delete of { target : Element_id.t }
  | Move of {
      target : Element_id.t;
      new_origin_left : Element_id.t option;
      new_origin_right : Element_id.t option;
    }
      (** Closed wire-level op set. *)

val empty : element_id:Element_id.t -> t
(** A fresh empty List identified by [element_id]. *)

val element_id : t -> Element_id.t
(** The List's own element_id; stable across {!apply}. *)

val apply : t -> op:op -> op_id:Op_id.t -> lamport:Lamport.t -> t * Element_id.t list
(** [apply l ~op ~op_id ~lamport] returns [(l', released)] where:

    - [l'] is the new List state.
    - [released] is the list of [Element_id]s whose ref left the list
      via this op (typically 0 or 1 ids). Caller (doc layer) decides
      orphan status by cross-checking other slot references.

    LWW for {!Move} per-entry on the move op's [(lamport, op_id)].
    {!Insert} and {!Delete} are idempotent on [entry_id] / [target]
    respectively. *)

val get : t -> int -> Value.t option
(** [get l i] returns the value at logical index [i] (live entries
    only, tombstones skipped). [None] if [i] out of range. *)

val get_entry : t -> Element_id.t -> Value.t option
(** [get_entry l eid] returns the value held at entry [eid] if it is
    live; [None] if absent or tombstoned. *)

val index_of : t -> Element_id.t -> int option
(** Logical index of a live entry, [None] if absent or tombstoned.
    Useful for displaying cursor positions. *)

val length : t -> int
(** Number of live entries (tombstones excluded). *)

val to_list : t -> Value.t list
(** Live entries in order. *)

val entries : t -> (Element_id.t * Value.t) list
(** Live [(entry_id, value)] pairs in order. *)

val fold : t -> init:'a -> f:('a -> Element_id.t -> Value.t -> 'a) -> 'a
(** Left fold over live entries in order. *)

val equal : t -> t -> bool
(** Structural equality across [element_id] and entry contents. *)

val pp : Format.formatter -> t -> unit
(** Multi-line human-readable dump. Format is debug-only, not stable. *)
