(** Slot values for Map (and other primitives that hold a single value).

    Value taxonomy follows ARCHITECTURE.md, section "Internal Data Model":

    {v
    Value =
      | Scalar  (string, int, float, bool, null)
      | Element (reference to a child CRDT by element_id)
      | Blob    (BlobRef — adds when the blob subsystem lands)
    v}

    Blob constructor lands alongside the blob subsystem. *)

type scalar = String of string | Int of int64 | Float of float | Bool of bool | Null

val pp_scalar : Format.formatter -> scalar -> unit
val equal_scalar : scalar -> scalar -> bool

type t = Scalar of scalar | Element of Element_id.t  (** reference to a child CRDT *)

val pp : Format.formatter -> t -> unit
val equal : t -> t -> bool
