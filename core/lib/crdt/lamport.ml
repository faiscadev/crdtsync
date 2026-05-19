(** Lamport clock. *)

type t = int64

let zero : t = 0L
let tick (l : t) : t = Int64.succ l
let merge ~recv:r ~local:l : t = Int64.max r l |> Int64.succ
let compare = Int64.compare
let equal = Int64.equal
let pp (fmt : Format.formatter) (l : t) : unit = Format.fprintf fmt "%Ld" l
let to_int64 (l : t) : int64 = l

let of_int64 (i : int64) : t =
  if i < 0L then failwith "Lamport.of_int64: negative value not allowed" else i
