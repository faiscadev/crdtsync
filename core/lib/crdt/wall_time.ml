(** Wall-clock time. *)

type t = int64

let now () : t = Int64.of_float (Unix.gettimeofday () *. 1000.0)

let of_ms (i : int64) : t =
  if i < 0L then failwith "Wall_time.of_ms: negative value not allowed" else i

let to_ms (t : t) : int64 = t
let compare = Int64.compare
let equal = Int64.equal
let pp (fmt : Format.formatter) (t : t) : unit = Format.fprintf fmt "%Ld" t
