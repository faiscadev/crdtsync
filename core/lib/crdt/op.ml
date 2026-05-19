(** Op kind variant. Placeholder for CORE-1; CORE-8 fills the closed enum. *)

type kind = Placeholder

let pp_kind (fmt : Format.formatter) (k : kind) : unit =
  match k with Placeholder -> Format.fprintf fmt "Placeholder"

let equal_kind (a : kind) (b : kind) : bool =
  match (a, b) with Placeholder, Placeholder -> true
