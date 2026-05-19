(** Slot values. *)

type scalar = String of string | Int of int64 | Float of float | Bool of bool | Null

let pp_scalar (fmt : Format.formatter) (scalar : scalar) : unit =
  match scalar with
  | String s -> Format.fprintf fmt "%S" s
  | Int i -> Format.fprintf fmt "%Ld" i
  | Float f -> Format.fprintf fmt "%f" f
  | Bool b -> Format.fprintf fmt "%b" b
  | Null -> Format.fprintf fmt "null"

let equal_scalar (a : scalar) (b : scalar) : bool =
  match (a, b) with
  | String s1, String s2 -> String.equal s1 s2
  | Int i1, Int i2 -> Int64.equal i1 i2
  | Float f1, Float f2 -> Float.equal f1 f2
  | Bool b1, Bool b2 -> Bool.equal b1 b2
  | Null, Null -> true
  | _ -> false

type t = Scalar of scalar | Element of Element_id.t

let pp (fmt : Format.formatter) (value : t) : unit =
  match value with
  | Scalar s -> pp_scalar fmt s
  | Element id -> Format.fprintf fmt "Element(%a)" Element_id.pp id

let equal (a : t) (b : t) : bool =
  match (a, b) with
  | Scalar s1, Scalar s2 -> equal_scalar s1 s2
  | Element id1, Element id2 -> Element_id.equal id1 id2
  | _ -> false
