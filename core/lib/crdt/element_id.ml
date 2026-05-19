(** Element identity. *)

type t = Uuidm.t

let root : t = Uuidm.nil
let derive ~parent ~key : t = Uuidm.v5 parent key
let to_string (id : t) : string = Uuidm.to_string id
let of_string (str : string) : t option = Uuidm.of_string str
let to_bytes (id : t) : bytes = Uuidm.to_binary_string id |> Bytes.of_string

let of_bytes (bs : bytes) : t option =
  if Bytes.length bs <> 16 then None else Uuidm.of_binary_string (Bytes.to_string bs)

let compare = Uuidm.compare
let equal = Uuidm.equal
let pp = Uuidm.pp
