(** Op identity. *)

type t = { client_id : Uuid_v7.t; client_seq : int64 }

let make ~client_id:cid ~client_seq:cseq : t = { client_id = cid; client_seq = cseq }
let client_id (opid : t) : Uuid_v7.t = opid.client_id
let client_seq (opid : t) : int64 = opid.client_seq

let compare (a : t) (b : t) : int =
  let c = Uuid_v7.compare a.client_id b.client_id in
  if c <> 0 then c else Int64.compare a.client_seq b.client_seq

let equal (a : t) (b : t) : bool =
  a.client_id = b.client_id && a.client_seq = b.client_seq

let pp (fmt : Format.formatter) (opid : t) : unit =
  Format.fprintf fmt "%a:%Ld" Uuid_v7.pp opid.client_id opid.client_seq
