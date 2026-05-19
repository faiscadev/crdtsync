(** Op envelope. *)

type tx_role = Member | Commit

let pp_tx_role (fmt : Format.formatter) (r : tx_role) : unit =
  Format.pp_print_string fmt (match r with Member -> "member" | Commit -> "commit")

let equal_tx_role (a : tx_role) (b : tx_role) : bool =
  match (a, b) with Member, Member | Commit, Commit -> true | _ -> false

type t = {
  op_id : Op_id.t;
  actor_id : string;
  room : string;
  branch : string;
  zone : string;
  schema_version : int;
  lamport : Lamport.t;
  wall_time : Wall_time.t;
  op : Op.kind;
  tx : (Uuid_v7.t * tx_role) option;
}

let make ~op_id ~actor_id ~room ~branch ~zone ~schema_version ~lamport ~wall_time ~op ?tx () : t =
  { op_id; actor_id; room; branch; zone; schema_version; lamport; wall_time; op; tx }

let equal_tx (a : (Uuid_v7.t * tx_role) option) (b : (Uuid_v7.t * tx_role) option) : bool =
  Option.equal
    (fun (id_a, role_a) (id_b, role_b) ->
      Uuid_v7.compare id_a id_b = 0 && equal_tx_role role_a role_b)
    a b

let equal (a : t) (b : t) : bool =
  Op_id.equal a.op_id b.op_id
  && String.equal a.actor_id b.actor_id
  && String.equal a.room b.room && String.equal a.branch b.branch && String.equal a.zone b.zone
  && Int.equal a.schema_version b.schema_version
  && Lamport.equal a.lamport b.lamport
  && Wall_time.equal a.wall_time b.wall_time
  && Op.equal_kind a.op b.op && equal_tx a.tx b.tx

let pp_tx (fmt : Format.formatter) (tx : (Uuid_v7.t * tx_role) option) : unit =
  match tx with
  | None -> Format.pp_print_string fmt "None"
  | Some (id, role) -> Format.fprintf fmt "Some(%a, %a)" Uuid_v7.pp id pp_tx_role role

let pp (fmt : Format.formatter) (e : t) : unit =
  Format.fprintf fmt
    "@[<v 2>Envelope {@,\
     op_id = %a;@,\
     actor_id = %S;@,\
     room = %S;@,\
     branch = %S;@,\
     zone = %S;@,\
     schema_version = %d;@,\
     lamport = %a;@,\
     wall_time = %a;@,\
     op = %a;@,\
     tx = %a;@]@,\
     }"
    Op_id.pp e.op_id e.actor_id e.room e.branch e.zone e.schema_version Lamport.pp e.lamport
    Wall_time.pp e.wall_time Op.pp_kind e.op pp_tx e.tx
