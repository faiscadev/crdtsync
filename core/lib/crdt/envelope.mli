(** Op envelope.

    Immutable record carried with every operation. Mirrors the wire shape described in
    ARCHITECTURE.md, section "Internal Data Model".

    Transaction membership: in the wire format, [tx_id] and [tx_role] appear iff together.
    Here they are folded into a single optional pair to make the invalid state
    unrepresentable. *)

type tx_role =
  | Member  (** part of an open tx *)
  | Commit  (** commit marker; closes the tx *)

val pp_tx_role : Format.formatter -> tx_role -> unit
val equal_tx_role : tx_role -> tx_role -> bool

type t = private {
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
      (** [Some (tx_id, role)] iff member of a tx; [tx_id] is client-generated UUID v7 *)
}
(** Read-only record. Construct via {!make}. *)

val make :
  op_id:Op_id.t ->
  actor_id:string ->
  room:string ->
  branch:string ->
  zone:string ->
  schema_version:int ->
  lamport:Lamport.t ->
  wall_time:Wall_time.t ->
  op:Op.kind ->
  ?tx:Uuid_v7.t * tx_role ->
  unit ->
  t
(** Construct an envelope. [?tx] omitted = standalone op (no transaction). *)

val equal : t -> t -> bool
(** Structural equality across all fields. *)

val pp : Format.formatter -> t -> unit
(** Multi-line human-readable dump. Format is debug-only, not stable. *)
