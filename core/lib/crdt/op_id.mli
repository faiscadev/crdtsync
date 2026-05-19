(** Op identity: [(client_id, client_seq)].

    Globally-unique identifier for every operation in the system. Used for idempotency
    (server ignores already-seen op_ids), undo-stack indexing, audit, and reconnect
    resume. See ARCHITECTURE.md, sections "Internal Data Model" and "Idempotency". *)

type t
(** Abstract. Construct via {!make}; project fields via {!client_id} / {!client_seq}. *)

val make : client_id:Uuid_v7.t -> client_seq:int64 -> t
(** [make ~client_id ~client_seq] constructs an op_id.

    [client_seq] is the monotonic per-client sequence number; callers are responsible for
    monotonicity (no enforcement here). *)

val client_id : t -> Uuid_v7.t
val client_seq : t -> int64

val compare : t -> t -> int
(** Total order on op_ids. Compares [client_id] first, then [client_seq] on tie. Stable,
    suitable for [Map.Make] / [Set.Make] keys. *)

val equal : t -> t -> bool
(** [equal a b] iff [compare a b = 0]. *)

val pp : Format.formatter -> t -> unit
(** Pretty-print as ["<client_id>:<client_seq>"]. *)
