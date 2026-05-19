(** Wall-clock time.

    Real-world timestamp captured at op authoring. Informational only — not used for
    causality (see {!Lamport} for that). Powers debug / audit / display / analytics. See
    ARCHITECTURE.md, section "Internal Data Model". *)

type t
(** Abstract, nonnegative [int64] milliseconds since the Unix epoch (1970-01-01 UTC).
    Pre-epoch timestamps not supported. *)

val now : unit -> t
(** Current wall-clock time. *)

val of_ms : int64 -> t
(** Wire injection. Raises [Failure] if [ms] is negative. *)

val to_ms : t -> int64
(** Wire projection. Returns ms since Unix epoch. *)

val compare : t -> t -> int
val equal : t -> t -> bool

val pp : Format.formatter -> t -> unit
(** Pretty-print as a decimal millisecond count. *)
