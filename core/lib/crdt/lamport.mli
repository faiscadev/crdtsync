(** Lamport clock.

    Per-zone logical clock. The module exposes a single-clock primitive; callers own one [Lamport.t]
    per zone. Internally a nonnegative [int64] (values [0L .. Int64.max_int]); at 1 tick/ns that
    bound is ~292 years, so the wrap case is not a practical concern. See ARCHITECTURE.md, section
    "Algorithms and Invariants". *)

type t
(** Abstract, nonnegative. Construct via {!zero} / {!of_int64}; advance via {!tick} / {!merge};
    project for the wire via {!to_int64}. *)

val zero : t
(** Initial clock value. *)

val tick : t -> t
(** [tick c] returns the next clock value for a local event ([c + 1]). *)

val merge : recv:t -> local:t -> t
(** [merge ~recv ~local] is the post-receive clock: [max(recv, local) + 1]. Apply on every observed
    remote op. *)

val compare : t -> t -> int
(** Total order on clock values. Stable, suitable for [Map.Make] / [Set.Make] keys. *)

val equal : t -> t -> bool
(** [equal a b] iff [compare a b = 0]. *)

val pp : Format.formatter -> t -> unit
(** Pretty-print as a decimal integer. *)

val to_int64 : t -> int64
(** Wire projection. Returns the underlying nonnegative [int64]. *)

val of_int64 : int64 -> t
(** Wire injection. Raises [Failure] if [i] is negative. Callers (e.g. wire codec) are expected to
    validate / surface malformed input upstream. *)
