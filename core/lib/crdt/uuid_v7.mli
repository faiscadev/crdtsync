(** UUID version 7 (RFC 9562 §5.7) — time-ordered, 128 bits.

    Thin wrapper over {!Uuidm} that pins the variant to v7. All [client_id] values in the crdtsync
    wire envelope are values of this type. *)

type t
(** A v7 UUID. Abstract; round-trip via {!to_bytes} / {!of_bytes} or {!to_string} / {!of_string}. *)

val v : unit -> t
(** Generate a fresh v7 UUID. Embeds the current millisecond timestamp + random bits per RFC 9562.
*)

val to_bytes : t -> bytes
(** 16-byte binary representation. *)

val of_bytes : bytes -> t option
(** Parse a 16-byte binary representation. Returns [None] if the input is not 16 bytes or the
    version nibble is not [0x7]. *)

val to_string : t -> string
(** Standard 36-character hex form (e.g. ["018f2c5b-d6f3-7000-89ab-...]"). *)

val of_string : string -> t option
(** Parse the 36-character hex form. Returns [None] on malformed input or when the version nibble is
    not [0x7]. *)

val compare : t -> t -> int
(** Total order. For v7, this is also a time-ordering of generation (timestamps tiebroken by random
    bits). *)

val pp : Format.formatter -> t -> unit
(** Pretty-print as the standard 36-character hex form. *)
