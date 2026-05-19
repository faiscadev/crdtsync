(** Op kind variant.

    Placeholder for v0.1; the closed enum of CRDT ops (text.insert, map.set, xml.setAttr, acl.grant,
    migrate, ...) lands in CORE-8. Each constructor carries its own target + payload — there is no
    separate [target] / [payload] field on the envelope. See ARCHITECTURE.md, sections "Internal
    Data Model" and "Supported Operations". *)

type kind = Placeholder  (** v0.1 placeholder. CORE-8 replaces with the full closed enum. *)

val pp_kind : Format.formatter -> kind -> unit
val equal_kind : kind -> kind -> bool
