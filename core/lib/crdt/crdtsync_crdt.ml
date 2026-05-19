(** CRDT primitives.

    Map, List, Text, Register, Counter — plus anchors / RelativePosition.

    Implementations land in CORE-2 through CORE-6 (see KANBAN.md). Design: see ARCHITECTURE.md,
    sections "CRDT Model", "Map Slot Safety", "Anchors and Element IDs", "Text and Unicode". *)

(* Wrapper module for the [crdtsync_crdt] library. Because a file matching the library name
   exists, dune does NOT auto-export sibling modules — they must be re-exported here to be
   reachable as [Crdtsync_crdt.<Submodule>] from outside. Add new lines here as submodules
   land. *)

module Uuid_v7 = Uuid_v7
module Op_id = Op_id
module Lamport = Lamport
module Wall_time = Wall_time
module Element_id = Element_id
module Value = Value
module Map = Map
module Op = Op
module Envelope = Envelope

let version = "0.0.0"
