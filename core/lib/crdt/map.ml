(** Map CRDT primitive. *)

module StringMap = Stdlib.Map.Make (String)

(* A slot is the per-key LWW state. Both Live and Tomb carry (lamport, op_id)
   so a stale Set with a lower lamport cannot resurrect a key that was deleted
   at a higher lamport. *)
type slot =
  | Live of { value : Value.t; op_id : Op_id.t; lamport : Lamport.t }
  | Tomb of { op_id : Op_id.t; lamport : Lamport.t }

type t = { element_id : Element_id.t; data : slot StringMap.t }
type op = Set of { key : string; value : Value.t } | Delete of { key : string }

let empty ~element_id : t = { element_id; data = StringMap.empty }
let element_id (m : t) : Element_id.t = m.element_id

let slot_meta = function
  | Live { lamport; op_id; _ } -> (lamport, op_id)
  | Tomb { lamport; op_id } -> (lamport, op_id)

(* LWW comparator: higher lamport wins; on equal lamport, higher op_id wins;
   if both equal -> same op (idempotent re-apply), candidate loses (no change). *)
let candidate_wins ~lamport ~op_id current =
  match current with
  | None -> true
  | Some c ->
      let cur_l, cur_o = slot_meta c in
      let cmp = Lamport.compare lamport cur_l in
      cmp > 0 || (cmp = 0 && Op_id.compare op_id cur_o > 0)

let apply (m : t) ~op ~op_id ~lamport : t * Element_id.t list =
  let key, new_slot =
    match op with
    | Set { key; value } -> (key, Live { value; op_id; lamport })
    | Delete { key } -> (key, Tomb { op_id; lamport })
  in
  let current = StringMap.find_opt key m.data in
  if not (candidate_wins ~lamport ~op_id current) then
    (* Losing op may still strand an Element id: a Set carrying an Element
       value whose id is not the same id currently held in the slot leaves
       that id with no home. Surface it so the doc layer can orphan it. *)
    let stranded =
      match (op, current) with
      | Set { value = Element new_id; _ }, Some (Live { value = Element cur_id; _ })
        when Element_id.equal new_id cur_id ->
          []
      | Set { value = Element new_id; _ }, _ -> [ new_id ]
      | _ -> []
    in
    (m, stranded)
  else
    (* Released = element_ids whose ref is no longer in this slot after the
       op. The same Element id being re-set by a concurrent client carrying
       the same derived id is NOT a release — the ref still lives in the
       slot. *)
    let released =
      match current with
      | Some (Live { value = Element old_id; _ }) -> (
          match op with
          | Set { value = Element new_id; _ } when Element_id.equal old_id new_id -> []
          | _ -> [ old_id ])
      | _ -> []
    in
    ({ m with data = StringMap.add key new_slot m.data }, released)

let get (m : t) (k : string) : Value.t option =
  match StringMap.find_opt k m.data with Some (Live { value; _ }) -> Some value | _ -> None

let keys (m : t) : string list =
  StringMap.bindings m.data
  |> List.filter_map (fun (k, slot) -> match slot with Live _ -> Some k | Tomb _ -> None)

let entries (m : t) : (string * Value.t) list =
  StringMap.bindings m.data
  |> List.filter_map (fun (k, slot) ->
         match slot with Live { value; _ } -> Some (k, value) | Tomb _ -> None)

let cardinal (m : t) : int =
  StringMap.fold (fun _ slot n -> match slot with Live _ -> n + 1 | Tomb _ -> n) m.data 0

let equal_slot (a : slot) (b : slot) : bool =
  match (a, b) with
  | Live l1, Live l2 ->
      Value.equal l1.value l2.value
      && Lamport.equal l1.lamport l2.lamport
      && Op_id.equal l1.op_id l2.op_id
  | Tomb t1, Tomb t2 -> Lamport.equal t1.lamport t2.lamport && Op_id.equal t1.op_id t2.op_id
  | _ -> false

let equal (a : t) (b : t) : bool =
  Element_id.equal a.element_id b.element_id && StringMap.equal equal_slot a.data b.data

let pp (fmt : Format.formatter) (m : t) : unit =
  Format.fprintf fmt "Map(element_id=%a, entries=[%a])" Element_id.pp m.element_id
    (Format.pp_print_list
       ~pp_sep:(fun fmt () -> Format.fprintf fmt "; ")
       (fun fmt (k, v) -> Format.fprintf fmt "%S: %a" k Value.pp v))
    (entries m)
