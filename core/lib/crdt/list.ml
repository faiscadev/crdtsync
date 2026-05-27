(** List CRDT primitive. CORE-3 — see KANBAN.md. *)

type node = {
  entry_id : Element_id.t;
  lamport : Lamport.t;
  op_id : Op_id.t;
  value : Value.t;
  tombstoned : bool;
  left : node list;
  right : node list;
}

type t = { id : Element_id.t; root : node list }

type op =
  | Insert of {
      entry_id : Element_id.t;
      value : Value.t;
      origin_left : Element_id.t option;
      origin_right : Element_id.t option;
    }
  | Delete of { target : Element_id.t }
  | Move of {
      target : Element_id.t;
      new_origin_left : Element_id.t option;
      new_origin_right : Element_id.t option;
    }

let empty ~element_id : t = { id = element_id; root = [] }
let element_id (l : t) : Element_id.t = l.id

(* ── Fugue helpers ─────────────────────────────────────────────────────── *)

type side = Left | Right

(* Did a get inserted before b? Total order on (lamport, op_id). *)
let inserted_before (a : node) (b : node) : bool =
  let c = Lamport.compare a.lamport b.lamport in
  c < 0 || (c = 0 && Op_id.compare a.op_id b.op_id < 0)

(* Siblings under the same parent+side are sorted DESCENDING by
   (lamport, op_id) so in-order traversal visits higher-priority
   inserts closer to the parent. *)
let sibling_cmp (a : node) (b : node) : int =
  let c = Lamport.compare b.lamport a.lamport in
  if c <> 0 then c else Op_id.compare b.op_id a.op_id

let insert_sibling (siblings : node list) (n : node) : node list =
  let rec loop acc = function
    | [] -> Stdlib.List.rev_append acc [ n ]
    | h :: t when sibling_cmp n h <= 0 -> Stdlib.List.rev_append acc (n :: h :: t)
    | h :: t -> loop (h :: acc) t
  in
  loop [] siblings

(* Find a node by entry_id anywhere in the tree (or None if absent). *)
let rec find_node (nodes : node list) (id : Element_id.t) : node option =
  match nodes with
  | [] -> None
  | h :: t -> (
      if Element_id.equal h.entry_id id then Some h
      else
        match find_node h.left id with
        | Some _ as r -> r
        | None -> (
            match find_node h.right id with
            | Some _ as r -> r
            | None -> find_node t id))

(* Walk the tree, attach new_node as a child of the node identified by
   parent_id on the given side. parent_id = None means "root sentinel" —
   attach to the top-level l.root list (which IS the root sentinel's
   right-children list). *)
let rec add_child (nodes : node list) ~parent_id ~side (new_node : node) : node list =
  match parent_id with
  | None -> insert_sibling nodes new_node
  | Some pid ->
      Stdlib.List.map
        (fun (n : node) ->
          if Element_id.equal n.entry_id pid then
            match side with
            | Left -> { n with left = insert_sibling n.left new_node }
            | Right -> { n with right = insert_sibling n.right new_node }
          else
            {
              n with
              left = add_child n.left ~parent_id ~side new_node;
              right = add_child n.right ~parent_id ~side new_node;
            })
        nodes

(* Fugue rule: pick (parent_id, side) from the wire's (origin_left, origin_right). *)
let resolve_parent (l : t) ~origin_left ~origin_right : Element_id.t option * side =
  match origin_right with
  | None ->
      (* origin_right absent -> attach as right child of origin_left
         (None = root sentinel if there's no left either). *)
      (origin_left, Right)
  | Some r_id -> (
      match find_node l.root r_id with
      | None ->
          (* origin_right references an unknown node (bad op or
             out-of-order delivery). Fall back to right of origin_left. *)
          (origin_left, Right)
      | Some r_node ->
          let r_has_left = r_node.left <> [] in
          let r_inserted_before_l =
            match origin_left with
            | None -> false (* root sentinel predates everything *)
            | Some l_id -> (
                match find_node l.root l_id with
                | Some l_node -> inserted_before r_node l_node
                | None -> false)
          in
          if r_has_left || r_inserted_before_l then (origin_left, Right)
          else (origin_right, Left))

(* ── Insert ───────────────────────────────────────────────────────────── *)

let apply_insert (l : t) ~entry_id ~value ~origin_left ~origin_right
    ~(op_id : Op_id.t) ~(lamport : Lamport.t) : t * Element_id.t list =
  (* Idempotency: re-applying the same entry_id is a no-op. *)
  if Option.is_some (find_node l.root entry_id) then (l, [])
  else
    let new_node =
      { entry_id; lamport; op_id; value; tombstoned = false; left = []; right = [] }
    in
    let parent_id, side = resolve_parent l ~origin_left ~origin_right in
    ({ l with root = add_child l.root ~parent_id ~side new_node }, [])

let apply (l : t) ~op ~op_id ~lamport : t * Element_id.t list =
  match op with
  | Insert { entry_id; value; origin_left; origin_right } ->
      apply_insert l ~entry_id ~value ~origin_left ~origin_right ~op_id ~lamport
  | Delete _ -> (l, [])
  | Move _ -> (l, [])

let get (_ : t) (_ : int) : Value.t option = failwith "List.get: not implemented (CORE-3)"

let get_entry (_ : t) (_ : Element_id.t) : Value.t option =
  failwith "List.get_entry: not implemented (CORE-3)"

let index_of (_ : t) (_ : Element_id.t) : int option =
  failwith "List.index_of: not implemented (CORE-3)"

let rec node_entries (node : node list) : (Element_id.t * Value.t) list =
  match node with
  | head :: tail ->
      let left_entries = node_entries head.left in
      let right_entries = node_entries head.right in
      let tail_entries = node_entries tail in
      let result =
        Stdlib.List.concat
          [ left_entries; [ (head.entry_id, head.value) ]; right_entries; tail_entries ]
      in
      result
  | [] -> []

let entries (l : t) : (Element_id.t * Value.t) list = node_entries l.root
let length (l : t) : int = entries l |> Stdlib.List.length
let to_list (l : t) : Value.t list = entries l |> Stdlib.List.map (fun (_, v) -> v)

let fold (l : t) ~init ~f : 'a =
  let rec loop a (id, value) rest =
    let a = f a id value in
    match rest with head :: tail -> loop a head tail | [] -> a
  in
  let e = entries l in
  match e with head :: tail -> loop init head tail | [] -> init

let equal (_ : t) (_ : t) : bool = failwith "List.equal: not implemented (CORE-3)"

let pp (_ : Format.formatter) (_ : t) : unit =
  failwith "List.pp: not implemented (CORE-3)"
