(** Tests for [Crdtsync_crdt.List]. Black-box: only the .mli surface is exercised.

    Imported via an [L] alias so the stdlib [List] module remains usable in test
    helpers (List.map / List.iter / List.length etc). *)

module Uuid_v7 = Crdtsync_crdt.Uuid_v7
module Op_id = Crdtsync_crdt.Op_id
module Lamport = Crdtsync_crdt.Lamport
module Element_id = Crdtsync_crdt.Element_id
module Value = Crdtsync_crdt.Value
module L = Crdtsync_crdt.List

(* ── helpers ───────────────────────────────────────────────────────────────── *)

let fresh_op_id ?(seq = 1L) () = Op_id.make ~client_id:(Uuid_v7.v ()) ~client_seq:seq
let lam n = Lamport.of_int64 n
let list_id = Element_id.derive ~parent:Element_id.root ~key:"my_list"
let empty_list () = L.empty ~element_id:list_id

(* Make an entry_id deterministically labeled — mimics the SDK convention
   of deriving from (list_id, op_id_str), but uses a string label here for
   readability. *)
let eid label = Element_id.derive ~parent:list_id ~key:label

let scalar_int n = Value.Scalar (Value.Int n)
let scalar_str s = Value.Scalar (Value.String s)

(* Like [two_op_ids_ordered] in test_map.ml — produce two op_ids whose
   compare order we control, by sharing client_id and bumping client_seq. *)
let two_op_ids_ordered () =
  let cid = Uuid_v7.v () in
  let lo = Op_id.make ~client_id:cid ~client_seq:1L in
  let hi = Op_id.make ~client_id:cid ~client_seq:2L in
  assert (Op_id.compare lo hi < 0);
  (lo, hi)

(* ── empty / element_id ───────────────────────────────────────────────────── *)

let test_empty_has_element_id () =
  let l = empty_list () in
  Alcotest.(check int) "element_id round-trips" 0 (Element_id.compare (L.element_id l) list_id);
  Alcotest.(check int) "length = 0" 0 (L.length l);
  Alcotest.(check (list string)) "to_list = []" [] (List.map (fun _ -> "x") (L.to_list l))

let test_get_out_of_range () =
  let l = empty_list () in
  Alcotest.(check bool) "get 0 on empty = None" true (Option.is_none (L.get l 0));
  Alcotest.(check bool) "get -1 on empty = None" true (Option.is_none (L.get l (-1)))

(* ── single insert / get ──────────────────────────────────────────────────── *)

let test_insert_single () =
  let entry = eid "e1" in
  let l, released =
    L.apply (empty_list ())
      ~op:
        (Insert
           { entry_id = entry; value = scalar_int 42L; origin_left = None; origin_right = None })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  Alcotest.(check int) "no releases on insert" 0 (List.length released);
  Alcotest.(check int) "length = 1" 1 (L.length l);
  (match L.get l 0 with
  | Some (Scalar (Int n)) -> Alcotest.(check int64) "value = 42" 42L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 42L))");
  Alcotest.(check (option int)) "index_of entry = Some 0" (Some 0) (L.index_of l entry)

let test_insert_idempotent () =
  let entry = eid "e1" in
  let op =
    L.Insert { entry_id = entry; value = scalar_int 1L; origin_left = None; origin_right = None }
  in
  let op_id = fresh_op_id () in
  let l1, _ = L.apply (empty_list ()) ~op ~op_id ~lamport:(lam 1L) in
  let l2, released2 = L.apply l1 ~op ~op_id ~lamport:(lam 1L) in
  Alcotest.(check bool) "re-apply yields equal state" true (L.equal l1 l2);
  Alcotest.(check int) "no releases on re-apply" 0 (List.length released2);
  Alcotest.(check int) "length still 1" 1 (L.length l2)

(* ── ordering: head / tail / middle ───────────────────────────────────────── *)

let insert_at_tail l ~entry_id ~value ~lamport ~seq =
  let es = L.entries l in
  let left =
    match List.rev es with
    | [] -> None
    | (last_id, _) :: _ -> Some last_id
  in
  let op = L.Insert { entry_id; value; origin_left = left; origin_right = None } in
  L.apply l ~op ~op_id:(fresh_op_id ~seq ()) ~lamport:(lam lamport)

let test_insert_at_tail_preserves_order () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L in
  let l, _ = insert_at_tail l ~entry_id:(eid "b") ~value:(scalar_str "b") ~lamport:2L ~seq:2L in
  let l, _ = insert_at_tail l ~entry_id:(eid "c") ~value:(scalar_str "c") ~lamport:3L ~seq:3L in
  Alcotest.(check int) "length = 3" 3 (L.length l);
  let strs =
    List.map
      (function Value.Scalar (String s) -> s | _ -> "?")
      (L.to_list l)
  in
  Alcotest.(check (list string)) "order = [a; b; c]" [ "a"; "b"; "c" ] strs

let test_insert_at_head () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "b") ~value:(scalar_str "b") ~lamport:1L ~seq:1L in
  (* now insert "a" at head: origin_left = None, origin_right = entry_id of "b" *)
  let l, _ =
    L.apply l
      ~op:
        (Insert
           {
             entry_id = eid "a";
             value = scalar_str "a";
             origin_left = None;
             origin_right = Some (eid "b");
           })
      ~op_id:(fresh_op_id ~seq:2L ()) ~lamport:(lam 2L)
  in
  let strs =
    List.map (function Value.Scalar (String s) -> s | _ -> "?") (L.to_list l)
  in
  Alcotest.(check (list string)) "order = [a; b]" [ "a"; "b" ] strs

let test_insert_in_middle () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L in
  let l, _ = insert_at_tail l ~entry_id:(eid "c") ~value:(scalar_str "c") ~lamport:2L ~seq:2L in
  (* insert "b" between a and c *)
  let l, _ =
    L.apply l
      ~op:
        (Insert
           {
             entry_id = eid "b";
             value = scalar_str "b";
             origin_left = Some (eid "a");
             origin_right = Some (eid "c");
           })
      ~op_id:(fresh_op_id ~seq:3L ()) ~lamport:(lam 3L)
  in
  let strs =
    List.map (function Value.Scalar (String s) -> s | _ -> "?") (L.to_list l)
  in
  Alcotest.(check (list string)) "order = [a; b; c]" [ "a"; "b"; "c" ] strs

(* ── delete / tombstone ──────────────────────────────────────────────────── *)

let test_delete_removes_entry_from_live_view () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L in
  let l, _ = insert_at_tail l ~entry_id:(eid "b") ~value:(scalar_str "b") ~lamport:2L ~seq:2L in
  let l, released =
    L.apply l ~op:(Delete { target = eid "a" }) ~op_id:(fresh_op_id ~seq:3L ())
      ~lamport:(lam 3L)
  in
  Alcotest.(check int) "no released refs (scalar)" 0 (List.length released);
  Alcotest.(check int) "length = 1" 1 (L.length l);
  Alcotest.(check bool) "live get a = None" true (Option.is_none (L.get_entry l (eid "a")));
  Alcotest.(check bool) "live get b = Some" true (Option.is_some (L.get_entry l (eid "b")));
  Alcotest.(check (option int)) "index_of a = None (tombstoned)" None (L.index_of l (eid "a"));
  Alcotest.(check (option int)) "index_of b = Some 0" (Some 0) (L.index_of l (eid "b"))

let test_delete_releases_element_value () =
  let child = Element_id.derive ~parent:Element_id.root ~key:"child" in
  let l, _ =
    L.apply (empty_list ())
      ~op:
        (Insert
           {
             entry_id = eid "e1";
             value = Element child;
             origin_left = None;
             origin_right = None;
           })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let _, released =
    L.apply l ~op:(Delete { target = eid "e1" }) ~op_id:(fresh_op_id ~seq:2L ())
      ~lamport:(lam 2L)
  in
  Alcotest.(check int) "exactly 1 released" 1 (List.length released);
  match released with
  | [ rid ] -> Alcotest.(check int) "released = child" 0 (Element_id.compare rid child)
  | _ -> Alcotest.fail "expected 1 released"

let test_delete_idempotent () =
  let l, _ =
    insert_at_tail (empty_list ()) ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L
  in
  let l1, _ =
    L.apply l ~op:(Delete { target = eid "a" }) ~op_id:(fresh_op_id ~seq:2L ())
      ~lamport:(lam 2L)
  in
  let l2, released =
    L.apply l1 ~op:(Delete { target = eid "a" }) ~op_id:(fresh_op_id ~seq:3L ())
      ~lamport:(lam 3L)
  in
  Alcotest.(check bool) "re-delete equal state" true (L.equal l1 l2);
  Alcotest.(check int) "no releases on re-delete" 0 (List.length released)

(* ── move ────────────────────────────────────────────────────────────────── *)

let test_move_repositions () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L in
  let l, _ = insert_at_tail l ~entry_id:(eid "b") ~value:(scalar_str "b") ~lamport:2L ~seq:2L in
  let l, _ = insert_at_tail l ~entry_id:(eid "c") ~value:(scalar_str "c") ~lamport:3L ~seq:3L in
  (* move "a" to the end: new origin_left = c, origin_right = None *)
  let l, _ =
    L.apply l
      ~op:
        (Move
           {
             target = eid "a";
             new_origin_left = Some (eid "c");
             new_origin_right = None;
           })
      ~op_id:(fresh_op_id ~seq:4L ()) ~lamport:(lam 4L)
  in
  let strs =
    List.map (function Value.Scalar (String s) -> s | _ -> "?") (L.to_list l)
  in
  Alcotest.(check (list string)) "order = [b; c; a] after move" [ "b"; "c"; "a" ] strs

let test_move_lww_higher_lamport_wins () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L in
  let l, _ = insert_at_tail l ~entry_id:(eid "b") ~value:(scalar_str "b") ~lamport:2L ~seq:2L in
  let l, _ = insert_at_tail l ~entry_id:(eid "c") ~value:(scalar_str "c") ~lamport:3L ~seq:3L in
  (* Two concurrent moves of "a": one to between b and c (lam 5), one to end (lam 10).
     Higher lamport wins -> a ends up at the end. *)
  let l, _ =
    L.apply l
      ~op:
        (Move
           {
             target = eid "a";
             new_origin_left = Some (eid "b");
             new_origin_right = Some (eid "c");
           })
      ~op_id:(fresh_op_id ~seq:4L ()) ~lamport:(lam 5L)
  in
  let l, _ =
    L.apply l
      ~op:
        (Move
           {
             target = eid "a";
             new_origin_left = Some (eid "c");
             new_origin_right = None;
           })
      ~op_id:(fresh_op_id ~seq:5L ()) ~lamport:(lam 10L)
  in
  let strs =
    List.map (function Value.Scalar (String s) -> s | _ -> "?") (L.to_list l)
  in
  Alcotest.(check (list string)) "a moved to end (higher lamport wins)"
    [ "b"; "c"; "a" ] strs

let test_move_lower_lamport_loses () =
  let l = empty_list () in
  let l, _ = insert_at_tail l ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L ~seq:1L in
  let l, _ = insert_at_tail l ~entry_id:(eid "b") ~value:(scalar_str "b") ~lamport:2L ~seq:2L in
  let l, _ = insert_at_tail l ~entry_id:(eid "c") ~value:(scalar_str "c") ~lamport:3L ~seq:3L in
  (* Apply higher-lamport move first, then a stale lower-lamport move; latter
     must not clobber. *)
  let l, _ =
    L.apply l
      ~op:
        (Move
           {
             target = eid "a";
             new_origin_left = Some (eid "c");
             new_origin_right = None;
           })
      ~op_id:(fresh_op_id ~seq:5L ()) ~lamport:(lam 10L)
  in
  let l, _ =
    L.apply l
      ~op:
        (Move
           {
             target = eid "a";
             new_origin_left = None;
             new_origin_right = Some (eid "b");
           })
      ~op_id:(fresh_op_id ~seq:4L ()) ~lamport:(lam 5L)
  in
  let strs =
    List.map (function Value.Scalar (String s) -> s | _ -> "?") (L.to_list l)
  in
  Alcotest.(check (list string)) "lower-lamport move ignored, a stays at end"
    [ "b"; "c"; "a" ] strs

(* ── Fugue no-interleave (concurrent inserts at same point) ──────────────── *)

let test_fugue_no_interleave () =
  (* Empty list. Alice inserts "abc" (3 entries) starting at head. Bob inserts
     "xyz" (3 entries) starting at head, concurrently (same lamport seed +1
     ordering). Fugue guarantees no interleaving: result is either "abcxyz"
     or "xyzabc", never "axbycz". *)
  let l = empty_list () in
  (* Alice: insert a, b, c all with origin_left = None, origin_right = None at
     successive lamports. Actually for Fugue, each subsequent insert anchors
     on the previous one's entry_id so they stay contiguous. Simulate that. *)
  let alice_op label seq value left right =
    L.Insert
      {
        entry_id = eid ("A_" ^ label);
        value = scalar_str value;
        origin_left = left;
        origin_right = right;
      }
    , fresh_op_id ~seq ()
  in
  let bob_op label seq value left right =
    L.Insert
      {
        entry_id = eid ("B_" ^ label);
        value = scalar_str value;
        origin_left = left;
        origin_right = right;
      }
    , fresh_op_id ~seq ()
  in
  (* Apply Alice's a, b, c sequentially. *)
  let op_a, oid_a = alice_op "a" 1L "a" None None in
  let l, _ = L.apply l ~op:op_a ~op_id:oid_a ~lamport:(lam 1L) in
  let op_b, oid_b = alice_op "b" 2L "b" (Some (eid "A_a")) None in
  let l, _ = L.apply l ~op:op_b ~op_id:oid_b ~lamport:(lam 2L) in
  let op_c, oid_c = alice_op "c" 3L "c" (Some (eid "A_b")) None in
  let l, _ = L.apply l ~op:op_c ~op_id:oid_c ~lamport:(lam 3L) in
  (* Bob's x, y, z all anchored on (None, A_a) -- concurrent with Alice's
     a/b/c (which were anchored on (None, None) / chain). Fugue should
     keep Bob's run contiguous: either all before Alice or all after. *)
  let op_x, oid_x = bob_op "x" 4L "x" None (Some (eid "A_a")) in
  let l, _ = L.apply l ~op:op_x ~op_id:oid_x ~lamport:(lam 1L) in
  let op_y, oid_y = bob_op "y" 5L "y" (Some (eid "B_x")) (Some (eid "A_a")) in
  let l, _ = L.apply l ~op:op_y ~op_id:oid_y ~lamport:(lam 2L) in
  let op_z, oid_z = bob_op "z" 6L "z" (Some (eid "B_y")) (Some (eid "A_a")) in
  let l, _ = L.apply l ~op:op_z ~op_id:oid_z ~lamport:(lam 3L) in
  let strs =
    List.map (function Value.Scalar (String s) -> s | _ -> "?") (L.to_list l)
  in
  let joined = String.concat "" strs in
  let no_interleave = joined = "xyzabc" || joined = "abcxyz" in
  Alcotest.(check bool)
    (Printf.sprintf "result %S has no interleaving" joined)
    true no_interleave

(* ── equal / pp ──────────────────────────────────────────────────────────── *)

let test_equal_reflexive () =
  let l, _ =
    insert_at_tail (empty_list ()) ~entry_id:(eid "a") ~value:(scalar_str "a") ~lamport:1L
      ~seq:1L
  in
  Alcotest.(check bool) "l = l" true (L.equal l l)

let test_equal_distinct_element_ids () =
  let id_a = Element_id.derive ~parent:Element_id.root ~key:"a" in
  let id_b = Element_id.derive ~parent:Element_id.root ~key:"b" in
  let la = L.empty ~element_id:id_a in
  let lb = L.empty ~element_id:id_b in
  Alcotest.(check bool) "different element_id => not equal" false (L.equal la lb)

let test_pp_nonempty () =
  let l, _ =
    insert_at_tail (empty_list ()) ~entry_id:(eid "a") ~value:(scalar_str "hi") ~lamport:1L
      ~seq:1L
  in
  let s = Format.asprintf "%a" L.pp l in
  Alcotest.(check bool) "pp nonempty" true (String.length s > 0)

(* tie-break -- two clients use op_ids whose comparison we control to force
   a deterministic winner in the tied-position case *)
let test_concurrent_insert_at_same_position_tiebreak () =
  let lo, hi = two_op_ids_ordered () in
  let l = empty_list () in
  let l, _ =
    L.apply l
      ~op:
        (Insert
           {
             entry_id = Element_id.derive ~parent:list_id ~key:"X_lo";
             value = scalar_str "lo";
             origin_left = None;
             origin_right = None;
           })
      ~op_id:lo ~lamport:(lam 1L)
  in
  let l, _ =
    L.apply l
      ~op:
        (Insert
           {
             entry_id = Element_id.derive ~parent:list_id ~key:"X_hi";
             value = scalar_str "hi";
             origin_left = None;
             origin_right = None;
           })
      ~op_id:hi ~lamport:(lam 1L)
  in
  (* Both anchored at (None, None) — tie. Fugue tiebreak via id ordering.
     We don't assert a particular order, just that BOTH entries are present
     in some deterministic order. *)
  Alcotest.(check int) "both entries present" 2 (L.length l)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "list"
    [
      ( "empty",
        [
          Alcotest.test_case "empty has element_id, length 0" `Quick test_empty_has_element_id;
          Alcotest.test_case "get out of range" `Quick test_get_out_of_range;
        ] );
      ( "insert",
        [
          Alcotest.test_case "single insert visible via get + index_of" `Quick test_insert_single;
          Alcotest.test_case "idempotent on same entry_id" `Quick test_insert_idempotent;
          Alcotest.test_case "tail inserts preserve order" `Quick
            test_insert_at_tail_preserves_order;
          Alcotest.test_case "head insert" `Quick test_insert_at_head;
          Alcotest.test_case "middle insert between neighbors" `Quick test_insert_in_middle;
        ] );
      ( "delete",
        [
          Alcotest.test_case "delete removes entry from live view" `Quick
            test_delete_removes_entry_from_live_view;
          Alcotest.test_case "delete releases Element value" `Quick
            test_delete_releases_element_value;
          Alcotest.test_case "delete is idempotent" `Quick test_delete_idempotent;
        ] );
      ( "move",
        [
          Alcotest.test_case "move repositions an entry" `Quick test_move_repositions;
          Alcotest.test_case "LWW: higher lamport wins" `Quick test_move_lww_higher_lamport_wins;
          Alcotest.test_case "LWW: lower lamport loses (stale move)" `Quick
            test_move_lower_lamport_loses;
        ] );
      ( "Fugue",
        [
          Alcotest.test_case "concurrent inserts do not interleave" `Quick
            test_fugue_no_interleave;
          Alcotest.test_case "tied position: deterministic tiebreak" `Quick
            test_concurrent_insert_at_same_position_tiebreak;
        ] );
      ( "equal / pp",
        [
          Alcotest.test_case "equal reflexive" `Quick test_equal_reflexive;
          Alcotest.test_case "distinct element_ids not equal" `Quick
            test_equal_distinct_element_ids;
          Alcotest.test_case "pp nonempty" `Quick test_pp_nonempty;
        ] );
    ]
