(** Tests for [Crdtsync_crdt.Map]. Black-box: only the .mli surface is exercised. *)

module Uuid_v7 = Crdtsync_crdt.Uuid_v7
module Op_id = Crdtsync_crdt.Op_id
module Lamport = Crdtsync_crdt.Lamport
module Element_id = Crdtsync_crdt.Element_id
module Value = Crdtsync_crdt.Value
module Map = Crdtsync_crdt.Map

(* ── helpers ───────────────────────────────────────────────────────────────── *)

let fresh_op_id ?(seq = 1L) () = Op_id.make ~client_id:(Uuid_v7.v ()) ~client_seq:seq
let lam n = Lamport.of_int64 n

(* For deterministic op_id ordering in tests we need to construct two op_ids
   whose compare order we control. Easiest: use the SAME client_id with
   different client_seqs. *)
let two_op_ids_ordered () =
  let cid = Uuid_v7.v () in
  let lo = Op_id.make ~client_id:cid ~client_seq:1L in
  let hi = Op_id.make ~client_id:cid ~client_seq:2L in
  assert (Op_id.compare lo hi < 0);
  (lo, hi)

let empty_map () = Map.empty ~element_id:Element_id.root

(* ── empty / element_id ───────────────────────────────────────────────────── *)

let test_empty_has_element_id () =
  let id = Element_id.derive ~parent:Element_id.root ~key:"m" in
  let m = Map.empty ~element_id:id in
  Alcotest.(check int)
    "empty.element_id = id" 0
    (Element_id.compare (Map.element_id m) id);
  Alcotest.(check int) "empty cardinal = 0" 0 (Map.cardinal m);
  Alcotest.(check (list string)) "empty keys = []" [] (Map.keys m)

let test_get_missing_returns_none () =
  let m = empty_map () in
  Alcotest.(check bool) "get on empty = None" true (Option.is_none (Map.get m "x"))

(* ── set / get ────────────────────────────────────────────────────────────── *)

let test_set_scalar_visible_via_get () =
  let m, released =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 42L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  Alcotest.(check (list int)) "no releases" [] (List.map (fun _ -> 0) released);
  match Map.get m "x" with
  | Some (Scalar (Int n)) -> Alcotest.(check int64) "value = 42L" 42L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 42L))"

let test_set_overwrite_by_higher_lamport () =
  let op1 = Map.Set { key = "x"; value = Scalar (Int 1L) } in
  let op2 = Map.Set { key = "x"; value = Scalar (Int 2L) } in
  let m, _ =
    Map.apply (empty_map ()) ~op:op1 ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  let m, _ = Map.apply m ~op:op2 ~op_id:(fresh_op_id ()) ~lamport:(lam 20L) in
  match Map.get m "x" with
  | Some (Scalar (Int n)) -> Alcotest.(check int64) "winner = 2L (higher lamport)" 2L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 2L))"

let test_set_lower_lamport_loses () =
  (* Apply newer first, then older — older must NOT clobber. *)
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 2L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 20L)
  in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  match Map.get m "x" with
  | Some (Scalar (Int n)) -> Alcotest.(check int64) "winner stays 2L" 2L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 2L))"

let test_set_tie_breaks_by_op_id () =
  let lo, hi = two_op_ids_ordered () in
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:lo ~lamport:(lam 5L)
  in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "x"; value = Scalar (Int 2L) })
      ~op_id:hi ~lamport:(lam 5L)
  in
  (* hi > lo at equal lamport, so hi wins. *)
  match Map.get m "x" with
  | Some (Scalar (Int n)) ->
      Alcotest.(check int64) "tiebreak: higher op_id wins (2L)" 2L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 2L))"

let test_set_tie_loser_does_not_clobber () =
  let lo, hi = two_op_ids_ordered () in
  (* Apply hi first, then lo at same lamport. lo must NOT win. *)
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 2L) })
      ~op_id:hi ~lamport:(lam 5L)
  in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:lo ~lamport:(lam 5L)
  in
  match Map.get m "x" with
  | Some (Scalar (Int n)) -> Alcotest.(check int64) "hi keeps slot (2L)" 2L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 2L))"

(* ── delete ───────────────────────────────────────────────────────────────── *)

let test_delete_removes_key () =
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let m, _ =
    Map.apply m ~op:(Delete { key = "x" }) ~op_id:(fresh_op_id ()) ~lamport:(lam 2L)
  in
  Alcotest.(check bool) "get x = None after delete" true (Option.is_none (Map.get m "x"));
  Alcotest.(check int) "cardinal = 0" 0 (Map.cardinal m)

let test_delete_lower_lamport_loses () =
  (* Apply Set @lam=10 then Delete @lam=5 — Delete must not remove. *)
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  let m, _ =
    Map.apply m ~op:(Delete { key = "x" }) ~op_id:(fresh_op_id ()) ~lamport:(lam 5L)
  in
  Alcotest.(check bool) "x still present" true (Option.is_some (Map.get m "x"))

let test_set_after_delete_with_higher_lamport () =
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let m, _ =
    Map.apply m ~op:(Delete { key = "x" }) ~op_id:(fresh_op_id ()) ~lamport:(lam 5L)
  in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "x"; value = Scalar (Int 2L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  match Map.get m "x" with
  | Some (Scalar (Int n)) -> Alcotest.(check int64) "x = 2L after re-set" 2L n
  | _ -> Alcotest.fail "expected Some (Scalar (Int 2L))"

(* ── released_refs ────────────────────────────────────────────────────────── *)

let test_set_displaces_element_returns_released () =
  let child = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "body"; value = Element child })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let m, released =
    Map.apply m
      ~op:(Set { key = "body"; value = Scalar (Int 0L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 2L)
  in
  Alcotest.(check int) "released has 1 element" 1 (List.length released);
  (match released with
  | [ rid ] -> Alcotest.(check int) "released id = child" 0 (Element_id.compare rid child)
  | _ -> Alcotest.fail "expected exactly 1 released id");
  Alcotest.(check bool)
    "slot now holds scalar" true
    (match Map.get m "body" with Some (Scalar (Int 0L)) -> true | _ -> false)

let test_delete_displaces_element_returns_released () =
  let child = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "body"; value = Element child })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let m, released =
    Map.apply m ~op:(Delete { key = "body" }) ~op_id:(fresh_op_id ()) ~lamport:(lam 2L)
  in
  Alcotest.(check int) "released has 1 element" 1 (List.length released);
  (match released with
  | [ rid ] -> Alcotest.(check int) "released id = child" 0 (Element_id.compare rid child)
  | _ -> Alcotest.fail "expected exactly 1 released id");
  Alcotest.(check bool) "slot removed" true (Option.is_none (Map.get m "body"))

let test_set_replacing_scalar_no_release () =
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let _, released =
    Map.apply m
      ~op:(Set { key = "x"; value = Scalar (Int 2L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 2L)
  in
  Alcotest.(check (list int))
    "no releases when replacing Scalar" []
    (List.map (fun _ -> 0) released)

let test_losing_set_scalar_releases_nothing () =
  (* Set @lam=10 with Element, then Set @lam=5 with Scalar — loser carries
     no Element id, so nothing is stranded and the winning slot Element
     stays put. *)
  let child = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "body"; value = Element child })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  let _, released =
    Map.apply m
      ~op:(Set { key = "body"; value = Scalar Null })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 5L)
  in
  Alcotest.(check (list int))
    "losing scalar set releases nothing" []
    (List.map (fun _ -> 0) released)

let test_losing_set_element_releases_stranded_id () =
  (* Set @lam=10 with Element child_a (winner), then Set @lam=5 with Element
     child_b. The loser carries a unique Element id (child_b) that never made
     it into any slot. Its id is stranded and must be reported as released
     so the doc layer can treat it as a candidate orphan. *)
  let child_a = Element_id.derive ~parent:Element_id.root ~key:"a" in
  let child_b = Element_id.derive ~parent:Element_id.root ~key:"b" in
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "slot"; value = Element child_a })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  let m', released =
    Map.apply m
      ~op:(Set { key = "slot"; value = Element child_b })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 5L)
  in
  Alcotest.(check int) "released has 1 stranded id" 1 (List.length released);
  (match released with
  | [ id ] ->
      Alcotest.(check int)
        "stranded id = child_b (the loser)" 0
        (Element_id.compare id child_b)
  | _ -> Alcotest.fail "expected exactly 1 stranded id");
  Alcotest.(check bool)
    "slot still holds child_a (winner kept)" true
    (match Map.get m' "slot" with
    | Some (Element id) -> Element_id.compare id child_a = 0
    | _ -> false)

let test_losing_set_same_element_id_releases_nothing () =
  (* Deterministic-SDK case: two clients both compute the same derived id
     and both emit Set with Element(derived). One wins LWW. The loser
     carries the SAME id as the winner, so it's not stranded — the id IS
     in the slot, just via the winning op. *)
  let derived = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "body"; value = Element derived })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 10L)
  in
  let _, released =
    Map.apply m
      ~op:(Set { key = "body"; value = Element derived })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 5L)
  in
  Alcotest.(check (list int))
    "deterministic loser releases nothing" []
    (List.map (fun _ -> 0) released)

(* ── idempotency ──────────────────────────────────────────────────────────── *)

let test_apply_same_op_twice_idempotent () =
  let op_id = fresh_op_id () in
  let m1, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id ~lamport:(lam 1L)
  in
  let m2, released2 =
    Map.apply m1 ~op:(Set { key = "x"; value = Scalar (Int 1L) }) ~op_id ~lamport:(lam 1L)
  in
  Alcotest.(check bool) "state unchanged on re-apply" true (Map.equal m1 m2);
  Alcotest.(check (list int))
    "no release on re-apply" []
    (List.map (fun _ -> 0) released2)

(* ── initOnce-by-SDK convention: two clients deriving same id converge ─────── *)

let test_concurrent_deterministic_set_converges () =
  (* Simulate two clients each running the SDK's "Map.text 'body'" helper:
     both derive the same Element_id, both emit Set with the same value.
     LWW picks one wire op as the winner, but the value is identical, so
     no orphan. *)
  let derived = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let m_a, rel_a =
    Map.apply (empty_map ())
      ~op:(Set { key = "body"; value = Element derived })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let m_b, rel_b =
    Map.apply m_a
      ~op:(Set { key = "body"; value = Element derived })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 2L)
  in
  (* Both ops carried the same Element id; second apply shouldn't release
     the SAME element_id as orphan (it's still in the slot, just under a
     different wire op_id). *)
  Alcotest.(check (list int))
    "no orphan from concurrent deterministic set" []
    (List.map (fun _ -> 0) rel_b);
  Alcotest.(check int) "first apply also no orphan" 0 (List.length rel_a);
  Alcotest.(check bool)
    "slot holds derived element" true
    (match Map.get m_b "body" with
    | Some (Element id) -> Element_id.compare id derived = 0
    | _ -> false)

(* ── keys / entries / cardinal ───────────────────────────────────────────── *)

let test_keys_entries_sorted () =
  let m = empty_map () in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "bbb"; value = Scalar (Int 2L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "aaa"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 2L)
  in
  let m, _ =
    Map.apply m
      ~op:(Set { key = "ccc"; value = Scalar (Int 3L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 3L)
  in
  Alcotest.(check (list string)) "keys sorted ASC" [ "aaa"; "bbb"; "ccc" ] (Map.keys m);
  Alcotest.(check int) "cardinal = 3" 3 (Map.cardinal m);
  Alcotest.(check (list string))
    "entries sorted by key" [ "aaa"; "bbb"; "ccc" ]
    (List.map fst (Map.entries m))

(* ── equal / pp ──────────────────────────────────────────────────────────── *)

let test_equal_reflexive () =
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (Int 1L) })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  Alcotest.(check bool) "m = m" true (Map.equal m m)

let test_equal_different_element_ids () =
  let id_a = Element_id.derive ~parent:Element_id.root ~key:"a" in
  let id_b = Element_id.derive ~parent:Element_id.root ~key:"b" in
  let m_a = Map.empty ~element_id:id_a in
  let m_b = Map.empty ~element_id:id_b in
  Alcotest.(check bool) "different element_ids => not equal" false (Map.equal m_a m_b)

let test_pp_nonempty () =
  let m, _ =
    Map.apply (empty_map ())
      ~op:(Set { key = "x"; value = Scalar (String "hello") })
      ~op_id:(fresh_op_id ()) ~lamport:(lam 1L)
  in
  let s = Format.asprintf "%a" Map.pp m in
  Alcotest.(check bool) "pp nonempty" true (String.length s > 0)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "map"
    [
      ( "empty",
        [
          Alcotest.test_case "carries element_id, cardinal 0" `Quick
            test_empty_has_element_id;
          Alcotest.test_case "get missing key returns None" `Quick
            test_get_missing_returns_none;
        ] );
      ( "set / LWW",
        [
          Alcotest.test_case "set scalar then get" `Quick test_set_scalar_visible_via_get;
          Alcotest.test_case "higher lamport wins" `Quick
            test_set_overwrite_by_higher_lamport;
          Alcotest.test_case "lower lamport loses" `Quick test_set_lower_lamport_loses;
          Alcotest.test_case "tie broken by higher op_id" `Quick
            test_set_tie_breaks_by_op_id;
          Alcotest.test_case "tie loser does not clobber" `Quick
            test_set_tie_loser_does_not_clobber;
        ] );
      ( "delete",
        [
          Alcotest.test_case "delete removes" `Quick test_delete_removes_key;
          Alcotest.test_case "delete with lower lamport loses" `Quick
            test_delete_lower_lamport_loses;
          Alcotest.test_case "set after delete with higher lamport wins" `Quick
            test_set_after_delete_with_higher_lamport;
        ] );
      ( "released_refs",
        [
          Alcotest.test_case "set displacing element releases its id" `Quick
            test_set_displaces_element_returns_released;
          Alcotest.test_case "delete displacing element releases its id" `Quick
            test_delete_displaces_element_returns_released;
          Alcotest.test_case "replacing scalar releases nothing" `Quick
            test_set_replacing_scalar_no_release;
          Alcotest.test_case "losing Set with Scalar releases nothing" `Quick
            test_losing_set_scalar_releases_nothing;
          Alcotest.test_case "losing Set with unique Element releases the stranded id"
            `Quick test_losing_set_element_releases_stranded_id;
          Alcotest.test_case "losing Set with same Element id releases nothing" `Quick
            test_losing_set_same_element_id_releases_nothing;
        ] );
      ( "idempotency",
        [
          Alcotest.test_case "applying same op twice is a no-op" `Quick
            test_apply_same_op_twice_idempotent;
          Alcotest.test_case "concurrent deterministic set converges (no orphan)" `Quick
            test_concurrent_deterministic_set_converges;
        ] );
      ( "introspection",
        [
          Alcotest.test_case "keys / entries / cardinal sorted" `Quick
            test_keys_entries_sorted;
        ] );
      ( "equal / pp",
        [
          Alcotest.test_case "equal reflexive" `Quick test_equal_reflexive;
          Alcotest.test_case "different element_ids not equal" `Quick
            test_equal_different_element_ids;
          Alcotest.test_case "pp nonempty" `Quick test_pp_nonempty;
        ] );
    ]
