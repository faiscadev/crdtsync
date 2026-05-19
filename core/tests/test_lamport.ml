(** Tests for [Crdtsync_crdt.Lamport]. Black-box: only the .mli surface is exercised. *)

module Lamport = Crdtsync_crdt.Lamport

(* ── alcotest unit tests ──────────────────────────────────────────────────── *)

let test_zero_round_trips () =
  Alcotest.(check int64) "to_int64 zero = 0L" 0L (Lamport.to_int64 Lamport.zero)

let test_zero_reflexive () =
  Alcotest.(check int) "compare zero zero = 0" 0 (Lamport.compare Lamport.zero Lamport.zero)

let test_tick_adds_one () =
  let one = Lamport.tick Lamport.zero in
  let two = Lamport.tick one in
  Alcotest.(check int64) "tick zero = 1L" 1L (Lamport.to_int64 one);
  Alcotest.(check int64) "tick (tick zero) = 2L" 2L (Lamport.to_int64 two)

let test_tick_strictly_increases () =
  let a = Lamport.of_int64 42L in
  let b = Lamport.tick a in
  Alcotest.(check bool) "tick a > a" true (Lamport.compare b a > 0)

let test_merge_max_plus_one_recv_larger () =
  let local = Lamport.of_int64 5L in
  let recv = Lamport.of_int64 10L in
  let merged = Lamport.merge ~recv ~local in
  Alcotest.(check int64) "max(10, 5) + 1 = 11" 11L (Lamport.to_int64 merged)

let test_merge_max_plus_one_local_larger () =
  let local = Lamport.of_int64 100L in
  let recv = Lamport.of_int64 50L in
  let merged = Lamport.merge ~recv ~local in
  Alcotest.(check int64) "max(50, 100) + 1 = 101" 101L (Lamport.to_int64 merged)

let test_merge_equal () =
  let v = Lamport.of_int64 7L in
  let merged = Lamport.merge ~recv:v ~local:v in
  Alcotest.(check int64) "max(7, 7) + 1 = 8" 8L (Lamport.to_int64 merged)

let test_merge_dominates_inputs () =
  let local = Lamport.of_int64 3L in
  let recv = Lamport.of_int64 9L in
  let merged = Lamport.merge ~recv ~local in
  Alcotest.(check bool) "merged > recv" true (Lamport.compare merged recv > 0);
  Alcotest.(check bool) "merged > local" true (Lamport.compare merged local > 0)

let test_equal_matches_compare () =
  let a = Lamport.of_int64 17L in
  let b = Lamport.of_int64 17L in
  let c = Lamport.of_int64 18L in
  Alcotest.(check bool) "equal a b when compare = 0" true (Lamport.equal a b);
  Alcotest.(check bool) "not equal a c when compare <> 0" false (Lamport.equal a c)

let test_pp_decimal () =
  let zero = Lamport.zero in
  let small = Lamport.of_int64 42L in
  let big = Lamport.of_int64 Int64.max_int in
  Alcotest.(check string) "pp 0 = \"0\"" "0" (Format.asprintf "%a" Lamport.pp zero);
  Alcotest.(check string) "pp 42 = \"42\"" "42" (Format.asprintf "%a" Lamport.pp small);
  Alcotest.(check string)
    "pp Int64.max_int" (Int64.to_string Int64.max_int)
    (Format.asprintf "%a" Lamport.pp big)

let test_of_int64_rejects_negative () =
  let is_failure = function Failure _ -> true | _ -> false in
  Alcotest.check_raises "of_int64 -1L raises Failure" (Failure "ignored") (fun () ->
      try
        let _ = Lamport.of_int64 (-1L) in
        ()
      with e -> if is_failure e then raise (Failure "ignored") else raise e);
  Alcotest.check_raises "of_int64 Int64.min_int raises Failure" (Failure "ignored") (fun () ->
      try
        let _ = Lamport.of_int64 Int64.min_int in
        ()
      with e -> if is_failure e then raise (Failure "ignored") else raise e)

let test_round_trip_int64_nonneg () =
  let cases = [ 0L; 1L; 42L; 1_000_000L; Int64.max_int ] in
  List.iter
    (fun n ->
      let r = Lamport.to_int64 (Lamport.of_int64 n) in
      Alcotest.(check int64) (Printf.sprintf "round-trip %Ld" n) n r)
    cases

(* ── qcheck property tests ────────────────────────────────────────────────── *)

(* Generate nonneg int64 in [0, Int64.max_int] by clearing the sign bit. *)
let nonneg_int64 : int64 QCheck.arbitrary =
  QCheck.map ~rev:(fun n -> n) (fun n -> Int64.logand n Int64.max_int) QCheck.int64

let arb_lamport : Lamport.t QCheck.arbitrary =
  QCheck.map ~rev:Lamport.to_int64 Lamport.of_int64 nonneg_int64

let prop_round_trip_int64_nonneg =
  QCheck.Test.make ~count:500 ~name:"of_int64 nonneg / to_int64 round-trip" nonneg_int64 (fun n ->
      Int64.equal n (Lamport.to_int64 (Lamport.of_int64 n)))

let prop_of_int64_raises_on_negative =
  QCheck.Test.make ~count:200 ~name:"of_int64 raises Failure on any negative input" QCheck.int64
    (fun n ->
      if n < 0L then
        try
          let _ = Lamport.of_int64 n in
          false
        with Failure _ -> true
      else true)

let prop_compare_reflexive =
  QCheck.Test.make ~count:200 ~name:"compare l l = 0" arb_lamport (fun l -> Lamport.compare l l = 0)

let prop_compare_antisymmetric =
  QCheck.Test.make ~count:200 ~name:"compare antisymmetric" (QCheck.pair arb_lamport arb_lamport)
    (fun (a, b) ->
      let ab = Lamport.compare a b in
      let ba = Lamport.compare b a in
      (ab = 0 && ba = 0) || ab * ba < 0)

let prop_equal_iff_compare_zero =
  QCheck.Test.make ~count:200 ~name:"equal a b <=> compare a b = 0"
    (QCheck.pair arb_lamport arb_lamport) (fun (a, b) ->
      Bool.equal (Lamport.equal a b) (Lamport.compare a b = 0))

let prop_tick_strictly_increases =
  (* Skip the wrap case (l = Int64.max_int) where tick wraps to min_int. *)
  QCheck.Test.make ~count:500 ~name:"tick l > l (unless wrap)" arb_lamport (fun l ->
      QCheck.assume (not (Int64.equal (Lamport.to_int64 l) Int64.max_int));
      Lamport.compare (Lamport.tick l) l > 0)

let prop_merge_dominates =
  QCheck.Test.make ~count:500 ~name:"merge result > recv and > local"
    (QCheck.pair arb_lamport arb_lamport) (fun (recv, local) ->
      let r_i = Lamport.to_int64 recv in
      let l_i = Lamport.to_int64 local in
      QCheck.assume ((not (Int64.equal r_i Int64.max_int)) && not (Int64.equal l_i Int64.max_int));
      let m = Lamport.merge ~recv ~local in
      Lamport.compare m recv > 0 && Lamport.compare m local > 0)

let prop_merge_symmetric =
  QCheck.Test.make ~count:500 ~name:"merge ~recv:a ~local:b = merge ~recv:b ~local:a"
    (QCheck.pair arb_lamport arb_lamport) (fun (a, b) ->
      Lamport.equal (Lamport.merge ~recv:a ~local:b) (Lamport.merge ~recv:b ~local:a))

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "lamport"
    [
      ( "zero",
        [
          Alcotest.test_case "to_int64 zero = 0L" `Quick test_zero_round_trips;
          Alcotest.test_case "compare zero zero = 0" `Quick test_zero_reflexive;
        ] );
      ( "tick",
        [
          Alcotest.test_case "tick = +1" `Quick test_tick_adds_one;
          Alcotest.test_case "tick strictly increases" `Quick test_tick_strictly_increases;
        ] );
      ( "merge",
        [
          Alcotest.test_case "recv larger" `Quick test_merge_max_plus_one_recv_larger;
          Alcotest.test_case "local larger" `Quick test_merge_max_plus_one_local_larger;
          Alcotest.test_case "equal" `Quick test_merge_equal;
          Alcotest.test_case "result dominates both inputs" `Quick test_merge_dominates_inputs;
        ] );
      ("equal", [ Alcotest.test_case "matches compare = 0" `Quick test_equal_matches_compare ]);
      ("pp", [ Alcotest.test_case "format is decimal" `Quick test_pp_decimal ]);
      ( "of_int64",
        [
          Alcotest.test_case "raises Failure on negative" `Quick test_of_int64_rejects_negative;
          Alcotest.test_case "round-trip on nonneg covers extremes" `Quick
            test_round_trip_int64_nonneg;
        ] );
      ( "properties (qcheck)",
        List.map QCheck_alcotest.to_alcotest
          [
            prop_round_trip_int64_nonneg;
            prop_of_int64_raises_on_negative;
            prop_compare_reflexive;
            prop_compare_antisymmetric;
            prop_equal_iff_compare_zero;
            prop_tick_strictly_increases;
            prop_merge_dominates;
            prop_merge_symmetric;
          ] );
    ]
