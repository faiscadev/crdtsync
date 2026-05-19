(** Tests for [Crdtsync_crdt.Wall_time]. Black-box: only the .mli surface is exercised. *)

module Wall_time = Crdtsync_crdt.Wall_time

(* ── alcotest unit tests ──────────────────────────────────────────────────── *)

let test_of_ms_round_trip () =
  let cases = [ 0L; 1L; 1733000000000L; Int64.max_int ] in
  List.iter
    (fun n ->
      let r = Wall_time.to_ms (Wall_time.of_ms n) in
      Alcotest.(check int64) (Printf.sprintf "round-trip %Ld" n) n r)
    cases

let test_of_ms_rejects_negative () =
  let is_failure = function Failure _ -> true | _ -> false in
  Alcotest.check_raises "of_ms -1L raises Failure" (Failure "ignored") (fun () ->
      try
        let _ = Wall_time.of_ms (-1L) in
        ()
      with e -> if is_failure e then raise (Failure "ignored") else raise e);
  Alcotest.check_raises "of_ms Int64.min_int raises Failure" (Failure "ignored") (fun () ->
      try
        let _ = Wall_time.of_ms Int64.min_int in
        ()
      with e -> if is_failure e then raise (Failure "ignored") else raise e)

let test_compare_total () =
  let a = Wall_time.of_ms 100L in
  let b = Wall_time.of_ms 200L in
  Alcotest.(check bool) "a < b" true (Wall_time.compare a b < 0);
  Alcotest.(check bool) "b > a" true (Wall_time.compare b a > 0);
  Alcotest.(check int) "a = a" 0 (Wall_time.compare a a)

let test_equal_matches_compare () =
  let a = Wall_time.of_ms 7L in
  let b = Wall_time.of_ms 7L in
  let c = Wall_time.of_ms 8L in
  Alcotest.(check bool) "equal a b when compare = 0" true (Wall_time.equal a b);
  Alcotest.(check bool) "not equal a c" false (Wall_time.equal a c)

let test_pp_decimal () =
  Alcotest.(check string) "pp 0" "0" (Format.asprintf "%a" Wall_time.pp (Wall_time.of_ms 0L));
  Alcotest.(check string) "pp 42" "42" (Format.asprintf "%a" Wall_time.pp (Wall_time.of_ms 42L));
  Alcotest.(check string)
    "pp 1733000000000" "1733000000000"
    (Format.asprintf "%a" Wall_time.pp (Wall_time.of_ms 1733000000000L))

let test_now_is_sensible () =
  (* Sanity bound: now should be between Jan 1 2020 (1577836800000 ms) and
     Jan 1 2100 (4102444800000 ms). Catches gross unit mistakes (s vs ms) or
     epoch confusion. *)
  let t = Wall_time.now () in
  let ms = Wall_time.to_ms t in
  Alcotest.(check bool) "now > 2020-01-01 in ms" true (Int64.compare ms 1577836800000L > 0);
  Alcotest.(check bool) "now < 2100-01-01 in ms" true (Int64.compare ms 4102444800000L < 0)

let test_now_monotone_ish () =
  (* Wall clock isn't monotonic in general (NTP slews etc.), but two successive
     calls should be ordered or equal within the same run. *)
  let a = Wall_time.now () in
  let b = Wall_time.now () in
  Alcotest.(check bool) "now b >= now a" true (Wall_time.compare b a >= 0)

(* ── qcheck property tests ────────────────────────────────────────────────── *)

let nonneg_int64 : int64 QCheck.arbitrary =
  QCheck.map ~rev:(fun n -> n) (fun n -> Int64.logand n Int64.max_int) QCheck.int64

let prop_round_trip =
  QCheck.Test.make ~count:500 ~name:"of_ms / to_ms round-trip on nonneg" nonneg_int64 (fun n ->
      Int64.equal n (Wall_time.to_ms (Wall_time.of_ms n)))

let prop_compare_antisymmetric =
  QCheck.Test.make ~count:200 ~name:"compare antisymmetric" (QCheck.pair nonneg_int64 nonneg_int64)
    (fun (a, b) ->
      let ta = Wall_time.of_ms a in
      let tb = Wall_time.of_ms b in
      let ab = Wall_time.compare ta tb in
      let ba = Wall_time.compare tb ta in
      (ab = 0 && ba = 0) || ab * ba < 0)

let prop_equal_iff_compare_zero =
  QCheck.Test.make ~count:200 ~name:"equal a b <=> compare a b = 0"
    (QCheck.pair nonneg_int64 nonneg_int64) (fun (a, b) ->
      let ta = Wall_time.of_ms a in
      let tb = Wall_time.of_ms b in
      Bool.equal (Wall_time.equal ta tb) (Wall_time.compare ta tb = 0))

let prop_of_ms_raises_on_negative =
  QCheck.Test.make ~count:200 ~name:"of_ms raises Failure on negative" QCheck.int64 (fun n ->
      if n < 0L then
        try
          let _ = Wall_time.of_ms n in
          false
        with Failure _ -> true
      else true)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "wall_time"
    [
      ( "of_ms",
        [
          Alcotest.test_case "round-trip on nonneg extremes" `Quick test_of_ms_round_trip;
          Alcotest.test_case "rejects negative" `Quick test_of_ms_rejects_negative;
        ] );
      ( "compare",
        [
          Alcotest.test_case "total order" `Quick test_compare_total;
          Alcotest.test_case "equal matches compare = 0" `Quick test_equal_matches_compare;
        ] );
      ("pp", [ Alcotest.test_case "format is decimal ms" `Quick test_pp_decimal ]);
      ( "now",
        [
          Alcotest.test_case "in sensible epoch ms range" `Quick test_now_is_sensible;
          Alcotest.test_case "successive calls ordered" `Quick test_now_monotone_ish;
        ] );
      ( "properties (qcheck)",
        List.map QCheck_alcotest.to_alcotest
          [
            prop_round_trip;
            prop_compare_antisymmetric;
            prop_equal_iff_compare_zero;
            prop_of_ms_raises_on_negative;
          ] );
    ]
