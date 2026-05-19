(** Tests for [Crdtsync_crdt.Uuid_v7]. Black-box: only the .mli surface is exercised,
    never internals. *)

module Uuid_v7 = Crdtsync_crdt.Uuid_v7

(* ── alcotest unit tests ──────────────────────────────────────────────────── *)

let test_to_bytes_length () =
  let u = Uuid_v7.v () in
  let b = Uuid_v7.to_bytes u in
  Alcotest.(check int) "16 bytes" 16 (Bytes.length b)

let test_distinct_calls () =
  let a = Uuid_v7.v () in
  let b = Uuid_v7.v () in
  Alcotest.(check bool) "consecutive calls differ" true (Uuid_v7.compare a b <> 0)

let test_compare_reflexive () =
  let u = Uuid_v7.v () in
  Alcotest.(check int) "u compare u = 0" 0 (Uuid_v7.compare u u)

let test_bytes_round_trip () =
  let u = Uuid_v7.v () in
  let b = Uuid_v7.to_bytes u in
  match Uuid_v7.of_bytes b with
  | None -> Alcotest.fail "of_bytes returned None on a freshly-generated UUID"
  | Some u' -> Alcotest.(check int) "round-trip equal" 0 (Uuid_v7.compare u u')

let test_string_round_trip () =
  let u = Uuid_v7.v () in
  let s = Uuid_v7.to_string u in
  match Uuid_v7.of_string s with
  | None -> Alcotest.failf "of_string returned None on output of to_string: %S" s
  | Some u' -> Alcotest.(check int) "round-trip equal" 0 (Uuid_v7.compare u u')

let test_to_string_format () =
  let u = Uuid_v7.v () in
  let s = Uuid_v7.to_string u in
  Alcotest.(check int) "36 characters" 36 (String.length s);
  Alcotest.(check char) "dash at index 8" '-' s.[8];
  Alcotest.(check char) "dash at index 13" '-' s.[13];
  Alcotest.(check char) "dash at index 18" '-' s.[18];
  Alcotest.(check char) "dash at index 23" '-' s.[23];
  Alcotest.(check char) "version digit '7' at index 14" '7' s.[14]

let test_of_bytes_wrong_length () =
  let too_short = Bytes.create 15 in
  let too_long = Bytes.create 17 in
  let empty = Bytes.create 0 in
  Alcotest.(check (option string))
    "15 bytes rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_bytes too_short));
  Alcotest.(check (option string))
    "17 bytes rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_bytes too_long));
  Alcotest.(check (option string))
    "0 bytes rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_bytes empty))

let test_of_bytes_wrong_version () =
  let u = Uuid_v7.v () in
  let b = Bytes.copy (Uuid_v7.to_bytes u) in
  (* Byte 6 high nibble is the version: 0x7 for v7. Force to 0x4 (v4). *)
  let b6 = Bytes.get_uint8 b 6 in
  Bytes.set_uint8 b 6 (b6 land 0x0f lor 0x40);
  Alcotest.(check (option string))
    "non-v7 version rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_bytes b))

let test_of_string_malformed () =
  Alcotest.(check (option string))
    "empty rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_string ""));
  Alcotest.(check (option string))
    "too short rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_string "not-a-uuid"));
  Alcotest.(check (option string))
    "garbage rejected" None
    (Option.map Uuid_v7.to_string
       (Uuid_v7.of_string "zzzzzzzz-zzzz-7zzz-zzzz-zzzzzzzzzzzz"))

let test_of_string_wrong_version () =
  let u = Uuid_v7.v () in
  let s = Uuid_v7.to_string u in
  (* Position 14 is the version hex digit; replace with '4' for v4. *)
  let buf = Bytes.of_string s in
  Bytes.set buf 14 '4';
  let s' = Bytes.to_string buf in
  Alcotest.(check (option string))
    "non-v7 version rejected" None
    (Option.map Uuid_v7.to_string (Uuid_v7.of_string s'))

let test_pp_matches_to_string () =
  let u = Uuid_v7.v () in
  let s = Format.asprintf "%a" Uuid_v7.pp u in
  Alcotest.(check string) "pp = to_string" (Uuid_v7.to_string u) s

let test_many_distinct () =
  (* No collisions across a batch (probabilistic but the chance of false
     failure is astronomical). *)
  let n = 1000 in
  let uuids = List.init n (fun _ -> Uuid_v7.v ()) in
  let sorted = List.sort Uuid_v7.compare uuids in
  let rec no_dups = function
    | [] | [ _ ] -> true
    | a :: (b :: _ as rest) -> Uuid_v7.compare a b <> 0 && no_dups rest
  in
  Alcotest.(check bool) "all 1000 distinct" true (no_dups sorted)

let test_stress_ms_counter_overflow () =
  (* Generate enough UUIDs that the per-millisecond counter (4096 for v7
     monotonic) probably overflows several times on a modern machine. This
     exercises the [None]-retry path in [v ()] without explicitly mocking the
     generator. Probabilistic coverage: on a slow CI runner the clock may
     advance faster than the counter fills, in which case None is never
     returned and the retry path is not exercised — that's fine; the test
     still asserts the no-collision invariant. *)
  let n = 100_000 in
  let table = Hashtbl.create n in
  for _ = 1 to n do
    let u = Uuid_v7.v () in
    let key = Uuid_v7.to_bytes u in
    if Hashtbl.mem table key then
      Alcotest.failf "duplicate UUID after %d generations: %s" (Hashtbl.length table)
        (Uuid_v7.to_string u);
    Hashtbl.add table key ()
  done;
  Alcotest.(check int) "all distinct under stress" n (Hashtbl.length table)

(* ── qcheck property tests ────────────────────────────────────────────────── *)

(* QCheck arbitrary that drives fresh UUID generation. We ignore qcheck's
   random state because Uuid_v7.v has its own internal randomness; this is
   fine for round-trip / structural properties. *)
let arb_uuid : Uuid_v7.t QCheck.arbitrary =
  QCheck.make ~print:(fun u -> Uuid_v7.to_string u) (fun _rand -> Uuid_v7.v ())

let prop_bytes_round_trip =
  QCheck.Test.make ~count:200 ~name:"to_bytes/of_bytes round-trip" arb_uuid (fun u ->
      match Uuid_v7.of_bytes (Uuid_v7.to_bytes u) with
      | Some u' -> Uuid_v7.compare u u' = 0
      | None -> false)

let prop_string_round_trip =
  QCheck.Test.make ~count:200 ~name:"to_string/of_string round-trip" arb_uuid (fun u ->
      match Uuid_v7.of_string (Uuid_v7.to_string u) with
      | Some u' -> Uuid_v7.compare u u' = 0
      | None -> false)

let prop_compare_total =
  (* Antisymmetry-of-sign: compare a b and compare b a have opposite signs (or both 0). *)
  QCheck.Test.make ~count:200 ~name:"compare antisymmetric"
    (QCheck.pair arb_uuid arb_uuid) (fun (a, b) ->
      let ab = Uuid_v7.compare a b in
      let ba = Uuid_v7.compare b a in
      (ab = 0 && ba = 0) || ab * ba < 0)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "uuid_v7"
    [
      ( "structure",
        [
          Alcotest.test_case "to_bytes returns 16 bytes" `Quick test_to_bytes_length;
          Alcotest.test_case "to_string is 36 chars with dashes and v7 nibble" `Quick
            test_to_string_format;
          Alcotest.test_case "pp matches to_string" `Quick test_pp_matches_to_string;
        ] );
      ( "generation",
        [
          Alcotest.test_case "consecutive calls differ" `Quick test_distinct_calls;
          Alcotest.test_case "compare is reflexive" `Quick test_compare_reflexive;
          Alcotest.test_case "1000 calls produce no duplicates" `Quick test_many_distinct;
          Alcotest.test_case "100k stress (exercises None retry path)" `Slow
            test_stress_ms_counter_overflow;
        ] );
      ( "round-trip",
        [
          Alcotest.test_case "bytes round-trip" `Quick test_bytes_round_trip;
          Alcotest.test_case "string round-trip" `Quick test_string_round_trip;
        ] );
      ( "parsing rejects",
        [
          Alcotest.test_case "of_bytes wrong length" `Quick test_of_bytes_wrong_length;
          Alcotest.test_case "of_bytes wrong version nibble" `Quick
            test_of_bytes_wrong_version;
          Alcotest.test_case "of_string malformed input" `Quick test_of_string_malformed;
          Alcotest.test_case "of_string wrong version digit" `Quick
            test_of_string_wrong_version;
        ] );
      ( "properties (qcheck)",
        List.map QCheck_alcotest.to_alcotest
          [ prop_bytes_round_trip; prop_string_round_trip; prop_compare_total ] );
    ]
