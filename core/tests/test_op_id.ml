(** Tests for [Crdtsync_crdt.Op_id]. Black-box: only the .mli surface is exercised. *)

module Uuid_v7 = Crdtsync_crdt.Uuid_v7
module Op_id = Crdtsync_crdt.Op_id

(* ── alcotest unit tests ──────────────────────────────────────────────────── *)

let test_make_projects_client_id () =
  let cid = Uuid_v7.v () in
  let op = Op_id.make ~client_id:cid ~client_seq:0L in
  Alcotest.(check int)
    "client_id round-trips through make" 0
    (Uuid_v7.compare cid (Op_id.client_id op))

let test_make_projects_client_seq () =
  let cid = Uuid_v7.v () in
  let op = Op_id.make ~client_id:cid ~client_seq:42L in
  Alcotest.(check int64) "client_seq round-trips through make" 42L (Op_id.client_seq op)

let test_compare_reflexive () =
  let op = Op_id.make ~client_id:(Uuid_v7.v ()) ~client_seq:7L in
  Alcotest.(check int) "compare op op = 0" 0 (Op_id.compare op op)

let test_compare_client_id_first () =
  (* Two distinct client_ids, same client_seq: order follows Uuid_v7.compare. *)
  let a_cid = Uuid_v7.v () in
  let b_cid = Uuid_v7.v () in
  let a = Op_id.make ~client_id:a_cid ~client_seq:100L in
  let b = Op_id.make ~client_id:b_cid ~client_seq:100L in
  let expected = Uuid_v7.compare a_cid b_cid in
  let actual = Op_id.compare a b in
  Alcotest.(check bool)
    "sign matches Uuid_v7.compare on client_id" true
    ((expected = 0 && actual = 0) || expected * actual > 0)

let test_compare_client_seq_on_tie () =
  (* Same client_id, different client_seq: order follows Int64.compare. *)
  let cid = Uuid_v7.v () in
  let lo = Op_id.make ~client_id:cid ~client_seq:1L in
  let hi = Op_id.make ~client_id:cid ~client_seq:2L in
  Alcotest.(check bool) "lo < hi" true (Op_id.compare lo hi < 0);
  Alcotest.(check bool) "hi > lo" true (Op_id.compare hi lo > 0);
  Alcotest.(check int) "lo = lo" 0 (Op_id.compare lo lo)

let test_compare_int64_unsigned_or_signed () =
  (* Document compare behaviour around Int64.min_int / max_int. Int64.compare is signed:
     min_int < 0 < max_int. If we ever switched to unsigned, this would flip. *)
  let cid = Uuid_v7.v () in
  let mn = Op_id.make ~client_id:cid ~client_seq:Int64.min_int in
  let zero = Op_id.make ~client_id:cid ~client_seq:0L in
  let mx = Op_id.make ~client_id:cid ~client_seq:Int64.max_int in
  Alcotest.(check bool) "min_int < 0" true (Op_id.compare mn zero < 0);
  Alcotest.(check bool) "0 < max_int" true (Op_id.compare zero mx < 0);
  Alcotest.(check bool) "min_int < max_int" true (Op_id.compare mn mx < 0)

let test_equal_matches_compare () =
  let cid = Uuid_v7.v () in
  let a = Op_id.make ~client_id:cid ~client_seq:5L in
  let b = Op_id.make ~client_id:cid ~client_seq:5L in
  let c = Op_id.make ~client_id:cid ~client_seq:6L in
  Alcotest.(check bool) "equal a b when compare = 0" true (Op_id.equal a b);
  Alcotest.(check bool) "not equal a c when compare <> 0" false (Op_id.equal a c)

let test_equal_distinct_client_id () =
  (* Catches the bug where equal compares a.client_id to itself instead of b.client_id. *)
  let a = Op_id.make ~client_id:(Uuid_v7.v ()) ~client_seq:1L in
  let b = Op_id.make ~client_id:(Uuid_v7.v ()) ~client_seq:1L in
  Alcotest.(check bool)
    "different client_id => not equal even with same seq" false (Op_id.equal a b)

let test_pp_format () =
  let cid = Uuid_v7.v () in
  let op = Op_id.make ~client_id:cid ~client_seq:123L in
  let s = Format.asprintf "%a" Op_id.pp op in
  let expected = Format.asprintf "%a:123" Uuid_v7.pp cid in
  Alcotest.(check string) "pp = \"<client_id>:<client_seq>\"" expected s

(* ── qcheck property tests ────────────────────────────────────────────────── *)

let arb_op_id : Op_id.t QCheck.arbitrary =
  let gen_seq = QCheck.Gen.(map Int64.of_int int) in
  QCheck.make
    ~print:(fun op -> Format.asprintf "%a" Op_id.pp op)
    (fun rand ->
      let cid = Uuid_v7.v () in
      let seq = gen_seq rand in
      Op_id.make ~client_id:cid ~client_seq:seq)

let prop_compare_reflexive =
  QCheck.Test.make ~count:200 ~name:"compare op op = 0" arb_op_id (fun op ->
      Op_id.compare op op = 0)

let prop_compare_antisymmetric =
  QCheck.Test.make ~count:200 ~name:"compare antisymmetric" (QCheck.pair arb_op_id arb_op_id)
    (fun (a, b) ->
      let ab = Op_id.compare a b in
      let ba = Op_id.compare b a in
      (ab = 0 && ba = 0) || ab * ba < 0)

let prop_equal_iff_compare_zero =
  QCheck.Test.make ~count:200 ~name:"equal a b <=> compare a b = 0"
    (QCheck.pair arb_op_id arb_op_id) (fun (a, b) ->
      Bool.equal (Op_id.equal a b) (Op_id.compare a b = 0))

let prop_make_round_trip =
  let cid_gen = QCheck.make ~print:Uuid_v7.to_string (fun _ -> Uuid_v7.v ()) in
  let seq_gen = QCheck.int64 in
  QCheck.Test.make ~count:200 ~name:"make/project round-trip" (QCheck.pair cid_gen seq_gen)
    (fun (cid, seq) ->
      let op = Op_id.make ~client_id:cid ~client_seq:seq in
      Uuid_v7.compare (Op_id.client_id op) cid = 0 && Int64.equal (Op_id.client_seq op) seq)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "op_id"
    [
      ( "construction",
        [
          Alcotest.test_case "make then client_id round-trips" `Quick test_make_projects_client_id;
          Alcotest.test_case "make then client_seq round-trips" `Quick test_make_projects_client_seq;
        ] );
      ( "compare",
        [
          Alcotest.test_case "reflexive" `Quick test_compare_reflexive;
          Alcotest.test_case "client_id is primary key" `Quick test_compare_client_id_first;
          Alcotest.test_case "client_seq breaks ties" `Quick test_compare_client_seq_on_tie;
          Alcotest.test_case "signed Int64 ordering at extremes" `Quick
            test_compare_int64_unsigned_or_signed;
        ] );
      ( "equal",
        [
          Alcotest.test_case "matches compare = 0" `Quick test_equal_matches_compare;
          Alcotest.test_case "distinct client_id => not equal" `Quick test_equal_distinct_client_id;
        ] );
      ("pp", [ Alcotest.test_case "format is \"<client_id>:<client_seq>\"" `Quick test_pp_format ]);
      ( "properties (qcheck)",
        List.map QCheck_alcotest.to_alcotest
          [
            prop_compare_reflexive;
            prop_compare_antisymmetric;
            prop_equal_iff_compare_zero;
            prop_make_round_trip;
          ] );
    ]
