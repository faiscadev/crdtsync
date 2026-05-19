(** Tests for [Crdtsync_crdt.Envelope] and [Crdtsync_crdt.Op]. Black-box: only the .mli surface is
    exercised. *)

module Uuid_v7 = Crdtsync_crdt.Uuid_v7
module Op_id = Crdtsync_crdt.Op_id
module Lamport = Crdtsync_crdt.Lamport
module Wall_time = Crdtsync_crdt.Wall_time
module Op = Crdtsync_crdt.Op
module Envelope = Crdtsync_crdt.Envelope

(* ── helpers ───────────────────────────────────────────────────────────────── *)

let fresh_op_id ?(seq = 1L) () = Op_id.make ~client_id:(Uuid_v7.v ()) ~client_seq:seq

let fresh_envelope ?(tx = None) () =
  Envelope.make ~op_id:(fresh_op_id ()) ~actor_id:"user_42" ~room:"doc-1" ~branch:"main"
    ~zone:"shared_content" ~schema_version:5 ~lamport:(Lamport.of_int64 18923L)
    ~wall_time:(Wall_time.of_ms 1733000000000L) ~op:Op.Placeholder ?tx ()

(* ── Op tests ──────────────────────────────────────────────────────────────── *)

let test_op_equal_kind_reflexive () =
  Alcotest.(check bool)
    "Placeholder = Placeholder" true
    (Op.equal_kind Op.Placeholder Op.Placeholder)

let test_op_pp_kind_nonempty () =
  let s = Format.asprintf "%a" Op.pp_kind Op.Placeholder in
  Alcotest.(check bool) "pp_kind output is nonempty" true (String.length s > 0)

(* ── Envelope construction / accessor round-trip ──────────────────────────── *)

let test_make_round_trips_all_fields () =
  let op_id = fresh_op_id ~seq:7L () in
  let lamport = Lamport.of_int64 42L in
  let wall_time = Wall_time.of_ms 1700000000000L in
  let env =
    Envelope.make ~op_id ~actor_id:"alice" ~room:"room-1" ~branch:"feature/x" ~zone:"zone-a"
      ~schema_version:3 ~lamport ~wall_time ~op:Op.Placeholder ()
  in
  Alcotest.(check bool) "op_id round-trips" true (Op_id.equal env.op_id op_id);
  Alcotest.(check string) "actor_id round-trips" "alice" env.actor_id;
  Alcotest.(check string) "room round-trips" "room-1" env.room;
  Alcotest.(check string) "branch round-trips" "feature/x" env.branch;
  Alcotest.(check string) "zone round-trips" "zone-a" env.zone;
  Alcotest.(check int) "schema_version round-trips" 3 env.schema_version;
  Alcotest.(check int64) "lamport round-trips" 42L (Lamport.to_int64 env.lamport);
  Alcotest.(check int64) "wall_time round-trips" 1700000000000L (Wall_time.to_ms env.wall_time);
  Alcotest.(check bool) "op round-trips" true (Op.equal_kind env.op Op.Placeholder);
  Alcotest.(check bool) "tx defaults to None when ?tx omitted" true (Option.is_none env.tx)

let test_make_with_tx_member () =
  let tx_id = Uuid_v7.v () in
  let env = fresh_envelope ~tx:(Some (tx_id, Envelope.Member)) () in
  match env.tx with
  | Some (id, Envelope.Member) ->
      Alcotest.(check int) "tx_id round-trips" 0 (Uuid_v7.compare id tx_id)
  | Some (_, Envelope.Commit) -> Alcotest.fail "got Commit, expected Member"
  | None -> Alcotest.fail "got None, expected Some Member"

let test_make_with_tx_commit () =
  let tx_id = Uuid_v7.v () in
  let env = fresh_envelope ~tx:(Some (tx_id, Envelope.Commit)) () in
  match env.tx with
  | Some (id, Envelope.Commit) ->
      Alcotest.(check int) "tx_id round-trips" 0 (Uuid_v7.compare id tx_id)
  | Some (_, Envelope.Member) -> Alcotest.fail "got Member, expected Commit"
  | None -> Alcotest.fail "got None, expected Some Commit"

(* ── equal ────────────────────────────────────────────────────────────────── *)

let test_equal_reflexive () =
  let env = fresh_envelope () in
  Alcotest.(check bool) "env = env" true (Envelope.equal env env)

let test_equal_same_fields_distinct_values () =
  (* Two envelopes constructed from the SAME field values must be equal. *)
  let op_id = fresh_op_id () in
  let lamport = Lamport.of_int64 100L in
  let mk () =
    Envelope.make ~op_id ~actor_id:"u" ~room:"r" ~branch:"b" ~zone:"z" ~schema_version:1 ~lamport
      ~wall_time:(Wall_time.of_ms 0L) ~op:Op.Placeholder ()
  in
  Alcotest.(check bool)
    "structurally equal envelopes compare equal" true
    (Envelope.equal (mk ()) (mk ()))

let test_equal_differing_op_id () =
  let lamport = Lamport.of_int64 100L in
  let a =
    Envelope.make ~op_id:(fresh_op_id ()) ~actor_id:"u" ~room:"r" ~branch:"b" ~zone:"z"
      ~schema_version:1 ~lamport ~wall_time:(Wall_time.of_ms 0L) ~op:Op.Placeholder ()
  in
  let b =
    Envelope.make ~op_id:(fresh_op_id ()) ~actor_id:"u" ~room:"r" ~branch:"b" ~zone:"z"
      ~schema_version:1 ~lamport ~wall_time:(Wall_time.of_ms 0L) ~op:Op.Placeholder ()
  in
  Alcotest.(check bool) "different op_id => not equal" false (Envelope.equal a b)

let test_equal_differing_tx () =
  let op_id = fresh_op_id () in
  let lamport = Lamport.of_int64 1L in
  let mk tx =
    Envelope.make ~op_id ~actor_id:"u" ~room:"r" ~branch:"b" ~zone:"z" ~schema_version:1 ~lamport
      ~wall_time:(Wall_time.of_ms 0L) ~op:Op.Placeholder ?tx ()
  in
  let t1 = Uuid_v7.v () in
  let t2 = Uuid_v7.v () in
  let none = mk None in
  let mem = mk (Some (t1, Envelope.Member)) in
  let com = mk (Some (t1, Envelope.Commit)) in
  let mem_other_id = mk (Some (t2, Envelope.Member)) in
  Alcotest.(check bool) "tx=None vs Some => not equal" false (Envelope.equal none mem);
  Alcotest.(check bool) "tx Member vs Commit => not equal" false (Envelope.equal mem com);
  Alcotest.(check bool) "tx differing id => not equal" false (Envelope.equal mem mem_other_id)

(* ── tx_role ──────────────────────────────────────────────────────────────── *)

let test_equal_tx_role () =
  Alcotest.(check bool) "Member = Member" true (Envelope.equal_tx_role Member Member);
  Alcotest.(check bool) "Commit = Commit" true (Envelope.equal_tx_role Commit Commit);
  Alcotest.(check bool) "Member <> Commit" false (Envelope.equal_tx_role Member Commit)

let test_pp_tx_role_nonempty () =
  let m = Format.asprintf "%a" Envelope.pp_tx_role Envelope.Member in
  let c = Format.asprintf "%a" Envelope.pp_tx_role Envelope.Commit in
  Alcotest.(check bool) "pp_tx_role Member nonempty" true (String.length m > 0);
  Alcotest.(check bool) "pp_tx_role Commit nonempty" true (String.length c > 0);
  Alcotest.(check bool) "Member and Commit print differently" true (not (String.equal m c))

(* ── pp envelope ──────────────────────────────────────────────────────────── *)

let test_pp_envelope_nonempty () =
  let s = Format.asprintf "%a" Envelope.pp (fresh_envelope ()) in
  Alcotest.(check bool) "pp output nonempty" true (String.length s > 0)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "envelope"
    [
      ( "op kind",
        [
          Alcotest.test_case "equal_kind reflexive" `Quick test_op_equal_kind_reflexive;
          Alcotest.test_case "pp_kind not empty" `Quick test_op_pp_kind_nonempty;
        ] );
      ( "make",
        [
          Alcotest.test_case "all fields round-trip through accessors" `Quick
            test_make_round_trips_all_fields;
          Alcotest.test_case "tx = Some Member" `Quick test_make_with_tx_member;
          Alcotest.test_case "tx = Some Commit" `Quick test_make_with_tx_commit;
        ] );
      ( "equal",
        [
          Alcotest.test_case "reflexive" `Quick test_equal_reflexive;
          Alcotest.test_case "same fields => equal" `Quick test_equal_same_fields_distinct_values;
          Alcotest.test_case "differing op_id => not equal" `Quick test_equal_differing_op_id;
          Alcotest.test_case "differing tx => not equal" `Quick test_equal_differing_tx;
        ] );
      ( "tx_role",
        [
          Alcotest.test_case "equal_tx_role" `Quick test_equal_tx_role;
          Alcotest.test_case "pp_tx_role nonempty and distinct" `Quick test_pp_tx_role_nonempty;
        ] );
      ("pp", [ Alcotest.test_case "envelope pp nonempty" `Quick test_pp_envelope_nonempty ]);
    ]
