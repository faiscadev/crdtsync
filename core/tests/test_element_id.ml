(** Tests for [Crdtsync_crdt.Element_id]. Black-box: only the .mli surface is exercised. *)

module Element_id = Crdtsync_crdt.Element_id

(* ── alcotest unit tests ──────────────────────────────────────────────────── *)

let test_root_round_trip_bytes () =
  let b = Element_id.to_bytes Element_id.root in
  Alcotest.(check int) "root bytes length = 16" 16 (Bytes.length b);
  match Element_id.of_bytes b with
  | None -> Alcotest.fail "root round-trip via bytes failed"
  | Some r -> Alcotest.(check int) "root = root" 0 (Element_id.compare r Element_id.root)

let test_root_round_trip_string () =
  let s = Element_id.to_string Element_id.root in
  Alcotest.(check int) "to_string is 36 chars" 36 (String.length s);
  match Element_id.of_string s with
  | None -> Alcotest.fail "root round-trip via string failed"
  | Some r -> Alcotest.(check int) "root = root" 0 (Element_id.compare r Element_id.root)

let test_root_is_nil () =
  (* Architecture says root is the Nil UUID (all-zero bytes). *)
  let b = Element_id.to_bytes Element_id.root in
  let all_zero = Bytes.create 16 in
  Bytes.fill all_zero 0 16 '\x00';
  Alcotest.(check bytes) "root bytes are all-zero" all_zero b

let test_derive_deterministic () =
  let a = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let b = Element_id.derive ~parent:Element_id.root ~key:"body" in
  Alcotest.(check int) "derive root body = derive root body" 0 (Element_id.compare a b)

let test_derive_different_keys_different_ids () =
  let a = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let b = Element_id.derive ~parent:Element_id.root ~key:"title" in
  Alcotest.(check bool) "derive root body <> derive root title" true (Element_id.compare a b <> 0)

let test_derive_different_parents_different_ids () =
  let p1 = Element_id.derive ~parent:Element_id.root ~key:"p1" in
  let p2 = Element_id.derive ~parent:Element_id.root ~key:"p2" in
  let a = Element_id.derive ~parent:p1 ~key:"body" in
  let b = Element_id.derive ~parent:p2 ~key:"body" in
  Alcotest.(check bool) "derive p1 body <> derive p2 body" true (Element_id.compare a b <> 0)

let test_derive_nested_chain () =
  let a = Element_id.derive ~parent:Element_id.root ~key:"a" in
  let b = Element_id.derive ~parent:a ~key:"b" in
  let c = Element_id.derive ~parent:b ~key:"c" in
  (* All three should round-trip via string. *)
  List.iter
    (fun id ->
      match Element_id.of_string (Element_id.to_string id) with
      | None -> Alcotest.failf "nested id failed string round-trip"
      | Some id' -> Alcotest.(check int) "nested id round-trips" 0 (Element_id.compare id id'))
    [ a; b; c ]

let test_derive_bytes_round_trip () =
  let id = Element_id.derive ~parent:Element_id.root ~key:"body" in
  match Element_id.of_bytes (Element_id.to_bytes id) with
  | None -> Alcotest.fail "derive id bytes round-trip failed"
  | Some id' -> Alcotest.(check int) "round-trip equal" 0 (Element_id.compare id id')

let test_derive_is_uuid_v5 () =
  (* UUID v5: byte 6 high nibble = 0x5 *)
  let id = Element_id.derive ~parent:Element_id.root ~key:"body" in
  let b = Element_id.to_bytes id in
  let nibble = (Bytes.get_uint8 b 6 land 0xF0) lsr 4 in
  Alcotest.(check int) "derived id is UUID v5 (version nibble = 5)" 5 nibble

let test_of_bytes_wrong_length () =
  let cases = [ 0; 15; 17; 32 ] in
  List.iter
    (fun n ->
      let bs = Bytes.create n in
      Alcotest.(check (option string))
        (Printf.sprintf "%d bytes rejected" n)
        None
        (Option.map Element_id.to_string (Element_id.of_bytes bs)))
    cases

let test_of_string_malformed () =
  let cases = [ ""; "not-a-uuid"; "zzzzzzzz-zzzz-5zzz-zzzz-zzzzzzzzzzzz" ] in
  List.iter
    (fun s ->
      Alcotest.(check (option string))
        (Printf.sprintf "%S rejected" s) None
        (Option.map Element_id.to_string (Element_id.of_string s)))
    cases

let test_equal_matches_compare () =
  let a = Element_id.derive ~parent:Element_id.root ~key:"x" in
  let b = Element_id.derive ~parent:Element_id.root ~key:"x" in
  let c = Element_id.derive ~parent:Element_id.root ~key:"y" in
  Alcotest.(check bool) "equal a b" true (Element_id.equal a b);
  Alcotest.(check bool) "not equal a c" false (Element_id.equal a c)

let test_pp_matches_to_string () =
  let id = Element_id.derive ~parent:Element_id.root ~key:"x" in
  let s = Format.asprintf "%a" Element_id.pp id in
  Alcotest.(check string) "pp = to_string" (Element_id.to_string id) s

(* ── qcheck property tests ────────────────────────────────────────────────── *)

let arb_key : string QCheck.arbitrary = QCheck.string_of_size (QCheck.Gen.int_bound 32)

let arb_element_id : Element_id.t QCheck.arbitrary =
  QCheck.map ~rev:Element_id.to_string
    (fun key -> Element_id.derive ~parent:Element_id.root ~key)
    arb_key

let prop_derive_deterministic =
  QCheck.Test.make ~count:300 ~name:"derive (parent, key) is deterministic"
    (QCheck.pair arb_element_id arb_key) (fun (parent, key) ->
      let a = Element_id.derive ~parent ~key in
      let b = Element_id.derive ~parent ~key in
      Element_id.compare a b = 0)

let prop_bytes_round_trip =
  QCheck.Test.make ~count:300 ~name:"bytes round-trip" arb_element_id (fun id ->
      match Element_id.of_bytes (Element_id.to_bytes id) with
      | Some id' -> Element_id.compare id id' = 0
      | None -> false)

let prop_string_round_trip =
  QCheck.Test.make ~count:300 ~name:"string round-trip" arb_element_id (fun id ->
      match Element_id.of_string (Element_id.to_string id) with
      | Some id' -> Element_id.compare id id' = 0
      | None -> false)

let prop_compare_antisymmetric =
  QCheck.Test.make ~count:300 ~name:"compare antisymmetric"
    (QCheck.pair arb_element_id arb_element_id) (fun (a, b) ->
      let ab = Element_id.compare a b in
      let ba = Element_id.compare b a in
      (ab = 0 && ba = 0) || ab * ba < 0)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "element_id"
    [
      ( "root",
        [
          Alcotest.test_case "bytes round-trip" `Quick test_root_round_trip_bytes;
          Alcotest.test_case "string round-trip" `Quick test_root_round_trip_string;
          Alcotest.test_case "is Nil UUID (all-zero bytes)" `Quick test_root_is_nil;
        ] );
      ( "derive",
        [
          Alcotest.test_case "deterministic on same (parent, key)" `Quick test_derive_deterministic;
          Alcotest.test_case "different keys yield different ids" `Quick
            test_derive_different_keys_different_ids;
          Alcotest.test_case "different parents yield different ids" `Quick
            test_derive_different_parents_different_ids;
          Alcotest.test_case "nested derivation chain round-trips" `Quick test_derive_nested_chain;
          Alcotest.test_case "bytes round-trip" `Quick test_derive_bytes_round_trip;
          Alcotest.test_case "produces UUID v5 (version nibble)" `Quick test_derive_is_uuid_v5;
        ] );
      ( "parsing rejects",
        [
          Alcotest.test_case "of_bytes wrong length" `Quick test_of_bytes_wrong_length;
          Alcotest.test_case "of_string malformed" `Quick test_of_string_malformed;
        ] );
      ( "equal/pp",
        [
          Alcotest.test_case "equal matches compare = 0" `Quick test_equal_matches_compare;
          Alcotest.test_case "pp matches to_string" `Quick test_pp_matches_to_string;
        ] );
      ( "properties (qcheck)",
        List.map QCheck_alcotest.to_alcotest
          [
            prop_derive_deterministic;
            prop_bytes_round_trip;
            prop_string_round_trip;
            prop_compare_antisymmetric;
          ] );
    ]
