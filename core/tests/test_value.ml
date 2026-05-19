(** Tests for [Crdtsync_crdt.Value]. Black-box: only the .mli surface is exercised. *)

module Element_id = Crdtsync_crdt.Element_id
module Value = Crdtsync_crdt.Value

(* ── scalar ────────────────────────────────────────────────────────────────── *)

let test_equal_scalar_reflexive () =
  let cases : Value.scalar list =
    [ String "hello"; Int 42L; Float 3.14; Bool true; Bool false; Null ]
  in
  List.iter
    (fun s -> Alcotest.(check bool) "scalar = itself" true (Value.equal_scalar s s))
    cases

let test_equal_scalar_distinguishes_variants () =
  (* Distinct constructors with bit-identical payload must NOT compare equal. *)
  Alcotest.(check bool)
    "String \"42\" <> Int 42L" false
    (Value.equal_scalar (String "42") (Int 42L));
  Alcotest.(check bool)
    "Int 1L <> Bool true" false
    (Value.equal_scalar (Int 1L) (Bool true));
  Alcotest.(check bool)
    "Int 0L <> Bool false" false
    (Value.equal_scalar (Int 0L) (Bool false));
  Alcotest.(check bool)
    "Float 0.0 <> Int 0L" false
    (Value.equal_scalar (Float 0.0) (Int 0L));
  Alcotest.(check bool) "Null <> Bool false" false (Value.equal_scalar Null (Bool false))

let test_equal_scalar_string () =
  Alcotest.(check bool)
    "same string" true
    (Value.equal_scalar (String "abc") (String "abc"));
  Alcotest.(check bool)
    "different string" false
    (Value.equal_scalar (String "abc") (String "abd"));
  Alcotest.(check bool)
    "empty vs nonempty" false
    (Value.equal_scalar (String "") (String "x"))

let test_equal_scalar_int () =
  Alcotest.(check bool) "same int" true (Value.equal_scalar (Int 5L) (Int 5L));
  Alcotest.(check bool) "different int" false (Value.equal_scalar (Int 5L) (Int 6L));
  Alcotest.(check bool)
    "max_int" true
    (Value.equal_scalar (Int Int64.max_int) (Int Int64.max_int))

let test_equal_scalar_float () =
  Alcotest.(check bool) "same float" true (Value.equal_scalar (Float 1.5) (Float 1.5));
  Alcotest.(check bool)
    "different float" false
    (Value.equal_scalar (Float 1.5) (Float 1.6));
  (* NaN is famously not equal to itself in IEEE 754, but for CRDT determinism
     we likely want NaN = NaN. Test documents the chosen behavior; if the
     implementation chooses IEEE semantics, this fails and we know to update. *)
  Alcotest.(check bool)
    "NaN = NaN (CRDT determinism)" true
    (Value.equal_scalar (Float Float.nan) (Float Float.nan))

let test_pp_scalar_distinct () =
  let strs =
    List.map
      (fun s -> Format.asprintf "%a" Value.pp_scalar s)
      [ Value.String "hello"; Int 42L; Float 3.14; Bool true; Bool false; Null ]
  in
  let unique = List.sort_uniq String.compare strs in
  Alcotest.(check int) "all 6 scalars print distinctly" 6 (List.length unique)

(* ── t (Scalar / Element) ─────────────────────────────────────────────────── *)

let test_equal_value_reflexive () =
  let s = Value.Scalar (Int 1L) in
  let e = Value.Element (Element_id.derive ~parent:Element_id.root ~key:"x") in
  Alcotest.(check bool) "Scalar = itself" true (Value.equal s s);
  Alcotest.(check bool) "Element = itself" true (Value.equal e e)

let test_equal_value_distinguishes_variants () =
  let e = Element_id.derive ~parent:Element_id.root ~key:"x" in
  Alcotest.(check bool)
    "Scalar(Int 0) <> Element _" false
    (Value.equal (Scalar (Int 0L)) (Element e))

let test_equal_value_scalar_inner_changes () =
  Alcotest.(check bool)
    "Scalar(Int 1) <> Scalar(Int 2)" false
    (Value.equal (Scalar (Int 1L)) (Scalar (Int 2L)));
  Alcotest.(check bool)
    "Scalar(Int 1) = Scalar(Int 1)" true
    (Value.equal (Scalar (Int 1L)) (Scalar (Int 1L)))

let test_equal_value_element_inner_changes () =
  let a = Element_id.derive ~parent:Element_id.root ~key:"a" in
  let b = Element_id.derive ~parent:Element_id.root ~key:"b" in
  Alcotest.(check bool) "Element a = Element a" true (Value.equal (Element a) (Element a));
  Alcotest.(check bool)
    "Element a <> Element b" false
    (Value.equal (Element a) (Element b))

let test_pp_value_nonempty () =
  let cases =
    [
      Value.Scalar (String "hi");
      Scalar (Int 7L);
      Scalar (Float 3.14);
      Scalar (Bool true);
      Scalar Null;
      Element (Element_id.derive ~parent:Element_id.root ~key:"k");
    ]
  in
  List.iter
    (fun v ->
      let s = Format.asprintf "%a" Value.pp v in
      Alcotest.(check bool) "pp output nonempty" true (String.length s > 0))
    cases

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Alcotest.run "value"
    [
      ( "equal_scalar",
        [
          Alcotest.test_case "reflexive on all variants" `Quick
            test_equal_scalar_reflexive;
          Alcotest.test_case "distinguishes variant kinds" `Quick
            test_equal_scalar_distinguishes_variants;
          Alcotest.test_case "string equality semantics" `Quick test_equal_scalar_string;
          Alcotest.test_case "int equality semantics" `Quick test_equal_scalar_int;
          Alcotest.test_case "float equality (incl NaN = NaN)" `Quick
            test_equal_scalar_float;
        ] );
      ( "pp_scalar",
        [
          Alcotest.test_case "distinct outputs across variants" `Quick
            test_pp_scalar_distinct;
        ] );
      ( "equal",
        [
          Alcotest.test_case "reflexive" `Quick test_equal_value_reflexive;
          Alcotest.test_case "distinguishes Scalar vs Element" `Quick
            test_equal_value_distinguishes_variants;
          Alcotest.test_case "Scalar inner change" `Quick
            test_equal_value_scalar_inner_changes;
          Alcotest.test_case "Element inner change" `Quick
            test_equal_value_element_inner_changes;
        ] );
      ( "pp",
        [
          Alcotest.test_case "output nonempty for all variants" `Quick
            test_pp_value_nonempty;
        ] );
    ]
