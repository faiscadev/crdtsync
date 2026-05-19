(** Smoke test: SDK library links + trivial assertion passes. *)

let test_sdk_links () =
  Alcotest.(check string) "sdk placeholder version" "0.0.0" Crdtsync_sdk.version

let () =
  Alcotest.run "ocaml-sdk-smoke" [ ("sdk", [ Alcotest.test_case "links" `Quick test_sdk_links ]) ]
