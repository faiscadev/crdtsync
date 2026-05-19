(** Smoke test: every sublibrary links + a trivial assertion passes.

    Just proves the workspace builds end-to-end. Real tests land alongside each feature
    implementation (CORE-1+ etc.). *)

let test_sublibraries_link () =
  Alcotest.(check string) "crdt placeholder version" "0.0.0" Crdtsync_crdt.version;
  Alcotest.(check string) "wire placeholder version" "0.0.0" Crdtsync_wire.version;
  Alcotest.(check string) "persist placeholder version" "0.0.0" Crdtsync_persist.version;
  Alcotest.(check string) "auth placeholder version" "0.0.0" Crdtsync_auth.version;
  Alcotest.(check string) "blob placeholder version" "0.0.0" Crdtsync_blob.version;
  Alcotest.(check string) "server placeholder version" "0.0.0" Crdtsync_server.version

let () =
  Alcotest.run "smoke"
    [
      ("sublibraries", [ Alcotest.test_case "all sublibraries link" `Quick test_sublibraries_link ]);
    ]
