(** crdtsync CLI entrypoint.

    Subcommands (per ARCHITECTURE.md, CLI section): serve — run the sync server snapshot — export /
    import snapshots migrate — generate / verify / apply schema migrations audit — query the op log
    compact — run log compaction

    None of these are implemented yet. See KANBAN.md, SERVER-7. *)

let () =
  match Sys.argv with
  | [| _ |] | [| _; "help" |] | [| _; "--help" |] | [| _; "-h" |] ->
      print_endline "crdtsync — self-hosted collaborative sync engine";
      print_endline "";
      print_endline "Usage: crdtsync <subcommand> [args...]";
      print_endline "";
      print_endline "Subcommands (planned, none implemented yet):";
      print_endline "  serve       run the sync server";
      print_endline "  snapshot    export / import snapshots";
      print_endline "  migrate     generate / verify / apply schema migrations";
      print_endline "  audit       query the op log";
      print_endline "  compact     run log compaction";
      print_endline "";
      print_endline "See https://crdtsync.com and ARCHITECTURE.md."
  | [| _; "serve" |] -> Crdtsync_server.run ()
  | _ ->
      prerr_endline "crdtsync: unknown subcommand or not yet implemented";
      prerr_endline "Run `crdtsync --help` for the list of planned subcommands.";
      exit 1
