(** UUID v7 wrapper over Uuidm *)

type t = Uuidm.t

let gen =
  Uuidm.v7_monotonic_gen
    ~now_ms:(fun () -> Int64.of_float (Unix.gettimeofday () *. 1000.))
    (Random.State.make_self_init ())

let rec v () : t =
  match gen () with
  | Some uuid -> uuid
  | None ->
      (* There is a limit of 2^12 UUIDs per millisecond *)
      (* If we hit that limit, we can just wait a bit and try again *)
      Unix.sleepf 0.001;
      v ()

let to_bytes (id : t) : bytes = Uuidm.to_binary_string id |> Bytes.of_string

let of_bytes (bs : bytes) : t option =
  if Bytes.length bs <> 16 then None
  else
    let s = Bytes.to_string bs in
    match Uuidm.of_binary_string s with
    | Some uuid when Uuidm.version uuid = 7 -> Some uuid
    | _ -> None

let to_string (id : t) : string = Uuidm.to_string id

let of_string (str : string) : t option =
  match Uuidm.of_string str with Some uuid when Uuidm.version uuid = 7 -> Some uuid | _ -> None

let compare = Uuidm.compare
let pp = Uuidm.pp
