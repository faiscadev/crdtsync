//! `crdtsync-replay` — a read-only operator tool for point-in-time inspection of
//! a persisted room.
//!
//! It reads the same durable data the server persists under `CRDTSYNC_DATA_DIR`
//! — a per-room op log plus optional compaction snapshot — and reconstructs the
//! room's exact merged state as of any past server sequence, or diffs two such
//! points. It never writes: no watermark advances, no durable byte changes. Use
//! it to debug divergence, inspect a suspect sequence, or investigate recovery.
//!
//! ```text
//! # State as of a sequence — summary to stdout, optional raw state to a file:
//! crdtsync-replay --data-dir ./data my-room --to 42
//! crdtsync-replay --data-dir ./data my-room --to 42 --out state-42.bin
//!
//! # Diff two points — rendered changes to stdout, optional encoded diff to a file:
//! crdtsync-replay --data-dir ./data my-room --from 10 --to 42 --diff
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use crdtsync_core::diff::render;
use crdtsync_core::ClientId;
use crdtsync_server::replay::{diff_at, load_room, reconstruct_at, DEFAULT_REPLAY_SERVER};
use crdtsync_server::store::Store;

/// Read-only point-in-time reconstruction and diff for a persisted crdtsync room.
#[derive(Parser)]
#[command(name = "crdtsync-replay", version, about)]
struct Cli {
    /// The room's durable data directory — the server's `CRDTSYNC_DATA_DIR`.
    /// Falls back to that environment variable when the flag is absent.
    #[arg(long, env = "CRDTSYNC_DATA_DIR")]
    data_dir: PathBuf,

    /// The room to replay, as a UTF-8 name (or hex bytes with `--hex-room`).
    room: String,

    /// Interpret `room` as hex-encoded raw bytes rather than a UTF-8 name.
    #[arg(long)]
    hex_room: bool,

    /// The target sequence: the state as of this point, or the newer end of a diff.
    #[arg(long)]
    to: u64,

    /// Diff from this sequence to `--to`. Requires `--diff`.
    #[arg(long, requires = "diff")]
    from: Option<u64>,

    /// Diff `--from`..`--to` instead of reconstructing a single point.
    #[arg(long, requires = "from")]
    diff: bool,

    /// Write the raw encoded state (reconstruct mode) or encoded diff (diff mode)
    /// to this file, in addition to the stdout summary.
    #[arg(long)]
    out: Option<PathBuf>,

    /// The reconstructing node's replica id, as 32 hex digits. Only affects the
    /// leading replica id of an uncompacted room's encoded state; a
    /// snapshot-backed room pins its own. Defaults to a fixed placeholder.
    #[arg(long)]
    server_id: Option<String>,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("crdtsync-replay: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    let room = if cli.hex_room {
        decode_hex(&cli.room).ok_or_else(|| format!("invalid hex room: {}", cli.room))?
    } else {
        cli.room.clone().into_bytes()
    };
    let server = ClientId::from_bytes(parse_server_id(cli.server_id.as_deref())?);

    let store = Store::open(&cli.data_dir).map_err(|e| format!("opening store: {e}"))?;
    let log = load_room(&store, &room)
        .map_err(|e| format!("loading room: {e}"))?
        .ok_or_else(|| format!("no such room: {}", cli.room))?;

    if cli.diff {
        let from = cli.from.expect("--diff requires --from (enforced by clap)");
        let changes =
            diff_at(&log, &room, from, cli.to, server).map_err(|e| format!("diff: {e}"))?;
        println!(
            "room {} — diff seq {} -> {} ({} change{})",
            cli.room,
            from,
            cli.to,
            changes.len(),
            if changes.len() == 1 { "" } else { "s" },
        );
        for line in render(&changes) {
            println!("{line}");
        }
        if let Some(path) = cli.out {
            let bytes = crdtsync_core::diff::encode_changes(&changes);
            std::fs::write(&path, &bytes)
                .map_err(|e| format!("writing {}: {e}", path.display()))?;
            eprintln!(
                "wrote {} bytes of encoded diff to {}",
                bytes.len(),
                path.display()
            );
        }
    } else {
        let got =
            reconstruct_at(&log, &room, cli.to, server).map_err(|e| format!("replay: {e}"))?;
        println!(
            "room {} @ seq {} (floor {}, head {}): {} bytes of state",
            cli.room,
            got.seq,
            got.floor,
            got.head,
            got.state.len(),
        );
        if let Some(path) = cli.out {
            std::fs::write(&path, &got.state)
                .map_err(|e| format!("writing {}: {e}", path.display()))?;
            eprintln!(
                "wrote {} bytes of encoded state to {}",
                got.state.len(),
                path.display()
            );
        }
    }
    Ok(())
}

/// Parse an optional 32-hex-digit server id, defaulting to the placeholder.
fn parse_server_id(arg: Option<&str>) -> Result<[u8; 16], String> {
    match arg {
        None => Ok(DEFAULT_REPLAY_SERVER),
        Some(hex) => {
            let bytes = decode_hex(hex).ok_or_else(|| format!("invalid hex server id: {hex}"))?;
            bytes
                .try_into()
                .map_err(|_| "server id must be exactly 32 hex digits (16 bytes)".to_string())
        }
    }
}

/// Decode an even-length hex string to bytes, or `None` if malformed.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
