//! fastingest — parallel gunzip + serde parse/validate + flatten of
//! cache/json/*.json.gz job listings, emitting an Arrow IPC file.
//!
//! Usage: fastingest <json_dir> <out_arrow> [--limit N]
//!
//! Prints one JSON stats line to stdout:
//!   {"files": N, "ok": N, "errors": N, "elapsed_sec": F, "error_samples": [...]}
//!
//! Exit code is 0 even when individual files fail validation (they are
//! counted and skipped, matching the original Python behavior); nonzero only
//! for fatal problems (bad args, unreadable dir, output write failure).

mod arrow_out;
mod flatten;
mod schema;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use rayon::prelude::*;

use flatten::{flatten, FlatRow};

fn process_file(path: &Path) -> Result<FlatRow, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    // gzip ISIZE trailer (uncompressed length mod 2^32) as a capacity hint.
    let hint = bytes
        .len()
        .checked_sub(4)
        .map(|i| u32::from_le_bytes(bytes[i..].try_into().unwrap()) as usize)
        .unwrap_or(0);
    let mut raw = Vec::with_capacity(hint);
    flate2::read::GzDecoder::new(&bytes[..])
        .read_to_end(&mut raw)
        .map_err(|e| format!("gunzip: {e}"))?;
    let page: schema::JobPage =
        serde_json::from_slice(&raw).map_err(|e| format!("parse/validate: {e}"))?;
    Ok(flatten(page))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut positional = Vec::new();
    let mut limit: Option<usize> = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--limit" {
            let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) else {
                eprintln!("--limit requires a numeric argument");
                return ExitCode::FAILURE;
            };
            limit = Some(v);
            i += 2;
        } else {
            positional.push(args[i].clone());
            i += 1;
        }
    }
    let [json_dir, out_arrow] = positional.as_slice() else {
        eprintln!("usage: fastingest <json_dir> <out_arrow> [--limit N]");
        return ExitCode::FAILURE;
    };

    let entries = match std::fs::read_dir(json_dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("cannot read {json_dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".json.gz"))
        })
        .collect();
    // Sort before truncating to match Python's `sorted(files)[:limit]`.
    files.sort();
    if let Some(n) = limit {
        files.truncate(n);
    }

    let t0 = Instant::now();
    let results: Vec<Result<FlatRow, String>> =
        files.par_iter().map(|p| process_file(p)).collect();

    let mut rows = Vec::with_capacity(results.len());
    let mut errors = 0usize;
    let mut error_samples = Vec::new();
    for (path, result) in files.iter().zip(results) {
        match result {
            Ok(row) => rows.push(row),
            Err(e) => {
                errors += 1;
                if error_samples.len() < 10 {
                    error_samples.push(format!("{}: {}", path.display(), e));
                }
            }
        }
    }

    if let Err(e) = arrow_out::write_ipc(&rows, Path::new(out_arrow)) {
        eprintln!("failed to write {out_arrow}: {e}");
        return ExitCode::FAILURE;
    }

    println!(
        "{}",
        serde_json::json!({
            "files": files.len(),
            "ok": rows.len(),
            "errors": errors,
            "elapsed_sec": t0.elapsed().as_secs_f64(),
            "error_samples": error_samples,
        })
    );
    ExitCode::SUCCESS
}
