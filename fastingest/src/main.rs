//! fastingest — parallel gunzip + serde parse/validate + flatten of
//! *.json.gz job listings, with incremental ingest.
//!
//! Usage: fastingest <json_dir> <out_dir> [--limit N] [--full] [--parquet]
//!
//! Outputs in <out_dir>:
//!   jobs.arrow            Arrow IPC of the rows parsed THIS run (full corpus
//!                         on full runs, delta on incremental runs)
//!   jobs.sqlite           upserted in place (recreated on full runs)
//!   jobs.parquet          only with --parquet; same rows as jobs.arrow
//!   ingest_manifest.json  file -> (mtime, size, requisition_id) of the last
//!                         successful parse; drives change detection
//!
//! Incremental runs skip files whose mtime+size match the manifest. A full
//! rebuild happens with --full, or automatically when the manifest or the
//! SQLite database is missing. Deleted input files are ignored (rows linger
//! until the next full run).
//!
//! Prints one JSON stats line to stdout:
//!   {"files": N, "skipped": N, "parsed": N, "ok": N, "errors": N,
//!    "full": bool, "parse_sec": F, "sqlite_insert_sec": F,
//!    "sqlite_total_rows": N, "error_samples": [...]}
//!
//! Exit code is 0 even when individual files fail validation (they are
//! counted, skipped, and retried next run since they never enter the
//! manifest); nonzero only for fatal problems (bad args, unreadable dir,
//! output write failure).

mod arrow_out;
mod flatten;
mod manifest;
mod schema;
mod sqlite_out;

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
    let mut full = false;
    let mut parquet = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--limit" => {
                let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) else {
                    eprintln!("--limit requires a numeric argument");
                    return ExitCode::FAILURE;
                };
                limit = Some(v);
                i += 2;
            }
            "--full" => {
                full = true;
                i += 1;
            }
            "--parquet" => {
                parquet = true;
                i += 1;
            }
            _ => {
                positional.push(args[i].clone());
                i += 1;
            }
        }
    }
    let [json_dir, out_dir] = positional.as_slice() else {
        eprintln!("usage: fastingest <json_dir> <out_dir> [--limit N] [--full] [--parquet]");
        return ExitCode::FAILURE;
    };
    let out_dir = Path::new(out_dir);
    if let Err(e) = std::fs::create_dir_all(out_dir) {
        eprintln!("cannot create {}: {e}", out_dir.display());
        return ExitCode::FAILURE;
    }
    let arrow_path = out_dir.join("jobs.arrow");
    let sqlite_path = out_dir.join("jobs.sqlite");
    let parquet_path = out_dir.join("jobs.parquet");
    let manifest_path = out_dir.join("ingest_manifest.json");

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

    // Incremental only works against an existing manifest AND SQLite DB;
    // otherwise fall back to a full rebuild.
    let old_manifest = if full { None } else { manifest::load(&manifest_path) };
    if !full && (old_manifest.is_none() || !sqlite_path.exists()) {
        full = true;
    }
    let old_manifest = old_manifest.unwrap_or_default();

    // Stat everything up front; a file is parsed if we're in full mode, it's
    // new, its mtime+size changed, or its stat failed (treated as changed).
    let stats_by_file: Vec<Option<(u64, u64)>> =
        files.par_iter().map(|p| manifest::stat(p)).collect();
    let to_parse: Vec<usize> = (0..files.len())
        .filter(|&i| {
            if full {
                return true;
            }
            let name = files[i].file_name().and_then(|n| n.to_str());
            match (name.and_then(|n| old_manifest.get(n)), stats_by_file[i]) {
                (Some(entry), Some((mtime_ns, size))) => {
                    entry.mtime_ns != mtime_ns || entry.size != size
                }
                _ => true,
            }
        })
        .collect();
    let skipped = files.len() - to_parse.len();

    let t0 = Instant::now();
    let results: Vec<Result<FlatRow, String>> = to_parse
        .par_iter()
        .map(|&i| process_file(&files[i]))
        .collect();
    let parse_sec = t0.elapsed().as_secs_f64();

    let mut rows = Vec::with_capacity(results.len());
    let mut errors = 0usize;
    let mut error_samples = Vec::new();
    // Changed files whose requisition_id changed: the old row must go, or it
    // would linger next to the upserted new one.
    let mut stale_rids: Vec<String> = Vec::new();
    let mut new_manifest = if full {
        manifest::Manifest::new()
    } else {
        old_manifest.clone()
    };
    for (&i, result) in to_parse.iter().zip(results) {
        let path = &files[i];
        match result {
            Ok(row) => {
                if let (Some(name), Some((mtime_ns, size))) = (
                    path.file_name().and_then(|n| n.to_str()),
                    stats_by_file[i],
                ) {
                    if let Some(old) = old_manifest.get(name) {
                        if old.requisition_id != row.requisition_id {
                            stale_rids.push(old.requisition_id.clone());
                        }
                    }
                    new_manifest.insert(
                        name.to_string(),
                        manifest::Entry {
                            mtime_ns,
                            size,
                            requisition_id: row.requisition_id.clone(),
                        },
                    );
                }
                rows.push(row);
            }
            Err(e) => {
                errors += 1;
                if error_samples.len() < 10 {
                    error_samples.push(format!("{}: {}", path.display(), e));
                }
            }
        }
    }

    let batch = match arrow_out::build_batch(&rows) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("failed to build record batch: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = arrow_out::write_ipc(&batch, &arrow_path) {
        eprintln!("failed to write {}: {e}", arrow_path.display());
        return ExitCode::FAILURE;
    }
    if parquet {
        if let Err(e) = arrow_out::write_parquet(&batch, &parquet_path) {
            eprintln!("failed to write {}: {e}", parquet_path.display());
            return ExitCode::FAILURE;
        }
    }

    let sqlite_stats = match sqlite_out::write(&sqlite_path, &rows, full, &stale_rids) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to write {}: {e}", sqlite_path.display());
            return ExitCode::FAILURE;
        }
    };

    // Saved only after all outputs succeed, so a failed run is fully retried.
    if let Err(e) = manifest::save(&manifest_path, &new_manifest) {
        eprintln!("failed to write {}: {e}", manifest_path.display());
        return ExitCode::FAILURE;
    }

    println!(
        "{}",
        serde_json::json!({
            "files": files.len(),
            "skipped": skipped,
            "parsed": to_parse.len(),
            "ok": rows.len(),
            "errors": errors,
            "full": full,
            "parse_sec": parse_sec,
            "sqlite_insert_sec": sqlite_stats.insert_sec,
            "sqlite_total_rows": sqlite_stats.total_rows,
            "error_samples": error_samples,
        })
    );
    ExitCode::SUCCESS
}
