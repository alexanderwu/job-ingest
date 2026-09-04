//! fastingest — parallel gunzip + serde parse/validate + flatten of
//! *.json.gz job listings, with incremental ingest.
//!
//! The binary in `main.rs` is a thin CLI over [`run`]; everything else lives
//! here so the integration tests in `tests/` can exercise the same code the
//! sidecar runs.
//!
//! Outputs in `out_dir`:
//!   jobs.arrow            Arrow IPC of the rows parsed THIS run (full corpus
//!                         on full runs, delta on incremental runs)
//!   jobs.sqlite           upserted in place (recreated on full runs)
//!   jobs.parquet          only with `parquet`; same rows as jobs.arrow
//!   ingest_manifest.json  file -> (mtime, size, requisition_id) of the last
//!                         successful parse; drives change detection
//!
//! Incremental runs skip files whose mtime+size match the manifest. A full
//! rebuild happens when requested, or automatically when the manifest or the
//! SQLite database is missing. Deleted input files are ignored (rows linger
//! until the next full run).
//!
//! Individual files that fail validation are counted, skipped, and reported;
//! they never enter the manifest, so they are retried next run. Only fatal
//! problems (unreadable directory, output write failure) are errors.

pub mod arrow_out;
pub mod flatten;
pub mod manifest;
pub mod schema;
pub mod sqlite_out;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;
use serde::Serialize;

use flatten::{flatten, FlatRow};

/// Fatal errors only. Per-file validation failures are reported in
/// [`Stats::errors`] rather than surfacing here.
pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

/// How many per-file validation errors to include in the stats output.
const MAX_ERROR_SAMPLES: usize = 10;

#[derive(Debug, Clone)]
pub struct Config {
    pub json_dir: PathBuf,
    pub out_dir: PathBuf,
    /// Process only the first N files (sorted by name).
    pub limit: Option<usize>,
    /// Force a full rebuild. Note that [`run`] may turn this on by itself when
    /// the manifest or SQLite database is missing.
    pub full: bool,
    pub parquet: bool,
}

/// The one JSON line the sidecar prints to stdout; the Python wrapper parses
/// these exact field names.
#[derive(Debug, Serialize)]
pub struct Stats {
    pub files: usize,
    pub skipped: usize,
    pub parsed: usize,
    pub ok: usize,
    pub errors: usize,
    /// Whether this run was *effectively* full, including the automatic
    /// upgrade when the manifest or SQLite database was missing.
    pub full: bool,
    pub parse_sec: f64,
    pub sqlite_insert_sec: f64,
    pub sqlite_total_rows: i64,
    pub error_samples: Vec<String>,
}

/// Read, gunzip, parse/validate, and flatten one file.
pub fn process_file(path: &Path) -> std::result::Result<FlatRow, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    Ok(flatten(decode_gzipped(&bytes)?))
}

/// Gunzip and parse/validate one in-memory `*.json.gz` document.
pub fn decode_gzipped(bytes: &[u8]) -> std::result::Result<schema::JobPage, String> {
    // gzip ISIZE trailer (uncompressed length mod 2^32) as a capacity hint.
    let hint = bytes
        .len()
        .checked_sub(4)
        .map(|i| u32::from_le_bytes(bytes[i..].try_into().unwrap()) as usize)
        .unwrap_or(0);
    let mut raw = Vec::with_capacity(hint);
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut raw)
        .map_err(|e| format!("gunzip: {e}"))?;
    serde_json::from_slice(&raw).map_err(|e| format!("parse/validate: {e}"))
}

/// List the `*.json.gz` inputs, sorted by name (matching `sorted(files)`) and
/// truncated to `limit`.
fn list_inputs(json_dir: &Path, limit: Option<usize>) -> Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(json_dir)
        .map_err(|e| format!("cannot read {}: {e}", json_dir.display()))?;
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".json.gz"))
        })
        .collect();
    // Sort before truncating so --limit selects a stable prefix.
    files.sort();
    if let Some(n) = limit {
        files.truncate(n);
    }
    Ok(files)
}

pub fn run(cfg: Config) -> Result<Stats> {
    let out_dir = cfg.out_dir.as_path();
    std::fs::create_dir_all(out_dir)
        .map_err(|e| format!("cannot create {}: {e}", out_dir.display()))?;
    let arrow_path = out_dir.join("jobs.arrow");
    let sqlite_path = out_dir.join("jobs.sqlite");
    let parquet_path = out_dir.join("jobs.parquet");
    let manifest_path = out_dir.join("ingest_manifest.json");

    let files = list_inputs(&cfg.json_dir, cfg.limit)?;

    // Incremental only works against an existing manifest AND SQLite DB;
    // otherwise fall back to a full rebuild.
    let old_manifest = if cfg.full {
        None
    } else {
        manifest::load(&manifest_path)
    };
    let full = cfg.full || old_manifest.is_none() || !sqlite_path.exists();
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
    let results: Vec<std::result::Result<FlatRow, String>> = to_parse
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
                if let (Some(name), Some((mtime_ns, size))) =
                    (path.file_name().and_then(|n| n.to_str()), stats_by_file[i])
                {
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
                if error_samples.len() < MAX_ERROR_SAMPLES {
                    error_samples.push(format!("{}: {}", path.display(), e));
                }
            }
        }
    }

    let batch =
        arrow_out::build_batch(&rows).map_err(|e| format!("failed to build record batch: {e}"))?;
    arrow_out::write_ipc(&batch, &arrow_path)
        .map_err(|e| format!("failed to write {}: {e}", arrow_path.display()))?;
    if cfg.parquet {
        arrow_out::write_parquet(&batch, &parquet_path)
            .map_err(|e| format!("failed to write {}: {e}", parquet_path.display()))?;
    }

    let sqlite_stats = sqlite_out::write(&sqlite_path, &rows, full, &stale_rids)
        .map_err(|e| format!("failed to write {}: {e}", sqlite_path.display()))?;

    // Saved only after all outputs succeed, so a failed run is fully retried.
    manifest::save(&manifest_path, &new_manifest)
        .map_err(|e| format!("failed to write {}: {e}", manifest_path.display()))?;

    Ok(Stats {
        files: files.len(),
        skipped,
        parsed: to_parse.len(),
        ok: rows.len(),
        errors,
        full,
        parse_sec,
        sqlite_insert_sec: sqlite_stats.insert_sec,
        sqlite_total_rows: sqlite_stats.total_rows,
        error_samples,
    })
}
