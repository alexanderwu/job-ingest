//! Ingest manifest: maps each input file name to the (mtime, size) it had
//! when last successfully parsed, plus the requisition_id its row was stored
//! under. Lets incremental runs skip unchanged files and replace the old row
//! when a file's requisition_id changes. Files that fail validation are NOT
//! recorded, so they are retried (and re-reported) on every run.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Entry {
    pub mtime_ns: u64,
    pub size: u64,
    pub requisition_id: String,
}

pub type Manifest = HashMap<String, Entry>;

/// mtime (ns since epoch) and size for change detection. Errors (e.g. file
/// deleted mid-run) surface as None and the file is treated as changed.
pub fn stat(path: &Path) -> Option<(u64, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime_ns = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos() as u64;
    Some((mtime_ns, meta.len()))
}

pub fn load(path: &Path) -> Option<Manifest> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn save(path: &Path, manifest: &Manifest) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(manifest).expect("manifest serializes");
    std::fs::write(path, bytes)
}
