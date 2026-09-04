//! Direct SQLite output via rusqlite (bundled): rows go straight from the
//! flattened structs into the database, with no Arrow round-trip.
//!
//! Column order in the DDL below must match the `FlatRow` field order in
//! `flatten.rs` — the INSERT binds positionally.

use std::path::Path;
use std::time::Instant;

use rusqlite::{params, Connection};

use crate::flatten::FlatRow;

const DDL: &str = "
CREATE TABLE jobs (
    id TEXT,
    source TEXT,
    board_token TEXT,
    apply_url TEXT,
    requisition_id TEXT PRIMARY KEY,
    collapse_key TEXT,
    is_expired INTEGER,
    title TEXT,
    job_title_raw TEXT,
    description TEXT,
    core_job_title TEXT,
    job_category TEXT,
    seniority_level TEXT,
    role_type TEXT,
    workplace_type TEXT,
    formatted_workplace_location TEXT,
    workplace_countries TEXT,
    min_industry_and_role_yoe REAL,
    yearly_min_compensation REAL,
    yearly_max_compensation REAL,
    listed_compensation_currency TEXT,
    technical_tools TEXT,
    estimated_publish_date TEXT,
    company_name TEXT,
    company_website TEXT,
    enriched_status TEXT,
    nb_employees INTEGER,
    year_founded INTEGER,
    latitude REAL,
    longitude REAL,
    job_information_json TEXT,
    v5_processed_job_data_json TEXT,
    enriched_company_data_json TEXT
);
";

pub struct SqliteStats {
    pub insert_sec: f64,
    pub total_rows: i64,
}

/// Full mode: recreate the DB from scratch. Incremental: upsert `rows` into
/// the existing DB (creating it if missing) and delete `stale_rids` first —
/// the previous requisition_ids of changed files whose id changed, so the
/// old row doesn't linger next to the new one.
pub fn write(
    path: &Path,
    rows: &[FlatRow],
    full: bool,
    stale_rids: &[String],
) -> rusqlite::Result<SqliteStats> {
    if full {
        for suffix in ["", "-wal", "-shm"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            let _ = std::fs::remove_file(&p);
        }
    }
    let fresh = !path.exists();
    let mut con = Connection::open(path)?;
    con.pragma_update(None, "journal_mode", "WAL")?;
    con.pragma_update(None, "synchronous", "OFF")?;
    if fresh {
        con.execute_batch(DDL)?;
    }

    let t0 = Instant::now();
    let tx = con.transaction()?;
    {
        for rid in stale_rids {
            tx.execute("DELETE FROM jobs WHERE requisition_id = ?1", [rid])?;
        }
        let mut stmt = tx.prepare(concat!(
            "INSERT OR REPLACE INTO jobs VALUES (",
            "?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,",
            "?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33)",
        ))?;
        for r in rows {
            stmt.execute(params![
                r.id,
                r.source,
                r.board_token,
                r.apply_url,
                r.requisition_id,
                r.collapse_key,
                r.is_expired,
                r.title,
                r.job_title_raw,
                r.description,
                r.core_job_title,
                r.job_category,
                r.seniority_level,
                r.role_type,
                r.workplace_type,
                r.formatted_workplace_location,
                r.workplace_countries,
                r.min_industry_and_role_yoe,
                r.yearly_min_compensation,
                r.yearly_max_compensation,
                r.listed_compensation_currency,
                r.technical_tools,
                r.estimated_publish_date,
                r.company_name,
                r.company_website,
                r.enriched_status,
                r.nb_employees,
                r.year_founded,
                r.latitude,
                r.longitude,
                r.job_information_json,
                r.v5_processed_job_data_json,
                r.enriched_company_data_json,
            ])?;
        }
    }
    tx.commit()?;
    let insert_sec = t0.elapsed().as_secs_f64();

    let total_rows: i64 = con.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0))?;
    Ok(SqliteStats {
        insert_sec,
        total_rows,
    })
}
