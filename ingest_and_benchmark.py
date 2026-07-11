#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb>=1.0", "pyarrow>=17"]
# ///
"""
Ingest cache/json/*.json.gz job listings into DuckDB and SQLite, and
benchmark load performance across the two backends.

Parsing, schema validation, and flattening are done by the Rust sidecar in
fastingest/ (a serde mirror of job_schema.py; see verify_parity.py for the
equivalence check), which gunzips + parses all files in parallel and emits
an Arrow IPC file. This replaced a single-threaded Python/Pydantic parse
phase that took ~53s for ~16k files; the Rust phase takes well under a
second warm.

Usage:
    uv run ingest_and_benchmark.py \
        --json-dir /path/to/cache/json \
        --out-dir /path/to/output_dir \
        [--limit 2000] [--batch-size 1000]

Outputs:
    <out-dir>/jobs.arrow
    <out-dir>/jobs.duckdb
    <out-dir>/jobs.sqlite
    A benchmark summary printed to stdout.

Design notes:
    - Parsing/validation is done once and shared by both backends, so the
      DB-load benchmark isolates database write performance rather than
      JSON/validation overhead.
    - SQLite uses batched `executemany` in a single transaction; DuckDB
      bulk-ingests the registered Arrow table via INSERT INTO ... SELECT.
    - Deeply nested / variable-shape data (job_information, the ~90-field
      v5_processed_job_data block, enriched_company_data) is stored as JSON
      text columns; a set of "hot" fields likely to be queried/filtered on
      are also flattened into typed columns.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc

import duckdb

CRATE_DIR = Path(__file__).resolve().parent / "fastingest"
RUST_BIN = CRATE_DIR / "target" / "release" / (
    "fastingest.exe" if os.name == "nt" else "fastingest"
)

COLUMNS = [
    "id", "source", "board_token", "apply_url", "requisition_id",
    "collapse_key", "is_expired", "title", "job_title_raw", "description",
    "core_job_title", "job_category", "seniority_level", "role_type",
    "workplace_type", "formatted_workplace_location", "workplace_countries",
    "min_industry_and_role_yoe", "yearly_min_compensation",
    "yearly_max_compensation", "listed_compensation_currency",
    "technical_tools", "estimated_publish_date", "company_name",
    "company_website", "enriched_status", "nb_employees", "year_founded",
    "latitude", "longitude", "job_information_json",
    "v5_processed_job_data_json", "enriched_company_data_json",
]

DUCKDB_DDL = """
CREATE TABLE jobs (
    id VARCHAR,
    source VARCHAR,
    board_token VARCHAR,
    apply_url VARCHAR,
    requisition_id VARCHAR PRIMARY KEY,
    collapse_key VARCHAR,
    is_expired BOOLEAN,
    title VARCHAR,
    job_title_raw VARCHAR,
    description VARCHAR,
    core_job_title VARCHAR,
    job_category VARCHAR,
    seniority_level VARCHAR,
    role_type VARCHAR,
    workplace_type VARCHAR,
    formatted_workplace_location VARCHAR,
    workplace_countries VARCHAR,
    min_industry_and_role_yoe DOUBLE,
    yearly_min_compensation DOUBLE,
    yearly_max_compensation DOUBLE,
    listed_compensation_currency VARCHAR,
    technical_tools VARCHAR,
    estimated_publish_date VARCHAR,
    company_name VARCHAR,
    company_website VARCHAR,
    enriched_status VARCHAR,
    nb_employees INTEGER,
    year_founded INTEGER,
    latitude DOUBLE,
    longitude DOUBLE,
    job_information_json VARCHAR,
    v5_processed_job_data_json VARCHAR,
    enriched_company_data_json VARCHAR
);
"""

SQLITE_DDL = """
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
"""


def ensure_rust_binary() -> Path:
    """Build fastingest if the binary is missing or older than its sources."""
    sources = list(CRATE_DIR.glob("src/*.rs")) + [CRATE_DIR / "Cargo.toml"]
    src_mtime = max(p.stat().st_mtime for p in sources)
    if RUST_BIN.exists() and RUST_BIN.stat().st_mtime >= src_mtime:
        return RUST_BIN
    if shutil.which("cargo") is None:
        sys.exit(
            f"fastingest binary missing/stale and cargo not found; run "
            f"`cargo build --release` in {CRATE_DIR}"
        )
    print("Building fastingest (cargo build --release)...", flush=True)
    subprocess.run(["cargo", "build", "--release"], cwd=CRATE_DIR, check=True)
    return RUST_BIN


def parse_and_validate(
    json_dir: Path, arrow_path: Path, limit: int | None
) -> tuple[pa.Table, int, float]:
    """Run the fastingest sidecar and load its Arrow output.

    The reported time is the full subprocess wall time plus the Arrow read —
    an honest end-to-end replacement of the old parse phase (build time,
    if any, is excluded).
    """
    binary = ensure_rust_binary()
    cmd = [str(binary), str(json_dir), str(arrow_path)]
    if limit:
        cmd += ["--limit", str(limit)]

    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    stats = json.loads(proc.stdout.strip().splitlines()[-1])
    with pa.ipc.open_file(arrow_path) as reader:
        table = reader.read_all()
    elapsed = time.perf_counter() - t0

    for sample in stats.get("error_samples", []):
        print(f"    error: {sample}", flush=True)
    assert table.num_rows == stats["ok"], (table.num_rows, stats)
    return table, stats["errors"], elapsed


def table_to_rows(table: pa.Table) -> list[tuple]:
    """Arrow table -> row tuples in COLUMNS order (for sqlite executemany)."""
    return list(zip(*(table.column(name).to_pylist() for name in COLUMNS)))


def load_sqlite(rows: list[tuple], db_path: Path, batch_size: int) -> float:
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = OFF")
    con.execute(SQLITE_DDL)
    placeholders = ",".join(["?"] * len(COLUMNS))
    insert_sql = f"INSERT INTO jobs VALUES ({placeholders})"

    t0 = time.perf_counter()
    con.execute("BEGIN")
    for i in range(0, len(rows), batch_size):
        con.executemany(insert_sql, rows[i:i + batch_size])
    con.commit()
    elapsed = time.perf_counter() - t0
    con.close()
    return elapsed


def load_duckdb(table: pa.Table, db_path: Path) -> float:
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    con.execute(DUCKDB_DDL)
    col_list = ", ".join(f'"{c}"' for c in COLUMNS)
    con.register("arrow_jobs", table)

    t0 = time.perf_counter()
    con.execute(f"INSERT INTO jobs ({col_list}) SELECT {col_list} FROM arrow_jobs")
    elapsed = time.perf_counter() - t0
    con.close()
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-dir", type=Path, default='data/raw/json/')
    parser.add_argument("--out-dir", type=Path, default='data/processed/')
    parser.add_argument("--limit", type=int, default=None,
                         help="Only process the first N files (for quick runs).")
    parser.add_argument("--batch-size", type=int, default=1000,
                         help="Insert batch size (SQLite only; DuckDB bulk-"
                              "ingests the Arrow table in one statement).")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    print("Parsing + validating with fastingest (Rust)...", flush=True)
    arrow_path = args.out_dir / "jobs.arrow"
    table, errors, parse_time = parse_and_validate(args.json_dir, arrow_path, args.limit)
    n_rows = table.num_rows
    print(f"  parsed {n_rows} rows ({errors} validation errors) in "
          f"{parse_time:.2f}s ({n_rows / parse_time:.0f} rows/sec)", flush=True)

    print("Loading into SQLite...", flush=True)
    rows = table_to_rows(table)
    sqlite_path = args.out_dir / "jobs.sqlite"
    sqlite_time = load_sqlite(rows, sqlite_path, args.batch_size)
    print(f"  inserted {len(rows)} rows in {sqlite_time:.2f}s "
          f"({len(rows) / sqlite_time:.0f} rows/sec)", flush=True)

    print("Loading into DuckDB...", flush=True)
    duckdb_path = args.out_dir / "jobs.duckdb"
    duckdb_time = load_duckdb(table, duckdb_path)
    print(f"  inserted {n_rows} rows in {duckdb_time:.2f}s "
          f"({n_rows / duckdb_time:.0f} rows/sec)", flush=True)

    sqlite_size = sqlite_path.stat().st_size / (1024 * 1024)
    duckdb_size = duckdb_path.stat().st_size / (1024 * 1024)

    print("\n" + "=" * 72, flush=True)
    print("BENCHMARK SUMMARY", flush=True)
    print("=" * 72, flush=True)
    print(f"{'stage':<28}{'time (s)':>12}{'rows/sec':>15}{'file size':>15}", flush=True)
    print(f"{'parse + validate':<28}{parse_time:>12.2f}{n_rows / parse_time:>15.0f}{'':>15}", flush=True)
    print(f"{'sqlite insert':<28}{sqlite_time:>12.2f}{n_rows / sqlite_time:>15.0f}{sqlite_size:>13.1f}MB", flush=True)
    print(f"{'duckdb insert':<28}{duckdb_time:>12.2f}{n_rows / duckdb_time:>15.0f}{duckdb_size:>13.1f}MB", flush=True)
    total_sqlite = parse_time + sqlite_time
    total_duckdb = parse_time + duckdb_time
    print(f"{'sqlite total (parse+load)':<28}{total_sqlite:>12.2f}{n_rows / total_sqlite:>15.0f}{'':>15}", flush=True)
    print(f"{'duckdb total (parse+load)':<28}{total_duckdb:>12.2f}{n_rows / total_duckdb:>15.0f}{'':>15}", flush=True)

    speedup = sqlite_time / duckdb_time if duckdb_time else float("inf")
    faster = "DuckDB" if speedup >= 1 else "SQLite"
    factor = speedup if speedup >= 1 else 1 / speedup
    print(f"\n{faster} was {factor:.2f}x faster than "
          f"{'SQLite' if faster == 'DuckDB' else 'DuckDB'} for the insert stage.", flush=True)

    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
