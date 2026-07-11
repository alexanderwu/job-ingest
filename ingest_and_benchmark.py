#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb>=1.0", "pyarrow>=17"]
# ///
"""
Ingest *.json.gz job listings into DuckDB and SQLite, incrementally, and
benchmark load performance across the two backends.

Parsing, schema validation, flattening, and the SQLite load are all done by
the Rust sidecar in fastingest/ (a serde mirror of job_schema.py; see
verify_parity.py for the equivalence check). It keeps a manifest of file
mtimes/sizes so repeat runs only parse new/changed files, upserts those rows
straight into SQLite (rusqlite, no Arrow round-trip), and emits the same
rows as an Arrow IPC file (<out-dir>/jobs.arrow), which this script reads,
upserts into DuckDB, and then deletes -- it's just the handoff format
between the two processes, not a durable output.

A full rebuild happens with --full, or automatically on the first run (or
whenever the manifest, SQLite, or DuckDB file is missing). Deleted input
files are ignored: their rows linger until the next full run.

Usage:
    uv run ingest_and_benchmark.py \
        --json-dir /path/to/json_dir \
        --out-dir /path/to/output_dir \
        [--limit 2000] [--full] [--parquet]

Outputs:
    <out-dir>/jobs.duckdb
    <out-dir>/jobs.sqlite
    <out-dir>/ingest_manifest.json   change-detection state (Rust-owned)
    <out-dir>/jobs.parquet           with --parquet (zstd)
    A benchmark summary printed to stdout.

Design notes:
    - Parsing/validation is done once and shared by both backends, so the
      DB-load benchmark isolates database write performance rather than
      JSON/validation overhead.
    - Both DBs key on requisition_id and are fed with INSERT OR REPLACE, so
      incremental runs are idempotent upserts.
    - With --parquet: on full runs the Rust sidecar writes jobs.parquet
      directly (it has the full corpus in hand); on incremental runs its
      output would only be the delta, so DuckDB exports the full table
      instead (COPY ... TO, zstd).
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


def run_fastingest(
    json_dir: Path,
    out_dir: Path,
    limit: int | None,
    full: bool,
    parquet: bool,
) -> tuple[pa.Table, dict]:
    """Run the fastingest sidecar and load its (delta) Arrow output.

    Returns the Arrow table of rows parsed this run plus the sidecar's stats
    dict. The caller times the whole call, so the reported parse stage stays
    an honest end-to-end number (subprocess spawn + parse + Arrow read).
    """
    binary = ensure_rust_binary()
    cmd = [str(binary), str(json_dir), str(out_dir)]
    if limit:
        cmd += ["--limit", str(limit)]
    if full:
        cmd += ["--full"]
    if parquet:
        cmd += ["--parquet"]

    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    stats = json.loads(proc.stdout.strip().splitlines()[-1])
    arrow_path = out_dir / "jobs.arrow"
    # Read via a plain file handle (not pa.ipc.open_file's default mmap) so
    # no memory mapping outlives this call -- on Windows a lingering mmap
    # would block the unlink() below even after the reader is closed.
    with open(arrow_path, "rb") as f, pa.ipc.open_file(f) as reader:
        table = reader.read_all()
    arrow_path.unlink()

    for sample in stats.get("error_samples", []):
        print(f"    error: {sample}", flush=True)
    assert table.num_rows == stats["ok"], (table.num_rows, stats)
    return table, stats


def load_duckdb(
    table: pa.Table, db_path: Path, full: bool
) -> tuple[float, int]:
    """Upsert the (delta) Arrow table; returns (insert_sec, total_rows)."""
    if full:
        for suffix in ("", ".wal"):
            p = Path(str(db_path) + suffix)
            if p.exists():
                p.unlink()
    fresh = not db_path.exists()
    con = duckdb.connect(str(db_path))
    if fresh:
        con.execute(DUCKDB_DDL)
    col_list = ", ".join(f'"{c}"' for c in COLUMNS)
    con.register("arrow_jobs", table)

    t0 = time.perf_counter()
    if table.num_rows:
        con.execute(
            f"INSERT OR REPLACE INTO jobs ({col_list}) "
            f"SELECT {col_list} FROM arrow_jobs"
        )
    elapsed = time.perf_counter() - t0
    total_rows = con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    con.close()
    return elapsed, total_rows


def export_parquet(db_path: Path, parquet_path: Path) -> float:
    """Full-table Parquet export via DuckDB (used on incremental runs)."""
    con = duckdb.connect(str(db_path), read_only=True)
    t0 = time.perf_counter()
    con.execute(
        f"COPY jobs TO '{parquet_path.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    elapsed = time.perf_counter() - t0
    con.close()
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-dir", type=Path, default='data/raw/json/')
    parser.add_argument("--out-dir", type=Path, default='data/processed/')
    parser.add_argument("--limit", type=int, default=None,
                         help="Only process the first N files (for quick runs).")
    parser.add_argument("--full", action="store_true",
                         help="Force a full rebuild (ignore the manifest and "
                              "recreate both databases).")
    parser.add_argument("--parquet", action="store_true",
                         help="Also write <out-dir>/jobs.parquet (full corpus, "
                              "zstd).")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = args.out_dir / "jobs.sqlite"
    duckdb_path = args.out_dir / "jobs.duckdb"
    parquet_path = args.out_dir / "jobs.parquet"
    manifest_path = args.out_dir / "ingest_manifest.json"

    # The Arrow output is a delta on incremental runs, so anything that needs
    # the full corpus (a missing DB) forces a full rebuild for everyone.
    full = (args.full or not manifest_path.exists()
            or not sqlite_path.exists() or not duckdb_path.exists())

    print(f"Parsing + validating with fastingest (Rust, "
          f"{'full rebuild' if full else 'incremental'})...", flush=True)
    t0 = time.perf_counter()
    table, stats = run_fastingest(
        args.json_dir, args.out_dir, args.limit, full,
        parquet=args.parquet and full,
    )
    # Full wall time minus the sidecar's own SQLite stage = parse + Arrow read.
    parse_time = time.perf_counter() - t0 - stats["sqlite_insert_sec"]
    n_rows = table.num_rows
    print(f"  {stats['files']} files: {stats['skipped']} unchanged (skipped), "
          f"{stats['parsed']} parsed, {stats['errors']} validation errors",
          flush=True)
    rate = f" ({n_rows / parse_time:.0f} rows/sec)" if n_rows else ""
    print(f"  parse + validate took {parse_time:.2f}s{rate}", flush=True)

    sqlite_time = stats["sqlite_insert_sec"]
    print(f"SQLite (written by fastingest): upserted {n_rows} rows in "
          f"{sqlite_time:.2f}s, table now {stats['sqlite_total_rows']} rows",
          flush=True)

    print("Loading into DuckDB...", flush=True)
    duckdb_time, duckdb_total = load_duckdb(table, duckdb_path, full)
    print(f"  upserted {n_rows} rows in {duckdb_time:.2f}s, "
          f"table now {duckdb_total} rows", flush=True)

    parquet_time = None
    if args.parquet:
        if full:
            parquet_time = 0.0  # written by the sidecar, inside parse_time
            print("Parquet written by fastingest (full corpus).", flush=True)
        else:
            print("Exporting Parquet (full table via DuckDB)...", flush=True)
            parquet_time = export_parquet(duckdb_path, parquet_path)
            print(f"  exported in {parquet_time:.2f}s", flush=True)

    sqlite_size = sqlite_path.stat().st_size / (1024 * 1024)
    duckdb_size = duckdb_path.stat().st_size / (1024 * 1024)

    def rows_per_sec(t: float) -> str:
        return f"{n_rows / t:.0f}" if n_rows and t > 0 else "-"

    print("\n" + "=" * 72, flush=True)
    print(f"BENCHMARK SUMMARY ({'full rebuild' if full else 'incremental'}: "
          f"{n_rows} rows this run)", flush=True)
    print("=" * 72, flush=True)
    print(f"{'stage':<28}{'time (s)':>12}{'rows/sec':>15}{'file size':>15}", flush=True)
    print(f"{'parse + validate':<28}{parse_time:>12.2f}{rows_per_sec(parse_time):>15}{'':>15}", flush=True)
    print(f"{'sqlite insert':<28}{sqlite_time:>12.2f}{rows_per_sec(sqlite_time):>15}{sqlite_size:>13.1f}MB", flush=True)
    print(f"{'duckdb insert':<28}{duckdb_time:>12.2f}{rows_per_sec(duckdb_time):>15}{duckdb_size:>13.1f}MB", flush=True)
    if args.parquet and parquet_path.exists():
        parquet_size = parquet_path.stat().st_size / (1024 * 1024)
        t = parquet_time or 0.0
        print(f"{'parquet export':<28}{t:>12.2f}{'-':>15}{parquet_size:>13.1f}MB", flush=True)
    total_sqlite = parse_time + sqlite_time
    total_duckdb = parse_time + duckdb_time
    print(f"{'sqlite total (parse+load)':<28}{total_sqlite:>12.2f}{rows_per_sec(total_sqlite):>15}{'':>15}", flush=True)
    print(f"{'duckdb total (parse+load)':<28}{total_duckdb:>12.2f}{rows_per_sec(total_duckdb):>15}{'':>15}", flush=True)

    if n_rows and duckdb_time > 0 and sqlite_time > 0:
        speedup = sqlite_time / duckdb_time
        faster = "DuckDB" if speedup >= 1 else "SQLite"
        factor = speedup if speedup >= 1 else 1 / speedup
        print(f"\n{faster} was {factor:.2f}x faster than "
              f"{'SQLite' if faster == 'DuckDB' else 'DuckDB'} for the insert stage.", flush=True)

    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
