#!/usr/bin/env python3
"""
Ingest *.json.gz job listings into DuckDB and SQLite, incrementally, and
benchmark load performance across the two backends.

Two interchangeable parse/validate/load engines, selected with --engine:

  msgspec (default)  Pure Python: gzip -> msgspec Struct decode+validate
                     (job_schema.py) -> flatten -> SQLite via sqlite3 and
                     DuckDB via an in-process Arrow table. --workers N
                     fans the parsing out over a ProcessPoolExecutor.

  rust               The fastingest/ sidecar (a serde mirror of
                     job_schema.py; see verify_parity.py for the
                     equivalence check). It upserts SQLite itself and hands
                     the rows to this script as an Arrow IPC file
                     (<out-dir>/jobs.arrow), which is read, upserted into
                     DuckDB, and deleted -- a transient handoff, not an
                     output.

Both engines keep the same manifest (<out-dir>/ingest_manifest.json) of
file mtimes/sizes, so repeat runs only parse new/changed files and you can
switch engines between runs. A full rebuild happens with --full, or
automatically on the first run (or whenever the manifest, SQLite, or
DuckDB file is missing). Deleted input files are ignored: their rows
linger until the next full run.

Usage:
    uv run ingest_and_benchmark.py \
        --json-dir /path/to/json_dir \
        --out-dir /path/to/output_dir \
        [--engine msgspec|rust] [--workers N] \
        [--limit 2000] [--full] [--parquet]

Outputs:
    <out-dir>/jobs.duckdb
    <out-dir>/jobs.sqlite
    <out-dir>/ingest_manifest.json   change-detection state
    <out-dir>/jobs.parquet           with --parquet (zstd)
    A benchmark summary printed to stdout.

Design notes:
    - Parsing/validation is done once and shared by both backends, so the
      DB-load benchmark isolates database write performance rather than
      JSON/validation overhead.
    - Both DBs key on requisition_id and are fed with INSERT OR REPLACE, so
      incremental runs are idempotent upserts.
    - With --parquet: on full runs the engine writes jobs.parquet directly
      (it has the full corpus in hand); on incremental runs its output
      would only be the delta, so DuckDB exports the full table instead
      (COPY ... TO, zstd).
    - Deeply nested / variable-shape data (job_information, the ~90-field
      v5_processed_job_data block, enriched_company_data) is stored as JSON
      text columns; a set of "hot" fields likely to be queried/filtered on
      are also flattened into typed columns.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import zlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import msgspec

import pyarrow as pa
import pyarrow.ipc
import pyarrow.parquet as pq

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_schema import COLUMNS, decode_page, flatten  # noqa: E402

CRATE_DIR = Path(__file__).resolve().parent / "fastingest"
RUST_BIN = (
    CRATE_DIR
    / "target"
    / "release"
    / ("fastingest.exe" if os.name == "nt" else "fastingest")
)

ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("source", pa.string()),
        ("board_token", pa.string()),
        ("apply_url", pa.string()),
        ("requisition_id", pa.string()),
        ("collapse_key", pa.string()),
        ("is_expired", pa.bool_()),
        ("title", pa.string()),
        ("job_title_raw", pa.string()),
        ("description", pa.string()),
        ("core_job_title", pa.string()),
        ("job_category", pa.string()),
        ("seniority_level", pa.string()),
        ("role_type", pa.string()),
        ("workplace_type", pa.string()),
        ("formatted_workplace_location", pa.string()),
        ("workplace_countries", pa.string()),
        ("min_industry_and_role_yoe", pa.float64()),
        ("yearly_min_compensation", pa.float64()),
        ("yearly_max_compensation", pa.float64()),
        ("listed_compensation_currency", pa.string()),
        ("technical_tools", pa.string()),
        ("estimated_publish_date", pa.string()),
        ("company_name", pa.string()),
        ("company_website", pa.string()),
        ("enriched_status", pa.string()),
        ("nb_employees", pa.int64()),
        ("year_founded", pa.int64()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("job_information_json", pa.string()),
        ("v5_processed_job_data_json", pa.string()),
        ("enriched_company_data_json", pa.string()),
    ]
)

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

# Mirror of fastingest/src/sqlite_out.rs (which itself mirrors the original
# Python loader).
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

_RID_INDEX = COLUMNS.index("requisition_id")


# --------------------------------------------------------------------------
# msgspec engine (Python mirror of the fastingest pipeline)
# --------------------------------------------------------------------------


def _parse_file(path_str: str) -> tuple[tuple | None, str | None]:
    """gunzip + decode + flatten one file; returns (row, None) or (None, err).

    Top-level so ProcessPoolExecutor workers can import it by reference.
    """
    try:
        raw = gzip.decompress(Path(path_str).read_bytes())
    except (OSError, EOFError, zlib.error) as e:
        return None, f"{path_str}: gunzip: {e}"
    try:
        return flatten(decode_page(raw)), None
    except msgspec.DecodeError as e:  # ValidationError is a subclass
        return None, f"{path_str}: parse/validate: {e}"


def _load_manifest(path: Path) -> dict | None:
    """Same JSON shape as fastingest/src/manifest.rs:
    {filename: {mtime_ns, size, requisition_id}}; None if missing/corrupt."""
    try:
        return json.loads(path.read_bytes())
    except (OSError, ValueError):
        return None


def _write_sqlite(
    path: Path, rows: list[tuple], full: bool, stale_rids: list[str]
) -> tuple[float, int]:
    """Mirror of sqlite_out.rs: same DDL, pragmas, and one-transaction upsert."""
    if full:
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(path) + suffix)
            if p.exists():
                p.unlink()
    fresh = not path.exists()
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    if fresh:
        con.executescript(SQLITE_DDL)
        con.commit()

    t0 = time.perf_counter()
    with con:
        for rid in stale_rids:
            con.execute("DELETE FROM jobs WHERE requisition_id = ?", (rid,))
        con.executemany(
            f"INSERT OR REPLACE INTO jobs VALUES ({','.join('?' * len(COLUMNS))})",
            rows,
        )
    insert_sec = time.perf_counter() - t0

    total_rows = con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    con.close()
    return insert_sec, total_rows


def run_msgspec_ingest(
    json_dir: Path,
    out_dir: Path,
    limit: int | None,
    full: bool,
    parquet: bool,
    workers: int,
) -> tuple[pa.Table, dict]:
    """Pure-Python engine: manifest-driven incremental parse (msgspec) +
    SQLite upsert + optional Parquet, returning the delta rows as an Arrow
    table for the DuckDB load. Same behavior and stats dict as the sidecar.
    """
    sqlite_path = out_dir / "jobs.sqlite"
    parquet_path = out_dir / "jobs.parquet"
    manifest_path = out_dir / "ingest_manifest.json"

    files = sorted(p for p in json_dir.iterdir() if p.name.endswith(".json.gz"))
    if limit:
        files = files[:limit]

    # Incremental only works against an existing manifest AND SQLite DB;
    # otherwise fall back to a full rebuild (mirrors the sidecar).
    old_manifest = None if full else _load_manifest(manifest_path)
    if not full and (old_manifest is None or not sqlite_path.exists()):
        full = True
    old_manifest = old_manifest or {}

    # Stat everything up front; a file is parsed if we're in full mode, it's
    # new, its mtime+size changed, or its stat failed (treated as changed).
    def stat(p: Path) -> tuple[int, int] | None:
        try:
            st = p.stat()
            return st.st_mtime_ns, st.st_size
        except OSError:
            return None

    stats_by_file = [stat(p) for p in files]

    def changed(i: int) -> bool:
        entry = old_manifest.get(files[i].name)
        if entry is None or stats_by_file[i] is None:
            return True
        mtime_ns, size = stats_by_file[i]
        return entry["mtime_ns"] != mtime_ns or entry["size"] != size

    to_parse = [i for i in range(len(files)) if full or changed(i)]
    skipped = len(files) - len(to_parse)

    t0 = time.perf_counter()
    if workers > 1 and len(to_parse) > 1:
        chunksize = max(16, len(to_parse) // (workers * 8))
        with ProcessPoolExecutor(max_workers=workers) as ex:
            results = list(
                ex.map(
                    _parse_file,
                    (str(files[i]) for i in to_parse),
                    chunksize=chunksize,
                )
            )
    else:
        results = [_parse_file(str(files[i])) for i in to_parse]
    parse_sec = time.perf_counter() - t0

    rows: list[tuple] = []
    errors = 0
    error_samples: list[str] = []
    # Changed files whose requisition_id changed: the old row must go, or it
    # would linger next to the upserted new one.
    stale_rids: list[str] = []
    new_manifest = {} if full else dict(old_manifest)
    for i, (row, err) in zip(to_parse, results):
        if err is not None:
            errors += 1
            if len(error_samples) < 10:
                error_samples.append(err)
            continue
        name = files[i].name
        if stats_by_file[i] is not None:
            rid = row[_RID_INDEX]
            old = old_manifest.get(name)
            if old is not None and old["requisition_id"] != rid:
                stale_rids.append(old["requisition_id"])
            mtime_ns, size = stats_by_file[i]
            new_manifest[name] = {
                "mtime_ns": mtime_ns,
                "size": size,
                "requisition_id": rid,
            }
        rows.append(row)

    columns = list(zip(*rows)) if rows else [[]] * len(COLUMNS)
    table = pa.Table.from_arrays(
        [pa.array(col, type=f.type) for col, f in zip(columns, ARROW_SCHEMA)],
        schema=ARROW_SCHEMA,
    )

    if parquet:
        pq.write_table(table, parquet_path, compression="zstd")

    sqlite_insert_sec, sqlite_total_rows = _write_sqlite(
        sqlite_path, rows, full, stale_rids
    )

    # Saved only after all outputs succeed, so a failed run is fully retried.
    manifest_path.write_text(json.dumps(new_manifest))

    stats = {
        "files": len(files),
        "skipped": skipped,
        "parsed": len(to_parse),
        "ok": len(rows),
        "errors": errors,
        "full": full,
        "parse_sec": parse_sec,
        "sqlite_insert_sec": sqlite_insert_sec,
        "sqlite_total_rows": sqlite_total_rows,
        "error_samples": error_samples,
    }
    return table, stats


# --------------------------------------------------------------------------
# Rust engine (fastingest sidecar)
# --------------------------------------------------------------------------


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

    assert table.num_rows == stats["ok"], (table.num_rows, stats)
    return table, stats


# --------------------------------------------------------------------------
# Shared DuckDB load / Parquet export / benchmark report
# --------------------------------------------------------------------------


def load_duckdb(table: pa.Table, db_path: Path, full: bool) -> tuple[float, int]:
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
        f"COPY jobs TO '{parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    elapsed = time.perf_counter() - t0
    con.close()
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-dir", type=Path, default="data/raw/json/")
    parser.add_argument("--out-dir", type=Path, default="data/processed/")
    parser.add_argument(
        "--engine",
        choices=["msgspec", "rust"],
        default="msgspec",
        help="Parse/validate engine: msgspec Structs in "
        "Python (default) or the fastingest Rust "
        "sidecar.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parse with a ProcessPoolExecutor of N workers "
        "(msgspec engine only; default 1 = "
        "single-threaded).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N files (for quick runs).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force a full rebuild (ignore the manifest and recreate both databases).",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Also write <out-dir>/jobs.parquet (full corpus, zstd).",
    )
    args = parser.parse_args()

    if args.engine == "rust" and args.workers != 1:
        print(
            "note: --workers only applies to the msgspec engine; the Rust "
            "sidecar always parses with all cores (rayon).",
            flush=True,
        )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = args.out_dir / "jobs.sqlite"
    duckdb_path = args.out_dir / "jobs.duckdb"
    parquet_path = args.out_dir / "jobs.parquet"
    manifest_path = args.out_dir / "ingest_manifest.json"

    # The engine output is a delta on incremental runs, so anything that
    # needs the full corpus (a missing DB) forces a full rebuild for everyone.
    full = (
        args.full
        or not manifest_path.exists()
        or not sqlite_path.exists()
        or not duckdb_path.exists()
    )

    engine_desc = (
        "fastingest (Rust)"
        if args.engine == "rust"
        else f"msgspec (Python, {args.workers} worker"
        f"{'s' if args.workers != 1 else ''})"
    )
    print(
        f"Parsing + validating with {engine_desc}, "
        f"{'full rebuild' if full else 'incremental'}...",
        flush=True,
    )
    t0 = time.perf_counter()
    if args.engine == "rust":
        table, stats = run_fastingest(
            args.json_dir,
            args.out_dir,
            args.limit,
            full,
            parquet=args.parquet and full,
        )
    else:
        table, stats = run_msgspec_ingest(
            args.json_dir,
            args.out_dir,
            args.limit,
            full,
            parquet=args.parquet and full,
            workers=args.workers,
        )
    for sample in stats.get("error_samples", []):
        print(f"    error: {sample}", flush=True)
    # Full wall time minus the engine's own SQLite stage = parse + everything
    # else the engine does (Arrow handoff, full-run Parquet).
    parse_time = time.perf_counter() - t0 - stats["sqlite_insert_sec"]
    n_rows = table.num_rows
    print(
        f"  {stats['files']} files: {stats['skipped']} unchanged (skipped), "
        f"{stats['parsed']} parsed, {stats['errors']} validation errors",
        flush=True,
    )
    rate = f" ({n_rows / parse_time:.0f} rows/sec)" if n_rows else ""
    print(f"  parse + validate took {parse_time:.2f}s{rate}", flush=True)

    sqlite_time = stats["sqlite_insert_sec"]
    print(
        f"SQLite (written by the {args.engine} engine): upserted {n_rows} "
        f"rows in {sqlite_time:.2f}s, table now "
        f"{stats['sqlite_total_rows']} rows",
        flush=True,
    )

    print("Loading into DuckDB...", flush=True)
    duckdb_time, duckdb_total = load_duckdb(table, duckdb_path, full)
    print(
        f"  upserted {n_rows} rows in {duckdb_time:.2f}s, "
        f"table now {duckdb_total} rows",
        flush=True,
    )

    parquet_time = None
    if args.parquet:
        if full:
            parquet_time = 0.0  # written by the engine, inside parse_time
            print(
                f"Parquet written by the {args.engine} engine (full corpus).",
                flush=True,
            )
        else:
            print("Exporting Parquet (full table via DuckDB)...", flush=True)
            parquet_time = export_parquet(duckdb_path, parquet_path)
            print(f"  exported in {parquet_time:.2f}s", flush=True)

    sqlite_size = sqlite_path.stat().st_size / (1024 * 1024)
    duckdb_size = duckdb_path.stat().st_size / (1024 * 1024)

    def rows_per_sec(t: float) -> str:
        return f"{n_rows / t:.0f}" if n_rows and t > 0 else "-"

    print("\n" + "=" * 72, flush=True)
    print(
        f"BENCHMARK SUMMARY (engine={args.engine}, "
        f"{'full rebuild' if full else 'incremental'}: "
        f"{n_rows} rows this run)",
        flush=True,
    )
    print("=" * 72, flush=True)
    print(f"{'stage':<28}{'time (s)':>12}{'rows/sec':>15}{'file size':>15}", flush=True)
    print(
        f"{'parse + validate':<28}{parse_time:>12.2f}{rows_per_sec(parse_time):>15}{'':>15}",
        flush=True,
    )
    print(
        f"{'sqlite insert':<28}{sqlite_time:>12.2f}{rows_per_sec(sqlite_time):>15}{sqlite_size:>13.1f}MB",
        flush=True,
    )
    print(
        f"{'duckdb insert':<28}{duckdb_time:>12.2f}{rows_per_sec(duckdb_time):>15}{duckdb_size:>13.1f}MB",
        flush=True,
    )
    if args.parquet and parquet_path.exists():
        parquet_size = parquet_path.stat().st_size / (1024 * 1024)
        t = parquet_time or 0.0
        print(
            f"{'parquet export':<28}{t:>12.2f}{'-':>15}{parquet_size:>13.1f}MB",
            flush=True,
        )
    total_sqlite = parse_time + sqlite_time
    total_duckdb = parse_time + duckdb_time
    print(
        f"{'sqlite total (parse+load)':<28}{total_sqlite:>12.2f}{rows_per_sec(total_sqlite):>15}{'':>15}",
        flush=True,
    )
    print(
        f"{'duckdb total (parse+load)':<28}{total_duckdb:>12.2f}{rows_per_sec(total_duckdb):>15}{'':>15}",
        flush=True,
    )

    if n_rows and duckdb_time > 0 and sqlite_time > 0:
        speedup = sqlite_time / duckdb_time
        faster = "DuckDB" if speedup >= 1 else "SQLite"
        factor = speedup if speedup >= 1 else 1 / speedup
        print(
            f"\n{faster} was {factor:.2f}x faster than "
            f"{'SQLite' if faster == 'DuckDB' else 'DuckDB'} for the insert stage.",
            flush=True,
        )

    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
