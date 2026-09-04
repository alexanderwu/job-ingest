#!/usr/bin/env python3
"""
Ingest *.json.gz job listings into DuckDB and SQLite, incrementally, and
benchmark load performance across the two backends.

The fastingest Rust sidecar gunzips, parses, validates, and flattens files
with serde. It upserts SQLite itself and hands this script the current
run's rows as a transient Arrow IPC file. This script loads those rows into
DuckDB, then deletes the handoff file.

The sidecar keeps an ingest manifest (<out-dir>/ingest_manifest.json) of
file mtimes and sizes, so repeat runs only parse new or changed files. A
full rebuild happens with --full, on the first run, or whenever the
manifest, SQLite, or DuckDB file is missing. Deleted input files are
ignored: their rows linger until the next full run.

Usage:
    uv run src/job_ingest/ingest_and_benchmark.py \
        --json-dir /path/to/json_dir \
        --out-dir /path/to/output_dir \
        [--limit 2000] [--full] [--parquet]

    or, equivalently: just ingest [--limit 2000] [--full] [--parquet]

Outputs:
    <out-dir>/jobs.duckdb
    <out-dir>/jobs.sqlite
    <out-dir>/ingest_manifest.json   change-detection state
    <out-dir>/jobs.parquet           with --parquet (zstd)
    A benchmark summary printed to stdout.

Library API:
    run_ingest() is the frontend-agnostic core: it returns a typed
    IngestResult and raises IngestError instead of calling sys.exit(), so
    the dashboard (job_ingest.dashboard) can run an ingest in-process and
    render the numbers. main() is a thin printer over it. Progress lines
    that only make sense *during* a run are emitted through the on_event
    callback; the summary block is derived from IngestResult afterwards.

Design notes:
    - Parsing and validation happen once and are shared by both backends, so
      the per-stage timings below exclude JSON/validation overhead.
    - The SQLite and DuckDB insert timings are NOT a like-for-like
      comparison and no winner is declared: SQLite is written row-wise by
      rusqlite inside the sidecar, while DuckDB bulk-registers an Arrow
      table from Python. They measure two different strategies in two
      different languages, one of them across a subprocess boundary. Read
      them as "what each stage costs in this pipeline", not as a benchmark
      of the two engines against each other.
    - Both DBs key on requisition_id and are fed with INSERT OR REPLACE, so
      incremental runs are idempotent upserts.
    - With --parquet: on full runs the sidecar writes jobs.parquet directly
      (it has the full corpus in hand); on incremental runs its output
      would only be the delta, so DuckDB exports the full table instead
      (COPY ... TO, zstd).
    - Deeply nested / variable-shape data (job_information, the ~90-field
      v5_processed_job_data block, enriched_company_data) is stored as JSON
      text columns; a set of "hot" fields likely to be queried/filtered on
      are also flattened into typed columns.
    - The sidecar commits SQLite and the manifest before Python commits
      DuckDB. <out-dir>/.ingest-incomplete bridges that window; see
      run_ingest().
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.ipc

BIN_NAME = "fastingest.exe" if os.name == "nt" else "fastingest"

#: Left behind while an ingest is mid-flight. See run_ingest().
INCOMPLETE_MARKER = ".ingest-incomplete"


class IngestError(RuntimeError):
    """An ingest could not be completed.

    This is the single failure type of the library API. Every operational
    failure -- a missing sidecar, a cargo build that fails, a malformed
    stats line, an unreadable Arrow handoff, a filesystem error, a DuckDB
    lock or query error -- is normalised into it with the original cause
    chained, so a caller (the CLI, the dashboard) needs exactly one except
    clause and never has to catch SystemExit.
    """


def _noop(_msg: str) -> None:
    """Default on_event sink; a module-level def keeps mypy happy."""


def find_crate_dir() -> Path | None:
    """Locate the fastingest crate, or None if this is an installed copy.

    $FASTINGEST_DIR wins; otherwise walk up from this file looking for
    fastingest/Cargo.toml. Walking up (rather than a fixed parents[N]) keeps
    this working from the src/ layout, from a checkout nested one level
    deeper, and from a tests/ subdirectory -- and correctly finds nothing
    when the package is installed into site-packages without the crate.
    """
    if env_dir := os.environ.get("FASTINGEST_DIR"):
        return Path(env_dir)
    for parent in Path(__file__).resolve().parents:
        if (parent / "fastingest" / "Cargo.toml").is_file():
            return parent / "fastingest"
    return None


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


def ddl_columns() -> tuple[str, ...]:
    """The jobs table's column names, in DDL order.

    DUCKDB_DDL is the single source of truth for the corpus schema. Parsing
    it here (rather than re-listing the columns) keeps stats.py's column
    whitelist and the Arrow-schema drift test honest against one definition.
    Column lines are exactly the four-space-indented lines of the CREATE
    TABLE body.
    """
    return tuple(
        line.strip().split()[0]
        for line in DUCKDB_DDL.splitlines()
        if line.startswith("    ")
    )


def ensure_rust_binary(on_event: Callable[[str], None] = _noop) -> Path:
    """Locate fastingest, rebuilding it if it is missing or stale.

    Falls back to an installed `fastingest` on PATH when the crate source
    isn't alongside this file (e.g. the package installed on its own).

    A cargo build takes tens of seconds, so the "Building..." notice goes
    through on_event: a frontend that swallowed it would look hung.
    """
    crate_dir = find_crate_dir()
    if crate_dir is None or not crate_dir.is_dir():
        if on_path := shutil.which("fastingest"):
            return Path(on_path)
        raise IngestError(
            "fastingest crate not found and no `fastingest` on PATH. Run from "
            "a checkout, or set FASTINGEST_DIR to the crate directory."
        )

    binary = crate_dir / "target" / "release" / BIN_NAME
    try:
        sources = list(crate_dir.rglob("src/**/*.rs")) + [crate_dir / "Cargo.toml"]
        src_mtime = max((p.stat().st_mtime for p in sources), default=0.0)
        fresh_enough = binary.exists() and binary.stat().st_mtime >= src_mtime
    except OSError as e:
        raise IngestError(
            f"cannot stat the fastingest crate in {crate_dir}: {e}"
        ) from e
    if fresh_enough:
        return binary
    if shutil.which("cargo") is None:
        if on_path := shutil.which("fastingest"):
            return Path(on_path)
        raise IngestError(
            f"fastingest binary missing/stale and cargo not found; run "
            f"`cargo build --release` in {crate_dir}"
        )
    on_event("Building fastingest (cargo build --release)...")
    try:
        subprocess.run(["cargo", "build", "--release"], cwd=crate_dir, check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        raise IngestError(f"cargo build --release failed in {crate_dir}: {e}") from e
    if not binary.exists():
        raise IngestError(f"cargo build succeeded but {binary} is missing")
    return binary


def run_fastingest(
    json_dir: Path,
    out_dir: Path,
    limit: int | None,
    full: bool,
    parquet: bool,
    on_event: Callable[[str], None] = _noop,
) -> tuple[pa.Table, dict[str, Any]]:
    """Run the fastingest sidecar and load its (delta) Arrow output.

    Returns the Arrow table of rows parsed this run plus the sidecar's stats
    dict. The caller times the whole call, so the reported parse stage stays
    an honest end-to-end number (subprocess spawn + parse + Arrow read).

    Raises IngestError for every failure mode: a missing/unbuildable
    sidecar, a non-zero exit, an empty or unparseable stats line, an
    unreadable Arrow handoff, and a row count that disagrees with the stats.
    """
    binary = ensure_rust_binary(on_event)
    cmd = [str(binary), str(json_dir), str(out_dir)]
    if limit:
        cmd += ["--limit", str(limit)]
    if full:
        cmd += ["--full"]
    if parquet:
        cmd += ["--parquet"]

    # check=False: CalledProcessError doesn't carry stderr in its message, so
    # a fatal sidecar error would otherwise surface as a bare traceback with
    # the actual diagnostic swallowed by capture_output.
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except OSError as e:
        raise IngestError(f"could not execute {binary}: {e}") from e
    if proc.returncode != 0:
        raise IngestError(
            f"fastingest failed (exit {proc.returncode}):\n"
            f"{proc.stderr.strip() or '<no stderr>'}"
        )
    lines = proc.stdout.strip().splitlines()
    if not lines:
        raise IngestError(
            f"fastingest produced no stats line on stdout.\n"
            f"stderr: {proc.stderr.strip() or '<empty>'}"
        )
    try:
        stats = json.loads(lines[-1])
    except json.JSONDecodeError as e:
        raise IngestError(
            f"fastingest's last stdout line is not valid JSON: {lines[-1]!r}"
        ) from e
    if not isinstance(stats, dict):
        raise IngestError(
            f"fastingest's stats line is not a JSON object: {lines[-1]!r}"
        )
    arrow_path = out_dir / "jobs.arrow"
    # Read via a plain file handle (not pa.ipc.open_file's default mmap) so
    # no memory mapping outlives this call -- on Windows a lingering mmap
    # would block the unlink() below even after the reader is closed.
    try:
        with open(arrow_path, "rb") as f, pa.ipc.open_file(f) as reader:
            table = reader.read_all()
    except (OSError, pa.ArrowInvalid) as e:
        raise IngestError(f"cannot read the Arrow handoff at {arrow_path}: {e}") from e
    try:
        arrow_path.unlink()
    except OSError as e:
        raise IngestError(f"cannot remove the Arrow handoff {arrow_path}: {e}") from e

    # Not an assert: this is library API now, and `python -O` strips asserts.
    ok = _stat_int(stats, "ok")
    if table.num_rows != ok:
        raise IngestError(
            f"fastingest handed back {table.num_rows} Arrow rows but reported "
            f"{ok} successful files; the handoff and the stats disagree."
        )
    return table, stats


def _stat_int(stats: dict[str, Any], key: str) -> int:
    try:
        return int(stats[key])
    except (KeyError, TypeError, ValueError) as e:
        raise IngestError(
            f"fastingest stats are missing/invalid {key!r}: {stats}"
        ) from e


def _stat_float(stats: dict[str, Any], key: str) -> float:
    try:
        return float(stats[key])
    except (KeyError, TypeError, ValueError) as e:
        raise IngestError(
            f"fastingest stats are missing/invalid {key!r}: {stats}"
        ) from e


# --------------------------------------------------------------------------
# Shared DuckDB load / Parquet export / benchmark report
# --------------------------------------------------------------------------


def load_duckdb(table: pa.Table, db_path: Path, full: bool) -> tuple[float, int]:
    """Upsert the (delta) Arrow table; returns (insert_sec, total_rows).

    Opens jobs.duckdb read-write. DuckDB's file lock is exclusive, so no
    other connection to this database -- including a read-only one held by
    stats.py in the same process -- may be open across this call. That is
    exactly why stats.py never caches a connection.
    """
    try:
        if full:
            for suffix in ("", ".wal"):
                p = Path(str(db_path) + suffix)
                if p.exists():
                    p.unlink()
        fresh = not db_path.exists()
    except OSError as e:
        raise IngestError(f"cannot reset {db_path}: {e}") from e

    try:
        con = duckdb.connect(str(db_path))
    except duckdb.Error as e:
        raise IngestError(f"cannot open {db_path}: {e}") from e
    try:
        if fresh:
            con.execute(DUCKDB_DDL)
        col_list = ", ".join(f'"{name}"' for name in table.column_names)
        con.register("arrow_jobs", table)

        t0 = time.perf_counter()
        if table.num_rows:
            con.execute(
                f"INSERT OR REPLACE INTO jobs ({col_list}) "
                f"SELECT {col_list} FROM arrow_jobs"
            )
        elapsed = time.perf_counter() - t0
        # fetchone() is Optional per the DB-API; COUNT(*) always returns a row,
        # but an `assert` here would be stripped under `python -O`.
        row = con.execute("SELECT COUNT(*) FROM jobs").fetchone()
        total_rows = int(row[0]) if row else 0
    except duckdb.Error as e:
        raise IngestError(f"DuckDB load into {db_path} failed: {e}") from e
    finally:
        # finally, not a trailing close(): a failed query must not leave the
        # exclusive file lock held for the rest of the process's life.
        con.close()
    return elapsed, total_rows


def export_parquet(db_path: Path, parquet_path: Path) -> float:
    """Full-table Parquet export via DuckDB (used on incremental runs)."""
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error as e:
        raise IngestError(f"cannot open {db_path} read-only: {e}") from e
    try:
        t0 = time.perf_counter()
        con.execute(
            f"COPY jobs TO '{parquet_path.as_posix()}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        elapsed = time.perf_counter() - t0
    except duckdb.Error as e:
        raise IngestError(f"Parquet export to {parquet_path} failed: {e}") from e
    finally:
        con.close()
    return elapsed


# --------------------------------------------------------------------------
# Frontend-agnostic core
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Everything one ingest run produced, as data.

    Purely run state: it says what this run did, never what the corpus
    contains. Corpus questions belong to stats.py, which never runs the
    pipeline.
    """

    full: bool
    files: int
    skipped: int
    parsed: int
    ok: int
    errors: int
    error_samples: tuple[str, ...]
    #: Python wall time for the sidecar call and Arrow read, minus
    #: sqlite_insert_sec -- i.e. the CLI's long-standing "parse + validate"
    #: number, which also absorbs subprocess spawn, the Arrow handoff and a
    #: full run's Parquet write. It is deliberately NOT the sidecar's
    #: narrower serde-only stats["parse_sec"].
    parse_sec: float
    sqlite_insert_sec: float
    sqlite_total_rows: int
    duckdb_insert_sec: float
    duckdb_total_rows: int
    #: None when --parquet was not requested. 0.0 on a full run, where the
    #: sidecar writes the file inside parse_sec.
    parquet_sec: float | None
    sqlite_bytes: int
    duckdb_bytes: int
    #: None when no Parquet file was requested or none exists.
    parquet_bytes: int | None


def run_ingest(
    json_dir: Path,
    out_dir: Path,
    *,
    limit: int | None = None,
    full: bool = False,
    parquet: bool = False,
    on_event: Callable[[str], None] = _noop,
) -> IngestResult:
    """Parse, load and (optionally) export one ingest run.

    Emits through on_event the handful of lines that only make sense while
    the run is in flight -- the interleaving of "Loading into DuckDB..."
    with the result lines cannot be reconstructed from the return value.
    Everything the summary block needs is in IngestResult instead.

    Raises IngestError, never SystemExit.

    Crash consistency
    -----------------
    The sidecar commits SQLite *and* ingest_manifest.json before this
    function loads DuckDB. If DuckDB is locked, or the process dies in
    between, the manifest would tell the next incremental run to skip a
    delta DuckDB never saw -- a silent split brain. So
    <out-dir>/.ingest-incomplete is written before the sidecar runs and
    removed only once DuckDB (and any requested Parquet export) has
    succeeded. Finding it forces a full rebuild. Dying after the writes but
    before the removal costs one extra full rebuild, which is the cheap
    direction to be wrong in.
    """
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise IngestError(f"cannot create {out_dir}: {e}") from e

    sqlite_path = out_dir / "jobs.sqlite"
    duckdb_path = out_dir / "jobs.duckdb"
    parquet_path = out_dir / "jobs.parquet"
    manifest_path = out_dir / "ingest_manifest.json"
    marker_path = out_dir / INCOMPLETE_MARKER

    # The engine output is a delta on incremental runs, so anything that
    # needs the full corpus (a missing DB, an interrupted previous run)
    # forces a full rebuild for everyone.
    full = (
        full
        or not manifest_path.exists()
        or not sqlite_path.exists()
        or not duckdb_path.exists()
        or marker_path.exists()
    )

    try:
        marker_path.write_text("in progress\n", encoding="utf-8")
    except OSError as e:
        raise IngestError(f"cannot write {marker_path}: {e}") from e

    on_event(
        "Parsing + validating with fastingest (Rust/serde), "
        f"{'full rebuild' if full else 'incremental'}..."
    )
    t0 = time.perf_counter()
    table, stats = run_fastingest(
        json_dir,
        out_dir,
        limit,
        full,
        parquet=parquet and full,
        on_event=on_event,
    )
    error_samples = tuple(str(s) for s in stats.get("error_samples", []))
    for sample in error_samples:
        on_event(f"    error: {sample}")
    sqlite_sec = _stat_float(stats, "sqlite_insert_sec")
    # Full wall time minus the engine's own SQLite stage = parse + everything
    # else the engine does (Arrow handoff, full-run Parquet).
    parse_sec = time.perf_counter() - t0 - sqlite_sec
    n_rows = table.num_rows
    files = _stat_int(stats, "files")
    skipped = _stat_int(stats, "skipped")
    parsed = _stat_int(stats, "parsed")
    errors = _stat_int(stats, "errors")
    sqlite_total_rows = _stat_int(stats, "sqlite_total_rows")
    on_event(
        f"  {files} files: {skipped} unchanged (skipped), "
        f"{parsed} parsed, {errors} validation errors"
    )
    rate = f" ({n_rows / parse_sec:.0f} rows/sec)" if n_rows else ""
    on_event(f"  parse + validate took {parse_sec:.2f}s{rate}")
    on_event(
        f"SQLite (written by fastingest): upserted {n_rows} "
        f"rows in {sqlite_sec:.2f}s, table now {sqlite_total_rows} rows"
    )

    on_event("Loading into DuckDB...")
    duckdb_sec, duckdb_total = load_duckdb(table, duckdb_path, full)
    on_event(
        f"  upserted {n_rows} rows in {duckdb_sec:.2f}s, table now {duckdb_total} rows"
    )

    parquet_sec: float | None = None
    if parquet:
        if full:
            parquet_sec = 0.0  # written by the engine, inside parse_sec
            on_event("Parquet written by fastingest (full corpus).")
        else:
            on_event("Exporting Parquet (full table via DuckDB)...")
            parquet_sec = export_parquet(duckdb_path, parquet_path)
            on_event(f"  exported in {parquet_sec:.2f}s")

    try:
        sqlite_bytes = sqlite_path.stat().st_size
        duckdb_bytes = duckdb_path.stat().st_size
        parquet_bytes = (
            parquet_path.stat().st_size if parquet and parquet_path.exists() else None
        )
    except OSError as e:
        raise IngestError(f"cannot stat the ingest outputs in {out_dir}: {e}") from e

    # Everything is durable; the recovery marker has done its job. Left in
    # place by any exception above, including a hard crash.
    try:
        marker_path.unlink(missing_ok=True)
    except OSError as e:
        raise IngestError(f"cannot remove {marker_path}: {e}") from e

    return IngestResult(
        full=full,
        files=files,
        skipped=skipped,
        parsed=parsed,
        ok=n_rows,
        errors=errors,
        error_samples=error_samples,
        parse_sec=parse_sec,
        sqlite_insert_sec=sqlite_sec,
        sqlite_total_rows=sqlite_total_rows,
        duckdb_insert_sec=duckdb_sec,
        duckdb_total_rows=duckdb_total,
        parquet_sec=parquet_sec,
        sqlite_bytes=sqlite_bytes,
        duckdb_bytes=duckdb_bytes,
        parquet_bytes=parquet_bytes,
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

_MB = 1024 * 1024


def _print_summary(result: IngestResult) -> None:
    """The 72-column BENCHMARK SUMMARY block, derived purely from data."""
    n_rows = result.ok
    sqlite_size = result.sqlite_bytes / _MB
    duckdb_size = result.duckdb_bytes / _MB

    def rows_per_sec(t: float) -> str:
        return f"{n_rows / t:.0f}" if n_rows and t > 0 else "-"

    print("\n" + "=" * 72, flush=True)
    print(
        f"BENCHMARK SUMMARY ({'full rebuild' if result.full else 'incremental'}: "
        f"{n_rows} rows this run)",
        flush=True,
    )
    print("=" * 72, flush=True)
    print(f"{'stage':<28}{'time (s)':>12}{'rows/sec':>15}{'file size':>15}", flush=True)
    print(
        f"{'parse + validate':<28}{result.parse_sec:>12.2f}"
        f"{rows_per_sec(result.parse_sec):>15}{'':>15}",
        flush=True,
    )
    print(
        f"{'sqlite insert':<28}{result.sqlite_insert_sec:>12.2f}"
        f"{rows_per_sec(result.sqlite_insert_sec):>15}{sqlite_size:>13.1f}MB",
        flush=True,
    )
    print(
        f"{'duckdb insert':<28}{result.duckdb_insert_sec:>12.2f}"
        f"{rows_per_sec(result.duckdb_insert_sec):>15}{duckdb_size:>13.1f}MB",
        flush=True,
    )
    if result.parquet_bytes is not None:
        parquet_size = result.parquet_bytes / _MB
        t = result.parquet_sec or 0.0
        print(
            f"{'parquet export':<28}{t:>12.2f}{'-':>15}{parquet_size:>13.1f}MB",
            flush=True,
        )
    total_sqlite = result.parse_sec + result.sqlite_insert_sec
    total_duckdb = result.parse_sec + result.duckdb_insert_sec
    print(
        f"{'sqlite total (parse+load)':<28}{total_sqlite:>12.2f}"
        f"{rows_per_sec(total_sqlite):>15}{'':>15}",
        flush=True,
    )
    print(
        f"{'duckdb total (parse+load)':<28}{total_duckdb:>12.2f}"
        f"{rows_per_sec(total_duckdb):>15}{'':>15}",
        flush=True,
    )

    # No SQLite-vs-DuckDB verdict is printed: the two insert stages use
    # different strategies in different languages (see Design notes), so a
    # head-to-head ratio would read as a benchmark result it can't support.

    print("\nDONE", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-dir", type=Path, default="data/raw/json/")
    parser.add_argument("--out-dir", type=Path, default="data/processed/")
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

    try:
        result = run_ingest(
            args.json_dir,
            args.out_dir,
            limit=args.limit,
            full=args.full,
            parquet=args.parquet,
            on_event=lambda msg: print(msg, flush=True),
        )
    except IngestError as e:
        # sys.exit(str) prints to stderr and exits 1 -- what the sys.exit()
        # calls scattered through the library used to do.
        sys.exit(str(e))
    _print_summary(result)


if __name__ == "__main__":
    main()
