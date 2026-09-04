# job-ingest — fast job-listing ingest benchmark

Ingests gzipped HiringCafe job listings from `data/raw/json/*.json.gz` into
SQLite and DuckDB with typed schema validation. Ingest is incremental: repeat
runs parse and upsert only new or changed files.

Parsing and validation run in the `fastingest` Rust sidecar. It uses rayon for
parallel file processing, flate2 for decompression, and serde for typed JSON
decoding. On the current corpus of 24,548 files (198 MB compressed), a full
parse and validation takes about 0.33 seconds and a no-change incremental scan
takes less than 0.01 seconds.

## Architecture

```text
data/raw/json/*.json.gz
        │
        │  fastingest sidecar
        │  stat files → skip unchanged files → parallel gunzip
        │  → serde validation → flatten to 33-column rows
        │
        ├─→ SQLite jobs.sqlite
        │     INSERT OR REPLACE in one transaction via rusqlite
        │
        ├─→ jobs.parquet
        │     optional zstd output on full runs
        │
        └─→ jobs.arrow
              transient Arrow IPC handoff for this run's rows
                    │
                    │  ingest_and_benchmark.py
                    ├─→ DuckDB jobs.duckdb
                    └─→ jobs.parquet
                          incremental runs export the full DuckDB table
```

Both databases key on `requisition_id`, and incremental runs use idempotent
`INSERT OR REPLACE` upserts. The sidecar records each successfully parsed
file's modification time, size, and requisition ID in
`ingest_manifest.json`. Files that fail validation are reported and retried on
the next run.

A full rebuild happens with `--full`, on the first run, or whenever the
manifest or either database is missing. Deleted input files are intentionally
ignored, so their rows remain until the next full rebuild.

## Components

- `fastingest/src/schema.rs` is the canonical input schema. It handles source
  field aliases, optional values, list normalization, union values, and RFC
  3339 datetime validation.
- `fastingest/src/flatten.rs` converts validated jobs into the 33-column output
  row. Frequently queried values have typed columns; deeply nested data is
  retained in three JSON text columns.
- `fastingest/src/arrow_out.rs` writes the transient Arrow IPC handoff and
  optional zstd-compressed Parquet output.
- `fastingest/src/sqlite_out.rs` owns SQLite creation and incremental upserts.
- `fastingest/src/manifest.rs` owns change detection state.
- `fastingest/src/lib.rs` holds the pipeline itself (`run`), so the tests
  exercise the same code path the binary does; `main.rs` is a thin clap CLI
  over it that prints the stats line and maps fatal errors to an exit code.
- `src/job_ingest/ingest_and_benchmark.py` builds and invokes the sidecar,
  loads its Arrow output into DuckDB, handles incremental Parquet exports, and
  prints stage timings.

The sidecar exits successfully when individual files fail validation. It
counts those failures, prints up to ten samples, skips invalid rows, and leaves
the files out of the manifest so they are retried. Fatal directory, argument,
or output errors return a nonzero exit code.

## Usage

```powershell
# Incremental run. The first run automatically performs a full rebuild.
# Defaults: --json-dir data/raw/json --out-dir data/processed
uv run src/job_ingest/ingest_and_benchmark.py

# Force a full rebuild of SQLite and DuckDB.
uv run src/job_ingest/ingest_and_benchmark.py --full

# Also write data/processed/jobs.parquet with zstd compression.
uv run src/job_ingest/ingest_and_benchmark.py --parquet

# Process only the first 2,000 files for a quick run.
uv run src/job_ingest/ingest_and_benchmark.py --limit 2000

# The same commands are available through just.
just ingest --limit 2000
```

Outputs are written beneath `--out-dir`:

- `jobs.sqlite`
- `jobs.duckdb`
- `ingest_manifest.json`
- `jobs.parquet` when `--parquet` is supplied

`jobs.arrow` exists only while the Python wrapper transfers rows from the
sidecar into DuckDB and is deleted after loading.

## Requirements

- Python 3.13 or newer
- `uv`
- A Rust toolchain; MSVC is required on Windows

Python dependencies are declared in `pyproject.toml` (`just install`, or
`uv sync`). If the release sidecar binary is missing or older than its Rust
sources, the Python wrapper runs `cargo build --release` automatically.

The wrapper finds the crate by walking up from its own location looking for
`fastingest/Cargo.toml`. Set `FASTINGEST_DIR` to override that, or put a built
`fastingest` on `PATH` to skip the crate lookup entirely.

## Development

```powershell
just check      # format check, lint, strict mypy, pytest, cargo fmt/clippy/test
just test       # Python + Rust tests
just bless      # re-record the golden snapshots after a schema change
```

`fastingest/tests/golden.rs` pins the flattened output of a small fixture
corpus (`fastingest/tests/fixtures/`, documented in its own README). `schema.rs`
is the only description of the input format, and the JSON blob columns are
stored as text — so reordering a struct field there silently rewrites every
stored row with no error anywhere. The snapshots exist to catch exactly that;
read the diff carefully whenever `just bless` changes one.

`tests/test_ingest_and_benchmark.py` covers the Python side: crate discovery,
the subprocess contract, the Arrow handoff, incremental skipping, and whether
`DUCKDB_DDL` still matches the sidecar's Arrow schema.

## Benchmark

Measurements below are full rebuilds over 24,548 valid files with no
validation errors.

| stage                     | Rust serde sidecar |
|---------------------------|-------------------:|
| parse + validate          | 0.33 s             |
| SQLite insert             | 1.47 s             |
| DuckDB insert             | 0.85 s             |
| DuckDB total (parse+load) | 1.18 s             |
| incremental, no changes   | <0.01 s            |

The SQLite and DuckDB insert rows are not a like-for-like comparison and no
winner is declared: SQLite is written row-wise by rusqlite inside the sidecar,
while DuckDB bulk-registers an Arrow table from Python. They measure two
different strategies in two different languages, one of them across a
subprocess boundary. Read them as the cost of each stage in this pipeline, not
as a benchmark of the two engines against each other.

The sidecar uses serde_json's `float_roundtrip` feature so parsed IEEE-754
values match Python float semantics. Full-run Parquet is written directly by
Rust; on incremental runs DuckDB exports its complete table so the Parquet
file always represents the full ingested corpus.
