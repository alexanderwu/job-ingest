# job-ingest — fast job-listing ingest benchmark

Ingests ~24.5k gzipped JSON job listings (`data/raw/json/*.json.gz`, 198 MB
compressed) into **SQLite** and **DuckDB**, with full schema
validation, and benchmarks the two backends. Ingest is **incremental**:
repeat runs parse and upsert only new/changed files.

The parse+validate phase originally ran single-threaded in Python
(stdlib `json` + Pydantic v2) and took **~53 s**. There are now two
interchangeable engines, selected with `--engine`:

- **msgspec (default)** — the schema as `msgspec.Struct`s in
  `job_schema.py`; decode+validate in one C pass, in-process, pure-Python
  packaging. **5.56 s** for the current corpus single-threaded, **2.13 s**
  with `--workers 8`.
- **rust** — the fastingest serde sidecar: **0.33 s** parse+validate, but needs a Rust
  toolchain.

Both keep the same `ingest_manifest.json`, so you can switch engines
between runs; a no-change incremental run takes **~0.55 s** with msgspec.

## Benchmark (24,548 files; 24,548 valid rows; 0 validation errors)

| stage                     | msgspec (1 worker) | msgspec (8 workers) | Rust sidecar |
|---------------------------|-------------------:|--------------------:|-------------:|
| parse + validate          | 5.56 s             | 2.13 s              | **0.33 s**   |
| SQLite insert             | 1.50 s             | 1.51 s              | 1.47 s       |
| DuckDB insert             | 0.86 s             | 0.93 s              | 0.85 s       |
| DuckDB total (parse+load) | 6.42 s             | 3.06 s              | **1.18 s**   |
| incremental, no changes   | ~0.55 s            | (not measured)      | **<0.01 s**  |

Measurements are full rebuilds on the current 198 MB compressed corpus.
The Rust parse and incremental times are the sidecar's reported processing
times; its full total adds the SQLite and DuckDB loads. DuckDB's insert dropped ~3x vs the original
because it bulk-ingests a columnar Arrow table instead of row-wise
`executemany`; the SQLite insert happens inside whichever engine parsed
(rusqlite in the sidecar, stdlib `sqlite3` `executemany` in the msgspec
engine — same DDL, pragmas, and single transaction).

## Architecture

```
data/raw/json/*.json.gz
        │  engine (--engine, default msgspec): stat all files, skip those
        │  unchanged per ingest_manifest.json; parse+validate+flatten the
        │  rest to 33-column rows —
        │    msgspec: in-process Python, gzip → msgspec Struct decode
        │      (job_schema.py); --workers N fans out over a process pool
        │    rust:    fastingest sidecar, rayon-parallel fs::read →
        │      flate2(zlib-rs) gunzip → serde_json into typed structs;
        │      hands its rows back as jobs.arrow (Arrow IPC, transient —
        │      deleted once loaded; the msgspec engine needs no handoff
        │      file, its Arrow table stays in-process)
        ├─→ SQLite  jobs.sqlite   (INSERT OR REPLACE, one tx; rusqlite or
        │                          stdlib sqlite3 — same DDL and pragmas)
        ├─→ jobs.parquet          (--parquet, zstd; full runs only)
        ▼
Arrow table of this run's rows
        │  ingest_and_benchmark.py (uv run, PEP 723 deps)
        ├─→ DuckDB  jobs.duckdb   (register Arrow table → INSERT OR REPLACE)
        └─→ jobs.parquet          (--parquet on incremental runs: DuckDB
                                   COPY of the full table, zstd)
```

Both databases key on `requisition_id` and are fed with `INSERT OR
REPLACE`, so incremental runs are idempotent upserts. A full rebuild
happens with `--full`, or automatically whenever the manifest or either
database file is missing (the Arrow delta alone couldn't rebuild a lost
DB). Deleted input files are ignored: their rows linger until the next
full run.

### Files

- **`fastingest/`** — Rust crate.
  - `src/schema.rs`: serde mirror of `job_schema.py`, field-for-field and in
    the same order (so the JSON blob columns round-trip with identical key
    order). Handles the field aliases (`401k_matching`,
    `bi-weekly_*_compensation`, `_geoloc`, `__N_SSG`, camelCase user-activity
    arrays), `str | int` unions (untagged enum), `null → []` list
    normalization, and RFC 3339 datetime validation via chrono.
  - `src/flatten.rs`: mirror of the original Python `flatten()` — 33-column
    row with 3 nested-JSON text blobs re-serialized from the typed structs.
  - `src/arrow_out.rs`: column builders → one RecordBatch → Arrow IPC file,
    plus an optional Parquet writer (zstd).
  - `src/sqlite_out.rs`: rusqlite (bundled) upsert — full rebuild or
    incremental `INSERT OR REPLACE` in one transaction.
  - `src/manifest.rs`: `ingest_manifest.json` — file → (mtime, size,
    requisition_id) at last successful parse; drives change detection.
    Files that fail validation are not recorded, so they retry every run.
  - `src/main.rs`: arg parsing (`--limit/--full/--parquet`), change
    detection, rayon pipeline, per-file error count-and-continue (exit 0,
    samples reported in the stats JSON).
- **`ingest_and_benchmark.py`** — decides full-vs-incremental, runs the
  selected engine, upserts the (delta) Arrow table into DuckDB, and handles
  the incremental `--parquet` export. Houses the msgspec engine (manifest
  handling, process-pool parsing, sqlite3 upsert); for `--engine rust` it
  subprocesses the binary (auto-rebuilding via cargo when sources are newer
  than the exe).
- **`verify_parity.py`** — samples N random files, runs the msgspec path
  (`decode_page` + `flatten` imported from `job_schema.py`), and compares
  all 33 values per row against the Rust Arrow output. JSON/datetime
  columns are compared at the value level (whitespace and `Z` vs `+00:00`
  formatting are not meaningful); everything else must match exactly, and
  blob key order is asserted.
- **`job_schema.py`** — the schema as `msgspec.Struct`s (converted from the
  original Pydantic v2 models, same classes/fields/order); the Python
  source of truth. Also home of the shared 33-column `COLUMNS` order and
  `flatten()`, used by both the msgspec engine and the parity checker.

## Usage

```powershell
# Incremental run with the default msgspec engine (first run is
# automatically a full rebuild).
# Defaults: --json-dir data/raw/json --out-dir data/processed
uv run ingest_and_benchmark.py

# Parallelize the msgspec parse over 8 processes:
uv run ingest_and_benchmark.py --workers 8

# Use the Rust sidecar instead (builds the binary if needed):
uv run ingest_and_benchmark.py --engine rust

# Force a full rebuild:
uv run ingest_and_benchmark.py --full

# Also emit data/processed/jobs.parquet (full corpus, zstd):
uv run ingest_and_benchmark.py --parquet

# Quick run on a subset:
uv run ingest_and_benchmark.py --limit 2000

# Rust-vs-msgspec parity spot-check (exit 1 on any mismatch).
# ingest_and_benchmark.py deletes jobs.arrow once it's loaded, so generate
# a fresh full-run one by invoking the Rust binary directly first:
fastingest/target/release/fastingest data/raw/json data/processed --full
uv run verify_parity.py --n 500
```

Requirements: `uv` (scripts declare their Python deps inline via PEP 723;
`requirements.txt` exists for non-uv users: `duckdb`, `pyarrow`,
`msgspec`). A Rust toolchain (MSVC on Windows; rusqlite's bundled SQLite
needs a C compiler, which MSVC provides) is only needed for
`--engine rust`.

## Trade-offs vs alternatives considered

Both of the top two options are now implemented, as the two `--engine`s:

**msgspec Structs (now the default engine)** — the schema ported to
`msgspec.Struct`; decode+validate in one C pass, ~19x faster than
json+Pydantic single-threaded and ~2.3x more with `--workers 8`, pure
Python packaging, no toolchain. `job_schema.py` stays the single Python
source of truth (the old Pydantic module was converted, not duplicated).

**Rust serde sidecar (`--engine rust`)** — fastest option that still does
real, typed, per-field validation; the struct definitions *are* the
schema, so malformed files fail loudly. Cost: a second language in the
repo, a toolchain dependency, and the schema exists twice
(`job_schema.py` + `schema.rs`) — any schema change must be made in both
places, with `verify_parity.py` as the safety net.

**Keep Pydantic, add orjson + ProcessPoolExecutor** — least churn, one
schema. But Pydantic validation itself is the bottleneck (not just
parsing), so this caps out around ~6–10 s on typical core counts, and
Windows process-spawn overhead eats into it.

**DuckDB native `read_json('*.json.gz')`** — DuckDB's C++ engine can
parallel-gunzip and parse the files directly; likely the fastest possible
DuckDB load. Rejected because it bypasses schema validation entirely and
would populate the two databases from different code paths.

**Gotchas that actually bit (or would have):**
- serde_json's default float parser is fast but can be **1 ULP off**; this
  produced real mismatches (e.g. `105.76923076923077` → `...076`). Fixed
  with the `float_roundtrip` feature — cost is negligible at this scale.
- Blob columns are **value-equal, not byte-equal**, across engines:
  chrono and msgspec print datetimes with different fractional-second
  padding / `Z` vs `+00:00`. The parity checker normalizes these; anything
  consuming the blobs should parse them as JSON rather than compare strings.
- Rust field order and `#[serde(rename)]` must exactly mirror the
  `job_schema.py` classes or blob key order silently changes — the parity
  checker asserts key order to catch this.
- serde and msgspec are both stricter than Pydantic's lax mode (e.g. no
  `"5"` → 5.0, no naive datetimes). The top-level static-generation marker
  may be named either `__N_SSG` or `__N_SSP`; the schema requires one of
  those two fields.
- msgspec `kw_only=True` is **not inherited** by fields declared on
  subclasses of a configured base Struct — it must be repeated on every
  class, or required-after-optional field orders raise at import.

## Next steps

1. **CI guard for schema drift**: run `cargo build` + `verify_parity.py
   --n 200` (on a small fixture set) whenever `job_schema.py` or
   `schema.rs` changes, so the two schemas can't silently diverge.
2. **Structured error report**: on validation failures, write a
   `errors.jsonl` (file, JSON pointer, message) instead of just 10 samples
   on stdout — useful once the corpus stops being perfectly clean.
3. **If the corpus grows ~100x**: chunk the Arrow output into multiple
   record batches (bounded memory) and consider `LargeUtf8` for the
   description column.
4. **Deletion handling** (currently ignored by choice): the manifest
   already knows each file's requisition_id, so detecting vanished files
   and deleting their rows is a small addition if the input dir ever
   starts shrinking.

Done (were steps 2–4): incremental ingest via `ingest_manifest.json`
(mtime+size, `INSERT OR REPLACE` upserts, auto full rebuild when outputs
are missing); direct SQLite writes from the Rust sidecar via rusqlite; and
`--parquet` output (Rust-written on full runs, DuckDB `COPY` on
incremental ones).
