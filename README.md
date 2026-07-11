# job-ingest — fast job-listing ingest benchmark

Ingests ~16k gzipped JSON job listings (`cache/json/*.json.gz`, ~101 MB
compressed / ~200+ MB raw) into **SQLite** and **DuckDB**, with full schema
validation, and benchmarks the two backends.

The parse+validate phase originally ran single-threaded in Python
(stdlib `json` + Pydantic v2) and took **~53 s**. It is now done by a Rust
sidecar and takes **~0.6 s** — an **~84x speedup** — putting the entire
pipeline (parse → validate → flatten → load both DBs) at **~2.4 s**.

## Benchmark (16,076 files, 0 validation errors)

| stage                     | before  | after      |
|---------------------------|---------|------------|
| parse + validate          | ~53 s   | **0.63 s** |
| SQLite insert             | ~1.4 s  | 1.35 s     |
| DuckDB insert             | ~1.3 s  | **0.42 s** |
| DuckDB total (parse+load) | ~54 s   | **1.05 s** |

Notes: the 0.63 s includes process spawn and reading the Arrow file back into
Python; the Rust binary alone parses everything in ~0.5 s warm. The very
first run after a reboot pays Windows file-cache/Defender overhead on 16k
small file opens (~5 s). DuckDB's insert dropped ~3x because it now
bulk-ingests a columnar Arrow table instead of row-wise `executemany`.

## Architecture

```
cache/json/*.json.gz
        │  fastingest (Rust): rayon-parallel per file —
        │    fs::read → flate2(zlib-rs) gunzip → serde_json parse into
        │    typed structs (= validation) → flatten to 33 columns
        ▼
db/jobs.arrow  (Arrow IPC, 33 cols; stats JSON on stdout)
        │  ingest_and_benchmark.py (uv run, PEP 723 deps)
        ├─→ SQLite  db/jobs.sqlite   (executemany, WAL, one transaction)
        └─→ DuckDB  db/jobs.duckdb   (register Arrow table → INSERT..SELECT)
```

### Files

- **`fastingest/`** — Rust crate.
  - `src/schema.rs`: serde mirror of `job_schema.py`, field-for-field and in
    the same order (so the JSON blob columns round-trip with identical key
    order). Handles the Pydantic aliases (`401k_matching`,
    `bi-weekly_*_compensation`, `_geoloc`, `__N_SSG`, camelCase user-activity
    arrays), `str | int` unions (untagged enum), `null → []` list
    normalization, and RFC 3339 datetime validation via chrono.
  - `src/flatten.rs`: mirror of the original Python `flatten()` — 33-column
    row with 3 nested-JSON text blobs re-serialized from the typed structs.
  - `src/arrow_out.rs`: column builders → one RecordBatch → Arrow IPC file.
  - `src/main.rs`: arg parsing, rayon pipeline, per-file error
    count-and-continue (exit 0, samples reported in the stats JSON).
- **`ingest_and_benchmark.py`** — same CLI/benchmark output as before, but
  the parse phase subprocesses the binary (auto-rebuilds via cargo when
  sources are newer than the exe). SQLite is fed row tuples; DuckDB
  bulk-ingests the registered pyarrow Table.
- **`verify_parity.py`** — samples N random files, runs the *original*
  gzip → json → Pydantic → `flatten()` path, and compares all 33 values per
  row against the Arrow output. JSON/datetime columns are compared at the
  value level (whitespace and `Z` vs `+00:00` formatting are not meaningful);
  everything else must match exactly, and blob key order is asserted.
- **`job_schema.py`** — unchanged; remains the documented source of truth
  and the reference implementation that parity is checked against.

## Usage

```powershell
# Full run (builds the Rust binary automatically if needed):
uv run ingest_and_benchmark.py --out-dir db

# Quick run on a subset:
uv run ingest_and_benchmark.py --out-dir db --limit 2000

# Parity spot-check against the Pydantic reference (exit 1 on any mismatch):
uv run verify_parity.py --n 500
```

Requirements: Rust toolchain (MSVC on Windows) and `uv` (scripts declare
their Python deps inline via PEP 723). `requirements.txt` exists for
non-uv users: `duckdb`, `pyarrow`, `pydantic`.

## Trade-offs vs alternatives considered

**Rust serde sidecar (chosen)** — fastest option that still does real,
typed, per-field validation; the struct definitions *are* the schema, so
malformed files fail loudly. Cost: a second language in the repo, a
toolchain dependency, and the schema now exists twice (`job_schema.py` +
`schema.rs`) — any schema change must be made in both places, with
`verify_parity.py` as the safety net.

**msgspec Structs (runner-up)** — port the schema to `msgspec.Struct`;
decode+validate in one C pass, ~10–20x faster than json+Pydantic, pure
Python packaging. With multiprocessing it would likely land at ~1–3 s —
close to Rust, with no toolchain or dual-schema cost. The Rust route was
chosen for maximum headroom; msgspec is the right fallback if maintaining
the Rust crate ever becomes a burden.

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
- Blob columns are **value-equal, not byte-equal**, to the Pydantic output:
  Python's `json.dumps` inserts spaces, and chrono prints
  `.657Z` / `+00:00` where Python prints `.657000Z` / `Z`. The parity
  checker normalizes these; anything consuming the blobs should parse them
  as JSON rather than compare strings.
- Rust field order and `#[serde(rename)]` must exactly mirror the Pydantic
  classes or blob key order silently changes — the parity checker asserts
  key order to catch this.
- serde is stricter than Pydantic's lax mode (e.g. no `"5"` → 5.0, no
  naive datetimes). This corpus is clean (0 errors both ways); if future
  data trips it, add a targeted `deserialize_with` shim for that field only.

## Next steps

1. **CI guard for schema drift**: run `cargo build` + `verify_parity.py
   --n 200` (on a small fixture set) whenever `job_schema.py` or
   `schema.rs` changes, so the two schemas can't silently diverge.
2. **Incremental ingest**: the loader rebuilds both DBs from scratch each
   run. If this becomes a recurring job, track file mtimes/hashes and
   upsert only new/changed listings (`INSERT OR REPLACE` /
   `INSERT ... ON CONFLICT`).
3. **Skip the Arrow round-trip for SQLite** if its 1.35 s ever matters:
   have fastingest write the SQLite file directly (`rusqlite`), or batch
   rows into SQLite from Arrow record batches without materializing all
   16k Python tuples.
4. **Emit Parquet alongside Arrow IPC** (one extra dependency in the
   crate): makes the validated corpus directly queryable by DuckDB, Polars,
   pandas, Spark, etc. without either database.
5. **Structured error report**: on validation failures, write a
   `errors.jsonl` (file, JSON pointer, message) instead of just 10 samples
   on stdout — useful once the corpus stops being perfectly clean.
6. **If the corpus grows ~100x**: chunk the Arrow output into multiple
   record batches (bounded memory) and consider `LargeUtf8` for the
   description column.
