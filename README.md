# job-ingest — fast job-listing ingest benchmark

Ingests gzipped HiringCafe job listings from `data/raw/json/*.json.gz` into
SQLite and DuckDB with typed schema validation. Ingest is incremental: repeat
runs parse and upsert only new or changed files.

`just dash` opens a [Textual dashboard](#dashboard) over the whole thing: run a
scrape or an ingest, watch it stream, and browse what the corpus actually
contains.

Parsing and validation run in the `fastingest` Rust sidecar. It uses rayon for
parallel file processing, flate2 for decompression, and serde for typed JSON
decoding. On the current corpus of 24,548 files (198 MB compressed), a full
parse and validation takes about 0.33 seconds and a no-change incremental scan
takes less than 0.01 seconds.

## Architecture

New pages are scraped into a *staging* directory, which the default ingest does
not read yet:

```text
hiringcafe.com
        │  job-scrape --raw-dir data/raw/_json
        │  {"pageProps": {"job": ...}, "__N_SSG": true}, gzipped, atomic
        ▼
data/raw/_json/*.json.gz          ← staged, ingest only with --json-dir
```

The established ingest still reads the `data/raw/json` symlink:

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
                    │     read-only by stats.py, dashboard.py
                    └─→ jobs.parquet
                          incremental runs export the full DuckDB table
```

This is a **staging loop, not a fully automatic closed loop.** A default scrape
followed by a default ingest does not yet consume the new files: the two source
directories differ on purpose. The ingest manifest is keyed by filename alone,
so alternating input roots against one output directory could overwrite newer
rows with older copies. To ingest staged pages, say so explicitly and pick an
output directory deliberately:

```powershell
uv run src/job_ingest/ingest_and_benchmark.py --json-dir data/raw/_json --out-dir data/processed
```

Reconciling or merging the two corpus roots is a separate design task. The
dashboard displays both resolved paths side by side so the discrepancy stays
visible.

`<out-dir>/.ingest-incomplete` is a durable recovery marker. The sidecar commits
SQLite and the manifest before Python commits DuckDB; the marker is written
before the sidecar runs and removed only after DuckDB (and any requested
Parquet export) succeeds, so a crash in that window forces one safe full
rebuild instead of silently skipping a delta DuckDB never received.

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
  prints stage timings. `run_ingest()` is the frontend-agnostic core: it
  returns a typed `IngestResult`, streams in-flight progress lines through an
  `on_event` callback, and raises `IngestError` rather than calling
  `sys.exit()`. `main()` is a printer over it.
- `src/job_ingest/scrape_hiringcafe.py` scrapes hiringcafe.com. `scrape()` is a
  synchronous generator of `ScrapeEvent`, raises `ScrapeError`, never prints,
  and writes ingest-compatible raw pages when given a `--raw-dir`. It also
  carries the four saved-search presets.
- `src/job_ingest/stats.py` answers "what is in the corpus?" as frozen
  dataclasses: overview, fill rates, breakdowns, tools, compensation, recency
  and a paged search. It never caches a DuckDB connection — the lock is
  exclusive, and a held read-only handle would block the dashboard's own
  in-process ingest. Every failure becomes `StatsUnavailable`.
- `src/job_ingest/dashboard.py` (+ `dashboard.tcss`) is the Textual TUI. It is
  a thin shell over the three modules above.

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

### Scraping

```powershell
# One of the four saved searches (see docs/saved_hiringcafe_searches.md).
just scrape --preset DS_SF_Remote --max-jobs 100

# Stage ingest-compatible raw pages. Without --raw-dir a scrape only logs.
just scrape --preset DA_Healthcare --max-jobs 50 --raw-dir data/raw/_json

# Ad hoc: at most one of --query, --url, --search-state, --preset.
just scrape --query "data analyst" --max-jobs 40
just scrape --url "https://hiringcafe.com/?searchState=..."
```

`--preset` keys, all sharing the saved full-time/contract, transparent-salary,
0-6-years, individual-contributor, doctorate-optional, last-1,440-days filters:

| Key | Roles | Geography / workplace | Industry |
| --- | --- | --- | --- |
| `DS_SF_Remote` | Data/ML titles, excluding software and electrical engineer | SF within 100 miles, or US remote | any |
| `DA_SF_Remote` | `Data and Analytics` department | SF within 100 miles, or US remote | any |
| `DS_Healthcare` | Same Data/ML titles | SF within 100 miles, or US remote/onsite/hybrid | biotech or healthcare |
| `DA_Healthcare` | `Data and Analytics` department | SF within 100 miles, or US remote | biotech or healthcare |

`--preset` is mutually exclusive with `--query`, `--url` and `--search-state`;
supplying none of them keeps the default feed. The packaged registry stores the
exact URLs and decodes them on demand, so `job-dash` does not need `docs/` to
survive installation; a test asserts the registry and
`docs/saved_hiringcafe_searches.md` never diverge.

`--raw-dir` is **incompatible with `--no-descriptions`**, and the combination is
a hard error rather than a silent no-op: a search hit carries no
`job_information.description`, a non-`Option` `String` in `schema.rs`, so a
page synthesised without one would either fail validation or store an empty
description. In the dashboard the corresponding toggles are kept consistent for
the same reason.

Raw pages are saved **verbatim**, including Firebase user-activity UIDs. That
is an explicit, accepted privacy tradeoff for this corpus; nothing scrubs or
transforms them. Filenames follow the `{requisition_id}.json.gz` convention
only when the id is already safe lowercase ASCII; anything else is sanitised
with a digest of the original id appended, because two ids colliding on one
filename would silently lose a row.

## Dashboard

```powershell
just dash                      # or: job-dash
just dash --out-dir data/processed --json-dir data/raw/json
```

A Textual TUI. One screen, three tabs, all mounted at once so a running scrape
keeps filling its log while you browse data. No screenshot here — it is a
terminal app; run it.

- **Overview** — total / active / expired rows, companies, sources, the active
  publication date range, database sizes, and the manifest's remembered-file
  count and age, over scrolling panels for tools, breakdowns, countries,
  compensation percentiles and column fill rates, plus a 24-month recency
  sparkline.
- **Run** — an ingest panel (full rebuild, Parquet, limit), a scrape panel
  (preset selector or Custom inputs, max jobs/pages, delay, "write raw pages"),
  one shared log, a progress bar, Cancel, and a result summary. Both resolved
  paths are shown together because they intentionally differ.
- **Browse** — search, an "include expired" toggle, a paged table of 200 rows
  at a time, a detail pane, and Previous/Next.

| Key | Action |
| --- | --- |
| `1` `2` `3` | Overview / Run / Browse |
| `r` | Refresh stats |
| `n` `p` | Next / previous page |
| `q` | Quit |

The buttons remain the reliable controls: a focused text input owns the
keyboard, so single-letter bindings do not fire while you are typing.

Corpus views default to **active jobs** (`is_expired IS NOT TRUE`); Overview
still shows all three counts and Browse has an explicit toggle.

Only one pipeline operation runs at a time, and both Run buttons are disabled
while either is active. Cancellation is cooperative: a scrape stops *after the
request already in flight*, and an ingest is not cancellable at all, so its
Cancel button stays disabled rather than pretending otherwise.

The dashboard runs ingest in-process, which takes `jobs.duckdb`'s exclusive
write lock inside the TUI's own process. Close any stray `duckdb` CLI holding
that file before starting, or every read will fail with `StatsUnavailable`.

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
the subprocess contract, the Arrow handoff, incremental skipping, whether
`DUCKDB_DDL` still matches the sidecar's Arrow schema, and the
`.ingest-incomplete` recovery marker. `tests/test_stats.py` builds a tiny
DuckDB from the DDL with hand-written edge-case rows — no Rust toolchain, well
under a second. `tests/test_scrape_hiringcafe.py` touches no network; its one
`@needs_sidecar` test round-trips a real Rust golden fixture through
`write_raw_page()` and back through the sidecar, so it fails loudly if
`schema.rs` drifts.

`tests/test_dashboard.py` drives the TUI headlessly with Textual's
`App.run_test()`. **pytest-asyncio and anyio are deliberately absent**:
wrapping `asyncio.run()` around a private `_drive_*` coroutine keeps each test
*function* synchronous, so plain pytest collects it and no plugin is needed.
Every test passes `size=(120, 40)` — the 80x24 default clips a three-pane
layout badly enough that a `DataTable` renders zero rows and the assertions
would lie. No dashboard test starts a real scrape or a cargo build: `just
check` runs pytest with no timeout, so one that did could hang CI forever.

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
