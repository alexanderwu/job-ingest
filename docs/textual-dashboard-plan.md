# Textual dashboard for job-ingest

## Context

Today the repo has two disconnected CLIs and no way to see what's actually in the corpus:

- `job-scrape` writes a 19-column CSV (plus optional JSONL); `job-ingest` reads
  `data/raw/json/*.json.gz`. **Neither feeds the other.** A dashboard with a
  "Scrape" button and an "Ingest" button would be two panes that never talk.
- **There is no stats layer at all.** Every number in the project is a bare
  `print()` inside `ingest_and_benchmark.main()` (lines 261–402). Nothing returns
  data, so nothing can be rendered anywhere but stdout.

The goal is a single pane that both *operates* the pipeline (run scrape/ingest,
watch progress, cancel, launch one of the four established HiringCafe searches)
and *explains* the corpus (counts, fill rates, breakdowns, compensation,
recency, row browsing).

**Why Textual over Streamlit/marimo.** The work splits roughly evenly between
operating and exploring, and the UI must stay inside `just check`. Streamlit and
marimo are browser+server tools that can't be driven headlessly in a CI gate;
Textual's `App.run_test()` gives a scriptable `Pilot` under plain pytest, ships
`py.typed` so it survives mypy strict, and costs exactly one new dependency
(`rich` is already locked via typer). Its real weakness — charts — is
survivable here because block-character bars plus the built-in `Sparkline`
cover what these stats need.

The bulk of the work is a **frontend-agnostic core**, so the TUI stays a thin
shell and a second frontend later is cheap.

## Verified facts this plan depends on

1. **DuckDB's lock is exclusive**: either one read-write process or N read-only.
   A `duckdb.connect(..., read_only=True)` against `data/processed/jobs.duckdb`
   fails while a stray `duckdb.exe` holds it. A cached read-only handle in the
   TUI would also block the TUI's own ingest.
2. **pytz landmine**: DuckDB's Python client raises
   `InvalidInputException: Required module 'pytz' failed to import` as soon as a
   query returns `TIMESTAMPTZ`. Every date query must return VARCHAR via
   `strftime(...)` or a naive `try_cast(... AS TIMESTAMP)`.
3. **mypy-strict friction is exactly three things** (measured against the
   stopwatch demo): subclass `App[None]` not `App`; annotate
   `x: reactive[int] = reactive(0)`; no callable defaults in `reactive(...)`.
   No `[[tool.mypy.overrides]]` needed. **`BINDINGS` does not need
   `# noqa: RUF012`** — there is no `[tool.ruff]` section, so only default
   `E4/E7/E9/F` rules run. The noqa in the demo file is inert; don't copy it.
4. **The scrape→ingest loop is a verbatim shape match.** `fetch_job_detail()`
   returns `props["job"]`, and `schema.rs:339` wants
   `{"pageProps": {"job": ...}, "__N_SSG": true}` (`n_ssg` is a required `bool`,
   aliased `__N_SSP`). All 24,548 manifest entries satisfy
   `filename_stem == requisition_id`.
5. **Headless tests work without pytest-asyncio**: `asyncio.run()` around a
   private async driver keeps the test function sync. Assertable surfaces:
   `DataTable.row_count`, `len(DataTable.columns)`, `RichLog.lines`,
   `str(Static.render())` (note: `.renderable` is gone in Textual 8),
   `app.workers.wait_for_complete()`.

## Confirmed product decisions

- Scraped raw pages go to `data/raw/_json/` for now, **not** the existing
  `data/raw/json` symlink into `Dev/job-search/data/cache/json`.
- Raw job objects are saved verbatim, including Firebase/user-activity fields.
  This is an explicit privacy tradeoff; do not silently scrub or transform them.
- The dashboard permits only one pipeline operation (scrape or ingest) at a
  time.
- The scraper exposes the four searches in
  [`saved_hiringcafe_searches.md`](saved_hiringcafe_searches.md) as named
  presets, while retaining a Custom option for an ad hoc query, URL, or raw
  `searchState`. A run executes exactly one preset; multi-preset batch runs and
  cross-search result merging are not implicit dashboard behavior.
- Corpus views default to active jobs (`is_expired IS NOT TRUE`). The overview
  still shows total, active, and expired counts, and Browse offers an explicit
  "include expired" toggle.
  influence dependencies, tests, or implementation decisions.

**Known source-directory discrepancy:** the established ingest default remains
`data/raw/json/`, while new scrapes are staged in `data/raw/_json/`. Therefore a
default scrape followed by a default ingest does **not yet** consume the new
files. The dashboard must state this plainly and display both resolved paths.
Do not silently switch the ingest source or combine directories: the manifest is
keyed only by filename, and alternating input roots against one output directory
can overwrite newer rows with older copies. For now, ingesting staged files is
an explicit operation using `--json-dir data/raw/_json` and an intentionally
chosen output directory. Reconciling/merging the two corpus roots is a follow-up
design task.

## Step 1 — Dependencies and scaffolding

`pyproject.toml`:
- Add `"textual>=8.2.8"` to `[project.dependencies]`.
- Drop `streamlit`, `lmstudio`, `markdown`, `aw`, and `[tool.uv.sources]`.
  Nothing in the gate touches them (mypy is scoped to `src/job_ingest`, pytest
  to `tests/`); dropping the `aw` git dependency removes the only git fetch from
  dependency resolution. Do not claim this alone guarantees an offline
  `uv sync`: registry artifacts still have to be cached.
- Add `job-dash = "job_ingest.dashboard:main"` to `[project.scripts]`.

`justfile`: add `dash *args:` mirroring `scrape`/`ingest`; delete the dead
commented-out `stream` recipe.

## Step 2 — Frontend-agnostic ingest core

File: [ingest_and_benchmark.py](../src/job_ingest/ingest_and_benchmark.py)

**2a. Kill the `sys.exit()` hostility and normalize library failures.** Add
`class IngestError(RuntimeError)`
and replace all four `sys.exit(...)` calls in `ensure_rust_binary()` /
`run_fastingest()` with it, same message text. Convert cargo build failures,
malformed/missing sidecar JSON, a missing or unreadable Arrow handoff, filesystem
failures, and DuckDB open/query failures into `IngestError` with their original
cause chained. Use `try/finally` or context managers so DuckDB connections close
after query failures. Promote the `assert table.num_rows == stats["ok"]`
(line 211) to a real `IngestError` — it's stripped under `python -O` and is now
library API.

Requires updating `tests/test_ingest_and_benchmark.py:109` from
`pytest.raises(SystemExit)` to `pytest.raises(IngestError)`; the `"cannot read"`
assertion still holds. Call this out in the commit message.

**2b. Extract the core:**

```python
@dataclass(frozen=True, slots=True)
class IngestResult:
    full: bool
    files: int
    skipped: int
    parsed: int
    ok: int
    errors: int
    error_samples: tuple[str, ...]
    parse_sec: float
    sqlite_insert_sec: float
    sqlite_total_rows: int
    duckdb_insert_sec: float
    duckdb_total_rows: int
    parquet_sec: float | None
    sqlite_bytes: int
    duckdb_bytes: int
    parquet_bytes: int | None


def run_ingest(
    json_dir: Path,
    out_dir: Path,
    *,
    limit: int | None = None,
    full: bool = False,
    parquet: bool = False,
    on_event: Callable[[str], None] = lambda _msg: None,
) -> IngestResult: ...
```

`run_ingest` owns the mkdir, the `full` auto-upgrade (lines 291–296), the
sidecar call, the DuckDB load, and the Parquet branch. It emits via `on_event`
the lines the CLI prints *during* the run — those are genuinely log lines and
can't be reconstructed afterwards (the DuckDB line is sandwiched between result
lines). Don't over-engineer an `IngestEvent` union for six lines. The *report*
(the 72-column `BENCHMARK SUMMARY` block) is fully data-driven and stays in
`main()`.

`ensure_rust_binary()`'s `print("Building fastingest...")` must also route
through `on_event` — a cargo build takes tens of seconds and the TUI would look
hung.

`IngestResult.parse_sec` keeps the CLI's existing meaning: Python wall time for
the sidecar call and Arrow read, less `sqlite_insert_sec`. It is not the Rust
sidecar's narrower serde-only `stats["parse_sec"]`; document this at the field.

**2c. Crash-consistency marker — required before adding an interactive
writer.** Today the sidecar commits SQLite and `ingest_manifest.json` before
Python commits DuckDB. If DuckDB is locked, or Python dies between those steps,
the manifest causes the next incremental run to skip a delta DuckDB never saw.

Use `<out-dir>/.ingest-incomplete` as a durable recovery marker:

1. A marker left by an earlier run forces `full=True`.
2. Create/replace the marker before invoking the sidecar.
3. Clear it only after the DuckDB load and requested Parquet export succeed.
4. On any exception or process crash, leave it in place. A crash after all
   writes but before removal merely causes one safe extra full rebuild.

This intentionally favors recovery over avoiding repeated work. Add a test in
which the sidecar succeeds, `load_duckdb` is stubbed to fail, and the next run is
forced full. This covers the pre-existing split-brain bug rather than only the
dashboard symptom.

**2d.** `main()` becomes a printer: parse args → `run_ingest(..., on_event=lambda
m: print(m, flush=True))` → `except IngestError as e: sys.exit(str(e))` →
`_print_summary(result, args.parquet)`. `sys.exit(str)` prints to stderr and
exits 1, identical to today.

**2e.** Add `def ddl_columns() -> tuple[str, ...]` parsing `DUCKDB_DDL` — the
single source of truth, reused by `stats.py` and the existing drift test.

**Acceptance gate:** capture `just ingest --limit 200` stdout before and after;
diff ignoring float timings. Must be byte-identical.

## Step 3 — `src/job_ingest/stats.py` (new)

**Connection policy drives everything else.** Never cache a connection:

```python
class StatsUnavailable(RuntimeError):
    """Corpus DB missing, or locked by another process. DuckDB allows either one
    read-write process or N read-only ones, so a running ingest -- or a stray
    `duckdb` CLI -- makes reads fail."""


@contextmanager
def _read_only(db_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open, query, close. A held read-only handle would block this same
    process's load_duckdb() write during an ingest."""
```

Translate a missing file, lock conflict, missing `jobs` table, or incompatible
schema into `StatsUnavailable`; do not leak raw DuckDB exceptions into the UI.
Public functions validate positive limits/months and non-negative offsets.

Dataclasses (all `frozen=True, slots=True`): `Overview`, `ColumnFill`,
`Bucket(label, count)`, `Breakdown(column, buckets, other, total)`,
`MultiBreakdown(column, buckets, other_mentions, jobs_with_value,
total_mentions)`, `CompensationCurrency`, `Compensation`, `Recency`, `JobRow`,
`Page(rows, total, offset)`.

```python
def overview(db_path: Path, out_dir: Path | None = None) -> Overview
def fill_rates(db_path: Path, columns: Sequence[str] = DEFAULT_FILL_COLUMNS, *, include_expired: bool = False) -> tuple[ColumnFill, ...]
def breakdown(db_path: Path, column: str, limit: int = 15, *, include_expired: bool = False) -> Breakdown
def technical_tools(db_path: Path, limit: int = 25, *, include_expired: bool = False) -> MultiBreakdown
def compensation(db_path: Path, *, include_expired: bool = False) -> Compensation
def recency(db_path: Path, months: int = 24, *, include_expired: bool = False) -> Recency
def search(db_path: Path, *, query: str = "", limit: int = 200, offset: int = 0,
           include_description: bool = False, include_expired: bool = False) -> Page
```

`Overview` blends DB state with pipeline state (total/active/expired row counts,
active company/source counts, active date range, DB file sizes, manifest entry
count + mtime). A manifest entry count is the sidecar's remembered successful
files, **not** a current raw-file count: deleted entries intentionally linger.
Label it accordingly. Parsing the 2.6 MB
`ingest_manifest.json` just to `len()` it costs ~25 ms — fine in a worker
thread; cache on `(mtime, size)`.

**Keep the per-run sidecar stats out of `stats.py`.** That's run state and lives
in `IngestResult`. `stats.py` never runs the pipeline; `IngestResult` never
queries the corpus. The dashboard refreshes stats *after* an ingest finishes.

**SQL-injection guard, not optional:** `breakdown(column=...)` can't bind a
column name — whitelist against `frozenset(ddl_columns())`, raise `ValueError`
otherwise. Everything else uses `?` params.

Verified SQL shapes (shown without the shared active-job predicate for brevity):

```sql
-- technical_tools (UNNEST drops empty arrays automatically; same pattern reuses
-- for workplace_countries)
SELECT u.tool AS label, COUNT(*) AS n
FROM jobs, UNNEST(from_json(technical_tools, '["VARCHAR"]')) AS u(tool)
GROUP BY 1 ORDER BY n DESC, label LIMIT ?

-- recency: the strftime is what dodges the pytz landmine. Do NOT return the
-- date_trunc result unwrapped -- that's a TIMESTAMPTZ and will raise.
WITH dated AS (
  SELECT try_cast(estimated_publish_date AS TIMESTAMPTZ) AS published
  FROM jobs
)
SELECT strftime(date_trunc('month', published), '%Y-%m') AS label,
       COUNT(*) AS n
FROM dated WHERE published IS NOT NULL
GROUP BY 1 ORDER BY 1 DESC LIMIT ?
```

Scalar `Breakdown.total` is the count of non-null/non-empty values and obeys
`total == sum(buckets) + other`. Array-valued data cannot obey that invariant:
one job may mention Python and Rust. `MultiBreakdown` therefore reports both
`jobs_with_value` and `total_mentions`; `other_mentions` is unshown mentions,
and each bucket percentage is "share of jobs with at least one value". Do not
reuse the scalar invariant in `technical_tools()` tests.

**Compensation contract:** active jobs by default; no currency conversion; one
`CompensationCurrency` per currency, ordered by qualifying job count. Accept a
positive annual minimum or maximum independently, reject zero/negative values,
and report qualifying-job count plus p25/median/p75 separately for minima and
maxima using DuckDB `quantile_cont`. Do not pool currencies or silently infer a
midpoint. Also report how many active jobs have any valid annual compensation,
so the coverage denominator is visible.

**Recency contract:** return exactly the last `months` calendar buckets ending
at the current UTC month, oldest to newest, inserting zero-count months in
Python so the sparkline has a real time axis. Report `older` and `undated`
separately; invalid timestamps count as undated. A stale corpus may therefore
correctly show many recent zeroes.

`fill_rates`: one `SELECT COUNT(*), COUNT(c1), COUNT(c2), ...` statement, not
33 queries. Default `DEFAULT_FILL_COLUMNS` to the ~20 interesting columns and
**exclude `description` and the three `*_json` blobs** — they're non-`Option`
`String` in `flatten.rs` so never null, and they're where all 969 MB live.

`search`: project only the 9 `JobRow` columns, **never `*`** — `description`
averages ~35 KB/row, so a 200-row `SELECT *` page pulls ~7 MB. Gate description
search behind `include_description=False`. Search is a case-insensitive literal
substring match; use `contains(lower(coalesce(column, '')), lower(?))` so `%`
and `_` are not wildcard syntax. Pagination must use a deterministic order:
parsed publish timestamp descending/nulls last, then `requisition_id`. The
timestamp may be used for ordering but must not be returned to Python.

## Step 4 — Produce ingest-compatible scrape output

File: [scrape_hiringcafe.py](../src/job_ingest/scrape_hiringcafe.py)

**4a. Spike first (~30 min), before any TUI work.** Run
`job-scrape --max-jobs 2 --raw-dir <tmp>` then
`job-ingest --json-dir <tmp> --out-dir <tmp>`, assert `ok == 2`. Fact 5 says the
shapes match, but `fetch_job_detail()` returns a *live* response while the
corpus was captured months ago. If `objectID` or `source_and_board_token` have
moved, the feature changes shape and you want to know now.

**4b. Core extraction — generator, not callback:**

```python
@dataclass(frozen=True, slots=True)
class ScrapeConfig:
    search_state: dict[str, Any]
    max_jobs: int = 40
    max_pages: int = 25
    delay: float = 1.0
    descriptions: bool = True
    raw_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class ScrapeEvent:
    kind: Literal["build_id", "page", "job", "warning", "done"]
    message: str  # ready to print
    index: int = 0
    total: int = 0  # for a ProgressBar
    row: dict[str, Any] | None = None  # flattened CSV row
    hit: dict[str, Any] | None = None
    detail: dict[str, Any] | None = None
    raw_path: Path | None = None


def scrape(
    config: ScrapeConfig, client: Client | None = None
) -> Iterator[ScrapeEvent]: ...
```

Add `class ScrapeError(RuntimeError)`. Library code must never call `sys.exit()`
or print directly: convert `get_build_id()`'s exit, configuration failures,
response/JSON-shape failures, and raw-write failures to `ScrapeError`; expose
retry/backoff diagnostics as `warning` events. `main()` alone maps the domain
exception to CLI stderr/exit behavior. The dashboard worker has a final
`except Exception` boundary (while preserving cancellation) so an unexpected
ordinary exception becomes `ScrapeFailed` rather than terminating the app.

Generator beats callback: cancellation is "stop iterating"; explicitly call
`generator.close()` in the worker's `finally` so cleanup does not depend on
garbage collection and `GeneratorExit` runs deterministically. It is
synchronous so it composes with
`@work(thread=True)`; and it's trivially testable by draining into a list
against a stub `Client`. `scrape()` does no CSV/JSONL I/O — `main()` collects
`ev.row` and keeps its existing `KeyboardInterrupt`/`finally` so partial results
are still written.

**4c. Raw corpus writing.** `scrape()` *does* call `write_raw_page` when
`raw_dir` is set — closing the loop is pipeline behavior, not presentation, and
the CLI and TUI must not diverge on it. The write itself stays pure:

```python
def raw_filename(requisition_id: str) -> str | None:
    """Corpus-compatible filename, or None if unusable.

    The convention is exactly {requisition_id}.json.gz. Corpus ids happen to be
    16-char [a-z0-9], but scraped ids come from arbitrary ATSes and may contain
    / \\ : ? *, be a Windows reserved name (CON, NUL, COM1...), or blow the path
    limit. Two ids colliding on one filename silently loses a row, so whenever
    sanitization or truncation changed anything, append a digest of the ORIGINAL
    UTF-8 id. This is collision-resistant, not mathematically injective. Account
    for Windows case-folding, reserved names, trailing dots/spaces, Unicode
    normalization, and path-length limits. Preserve a name without a digest only
    when it is already in the safe lowercase ASCII convention; uppercase or any
    other normalization counts as a change and therefore gets a digest. The
    filename is
    not the ingest key (requisition_id inside the JSON is), so mangling costs
    only readability."""


def write_raw_page(
    detail: dict[str, Any], raw_dir: Path, *, hit: dict[str, Any] | None = None
) -> Path | None: ...
```

Payload is exactly `{"pageProps": {"job": detail}, "__N_SSG": True}`.

**Atomic write — the temp name matters:**

Create a unique same-directory temporary file using an exclusive create (PID
alone is insufficient if concurrent callers target one requisition), ensure its
name does **not** end in `.json.gz`, gzip/write it, then `os.replace` it into
place. Remove the temporary file in `finally` after failures.

A temp file ending in `.json.gz` gets picked up mid-write by `list_inputs()` in
`lib.rs` and reported as a phantom validation error. `os.replace` gives a fresh
mtime, so a re-scraped job correctly re-triggers incremental ingest via the
`(mtime_ns, size)` manifest key.

Raw payloads are intentionally saved verbatim. They can contain real Firebase
user-activity UIDs; this has been explicitly accepted, so document it but do not
scrub fields.

Pre-flight validate against a `REQUIRED_JOB_KEYS` frozenset mirroring `struct
Job` (`id`, `board_token`, `source`, `apply_url`, `source_and_board_token`,
`requisition_id`, `collapse_key`, `is_expired`, `objectID`, `job_information`,
`v5_processed_job_data`). Fill missing flat identity keys from `hit` when
available; if still incomplete, skip and yield a `warning` naming the missing
keys. Better an offline warning than a file that fails at ingest.

**`--no-descriptions` + `--raw-dir` → hard error**, not a silent no-op. The
search `hit` lacks `job_information.description`, a non-`Option` `String` in
`schema.rs`; a synthesized page either fails validation or silently stores empty
descriptions. In the TUI, disable the "no descriptions" toggle when "write to
corpus" is checked.

**4d. CLI and staging path:** `--raw-dir PATH` remains opt-in. The dashboard's
"write raw pages" checkbox targets `data/raw/_json/` by default and displays its
resolved absolute path before starting. Do not call this directory the active
corpus and do not default it to the `data/raw/json` symlink. The known
source-directory discrepancy at the top of this plan applies.

**4e. Saved-search registry.** Add a small typed registry (in
`scrape_hiringcafe.py`, or a sibling `saved_searches.py` if keeping the scraper
focused) containing the four URLs from
[`saved_hiringcafe_searches.md`](saved_hiringcafe_searches.md). Store the exact
URLs and obtain a fresh `search_state` through `parse_search_state()` when a
run starts; do not hand-maintain a second, potentially drifting interpretation
of the encoded JSON. The Markdown file is the human-readable provenance, while
the packaged Python registry is the runtime source so `job-dash` does not
depend on the repository's `docs/` directory being present after installation.

```python
@dataclass(frozen=True, slots=True)
class SavedSearch:
    key: str
    label: str
    url: str
    summary: str


SAVED_SEARCHES: tuple[SavedSearch, ...] = ...


def saved_search(key: str) -> SavedSearch: ...
```

Preserve the saved names as stable keys and expose clearer labels/summaries in
the UI. The exact decoded distinctions are:

| Key | Role filter | Geography/workplace | Industry | Age |
| --- | --- | --- | --- | --- |
| `DS_SF_Remote` | Data/ML title expression, excluding software/electrical engineer | SF within 100 miles **or** US remote | Any | 1,440 days |
| `DA_SF_Remote` | `Data and Analytics` department | SF within 100 miles **or** US remote | Any | 1,440 days |
| `DS_Healthcare` | Same Data/ML title expression | SF within 100 miles **or** US remote/onsite/hybrid | Biotechnology or healthcare | 1,440 days |
| `DA_Healthcare` | `Data and Analytics` department | SF within 100 miles **or** US remote | Biotechnology or healthcare | 1,440 days |

All four also retain their saved 1,440-day window, full-time/contract,
transparent-salary, 0–6-years, individual-contributor, and
doctorate-preferred/not-mentioned filters. Keep that age window consistent
across the registry, and do not broaden `DA_Healthcare` to onsite/hybrid: those
are the values in the supplied URLs.

Add `--preset KEY` to the CLI. It is mutually exclusive with `--query`,
`--url`, and `--search-state`; reject ambiguous combinations with
`ScrapeError`, list valid keys for an unknown preset, and keep the current
default-feed behavior when none of the four input modes is supplied. Resolve
the preset before constructing `ScrapeConfig`, so the core generator continues
to accept only a concrete `search_state` and stays unaware of UI/CLI naming.

## Step 5 — `src/job_ingest/dashboard.py` + `dashboard.tcss` (new)

**One screen, `TabbedContent` with three tabs** bound to `1`/`2`/`3`, plus
`Header`/`Footer`. Tabs (not separate `Screen`s) so all three widget trees stay
mounted — a running scrape keeps filling its log while you browse data.

- **Overview** — key-figures `Static` (total/active/expired rows, active
  companies, active date range, DB sizes, manifest entry count and age) over a
  `VerticalScroll` of active-job breakdown panels.
- **Run** — Ingest panel (`Checkbox` full/parquet, `Input` limit, Run), Scrape
  panel (saved-search `Select`, Custom query/URL/raw-JSON inputs,
  `Input` max-jobs/max-pages/delay, `Checkbox` "write raw pages", Run), one
  shared `RichLog`, a `ProgressBar`, Cancel, and a summary `Static` fed from
  `IngestResult`. Show the resolved scrape-output and ingest-input paths
  together because they intentionally differ for now.
- **Browse** — search `Input`, "include expired" checkbox, `DataTable`, detail
  `Static`, "showing X–Y of N", and explicit Previous/Next buttons. Search and
  analytics default to active jobs.

**Charts with zero new deps:** `def bar(value, peak, width=24) -> str` using
eighth-blocks (U+2588 full, U+258F–U+2589 partials); each breakdown row renders
as `f"{label:<28.28} {bar(n, peak)} {n:>6} {pct:>5.1f}%"`. For recency use the
built-in `textual.widgets.Sparkline` (takes `list[float]`) with block-bar
numbers beneath.

**Threading:**

- **One operation at a time.** Put ingest and scrape in one exclusive worker
  group (for example `group="pipeline"`) and disable both Run buttons while
  either is active. This makes the shared log/progress/cancel state unambiguous
  and prevents concurrent writers targeting the same raw page. Event handlers
  must also re-check a busy flag and refuse a second start; do not rely only on
  disabled buttons or `exclusive=True` (which could try to cancel the existing
  worker).

- A stats query must not overlap the in-process DuckDB writer. Give the
  Dashboard one process-local `threading.Lock`; every stats/search worker and
  the entire `run_ingest` call acquire it. Disable Browse/refresh actions during
  ingest and refresh stats only after ingest releases the lock. This closes the
  race that short-lived connections alone cannot prevent.

- Ingest → `@work(thread=True, exclusive=True, group="pipeline", exit_on_error=False)`,
  calling `run_ingest(..., on_event=lambda m: self.call_from_thread(log.write, m))`.
  Results come back as `Message` subclasses (`IngestFinished(result)`,
  `IngestFailed(error)`) posted with `self.post_message(...)`.
- Scraper → **also `@work(thread=True)`, not async.** The scraper is blocking
  `requests` plus a blocking rate-limit `sleep`; async means either a new HTTP
  dep (violating "one new dep") or `asyncio.to_thread` (a thread in disguise).
  The worker drives `scrape()` and posts one `ScrapeProgress` per yielded event.
- **`exit_on_error=False` on both.** The default `True` tears the whole app down
  on an unhandled exception. Catch `IngestError` / `ScrapeError`, with a final
  ordinary-exception safety boundary in each worker body → a `...Failed`
  message → red log line plus
  `self.notify(..., severity="error")`.
- **Cancellation is cooperative — say so in the UI.** `worker.cancel()` can't
  interrupt a blocking `requests.get` or `subprocess.run`. *Scraper:* check a
  cancellation event before each retry/event and replace backoff `sleep()` with
  `event.wait(timeout)` so backoff is interruptible. A request already inside
  `requests` can still take up to its configured timeout; describe cancellation
  as "after the current request," not as a ~1s guarantee. Explicitly close the
  generator on cancellation. *Ingest:* **not cancellable, don't pretend** — a
  normal full run is ~1.8s but a cargo build may be much longer; disable the
  Cancel button during ingest and explain that the current stage must finish.

**Ingest runs in-process, not as a subprocess.** At ~1.8s streaming buys
nothing; in-process yields a typed `IngestResult` instead of stdout scraping
(the whole point of step 2); the `sys.exit()` objection is fixed by step 2a; and
a subprocess needs `uv run`/`sys.executable` juggling on Windows. The one cost:
`load_duckdb` opens `jobs.duckdb` read-write *in the TUI's own process*, so no
`stats.py` handle may be open then — which the step 3 short-lived-connection
policy guarantees. Comment that coupling explicitly; it is *the* reason for the
policy.

**`DataTable` pages from DuckDB, never loads all.** 24.5k × 9 is ~220k cell
objects. Use `stats.search(limit=200, offset=...)` with `n`/`p` bindings; deep
`OFFSET` on 24.5k rows is free, no keyset pagination needed. Buttons remain the
reliable controls while a text input owns keyboard focus; do not assume number
or `n`/`p` bindings always win over focused inputs.

Treat all job-originated strings as untrusted display text. Do not enable Rich
markup for titles, companies, descriptions, or log messages derived from the
site.

**Saved-search UX:** populate the `Select` in registry order with the four
friendly labels followed by `Custom`. Default to `DS_SF_Remote`, show its
human-readable summary below the selector, and log the stable key plus summary
at run start (never the multi-kilobyte encoded URL). Selecting a preset disables
the three Custom inputs and uses its exact decoded state. Selecting `Custom`
enables them and applies the same precedence/validation as the CLI, with at most
one of query, URL, or raw JSON non-empty. Changing the selector during a run is
disabled along with the other scrape controls. Do not offer an "All" choice in
this iteration: it would require separate per-search progress, deduplication,
partial-failure, and output semantics.

**mypy-strict checklist:** `class Dashboard(App[None])`; explicit
`reactive[T]` annotations, no callable defaults; `-> None` on every
`on_*`/`watch_*`/`action_*`; always two-arg `query_one("#id", DataTable)` (the
one-arg form returns `Widget`); annotate `Message.__init__` and call
`super().__init__()`.

**CSS:** `CSS_PATH = "dashboard.tcss"` resolves relative to the module. `just
check` never builds a wheel so packaging only breaks for installed copies;
`uv_build` includes files under the module dir by default — add a one-line test
asserting the file exists next to the module. (Inline `CSS = """..."""`
sidesteps it entirely if preferred.)

**`main()`** is a small argparse entry (`--db`, `--out-dir`, `--json-dir`,
mirroring `ingest_and_benchmark`) constructing `Dashboard(...)` and calling
`.run()` — don't point the console script at the `App` class. It must **not**
`stat()` a missing DB at import or construction time: `data/` is gitignored, so
a fresh clone has none.

## Step 6 — Tests

**`tests/test_stats.py`** — no Rust toolchain, <1s. A fixture builds a tiny
DuckDB in `tmp_path` from `DUCKDB_DDL` plus ~6 hand-written edge-case rows
(`technical_tools` of `'[]'` / `'["Python","Rust"]'` / `'["Python"]'`, NULL
`estimated_publish_date`, NULL `company_name`, NULL compensations). Assert:
`overview()` total/active/expired counts and `StatsUnavailable` on a missing,
locked, or schema-incompatible DB; `technical_tools()` → `Python=2, Rust=1`
with `'[]'` contributing nothing, plus distinct job and mention totals;
`recency()` returns exactly `months` oldest-to-newest string buckets including
zeroes, with invalid/NULL dates in `undated`; `compensation()` keeps currencies
separate, ignores expired and non-positive values by default, and computes the
specified percentiles; scalar `breakdown()` total == sum(buckets) + other;
`breakdown(column="; DROP TABLE jobs")` raises `ValueError`; `search()` defaults
to active jobs, treats `%`/`_` literally, validates limit/offset, and has stable
ordering across pages; and the drift guard
`set(DEFAULT_FILL_COLUMNS) <= set(ddl_columns())`.

**`tests/test_scrape_hiringcafe.py`** — no network. `raw_filename()` table:
`"abc123" -> "abc123.json.gz"`, `"REQ/12:34"` sanitized *and* hashed, two ids
that sanitize to the same slug produce different filenames, case-only ids do
not collide on Windows, and `"CON"`, trailing dots/spaces, Unicode, long ids,
and `""` are handled. `write_raw_page()` into `tmp_path`: gzip round-trips to
the expected verbatim shape, no `*.tmp*` leftovers after success or failure,
and a second write overwrites cleanly. `scrape()` drained against a stub
`Client`: event sequence, warning routing, `ScrapeError`, cleanup, and
`max_jobs` honoured.

Also assert that the registry has exactly the four stable keys, each URL parses
to the expected distinguishing fields in the table above, and repeated
resolution returns equal but independently owned `search_state` dictionaries.
Cover CLI/input resolution for preset vs. Custom mutual exclusion and unknown
keys. As a repository-only drift guard, parse the four headings/URLs in
`docs/saved_hiringcafe_searches.md` and assert they exactly match the packaged
registry; this keeps edits to the human source from silently diverging from the
installed app.

**The test that proves the formats compose** (`@needs_sidecar`): read
`fastingest/tests/fixtures/no_enrichment.json.gz` → `["pageProps"]["job"]` →
round-trip through `write_raw_page` into an empty `tmp_path` →
`run_fastingest(tmp_path, out, full=True)` → assert `stats["ok"] == 1`. Using a
real fixture guarantees a valid job object without inventing one, and it fails
loudly if `schema.rs` drifts.

**`tests/test_dashboard.py`** — no pytest-asyncio (pattern verified):

```python
def test_dashboard_mounts(tmp_path: Path) -> None:
    asyncio.run(_drive_mount(tmp_path))


async def _drive_mount(tmp_path: Path) -> None:
    app = Dashboard(db_path=..., out_dir=...)
    async with app.run_test(size=(120, 40)) as pilot:
        ...
```

Docstring the rationale: wrapping `asyncio.run()` around a private `_drive_*`
coroutine keeps the *test function* synchronous, so plain pytest collects it and
neither `pytest-asyncio` nor `anyio` is needed. **Pass `size=(120, 40)`** — the
80×24 default clips a three-pane layout and can make `DataTable` render zero
rows. Assert wiring, not pixels:

1. Mounts against a populated temp DB; the overview `Static` shows the row count.
2. Mounts against a **missing** DB without crashing, showing the
   `StatsUnavailable` message — the most likely real-world failure.
3. Browse tab populates the `DataTable`; the next-page key advances the offset.
4. Monkeypatch `job_ingest.dashboard.run_ingest` with a stub returning a canned
   `IngestResult`; press Run; `await app.workers.wait_for_complete()`; assert
   the summary `Static` and `RichLog.lines`. Exercises worker → message →
   widget with zero subprocess/Rust dependency.
5. A stub raising `IngestError` → assert `app.is_running` is still true and a
   failure line landed in the log. This proves `exit_on_error=False` is wired.
6. `dashboard.tcss` exists next to the module.
7. While either worker is active both Run buttons are disabled; Cancel is
   enabled only for scrape, and a second operation cannot start.
8. Browse excludes expired rows by default and the toggle includes them.
9. The scrape selector contains the four stable presets plus Custom; selecting
   each preset displays its summary and the worker receives its exact decoded
   state, while Custom enables its inputs and rejects multiple input modes.

**Extend `tests/test_ingest_and_benchmark.py`** with a `TestRunIngest`
(`@needs_sidecar`) asserting `IngestResult` fields over the fixtures (`ok == 7`,
`errors == 2`, `duckdb_total_rows == 7`) and that `on_event` received lines.
Add non-sidecar unit coverage for malformed stats/Arrow errors and the durable
`.ingest-incomplete` recovery marker. The key regression is: sidecar succeeds,
DuckDB load fails, marker remains, and the next call forces a full rebuild.

**CI-hang guard:** `just check` runs pytest with no timeout. Never start a real
scrape or a real cargo build from a dashboard test; keep every worker stubbed or
bounded by `wait_for_complete()`. Don't add `pytest-timeout`.

## Step 7 — Documentation

`README.md`: extend the Architecture block with the scrape staging flow
(`hiringcafe.com ──job-scrape --raw-dir──▶ data/raw/_json/*.json.gz`) and
separately show that the current default ingest still reads the
`data/raw/json` symlink. Call this a staging loop, not a fully automatic closed
loop, until the source-directory discrepancy is reconciled;
add a Dashboard section (three panes, keybindings, no screenshot — it's a TUI);
add `stats.py` and `dashboard.py` to Components; add `just dash` to Usage;
document the four `--preset` keys, the dashboard's one-search-per-run behavior,
the `--raw-dir` + `--no-descriptions` incompatibility, and the symlink caveat;
add a Development note on the `asyncio.run()` test pattern and why
pytest-asyncio is deliberately absent. Update the scraper module docstring's
usage block with a preset example.

## Ordering

```
1 (deps/scaffolding)
 ├─▶ 2 (run_ingest + IngestError) ─┐
 ├─▶ 3 (stats.py + tests) ─────────┼─▶ 5 (dashboard) ─▶ 6c (dashboard tests) ─▶ 7 (README)
 └─▶ 4 (scraper core + --raw-dir) ─┘
```

Steps 2, 3, 4 are mutually independent (3 needs only `ddl_columns()` from 2) and
each is independently shippable with `just check` green. Don't bundle them.
Step 4a (the spike) gates step 4.

## Verification

- After each of steps 1–4: `just check` green (ruff format/lint, mypy strict,
  pytest, cargo fmt/clippy/test).
- Step 2 regression: `just ingest --limit 200` stdout byte-identical to a
  pre-change capture, modulo float timings.
- Step 2 recovery: simulate failure after the sidecar commits but before DuckDB
  does; verify `.ingest-incomplete` remains and the next run forces full.
- Step 4 end-to-end: `job-scrape --max-jobs 2 --raw-dir <tmp>` then
  `job-ingest --json-dir <tmp> --out-dir <tmp>` → `ok == 2`.
- Step 4 preset smoke test: run `job-scrape --preset DS_SF_Remote --max-jobs 2`
  and confirm the first-page request contains the exact saved state; do not
  exercise all four live URLs in the automated gate.
- Step 5 manual: `just dash` against the real corpus — overview populates,
  ingest runs and streams to the log then refreshes stats, browse pages through
  active rows by default, scrape runs and cancels after its current request, and
  a second operation cannot start while one is active. Confirm the UI displays
  the differing resolved scrape-output and ingest-input paths. Then `just dash`
  with `data/` renamed away, to confirm the empty-state path.
- Close any stray `duckdb.exe` before testing — one will hold
  `data/processed/jobs.duckdb` exclusively and every read will fail.

## Resolved implementation decisions

- New scraped pages are staged in `data/raw/_json/`; never write them through
  the `data/raw/json` symlink. The path mismatch remains visible and documented.
- Preserve raw job objects verbatim, including Firebase user-activity UIDs.
- Run only one scrape or ingest operation at a time.
- Ship the four named HiringCafe presets with both CLI and dashboard access;
  run one preset at a time and preserve their encoded filters exactly.
- Default corpus queries and Browse to active jobs; make expired inclusion
  explicit and show all three counts in Overview.

## Explicitly out of scope / rejected

- **An async worker for the scraper** — `requests` is blocking; async buys
  nothing without a new HTTP dep.
- **`SELECT *` into the browse table** — ~35 KB/row of description.
- **Caching a DuckDB connection on the App** — breaks the TUI's own ingest.
- **Promising instant cancellation** — cooperative only; don't restructure
  ingest into a killable subprocess to fake it.
- **Silently merging or alternating `data/raw/json` and `data/raw/_json`** — the
  manifest namespace is filename-only, so source reconciliation needs its own
  design.
- **An implicit "run all saved searches" batch** — overlapping results require
  explicit deduplication, progress, output, and partial-failure contracts.
- **New deps `pytest-asyncio`, `pytest-timeout`, `plotext`, `pandas`** — the
  first is verified unnecessary, the rest are avoidable.
