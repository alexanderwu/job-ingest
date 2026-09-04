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
watch progress, cancel) and *explains* the corpus (counts, fill rates,
breakdowns, compensation, recency, row browsing).

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
3. **`textual` is in `.venv` but absent from `uv.lock`** — installed ad-hoc for
   `src/_AW_stopwatch.py`. The next `uv sync` deletes it. Declaring it is
   mandatory.
4. **mypy-strict friction is exactly three things** (measured against the
   stopwatch demo): subclass `App[None]` not `App`; annotate
   `x: reactive[int] = reactive(0)`; no callable defaults in `reactive(...)`.
   No `[[tool.mypy.overrides]]` needed. **`BINDINGS` does not need
   `# noqa: RUF012`** — there is no `[tool.ruff]` section, so only default
   `E4/E7/E9/F` rules run. The noqa in the demo file is inert; don't copy it.
5. **The scrape→ingest loop is a verbatim shape match.** `fetch_job_detail()`
   returns `props["job"]`, and `schema.rs:339` wants
   `{"pageProps": {"job": ...}, "__N_SSG": true}` (`n_ssg` is a required `bool`,
   aliased `__N_SSP`). All 24,548 manifest entries satisfy
   `filename_stem == requisition_id`.
6. **Headless tests work without pytest-asyncio**: `asyncio.run()` around a
   private async driver keeps the test function sync. Assertable surfaces:
   `DataTable.row_count`, `len(DataTable.columns)`, `RichLog.lines`,
   `str(Static.render())` (note: `.renderable` is gone in Textual 8),
   `app.workers.wait_for_complete()`.

## Step 1 — Dependencies and scaffolding

`pyproject.toml`:
- Add `"textual>=8.2.8"` to `[project.dependencies]`.
- Drop `streamlit`, `lmstudio`, `markdown`, `aw`, and `[tool.uv.sources]`.
  Nothing in the gate touches them (mypy is scoped to `src/job_ingest`, pytest
  to `tests/`); dropping the `aw` git dep also makes `uv sync` offline-capable.
  If `_AW_streamlit/` should stay runnable, put those in a
  `[dependency-groups] scratch`, not runtime deps.
- Add `job-dash = "job_ingest.dashboard:main"` to `[project.scripts]`.

`justfile`: add `dash *args:` mirroring `scrape`/`ingest`; delete the dead
commented-out `stream` recipe.

## Step 2 — Frontend-agnostic ingest core

File: [ingest_and_benchmark.py](../src/job_ingest/ingest_and_benchmark.py)

**2a. Kill the `sys.exit()` hostility.** Add `class IngestError(RuntimeError)`
and replace all four `sys.exit(...)` calls in `ensure_rust_binary()` /
`run_fastingest()` with it, same message text. Wrap `load_duckdb`'s
`duckdb.connect` in `except duckdb.IOException` → `IngestError` (that's fact 1
hit from the write side). Promote the `assert table.num_rows == stats["ok"]`
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
    files: int; skipped: int; parsed: int; ok: int; errors: int
    error_samples: tuple[str, ...]
    parse_sec: float
    sqlite_insert_sec: float; sqlite_total_rows: int
    duckdb_insert_sec: float; duckdb_total_rows: int
    parquet_sec: float | None
    sqlite_bytes: int; duckdb_bytes: int; parquet_bytes: int | None

def run_ingest(
    json_dir: Path, out_dir: Path, *,
    limit: int | None = None, full: bool = False, parquet: bool = False,
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

**2c.** `main()` becomes a printer: parse args → `run_ingest(..., on_event=lambda
m: print(m, flush=True))` → `except IngestError as e: sys.exit(str(e))` →
`_print_summary(result, args.parquet)`. `sys.exit(str)` prints to stderr and
exits 1, identical to today.

**2d.** Add `def ddl_columns() -> tuple[str, ...]` parsing `DUCKDB_DDL` — the
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

Dataclasses (all `frozen=True, slots=True`): `Overview`, `ColumnFill`,
`Bucket(label, count)`, `Breakdown(column, buckets, other, total)`,
`Compensation`, `Recency`, `JobRow`, `Page(rows, total, offset)`.

```python
def overview(db_path: Path, out_dir: Path | None = None) -> Overview
def fill_rates(db_path: Path, columns: Sequence[str] = DEFAULT_FILL_COLUMNS) -> tuple[ColumnFill, ...]
def breakdown(db_path: Path, column: str, limit: int = 15) -> Breakdown
def technical_tools(db_path: Path, limit: int = 25) -> Breakdown
def compensation(db_path: Path) -> Compensation
def recency(db_path: Path, months: int = 24) -> Recency
def search(db_path: Path, *, query: str = "", limit: int = 200, offset: int = 0,
           include_description: bool = False) -> Page
```

`Overview` blends DB state with pipeline state (row/company/source counts, date
range, DB file sizes, manifest file count + mtime). Parsing the 2.6 MB
`ingest_manifest.json` just to `len()` it costs ~25 ms — fine in a worker
thread; cache on `(mtime, size)`.

**Keep the per-run sidecar stats out of `stats.py`.** That's run state and lives
in `IngestResult`. `stats.py` never runs the pipeline; `IngestResult` never
queries the corpus. The dashboard refreshes stats *after* an ingest finishes.

**SQL-injection guard, not optional:** `breakdown(column=...)` can't bind a
column name — whitelist against `frozenset(ddl_columns())`, raise `ValueError`
otherwise. Everything else uses `?` params.

Verified SQL shapes:

```sql
-- technical_tools (UNNEST drops empty arrays automatically; same pattern reuses
-- for workplace_countries). Denominator: json_array_length(...) > 0
SELECT u.tool AS label, COUNT(*) AS n
FROM jobs, UNNEST(from_json(technical_tools, '["VARCHAR"]')) AS u(tool)
GROUP BY 1 ORDER BY n DESC, label LIMIT ?

-- recency: the strftime is what dodges the pytz landmine. Do NOT return the
-- date_trunc result unwrapped -- that's a TIMESTAMPTZ and will raise.
SELECT strftime(date_trunc('month', CAST(estimated_publish_date AS TIMESTAMPTZ)), '%Y-%m') AS label,
       COUNT(*) AS n
FROM jobs WHERE try_cast(estimated_publish_date AS TIMESTAMPTZ) IS NOT NULL
GROUP BY 1 ORDER BY 1 DESC LIMIT ?
```

`fill_rates`: one `SELECT COUNT(*), COUNT(c1), COUNT(c2), ...` statement, not
33 queries. Default `DEFAULT_FILL_COLUMNS` to the ~20 interesting columns and
**exclude `description` and the three `*_json` blobs** — they're non-`Option`
`String` in `flatten.rs` so never null, and they're where all 969 MB live.

`search`: project only the 9 `JobRow` columns, **never `*`** — `description`
averages ~35 KB/row, so a 200-row `SELECT *` page pulls ~7 MB. Gate description
search behind `include_description=False`.

## Step 4 — Close the scrape→ingest loop

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
    max_jobs: int = 40; max_pages: int = 25; delay: float = 1.0
    descriptions: bool = True
    raw_dir: Path | None = None

@dataclass(frozen=True, slots=True)
class ScrapeEvent:
    kind: Literal["build_id", "page", "job", "warning", "done"]
    message: str                       # ready to print
    index: int = 0; total: int = 0     # for a ProgressBar
    row: dict[str, Any] | None = None  # flattened CSV row
    hit: dict[str, Any] | None = None
    detail: dict[str, Any] | None = None
    raw_path: Path | None = None

def scrape(config: ScrapeConfig, client: Client | None = None) -> Iterator[ScrapeEvent]: ...
```

Generator beats callback: cancellation is just "stop iterating" (the `finally`
runs on `GeneratorExit`); it's synchronous so it composes with
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
    limit. The mapping MUST be injective -- two ids colliding on one filename
    silently loses a row -- so whenever sanitization or truncation changed
    anything, append a 12-hex-char digest of the ORIGINAL id. The filename is
    not the ingest key (requisition_id inside the JSON is), so mangling costs
    only readability."""

def write_raw_page(detail: dict[str, Any], raw_dir: Path, *,
                   hit: dict[str, Any] | None = None) -> Path | None: ...
```

Payload is exactly `{"pageProps": {"job": detail}, "__N_SSG": True}`.

**Atomic write — the temp name matters:**

```python
tmp = raw_dir / f".{name}.tmp{os.getpid()}"   # NOT *.json.gz
with gzip.open(tmp, "wt", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False)
os.replace(tmp, raw_dir / name)               # atomic on Windows, same volume
```

A temp file ending in `.json.gz` gets picked up mid-write by `list_inputs()` in
`lib.rs` and reported as a phantom validation error. `os.replace` gives a fresh
mtime, so a re-scraped job correctly re-triggers incremental ingest via the
`(mtime_ns, size)` manifest key.

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

**4d. CLI:** `--raw-dir PATH`, opt-in, no default.

## Step 5 — `src/job_ingest/dashboard.py` + `dashboard.tcss` (new)

**One screen, `TabbedContent` with three tabs** bound to `1`/`2`/`3`, plus
`Header`/`Footer`. Tabs (not separate `Screen`s) so all three widget trees stay
mounted — a running scrape keeps filling its log while you browse data.

- **Overview** — key-figures `Static` (rows, companies, date range, DB sizes,
  manifest file count and age) over a `VerticalScroll` of breakdown panels.
- **Run** — Ingest panel (`Checkbox` full/parquet, `Input` limit, Run), Scrape
  panel (`Input` query/url/max-jobs/delay, `Checkbox` "write into corpus", Run),
  one shared `RichLog`, a `ProgressBar`, Cancel, and a summary `Static` fed from
  `IngestResult`.
- **Browse** — search `Input`, `DataTable`, detail `Static`, "showing X–Y of N".

**Charts with zero new deps:** `def bar(value, peak, width=24) -> str` using
eighth-blocks (U+2588 full, U+258F–U+2589 partials); each breakdown row renders
as `f"{label:<28.28} {bar(n, peak)} {n:>6} {pct:>5.1f}%"`. For recency use the
built-in `textual.widgets.Sparkline` (takes `list[float]`) with block-bar
numbers beneath.

**Threading:**

- Ingest → `@work(thread=True, exclusive=True, group="ingest", exit_on_error=False)`,
  calling `run_ingest(..., on_event=lambda m: self.call_from_thread(log.write, m))`.
  Results come back as `Message` subclasses (`IngestFinished(result)`,
  `IngestFailed(error)`) posted with `self.post_message(...)`.
- Scraper → **also `@work(thread=True)`, not async.** The scraper is blocking
  `requests` plus a blocking rate-limit `sleep`; async means either a new HTTP
  dep (violating "one new dep") or `asyncio.to_thread` (a thread in disguise).
  The worker drives `scrape()` and posts one `ScrapeProgress` per yielded event.
- **`exit_on_error=False` on both.** The default `True` tears the whole app down
  on an unhandled exception. Catch `IngestError` / `requests.RequestException`
  in the worker body → a `...Failed` message → red log line plus
  `self.notify(..., severity="error")`.
- **Cancellation is cooperative — say so in the UI.** `worker.cancel()` can't
  interrupt a blocking `requests.get` or `subprocess.run`. *Scraper:* check
  `get_current_worker().is_cancelled` after each yielded event and `break`
  (worst case ~1s latency). *Ingest:* **not cancellable, don't pretend** — a
  full run is ~1.8s; disable the Cancel button during ingest and say why.

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
`OFFSET` on 24.5k rows is free, no keyset pagination needed.

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
`overview()` counts and `StatsUnavailable` on a missing DB; `technical_tools()`
→ `Python=2, Rust=1` with `'[]'` contributing nothing; `recency()` buckets,
`undated` catching the NULL, and **that the labels are `str`** (the pytz
regression guard); `compensation()` percentiles; `breakdown()` total ==
sum(buckets) + other; `breakdown(column="; DROP TABLE jobs")` raises
`ValueError`; `search()` limit/offset with a stable `total`; and the drift guard
`set(DEFAULT_FILL_COLUMNS) <= set(ddl_columns())`.

**`tests/test_scrape_hiringcafe.py`** — no network. `raw_filename()` table:
`"abc123" -> "abc123.json.gz"`, `"REQ/12:34"` sanitized *and* hashed, **two ids
that sanitize to the same slug produce different filenames** (the injectivity
property), `"CON"` and `""` handled. `write_raw_page()` into `tmp_path`: gzip
round-trips to the expected shape, no `*.tmp*` leftovers, second write
overwrites cleanly. `scrape()` drained against a stub `Client`: event sequence
and `max_jobs` honoured.

**The test that proves the loop is closed** (`@needs_sidecar`): read
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

**Extend `tests/test_ingest_and_benchmark.py`** with a `TestRunIngest`
(`@needs_sidecar`) asserting `IngestResult` fields over the fixtures (`ok == 7`,
`errors == 2`, `duckdb_total_rows == 7`) and that `on_event` received lines.

**CI-hang guard:** `just check` runs pytest with no timeout. Never start a real
scrape or a real cargo build from a dashboard test; keep every worker stubbed or
bounded by `wait_for_complete()`. Don't add `pytest-timeout`.

## Step 7 — Documentation

`README.md`: extend the Architecture block with the closed loop
(`hiringcafe.com ──job-scrape --raw-dir──▶ data/raw/json/*.json.gz ──▶ fastingest ...`);
add a Dashboard section (three panes, keybindings, no screenshot — it's a TUI);
add `stats.py` and `dashboard.py` to Components; add `just dash` to Usage;
document the `--raw-dir` + `--no-descriptions` incompatibility and the symlink
caveat; add a Development note on the `asyncio.run()` test pattern and why
pytest-asyncio is deliberately absent. Update the scraper module docstring's
usage block.

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
- Step 4 end-to-end: `job-scrape --max-jobs 2 --raw-dir <tmp>` then
  `job-ingest --json-dir <tmp> --out-dir <tmp>` → `ok == 2`.
- Step 5 manual: `just dash` against the real corpus — overview populates,
  ingest runs and streams to the log then refreshes stats, browse pages through
  rows, scrape runs and cancels. Then `just dash` with `data/` renamed away, to
  confirm the empty-state path.
- Close any stray `duckdb.exe` before testing — one will hold
  `data/processed/jobs.duckdb` exclusively and every read will fail.

## Decisions to confirm during implementation

- **`--raw-dir data/raw/json` writes through a symlink** into
  `Dev/job-search/data/cache/json` — another project's cache. The TUI should
  display the *resolved absolute* path before running. Confirm that's intended,
  or point `--raw-dir` at the real `data/raw/_json/` instead.
- **Privacy:** corpus job objects carry real Firebase user-activity UIDs
  (`job_information.viewedByUsers` and friends) — which is why the test fixtures
  were scrubbed. Verbatim `--raw-dir` writes will store other users' UIDs.

## Explicitly out of scope / rejected

- **An async worker for the scraper** — `requests` is blocking; async buys
  nothing without a new HTTP dep.
- **`SELECT *` into the browse table** — ~35 KB/row of description.
- **Caching a DuckDB connection on the App** — breaks the TUI's own ingest.
- **Promising instant cancellation** — cooperative only; don't restructure
  ingest into a killable subprocess to fake it.
- **New deps `pytest-asyncio`, `pytest-timeout`, `plotext`, `pandas`** — the
  first is verified unnecessary, the rest are avoidable.
