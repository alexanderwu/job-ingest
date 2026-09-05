#!/usr/bin/env python3
"""
Textual dashboard for job-ingest: one pane that both operates the pipeline
and explains the corpus.

    just dash                 # or: job-dash

Three tabs, bound to 1/2/3, all mounted at once so a running scrape keeps
filling its log while you browse data:

  Overview  key figures over active-job breakdowns, fill rates,
            compensation and a recency sparkline
  Run       ingest and scrape panels sharing one log, progress bar,
            cancel button and result summary
  Browse    paged, searchable job rows

This module is a thin shell. Everything it shows comes from
`job_ingest.stats` (corpus state) or `job_ingest.ingest_and_benchmark`
/ `job_ingest.scrape_hiringcafe` (run state); a second frontend would reuse
all of it.

Why Textual: this work splits about evenly between operating and exploring,
and the UI has to stay inside `just check`. Streamlit and marimo are
browser+server tools that cannot be driven headlessly in a CI gate;
`App.run_test()` gives a scriptable Pilot under plain pytest, textual ships
py.typed so it survives mypy strict, and it costs exactly one new
dependency. Charts are its weak spot, which block-character bars and the
built-in Sparkline cover for what these stats need.

Concurrency
-----------
One pipeline operation at a time. Ingest and scrape share an exclusive
worker group, both Run buttons are disabled while either is active, and the
handlers re-check a busy flag rather than trusting disabled buttons alone.

Ingest runs **in-process**, not as a subprocess: a normal run is ~2s so
streaming buys nothing, in-process yields a typed IngestResult instead of
scraped stdout, and a subprocess would need uv/sys.executable juggling on
Windows. The cost is that load_duckdb() takes jobs.duckdb's exclusive write
lock inside this very process, so no stats.py handle may be open at that
moment. That is precisely why stats.py never caches a connection, and why
every stats query and the whole run_ingest call take one process-local
lock here.

Cancellation is cooperative and the UI says so. worker.cancel() cannot
interrupt a blocking Client.get or subprocess.run. A scrape stops after
its current request; an ingest is not cancellable at all, so its Cancel
button stays disabled rather than lying.

Untrusted text
--------------
Titles, companies, descriptions and log lines come from job postings. They
are rendered as `rich.text.Text`, never as markup: DataTable and Static
would otherwise interpret square brackets in a posting as style tags.
"""

from __future__ import annotations

import argparse
import threading
from dataclasses import dataclass
from pathlib import Path

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ProgressBar,
    RichLog,
    Select,
    Sparkline,
    Static,
    TabbedContent,
    TabPane,
)

from job_ingest import stats
from job_ingest.ingest_and_benchmark import IngestError, IngestResult, run_ingest
from job_ingest.scrape_hiringcafe import (
    DEFAULT_RAW_DIR,
    SAVED_PRESETS,
    DEFAULT_INTERIM_DIR,
    SHARED_FILTERS,
    Client,
    ScrapeConfig,
    ScrapeError,
    ScrapeEvent,
    resolve_source,
    saved_search,
    scrape,
)

DEFAULT_OUT_DIR = Path("data/processed")
#: The established ingest source. Deliberately different from
#: DEFAULT_RAW_DIR, where new scrapes are staged -- see _paths_note().
DEFAULT_JSON_DIR = Path("data/raw/json")
PAGE_SIZE = 200
#: The Select value meaning "use the query/URL/raw-JSON inputs below".
CUSTOM = "__custom__"

_BLOCKS = "▏▎▍▌▋▊▉█"


def bar(value: float, peak: float, width: int = 24) -> str:
    """A horizontal bar in eighth-blocks. No dependencies, no canvas."""
    if peak <= 0 or value <= 0:
        return " " * width
    filled = min(1.0, value / peak) * width
    full = int(filled)
    if full >= width:
        return _BLOCKS[-1] * width
    partial = _BLOCKS[int((filled - full) * 8) - 1] if int((filled - full) * 8) else ""
    return (_BLOCKS[-1] * full + partial).ljust(width)


def _n(value: float | None, digits: int = 0) -> str:
    return "-" if value is None else f"{value:,.{digits}f}"


def _mb(value: int | None) -> str:
    return "-" if value is None else f"{value / (1024 * 1024):,.1f} MB"


def _rows(title: str, pairs: list[tuple[str, int]], denominator: int) -> str:
    """One breakdown panel: label, bar, count, share."""
    if not pairs:
        return f"{title}\n  (nothing to show)\n"
    peak = max(n for _, n in pairs)
    lines = [title]
    for label, n in pairs:
        pct = 100.0 * n / denominator if denominator else 0.0
        lines.append(f"  {label:<28.28} {bar(n, peak)} {n:>6,} {pct:>5.1f}%")
    return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """Everything the Overview tab renders, computed off the UI thread."""

    headline: str
    panels: str
    sparkline: tuple[float, ...]
    sparkline_title: str


def _collect(db_path: Path, out_dir: Path) -> _Snapshot:
    """Query the corpus and format it. Runs in a worker thread."""
    overview = stats.overview(db_path, out_dir)
    fills = stats.fill_rates(db_path)
    tools = stats.technical_tools(db_path, limit=15)
    countries = stats.workplace_countries(db_path, limit=8)
    comp = stats.compensation(db_path)
    recent = stats.recency(db_path, months=24)

    headline = "\n".join(
        (
            f"{_n(overview.total_rows)} rows: "
            f"{_n(overview.active_rows)} active, "
            f"{_n(overview.expired_rows)} expired",
            f"{_n(overview.active_companies)} companies, "
            f"{_n(overview.active_sources)} sources (active jobs)",
            f"Active published {overview.earliest_active or '?'} "
            f"to {overview.latest_active or '?'}",
            f"duckdb {_mb(overview.duckdb_bytes)} | "
            f"sqlite {_mb(overview.sqlite_bytes)} | "
            f"parquet {_mb(overview.parquet_bytes)}",
            # Not a count of raw files on disk: entries for deleted inputs
            # linger by design until the next full rebuild.
            f"Manifest: {_n(overview.manifest_entries)} remembered files, "
            f"last written {overview.manifest_mtime or 'never'}",
        )
    )

    panels = [
        _rows(
            f"Tools ({_n(tools.total_mentions)} mentions across "
            f"{_n(tools.jobs_with_value)} jobs; share of those jobs)",
            [(b.label, b.count) for b in tools.buckets],
            tools.jobs_with_value,
        )
    ]
    for column, limit in (
        ("job_category", 10),
        ("seniority_level", 8),
        ("workplace_type", 6),
        ("company_name", 10),
    ):
        bd = stats.breakdown(db_path, column, limit=limit)
        panels.append(
            _rows(
                f"{column} (top {limit} of {_n(bd.total)}; "
                f"{_n(bd.other)} in other values)",
                [(b.label, b.count) for b in bd.buckets],
                bd.total,
            )
        )
    panels.append(
        _rows(
            f"Countries ({_n(countries.jobs_with_value)} jobs list one)",
            [(b.label, b.count) for b in countries.buckets],
            countries.jobs_with_value,
        )
    )
    panels.append(_compensation_panel(comp))
    panels.append(
        "\n".join(
            (
                "Fill rates (active jobs)",
                *(
                    f"  {f.column:<28.28} {bar(f.filled, f.total)} "
                    f"{f.filled:>6,} {f.pct:>5.1f}%"
                    for f in fills
                ),
            )
        )
        + "\n"
    )

    dated = sum(b.count for b in recent.buckets)
    return _Snapshot(
        headline=headline,
        panels="\n".join(panels),
        sparkline=tuple(float(b.count) for b in recent.buckets),
        sparkline_title=(
            f"Published per month, {recent.buckets[0].label} to "
            f"{recent.buckets[-1].label}: {_n(dated)} in window, "
            f"{_n(recent.older)} older, {_n(recent.undated)} undated"
        ),
    )


def _compensation_panel(comp: stats.Compensation) -> str:
    """Currencies side by side; never pooled, never converted."""
    pct = (
        100.0 * comp.jobs_with_compensation / comp.active_jobs
        if comp.active_jobs
        else 0.0
    )
    lines = [
        f"Annual compensation ({_n(comp.jobs_with_compensation)} of "
        f"{_n(comp.active_jobs)} active jobs, {pct:.1f}%; no conversion)",
        f"  {'currency':<10}{'jobs':>7}{'min p25':>12}{'min med':>12}"
        f"{'min p75':>12}{'max med':>12}",
    ]
    for c in comp.currencies:
        lines.append(
            f"  {c.currency:<10.10}{c.jobs:>7,}{_n(c.min_p25):>12}"
            f"{_n(c.min_median):>12}{_n(c.min_p75):>12}{_n(c.max_median):>12}"
        )
    if not comp.currencies:
        lines.append("  (no job lists a positive annual figure)")
    return "\n".join(lines) + "\n"


class Dashboard(App[None]):
    """The one screen."""

    CSS_PATH = "dashboard.tcss"
    TITLE = "job-ingest"

    BINDINGS = [
        Binding("1", "show_tab('tab-overview')", "Overview"),
        Binding("2", "show_tab('tab-run')", "Run"),
        Binding("3", "show_tab('tab-browse')", "Browse"),
        Binding("r", "refresh_stats", "Refresh"),
        Binding("n", "next_page", "Next page"),
        Binding("p", "prev_page", "Prev page"),
        Binding("q", "quit", "Quit"),
    ]

    #: True while a pipeline operation (ingest or scrape) is running.
    busy: reactive[bool] = reactive(False)
    #: Only a scrape can be cancelled; an ingest cannot, so we do not pretend.
    cancellable: reactive[bool] = reactive(False)
    #: Browse pagination offset.
    offset: reactive[int] = reactive(0)

    # ---------------------------------------------------------- messages
    class StatsLoaded(Message):
        def __init__(self, snapshot: _Snapshot) -> None:
            super().__init__()
            self.snapshot = snapshot

    class StatsFailed(Message):
        def __init__(self, error: str) -> None:
            super().__init__()
            self.error = error

    class PageLoaded(Message):
        def __init__(self, page: stats.Page) -> None:
            super().__init__()
            self.page = page

    class PageFailed(Message):
        def __init__(self, error: str) -> None:
            super().__init__()
            self.error = error

    class IngestFinished(Message):
        def __init__(self, result: IngestResult) -> None:
            super().__init__()
            self.result = result

    class IngestFailed(Message):
        def __init__(self, error: str) -> None:
            super().__init__()
            self.error = error

    class ScrapeProgress(Message):
        def __init__(self, event: ScrapeEvent) -> None:
            super().__init__()
            self.event = event

    class ScrapeFinished(Message):
        def __init__(self, jobs: int) -> None:
            super().__init__()
            self.jobs = jobs

    class ScrapeFailed(Message):
        def __init__(self, error: str) -> None:
            super().__init__()
            self.error = error

    def __init__(
        self,
        *,
        db_path: Path,
        out_dir: Path = DEFAULT_OUT_DIR,
        json_dir: Path = DEFAULT_JSON_DIR,
        raw_dir: Path = DEFAULT_RAW_DIR,
        interim_dir: Path = DEFAULT_INTERIM_DIR,
    ) -> None:
        super().__init__()
        # No stat() here: data/ is gitignored, so a fresh clone has no DB and
        # constructing the app must still work.
        self.db_path = db_path
        self.out_dir = out_dir
        self.json_dir = json_dir
        self.raw_dir = raw_dir
        self.interim_dir = interim_dir
        # Serialises every corpus read against this process's own DuckDB
        # writer. Short-lived connections alone cannot close that race.
        self._db_lock = threading.Lock()
        self._cancel = threading.Event()
        self._page_total = 0

    # ------------------------------------------------------------ layout
    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(initial="tab-overview", id="tabs"):
            with TabPane("Overview", id="tab-overview"):
                yield Static(id="headline")
                with VerticalScroll(id="overview-scroll"):
                    yield Static(id="spark-title")
                    yield Sparkline([], id="spark")
                    yield Static(id="panels")
            with TabPane("Run", id="tab-run"):
                yield Static(id="paths")
                with Horizontal(id="run-panels"):
                    with Vertical(id="ingest-panel"):
                        yield Label("Ingest")
                        yield Checkbox("Full rebuild", id="ingest-full")
                        yield Checkbox("Also write Parquet", id="ingest-parquet")
                        yield Input(
                            placeholder="limit (blank = all)", id="ingest-limit"
                        )
                        yield Button("Run ingest", id="run-ingest", variant="primary")
                    with Vertical(id="scrape-panel"):
                        yield Label("Scrape")
                        yield Select(
                            [(s.label, s.key) for s in SAVED_PRESETS]
                            + [("Custom", CUSTOM)],
                            value=SAVED_PRESETS[0].key,
                            allow_blank=False,
                            id="preset",
                        )
                        yield Static(id="preset-summary")
                        yield Static(SHARED_FILTERS, id="shared-filters")
                        yield Input(placeholder="custom query", id="query")
                        yield Input(placeholder="custom hiringcafe URL", id="url")
                        yield Input(placeholder="custom searchState JSON", id="state")
                        with Horizontal(id="scrape-numbers"):
                            yield Input(
                                value="40", placeholder="max jobs", id="max-jobs"
                            )
                            yield Input(
                                value="50", placeholder="max pages", id="max-pages"
                            )
                            yield Input(value="1.0", placeholder="delay s", id="delay")
                        yield Checkbox("Write raw pages", id="write-raw")
                        yield Checkbox("Refetch cached pages", id="refresh")
                        yield Checkbox(
                            "Skip jobs already staged",
                            id="skip-existing-raw",
                            disabled=True,
                        )
                        yield Button("Run scrape", id="run-scrape", variant="primary")
                with Horizontal(id="run-status"):
                    yield ProgressBar(id="progress", show_eta=False)
                    yield Button("Cancel scrape", id="cancel", variant="warning")
                yield RichLog(id="log", markup=False, highlight=False, wrap=True)
                yield Static(id="summary")
            with TabPane("Browse", id="tab-browse"):
                with Horizontal(id="browse-controls"):
                    yield Input(
                        placeholder="search title / company / location", id="search"
                    )
                    yield Checkbox("Include expired", id="include-expired")
                yield DataTable(id="results", cursor_type="row")
                yield Static(id="page-info")
                with Horizontal(id="browse-nav"):
                    yield Button("Previous", id="prev")
                    yield Button("Next", id="next")
                yield Static(id="detail")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#results", DataTable)
        for column in stats.JOB_ROW_COLUMNS:
            table.add_column(column.replace("_", " "), key=column)
        self.query_one("#paths", Static).update(self._paths_note())
        self._show_preset_summary(SAVED_PRESETS[0].key)
        self._set_custom_inputs_enabled(False)
        self.busy = False
        self.cancellable = False
        self.refresh_stats()
        self.load_page()

    def _paths_note(self) -> str:
        """Both resolved paths, because they intentionally differ for now.

        New scrapes are staged in data/raw/_json while the established ingest
        still reads the data/raw/json symlink, so a default scrape followed
        by a default ingest does NOT yet consume the new files. Ingesting
        staged pages is an explicit --json-dir choice; silently merging the
        two roots would let the filename-keyed manifest overwrite newer rows
        with older copies.
        """
        return (
            f"ingest reads {self.json_dir.resolve()}  |  "
            f"scrape writes {self.raw_dir.resolve()}\n"
            f"Board archive/cache root {self.interim_dir.resolve()}\n"
            f"corpus DB    {self.db_path.resolve()}\n"
            "Those differ on purpose: a default scrape is staged, not "
            "ingested. Point --json-dir at it to ingest it."
        )

    # ----------------------------------------------------------- reactive
    def watch_busy(self, busy: bool) -> None:
        for button_id in ("run-ingest", "run-scrape"):
            self.query_one(f"#{button_id}", Button).disabled = busy
        # A corpus read must not overlap this process's own DuckDB writer.
        for widget_id in ("prev", "next"):
            self.query_one(f"#{widget_id}", Button).disabled = busy
        self.query_one("#search", Input).disabled = busy
        for widget_id in ("preset", "query", "url", "state"):
            self.query_one(f"#{widget_id}").disabled = busy or (
                widget_id != "preset" and not self._custom_selected()
            )

    def watch_cancellable(self, cancellable: bool) -> None:
        self.query_one("#cancel", Button).disabled = not cancellable

    # ------------------------------------------------------------ actions
    def action_show_tab(self, tab: str) -> None:
        self.query_one("#tabs", TabbedContent).active = tab

    def action_refresh_stats(self) -> None:
        self.refresh_stats()

    def action_next_page(self) -> None:
        if not self.busy and self.offset + PAGE_SIZE < self._page_total:
            self.offset += PAGE_SIZE
            self.load_page()

    def action_prev_page(self) -> None:
        if not self.busy and self.offset:
            self.offset = max(0, self.offset - PAGE_SIZE)
            self.load_page()

    def action_cancel(self) -> None:
        """Cooperative: the run stops after the request already in flight."""
        if self.cancellable:
            self._cancel.set()
            self.log_line(
                "Cancellation requested; stopping after the current request..."
            )

    # ----------------------------------------------------------- handlers
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-ingest":
            self.start_ingest()
        elif event.button.id == "run-scrape":
            self.start_scrape()
        elif event.button.id == "cancel":
            self.action_cancel()
        elif event.button.id == "next":
            self.action_next_page()
        elif event.button.id == "prev":
            self.action_prev_page()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "preset":
            return
        key = str(event.value)
        self._set_custom_inputs_enabled(key == CUSTOM)
        self._show_preset_summary(key)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search" and not self.busy:
            self.offset = 0
            self.load_page()

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if event.checkbox.id == "include-expired" and not self.busy:
            self.offset = 0
            self.load_page()
        elif event.checkbox.id == "write-raw":
            skip = self.query_one("#skip-existing-raw", Checkbox)
            skip.disabled = not event.value
            if not event.value:
                skip.value = False
            # A search hit has no description, so a raw page cannot be
            # synthesised without one. Keep the two settings consistent.
            self.query_one(
                "#write-raw", Checkbox
            ).tooltip = f"Stages ingest-compatible pages in {self.raw_dir}"

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        row = self.query_one("#results", DataTable).get_row(event.row_key)
        # Text(), not markup: a posting containing [bold] must not style.
        self.query_one("#detail", Static).update(
            Text(
                "\n".join(
                    f"{name}: {value}"
                    for name, value in zip(stats.JOB_ROW_COLUMNS, row, strict=False)
                )
            )
        )

    # ------------------------------------------------------------ ingest
    def start_ingest(self) -> None:
        # Re-checked here, not just via the disabled button: exclusive=True
        # would try to cancel the running worker rather than refuse.
        if self.busy:
            self.notify("A pipeline operation is already running.", severity="warning")
            return
        raw_limit = self.query_one("#ingest-limit", Input).value.strip()
        limit: int | None = None
        if raw_limit:
            try:
                limit = int(raw_limit)
            except ValueError:
                self.notify(
                    f"limit must be a number, got {raw_limit!r}", severity="error"
                )
                return
            if limit <= 0:
                self.notify("limit must be positive.", severity="error")
                return
        self.busy = True
        self.cancellable = False  # ingest is not cancellable; do not pretend
        self.query_one("#summary", Static).update("")
        self.log_line("--- ingest ---")
        self._ingest_worker(
            limit=limit,
            full=self.query_one("#ingest-full", Checkbox).value,
            parquet=self.query_one("#ingest-parquet", Checkbox).value,
        )

    @work(thread=True, exclusive=True, group="pipeline", exit_on_error=False)
    def _ingest_worker(self, *, limit: int | None, full: bool, parquet: bool) -> None:
        try:
            # The lock covers the whole call: load_duckdb() opens the corpus
            # read-write in this very process.
            with self._db_lock:
                result = run_ingest(
                    self.json_dir,
                    self.out_dir,
                    limit=limit,
                    full=full,
                    parquet=parquet,
                    on_event=lambda msg: self.call_from_thread(self.log_line, msg),
                )
            self.post_message(self.IngestFinished(result))
        except IngestError as e:
            self.post_message(self.IngestFailed(str(e)))
        except Exception as e:
            # exit_on_error=False plus this boundary: an unexpected exception
            # becomes a red log line, not a dead app.
            self.post_message(self.IngestFailed(f"unexpected {type(e).__name__}: {e}"))

    def on_dashboard_ingest_finished(self, message: Dashboard.IngestFinished) -> None:
        result = message.result
        self.query_one("#summary", Static).update(
            f"{'full rebuild' if result.full else 'incremental'}: "
            f"{result.ok:,} rows ok, {result.errors:,} errors, "
            f"{result.skipped:,} skipped of {result.files:,} files\n"
            f"parse {result.parse_sec:.2f}s | "
            f"sqlite {result.sqlite_insert_sec:.2f}s ({result.sqlite_total_rows:,} rows) "
            f"| duckdb {result.duckdb_insert_sec:.2f}s "
            f"({result.duckdb_total_rows:,} rows)"
        )
        self.log_line("ingest finished")
        self._finish_operation()
        # Only now: the writer has released the lock.
        self.refresh_stats()
        self.load_page()

    def on_dashboard_ingest_failed(self, message: Dashboard.IngestFailed) -> None:
        self.log_line(f"INGEST FAILED: {message.error}")
        self.notify(message.error, severity="error", title="Ingest failed")
        self._finish_operation()

    # ------------------------------------------------------------ scrape
    def start_scrape(self) -> None:
        if self.busy:
            self.notify("A pipeline operation is already running.", severity="warning")
            return
        try:
            config = self._scrape_config()
        except (ScrapeError, ValueError) as e:
            self.notify(str(e), severity="error", title="Cannot start scrape")
            return
        key = str(self.query_one("#preset", Select).value)
        self.log_line("--- scrape ---")
        if key != CUSTOM:
            chosen = saved_search(key)
            # The stable key and summary; never the multi-kilobyte URL.
            self.log_line(f"preset {chosen.key}: {chosen.summary}")
        if config.raw_dir is not None:
            self.log_line(f"writing raw pages to {config.raw_dir.resolve()}")
        if config.archive_dir is not None:
            self.log_line(f"Board archive/cache: {config.archive_dir}")
        self._cancel = threading.Event()
        self.busy = True
        self.cancellable = True
        self.query_one("#progress", ProgressBar).update(
            total=config.max_jobs, progress=0
        )
        self._scrape_worker(config)

    def _scrape_config(self) -> ScrapeConfig:
        key = str(self.query_one("#preset", Select).value)
        if key == CUSTOM:
            source = resolve_source(
                query=self.query_one("#query", Input).value.strip() or None,
                url=self.query_one("#url", Input).value.strip() or None,
                search_state=self.query_one("#state", Input).value.strip() or None,
                on_warning=self.log_line,
            )
        else:
            source = saved_search(key).source()
        return ScrapeConfig(
            source=source,
            interim_dir=self.interim_dir,
            refresh=self.query_one("#refresh", Checkbox).value,
            skip_existing_raw=self.query_one("#skip-existing-raw", Checkbox).value,
            max_jobs=self._int_input("max-jobs"),
            max_pages=self._int_input("max-pages"),
            delay=self._float_input("delay"),
            descriptions=True,
            raw_dir=(
                self.raw_dir if self.query_one("#write-raw", Checkbox).value else None
            ),
        )

    def _int_input(self, widget_id: str) -> int:
        raw = self.query_one(f"#{widget_id}", Input).value.strip()
        try:
            return int(raw)
        except ValueError as e:
            raise ScrapeError(f"{widget_id} must be a whole number, got {raw!r}") from e

    def _float_input(self, widget_id: str) -> float:
        raw = self.query_one(f"#{widget_id}", Input).value.strip()
        try:
            return float(raw)
        except ValueError as e:
            raise ScrapeError(f"{widget_id} must be a number, got {raw!r}") from e

    @work(thread=True, exclusive=True, group="pipeline", exit_on_error=False)
    def _scrape_worker(self, config: ScrapeConfig) -> None:
        client = Client(config.delay, cancel=self._cancel)
        events = None
        jobs = 0
        try:
            events = scrape(config, client)
            for event in events:
                if event.kind == "job":
                    jobs = event.index
                self.post_message(self.ScrapeProgress(event))
            self.post_message(self.ScrapeFinished(jobs))
        except ScrapeError as e:
            self.post_message(self.ScrapeFailed(str(e)))
        except Exception as e:
            self.post_message(self.ScrapeFailed(f"unexpected {type(e).__name__}: {e}"))
        finally:
            if events is not None:
                # Explicit, so GeneratorExit runs here rather than at GC time.
                events.close()

    def on_dashboard_scrape_progress(self, message: Dashboard.ScrapeProgress) -> None:
        event = message.event
        # log_line wraps in Text(): the message embeds job titles.
        self.log_line(event.message)
        if event.kind == "job" and event.total:
            self.query_one("#progress", ProgressBar).update(
                total=event.total, progress=event.index
            )

    def on_dashboard_scrape_finished(self, message: Dashboard.ScrapeFinished) -> None:
        self.query_one("#summary", Static).update(
            f"scrape finished: {message.jobs:,} jobs"
        )
        self._finish_operation()

    def on_dashboard_scrape_failed(self, message: Dashboard.ScrapeFailed) -> None:
        self.log_line(f"SCRAPE FAILED: {message.error}")
        self.notify(message.error, severity="error", title="Scrape failed")
        self._finish_operation()

    def _finish_operation(self) -> None:
        self.busy = False
        self.cancellable = False

    # ------------------------------------------------------------- stats
    @work(thread=True, group="stats", exit_on_error=False)
    def refresh_stats(self) -> None:
        try:
            with self._db_lock:
                snapshot = _collect(self.db_path, self.out_dir)
            self.post_message(self.StatsLoaded(snapshot))
        except stats.StatsUnavailable as e:
            self.post_message(self.StatsFailed(str(e)))
        except Exception as e:
            self.post_message(self.StatsFailed(f"unexpected {type(e).__name__}: {e}"))

    def on_dashboard_stats_loaded(self, message: Dashboard.StatsLoaded) -> None:
        snapshot = message.snapshot
        self.query_one("#headline", Static).update(snapshot.headline)
        self.query_one("#spark-title", Static).update(snapshot.sparkline_title)
        self.query_one("#spark", Sparkline).data = list(snapshot.sparkline)
        self.query_one("#panels", Static).update(snapshot.panels)

    def on_dashboard_stats_failed(self, message: Dashboard.StatsFailed) -> None:
        self.query_one("#headline", Static).update(message.error)
        self.query_one("#panels", Static).update("")
        self.query_one("#spark-title", Static).update("")

    # ------------------------------------------------------------ browse
    @work(thread=True, group="browse", exit_on_error=False)
    def load_page(self) -> None:
        query = self.query_one("#search", Input).value
        include_expired = self.query_one("#include-expired", Checkbox).value
        offset = self.offset
        try:
            with self._db_lock:
                page = stats.search(
                    self.db_path,
                    query=query,
                    limit=PAGE_SIZE,
                    offset=offset,
                    include_expired=include_expired,
                )
            self.post_message(self.PageLoaded(page))
        except stats.StatsUnavailable as e:
            self.post_message(self.PageFailed(str(e)))
        except Exception as e:
            self.post_message(self.PageFailed(f"unexpected {type(e).__name__}: {e}"))

    def on_dashboard_page_loaded(self, message: Dashboard.PageLoaded) -> None:
        page = message.page
        self._page_total = page.total
        table = self.query_one("#results", DataTable)
        table.clear()
        for row in page.rows:
            # Text() for every job-originated value: DataTable interprets
            # markup in plain strings.
            table.add_row(
                *(
                    Text("" if value is None else str(value))
                    for value in (
                        row.requisition_id,
                        row.title,
                        row.company_name,
                        row.formatted_workplace_location,
                        row.workplace_type,
                        row.yearly_min_compensation,
                        row.yearly_max_compensation,
                        row.listed_compensation_currency,
                        row.estimated_publish_date,
                    )
                )
            )
        first = page.offset + 1 if page.rows else 0
        self.query_one("#page-info", Static).update(
            f"showing {first:,}-{page.offset + len(page.rows):,} of {page.total:,}"
        )

    def on_dashboard_page_failed(self, message: Dashboard.PageFailed) -> None:
        self.query_one("#results", DataTable).clear()
        self.query_one("#page-info", Static).update(message.error)

    # ------------------------------------------------------------ helpers
    def log_line(self, message: str) -> None:
        """Append one line to the shared log.

        Wraps in Text() because the line may quote a job title or a sidecar
        error naming a file, and RichLog would otherwise be free to read
        square brackets in it as style tags. Also returns None, which
        `run_ingest(on_event=...)` requires -- RichLog.write returns self.
        """
        self.query_one("#log", RichLog).write(Text(message))

    def _custom_selected(self) -> bool:
        return str(self.query_one("#preset", Select).value) == CUSTOM

    def _set_custom_inputs_enabled(self, enabled: bool) -> None:
        for widget_id in ("query", "url", "state"):
            self.query_one(f"#{widget_id}", Input).disabled = not enabled

    def _show_preset_summary(self, key: str) -> None:
        summary = (
            "Custom: fill in at most one of query, URL or raw searchState."
            if key == CUSTOM
            else saved_search(key).summary
        )
        self.query_one("#preset-summary", Static).update(summary)
        self.query_one("#shared-filters", Static).display = (
            key != CUSTOM and saved_search(key).kind == "search"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Textual dashboard for job-ingest")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="where the corpus databases live",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="corpus DuckDB file (default: <out-dir>/jobs.duckdb)",
    )
    parser.add_argument(
        "--json-dir",
        type=Path,
        default=DEFAULT_JSON_DIR,
        help="what an ingest reads",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help="where a scrape stages raw pages",
    )
    parser.add_argument(
        "--interim-dir",
        type=Path,
        default=DEFAULT_INTERIM_DIR,
        help="Board archive and cache root",
    )
    args = parser.parse_args()
    Dashboard(
        db_path=args.db if args.db is not None else args.out_dir / "jobs.duckdb",
        out_dir=args.out_dir,
        json_dir=args.json_dir,
        raw_dir=args.raw_dir,
        interim_dir=args.interim_dir,
    ).run()


if __name__ == "__main__":
    main()
