"""Tests for the Textual dashboard.

No pytest-asyncio and no anyio, deliberately. Textual's `App.run_test()` is
a coroutine, but wrapping `asyncio.run()` around a private `_drive_*`
coroutine keeps the *test function* synchronous, so plain pytest collects
and runs it. That is the whole reason neither plugin is a dependency; see
the Development section of the README.

Every test passes `size=(120, 40)`. The 80x24 default clips a three-pane
layout badly enough that a DataTable can render zero rows, which would make
these assertions lie.

These assert wiring, not pixels: that a worker's result reaches the right
widget, that a failure lands in the log instead of killing the app, and
that the busy interlock holds. No test starts a real scrape or a real cargo
build -- `just check` runs pytest with no timeout, so a dashboard test that
touched the network could hang CI forever.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest

from textual.widgets import TabbedContent

from job_ingest.dashboard import CUSTOM, PAGE_SIZE, Dashboard, bar
from job_ingest.ingest_and_benchmark import DUCKDB_DDL, IngestError, IngestResult
from job_ingest.scrape_hiringcafe import SAVED_SEARCHES, ScrapeConfig
from job_ingest.stats import JOB_ROW_COLUMNS

CANNED = IngestResult(
    full=True,
    files=9,
    skipped=0,
    parsed=9,
    ok=7,
    errors=2,
    error_samples=("bad.json.gz: nope",),
    parse_sec=0.5,
    sqlite_insert_sec=0.1,
    sqlite_total_rows=7,
    duckdb_insert_sec=0.2,
    duckdb_total_rows=7,
    parquet_sec=None,
    sqlite_bytes=1024,
    duckdb_bytes=2048,
    parquet_bytes=None,
)


def a_corpus(tmp_path: Path, rows: int = 3, expired: int = 1) -> Path:
    """A tiny populated DuckDB built from the real DDL."""
    db_path = tmp_path / "jobs.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(DUCKDB_DDL)
        con.executemany(
            "INSERT INTO jobs (requisition_id, title, company_name, is_expired, "
            "workplace_type, technical_tools, workplace_countries, "
            "estimated_publish_date, job_category, seniority_level) "
            "VALUES (?, ?, ?, ?, 'Remote', '[\"Python\"]', '[\"US\"]', "
            "'2026-07-01T00:00:00Z', 'Data', 'Mid')",
            [
                [f"req-{i}", f"Job {i}", "Acme", i >= rows - expired]
                for i in range(rows)
            ],
        )
    finally:
        con.close()
    return db_path


def drive(coro: Callable[[], Awaitable[None]]) -> None:
    """Run one async driver from a synchronous test function."""
    asyncio.run(coro())


async def show(app: Dashboard, pilot: Any, tab: str) -> None:
    """Bring a tab to the front.

    All three widget trees stay mounted -- that is the point of TabbedContent
    over separate Screens -- but a widget in a background pane is not
    visible, and Pilot.click() on an invisible widget does nothing.
    """
    app.query_one("#tabs", TabbedContent).active = tab
    await pilot.pause()


class TestBar:
    def test_it_fills_proportionally(self) -> None:
        assert bar(10, 10, width=8) == "█" * 8
        assert bar(0, 10, width=8) == " " * 8
        assert len(bar(3, 10, width=8)) == 8

    def test_a_zero_peak_does_not_divide_by_zero(self) -> None:
        assert bar(0, 0, width=4) == "    "


def test_the_stylesheet_ships_next_to_the_module() -> None:
    """CSS_PATH resolves relative to the module and uv_build includes files
    under the module directory, so an installed copy needs this file to be
    there. `just check` never builds a wheel, so nothing else would notice."""
    import job_ingest.dashboard as module

    assert (Path(module.__file__).parent / Dashboard.CSS_PATH).is_file()


def test_it_mounts_against_a_populated_corpus(tmp_path: Path) -> None:
    drive(lambda: _drive_populated(tmp_path))


async def _drive_populated(tmp_path: Path) -> None:
    db_path = a_corpus(tmp_path, rows=3, expired=1)
    app = Dashboard(db_path=db_path, out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await pilot.pause()

        headline = str(app.query_one("#headline").render())
        assert "3 rows" in headline
        assert "2 active" in headline
        assert "1 expired" in headline


def test_it_mounts_against_a_missing_corpus(tmp_path: Path) -> None:
    """The most likely real-world failure: data/ is gitignored, so a fresh
    clone has no database at all. Constructing and mounting must still work
    and show the StatsUnavailable message rather than crash."""
    drive(lambda: _drive_missing(tmp_path))


async def _drive_missing(tmp_path: Path) -> None:
    app = Dashboard(db_path=tmp_path / "absent.duckdb", out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await pilot.pause()

        assert app.is_running
        assert "No corpus database" in str(app.query_one("#headline").render())
        assert "No corpus database" in str(app.query_one("#page-info").render())


def test_browse_pages_through_active_rows(tmp_path: Path) -> None:
    drive(lambda: _drive_browse(tmp_path))


async def _drive_browse(tmp_path: Path) -> None:
    db_path = a_corpus(tmp_path, rows=5, expired=2)
    app = Dashboard(db_path=db_path, out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-browse")
        table = app.query_one("#results")

        assert len(table.columns) == len(JOB_ROW_COLUMNS)
        assert table.row_count == 3, "active jobs only, by default"
        assert "of 3" in str(app.query_one("#page-info").render())

        # The toggle is the only way to see expired rows.
        await pilot.click("#include-expired")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert table.row_count == 5


def test_the_next_page_button_advances_the_offset(tmp_path: Path) -> None:
    drive(lambda: _drive_paging(tmp_path))


async def _drive_paging(tmp_path: Path) -> None:
    db_path = a_corpus(tmp_path, rows=3, expired=0)
    app = Dashboard(db_path=db_path, out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-browse")

        # Only one page of results exists, so paging forward is a no-op
        # rather than running off the end. Driven through the action because
        # two Pilot clicks on one widget chain into a double-click and the
        # second Button.Pressed never fires.
        app.action_next_page()
        await pilot.pause()
        assert app.offset == 0

        # Pretend the corpus is three pages deep, then drive the buttons.
        app._page_total = PAGE_SIZE * 3
        await pilot.click("#next")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.offset == PAGE_SIZE

        await pilot.click("#prev")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.offset == 0


def test_a_successful_ingest_reaches_the_summary_and_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worker -> message -> widget, with zero subprocess or Rust dependency."""
    drive(lambda: _drive_ingest_ok(tmp_path, monkeypatch))


async def _drive_ingest_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def stub_run_ingest(*_args: Any, **kwargs: Any) -> IngestResult:
        kwargs["on_event"]("Loading into DuckDB...")
        return CANNED

    monkeypatch.setattr("job_ingest.dashboard.run_ingest", stub_run_ingest)
    db_path = a_corpus(tmp_path)
    app = Dashboard(db_path=db_path, out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        await pilot.click("#run-ingest")
        await app.workers.wait_for_complete()
        await pilot.pause()

        summary = str(app.query_one("#summary").render())
        assert "7 rows ok" in summary
        assert "2 errors" in summary
        lines = "\n".join(str(line) for line in app.query_one("#log").lines)
        assert "Loading into DuckDB" in lines
        assert app.busy is False


def test_a_failing_ingest_does_not_tear_the_app_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proves exit_on_error=False is wired: the worker's default would take
    the whole app with it."""
    drive(lambda: _drive_ingest_fail(tmp_path, monkeypatch))


async def _drive_ingest_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_args: Any, **_kwargs: Any) -> IngestResult:
        raise IngestError("duckdb is locked by another process")

    monkeypatch.setattr("job_ingest.dashboard.run_ingest", boom)
    app = Dashboard(db_path=a_corpus(tmp_path), out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        await pilot.click("#run-ingest")
        await app.workers.wait_for_complete()
        await pilot.pause()

        assert app.is_running
        lines = "\n".join(str(line) for line in app.query_one("#log").lines)
        assert "INGEST FAILED" in lines
        assert "locked" in lines
        assert app.busy is False


def test_an_unexpected_exception_also_becomes_a_log_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The final safety boundary: an ordinary exception from library code
    must not terminate the app either."""
    drive(lambda: _drive_ingest_surprise(tmp_path, monkeypatch))


async def _drive_ingest_surprise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_args: Any, **_kwargs: Any) -> IngestResult:
        raise ZeroDivisionError("surprise")

    monkeypatch.setattr("job_ingest.dashboard.run_ingest", boom)
    app = Dashboard(db_path=a_corpus(tmp_path), out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        await pilot.click("#run-ingest")
        await app.workers.wait_for_complete()
        await pilot.pause()

        assert app.is_running
        lines = "\n".join(str(line) for line in app.query_one("#log").lines)
        assert "ZeroDivisionError" in lines


def test_only_one_operation_may_run_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive(lambda: _drive_interlock(tmp_path, monkeypatch))


async def _drive_interlock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    loop = asyncio.get_running_loop()
    calls = 0

    def slow_run_ingest(*_args: Any, **_kwargs: Any) -> IngestResult:
        nonlocal calls
        calls += 1
        loop.call_soon_threadsafe(started.set)
        # Blocks the worker thread until the test lets go, without sleeping.
        asyncio.run_coroutine_threadsafe(release.wait(), loop).result()
        return CANNED

    monkeypatch.setattr("job_ingest.dashboard.run_ingest", slow_run_ingest)
    app = Dashboard(db_path=a_corpus(tmp_path), out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        await pilot.click("#run-ingest")
        await started.wait()
        await pilot.pause()

        assert app.busy is True
        assert app.query_one("#run-ingest").disabled
        assert app.query_one("#run-scrape").disabled
        # Cancel is enabled only for a scrape: an ingest cannot be
        # interrupted, so the button must not claim otherwise.
        assert app.query_one("#cancel").disabled
        # The handler re-checks the flag rather than trusting the button;
        # exclusive=True alone would try to cancel the running worker.
        app.start_ingest()
        assert calls == 1

        release.set()
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.busy is False


def test_the_scrape_selector_offers_the_four_presets_plus_custom(
    tmp_path: Path,
) -> None:
    drive(lambda: _drive_presets(tmp_path))


async def _drive_presets(tmp_path: Path) -> None:
    app = Dashboard(db_path=a_corpus(tmp_path), out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        select = app.query_one("#preset")

        values = [value for _label, value in select._options]
        assert values == [s.key for s in SAVED_SEARCHES] + [CUSTOM]
        assert select.value == SAVED_SEARCHES[0].key
        assert SAVED_SEARCHES[0].summary in str(
            app.query_one("#preset-summary").render()
        )
        # A preset owns the search, so the custom inputs are inert.
        assert app.query_one("#query").disabled

        select.value = CUSTOM
        await pilot.pause()
        assert "Custom" in str(app.query_one("#preset-summary").render())
        assert not app.query_one("#query").disabled


def test_the_worker_receives_the_presets_exact_decoded_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive(lambda: _drive_preset_state(tmp_path, monkeypatch))


async def _drive_preset_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[ScrapeConfig] = []

    def stub_scrape(config: ScrapeConfig, _client: Any = None) -> Any:
        seen.append(config)
        return iter(())

    monkeypatch.setattr("job_ingest.dashboard.scrape", stub_scrape)
    app = Dashboard(db_path=a_corpus(tmp_path), out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        app.query_one("#preset").value = "DA_Healthcare"
        await pilot.pause()
        await pilot.click("#run-scrape")
        await app.workers.wait_for_complete()
        await pilot.pause()

        assert len(seen) == 1
        state = seen[0].search_state
        assert state["departments"] == ["Data and Analytics"]
        assert state["industries"] == ["biotechnology", "healthcare"]
        assert state["dateFetchedPastNDays"] == 1440
        assert seen[0].raw_dir is None  # the checkbox is off by default
        lines = "\n".join(str(line) for line in app.query_one("#log").lines)
        assert "preset DA_Healthcare" in lines
        assert "hiringcafe.com/?searchState" not in lines, "never log the raw URL"


def test_custom_rejects_more_than_one_input_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive(lambda: _drive_custom_conflict(tmp_path, monkeypatch))


async def _drive_custom_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[ScrapeConfig] = []
    monkeypatch.setattr(
        "job_ingest.dashboard.scrape",
        lambda config, _client=None: (started.append(config), iter(()))[1],
    )
    app = Dashboard(db_path=a_corpus(tmp_path), out_dir=tmp_path)
    async with app.run_test(size=(120, 40)) as pilot:
        await app.workers.wait_for_complete()
        await show(app, pilot, "tab-run")
        app.query_one("#preset").value = CUSTOM
        await pilot.pause()
        app.query_one("#query").value = "data"
        app.query_one("#state").value = "{}"
        await pilot.click("#run-scrape")
        await pilot.pause()

        assert started == [], "the same precedence rules as the CLI apply"
        assert app.busy is False
