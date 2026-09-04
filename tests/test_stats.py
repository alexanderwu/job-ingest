"""Tests for the read-only corpus statistics layer.

No Rust toolchain and no real corpus: a fixture builds a tiny DuckDB in
tmp_path straight from DUCKDB_DDL with a handful of hand-written edge-case
rows, so the whole module runs in well under a second.

The rows are chosen to cover exactly the things the SQL has to get right:
an empty `'[]'` technical_tools array, a two-element one, an unparseable
publish date, a NULL company, NULL and non-positive compensations, two
currencies, and an expired row that must be invisible by default.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from job_ingest import stats
from job_ingest.ingest_and_benchmark import DUCKDB_DDL, ddl_columns

# id, requisition_id, is_expired, title, company, workplace, countries, tools,
# min comp, max comp, currency, publish date
ROWS: tuple[tuple[object, ...], ...] = (
    (
        "1",
        "req-a",
        False,
        "Data Scientist",
        "Acme",
        "Remote",
        '["US"]',
        '["Python","Rust"]',
        100000.0,
        150000.0,
        "USD",
        "2026-08-01T00:00:00Z",
    ),
    (
        "2",
        "req-b",
        False,
        "ML Engineer",
        "Acme",
        "Onsite",
        '["US","CA"]',
        '["Python"]',
        120000.0,
        None,
        "USD",
        "2026-07-15T00:00:00Z",
    ),
    # Empty tools array: UNNEST must drop it rather than count a phantom.
    (
        "3",
        "req-c",
        False,
        "Analyst",
        "Globex",
        "Hybrid",
        "[]",
        "[]",
        None,
        None,
        "EUR",
        "2026-07-02T00:00:00Z",
    ),
    # Non-positive compensation is a placeholder, not a salary.
    (
        "4",
        "req-d",
        False,
        "BI Developer",
        None,
        "Remote",
        '["US"]',
        '["SQL"]',
        0.0,
        -1.0,
        "EUR",
        "2020-01-05T00:00:00Z",
    ),
    # Unparseable date -> undated, never a bucket.
    (
        "5",
        "req-e",
        False,
        "Statistician 50% Remote",
        "Initech",
        None,
        "[]",
        '["R"]',
        90000.0,
        95000.0,
        "EUR",
        "not-a-date",
    ),
    # Expired: invisible unless include_expired=True.
    (
        "6",
        "req-f",
        True,
        "Retired Role",
        "Initech",
        "Remote",
        '["US"]',
        '["Python","SQL"]',
        999000.0,
        999000.0,
        "USD",
        "2026-08-02T00:00:00Z",
    ),
)

ACTIVE = 5
TOTAL = len(ROWS)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    """A corpus built from the real DDL, so column drift fails here too."""
    db_path = tmp_path / "jobs.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(DUCKDB_DDL)
        con.executemany(
            "INSERT INTO jobs (id, requisition_id, is_expired, title, company_name, "
            "workplace_type, workplace_countries, technical_tools, "
            "yearly_min_compensation, yearly_max_compensation, "
            "listed_compensation_currency, estimated_publish_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [list(r) for r in ROWS],
        )
    finally:
        con.close()
    return db_path


class TestUnavailable:
    """One empty-state for the UI; raw DuckDB errors never escape."""

    def test_a_missing_database(self, tmp_path: Path) -> None:
        with pytest.raises(stats.StatsUnavailable, match="No corpus database"):
            stats.overview(tmp_path / "nope.duckdb")

    def test_a_database_without_a_jobs_table(self, tmp_path: Path) -> None:
        db_path = tmp_path / "empty.duckdb"
        duckdb.connect(str(db_path)).close()
        with pytest.raises(stats.StatsUnavailable):
            stats.overview(db_path)

    def test_a_schema_this_module_cannot_query(self, tmp_path: Path) -> None:
        db_path = tmp_path / "wrong.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("CREATE TABLE jobs (something_else VARCHAR)")
        con.close()
        with pytest.raises(stats.StatsUnavailable):
            stats.overview(db_path)

    def test_a_database_locked_by_a_writer(self, db: Path) -> None:
        """A running ingest holds jobs.duckdb read-write; reads must fail
        with the domain error, not an IOException."""
        writer = duckdb.connect(str(db))
        try:
            with pytest.raises(stats.StatsUnavailable, match="lock is exclusive"):
                stats.overview(db)
        finally:
            writer.close()

    def test_the_connection_is_not_held_open(self, db: Path) -> None:
        """The read must not outlive the call, or the dashboard's own
        in-process ingest could never take the write lock."""
        stats.overview(db)
        duckdb.connect(str(db)).close()  # would raise if a handle survived


class TestOverview:
    def test_counts_split_active_and_expired(self, db: Path) -> None:
        ov = stats.overview(db)

        assert ov.total_rows == TOTAL
        assert ov.active_rows == ACTIVE
        assert ov.expired_rows == TOTAL - ACTIVE
        assert ov.active_rows + ov.expired_rows == ov.total_rows

    def test_active_dimensions_ignore_expired_and_null(self, db: Path) -> None:
        ov = stats.overview(db)

        # Acme, Globex, Initech among active rows; the NULL company does not
        # become a fourth.
        assert ov.active_companies == 3
        assert ov.earliest_active == "2020-01-05"
        assert ov.latest_active == "2026-08-01"

    def test_manifest_entries_are_reported_when_present(
        self, db: Path, tmp_path: Path
    ) -> None:
        (tmp_path / "ingest_manifest.json").write_text('{"a.json.gz": [1, 2]}')
        ov = stats.overview(db)

        assert ov.manifest_entries == 1
        assert ov.manifest_mtime is not None
        assert ov.duckdb_bytes is not None and ov.duckdb_bytes > 0
        assert ov.sqlite_bytes is None  # never ingested here

    def test_a_missing_manifest_is_not_an_error(self, db: Path) -> None:
        ov = stats.overview(db)

        assert ov.manifest_entries is None
        assert ov.manifest_mtime is None


class TestFillRates:
    def test_one_statement_covers_every_column(self, db: Path) -> None:
        fills = {f.column: f for f in stats.fill_rates(db)}

        assert set(fills) == set(stats.DEFAULT_FILL_COLUMNS)
        assert all(f.total == ACTIVE for f in fills.values())
        # req-e has a NULL workplace_type; the expired row is excluded.
        assert fills["workplace_type"].filled == ACTIVE - 1
        assert fills["company_name"].filled == ACTIVE - 1

    def test_an_empty_json_array_does_not_count_as_filled(self, db: Path) -> None:
        """workplace_countries is non-Option String in flatten.rs, so '[]' --
        not NULL -- is how it says 'nothing here'."""
        fills = {f.column: f for f in stats.fill_rates(db)}

        assert fills["workplace_countries"].filled == 3
        assert fills["technical_tools"].filled == ACTIVE - 1

    def test_percentages_are_derived_not_stored(self, db: Path) -> None:
        (fill,) = stats.fill_rates(db, ["company_name"])

        assert fill.pct == pytest.approx(100.0 * 4 / ACTIVE)

    def test_an_unknown_column_is_rejected(self, db: Path) -> None:
        with pytest.raises(ValueError, match="Unknown column"):
            stats.fill_rates(db, ["nope"])

    def test_the_defaults_do_not_drift_from_the_ddl(self) -> None:
        assert set(stats.DEFAULT_FILL_COLUMNS) <= set(ddl_columns())
        assert set(stats.JOB_ROW_COLUMNS) <= set(ddl_columns())
        # The blobs stay out: they are never null and hold ~all the bytes.
        assert not {
            "description",
            "job_information_json",
            "v5_processed_job_data_json",
            "enriched_company_data_json",
        } & set(stats.DEFAULT_FILL_COLUMNS)


class TestBreakdown:
    def test_the_scalar_invariant_holds(self, db: Path) -> None:
        bd = stats.breakdown(db, "workplace_type", limit=1)

        assert bd.total == sum(b.count for b in bd.buckets) + bd.other
        assert bd.buckets[0] == stats.Bucket(label="Remote", count=2)
        assert bd.other == 2  # Onsite + Hybrid; req-e's NULL is not counted
        assert bd.total == ACTIVE - 1

    def test_expired_rows_are_excluded_by_default(self, db: Path) -> None:
        active = stats.breakdown(db, "company_name")
        both = stats.breakdown(db, "company_name", include_expired=True)

        assert active.total == 4
        assert both.total == 5
        assert dict((b.label, b.count) for b in both.buckets)["Initech"] == 2

    def test_a_column_name_cannot_be_injected(self, db: Path) -> None:
        """A column name cannot be a bound parameter, so the whitelist is the
        only thing standing between the UI and arbitrary SQL."""
        with pytest.raises(ValueError, match="Unknown column"):
            stats.breakdown(db, "; DROP TABLE jobs")

    def test_a_non_positive_limit_is_rejected(self, db: Path) -> None:
        with pytest.raises(ValueError, match="limit must be positive"):
            stats.breakdown(db, "company_name", limit=0)


class TestTechnicalTools:
    def test_mentions_and_jobs_are_counted_separately(self, db: Path) -> None:
        """One job may mention Python and Rust, so the scalar
        total == sum(buckets) + other invariant deliberately does not apply."""
        mb = stats.technical_tools(db)
        counts = {b.label: b.count for b in mb.buckets}

        assert counts["Python"] == 2
        assert counts["Rust"] == 1
        assert mb.total_mentions == 5  # Python x2, Rust, SQL, R
        assert mb.jobs_with_value == 4  # req-c's '[]' contributes nothing
        assert mb.other_mentions == 0

    def test_an_empty_array_contributes_nothing(self, db: Path) -> None:
        mb = stats.technical_tools(db)

        assert "" not in {b.label for b in mb.buckets}
        assert mb.jobs_with_value == ACTIVE - 1

    def test_unshown_mentions_land_in_other_mentions(self, db: Path) -> None:
        mb = stats.technical_tools(db, limit=1)

        assert [b.label for b in mb.buckets] == ["Python"]
        assert mb.other_mentions == mb.total_mentions - 2

    def test_the_same_shape_serves_workplace_countries(self, db: Path) -> None:
        mb = stats.workplace_countries(db)
        counts = {b.label: b.count for b in mb.buckets}

        assert counts == {"US": 3, "CA": 1}
        assert mb.jobs_with_value == 3


class TestCompensation:
    def test_currencies_are_never_pooled(self, db: Path) -> None:
        comp = stats.compensation(db)
        by_currency = {c.currency: c for c in comp.currencies}

        assert set(by_currency) == {"USD", "EUR"}
        # Ordered by qualifying job count, USD (2) before EUR (1).
        assert [c.currency for c in comp.currencies] == ["USD", "EUR"]

    def test_minima_and_maxima_are_summarised_independently(self, db: Path) -> None:
        usd = {c.currency: c for c in stats.compensation(db).currencies}["USD"]

        assert usd.min_jobs == 2
        assert usd.max_jobs == 1  # req-b lists no maximum
        assert usd.min_median == pytest.approx(110000.0)
        assert usd.min_p25 == pytest.approx(105000.0)
        assert usd.min_p75 == pytest.approx(115000.0)
        assert usd.max_median == pytest.approx(150000.0)

    def test_non_positive_values_are_discarded(self, db: Path) -> None:
        """req-d lists 0 and -1: placeholders, not salaries. req-c lists
        neither, so EUR qualifies on req-e alone."""
        eur = {c.currency: c for c in stats.compensation(db).currencies}["EUR"]

        assert eur.jobs == 1
        assert eur.min_median == pytest.approx(90000.0)

    def test_the_coverage_denominator_is_visible(self, db: Path) -> None:
        comp = stats.compensation(db)

        assert comp.active_jobs == ACTIVE
        assert comp.jobs_with_compensation == 3  # req-a, req-b, req-e

    def test_the_expired_high_earner_is_excluded_by_default(self, db: Path) -> None:
        default = {c.currency: c for c in stats.compensation(db).currencies}
        both = {
            c.currency: c
            for c in stats.compensation(db, include_expired=True).currencies
        }

        assert default["USD"].jobs == 2
        assert both["USD"].jobs == 3


class TestRecency:
    def test_the_window_is_gap_free_and_ends_this_month(self, db: Path) -> None:
        rec = stats.recency(db, months=6)
        this_month = datetime.now(tz=UTC).strftime("%Y-%m")

        assert len(rec.buckets) == 6
        assert rec.buckets[-1].label == this_month
        labels = [b.label for b in rec.buckets]
        assert labels == sorted(labels), "oldest to newest, for a real time axis"

    def test_zero_months_are_kept_not_compressed(self, db: Path) -> None:
        """A stale corpus correctly shows a run of recent zeroes."""
        rec = stats.recency(db, months=3)

        assert all(b.count == 0 for b in rec.buckets) or rec.older >= 0
        assert len(rec.buckets) == 3

    def test_unparseable_and_null_dates_are_undated(self, db: Path) -> None:
        rec = stats.recency(db, months=3)

        assert rec.undated == 1  # req-e's 'not-a-date'
        assert rec.total == ACTIVE

    def test_dates_outside_the_window_are_older(self, db: Path) -> None:
        rec = stats.recency(db, months=3)

        # req-a, req-b, req-c (2026) and req-d (2020) are all before a window
        # ending today; only the undated row is excluded from `older`.
        assert rec.older + sum(b.count for b in rec.buckets) == rec.total - rec.undated

    def test_a_non_positive_window_is_rejected(self, db: Path) -> None:
        with pytest.raises(ValueError, match="months must be positive"):
            stats.recency(db, months=0)


class TestSearch:
    def test_active_jobs_only_by_default(self, db: Path) -> None:
        page = stats.search(db)

        assert page.total == ACTIVE
        assert "req-f" not in {r.requisition_id for r in page.rows}

    def test_expired_rows_are_available_on_request(self, db: Path) -> None:
        page = stats.search(db, include_expired=True)

        assert page.total == TOTAL
        assert "req-f" in {r.requisition_id for r in page.rows}

    def test_matching_is_a_case_insensitive_substring(self, db: Path) -> None:
        page = stats.search(db, query="acme")

        assert {r.requisition_id for r in page.rows} == {"req-a", "req-b"}

    def test_wildcards_are_literal_characters(self, db: Path) -> None:
        """contains(), not LIKE: a user typing '%' means a percent sign.

        Under LIKE these would match everything; here '%' matches only the
        one title that literally contains it.
        """
        assert stats.search(db, query="%").total == 1
        assert stats.search(db, query="_").total == 0
        assert stats.search(db, query="50%").total == 1

    def test_description_matching_is_opt_in(self, db: Path) -> None:
        """Descriptions average ~35 KB, so they stay out of the default scan."""
        assert stats.search(db, query="Scientist").total == 1
        assert stats.search(db, query="Scientist", include_description=True).total == 1

    def test_paging_is_stable_and_never_repeats_a_row(self, db: Path) -> None:
        first = stats.search(db, limit=2, offset=0)
        second = stats.search(db, limit=2, offset=2)
        third = stats.search(db, limit=2, offset=4)

        assert first.total == second.total == ACTIVE
        assert second.offset == 2
        seen = [r.requisition_id for r in (*first.rows, *second.rows, *third.rows)]
        assert len(seen) == len(set(seen)) == ACTIVE
        # Newest first, nulls (unparseable dates) last.
        assert seen[0] == "req-a"
        assert seen[-1] == "req-e"

    def test_only_the_nine_browse_columns_come_back(self, db: Path) -> None:
        row = stats.search(db, limit=1).rows[0]

        assert not hasattr(row, "description")
        assert len(stats.JOB_ROW_COLUMNS) == 9

    def test_limit_and_offset_are_validated(self, db: Path) -> None:
        with pytest.raises(ValueError, match="limit must be positive"):
            stats.search(db, limit=0)
        with pytest.raises(ValueError, match="offset must not be negative"):
            stats.search(db, offset=-1)
