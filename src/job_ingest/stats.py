"""
Read-only corpus statistics over <out-dir>/jobs.duckdb.

This is the frontend-agnostic answer to "what is actually in the corpus?".
Every public function takes a path, opens the database, queries, closes, and
returns plain frozen dataclasses -- no Rich, no Textual, no printing. The
dashboard renders these; a second frontend could too.

Scope boundary
--------------
stats.py never runs the pipeline and ingest_and_benchmark.IngestResult never
queries the corpus. Per-run sidecar numbers are run state and live there;
corpus questions live here. A frontend refreshes stats *after* an ingest
finishes.

Connection policy
-----------------
Connections are never cached. DuckDB's file lock is exclusive: either one
read-write process or N read-only ones. A read-only handle held on the App
would block that same process's own load_duckdb() write during an ingest --
the dashboard runs ingest in-process, so this is not a hypothetical. Open,
query, close.

Timestamps
----------
DuckDB's Python client raises `InvalidInputException: Required module 'pytz'
failed to import` the moment a query returns a TIMESTAMPTZ. Every date query
here therefore returns VARCHAR via strftime(), or casts to a naive TIMESTAMP
that is used for ordering only and never returned.

Active jobs
-----------
Everything defaults to active jobs (`is_expired IS NOT TRUE`, so an unknown
NULL counts as active). Expired rows are included only on explicit request.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from job_ingest.ingest_and_benchmark import ddl_columns

#: The predicate every default query carries. NULL is treated as active: an
#: unknown expiry is much more likely to be a live job than a dead one.
_ACTIVE = "is_expired IS NOT TRUE"

#: Columns worth a fill-rate readout. Deliberately excludes `description` and
#: the three *_json blobs: they are non-Option String in flatten.rs so they
#: are never null (a 100% row tells you nothing), and they are where all
#: ~969 MB of the corpus lives.
DEFAULT_FILL_COLUMNS: tuple[str, ...] = (
    "core_job_title",
    "job_category",
    "seniority_level",
    "role_type",
    "workplace_type",
    "formatted_workplace_location",
    "workplace_countries",
    "min_industry_and_role_yoe",
    "yearly_min_compensation",
    "yearly_max_compensation",
    "listed_compensation_currency",
    "technical_tools",
    "estimated_publish_date",
    "company_name",
    "company_website",
    "enriched_status",
    "nb_employees",
    "year_founded",
    "latitude",
    "longitude",
)

#: The nine columns Browse projects. Never `SELECT *`: `description` averages
#: ~35 KB/row, so a 200-row page of it would pull ~7 MB per keystroke.
JOB_ROW_COLUMNS: tuple[str, ...] = (
    "requisition_id",
    "title",
    "company_name",
    "formatted_workplace_location",
    "workplace_type",
    "yearly_min_compensation",
    "yearly_max_compensation",
    "listed_compensation_currency",
    "estimated_publish_date",
)

#: Columns a Browse query matches when the caller supplies text.
_SEARCH_COLUMNS: tuple[str, ...] = (
    "title",
    "company_name",
    "formatted_workplace_location",
)


class StatsUnavailable(RuntimeError):
    """Corpus DB missing, or locked by another process.

    DuckDB allows either one read-write process or N read-only ones, so a
    running ingest -- or a stray `duckdb` CLI left open on the file -- makes
    reads fail. A missing `jobs` table or a schema this module cannot query
    lands here too, so a frontend has exactly one empty-state to render and
    never sees a raw DuckDB exception.
    """


# --------------------------------------------------------------------------
# Result types
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Overview:
    """Corpus state blended with pipeline state."""

    total_rows: int
    active_rows: int
    expired_rows: int
    active_companies: int
    active_sources: int
    #: 'YYYY-MM-DD' strings, or None when nothing parses as a date.
    earliest_active: str | None
    latest_active: str | None
    duckdb_bytes: int | None
    sqlite_bytes: int | None
    parquet_bytes: int | None
    #: Files the sidecar remembers ingesting successfully -- NOT a count of
    #: the raw files on disk right now. Entries for deleted inputs linger by
    #: design, so this can exceed what the source directory holds.
    manifest_entries: int | None
    #: Manifest mtime as 'YYYY-MM-DD HH:MM' UTC, or None if there is none.
    manifest_mtime: str | None


@dataclass(frozen=True, slots=True)
class ColumnFill:
    column: str
    filled: int
    total: int

    @property
    def pct(self) -> float:
        return 100.0 * self.filled / self.total if self.total else 0.0


@dataclass(frozen=True, slots=True)
class Bucket:
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class Breakdown:
    """A scalar column's top values.

    Invariant: ``total == sum(b.count for b in buckets) + other``. One row
    has exactly one value, so the counts partition the rows that have one.
    """

    column: str
    buckets: tuple[Bucket, ...]
    other: int
    total: int


@dataclass(frozen=True, slots=True)
class MultiBreakdown:
    """An array-valued column's top values.

    The scalar invariant cannot hold here: one job may mention both Python
    and Rust, so mentions outnumber jobs. Both denominators are reported and
    the caller must say which one it is using. `other_mentions` is the
    mentions not shown; each bucket's natural percentage is "share of jobs
    with at least one value", i.e. count / jobs_with_value.
    """

    column: str
    buckets: tuple[Bucket, ...]
    other_mentions: int
    jobs_with_value: int
    total_mentions: int


@dataclass(frozen=True, slots=True)
class CompensationCurrency:
    """Annual compensation percentiles for one listed currency.

    Minima and maxima are summarised independently because a job may list
    only one of them; no midpoint is inferred. Percentiles are None when no
    job in this currency carries that side.
    """

    currency: str
    jobs: int
    min_jobs: int
    min_p25: float | None
    min_median: float | None
    min_p75: float | None
    max_jobs: int
    max_p25: float | None
    max_median: float | None
    max_p75: float | None


@dataclass(frozen=True, slots=True)
class Compensation:
    """No currency conversion, ever: currencies are reported side by side.

    `jobs_with_compensation` over `active_jobs` is the coverage denominator,
    so a reader can see how much of the corpus these percentiles speak for.
    """

    currencies: tuple[CompensationCurrency, ...]
    jobs_with_compensation: int
    active_jobs: int


@dataclass(frozen=True, slots=True)
class Recency:
    """Exactly `months` calendar buckets ending at the current UTC month.

    Oldest to newest, zero-count months included, so a sparkline has a real
    time axis. A stale corpus therefore correctly shows a run of recent
    zeroes rather than compressing them away.
    """

    buckets: tuple[Bucket, ...]
    #: Dated jobs older than the first bucket.
    older: int
    #: Jobs with no timestamp, or one that does not parse.
    undated: int
    total: int


@dataclass(frozen=True, slots=True)
class JobRow:
    requisition_id: str
    title: str | None
    company_name: str | None
    formatted_workplace_location: str | None
    workplace_type: str | None
    yearly_min_compensation: float | None
    yearly_max_compensation: float | None
    listed_compensation_currency: str | None
    estimated_publish_date: str | None


@dataclass(frozen=True, slots=True)
class Page:
    rows: tuple[JobRow, ...]
    total: int
    offset: int


# --------------------------------------------------------------------------
# Connection handling
# --------------------------------------------------------------------------


@contextmanager
def _read_only(db_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open, query, close.

    A held read-only handle would block this same process's load_duckdb()
    write during an ingest, so the handle must not outlive the query. Every
    DuckDB failure -- missing file, lock conflict, missing table, unusable
    schema -- becomes StatsUnavailable.
    """
    if not db_path.exists():
        raise StatsUnavailable(
            f"No corpus database at {db_path}. Run an ingest to create it."
        )
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error as e:
        raise StatsUnavailable(
            f"Cannot open {db_path} read-only: {e}\n"
            f"DuckDB's lock is exclusive -- a running ingest or a stray "
            f"`duckdb` process on this file will do it."
        ) from e
    try:
        yield con
    except duckdb.Error as e:
        raise StatsUnavailable(f"Query against {db_path} failed: {e}") from e
    finally:
        con.close()


def _one(
    con: duckdb.DuckDBPyConnection, sql: str, *params: object
) -> tuple[object, ...]:
    row = con.execute(sql, list(params)).fetchone()
    if row is None:
        raise StatsUnavailable(f"Expected one row from: {sql}")
    return tuple(row)


def _where(include_expired: bool, *extra: str) -> str:
    clauses = list(extra)
    if not include_expired:
        clauses.insert(0, _ACTIVE)
    return f"WHERE {' AND '.join(clauses)}" if clauses else ""


def _check_column(column: str) -> str:
    """Whitelist a column name against the DDL.

    A column name cannot be bound as a `?` parameter, so it is interpolated
    -- which makes this the one place SQL injection is possible. DUCKDB_DDL
    is the single source of truth for what exists.
    """
    if column not in frozenset(ddl_columns()):
        raise ValueError(f"Unknown column {column!r}; not in the jobs table.")
    return column


def _positive(name: str, value: int) -> int:
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _non_negative(name: str, value: int) -> int:
    if value < 0:
        raise ValueError(f"{name} must not be negative, got {value}")
    return value


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _num(value: object) -> float | None:
    """DuckDB hands back Decimal, float or None depending on the column."""
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return float(str(value))


def _count(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float, Decimal)):
        return int(value)
    return int(str(value))


# --------------------------------------------------------------------------
# Overview
# --------------------------------------------------------------------------

#: Cached on (mtime_ns, size). Parsing the real 2.6 MB manifest just to len()
#: it costs ~25 ms -- fine in a worker thread, wasteful once a second.
_manifest_cache: dict[Path, tuple[tuple[int, int], int]] = {}


def _manifest_entries(manifest_path: Path) -> tuple[int | None, str | None]:
    try:
        st = manifest_path.stat()
    except OSError:
        return None, None
    mtime = datetime.fromtimestamp(st.st_mtime, tz=UTC).strftime("%Y-%m-%d %H:%M")
    key = (st.st_mtime_ns, st.st_size)
    cached = _manifest_cache.get(manifest_path)
    if cached is not None and cached[0] == key:
        return cached[1], mtime
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, mtime
    if not isinstance(data, dict):
        return None, mtime
    _manifest_cache[manifest_path] = (key, len(data))
    return len(data), mtime


def _size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None


def overview(db_path: Path, out_dir: Path | None = None) -> Overview:
    """Headline corpus counts plus the pipeline artefacts beside the DB."""
    out_dir = out_dir if out_dir is not None else db_path.parent
    with _read_only(db_path) as con:
        total, active, expired = _one(
            con,
            "SELECT COUNT(*), "
            f"COUNT(*) FILTER (WHERE {_ACTIVE}), "
            f"COUNT(*) FILTER (WHERE NOT ({_ACTIVE})) "
            "FROM jobs",
        )
        companies, sources, earliest, latest = _one(
            con,
            "SELECT COUNT(DISTINCT nullif(trim(company_name), '')), "
            "COUNT(DISTINCT nullif(trim(source), '')), "
            # strftime keeps this VARCHAR: returning the timestamp itself
            # would need pytz, which is not a dependency.
            "strftime(min(try_cast(estimated_publish_date AS TIMESTAMP)), '%Y-%m-%d'), "
            "strftime(max(try_cast(estimated_publish_date AS TIMESTAMP)), '%Y-%m-%d') "
            f"FROM jobs WHERE {_ACTIVE}",
        )
    entries, mtime = _manifest_entries(out_dir / "ingest_manifest.json")
    return Overview(
        total_rows=_count(total),
        active_rows=_count(active),
        expired_rows=_count(expired),
        active_companies=_count(companies),
        active_sources=_count(sources),
        earliest_active=_text(earliest),
        latest_active=_text(latest),
        duckdb_bytes=_size(db_path),
        sqlite_bytes=_size(out_dir / "jobs.sqlite"),
        parquet_bytes=_size(out_dir / "jobs.parquet"),
        manifest_entries=entries,
        manifest_mtime=mtime,
    )


# --------------------------------------------------------------------------
# Fill rates and breakdowns
# --------------------------------------------------------------------------

#: Treats NULL, blank and the empty JSON array alike. workplace_countries and
#: technical_tools are non-Option String in flatten.rs, so '[]' -- not NULL --
#: is how they say "nothing here".
_FILLED = "nullif(nullif(CAST(\"{c}\" AS VARCHAR), ''), '[]')"


def fill_rates(
    db_path: Path,
    columns: Sequence[str] = DEFAULT_FILL_COLUMNS,
    *,
    include_expired: bool = False,
) -> tuple[ColumnFill, ...]:
    """How populated each column is, in one statement rather than N."""
    checked = [_check_column(c) for c in columns]
    if not checked:
        return ()
    projection = ", ".join(f"COUNT({_FILLED.format(c=c)})" for c in checked)
    with _read_only(db_path) as con:
        row = _one(
            con,
            f"SELECT COUNT(*), {projection} FROM jobs {_where(include_expired)}",
        )
    total = _count(row[0])
    return tuple(
        ColumnFill(column=c, filled=_count(v), total=total)
        for c, v in zip(checked, row[1:], strict=True)
    )


def breakdown(
    db_path: Path,
    column: str,
    limit: int = 15,
    *,
    include_expired: bool = False,
) -> Breakdown:
    """Top `limit` values of a scalar column, plus an `other` remainder."""
    col = _check_column(column)
    _positive("limit", limit)
    filled = _FILLED.format(c=col)
    with _read_only(db_path) as con:
        where = _where(include_expired, f"{filled} IS NOT NULL")
        (total,) = _one(con, f"SELECT COUNT(*) FROM jobs {where}")
        rows = con.execute(
            f"SELECT {filled} AS label, COUNT(*) AS n "
            f"FROM jobs {where} GROUP BY 1 ORDER BY n DESC, label LIMIT ?",
            [limit],
        ).fetchall()
    buckets = tuple(Bucket(label=str(label), count=int(n)) for label, n in rows)
    shown = sum(b.count for b in buckets)
    return Breakdown(
        column=col,
        buckets=buckets,
        other=_count(total) - shown,
        total=_count(total),
    )


def _multi_breakdown(
    db_path: Path,
    column: str,
    limit: int,
    include_expired: bool,
) -> MultiBreakdown:
    """Shared shape for the JSON-array text columns.

    UNNEST over from_json drops empty arrays for free, so '[]' rows simply
    contribute nothing rather than needing a filter.
    """
    col = _check_column(column)
    _positive("limit", limit)
    exploded = (
        f"WITH exploded AS ("
        f"  SELECT requisition_id, u.value AS label FROM jobs, "
        f"  UNNEST(from_json(nullif(coalesce(\"{col}\", '[]'), ''), '[\"VARCHAR\"]')) "
        f"  AS u(value) {_where(include_expired)}"
        f")"
    )
    with _read_only(db_path) as con:
        mentions, jobs_with_value = _one(
            con,
            f"{exploded} SELECT COUNT(*), COUNT(DISTINCT requisition_id) FROM exploded",
        )
        rows = con.execute(
            f"{exploded} SELECT label, COUNT(*) AS n FROM exploded "
            f"GROUP BY 1 ORDER BY n DESC, label LIMIT ?",
            [limit],
        ).fetchall()
    buckets = tuple(Bucket(label=str(label), count=int(n)) for label, n in rows)
    return MultiBreakdown(
        column=col,
        buckets=buckets,
        other_mentions=_count(mentions) - sum(b.count for b in buckets),
        jobs_with_value=_count(jobs_with_value),
        total_mentions=_count(mentions),
    )


def technical_tools(
    db_path: Path, limit: int = 25, *, include_expired: bool = False
) -> MultiBreakdown:
    """Most-mentioned tools. See MultiBreakdown for which denominator is which."""
    return _multi_breakdown(db_path, "technical_tools", limit, include_expired)


def workplace_countries(
    db_path: Path, limit: int = 15, *, include_expired: bool = False
) -> MultiBreakdown:
    """Most-listed countries; same array-valued shape as technical_tools()."""
    return _multi_breakdown(db_path, "workplace_countries", limit, include_expired)


# --------------------------------------------------------------------------
# Compensation
# --------------------------------------------------------------------------


def compensation(db_path: Path, *, include_expired: bool = False) -> Compensation:
    """Annual compensation percentiles, one row per listed currency.

    A positive annual minimum and a positive annual maximum each qualify
    independently; zero and negative values are discarded as placeholders.
    Currencies are never pooled and no midpoint is inferred -- USD 200k and
    JPY 200k are not comparable numbers.
    """
    positive = (
        "SELECT nullif(trim(listed_compensation_currency), '') AS currency, "
        "CASE WHEN yearly_min_compensation > 0 THEN yearly_min_compensation END AS lo, "
        "CASE WHEN yearly_max_compensation > 0 THEN yearly_max_compensation END AS hi "
        f"FROM jobs {_where(include_expired)}"
    )
    with _read_only(db_path) as con:
        active_jobs, with_comp = _one(
            con,
            f"WITH c AS ({positive}) "
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE lo IS NOT NULL OR hi IS NOT NULL) "
            "FROM c",
        )
        rows = con.execute(
            f"WITH c AS ({positive}) "
            "SELECT currency, COUNT(*) AS jobs, "
            "COUNT(lo) AS min_jobs, quantile_cont(lo, [0.25, 0.5, 0.75]) AS min_q, "
            "COUNT(hi) AS max_jobs, quantile_cont(hi, [0.25, 0.5, 0.75]) AS max_q "
            "FROM c WHERE currency IS NOT NULL "
            "AND (lo IS NOT NULL OR hi IS NOT NULL) "
            "GROUP BY 1 ORDER BY jobs DESC, currency"
        ).fetchall()

    def q(values: object, i: int) -> float | None:
        return (
            _num(values[i]) if isinstance(values, list) and len(values) == 3 else None
        )

    return Compensation(
        currencies=tuple(
            CompensationCurrency(
                currency=str(currency),
                jobs=int(jobs),
                min_jobs=int(min_jobs),
                min_p25=q(min_q, 0),
                min_median=q(min_q, 1),
                min_p75=q(min_q, 2),
                max_jobs=int(max_jobs),
                max_p25=q(max_q, 0),
                max_median=q(max_q, 1),
                max_p75=q(max_q, 2),
            )
            for currency, jobs, min_jobs, min_q, max_jobs, max_q in rows
        ),
        jobs_with_compensation=_count(with_comp),
        active_jobs=_count(active_jobs),
    )


# --------------------------------------------------------------------------
# Recency
# --------------------------------------------------------------------------


def _month_labels(months: int) -> list[str]:
    """The last `months` 'YYYY-MM' labels, oldest first, ending this UTC month."""
    now = datetime.now(tz=UTC)
    labels: list[str] = []
    year, month = now.year, now.month
    for _ in range(months):
        labels.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return list(reversed(labels))


def recency(
    db_path: Path, months: int = 24, *, include_expired: bool = False
) -> Recency:
    """Monthly publication counts over a fixed, gap-free window."""
    _positive("months", months)
    with _read_only(db_path) as con:
        # The strftime is what dodges the pytz landmine: returning the
        # date_trunc result unwrapped hands Python a TIMESTAMPTZ and raises.
        rows = con.execute(
            "WITH dated AS ("
            "  SELECT try_cast(estimated_publish_date AS TIMESTAMPTZ) AS published "
            f"  FROM jobs {_where(include_expired)}"
            ") "
            "SELECT strftime(date_trunc('month', published), '%Y-%m') AS label, "
            "COUNT(*) AS n FROM dated WHERE published IS NOT NULL "
            "GROUP BY 1 ORDER BY 1"
        ).fetchall()
        (total,) = _one(con, f"SELECT COUNT(*) FROM jobs {_where(include_expired)}")
    counts = {str(label): int(n) for label, n in rows}
    labels = _month_labels(months)
    window = set(labels)
    dated = sum(counts.values())
    return Recency(
        buckets=tuple(Bucket(label=lb, count=counts.get(lb, 0)) for lb in labels),
        # Anything dated outside the window is necessarily older: the window
        # ends at the current month, and a future date would be corrupt data.
        older=sum(n for lb, n in counts.items() if lb not in window),
        undated=_count(total) - dated,
        total=_count(total),
    )


# --------------------------------------------------------------------------
# Browse
# --------------------------------------------------------------------------


def search(
    db_path: Path,
    *,
    query: str = "",
    limit: int = 200,
    offset: int = 0,
    include_description: bool = False,
    include_expired: bool = False,
) -> Page:
    """One page of job rows, newest first.

    `query` is a case-insensitive *literal* substring match: `contains()`,
    not LIKE, so `%` and `_` are ordinary characters rather than wildcards a
    user has to know to escape. Description matching is opt-in because those
    values average ~35 KB.

    Ordering is deterministic -- parsed publish timestamp descending with
    nulls last, then requisition_id -- so paging cannot skip or repeat a
    row. The timestamp is used for ordering only and never returned, which
    is what keeps this away from the pytz landmine.
    """
    _positive("limit", limit)
    _non_negative("offset", offset)
    columns = list(_SEARCH_COLUMNS) + (["description"] if include_description else [])
    clauses: list[str] = []
    params: list[object] = []
    if query:
        matches = " OR ".join(
            f"contains(lower(coalesce(\"{c}\", '')), lower(?))" for c in columns
        )
        clauses.append(f"({matches})")
        params.extend([query] * len(columns))
    where = _where(include_expired, *clauses)
    projection = ", ".join(f'"{c}"' for c in JOB_ROW_COLUMNS)
    with _read_only(db_path) as con:
        (total,) = _one(con, f"SELECT COUNT(*) FROM jobs {where}", *params)
        rows = con.execute(
            f"SELECT {projection} FROM jobs {where} "
            "ORDER BY try_cast(estimated_publish_date AS TIMESTAMP) DESC NULLS LAST, "
            "requisition_id LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
    return Page(
        rows=tuple(
            JobRow(
                requisition_id=str(r[0]),
                title=_text(r[1]),
                company_name=_text(r[2]),
                formatted_workplace_location=_text(r[3]),
                workplace_type=_text(r[4]),
                yearly_min_compensation=_num(r[5]),
                yearly_max_compensation=_num(r[6]),
                listed_compensation_currency=_text(r[7]),
                estimated_publish_date=_text(r[8]),
            )
            for r in rows
        ),
        total=_count(total),
        offset=offset,
    )
