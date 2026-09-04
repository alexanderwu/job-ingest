#!/usr/bin/env python3
"""
HiringCafe job scraper (Job Scout).

Scrapes job listings, including full description text, from hiringcafe.com via its
server-side-rendered Next.js JSON data routes (no HTML parsing, no browser).

How it works (verified 2026-07-15):
  1. GET https://hiringcafe.com/          -> extract Next.js buildId from __NEXT_DATA__
  2. GET /_next/data/{buildId}/index.json?searchState={...}&page=N
        -> paginated search results ("ssrHits"), ~40+ jobs/page, no descriptions
  3. GET /_next/data/{buildId}/job/x-{requisition_id}.json
        -> 308 redirect payload with the canonical job slug
  4. GET /_next/data/{buildId}/job/{canonical-slug}.json
        -> full job object incl. job_information.description (HTML)

Usage:
  python scrape_hiringcafe.py --query "software engineer" --max-jobs 50
  python scrape_hiringcafe.py --preset DS_SF_Remote --max-jobs 100
  python scrape_hiringcafe.py --url "https://hiringcafe.com/?searchState=..." --max-jobs 100
  python scrape_hiringcafe.py --query "data analyst" --no-descriptions   # fast, cards only
  python scrape_hiringcafe.py --preset DA_Healthcare --raw-dir data/raw/_json

Tip: for filters (salary, remote, seniority...), set them in the hiringcafe.com UI,
copy the URL from your address bar, and pass it via --url. The four searches that
are used often enough to be worth naming ship as --preset keys; see
SAVED_SEARCHES and docs/saved_hiringcafe_searches.md.

Example URL:
  https://hiringcafe.com/?searchState=%7B%22searchQuery%22%3A%22software%20engineer%22%7D

Library API:
  scrape(ScrapeConfig) is a synchronous generator of ScrapeEvent, so a
  frontend can render progress, cancel by stopping iteration, and never has
  to parse stdout. It raises ScrapeError instead of calling sys.exit() and
  never prints. main() is the only place that maps the domain exception to
  CLI stderr and an exit code.

Requires: pip install requests typer
Be polite: keep --delay >= 0.5s. Unofficial API; their robots.txt discourages
bulk crawling, so scrape only what you need.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import time
import unicodedata
from collections import deque
from collections.abc import Callable, Generator
from contextlib import suppress
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, quote, urlparse

import requests
import typer

BASE = "https://hiringcafe.com"
USER_AGENT = (
    "JobScout/1.0 (personal job search script; contact: alexander.wu7@gmail.com)"
)

CSV_COLUMNS = [
    "id",
    "requisition_id",
    "title",
    "company",
    "location",
    "workplace_type",
    "commitment",
    "yearly_min_compensation",
    "yearly_max_compensation",
    "compensation_currency",
    "compensation_frequency",
    "date_posted",
    "source_ats",
    "board_token",
    "technical_tools",
    "requirements_summary",
    "hiringcafe_url",
    "apply_url",
    "description_text",
]

#: Where the dashboard stages newly scraped pages. Deliberately NOT the
#: `data/raw/json` symlink the established ingest reads: the manifest is keyed
#: by filename alone, so alternating input roots against one output directory
#: could overwrite newer rows with older copies. Reconciling the two corpus
#: roots is its own design task; until then a staged ingest is an explicit
#: `--json-dir data/raw/_json` with an intentionally chosen output directory.
DEFAULT_RAW_DIR = Path("data/raw/_json")


class ScrapeError(RuntimeError):
    """A scrape could not be completed.

    The single failure type of the library API: configuration mistakes,
    an unrecognisable homepage, unexpected response shapes and raw-write
    failures all arrive here with their cause chained. Library code never
    calls sys.exit() and never prints -- main() alone decides what a CLI
    does about it.
    """


class ScrapeCancelled(ScrapeError):
    """Cancellation was requested and observed at a checkpoint.

    Cooperative, not immediate: a request already inside `requests` runs to
    its timeout. See ScrapeConfig for what a caller can promise a user.
    """


class StaleBuildId(Exception):
    pass


def _noop(_msg: str) -> None:
    """Default warning sink; a module-level def keeps mypy happy."""


# ---------------------------------------------------------------- HTML -> text
class _TextExtractor(HTMLParser):
    BLOCK_TAGS = frozenset(
        {
            "p",
            "div",
            "br",
            "li",
            "ul",
            "ol",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "tr",
            "table",
            "section",
            "article",
        }
    )

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")
        if tag == "li":
            self.parts.append("- ")

    def handle_endtag(self, tag: str) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def html_to_text(html: str) -> str:
    if not html:
        return ""
    p = _TextExtractor()
    p.feed(html)
    text = "".join(p.parts)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


# ------------------------------------------------------------------- HTTP layer
class Client:
    """Rate-limited HTTP with retries.

    Diagnostics go to `on_warning` rather than stderr so a TUI can render
    them; `cancel` makes the backoff sleeps interruptible. Cancellation is
    checked before each attempt, so the worst case is one in-flight request
    running to its 30s timeout -- describe it as "after the current
    request", never as an instant stop.
    """

    def __init__(
        self,
        delay: float,
        *,
        cancel: threading.Event | None = None,
        on_warning: Callable[[str], None] = _noop,
    ) -> None:
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/html;q=0.9",
            }
        )
        self._last_request = 0.0
        self._cancel = cancel
        self._on_warning = on_warning

    @property
    def cancelled(self) -> bool:
        return self._cancel is not None and self._cancel.is_set()

    def route_warnings(self, sink: Callable[[str], None]) -> None:
        """Redirect retry/backoff diagnostics somewhere else.

        scrape() calls this so a caller-supplied Client's warnings still
        become ScrapeEvents instead of vanishing into the default sink.
        """
        self._on_warning = sink

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep: Event.wait() returns as soon as we're cancelled."""
        if seconds <= 0:
            return
        if self._cancel is not None:
            self._cancel.wait(seconds)
        else:
            time.sleep(seconds)

    def get(self, url: str, retries: int = 3) -> requests.Response:
        for attempt in range(retries):
            if self.cancelled:
                raise ScrapeCancelled("cancelled before requesting " + url)
            wait = self.delay - (time.time() - self._last_request)
            if wait > 0:
                self._sleep(wait)
            self._last_request = time.time()
            try:
                resp = self.session.get(url, timeout=30)
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise ScrapeError(f"GET {url} failed: {e}") from e
                self._on_warning(f"  ! {type(e).__name__}, retrying...")
                self._sleep(2**attempt)
                continue
            if resp.status_code == 404:
                raise StaleBuildId(url)
            if resp.status_code in (429, 403, 500, 502, 503):
                if attempt == retries - 1:
                    raise ScrapeError(
                        f"GET {url} gave HTTP {resp.status_code} after {retries} tries"
                    )
                backoff = 5 * (2**attempt)
                self._on_warning(
                    f"  ! HTTP {resp.status_code}, backing off {backoff}s..."
                )
                self._sleep(backoff)
                continue
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                raise ScrapeError(f"GET {url} failed: {e}") from e
            return resp
        raise ScrapeError(f"GET {url}: retries exhausted")


def _json_body(resp: requests.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except ValueError as e:
        raise ScrapeError(f"{resp.url} did not return JSON: {e}") from e
    if not isinstance(data, dict):
        raise ScrapeError(
            f"{resp.url} returned {type(data).__name__}, expected an object"
        )
    return data


# --------------------------------------------------------------- API functions
def get_build_id(client: Client) -> str:
    html = client.get(BASE + "/").text
    m = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
    if not m:
        raise ScrapeError(
            "could not find Next.js buildId on the homepage. "
            "The site structure may have changed."
        )
    return m.group(1)


def search_page(
    client: Client, build_id: str, search_state: dict[str, Any], page: int
) -> dict[str, Any]:
    ss = quote(json.dumps(search_state, separators=(",", ":")))
    url = f"{BASE}/_next/data/{build_id}/index.json?searchState={ss}&page={page}"
    props = _json_body(client.get(url)).get("pageProps", {})
    if not isinstance(props, dict):
        raise ScrapeError(f"search page {page} returned no pageProps object")
    return props


def fetch_job_detail(
    client: Client, build_id: str, requisition_id: str
) -> tuple[dict[str, Any] | None, str]:
    """Returns (job_dict_or_None, canonical_hiringcafe_url_or_empty)."""
    url = f"{BASE}/_next/data/{build_id}/job/x-{quote(requisition_id)}.json"
    props = _json_body(client.get(url)).get("pageProps", {})
    canonical_url = ""
    if isinstance(props, dict) and "__N_REDIRECT" in props:  # canonical slug redirect
        path = props["__N_REDIRECT"]  # e.g. /job/title-company-city-<id>
        canonical_url = BASE + str(path)
        slug = str(path).split("/job/", 1)[-1]
        url = f"{BASE}/_next/data/{build_id}/job/{quote(slug)}.json"
        props = _json_body(client.get(url)).get("pageProps", {})
    if not isinstance(props, dict):
        return None, canonical_url
    job = props.get("job")
    return (job if isinstance(job, dict) else None), canonical_url


# ------------------------------------------------------------------ flattening
def _join(val: Any) -> str:
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    return "" if val is None else str(val)


def flatten(
    hit: dict[str, Any], detail: dict[str, Any] | None, canonical_url: str
) -> dict[str, Any]:
    v5 = (
        (detail or hit).get("v5_processed_job_data")
        or hit.get("v5_processed_job_data")
        or {}
    )
    ji = (detail or hit).get("job_information") or {}
    return {
        "id": hit.get("id", ""),
        "requisition_id": hit.get("requisition_id", ""),
        "title": ji.get("title") or v5.get("core_job_title", ""),
        "company": v5.get("company_name", ""),
        "location": v5.get("formatted_workplace_location", ""),
        "workplace_type": v5.get("workplace_type", ""),
        "commitment": _join(v5.get("commitment")),
        "yearly_min_compensation": v5.get("yearly_min_compensation", ""),
        "yearly_max_compensation": v5.get("yearly_max_compensation", ""),
        "compensation_currency": v5.get("listed_compensation_currency", ""),
        "compensation_frequency": v5.get("listed_compensation_frequency", ""),
        "date_posted": v5.get("estimated_publish_date", ""),
        "source_ats": hit.get("source", ""),
        "board_token": hit.get("board_token", ""),
        "technical_tools": _join(v5.get("technical_tools")),
        "requirements_summary": v5.get("requirements_summary", ""),
        "hiringcafe_url": canonical_url,
        "apply_url": (detail or {}).get("apply_url") or hit.get("hc_apply_url", ""),
        "description_text": html_to_text(ji.get("description", "")),
    }


# --------------------------------------------------------- raw corpus writing
#: Mirrors `struct Job` in fastingest/src/schema.rs. A page missing any of
#: these fails validation at ingest time; catching it here turns a silent
#: corpus hole into an offline warning.
REQUIRED_JOB_KEYS: frozenset[str] = frozenset(
    {
        "id",
        "board_token",
        "source",
        "apply_url",
        "source_and_board_token",
        "requisition_id",
        "collapse_key",
        "is_expired",
        "objectID",
        "job_information",
        "v5_processed_job_data",
    }
)

#: The flat identity fields a search hit can supply when a detail page omits
#: them. The two nested blocks are never synthesised.
_IDENTITY_KEYS: tuple[str, ...] = (
    "id",
    "board_token",
    "source",
    "apply_url",
    "source_and_board_token",
    "requisition_id",
    "collapse_key",
    "is_expired",
    "objectID",
)

RAW_SUFFIX = ".json.gz"
#: The temp file must NOT end in .json.gz: list_inputs() in fastingest's
#: lib.rs globs on that suffix and would pick a half-written file up
#: mid-write, reporting it as a phantom validation error.
_TEMP_SUFFIX = ".part"
_MAX_SLUG = 64
#: Windows refuses these as filenames regardless of case or extension.
_RESERVED_STEMS: frozenset[str] = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{i}" for i in range(10)}
    | {f"lpt{i}" for i in range(10)}
)
#: The corpus convention: lowercase ASCII, no leading/trailing/doubled
#: separators. Corpus ids happen to be 16-char [a-z0-9]; this is a little
#: broader but still round-trips safely on every filesystem we care about.
_SAFE_STEM = re.compile(r"\A[a-z0-9]+(?:[._-][a-z0-9]+)*\Z")
_UNSAFE_CHARS = re.compile(r"[^a-z0-9._-]+")


def raw_filename(requisition_id: str) -> str | None:
    """Corpus-compatible filename, or None if unusable.

    The convention is exactly ``{requisition_id}.json.gz``. Corpus ids happen
    to be 16-char [a-z0-9], but scraped ids come from arbitrary ATSes and may
    contain ``/ \\ : ? *``, be a Windows reserved name (CON, NUL, COM1...),
    carry trailing dots or spaces, be non-ASCII, or blow the path limit.

    Two ids colliding on one filename would silently lose a row, so whenever
    sanitisation or truncation changed anything -- uppercase and Unicode
    normalisation included -- a digest of the ORIGINAL UTF-8 id is appended.
    That is collision-resistant, not mathematically injective. A name is
    preserved bare only when it is already in the safe lowercase ASCII
    convention and is not a reserved stem.

    Mangling costs only readability: the filename is not the ingest key.
    ``requisition_id`` inside the JSON is, and it is stored verbatim.
    """
    original = requisition_id
    if not original or not original.strip():
        return None

    if (
        _SAFE_STEM.fullmatch(original)
        and len(original) <= _MAX_SLUG
        and original.split(".", 1)[0] not in _RESERVED_STEMS
        and unicodedata.normalize("NFC", original) == original
    ):
        return f"{original}{RAW_SUFFIX}"

    folded = unicodedata.normalize("NFKD", original)
    ascii_only = folded.encode("ascii", "ignore").decode("ascii").lower()
    slug = _UNSAFE_CHARS.sub("-", ascii_only).strip("._-")[:_MAX_SLUG].strip("._-")
    # blake2b over the original bytes, so ids that sanitise to one slug --
    # and case-only variants, which a case-folding filesystem would merge --
    # still land on different files.
    digest = hashlib.blake2b(original.encode("utf-8"), digest_size=8).hexdigest()
    stem = f"{slug}-{digest}" if slug else digest
    return f"{stem}{RAW_SUFFIX}"


def _merge_identity(
    detail: dict[str, Any], hit: dict[str, Any] | None
) -> dict[str, Any]:
    """Fill flat identity fields the detail page omitted from the search hit."""
    merged = dict(detail)
    if not hit:
        return merged
    for key in _IDENTITY_KEYS:
        if merged.get(key) is None and hit.get(key) is not None:
            merged[key] = hit[key]
    if merged.get("apply_url") is None and hit.get("hc_apply_url") is not None:
        merged["apply_url"] = hit["hc_apply_url"]
    return merged


def missing_job_keys(job: dict[str, Any]) -> tuple[str, ...]:
    """Which REQUIRED_JOB_KEYS this object lacks, sorted; empty means valid."""
    return tuple(sorted(k for k in REQUIRED_JOB_KEYS if job.get(k) is None))


def write_raw_page(
    detail: dict[str, Any], raw_dir: Path, *, hit: dict[str, Any] | None = None
) -> Path | None:
    """Write one ingest-compatible page, atomically. None if it would not validate.

    The payload is exactly ``{"pageProps": {"job": detail}, "__N_SSG": True}``
    -- the shape fastingest's JobPage expects.

    Raw job objects are stored **verbatim**, including Firebase user-activity
    UIDs and anything else the site returns. That is an explicit, accepted
    privacy tradeoff for this corpus; do not silently scrub or transform
    fields here.

    The write goes to a uniquely named same-directory temp file created with
    an exclusive open (a PID is not enough if two callers target one
    requisition), and is moved into place with os.replace. os.replace gives
    the file a fresh mtime, so a re-scraped job correctly re-triggers
    incremental ingest through the manifest's (mtime_ns, size) key.
    """
    merged = _merge_identity(detail, hit)
    if missing_job_keys(merged):
        return None
    name = raw_filename(str(merged.get("requisition_id") or ""))
    if name is None:
        return None

    try:
        raw_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise ScrapeError(f"cannot create {raw_dir}: {e}") from e

    payload = {"pageProps": {"job": merged}, "__N_SSG": True}
    try:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise ScrapeError(f"job {name} is not JSON-serialisable: {e}") from e

    target = raw_dir / name
    try:
        fd, tmp_name = tempfile.mkstemp(
            dir=raw_dir, prefix=".raw-", suffix=_TEMP_SUFFIX
        )
    except OSError as e:
        raise ScrapeError(f"cannot create a temp file in {raw_dir}: {e}") from e
    tmp_path = Path(tmp_name)
    replaced = False
    try:
        with os.fdopen(fd, "wb") as handle:
            # mtime=0: the gzip header should not vary run to run. The file's
            # own mtime is what incremental ingest keys on.
            with gzip.GzipFile(fileobj=handle, mode="wb", mtime=0) as gz:
                gz.write(body)
        os.replace(tmp_path, target)
        replaced = True
    except OSError as e:
        raise ScrapeError(f"cannot write {target}: {e}") from e
    finally:
        if not replaced:
            with suppress(OSError):
                tmp_path.unlink()
    return target


# ------------------------------------------------------------- saved searches
@dataclass(frozen=True, slots=True)
class SavedSearch:
    """One named HiringCafe search, stored as its exact encoded URL.

    The URL is the source of truth: `search_state()` decodes it on demand
    rather than keeping a hand-maintained second interpretation of the
    encoded JSON that could drift from it.
    docs/saved_hiringcafe_searches.md is the human-readable provenance, but
    this registry is what runs -- `job-dash` must not depend on the
    repository's docs/ directory surviving installation.
    """

    key: str
    label: str
    url: str
    #: The filters that distinguish this search from the other three. What
    #: they all share is in SHARED_FILTERS, so this stays readable in a
    #: narrow panel.
    summary: str

    def search_state(self) -> dict[str, Any]:
        """A fresh, independently owned searchState dict."""
        return parse_search_state(None, self.url, None)


#: What all four saved searches have in common. Kept out of the individual
#: summaries so those stay short enough to read in a TUI panel; the decoded
#: values themselves are asserted in the tests.
SHARED_FILTERS = (
    "Shared by all four: full-time/contract, transparent salary, 0-6 years, "
    "individual contributor, doctorate optional, last 1,440 days."
)

_DS_TITLES = "Data/ML titles, excluding software and electrical engineer"
_SF_REMOTE = "SF within 100 miles or US remote"

SAVED_SEARCHES: tuple[SavedSearch, ...] = (
    SavedSearch(
        key="DS_SF_Remote",
        label="Data science - SF or US remote",
        url=(
            "https://hiringcafe.com/?searchState=%7B%22locations%22%3A%5B%7B%22id%22%3A%226xk1yZQBoEtHp_8Uv-2X%22%2C%22types%22%3A%5B%22locality%22%5D%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22San+Francisco%22%2C%22short_name%22%3A%22San+Francisco%22%2C%22types%22%3A%5B%22locality%22%5D%7D%2C%7B%22long_name%22%3A%22California%22%2C%22short_name%22%3A%22CA%22%2C%22types%22%3A%5B%22administrative_area_level_1%22%5D%7D%2C%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22geometry%22%3A%7B%22location%22%3A%7B%22lat%22%3A37.77493%2C%22lon%22%3A-122.41942%7D%7D%2C%22formatted_address%22%3A%22San+Francisco%2C+CA%2C+US%22%2C%22population%22%3A864816%2C%22workplace_types%22%3A%5B%5D%2C%22options%22%3A%7B%22radius%22%3A100%2C%22radius_unit%22%3A%22miles%22%2C%22ignore_radius%22%3Afalse%7D%7D%2C%7B%22types%22%3A%5B%22country%22%5D%2C%22formatted_address%22%3A%22United+States%22%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22workplace_types%22%3A%5B%22Remote%22%5D%2C%22options%22%3A%7B%7D%2C%22id%22%3A%22United+Statescountry%22%7D%5D%2C%22commitmentTypes%22%3A%5B%22Full+Time%22%2C%22Contract%22%5D%2C%22dateFetchedPastNDays%22%3A1440%2C%22restrictJobsToTransparentSalaries%22%3Atrue%2C%22roleYoeRange%22%3A%5B0%2C6%5D%2C%22roleTypes%22%3A%5B%22Individual+Contributor%22%5D%2C%22doctorateDegreeRequirements%22%3A%5B%22Preferred%22%2C%22Not+Mentioned%22%5D%2C%22jobTitleQuery%22%3A%22%28%28data+OR+ml+OR+%5C%22machine+learning%5C%22+OR+%5C%22ai%5C%22+OR+%5C%22artificial+intelligence%5C%22+OR+nlp+OR+statistical+OR+bi+OR+%5C%22business+intelligence%5C%22+OR+devops+OR+mlops%29+AND+%28engineer+OR+scientist+OR+science+OR+programmer%29%29+AND+NOT+%5C%22software+engineer%5C%22+AND+NOT+%5C%22electrical+engineer%5C%22%5Cn%22%7D"
        ),
        summary=f"{_DS_TITLES}; {_SF_REMOTE}; any industry.",
    ),
    SavedSearch(
        key="DA_SF_Remote",
        label="Data & analytics dept - SF or US remote",
        url=(
            "https://hiringcafe.com/?searchState=%7B%22locations%22%3A%5B%7B%22id%22%3A%226xk1yZQBoEtHp_8Uv-2X%22%2C%22types%22%3A%5B%22locality%22%5D%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22San+Francisco%22%2C%22short_name%22%3A%22San+Francisco%22%2C%22types%22%3A%5B%22locality%22%5D%7D%2C%7B%22long_name%22%3A%22California%22%2C%22short_name%22%3A%22CA%22%2C%22types%22%3A%5B%22administrative_area_level_1%22%5D%7D%2C%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22geometry%22%3A%7B%22location%22%3A%7B%22lat%22%3A37.77493%2C%22lon%22%3A-122.41942%7D%7D%2C%22formatted_address%22%3A%22San+Francisco%2C+CA%2C+US%22%2C%22population%22%3A864816%2C%22workplace_types%22%3A%5B%5D%2C%22options%22%3A%7B%22radius%22%3A100%2C%22radius_unit%22%3A%22miles%22%2C%22ignore_radius%22%3Afalse%7D%7D%2C%7B%22types%22%3A%5B%22country%22%5D%2C%22formatted_address%22%3A%22United+States%22%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22workplace_types%22%3A%5B%22Remote%22%5D%2C%22options%22%3A%7B%7D%2C%22id%22%3A%22United+Statescountry%22%7D%5D%2C%22commitmentTypes%22%3A%5B%22Full+Time%22%2C%22Contract%22%5D%2C%22dateFetchedPastNDays%22%3A1440%2C%22restrictJobsToTransparentSalaries%22%3Atrue%2C%22departments%22%3A%5B%22Data+and+Analytics%22%5D%2C%22roleYoeRange%22%3A%5B0%2C6%5D%2C%22roleTypes%22%3A%5B%22Individual+Contributor%22%5D%2C%22doctorateDegreeRequirements%22%3A%5B%22Preferred%22%2C%22Not+Mentioned%22%5D%7D"
        ),
        summary=f"Data and Analytics department; {_SF_REMOTE}; any industry.",
    ),
    SavedSearch(
        key="DS_Healthcare",
        label="Data science - biotech & healthcare",
        url=(
            "https://hiringcafe.com/?searchState=%7B%22locations%22%3A%5B%7B%22id%22%3A%226xk1yZQBoEtHp_8Uv-2X%22%2C%22types%22%3A%5B%22locality%22%5D%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22San+Francisco%22%2C%22short_name%22%3A%22San+Francisco%22%2C%22types%22%3A%5B%22locality%22%5D%7D%2C%7B%22long_name%22%3A%22California%22%2C%22short_name%22%3A%22CA%22%2C%22types%22%3A%5B%22administrative_area_level_1%22%5D%7D%2C%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22geometry%22%3A%7B%22location%22%3A%7B%22lat%22%3A37.77493%2C%22lon%22%3A-122.41942%7D%7D%2C%22formatted_address%22%3A%22San+Francisco%2C+CA%2C+US%22%2C%22population%22%3A864816%2C%22workplace_types%22%3A%5B%5D%2C%22options%22%3A%7B%22radius%22%3A100%2C%22radius_unit%22%3A%22miles%22%2C%22ignore_radius%22%3Afalse%7D%7D%2C%7B%22types%22%3A%5B%22country%22%5D%2C%22formatted_address%22%3A%22United+States%22%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22workplace_types%22%3A%5B%22Remote%22%2C%22Onsite%22%2C%22Hybrid%22%5D%2C%22options%22%3A%7B%7D%2C%22id%22%3A%22United+Statescountry%22%7D%5D%2C%22commitmentTypes%22%3A%5B%22Full+Time%22%2C%22Contract%22%5D%2C%22dateFetchedPastNDays%22%3A1440%2C%22restrictJobsToTransparentSalaries%22%3Atrue%2C%22industries%22%3A%5B%22biotechnology%22%2C%22healthcare%22%5D%2C%22roleYoeRange%22%3A%5B0%2C6%5D%2C%22roleTypes%22%3A%5B%22Individual+Contributor%22%5D%2C%22doctorateDegreeRequirements%22%3A%5B%22Preferred%22%2C%22Not+Mentioned%22%5D%2C%22jobTitleQuery%22%3A%22%28%28data+OR+ml+OR+%5C%22machine+learning%5C%22+OR+%5C%22ai%5C%22+OR+%5C%22artificial+intelligence%5C%22+OR+nlp+OR+statistical+OR+bi+OR+%5C%22business+intelligence%5C%22+OR+devops+OR+mlops%29+AND+%28engineer+OR+scientist+OR+science+OR+programmer%29%29+AND+NOT+%5C%22software+engineer%5C%22+AND+NOT+%5C%22electrical+engineer%5C%22%5Cn%22%7D"
        ),
        summary=(
            f"{_DS_TITLES}; SF within 100 miles or US remote/onsite/hybrid; "
            f"biotechnology or healthcare."
        ),
    ),
    SavedSearch(
        key="DA_Healthcare",
        label="Data & analytics dept - biotech & healthcare",
        url=(
            "https://hiringcafe.com/?searchState=%7B%22locations%22%3A%5B%7B%22id%22%3A%226xk1yZQBoEtHp_8Uv-2X%22%2C%22types%22%3A%5B%22locality%22%5D%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22San+Francisco%22%2C%22short_name%22%3A%22San+Francisco%22%2C%22types%22%3A%5B%22locality%22%5D%7D%2C%7B%22long_name%22%3A%22California%22%2C%22short_name%22%3A%22CA%22%2C%22types%22%3A%5B%22administrative_area_level_1%22%5D%7D%2C%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22geometry%22%3A%7B%22location%22%3A%7B%22lat%22%3A37.77493%2C%22lon%22%3A-122.41942%7D%7D%2C%22formatted_address%22%3A%22San+Francisco%2C+CA%2C+US%22%2C%22population%22%3A864816%2C%22workplace_types%22%3A%5B%5D%2C%22options%22%3A%7B%22radius%22%3A100%2C%22radius_unit%22%3A%22miles%22%2C%22ignore_radius%22%3Afalse%7D%7D%2C%7B%22types%22%3A%5B%22country%22%5D%2C%22formatted_address%22%3A%22United+States%22%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22United+States%22%2C%22short_name%22%3A%22US%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22workplace_types%22%3A%5B%22Remote%22%5D%2C%22options%22%3A%7B%7D%2C%22id%22%3A%22United+Statescountry%22%7D%5D%2C%22commitmentTypes%22%3A%5B%22Full+Time%22%2C%22Contract%22%5D%2C%22dateFetchedPastNDays%22%3A1440%2C%22restrictJobsToTransparentSalaries%22%3Atrue%2C%22industries%22%3A%5B%22biotechnology%22%2C%22healthcare%22%5D%2C%22departments%22%3A%5B%22Data+and+Analytics%22%5D%2C%22roleYoeRange%22%3A%5B0%2C6%5D%2C%22roleTypes%22%3A%5B%22Individual+Contributor%22%5D%2C%22doctorateDegreeRequirements%22%3A%5B%22Preferred%22%2C%22Not+Mentioned%22%5D%7D"
        ),
        summary=(
            f"Data and Analytics department; {_SF_REMOTE}; biotechnology or healthcare."
        ),
    ),
)


def saved_search(key: str) -> SavedSearch:
    """Look up a preset by its stable key."""
    for search in SAVED_SEARCHES:
        if search.key == key:
            return search
    valid = ", ".join(s.key for s in SAVED_SEARCHES)
    raise ScrapeError(f"unknown preset {key!r}; valid keys are: {valid}")


# ------------------------------------------------------------------- core API
@dataclass(frozen=True, slots=True)
class ScrapeConfig:
    """One scrape run.

    `search_state` is always a concrete decoded dict: preset and CLI naming
    are resolved before this is built, so the core generator stays unaware
    of how a user expressed the search.
    """

    search_state: dict[str, Any]
    max_jobs: int = 40
    max_pages: int = 25
    delay: float = 1.0
    descriptions: bool = True
    #: When set, each job is also written as an ingest-compatible page.
    #: Requires descriptions=True.
    raw_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class ScrapeEvent:
    """One thing that happened, ready to render.

    `message` is already formatted for a log line. Treat it -- and every
    string reachable through `row`, `hit` and `detail` -- as untrusted
    display text: it comes from job postings.
    """

    kind: Literal["build_id", "page", "job", "warning", "done"]
    message: str
    index: int = 0
    total: int = 0
    #: The flattened CSV row, on "job" events.
    row: dict[str, Any] | None = None
    hit: dict[str, Any] | None = None
    detail: dict[str, Any] | None = None
    #: Where the raw page was written, when raw_dir is configured.
    raw_path: Path | None = field(default=None)


def scrape(
    config: ScrapeConfig, client: Client | None = None
) -> Generator[ScrapeEvent, None, None]:
    """Run one search, yielding an event per thing that happened.

    A generator rather than a callback: cancellation is simply "stop
    iterating", it is synchronous so it composes with a thread worker, and
    it is trivially testable by draining into a list against a stub Client.
    Callers should close it explicitly (``gen.close()`` in a ``finally``) so
    GeneratorExit runs deterministically instead of at GC time.

    It performs no CSV or JSONL I/O -- collect `ev.row` for that -- but it
    *does* write raw pages when `config.raw_dir` is set. Closing the
    scrape->ingest loop is pipeline behaviour, not presentation, and the CLI
    and the dashboard must not diverge on it.

    Pass a Client built with a cancellation Event to make a run stoppable.
    Cancellation is cooperative: a request already inside `requests` runs to
    its timeout, so promise users "after the current request", not a second.

    Raises ScrapeError for configuration and protocol failures. Validation
    is eager, so a bad config raises here rather than on first iteration.
    """
    if config.max_jobs <= 0:
        raise ScrapeError(f"max_jobs must be positive, got {config.max_jobs}")
    if config.max_pages <= 0:
        raise ScrapeError(f"max_pages must be positive, got {config.max_pages}")
    if config.raw_dir is not None and not config.descriptions:
        raise ScrapeError(
            "--raw-dir requires descriptions: a search hit has no "
            "job_information.description, which is a non-Option String in "
            "fastingest's schema. A page synthesised without it either fails "
            "validation or silently stores an empty description."
        )
    warnings: deque[str] = deque()
    if client is None:
        client = Client(config.delay)
    # scrape() owns warning routing for the duration of the run: retry and
    # backoff diagnostics are events, not stderr, whoever built the Client.
    client.route_warnings(warnings.append)
    return _scrape(config, client, warnings)


def _scrape(
    config: ScrapeConfig, client: Client, warnings: deque[str]
) -> Generator[ScrapeEvent, None, None]:
    def drain() -> Generator[ScrapeEvent, None, None]:
        while warnings:
            yield ScrapeEvent("warning", warnings.popleft())

    rows = 0
    with_descriptions = 0
    seen: set[Any] = set()
    page = 0
    try:
        yield ScrapeEvent("build_id", "Bootstrapping buildId...")
        build_id = get_build_id(client)
        yield from drain()
        yield ScrapeEvent("build_id", f"  buildId = {build_id}")

        while rows < config.max_jobs and page < config.max_pages:
            yield ScrapeEvent("page", f"Search page {page}...", index=page)
            try:
                props = search_page(client, build_id, config.search_state, page)
            except StaleBuildId:
                yield ScrapeEvent(
                    "warning",
                    "  buildId went stale (site redeployed); re-bootstrapping...",
                )
                build_id = get_build_id(client)
                props = search_page(client, build_id, config.search_state, page)
            yield from drain()

            hits = props.get("ssrHits") or []
            if page == 0:
                yield ScrapeEvent(
                    "page", f"  {props.get('ssrTotalCount', '?')} total jobs match"
                )
            new_hits = [
                h
                for h in hits
                if isinstance(h, dict)
                and not h.get("is_hc_pinned")
                and h.get("requisition_id")
                and h.get("id") not in seen
            ]
            yield ScrapeEvent("page", f"  {len(hits)} hits, {len(new_hits)} new")

            for hit in new_hits:
                if rows >= config.max_jobs:
                    break
                if client.cancelled:
                    raise ScrapeCancelled("cancelled between jobs")
                seen.add(hit.get("id"))
                detail, canonical_url = None, ""
                if config.descriptions:
                    try:
                        detail, canonical_url = fetch_job_detail(
                            client, build_id, hit["requisition_id"]
                        )
                    except StaleBuildId:
                        build_id = get_build_id(client)
                        detail, canonical_url = fetch_job_detail(
                            client, build_id, hit["requisition_id"]
                        )
                    except ScrapeCancelled:
                        raise
                    except ScrapeError as e:
                        yield ScrapeEvent(
                            "warning",
                            f"  ! detail fetch failed for {hit.get('id')}: {e}",
                        )
                yield from drain()

                raw_path = None
                if config.raw_dir is not None and detail is not None:
                    merged = _merge_identity(detail, hit)
                    if missing := missing_job_keys(merged):
                        yield ScrapeEvent(
                            "warning",
                            f"  ! not writing a raw page for {hit.get('id')}: "
                            f"missing {', '.join(missing)}",
                        )
                    else:
                        raw_path = write_raw_page(detail, config.raw_dir, hit=hit)

                row = flatten(hit, detail, canonical_url)
                rows += 1
                description_chars = len(row["description_text"])
                if description_chars > 100:
                    with_descriptions += 1
                yield ScrapeEvent(
                    "job",
                    f"  [{rows}/{config.max_jobs}] {row['title']} @ {row['company']}"
                    f" (desc: {description_chars} chars)",
                    index=rows,
                    total=config.max_jobs,
                    row=row,
                    hit=hit,
                    detail=detail,
                    raw_path=raw_path,
                )

            if props.get("ssrIsLastPage") or not new_hits:
                yield ScrapeEvent("page", "  last page reached.")
                break
            page += 1
    except ScrapeCancelled:
        yield ScrapeEvent(
            "done",
            f"Cancelled after the current request: {rows} jobs "
            f"({with_descriptions} with full descriptions)",
            index=rows,
            total=config.max_jobs,
        )
        return
    yield ScrapeEvent(
        "done",
        f"Done: {rows} jobs ({with_descriptions} with full descriptions)",
        index=rows,
        total=config.max_jobs,
    )


# ------------------------------------------------------------------------ main
def parse_search_state(
    query: str | None,
    url: str | None,
    search_state: str | None,
    on_warning: Callable[[str], None] = _noop,
) -> dict[str, Any]:
    if url:
        qs = parse_qs(urlparse(url).query)
        if "searchState" in qs:
            try:
                from_url = json.loads(qs["searchState"][0])
            except json.JSONDecodeError as e:
                raise ScrapeError(f"searchState in --url is not valid JSON: {e}") from e
            if not isinstance(from_url, dict):
                raise ScrapeError("searchState in --url is not a JSON object")
            return from_url
        on_warning("WARNING: no searchState in --url; scraping default feed.")
        return {}
    if search_state:
        try:
            from_arg = json.loads(search_state)
        except json.JSONDecodeError as e:
            raise ScrapeError(f"--search-state is not valid JSON: {e}") from e
        if not isinstance(from_arg, dict):
            raise ScrapeError("--search-state is not a JSON object")
        return from_arg
    if query:
        return {"searchQuery": query}
    return {}


def resolve_search_state(
    preset: str | None = None,
    query: str | None = None,
    url: str | None = None,
    search_state: str | None = None,
    on_warning: Callable[[str], None] = _noop,
) -> dict[str, Any]:
    """Turn the four mutually exclusive input modes into one searchState.

    Shared by the CLI and the dashboard so they cannot disagree on
    precedence. Supplying none of them keeps the long-standing default-feed
    behaviour.
    """
    supplied = [
        name
        for name, value in (
            ("--preset", preset),
            ("--query", query),
            ("--url", url),
            ("--search-state", search_state),
        )
        if value
    ]
    if len(supplied) > 1:
        raise ScrapeError(
            f"{' and '.join(supplied)} are mutually exclusive; supply at most one."
        )
    if preset:
        return saved_search(preset).search_state()
    return parse_search_state(query, url, search_state, on_warning)


app = typer.Typer(
    add_completion=False,
    help="Scrape job listings from hiringcafe.com",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.command()
def main(
    query: str | None = typer.Option(
        None, "--query", help='keyword search, e.g. "software engineer"'
    ),
    url: str | None = typer.Option(
        None,
        "--url",
        help="a hiringcafe.com URL with searchState (set filters in the UI, copy the URL)",
    ),
    search_state: str | None = typer.Option(
        None, "--search-state", help="raw searchState JSON string"
    ),
    preset: str | None = typer.Option(
        None,
        "--preset",
        help=(
            "a saved search: "
            + ", ".join(s.key for s in SAVED_SEARCHES)
            + " (mutually exclusive with --query/--url/--search-state)"
        ),
    ),
    max_jobs: int = typer.Option(40, "--max-jobs"),
    max_pages: int = typer.Option(25, "--max-pages"),
    delay: float = typer.Option(
        1.0, "--delay", help="seconds between requests (default 1.0)"
    ),
    out: str = typer.Option("hiringcafe_jobs.csv", "--out"),
    jsonl: str | None = typer.Option(
        None, "--jsonl", help="also dump raw job JSON to this file"
    ),
    raw_dir: Path | None = typer.Option(
        None,
        "--raw-dir",
        help=(
            "also write ingest-compatible {requisition_id}.json.gz pages here "
            f"(the dashboard stages them in {DEFAULT_RAW_DIR}); "
            "incompatible with --no-descriptions"
        ),
    ),
    no_descriptions: bool = typer.Option(
        False,
        "--no-descriptions",
        help="skip per-job detail fetches (much faster, no description text)",
    ),
) -> None:
    if delay < 0.5:
        print("Refusing delay < 0.5s — be polite to their servers.", file=sys.stderr)
        delay = 0.5

    def warn(message: str) -> None:
        print(message, file=sys.stderr)

    try:
        config = ScrapeConfig(
            search_state=resolve_search_state(
                preset, query, url, search_state, on_warning=warn
            ),
            max_jobs=max_jobs,
            max_pages=max_pages,
            delay=delay,
            descriptions=not no_descriptions,
            raw_dir=raw_dir,
        )
        if preset:
            chosen = saved_search(preset)
            # The stable key and summary, never the multi-kilobyte URL.
            print(f"Preset {chosen.key}: {chosen.summary}")
        if raw_dir is not None:
            print(f"Writing raw pages to {raw_dir.resolve()}")
        events = scrape(config)
    except ScrapeError as e:
        sys.exit(str(e))

    rows: list[dict[str, Any]] = []
    raw_dump = None
    failure: str | None = None
    try:
        raw_dump = open(jsonl, "w", encoding="utf-8") if jsonl else None
        for event in events:
            print(event.message, flush=True)
            if event.row is not None:
                rows.append(event.row)
                if raw_dump:
                    raw_dump.write(
                        json.dumps(
                            {"hit": event.hit, "detail": event.detail},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
    except KeyboardInterrupt:
        print("\nInterrupted — writing what we have...")
    except ScrapeError as e:
        # Partial results are still worth writing, but the exit code must
        # not claim success.
        failure = str(e)
    finally:
        # Explicit close so GeneratorExit runs here rather than at GC time.
        events.close()
        if raw_dump:
            raw_dump.close()

    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {out}")
    if failure is not None:
        sys.exit(failure)


if __name__ == "__main__":
    app()
