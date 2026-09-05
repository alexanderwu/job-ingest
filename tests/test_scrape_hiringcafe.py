"""Tests for the HiringCafe scraper. No network is touched.

The HTTP layer is stubbed at the `Client` boundary, so `scrape()` is drained
into a list exactly the way the dashboard's worker drives it. The one test
that needs a real job object reuses a Rust golden fixture rather than
inventing one, which means it fails loudly if schema.rs ever drifts.
"""

from __future__ import annotations

import gzip
import json
import re
import shutil
import threading
from pathlib import Path
from typing import Any

import pytest

from job_ingest.ingest_and_benchmark import run_fastingest
from job_ingest.scrape_hiringcafe import (
    SAVED_SEARCHES,
    SAVED_BOARDS,
    SearchSource,
    Client,
    ScrapeCancelled,
    ScrapeConfig,
    ScrapeError,
    missing_job_keys,
    raw_filename,
    resolve_search_state,
    saved_search,
    scrape,
    write_raw_page,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "fastingest" / "tests" / "fixtures"
DOCS = REPO_ROOT / "docs" / "saved_hiringcafe_searches.md"

needs_sidecar = pytest.mark.skipif(
    shutil.which("cargo") is None,
    reason="needs a Rust toolchain to build the fastingest sidecar",
)


def a_job(requisition_id: str = "abc123", **overrides: Any) -> dict[str, Any]:
    """A minimal object satisfying REQUIRED_JOB_KEYS."""
    job: dict[str, Any] = {
        "id": f"id-{requisition_id}",
        "board_token": "acme",
        "source": "greenhouse",
        "apply_url": "https://example.invalid/apply",
        "source_and_board_token": "greenhouse:acme",
        "requisition_id": requisition_id,
        "collapse_key": f"ck-{requisition_id}",
        "is_expired": False,
        "objectID": f"obj-{requisition_id}",
        "job_information": {"title": "Data Scientist", "description": "<p>Hi</p>"},
        "v5_processed_job_data": {"company_name": "Acme"},
    }
    job.update(overrides)
    return job


class TestRawFilename:
    """The filename is not the ingest key -- requisition_id inside the JSON is
    -- so mangling costs only readability. Collisions, though, silently lose a
    row, which is why anything sanitisation touched gets a digest."""

    def test_a_conventional_id_is_used_verbatim(self) -> None:
        assert raw_filename("abc123") == "abc123.json.gz"
        assert raw_filename("0123456789abcdef") == "0123456789abcdef.json.gz"

    @pytest.mark.parametrize(
        "requisition_id",
        ["REQ/12:34", "a b", "x?y*z", "back\\slash", "trailing.", "trailing ", "café"],
    )
    def test_anything_unsafe_is_sanitised_and_hashed(self, requisition_id: str) -> None:
        name = raw_filename(requisition_id)

        assert name is not None
        assert name.endswith(".json.gz")
        stem = name[: -len(".json.gz")]
        assert re.fullmatch(r"[a-z0-9._-]+", stem), stem
        # Sanitisation changed something, so a digest of the original must be
        # present: 16 hex characters after a hyphen.
        assert re.search(r"-[0-9a-f]{16}$", stem), stem

    def test_two_ids_that_sanitise_alike_get_different_files(self) -> None:
        """Both slug to 'req-12-34'; without the digest one would clobber the
        other and a row would vanish with no error anywhere."""
        assert raw_filename("REQ/12/34") != raw_filename("REQ:12:34")

    def test_case_only_variants_do_not_collide_on_windows(self) -> None:
        """NTFS folds case, so 'ABC123' and 'abc123' would be one file."""
        names = {raw_filename(i) for i in ("abc123", "ABC123", "AbC123")}

        assert len(names) == 3

    @pytest.mark.parametrize("reserved", ["con", "CON", "nul", "com1", "LPT9"])
    def test_windows_reserved_names_are_escaped(self, reserved: str) -> None:
        name = raw_filename(reserved)

        assert name is not None
        assert name.split(".", 1)[0] != reserved.lower()

    def test_a_long_id_is_truncated_and_still_unique(self) -> None:
        long_a = "x" * 400 + "a"
        long_b = "x" * 400 + "b"

        name_a, name_b = raw_filename(long_a), raw_filename(long_b)

        assert name_a is not None and name_b is not None
        assert name_a != name_b
        assert len(name_a) < 120

    @pytest.mark.parametrize("unusable", ["", "   ", "\t"])
    def test_an_unusable_id_yields_nothing(self, unusable: str) -> None:
        assert raw_filename(unusable) is None

    def test_it_is_deterministic(self) -> None:
        assert raw_filename("REQ/12:34") == raw_filename("REQ/12:34")


class TestWriteRawPage:
    def test_it_round_trips_to_the_shape_fastingest_expects(
        self, tmp_path: Path
    ) -> None:
        path = write_raw_page(a_job(), tmp_path)

        assert path == tmp_path / "abc123.json.gz"
        assert path is not None
        with gzip.open(path, "rt", encoding="utf-8") as f:
            payload = json.load(f)
        assert payload["__N_SSG"] is True
        assert payload["pageProps"]["job"]["requisition_id"] == "abc123"

    def test_the_payload_is_stored_verbatim(self, tmp_path: Path) -> None:
        """An explicit, accepted privacy tradeoff: raw pages keep whatever the
        site returned, Firebase user-activity UIDs included. Nothing here may
        silently scrub or transform them."""
        job = a_job(user_activity={"uid": "firebase-uid-1234"})

        path = write_raw_page(job, tmp_path)

        assert path is not None
        with gzip.open(path, "rt", encoding="utf-8") as f:
            stored = json.load(f)["pageProps"]["job"]
        assert stored["user_activity"] == {"uid": "firebase-uid-1234"}

    def test_no_temp_files_survive_a_successful_write(self, tmp_path: Path) -> None:
        """A temp file ending in .json.gz would be picked up mid-write by
        list_inputs() in lib.rs and reported as a phantom validation error --
        hence the .part suffix and the cleanup."""
        write_raw_page(a_job(), tmp_path)

        assert [p.name for p in tmp_path.iterdir()] == ["abc123.json.gz"]

    def test_no_temp_files_survive_a_failed_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "job_ingest.scrape_hiringcafe.os.replace",
            lambda *_a, **_k: (_ for _ in ()).throw(OSError("disk full")),
        )
        with pytest.raises(ScrapeError, match="cannot write"):
            write_raw_page(a_job(), tmp_path)

        assert list(tmp_path.iterdir()) == []

    def test_a_second_write_overwrites_cleanly(self, tmp_path: Path) -> None:
        write_raw_page(a_job(), tmp_path)
        path = write_raw_page(a_job(collapse_key="ck-2"), tmp_path)

        assert path is not None
        assert [p.name for p in tmp_path.iterdir()] == ["abc123.json.gz"]
        with gzip.open(path, "rt", encoding="utf-8") as f:
            assert json.load(f)["pageProps"]["job"]["collapse_key"] == "ck-2"

    def test_missing_identity_fields_are_filled_from_the_hit(
        self, tmp_path: Path
    ) -> None:
        detail = a_job()
        del detail["objectID"]
        del detail["source_and_board_token"]

        path = write_raw_page(
            detail,
            tmp_path,
            hit={"objectID": "obj-x", "source_and_board_token": "greenhouse:acme"},
        )

        assert path is not None
        with gzip.open(path, "rt", encoding="utf-8") as f:
            assert json.load(f)["pageProps"]["job"]["objectID"] == "obj-x"

    def test_an_incomplete_page_is_skipped_rather_than_written(
        self, tmp_path: Path
    ) -> None:
        """Better an offline warning than a file that fails at ingest time."""
        detail = a_job()
        del detail["v5_processed_job_data"]

        assert write_raw_page(detail, tmp_path) is None
        assert list(tmp_path.iterdir()) == []

    def test_missing_keys_are_named(self) -> None:
        detail = a_job()
        del detail["objectID"]
        del detail["collapse_key"]

        assert missing_job_keys(detail) == ("collapse_key", "objectID")


@needs_sidecar
def test_a_written_page_actually_ingests(tmp_path: Path) -> None:
    """The test that proves the two formats compose.

    A real Rust golden fixture guarantees a valid job object without
    inventing one, so this fails loudly if schema.rs drifts away from what
    write_raw_page() produces.
    """
    with gzip.open(FIXTURES / "no_enrichment.json.gz", "rt", encoding="utf-8") as f:
        job = json.load(f)["pageProps"]["job"]
    raw_dir = tmp_path / "raw"
    out_dir = tmp_path / "out"

    assert write_raw_page(job, raw_dir) is not None
    _table, stats = run_fastingest(
        raw_dir, out_dir, limit=None, full=True, parquet=False
    )

    assert stats["ok"] == 1, stats.get("error_samples")


class StubClient(Client):
    """A Client that answers from a canned URL -> payload map.

    Subclassing keeps `delay`, the cancellation Event and the warning sink
    real, so cancellation and backoff behaviour are exercised rather than
    mocked away.
    """

    def __init__(self, pages: int = 1, jobs_per_page: int = 2, **kwargs: Any) -> None:
        super().__init__(delay=0.0, **kwargs)
        self.pages = pages
        self.jobs_per_page = jobs_per_page
        self.urls: list[str] = []

    def get(self, url: str, retries: int = 3) -> Any:  # type: ignore[override]
        if self.cancelled:
            raise ScrapeCancelled(f"cancelled before requesting {url}")
        self.urls.append(url)
        if url.endswith("/"):
            return _Resp(text='"buildId":"BUILD1"')
        if "index.json" in url:
            page = int(url.rsplit("page=", 1)[1])
            hits = [
                {
                    "id": f"id-p{page}n{i}",
                    "requisition_id": f"p{page}n{i}",
                    "source": "greenhouse",
                    "board_token": "acme",
                }
                for i in range(self.jobs_per_page)
            ]
            return _Resp(
                json_body={
                    "pageProps": {
                        "ssrHits": hits,
                        "ssrTotalCount": self.pages * self.jobs_per_page,
                        "ssrIsLastPage": page >= self.pages - 1,
                    }
                }
            )
        requisition_id = url.rsplit("/job/x-", 1)[1].removesuffix(".json")
        return _Resp(json_body={"pageProps": {"job": a_job(requisition_id)}})


class _Resp:
    def __init__(self, text: str = "", json_body: Any = None) -> None:
        self.text = text
        self.url = "stub://"
        self._json = json_body

    def json(self) -> Any:
        return self._json


class TestScrape:
    def test_the_event_sequence_is_what_a_frontend_expects(self) -> None:
        events = list(scrape(ScrapeConfig(SearchSource(), max_jobs=4), StubClient()))
        kinds = [e.kind for e in events]

        assert kinds[0] == "build_id"
        assert kinds[-1] == "done"
        assert kinds.count("job") == 2
        jobs = [e for e in events if e.kind == "job"]
        assert jobs[0].index == 1 and jobs[0].total == 4
        assert jobs[0].hit is not None and jobs[0].detail is not None

    def test_max_jobs_is_honoured_across_pages(self) -> None:
        events = list(
            scrape(
                ScrapeConfig(SearchSource(), max_jobs=3),
                StubClient(pages=5, jobs_per_page=2),
            )
        )

        assert sum(1 for e in events if e.kind == "job") == 3

    def test_max_pages_bounds_the_walk(self) -> None:
        client = StubClient(pages=99, jobs_per_page=1)

        list(scrape(ScrapeConfig(SearchSource(), max_jobs=999, max_pages=2), client))

        assert sum(1 for u in client.urls if "index.json" in u) == 2

    def test_it_writes_nothing_without_a_raw_dir(self, tmp_path: Path) -> None:
        """The raw corpus is the only thing a scrape writes, and only when
        it is asked to; anything else is the caller's business."""
        list(scrape(ScrapeConfig(SearchSource(), max_jobs=2), StubClient()))

        assert list(tmp_path.iterdir()) == []

    def test_it_writes_raw_pages_when_configured(self, tmp_path: Path) -> None:
        """Closing the scrape->ingest loop is pipeline behaviour, so the CLI
        and the dashboard cannot diverge on it."""
        events = list(
            scrape(
                ScrapeConfig(SearchSource(), max_jobs=2, raw_dir=tmp_path), StubClient()
            )
        )

        written = [e.raw_path for e in events if e.kind == "job"]
        assert all(p is not None for p in written)
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "p0n0.json.gz",
            "p0n1.json.gz",
        ]

    def test_an_incomplete_page_becomes_a_warning_not_a_crash(
        self, tmp_path: Path
    ) -> None:
        class Incomplete(StubClient):
            def get(self, url: str, retries: int = 3) -> Any:
                resp = super().get(url, retries)
                if "/job/x-" in url:
                    del resp._json["pageProps"]["job"]["objectID"]
                return resp

        events = list(
            scrape(
                ScrapeConfig(SearchSource(), max_jobs=1, raw_dir=tmp_path), Incomplete()
            )
        )

        warnings = [e.message for e in events if e.kind == "warning"]
        assert any("objectID" in w for w in warnings)
        assert list(tmp_path.iterdir()) == []

    def test_raw_dir_without_descriptions_is_a_hard_error(self, tmp_path: Path) -> None:
        """A search hit has no job_information.description, a non-Option String
        in schema.rs; a synthesised page would fail validation or silently
        store an empty description."""
        with pytest.raises(ScrapeError, match="requires descriptions"):
            scrape(ScrapeConfig(SearchSource(), descriptions=False, raw_dir=tmp_path))

    def test_configuration_is_validated_eagerly(self) -> None:
        """Not on first iteration: a bad config is the caller's mistake and
        should surface where it was made."""
        with pytest.raises(ScrapeError, match="max_jobs must be positive"):
            scrape(ScrapeConfig(SearchSource(), max_jobs=0))

    def test_an_unrecognisable_homepage_raises_rather_than_exits(self) -> None:
        class NoBuildId(StubClient):
            def get(self, url: str, retries: int = 3) -> Any:
                return _Resp(text="<html>nothing here</html>")

        with pytest.raises(ScrapeError, match="buildId"):
            list(scrape(ScrapeConfig(SearchSource()), NoBuildId()))

    def test_cancellation_stops_after_the_current_request(self) -> None:
        """Cooperative, not instant: the promise is 'after the current
        request', which is what the UI must say."""
        cancel = threading.Event()
        client = StubClient(pages=9, jobs_per_page=5, cancel=cancel)
        events = []
        for event in scrape(ScrapeConfig(SearchSource(), max_jobs=99), client):
            events.append(event)
            if event.kind == "job":
                cancel.set()

        assert events[-1].kind == "done"
        assert "Cancelled" in events[-1].message
        assert sum(1 for e in events if e.kind == "job") == 1

    def test_closing_the_generator_runs_cleanup_deterministically(self) -> None:
        gen = scrape(ScrapeConfig(SearchSource(), max_jobs=99), StubClient(pages=9))
        next(gen)

        gen.close()

        with pytest.raises(StopIteration):
            next(gen)

    def test_client_retry_diagnostics_surface_as_warnings(self) -> None:
        class Flaky(StubClient):
            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self.failed = False

            def get(self, url: str, retries: int = 3) -> Any:
                if "index.json" in url and not self.failed:
                    self.failed = True
                    self._on_warning("  ! HTTP 503, backing off 5s...")
                return super().get(url, retries)

        events = list(scrape(ScrapeConfig(SearchSource(), max_jobs=1), Flaky()))

        assert any("503" in e.message for e in events if e.kind == "warning")


class TestSavedSearches:
    def test_the_registry_has_exactly_the_four_stable_keys(self) -> None:
        assert tuple(s.key for s in SAVED_SEARCHES) == (
            "DS_SF_Remote",
            "DA_SF_Remote",
            "DS_Healthcare",
            "DA_Healthcare",
        )

    @pytest.mark.parametrize(
        ("key", "has_title_query", "has_department", "has_industries", "workplaces"),
        [
            ("DS_SF_Remote", True, False, False, ["Remote"]),
            ("DA_SF_Remote", False, True, False, ["Remote"]),
            ("DS_Healthcare", True, False, True, ["Remote", "Onsite", "Hybrid"]),
            ("DA_Healthcare", False, True, True, ["Remote"]),
        ],
    )
    def test_each_url_decodes_to_its_distinguishing_filters(
        self,
        key: str,
        has_title_query: bool,
        has_department: bool,
        has_industries: bool,
        workplaces: list[str],
    ) -> None:
        state = saved_search(key).search_state()

        assert ("jobTitleQuery" in state) is has_title_query
        assert (state.get("departments") == ["Data and Analytics"]) is has_department
        assert (state.get("industries") == ["biotechnology", "healthcare"]) is (
            has_industries
        )
        # The US-wide location entry carries the workplace types.
        assert state["locations"][1]["workplace_types"] == workplaces

    def test_the_shared_filters_are_preserved_exactly(self) -> None:
        for search in SAVED_SEARCHES:
            state = search.search_state()

            assert state["dateFetchedPastNDays"] == 1440, search.key
            assert state["commitmentTypes"] == ["Full Time", "Contract"], search.key
            assert state["restrictJobsToTransparentSalaries"] is True, search.key
            assert state["roleYoeRange"] == [0, 6], search.key
            assert state["roleTypes"] == ["Individual Contributor"], search.key
            assert state["doctorateDegreeRequirements"] == [
                "Preferred",
                "Not Mentioned",
            ], search.key

    def test_resolution_returns_independently_owned_dicts(self) -> None:
        """A caller mutating one run's state must not poison the next."""
        first = saved_search("DS_SF_Remote").search_state()
        second = saved_search("DS_SF_Remote").search_state()

        assert first == second
        first["locations"].clear()
        assert second["locations"] != []

    def test_an_unknown_preset_lists_the_valid_keys(self) -> None:
        with pytest.raises(ScrapeError, match="DS_SF_Remote"):
            saved_search("nope")

    def test_the_packaged_registry_matches_the_documented_source(self) -> None:
        """A repository-only drift guard: docs/ is the human-readable
        provenance, but the packaged registry is what runs, and an edit to
        one must not silently diverge from the other."""
        text = DOCS.read_text(encoding="utf-8").split("# Searches", 1)[1]
        documented = dict(
            re.findall(r"^## (\S+)\s*\n\s*\n(https://\S+)\s*$", text, re.MULTILINE)
        )

        assert documented == {s.key: s.url for s in SAVED_SEARCHES}
        boards = DOCS.read_text(encoding="utf-8").split("# Searches", 1)[0]
        documented_boards = dict(
            re.findall(r"^## (\S+)\s*\n\s*\n(https://\S+)\s*$", boards, re.MULTILINE)
        )
        assert documented_boards == {
            s.key.removeprefix("Board_"): s.url for s in SAVED_BOARDS
        }


class TestResolveSearchState:
    def test_a_preset_wins_when_it_is_the_only_input(self) -> None:
        state = resolve_search_state(preset="DA_SF_Remote")

        assert state["departments"] == ["Data and Analytics"]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"preset": "DS_SF_Remote", "query": "data"},
            {
                "preset": "DS_SF_Remote",
                "url": "https://hiringcafe.com/?searchState=%7B%7D",
            },
            {"preset": "DS_SF_Remote", "search_state": "{}"},
            {"query": "data", "search_state": "{}"},
        ],
    )
    def test_input_modes_are_mutually_exclusive(self, kwargs: dict[str, str]) -> None:
        with pytest.raises(ScrapeError, match="mutually exclusive"):
            resolve_search_state(**kwargs)

    def test_no_input_keeps_the_default_feed(self) -> None:
        assert resolve_search_state() == {}

    def test_a_query_becomes_a_search_query(self) -> None:
        assert resolve_search_state(query="data analyst") == {
            "searchQuery": "data analyst"
        }

    def test_a_url_without_search_state_warns_rather_than_prints(self) -> None:
        warnings: list[str] = []

        state = resolve_search_state(
            url="https://hiringcafe.com/", on_warning=warnings.append
        )

        assert state == {}
        assert any("default feed" in w for w in warnings)

    def test_malformed_raw_json_is_a_domain_error(self) -> None:
        with pytest.raises(ScrapeError, match="not valid JSON"):
            resolve_search_state(search_state="{nope")

    def test_an_unknown_preset_is_rejected(self) -> None:
        with pytest.raises(ScrapeError, match="unknown preset"):
            resolve_search_state(preset="DS_Mars")
