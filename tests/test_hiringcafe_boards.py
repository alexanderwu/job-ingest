"""Board protocol, cache and shared traversal tests; no live requests."""

from dataclasses import replace
from datetime import date
import gzip
import threading

import pytest
from typer.testing import CliRunner

from job_ingest import scrape_hiringcafe as hc
from test_scrape_hiringcafe import StubClient, _Resp, a_job

DAY = date(2026, 9, 4)
SLUG = "healthcare-9ierbt6f"


def envelope(page=0, hits=None, last=True):
    return {
        "__N_SSP": True,
        "pageProps": {
            "hits": hits if hits is not None else [a_job()],
            "page": page,
            "totalCount": 3493,
            "companyCount": 56,
            "pageSize": 40,
            "isLastPage": last,
            "archived": False,
            "ssrError": None,
            "board": {
                "name": "Healthcare",
                "tagline": "Curated roles",
                "pinned_job_ids": ["id-abc123"],
                "owner_uid": "kept-verbatim",
            },
        },
    }


class BoardClient(StubClient):
    def __init__(self, payloads=None, **kwargs):
        super().__init__(**kwargs)
        self.payloads = payloads or [envelope()]

    def get(self, url, retries=3):
        if "/b/" in url:
            self.urls.append(url)
            return _Resp(json_body=self.payloads[int(url.rsplit("page=", 1)[1])])
        return super().get(url, retries)


class Offline(BoardClient):
    def get(self, *args, **kwargs):
        pytest.fail("a fully cached run must make zero requests")


def config(tmp_path, **kwargs):
    return hc.ScrapeConfig(
        hc.BoardSource(SLUG),
        interim_dir=tmp_path,
        run_date=DAY,
        descriptions=False,
        **kwargs,
    )


def jobs(events):
    return [e for e in events if e.kind == "job"]


@pytest.mark.parametrize(
    "url",
    [
        "https://foreign.example/b/valid",
        "https://hiringcafe.com.evil/b/valid",
        "https://hiringcafe.com/b/",
        "https://hiringcafe.com/b/UPPER",
        "https://hiringcafe.com/b/a/extra",
        "https://hiringcafe.com/b/../a",
        "https://hiringcafe.com/b/a%2fb",
        "https://hiringcafe.com/b/a?searchState={}",
        "https://hiringcafe.com/job/a",
        "https://user@hiringcafe.com/b/a",
    ],
)
def test_bad_urls(url):
    with pytest.raises(hc.ScrapeError):
        hc.resolve_source(url=url)


def test_sources_and_presets():
    assert hc.resolve_source(url=f"{hc.BASE}/b/{SLUG}") == hc.BoardSource(SLUG)
    assert hc.resolve_source() == hc.SearchSource({})
    assert hc.resolve_source(
        url=hc.BASE + "/?searchState=%7B%22x%22%3A1%7D"
    ) == hc.SearchSource({"x": 1})
    assert {s.key for s in hc.SAVED_PRESETS} == {
        "DS_SF_Remote",
        "DA_SF_Remote",
        "DS_Healthcare",
        "DA_Healthcare",
        "Board_DS_SF_Remote",
        "Board_DA_SF_Remote",
        "Board_DA_Healthcare",
        "Board_Healthcare",
    }
    for preset in hc.SAVED_PRESETS:
        assert isinstance(
            preset.source(),
            hc.BoardSource if preset.kind == "board" else hc.SearchSource,
        )
        assert preset.label.startswith(
            "Board -" if preset.kind == "board" else "Search -"
        )
    with pytest.raises(hc.ScrapeError, match="mutually exclusive"):
        hc.resolve_source(preset="Board_Healthcare", query="data")


@pytest.mark.parametrize(
    "change",
    [
        {"hits": {}},
        {"ssrError": "failed"},
        {"page": 2},
        {"isLastPage": "false"},
        {"totalCount": -1},
        {"pageSize": 0},
        {"companyCount": True},
    ],
)
def test_protocol_drift(change):
    payload = envelope()
    payload["pageProps"].update(change)
    with pytest.raises(hc.ScrapeError):
        hc.board_page(BoardClient([payload]), "BUILD1", SLUG, 0)


def test_missing_props():
    with pytest.raises(hc.ScrapeError):
        hc.board_page(BoardClient([{"__N_SSP": True}]), "BUILD1", SLUG, 0)


def test_atomic_roundtrip_and_replacement(tmp_path, monkeypatch):
    path = hc.board_page_path(tmp_path, DAY, SLUG, 0)
    assert path == tmp_path / "2026/09/04" / SLUG / "page0.json.gz"
    payload = envelope()
    hc.write_board_page(payload, path)
    first = path.read_bytes()
    hc.write_board_page(payload, path)
    assert path.read_bytes() == first
    assert hc.read_board_page(path) == payload
    monkeypatch.setattr(
        hc.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk full"))
    )
    with pytest.raises(hc.ScrapeError, match="cannot write"):
        hc.write_board_page(envelope(hits=[]), path)
    assert path.read_bytes() == first
    assert not list(path.parent.glob("*.part"))
    with pytest.raises(hc.ScrapeError):
        hc.write_board_page({"bad": object()}, path)


@pytest.mark.parametrize("wrapped", [False, True])
def test_offline_cache_drives_pages_and_dedup(tmp_path, wrapped):
    for page in range(2):
        payload = envelope(page, [a_job("same"), a_job(f"unique-{page}")], page == 1)
        if wrapped:
            payload = {"props": payload}
        hc.write_board_page(payload, hc.board_page_path(tmp_path, DAY, SLUG, page))
    # Stray filenames are never considered.
    (hc.board_page_path(tmp_path, DAY, SLUG, 0).parent / "page.json.gz").write_bytes(
        b"bad"
    )
    events = list(hc.scrape(config(tmp_path), Offline()))
    assert len(jobs(events)) == 3
    assert not any(e.kind == "build_id" for e in events)
    assert sum("(cached)" in e.message for e in events) == 2
    assert any("Healthcare" in e.message for e in events)
    assert events[-1].kind == "done"


@pytest.mark.parametrize(
    "bad",
    [
        b"bad gzip",
        gzip.compress(b"{"),
        gzip.compress(b"[]"),
        gzip.compress(b'{"pageProps":null}'),
        gzip.compress(b'{"pageProps":{}}'),
    ],
)
def test_corrupt_cache_warns_and_refetches(tmp_path, bad):
    path = hc.board_page_path(tmp_path, DAY, SLUG, 0)
    path.parent.mkdir(parents=True)
    path.write_bytes(bad)
    client = BoardClient()
    events = list(hc.scrape(config(tmp_path), client))
    assert sum(e.kind == "warning" for e in events) == 1
    assert len(jobs(events)) == 1
    assert hc.read_board_page(path) == envelope()
    assert client.urls == [
        hc.BASE + "/",
        f"{hc.BASE}/_next/data/BUILD1/b/{SLUG}.json?page=0",
    ]


def test_refresh_limits_and_preserves_higher_pages(tmp_path):
    path = hc.board_page_path(tmp_path, DAY, SLUG, 0)
    higher = hc.board_page_path(tmp_path, DAY, SLUG, 5)
    hc.write_board_page(envelope(hits=[]), path)
    hc.write_board_page(envelope(5), higher)
    original = higher.read_bytes()
    payload = envelope(hits=[a_job(str(n)) for n in range(106)], last=False)
    client = BoardClient([payload])
    events = list(
        hc.scrape(config(tmp_path, refresh=True, max_jobs=2, max_pages=1), client)
    )
    assert len(jobs(events)) == 2
    assert hc.read_board_page(path) == payload
    assert higher.read_bytes() == original
    assert any("fetched ->" in e.message for e in events)


@pytest.mark.parametrize("board", [False, True])
def test_requisition_identity_for_both_sources(tmp_path, board):
    hits = [
        a_job("same", id="one"),
        a_job("same", id="two"),
        a_job("different", id="one"),
        a_job(123),
        a_job("123"),
        a_job("Case"),
        a_job("case"),
        a_job(None, id=None),
        a_job("idless1", id=None),
        a_job("idless2", id=None),
    ]

    class IdentityClient(BoardClient):
        def get(self, url, retries=3):
            if "index.json" in url:
                return _Resp(
                    json_body={"pageProps": {"ssrHits": hits, "ssrIsLastPage": True}}
                )
            return super().get(url, retries)

    cfg = (
        config(tmp_path)
        if board
        else hc.ScrapeConfig(hc.SearchSource(), descriptions=False)
    )
    events = list(hc.scrape(cfg, IdentityClient([envelope(hits=hits)])))
    assert len(jobs(events)) == 7
    assert any("1 hits without a requisition_id" in e.message for e in events)
    assert hc.raw_filename("Case") != hc.raw_filename("case")


def test_skip_existing_does_not_stop_or_spend_budget(tmp_path):
    raw = tmp_path / "raw"
    hc.write_raw_page(a_job("old"), raw)
    old = raw / hc.raw_filename("old")
    mtime = old.stat().st_mtime_ns
    client = BoardClient(
        [
            envelope(0, [a_job("old")], False),
            envelope(1, [a_job("old"), a_job("new"), a_job("extra")]),
        ]
    )
    cfg = replace(
        config(tmp_path, max_jobs=1),
        descriptions=True,
        raw_dir=raw,
        skip_existing_raw=True,
    )
    events = list(hc.scrape(cfg, client))
    assert [e.hit["requisition_id"] for e in jobs(events)] == ["new"]
    assert not any("/job/x-old" in u for u in client.urls)
    assert any("/job/x-new" in u for u in client.urls)
    assert sum("already present" in e.message for e in events) == 1
    assert old.stat().st_mtime_ns == mtime


def test_cancel_after_page_keeps_archive(tmp_path):
    cancel = threading.Event()

    class CancelAfterPage(BoardClient):
        def get(self, url, retries=3):
            response = super().get(url, retries)
            if "/b/" in url:
                cancel.set()
            return response

    events = list(hc.scrape(config(tmp_path), CancelAfterPage(cancel=cancel)))
    assert not jobs(events)
    assert "Cancelled" in events[-1].message
    assert hc.read_board_page(hc.board_page_path(tmp_path, DAY, SLUG, 0)) == envelope()


def test_stale_build_retries_same_board_page(tmp_path):
    class Stale(BoardClient):
        stale = True

        def get(self, url, retries=3):
            if "/b/" in url and self.stale:
                self.stale = False
                self.urls.append(url)
                raise hc.StaleBuildId(url)
            return super().get(url, retries)

    client = Stale()
    events = list(hc.scrape(config(tmp_path), client))
    assert len(jobs(events)) == 1
    assert sum("/b/" in u for u in client.urls) == 2
    assert any("stale" in e.message for e in events)


@pytest.mark.parametrize(
    "args",
    [
        ["--skip-existing-raw"],
        ["--skip-existing-raw", "--raw-dir", "unused", "--no-descriptions"],
    ],
)
def test_cli_eager_skip_validation(args, monkeypatch):
    monkeypatch.setattr(
        hc.Client, "get", lambda *a: pytest.fail("must fail before network")
    )
    result = CliRunner().invoke(hc.app, args)
    assert result.exit_code != 0
    assert "requires" in result.output


def test_yesterday_is_not_a_cache_hit(tmp_path):
    yesterday = hc.board_page_path(tmp_path, date(2026, 9, 3), SLUG, 0)
    hc.write_board_page(envelope(hits=[]), yesterday)
    client = BoardClient()
    assert len(jobs(list(hc.scrape(config(tmp_path), client)))) == 1
    assert any("/b/" in u for u in client.urls)
    assert hc.read_board_page(yesterday)["pageProps"]["hits"] == []


def test_cached_existing_board_is_entirely_offline(tmp_path):
    raw = tmp_path / "raw"
    hc.write_raw_page(a_job(), raw)
    hc.write_board_page(envelope(), hc.board_page_path(tmp_path, DAY, SLUG, 0))
    cfg = replace(
        config(tmp_path), descriptions=True, raw_dir=raw, skip_existing_raw=True
    )
    client = Offline()
    events = list(hc.scrape(cfg, client))
    assert not jobs(events)
    assert client._last_request == 0
    assert any("1 already present" in e.message for e in events)


def test_cross_page_duplicate_fetches_detail_once(tmp_path):
    client = BoardClient(
        [
            envelope(0, [a_job("same")], False),
            envelope(1, [a_job("same"), a_job("new")]),
        ]
    )
    events = list(hc.scrape(replace(config(tmp_path), descriptions=True), client))
    assert len(jobs(events)) == 2
    assert sum("/job/x-same" in url for url in client.urls) == 1


def test_archived_board_warns_but_keeps_owner_pins(tmp_path):
    payload = envelope()
    payload["pageProps"]["archived"] = True
    events = list(hc.scrape(config(tmp_path), BoardClient([payload])))
    assert len(jobs(events)) == 1
    assert any(e.kind == "warning" and "archived" in e.message for e in events)


def test_malformed_url_is_a_domain_error():
    with pytest.raises(hc.ScrapeError):
        hc.resolve_source(url="https://[invalid/b/a")


def test_cli_board_source_and_options(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(
        hc, "scrape", lambda cfg: seen.append(cfg) or (event for event in ())
    )
    result = CliRunner().invoke(
        hc.app,
        [
            "--preset",
            "Board_Healthcare",
            "--interim-dir",
            str(tmp_path),
            "--refresh",
            "--raw-dir",
            str(tmp_path / "raw"),
            "--skip-existing-raw",
            "--impersonate",
            "none",
            "--user-agent",
            "",
        ],
    )
    assert result.exit_code == 0, result.output
    cfg = seen[0]
    assert cfg.source == hc.BoardSource(SLUG)
    assert cfg.interim_dir == tmp_path
    assert cfg.refresh and cfg.skip_existing_raw
    assert cfg.impersonate == "none" and cfg.user_agent == ""
    assert cfg.max_pages == 50 and cfg.max_jobs == 40
    assert str(cfg.archive_dir) in result.output
