from types import SimpleNamespace

import pytest

from job_ingest.scrape_hiringcafe import BASE, USER_AGENT, Client, ScrapeError


@pytest.mark.parametrize("ua", [USER_AGENT, ""])
def test_transport_headers(monkeypatch, ua):
    sessions = []
    calls = []

    def session(**kwargs):
        obj = SimpleNamespace(
            headers={}, get=lambda url, **kw: calls.append((url, kw)) or response(200)
        )
        sessions.append((kwargs, obj))
        return obj

    monkeypatch.setattr("job_ingest.scrape_hiringcafe.curl_requests.Session", session)
    client = Client(0, user_agent=ua)
    client.get(BASE + "/")
    client.get(BASE + "/_next/data/build/index.json")
    assert sessions[0][0] == {"impersonate": "chrome"}
    assert sessions[0][1].headers.get("User-Agent") == (ua or None)
    assert calls[0][1]["headers"] == {}
    assert calls[1][1]["headers"] == {
        "Accept": "*/*",
        "x-nextjs-data": "1",
        "Referer": BASE + "/",
    }


def response(status, headers=None):
    return SimpleNamespace(
        status_code=status, headers=headers or {}, raise_for_status=lambda: None
    )


@pytest.mark.parametrize("status", [403, 429, 500, 502, 503, 504])
def test_transport_backoff(monkeypatch, status):
    client = Client(0, impersonate="none")
    calls, sleeps, warnings = [], [], []
    monkeypatch.setattr(
        client.session,
        "get",
        lambda *a, **kw: (
            calls.append(a) or response(status, {"cf-mitigated": "challenge"})
        ),
    )
    monkeypatch.setattr(client, "_sleep", sleeps.append)
    client.route_warnings(warnings.append)
    with pytest.raises(ScrapeError) as exc:
        client.get(BASE)
    if status == 403:
        assert len(calls) == 1
        assert sleeps == []
        assert "'none'" in str(exc.value) and "--user-agent" in str(exc.value)
    else:
        assert len(calls) == 3
        assert sleeps == [5, 10]
    if status == 503:
        assert any("cf-mitigated" in w for w in warnings)
