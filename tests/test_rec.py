"""
End-to-end tests for the recommendation engine over a synthetic corpus:
fixtures -> msgspec ingest -> recindex (hash embedder: deterministic,
no network) -> the four query types, incremental re-index, the sqlite-vec
-> numpy fallback, and the FastAPI app.
"""

import gzip
import json
import os
import sqlite3
import time

import pytest

import make_fixtures
import recindex
import recommend
from ingest_and_benchmark import load_duckdb, run_msgspec_ingest
from make_fixtures import RESUMES, archetype_of
from recommend import Filters

N_JOBS = 120


def build_corpus(root, n=N_JOBS):
    raw, out = root / "raw", root / "out"
    out.mkdir()
    make_fixtures.write_files(raw, n)
    table, stats = run_msgspec_ingest(
        raw, out, limit=None, full=True, parquet=False, workers=1
    )
    assert stats["errors"] == 0 and stats["ok"] == n
    load_duckdb(table, out / "jobs.duckdb", full=True)
    recindex.build_index(out / "jobs.sqlite", model="hash", quiet=True)
    return raw, out


@pytest.fixture(scope="session")
def corpus(tmp_path_factory):
    """Read-only shared corpus; mutation tests build their own."""
    return build_corpus(tmp_path_factory.mktemp("corpus"))


@pytest.fixture(scope="session")
def con(corpus):
    return recommend.get_connection(corpus[1] / "jobs.sqlite")


def archetypes(rows):
    return [archetype_of(r["requisition_id"]) for r in rows]


# -- indexing ---------------------------------------------------------------


def test_index_full_then_noop(corpus):
    db = corpus[1] / "jobs.sqlite"
    stats = recindex.build_index(db, model="hash", quiet=True)
    assert (stats["new"], stats["changed"], stats["removed"]) == (0, 0, 0)
    assert stats["rows"] == N_JOBS and stats["vec0"]


def test_model_change_forces_rebuild(tmp_path):
    db = tmp_path / "jobs.sqlite"
    make_fixtures.populate_sqlite(db, 30)
    recindex.build_index(db, model="hash", quiet=True)
    stats = recindex.build_index(db, model="hash-v1-64", quiet=True)
    assert stats["full"] and stats["new"] == 30
    assert (
        dict(sqlite3.connect(db).execute("SELECT key, value FROM rec_meta"))["dim"]
        == "64"
    )


# -- 1) filtering -----------------------------------------------------------


def test_filter_category_and_comp(con):
    rows = recommend.filter_jobs(con, Filters(categories=["Healthcare"]), limit=50)
    assert rows and all(r["job_category"] == "Healthcare" for r in rows)
    rows = recommend.filter_jobs(con, Filters(min_comp=150_000), limit=50)
    assert rows and all((r["yearly_max_compensation"] or 0) >= 150_000 for r in rows)


def test_filter_excludes_expired_by_default(con):
    assert not any(r["is_expired"] for r in recommend.filter_jobs(con, limit=200))
    rows = recommend.filter_jobs(con, Filters(include_expired=True), limit=200)
    assert any(r["is_expired"] for r in rows)


def test_filter_tools_and_location(con):
    rows = recommend.filter_jobs(
        con, Filters(tools=["PostgreSQL"], location="San Francisco"), limit=50
    )
    assert rows
    for r in rows:
        assert "San Francisco" in r["formatted_workplace_location"]


# -- 2) keyword search ------------------------------------------------------


def test_search_finds_the_right_archetypes(con):
    rows = recommend.search_keywords(con, "kubernetes terraform", k=10)
    assert rows and set(archetypes(rows)) <= {"backend", "devops", "mleng"}
    assert rows[0]["score"] >= rows[-1]["score"]


def test_search_and_falls_back_to_or(con):
    # no single job mentions both nursing and accounting tools
    rows = recommend.search_keywords(con, "epic quickbooks", k=10)
    assert rows and set(archetypes(rows)) <= {"nurse", "accountant"}


def test_search_composes_with_filters(con):
    rows = recommend.search_keywords(
        con, "python", Filters(categories=["Data Science"]), k=20
    )
    assert rows and all(r["job_category"] == "Data Science" for r in rows)


def test_search_garbage_query_is_empty_not_error(con):
    assert recommend.search_keywords(con, '"""', k=5) == []


# -- 3) similar jobs --------------------------------------------------------


def test_similar_prefers_same_archetype(con):
    rid = "SYN-nurse-00005"
    rows = recommend.similar_jobs(con, rid, k=5)
    assert rows and rid not in {r["requisition_id"] for r in rows}
    assert archetypes(rows).count("nurse") >= 4


def test_similar_collapses_duplicates(con):
    rows = recommend.similar_jobs(con, "SYN-backend-00000", k=20)
    keys = [r["collapse_key"] for r in rows]
    assert len(keys) == len(set(keys))


def test_similar_unknown_id_raises(con):
    with pytest.raises(KeyError):
        recommend.similar_jobs(con, "NOPE-1")


# -- 4) resume matching -----------------------------------------------------


@pytest.mark.parametrize("arch", sorted(RESUMES))
def test_resume_matches_own_archetype(con, arch):
    rows, keywords = recommend.match_resume(con, RESUMES[arch], k=5)
    assert keywords
    assert archetypes(rows).count(arch) >= 4
    assert all("signals" in r for r in rows)


def test_resume_respects_filters(con):
    rows, _ = recommend.match_resume(
        con, RESUMES["backend"], Filters(workplace_types=["Remote"]), k=5
    )
    assert rows and all(r["workplace_type"] == "Remote" for r in rows)


# -- incremental re-index ---------------------------------------------------


def test_incremental_change_and_delete(tmp_path):
    raw, out = build_corpus(tmp_path, n=40)
    db = out / "jobs.sqlite"

    # rewrite one file with a distinctive new title
    victim = raw / "SYN-backend-00000.json.gz"
    page = json.loads(gzip.decompress(victim.read_bytes()))
    page["pageProps"]["job"]["job_information"]["title"] = "Zorbulon Wrangler"
    victim.write_bytes(gzip.compress(json.dumps(page).encode()))
    os.utime(victim, (time.time() + 5, time.time() + 5))

    run_msgspec_ingest(raw, out, None, full=False, parquet=False, workers=1)
    stats = recindex.build_index(db, model="hash", quiet=True)
    assert stats["changed"] == 1 and stats["new"] == 0

    con = recommend.get_connection(db)
    rows = recommend.search_keywords(con, "zorbulon", k=5)
    assert [r["requisition_id"] for r in rows] == ["SYN-backend-00000"]
    con.close()

    # deleting a row from jobs drops it from every index
    w = sqlite3.connect(db)
    with w:
        w.execute("DELETE FROM jobs WHERE requisition_id = ?", ("SYN-nurse-00005",))
    w.close()
    stats = recindex.build_index(db, model="hash", quiet=True)
    assert stats["removed"] == 1
    con = recommend.get_connection(db)
    hits = {r["requisition_id"] for r in recommend.search_keywords(con, "nurse", k=50)}
    assert "SYN-nurse-00005" not in hits
    con.close()


# -- numpy fallback ---------------------------------------------------------


def test_numpy_fallback_matches_vec0(tmp_path, monkeypatch):
    db = tmp_path / "jobs.sqlite"
    make_fixtures.populate_sqlite(db, 60)
    recindex.build_index(db, model="hash", quiet=True)
    con = recommend.get_connection(db)
    with_vec0 = [
        r["requisition_id"] for r in recommend.similar_jobs(con, "SYN-mleng-00003", k=5)
    ]
    con.close()

    monkeypatch.setattr(recindex, "load_vec_extension", lambda c: False)
    stats = recindex.build_index(db, model="hash", quiet=True)
    assert not stats["vec0"] and stats["full"]  # config change -> rebuild
    con = recommend.get_connection(db)
    no_vec0 = [
        r["requisition_id"] for r in recommend.similar_jobs(con, "SYN-mleng-00003", k=5)
    ]
    con.close()
    assert with_vec0 == no_vec0


# -- FastAPI app ------------------------------------------------------------


def test_api_endpoints(corpus):
    fastapi = pytest.importorskip("fastapi")  # noqa: F841
    from fastapi.testclient import TestClient

    import api

    api.DB_PATH = corpus[1] / "jobs.sqlite"
    client = TestClient(api.app)

    r = client.get("/jobs", params={"category": "Finance", "k": 5})
    assert r.status_code == 200
    assert all(j["job_category"] == "Finance" for j in r.json())

    r = client.get("/search", params={"q": "postgresql kubernetes"})
    assert r.status_code == 200 and r.json()

    r = client.get("/similar/SYN-datasci-00002", params={"k": 3})
    assert r.status_code == 200
    assert {archetype_of(j["requisition_id"]) for j in r.json()} == {"datasci"}
    assert client.get("/similar/NOPE").status_code == 404

    r = client.post("/resume", json={"markdown": RESUMES["accountant"]})
    assert r.status_code == 200
    body = r.json()
    assert body["keywords"]
    top = [archetype_of(j["requisition_id"]) for j in body["results"][:5]]
    assert top.count("accountant") >= 4
