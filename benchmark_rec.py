#!/usr/bin/env python3
"""
Recommendation-engine benchmarks (phase 4 of PLAN.md): index build time
(full + incremental) and per-query-type latency, plus an optional
SQLite-vs-DuckDB appendix backing the PLAN's engine decision with
numbers in-repo.

Against the real corpus (after ingest + recindex):
    uv run benchmark_rec.py --db data/processed/jobs.sqlite

Against a synthetic corpus (no data/ needed; PLAN asks for a 100k run):
    uv run benchmark_rec.py --synthetic 16000
    uv run benchmark_rec.py --synthetic 100000 --model hash
    (--model hash benchmarks retrieval without embedding-model downloads;
    retrieval latency is embedding-model-agnostic for equal dims)

Add --duckdb-appendix for the keyword/vector comparison. Latencies are
wall-clock per call, reported as p50/p95 over --iters runs.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embedders  # noqa: E402
import make_fixtures  # noqa: E402
import recindex  # noqa: E402
import recommend  # noqa: E402


def _pctl(ms: list[float]) -> str:
    p50 = statistics.median(ms)
    p95 = sorted(ms)[max(0, int(len(ms) * 0.95) - 1)]
    return f"{p50:8.2f} {p95:8.2f}"


def _time(fn, iters: int) -> list[float]:
    fn()  # warmup
    out = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t0) * 1000)
    return out


def _search_queries(con) -> list[str]:
    """2-tool keyword queries from the corpus's most common tools."""
    tools = [t for (t,) in con.execute(
        "SELECT value FROM jobs, json_each(jobs.technical_tools) "
        "GROUP BY value ORDER BY COUNT(*) DESC LIMIT 12")]
    if len(tools) < 2:
        return ["engineer"]
    return [f"{tools[i]} {tools[(i + 3) % len(tools)]}"
            for i in range(len(tools))]


def bench_queries(db: Path, iters: int, k: int) -> None:
    con = recommend.get_connection(db)
    meta = recommend.get_meta(con)
    n = con.execute("SELECT COUNT(*) FROM rec_rows").fetchone()[0]
    cats = [c for (c,) in con.execute(
        "SELECT DISTINCT job_category FROM jobs "
        "WHERE job_category IS NOT NULL")]
    rids = [r for (r,) in con.execute(
        "SELECT requisition_id FROM rec_rows ORDER BY random() LIMIT 64")]
    queries = _search_queries(con)
    resumes = list(make_fixtures.RESUMES.values())
    flt = recommend.Filters(min_comp=50_000)

    state = {"i": 0}

    def rotate(pool):
        state["i"] += 1
        return pool[state["i"] % len(pool)]

    print(f"\nQuery latency over {n} rows (k={k}, {iters} iters, ms)   "
          f"vec0={'yes' if meta.get('vec0') == '1' else 'no (numpy)'}")
    print(f"{'query type':<28}{'p50':>8} {'p95':>8}")
    rows = [
        ("filter (category+comp)",
         lambda: recommend.filter_jobs(
             con, recommend.Filters(categories=[rotate(cats)],
                                    min_comp=100_000), limit=k)),
        ("keywords (fts5/bm25)",
         lambda: recommend.search_keywords(con, rotate(queries), k=k)),
        ("keywords + filters",
         lambda: recommend.search_keywords(con, rotate(queries), flt,
                                           k=k)),
        ("similar (knn)",
         lambda: recommend.similar_jobs(con, rotate(rids), k=k)),
        ("similar + filters",
         lambda: recommend.similar_jobs(con, rotate(rids), flt, k=k)),
        ("resume (hybrid, embed inc.)",
         lambda: recommend.match_resume(con, rotate(resumes), k=k)),
    ]
    for name, fn in rows:
        print(f"{name:<28}{_pctl(_time(fn, iters))}")
    con.close()


def bench_build(db: Path, model: str) -> None:
    print(f"\nIndex build ({model}):")
    s = recindex.build_index(db, model=model, full=True, quiet=True)
    print(f"  full build of {s['rows']} rows: fts {s['fts_sec']:.2f}s, "
          f"embed+vec {s['embed_sec']:.2f}s")
    s = recindex.build_index(db, model=model, quiet=True)
    total = s["diff_sec"] + s["fts_sec"] + s["embed_sec"]
    print(f"  incremental, no changes: {total:.2f}s")
    import sqlite3
    con = sqlite3.connect(db)
    with con:
        con.execute(
            "UPDATE jobs SET description = description || ' (updated)' "
            "WHERE rowid IN (SELECT rowid FROM jobs LIMIT 50)")
    con.close()
    s = recindex.build_index(db, model=model, quiet=True)
    total = s["diff_sec"] + s["fts_sec"] + s["embed_sec"]
    print(f"  incremental, 50 changed rows: {total:.2f}s "
          f"({s['changed']} reindexed)")


def bench_duckdb_appendix(db: Path, iters: int, k: int) -> None:
    """DuckDB on the same rows: brute-force cosine + fts extension."""
    import duckdb
    import sqlite3

    print("\nDuckDB appendix (same rows, same embeddings):")
    scon = sqlite3.connect(db)
    dim = int(dict(scon.execute("SELECT key, value FROM rec_meta"))["dim"])
    rows = scon.execute(
        "SELECT e.requisition_id, j.title, j.description, e.vec "
        "FROM job_embeddings e "
        "JOIN jobs j ON j.requisition_id = e.requisition_id").fetchall()
    scon.close()

    import pyarrow as pa

    mat = np.frombuffer(b"".join(r[3] for r in rows), dtype=np.float32)
    arrow = pa.table({
        "requisition_id": [r[0] for r in rows],
        "title": [r[1] for r in rows],
        "description": [r[2] for r in rows],
        "vec": pa.FixedSizeListArray.from_arrays(pa.array(mat), dim),
    })
    con = duckdb.connect()
    con.register("arrow_jobs", arrow)
    con.execute(f"CREATE TABLE jobs AS SELECT requisition_id, title, "
                f"description, vec::FLOAT[{dim}] AS vec FROM arrow_jobs")

    q = np.frombuffer(rows[0][3], dtype=np.float32).tolist()
    ms = _time(lambda: con.execute(
        f"SELECT requisition_id FROM jobs "
        f"ORDER BY array_cosine_distance(vec, ?::FLOAT[{dim}]) LIMIT ?",
        (q, k)).fetchall(), iters)
    print(f"{'vector: native brute force':<28}{_pctl(ms)}   (p50/p95 ms)")

    try:
        con.execute("INSTALL fts; LOAD fts")
    except duckdb.Error as e:
        print(f"fts extension unavailable ({str(e).splitlines()[0]}); "
              f"skipping keyword comparison")
        return
    t0 = time.perf_counter()
    con.execute("PRAGMA create_fts_index('jobs', 'requisition_id', "
                "'title', 'description')")
    print(f"fts index build: {time.perf_counter() - t0:.2f}s "
          f"(NB: full rebuild required after every ingest)")
    ms = _time(lambda: con.execute(
        "SELECT requisition_id, fts_main_jobs.match_bm25(requisition_id, "
        "'kubernetes terraform') AS s FROM jobs WHERE s IS NOT NULL "
        "ORDER BY s DESC LIMIT ?", (k,)).fetchall(), iters)
    print(f"{'keywords: fts match_bm25':<28}{_pctl(ms)}   (p50/p95 ms)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path,
                        help="existing (ingested + indexed) jobs.sqlite")
    parser.add_argument("--synthetic", type=int, metavar="N",
                        help="benchmark against N generated jobs instead")
    parser.add_argument("--model", default=embedders.DEFAULT_MODEL)
    parser.add_argument("--iters", type=int, default=50)
    parser.add_argument("-k", type=int, default=10)
    parser.add_argument("--duckdb-appendix", action="store_true")
    parser.add_argument("--skip-build", action="store_true",
                        help="only run the query benchmarks")
    args = parser.parse_args()
    if bool(args.db) == bool(args.synthetic):
        parser.error("pass exactly one of --db or --synthetic N")

    if args.synthetic:
        tmp = Path(tempfile.mkdtemp(prefix="bench_rec_"))
        db = tmp / "jobs.sqlite"
        print(f"generating {args.synthetic} synthetic jobs -> {db}")
        t0 = time.perf_counter()
        make_fixtures.populate_sqlite(db, args.synthetic)
        print(f"  {time.perf_counter() - t0:.2f}s")
    else:
        db = args.db

    if args.skip_build:
        recindex.build_index(db, model=args.model, quiet=True)
    else:
        bench_build(db, args.model)
    bench_queries(db, args.iters, args.k)
    if args.duckdb_appendix:
        bench_duckdb_appendix(db, args.iters, args.k)


if __name__ == "__main__":
    main()
