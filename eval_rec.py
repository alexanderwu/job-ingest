#!/usr/bin/env python3
"""
Small retrieval-quality eval harness (phase 5 of PLAN.md): checks that
hybrid retrieval beats each signal alone, and compares embedding models,
before the default is locked in.

Two data sources:

  --synthetic N (self-contained, default 2000)
      Generates a labeled corpus with make_fixtures: the requisition_id
      encodes each job's archetype, so for the bundled backend resume
      the relevant set is exactly the backend jobs — free ground truth.

  --db path --labels labels.json (real corpus, hand labels)
      labels.json is a JSON array of cases:
        {"type": "resume",  "markdown_file": "me.md",  "relevant": [rids]}
        {"type": "similar", "requisition_id": "R1",    "relevant": [rids]}

For every case it runs three rankers — keyword-only (FTS/bm25),
vector-only (embedding k-NN), and hybrid (RRF of both) — and reports
Precision@10 and MRR per ranker.

    uv run eval_rec.py --synthetic 2000                 # potion (default)
    uv run eval_rec.py --synthetic 2000 --model minilm  # compare models
    uv run eval_rec.py --db data/processed/jobs.sqlite --labels my.json
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embedders  # noqa: E402
import make_fixtures  # noqa: E402
import recindex  # noqa: E402
import recommend  # noqa: E402

K = 10


# All rankers see the same candidate pool as the labels (expired included).
_FLT = recommend.Filters(include_expired=True)


def _rank_resume(con, markdown: str) -> dict[str, list[str]]:
    """-> ranker name -> ranked requisition_ids (depth K)."""
    text = recommend.strip_markdown(markdown)
    keywords = recommend.extract_keywords(con, text)
    fts = [rid for rid, _ in recommend._fts_search(
        con, recommend._fts_query(keywords, "OR"), _FLT, K)]
    vec = recommend._query_embedder(con).encode([text])[0]
    knn = [rid for rid, _ in recommend._knn(con, vec, None, K)]
    hybrid = [r["requisition_id"] for r in
              recommend.match_resume(con, markdown, _FLT, k=K)[0]]
    return {"keyword": fts, "vector": knn, "hybrid": hybrid}


def _rank_similar(con, rid: str) -> dict[str, list[str]]:
    row = con.execute(
        "SELECT j.title, j.technical_tools, e.vec FROM jobs j "
        "JOIN job_embeddings e ON e.requisition_id = j.requisition_id "
        "WHERE j.requisition_id = ?", (rid,)).fetchone()
    if row is None:
        raise SystemExit(f"labels reference unindexed job {rid!r}")
    tools = " ".join(json.loads(row["technical_tools"] or "[]"))
    toks = recommend._token_re.findall(f"{row['title']} {tools}")
    fts = [r for r, _ in recommend._fts_search(
        con, recommend._fts_query(toks, "OR"), _FLT, K + 1) if r != rid][:K]
    vec = np.frombuffer(row["vec"], dtype=np.float32)
    knn = [r for r, _ in recommend._knn(con, vec, None, K + 1)
           if r != rid][:K]
    fused = recommend.rrf([knn, fts])
    hybrid = sorted(fused, key=fused.__getitem__, reverse=True)[:K]
    return {"keyword": fts, "vector": knn, "hybrid": hybrid}


def _score(ranked: list[str], relevant: set[str]) -> tuple[float, float]:
    """-> (Precision@K, MRR)."""
    hits = [rid in relevant for rid in ranked[:K]]
    p_at_k = sum(hits) / K
    mrr = next((1 / (i + 1) for i, h in enumerate(hits) if h), 0.0)
    return p_at_k, mrr


def run_cases(con, cases: list[dict]) -> None:
    sums: dict[str, list[float]] = {}
    for case in cases:
        relevant = set(case["relevant"])
        if case["type"] == "resume":
            rankings = _rank_resume(con, case["markdown"])
        else:
            rankings = _rank_similar(con, case["requisition_id"])
        for ranker, ranked in rankings.items():
            p, mrr = _score(ranked, relevant)
            acc = sums.setdefault(ranker, [0.0, 0.0])
            acc[0] += p
            acc[1] += mrr

    n = len(cases)
    print(f"\n{n} cases   {'ranker':<10}{'P@10':>8}{'MRR':>8}")
    for ranker in ("keyword", "vector", "hybrid"):
        p, mrr = sums[ranker]
        print(f"{'':<11}{ranker:<10}{p / n:>8.3f}{mrr / n:>8.3f}")
    if sums["hybrid"][0] < max(sums["keyword"][0], sums["vector"][0]):
        print("note: hybrid did NOT beat the best single signal here")


def synthetic_cases(con) -> list[dict]:
    """Resume + similar cases with archetype-derived ground truth."""
    by_arch: dict[str, list[str]] = {}
    for (rid,) in con.execute("SELECT requisition_id FROM rec_rows"):
        by_arch.setdefault(make_fixtures.archetype_of(rid), []).append(rid)
    cases: list[dict] = []
    for arch, md in make_fixtures.RESUMES.items():
        cases.append({"type": "resume", "markdown": md,
                      "relevant": by_arch[arch]})
    for arch, rids in sorted(by_arch.items()):
        cases.append({"type": "similar", "requisition_id": rids[0],
                      "relevant": [r for r in rids if r != rids[0]]})
    return cases


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthetic", type=int, metavar="N")
    parser.add_argument("--db", type=Path)
    parser.add_argument("--labels", type=Path)
    parser.add_argument("--model", default=embedders.DEFAULT_MODEL)
    args = parser.parse_args()

    if args.synthetic:
        db = Path(tempfile.mkdtemp(prefix="eval_rec_")) / "jobs.sqlite"
        make_fixtures.populate_sqlite(db, args.synthetic)
        recindex.build_index(db, model=args.model, quiet=True)
        con = recommend.get_connection(db)
        cases = synthetic_cases(con)
    elif args.db and args.labels:
        recindex.build_index(args.db, model=args.model, quiet=True)
        con = recommend.get_connection(args.db)
        cases = json.loads(args.labels.read_text())
        for c in cases:
            if c["type"] == "resume" and "markdown" not in c:
                base = args.labels.parent
                c["markdown"] = (base / c["markdown_file"]).read_text()
    else:
        parser.error("pass --synthetic N, or --db with --labels")

    print(f"model: {recommend.get_meta(con)['model']}")
    run_cases(con, cases)


if __name__ == "__main__":
    main()
