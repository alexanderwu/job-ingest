#!/usr/bin/env python3
"""
Minimal FastAPI app over the recommendation query layer (phase 3 of
PLAN.md) — the same four query types recommend.py serves on the CLI,
via HTTP for a local web app.

Run (needs the api extra: `uv sync --extra api`):

    uv run uvicorn api:app --reload
    JOBS_DB=path/to/jobs.sqlite uv run uvicorn api:app  # non-default DB

Endpoints:
    GET  /jobs                    structured filtering
    GET  /search?q=...            keyword search (FTS5/bm25)
    GET  /similar/{requisition_id}
    POST /resume                  {"markdown": "..."} -> hybrid matches

Each request opens its own read-only SQLite connection (WAL: many
readers coexist with the ingest's writer; connection cost is ~zero),
so the app never blocks ingest runs, CLI calls, or notebooks.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent))
import recommend  # noqa: E402
from recommend import Filters  # noqa: E402

DB_PATH = Path(os.environ.get("JOBS_DB", str(recommend.DEFAULT_DB)))

app = FastAPI(title="job-ingest recommendations", version="0.1.0")


def get_conn():
    if not DB_PATH.exists():
        raise HTTPException(503, f"{DB_PATH} not found — run the ingest "
                                 f"and recindex first")
    con = recommend.get_connection(DB_PATH)
    try:
        yield con
    finally:
        con.close()


Conn = Annotated[sqlite3.Connection, Depends(get_conn)]


def filter_params(
    category: Annotated[list[str], Query()] = [],
    seniority: Annotated[list[str], Query()] = [],
    workplace: Annotated[list[str], Query()] = [],
    role_type: Annotated[list[str], Query()] = [],
    country: Annotated[list[str], Query()] = [],
    tool: Annotated[list[str], Query()] = [],
    location: str | None = None,
    company: str | None = None,
    min_comp: float | None = None,
    max_yoe: float | None = None,
    include_expired: bool = False,
) -> Filters:
    return Filters(
        categories=category, seniority=seniority,
        workplace_types=workplace, role_types=role_type,
        countries=country, tools=tool, location=location, company=company,
        min_comp=min_comp, max_yoe=max_yoe,
        include_expired=include_expired,
    )


Flt = Annotated[Filters, Depends(filter_params)]
K = Annotated[int, Query(ge=1, le=200)]


@app.get("/jobs")
def jobs(con: Conn, flt: Flt, k: K = 20) -> list[dict]:
    return recommend.filter_jobs(con, flt, limit=k)


@app.get("/search")
def search(con: Conn, flt: Flt, q: str, k: K = 20) -> list[dict]:
    return recommend.search_keywords(con, q, flt, k=k)


@app.get("/similar/{requisition_id}")
def similar(con: Conn, flt: Flt, requisition_id: str, k: K = 20,
            collapse_dupes: bool = True) -> list[dict]:
    try:
        return recommend.similar_jobs(con, requisition_id, flt, k=k,
                                      collapse_dupes=collapse_dupes)
    except KeyError as e:
        raise HTTPException(404, str(e))


class ResumeBody(BaseModel):
    markdown: str


@app.post("/resume")
def resume(con: Conn, flt: Flt, body: ResumeBody, k: K = 20) -> dict:
    results, keywords = recommend.match_resume(con, body.markdown, flt,
                                               k=k)
    return {"keywords": keywords, "results": results}
