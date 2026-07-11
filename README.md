# job-ingest — a fast, local job recommendation engine

A personal, local-first pipeline that turns a scraped corpus of ~16k job
listings into a queryable recommendation engine: validate and load the
raw JSON in about a second, index it for keyword + semantic search, and
ask it things — from a CLI, a local web API, or a notebook. No servers
to run, no cloud dependencies: everything lives in two database files on
disk, and every stage is benchmarked.

## Goals & vision

1. **Fast enough to never think about.** The original parse+validate
   took ~53 s; it now takes ~1–3 s (msgspec) or ~0.7 s (Rust sidecar),
   and incremental re-runs are ~0.1–0.3 s. The recommendation layer
   keeps that ethos: every query type answers in single-digit to
   low-double-digit milliseconds at 16k–100k rows.
2. **Local-first, file-based.** SQLite serves the recommendation engine;
   DuckDB and Parquet serve notebook analytics. A web app, the CLI, an
   ingest run, and a notebook can all touch the data at the same time
   (SQLite WAL: n readers + 1 writer) without anyone starting a server.
3. **Validated data, one schema.** Every record passes typed,
   per-field validation (`job_schema.py` msgspec Structs, mirrored by
   the optional Rust engine, with a parity checker so they can't drift).
4. **Recommendations that use both signals.** Hybrid retrieval — BM25
   keyword search (FTS5) fused with semantic embeddings (sqlite-vec) via
   Reciprocal Rank Fusion — because lexical-only misses paraphrases and
   embedding-only misses exact constraints ("RN license", "TS/SCI").
5. **Everything measured.** Benchmarks are part of the repo
   (`ingest_and_benchmark.py`, `benchmark_rec.py`, `eval_rec.py`), so
   design decisions are backed by numbers, not vibes.

```
data/raw/json/*.json.gz                          (the scraped corpus)
        │ ingest_and_benchmark.py    incremental parse+validate+load
        ├─→ data/processed/jobs.sqlite           serving store
        ├─→ data/processed/jobs.duckdb           analytics store
        └─→ data/processed/jobs.parquet          (--parquet)
        │ recindex.py                incremental FTS5 + embedding index
        ▼
recommend.py  ──  filter | search | similar | resume   (RRF hybrid)
        ├─ CLI:       uv run recommend.py ...
        ├─ Web API:   uv run uvicorn api:app        (api.py, FastAPI)
        └─ Notebook:  examples/analytics.ipynb
```

## Getting started

Install [uv](https://docs.astral.sh/uv/) — it's the only prerequisite;
it fetches the right Python and all dependencies automatically:

```bash
uv sync                      # create .venv from pyproject.toml + uv.lock
```

Then, with your corpus in `data/raw/json/*.json.gz`:

```bash
# 1. Ingest (incremental; first run is a full build). ~2.8s for 16k
#    files single-threaded, ~1.3s with --workers 8.
uv run ingest_and_benchmark.py

# 2. Build the search indexes (incremental too). Downloads the default
#    embedding model (potion-base-8M, ~30 MB, no torch) on first use.
uv run recindex.py

# 3. Ask it things.
uv run recommend.py filter --category "Software Engineering" \
    --workplace Remote --min-comp 150000
uv run recommend.py search "staff platform engineer kubernetes"
uv run recommend.py similar <requisition_id>
uv run recommend.py resume my_resume.md -k 20

# Or serve it to a local web app:
uv sync --extra api && uv run uvicorn api:app
# GET /jobs, GET /search?q=, GET /similar/{id}, POST /resume
```

**No corpus handy?** Generate a synthetic one and take the whole
pipeline for a spin:

```bash
uv run make_fixtures.py --out data/raw/json --n 2000
uv run ingest_and_benchmark.py && uv run recindex.py
```

Development:

```bash
uv run pytest                          # end-to-end tests (offline)
uv run benchmark_rec.py --synthetic 16000 --model hash   # rec benchmarks
uv run eval_rec.py --synthetic 2000    # retrieval-quality eval
```

`--model hash` is a deterministic, network-free embedder for tests and
benchmarks; use the default (`potion`) or `--model minilm`
(`uv sync --extra minilm`, pulls torch) for real quality — and
`eval_rec.py` to compare them on your data.

## Repo layout

| Path | What it is |
|---|---|
| `job_schema.py` | The schema (msgspec Structs), 33-column flatten — Python source of truth |
| `ingest_and_benchmark.py` | Incremental ingest → SQLite + DuckDB (+ Parquet), with benchmark |
| `fastingest/` | Optional Rust engine (`--engine rust`), ~4x faster parse |
| `verify_parity.py` | Asserts Rust and Python engines produce identical rows |
| `recindex.py` | Builds FTS5 + embedding indexes inside jobs.sqlite, incrementally |
| `recommend.py` | Query layer: filter / search / similar / resume + CLI |
| `embedders.py` | Embedding backends: model2vec (default), MiniLM, hash |
| `api.py` | FastAPI app over the same query layer |
| `make_fixtures.py` | Synthetic, schema-conformant corpus generator |
| `benchmark_rec.py`, `eval_rec.py` | Rec-engine latency benchmarks and quality eval |
| `tests/` | End-to-end pytest suite (runs fully offline) |
| `PLAN.md` | The SQLite-vs-DuckDB decision document for the rec engine |
| `docs/ingest.md` | Ingest layer deep-dive (engines, benchmarks, gotchas) |
| `examples/analytics.ipynb` | Notebook: DuckDB analytics + rec queries |

## Why these choices (vs alternatives)

**Why uv for environment management** — one tool replaces
pip + venv + pyenv + pip-tools: it pins Python itself
(`requires-python`), locks every dependency cross-platform (`uv.lock`,
committed), and `uv run` re-syncs the environment automatically, so
"clone → `uv run pytest`" just works, identically, on every machine.
Poetry manages packages but not Python versions and resolves far more
slowly; conda is heavyweight for a pure-wheel dependency set; plain
`pip install -r requirements.txt` gives unrepeatable environments and
no lockfile. (`requirements.txt` is still kept, mirroring pyproject, for
non-uv users.)

**Why SQLite serves the rec engine (and DuckDB stays for analytics)** —
the full head-to-head is [PLAN.md](PLAN.md); the short version: a
recommendation engine is an OLTP-shaped workload (many small top-k
queries, incremental updates, concurrent readers), which is SQLite home
turf. FTS5 is mature and updates incrementally, while DuckDB's fts
extension needs a full index rebuild after every ingest; sqlite-vec does
exact k-NN with row-level maintenance, while DuckDB's HNSW persistence
is experimental; and SQLite WAL lets the API, CLI, notebooks, and an
ingest run coexist, while a read-write DuckDB process locks everyone
else out. At ≤100k rows both are sub-millisecond at filtering, so the
tiebreakers are maturity, incrementality, and concurrency — SQLite wins
all three. DuckDB keeps doing what it's genuinely better at: columnar
analytics over `jobs.duckdb`/`jobs.parquet`. If the corpus outgrows
~1M rows, the schema ports directly to Postgres + pgvector.

**Why exact k-NN, not a vector database** — at 16k–100k × 256-dim
vectors, brute-force cosine takes single-digit milliseconds; an ANN
index (or a Qdrant/Chroma/LanceDB sidecar) adds infrastructure,
consistency headaches, and cross-system joins for zero perceptible
speedup. Embeddings also live in a plain BLOB table, so the fallback
(numpy in-process) and the migration path (pgvector) are both trivial.

**Why hybrid BM25 + embeddings** — lexical-only reduces resume matching
to keyword overlap; embedding-only is weak on exact skill/certification
constraints. RRF fusion of both is the standard answer and both legs are
cheap here. The default embedding model is static
(`potion-base-8M` via model2vec: no torch, ~30 MB, embeds the whole
corpus in seconds) with `all-MiniLM-L6-v2` behind a flag —
`eval_rec.py` exists precisely to check what the quality flag buys.

**Why msgspec (and optionally Rust) for ingest** — decode+validate in
one C pass is ~19x faster than json+Pydantic; the serde sidecar is
another ~4x. Full trade-offs, benchmark table, and the gotchas that
actually bit: [docs/ingest.md](docs/ingest.md).

## Performance

Ingest (16,076 real files, from [docs/ingest.md](docs/ingest.md)):
parse+validate 2.8 s msgspec / 0.67 s Rust (was ~53 s); no-change
incremental run ~0.1–0.3 s.

Recommendation layer (16k synthetic rows, `--model hash`, this repo's
`benchmark_rec.py --synthetic 16000`; k=10, p50/p95 ms):

| query type | p50 | p95 |
|---|---:|---:|
| filter (category + comp) | 2.6 | 7.0 |
| keywords (FTS5/bm25) | 9.8 | 17.2 |
| similar (k-NN, sqlite-vec) | 14.4 | 15.4 |
| resume (hybrid, embedding included) | 39.3 | 56.4 |

At the PLAN's 100k-row ceiling (`--synthetic 100000`) everything stays
interactive: filter ~17 ms, keywords ~81 ms, similar ~87 ms, resume
~249 ms p50 — and the synthetic corpus is a worst case for term
selectivity (10 archetypes sharing vocabulary across 100k rows).

Index build at 16k rows: FTS ~0.7 s + embeddings; a no-change
incremental `recindex.py` run is ~0.1 s, and a 50-row delta ~0.2 s.
Reproduce with `uv run benchmark_rec.py --synthetic 16000 --model hash
--duckdb-appendix` (the appendix prints the DuckDB keyword/vector
comparison behind PLAN.md).

## Status & roadmap

Implemented (PLAN.md phases 1–5): incremental index build, the four
query types with RRF fusion, CLI + FastAPI + notebook interfaces,
benchmarks, and the eval harness. Open questions tracked in PLAN.md:
popularity priors from user-activity arrays, and persisting user state
(saved searches/feedback) — which would land in SQLite too.
