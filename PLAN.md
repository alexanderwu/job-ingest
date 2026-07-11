# PLAN — Job recommendation engine: SQLite vs DuckDB

## Decision (TL;DR)

**Serve recommendations from SQLite** (FTS5 for keywords, sqlite-vec for
embeddings, plain SQL for filters), **keep DuckDB in the pipeline** for what
it's already best at: bulk Arrow ingest, Parquet export, and notebook
analytics. Ingest keeps writing both databases (the benchmark stays intact);
the rec engine builds its search indexes on SQLite only.

The one-line reason: a recommendation engine is an **OLTP-shaped workload**
— many small top-k queries with per-row filters, served concurrently to a
web app and a CLI, over a corpus that updates incrementally — and that is
exactly SQLite's home turf. DuckDB's advantages (columnar scans, vectorized
aggregation) matter at analytical scale, but at 16k–100k rows every query
in this engine finishes in single-digit milliseconds on either engine, so
the deciding factors become **extension maturity, incremental-index
maintenance, and multi-process concurrency** — and SQLite wins all three
today.

## Requirements recap

From the discussion:

- **Four query types**: structured filtering, keyword search, "jobs similar
  to job X", and resume (markdown) → job matching.
- **Matching tech**: hybrid — BM25 lexical + local embedding model.
- **Consumers**: local web app (FastAPI-style, concurrent reads), CLI, and
  notebooks. Performance/latency is a priority.
- **Scale**: ~16k today, up to ~100k. Not designing for millions.
- **Repo shape**: pick one DB for the rec engine; dual-DB ingest stays.

What each query type needs from the database:

| Query type      | Needs                                                        |
|-----------------|--------------------------------------------------------------|
| Filtering       | Indexed WHERE on the 33 flat columns (category, seniority, workplace_type, comp range, YoE, location, expiry…) |
| Keywords        | Full-text index with BM25 ranking over title/description/technical_tools |
| Similar jobs    | Embedding per job + k-NN search, optionally under filters    |
| Resume matching | Embed resume text at query time → same k-NN path + keyword extraction → hybrid fusion |

## Head-to-head: SQLite vs DuckDB for this workload

### 1. Full-text search — SQLite wins clearly

- **SQLite FTS5** is one of the most battle-tested FTS engines anywhere
  (it's in every browser and phone). BM25 ranking built in, prefix queries,
  phrase queries, boolean operators, custom tokenizers, and — critically —
  **incremental index maintenance**: an external-content FTS5 table stays
  in sync via ordinary INSERT/DELETE statements (or triggers) as rows are
  upserted. That composes perfectly with this repo's manifest-based
  incremental ingest: an incremental run touching 50 files updates 50 FTS
  rows.
- **DuckDB `fts` extension** is a port of a research prototype (Sqlserver-
  style OKAPI BM25 over a macro-generated schema). It has one structural
  problem for us: **the index does not follow data changes — you must
  `PRAGMA drop_fts_index` + `create_fts_index` (a full rebuild) after every
  ingest**. At 16k rows a rebuild is seconds, so it's survivable, but it
  turns every incremental ingest into a full-index job and grows linearly
  with the corpus. It also has a much thinner query language (no prefix or
  phrase queries) and far less production mileage.

### 2. Vector search — SQLite (sqlite-vec) is the safer bet

At 16k–100k vectors, **brute-force exact k-NN is the right algorithm** —
384-dim float32 embeddings for 100k jobs are ~150 MB, and a full scan with
SIMD is a few milliseconds. Nobody needs an ANN index at this scale, which
neutralizes DuckDB's theoretical HNSW advantage entirely.

- **sqlite-vec** (successor to sqlite-vss): a single-file, zero-dependency
  loadable extension. `vec0` virtual tables store vectors, `MATCH` does
  exact k-NN, recent versions support metadata columns and partition keys
  for filtered k-NN. Rows are inserted/deleted individually → **works with
  incremental ingest**. It is pre-1.0, but its design is deliberately
  boring (brute force, no index state to corrupt), and the fallback is
  trivial (see §4 alternatives: numpy).
- **DuckDB `vss` extension** is **explicitly experimental**: HNSW index
  persistence sits behind
  `SET hnsw_enable_experimental_persistence = true` with documented
  caveats about WAL recovery and potential data loss on unclean shutdown;
  deletes don't compact the index (recall degrades until a manual
  `PRAGMA hnsw_compact_index`). Without VSS, DuckDB can still brute-force
  with native `array_cosine_similarity()` over a `FLOAT[384]` column —
  that path is actually excellent and is DuckDB's best card — but it's not
  better *enough* to overcome the FTS and concurrency gaps.

### 3. Concurrency & serving — SQLite wins for this exact consumer mix

You want a **web app, a CLI, and a notebook potentially open at the same
time**, all against the same store:

- **SQLite in WAL mode**: any number of reader processes plus one writer
  (the ingest), no coordination needed. The web app, several CLI
  invocations, and a notebook can all hold connections while an ingest run
  upserts. Connection cost is ~zero, so per-request latency in a web app
  is microseconds of overhead.
- **DuckDB**: one process may hold the database read-write; other
  processes can attach only in `read_only` mode, and *no* readers can
  attach while a writer holds it. Concretely: while your API server holds
  `jobs.duckdb` read-write (needed if the rec engine writes feedback/state
  or refreshes indexes), an ingest run or a notebook **cannot open the
  file at all**. Workable with discipline (everyone opens read-only,
  ingest gets an exclusive window), but it's a footgun SQLite simply
  doesn't have.

### 4. Filtering & point lookups — tie at this scale

DuckDB would crush SQLite on analytical scans over billions of rows, but a
filtered top-k over ≤100k rows is sub-millisecond in SQLite with a few
B-tree indexes on the hot columns (`job_category`, `seniority_level`,
`workplace_type`, `yearly_min_compensation`, `is_expired`). SQLite is also
faster at single-row lookups (fetch job X to display / to get its
embedding). No advantage either way that a user could perceive.

### 5. Operational fit with the existing pipeline

- Ingest **already writes SQLite** from both engines (rusqlite and stdlib
  `sqlite3`) with identical DDL — adding "sync the FTS table and embedding
  table for the delta rows" is a natural post-step keyed on the same
  `requisition_id` upsert set the manifest already produces.
- The stdlib `sqlite3` build here has FTS5 **and** `load_extension`
  enabled (verified: SQLite 3.45.1, `ENABLE_FTS5`,
  `ENABLE_LOAD_EXTENSION`), so no custom Python build is needed;
  `sqlite-vec` ships as a pip wheel.
- DuckDB stays exactly as valuable as it is today for the **notebook
  consumer**: ad-hoc analytics over `jobs.duckdb` or `jobs.parquet`
  (opened read-only) is where DuckDB genuinely beats SQLite, and we keep
  that.

### Decision matrix

| Criterion (weight for this project)        | SQLite                              | DuckDB                                   |
|--------------------------------------------|-------------------------------------|------------------------------------------|
| Keyword search / BM25 (high)               | **FTS5: mature, incremental**       | fts ext: full rebuild per ingest, thin query language |
| Vector k-NN at 16k–100k (high)             | **sqlite-vec: exact, incremental**  | native brute force good; VSS experimental persistence |
| Multi-process serving: API+CLI+notebook (high) | **WAL: n readers + 1 writer**    | single RW process; readers blocked by writer |
| Incremental ingest compatibility (high)    | **Row-level index maintenance**     | FTS/HNSW indexes need rebuilds           |
| Filtered top-k latency at this scale (med) | Sub-ms with B-tree indexes (tie)    | Sub-ms via scan (tie)                    |
| Analytical/notebook queries (med)          | Adequate                            | **Excellent — and we keep it for this**  |
| New dependencies (low)                     | sqlite-vec wheel                    | fts + vss extensions (autoload)          |

## Alternatives considered (and why not)

1. **DuckDB as the rec-engine store** — the full case is above; summarized:
   its two decisive weaknesses are index maintenance (FTS full rebuilds,
   experimental HNSW persistence) and the single-writer/no-concurrent-
   reader file model, both of which directly collide with "incremental
   ingest + concurrent web app/CLI/notebook". Its strengths (scan speed,
   Arrow/Parquet integration) don't differentiate at ≤100k rows and are
   retained anyway via dual ingest.
2. **Postgres + pgvector** — the "grown-up" answer, and the right one at
   ~1M+ rows or multi-writer. Rejected: a server process to install,
   configure, and keep running, for a local single-user tool. Everything
   pgvector buys (filtered ANN, mature ops) is overkill at this scale.
   This is the named **migration path** if the corpus outgrows the plan.
3. **Dedicated vector store (Qdrant, Chroma, LanceDB)** — splits the data
   across two systems, so every filtered vector query becomes a cross-
   system join you write by hand, and ingest must dual-write with its own
   consistency handling. Justifiable at millions of vectors; pure overhead
   at 16k.
4. **No vector DB at all: embeddings as a BLOB column + numpy in-process**
   — load all embeddings into one `float32[N,384]` matrix at app start
   (~25 MB today), cosine top-k via one matmul (<1 ms). Honestly *faster*
   than any DB extension at this scale and nearly dependency-free. Not
   chosen as the primary because filtered k-NN then means re-implementing
   predicate logic in Python and keeping the matrix in sync per process —
   but it is the **designated fallback** if sqlite-vec misbehaves, and the
   embedding BLOBs will live in a plain SQLite table precisely so this
   swap is a ~50-line change.
5. **Elasticsearch/OpenSearch or Meilisearch/Tantivy for lexical** — best-
   in-class text relevance, but a running server (or a new Rust dependency)
   to replace FTS5, which is already excellent at this corpus size.
6. **Lexical-only or embeddings-only ranking** — rejected per discussion:
   lexical-only makes resume matching keyword-overlap quality; embeddings-
   only is weak at exact skill/certification constraints ("RN license",
   "TS/SCI clearance"). Hybrid BM25 + cosine with rank fusion is the
   standard answer and both signals are cheap here.

## Architecture

```
ingest (unchanged: msgspec/rust → SQLite + DuckDB + parquet)
        │  delta rows (requisition_ids upserted this run)
        ▼
recindex build  (new post-ingest step, incremental)
        ├─→ jobs_fts      FTS5 external-content table over
        │                 title, core_job_title, description,
        │                 technical_tools, company_name
        ├─→ job_embeddings(requisition_id, vec BLOB float32[384])
        │                 model: embed(title + core fields + description)
        └─→ vec0 virtual table (sqlite-vec) mirroring job_embeddings
        ▼
recommend  (query layer, one module used by CLI + API + notebooks)
        ├─ filters:   SQL WHERE on flat columns (B-tree indexes)
        ├─ keywords:  FTS5 MATCH, bm25() ranking
        ├─ similar:   look up job's vector → k-NN via sqlite-vec
        ├─ resume:    markdown → text → embed → k-NN
        │             + extract skills/titles → FTS query
        └─ fusion:    Reciprocal Rank Fusion (RRF) of BM25 + cosine lists,
                      filters applied as pre-filter to both legs
```

**Embedding model**: default to a static-embedding model
(`model2vec`, e.g. `potion-base-8M`, 256-dim) — no torch, ~30 MB, embeds
the full 16k corpus in seconds, which matches this repo's speed ethos and
keeps `uv run` startup light. Offer `sentence-transformers/all-MiniLM-L6-v2`
(384-dim) behind a flag for higher quality; the embedding table records
model name + dim so the two never mix. Re-embedding only happens for delta
rows, mirroring the ingest manifest logic.

**Resume handling**: markdown → strip to plain text (headings/bullets kept
as sentence boundaries) → one embedding for the whole resume plus optional
per-section embeddings (skills section weighted up) → k-NN; in parallel, a
light keyword pass (titles held, tools listed) feeds an FTS query; RRF
merges the two ranked lists.

## Implementation phases

1. **Index build step** (`recindex.py` or a `--recindex` flag on ingest):
   create FTS5 table + embedding tables; full build first run, delta
   builds after, driven by the same changed-`requisition_id` set the
   manifest produces. Store model name/dim/version in a metadata table.
2. **Query layer** (`recommend.py`): the four query types + RRF fusion,
   returning scored rows in `COLUMNS` order. Pure functions over a
   read-only connection — usable from CLI, API, and notebooks unchanged.
3. **Interfaces**: `recommend` CLI subcommands (`filter`, `search`,
   `similar <requisition_id>`, `resume <file.md>`); a minimal FastAPI app
   with the same four endpoints; an example notebook that also shows the
   DuckDB/parquet analytics side.
4. **Benchmarks** (repo tradition): index build time (full + incremental),
   and p50/p95 latency for each query type at 16k, plus a synthetic 100k
   run — including a SQLite-vs-DuckDB appendix for keyword and vector
   queries so the decision above is backed by numbers in-repo.
5. **Eval harness** (small): a handful of hand-labeled resume→job and
   job→job pairs to sanity-check that hybrid beats each signal alone and
   to compare model2vec vs MiniLM before locking the default.

## Risks & mitigations

- **sqlite-vec is pre-1.0** → embeddings also live in a plain table
  (BLOBs); fallback to in-process numpy top-k is a small, isolated change
  (alternative #4).
- **Static embeddings (model2vec) rank worse than transformer embeddings**
  → model choice is a flag; the eval harness in phase 5 decides the
  default with data. Lexical leg (FTS5) carries exact-term quality either
  way.
- **Description text is large** (FTS index size ≈ corpus text size, tens
  of MB) → fine at this scale; use FTS5 `detail=full` now, `detail=none` +
  column weighting if size ever matters.
- **Corpus grows past ~100k–1M** → keep brute-force until latency says
  otherwise; the schema (embeddings keyed by `requisition_id`) ports
  directly to pgvector, per alternative #2.
- **Windows is a target platform** (per README) → sqlite-vec ships Windows
  wheels; stdlib `sqlite3` there also has FTS5. Verify `load_extension` is
  enabled in the user's Python build on Windows early in phase 1; if not,
  the numpy fallback covers vectors and FTS5 needs nothing.

## Open questions (fine to defer)

- Should "similar jobs" exclude same-company duplicates / collapse by
  `collapse_key`? (Probably yes — dedupe by `collapse_key` in ranking.)
- Should user-activity arrays (`viewedByUsers`, `savedFromUsers`) feed a
  popularity prior into ranking? Data exists in the `job_information_json`
  blob; cheap to add as a small boost term later.
- Does the web app need write state (saved searches, feedback)? If yes,
  that's another point for SQLite — it can live in the same file or a
  sidecar DB without any new infrastructure.
