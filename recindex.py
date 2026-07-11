#!/usr/bin/env python3
"""
Build the recommendation-engine search indexes inside jobs.sqlite
(phase 1 of PLAN.md). Run it after every ingest:

    uv run ingest_and_benchmark.py && uv run recindex.py

It adds four things next to the `jobs` table the ingest already writes:

    rec_rows        requisition_id -> stable integer id + content
                    fingerprint. The id is the shared rowid for the FTS
                    and vector tables (jobs.rowid itself is unstable:
                    INSERT OR REPLACE reassigns it on every upsert).
    jobs_fts        FTS5 index over title, core_job_title, description,
                    technical_tools, company_name (porter stemming).
    job_embeddings  one L2-normalized float32 vector per job (BLOB), the
                    plain-table copy that keeps the numpy fallback and a
                    future pgvector migration trivial (PLAN alternatives
                    #4 / #2).
    jobs_vec        sqlite-vec vec0 mirror of job_embeddings for k-NN
                    (skipped, with a warning, if the extension can't
                    load; the query layer then brute-forces via numpy).

Builds are incremental and engine-agnostic: rather than trusting the
ingest manifest, each run fingerprints the indexed fields of every row
(16k rows is milliseconds) and re-indexes only new/changed rows, dropping
rows that vanished from `jobs`. A model change or --full rebuilds from
scratch. rec_meta records the embedding model name + dim so index and
query vectors can never mix models.

Usage:
    uv run recindex.py [--db data/processed/jobs.sqlite]
                       [--model potion|minilm|hash] [--full]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embedders  # noqa: E402

SCHEMA_VERSION = "1"
DEFAULT_DB = Path("data/processed/jobs.sqlite")
EMBED_BATCH = 512
# Bound per-job embedding cost; FTS still indexes the full description.
EMBED_DESC_CHARS = 5000

# Fields read from `jobs` for indexing; the fingerprint covers all of
# them, so any change re-does both the FTS row and the embedding.
_FIELDS = (
    "requisition_id",
    "title",
    "core_job_title",
    "description",
    "technical_tools",
    "company_name",
    "job_category",
    "seniority_level",
    "workplace_type",
    "formatted_workplace_location",
)

# Hot filter columns (PLAN §4). Text columns are indexed NOCASE to match
# the query layer's case-insensitive comparisons.
BTREE_INDEXES = {
    "idx_jobs_category": "job_category COLLATE NOCASE",
    "idx_jobs_seniority": "seniority_level COLLATE NOCASE",
    "idx_jobs_workplace": "workplace_type COLLATE NOCASE",
    "idx_jobs_role_type": "role_type COLLATE NOCASE",
    # no index on is_expired: too low-cardinality to help, and without
    # stats the planner would prefer it over the selective indexes
    "idx_jobs_comp_min": "yearly_min_compensation",
    "idx_jobs_comp_max": "yearly_max_compensation",
    "idx_jobs_yoe": "min_industry_and_role_yoe",
    "idx_jobs_company": "company_name COLLATE NOCASE",
    "idx_jobs_collapse": "collapse_key",
    "idx_jobs_publish": "estimated_publish_date",
}


def load_vec_extension(con: sqlite3.Connection) -> bool:
    """Load sqlite-vec; False if the extension or loading is unavailable."""
    try:
        import sqlite_vec
    except ImportError:
        return False
    try:
        con.enable_load_extension(True)
        sqlite_vec.load(con)
        con.enable_load_extension(False)
        return True
    except (AttributeError, sqlite3.OperationalError):
        return False


def _tools_text(technical_tools: str | None) -> str:
    """JSON '["Python","AWS"]' -> 'Python AWS' for FTS/embedding text."""
    if not technical_tools:
        return ""
    try:
        return " ".join(json.loads(technical_tools))
    except (ValueError, TypeError):
        return ""


def _fingerprint(row: tuple) -> str:
    h = hashlib.blake2b(digest_size=16)
    for v in row[1:]:  # skip requisition_id, the key itself
        h.update(b"\x1f" if v is None else str(v).encode())
        h.update(b"\x1e")
    return h.hexdigest()


def embed_text(row: dict) -> str:
    """title + core fields + description, per PLAN 'Architecture'."""
    parts = [
        row["title"],
        row["core_job_title"],
        row["company_name"],
        row["job_category"],
        row["seniority_level"],
        row["workplace_type"],
        row["formatted_workplace_location"],
        _tools_text(row["technical_tools"]),
        (row["description"] or "")[:EMBED_DESC_CHARS],
    ]
    return ". ".join(p for p in parts if p)


def _get_meta(con: sqlite3.Connection) -> dict:
    try:
        return dict(con.execute("SELECT key, value FROM rec_meta"))
    except sqlite3.OperationalError:
        return {}


def _drop_jobs_vec(con: sqlite3.Connection, db_path: Path) -> sqlite3.Connection:
    """Drop the vec0 table. DROP needs the vec0 module loaded; when
    sqlite-vec is unavailable (e.g. the index moved to a machine without
    it), excise the virtual-table schema entry instead (it owns no data
    pages), reopen, and drop its shadow tables as the ordinary tables
    they are. Returns the (possibly reopened) connection."""
    try:
        con.execute("DROP TABLE IF EXISTS jobs_vec")
        return con
    except sqlite3.OperationalError:
        con.execute("PRAGMA writable_schema=1")
        con.execute("DELETE FROM sqlite_master WHERE name = 'jobs_vec'")
        con.execute("PRAGMA writable_schema=0")
        con.commit()
        con.close()
        con = sqlite3.connect(db_path)
        shadows = [
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name LIKE 'jobs_vec_%'"
            )
        ]
        for name in shadows:
            con.execute(f'DROP TABLE "{name}"')
        con.commit()
        return con


def _drop_index_tables(con: sqlite3.Connection) -> None:
    for t in ("jobs_fts", "job_embeddings", "rec_rows", "rec_meta"):
        con.execute(f"DROP TABLE IF EXISTS {t}")


def _create_index_tables(con: sqlite3.Connection, dim: int, have_vec: bool) -> None:
    con.execute("CREATE TABLE rec_meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("""
        CREATE TABLE rec_rows (
            id INTEGER PRIMARY KEY,
            requisition_id TEXT UNIQUE NOT NULL,
            fingerprint TEXT NOT NULL
        )""")
    con.execute("""
        CREATE VIRTUAL TABLE jobs_fts USING fts5(
            title, core_job_title, description, technical_tools,
            company_name, tokenize='porter unicode61'
        )""")
    con.execute("""
        CREATE TABLE job_embeddings (
            id INTEGER PRIMARY KEY,
            requisition_id TEXT UNIQUE NOT NULL,
            vec BLOB NOT NULL
        )""")
    if have_vec:
        con.execute(
            f"CREATE VIRTUAL TABLE jobs_vec USING vec0("
            f"embedding float[{dim}] distance_metric=cosine)"
        )


def build_index(
    db_path: Path | str,
    model: str = embedders.DEFAULT_MODEL,
    full: bool = False,
    batch_size: int = EMBED_BATCH,
    quiet: bool = False,
) -> dict:
    """Create/refresh all rec tables; returns a stats dict."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise SystemExit(f"{db_path} not found — run the ingest first")

    def log(msg: str) -> None:
        if not quiet:
            print(msg, flush=True)

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    have_vec = load_vec_extension(con)
    if not have_vec:
        log(
            "warning: sqlite-vec unavailable; skipping jobs_vec "
            "(queries will brute-force via numpy)"
        )

    t0 = time.perf_counter()
    embedder = embedders.get_embedder(model)
    load_sec = time.perf_counter() - t0

    meta = _get_meta(con)
    if meta and (
        meta.get("schema_version") != SCHEMA_VERSION
        or meta.get("model") != embedder.name
        or int(meta.get("dim", -1)) != embedder.dim
        or (meta.get("vec0") == "1") != have_vec
    ):
        log(
            f"index config changed (model {meta.get('model')} -> "
            f"{embedder.name}); rebuilding from scratch"
        )
        full = True
    if full or not meta:
        con = _drop_jobs_vec(con, db_path)
        _drop_index_tables(con)
        _create_index_tables(con, embedder.dim, have_vec)
        old_fp: dict[str, tuple[int, str]] = {}
    else:
        old_fp = {
            rid: (rowid, fp)
            for rowid, rid, fp in con.execute(
                "SELECT id, requisition_id, fingerprint FROM rec_rows"
            )
        }

    for name, col in BTREE_INDEXES.items():
        con.execute(f"CREATE INDEX IF NOT EXISTS {name} ON jobs({col})")

    t0 = time.perf_counter()
    jobs = con.execute(f"SELECT {', '.join(_FIELDS)} FROM jobs").fetchall()
    changed: list[dict] = []  # rows to (re)index, with 'id' filled in later
    seen: set[str] = set()
    stale_ids: list[int] = []  # rec ids whose FTS/vec rows must be deleted
    n_new = n_changed = 0
    for row in jobs:
        rid = row[0]
        seen.add(rid)
        fp = _fingerprint(row)
        old = old_fp.get(rid)
        if old is not None and old[1] == fp:
            continue
        rec = dict(zip(_FIELDS, row))
        rec["fingerprint"] = fp
        if old is None:
            n_new += 1
        else:
            n_changed += 1
            stale_ids.append(old[0])
        changed.append(rec)
    removed = [(rowid, rid) for rid, (rowid, _) in old_fp.items() if rid not in seen]
    diff_sec = time.perf_counter() - t0

    t0 = time.perf_counter()
    with con:
        for rowid, rid in removed:
            stale_ids.append(rowid)
            con.execute("DELETE FROM rec_rows WHERE id = ?", (rowid,))
        for rowid in stale_ids:
            con.execute("DELETE FROM jobs_fts WHERE rowid = ?", (rowid,))
            con.execute("DELETE FROM job_embeddings WHERE id = ?", (rowid,))
            if have_vec:
                con.execute("DELETE FROM jobs_vec WHERE rowid = ?", (rowid,))
        for rec in changed:
            _cur = con.execute(
                "INSERT INTO rec_rows (requisition_id, fingerprint) "
                "VALUES (?, ?) ON CONFLICT(requisition_id) "
                "DO UPDATE SET fingerprint = excluded.fingerprint",
                (rec["requisition_id"], rec["fingerprint"]),
            )
            rec["id"] = con.execute(
                "SELECT id FROM rec_rows WHERE requisition_id = ?",
                (rec["requisition_id"],),
            ).fetchone()[0]
        con.executemany(
            "INSERT INTO jobs_fts (rowid, title, core_job_title, "
            "description, technical_tools, company_name) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    r["id"],
                    r["title"],
                    r["core_job_title"],
                    r["description"],
                    _tools_text(r["technical_tools"]),
                    r["company_name"],
                )
                for r in changed
            ],
        )
    fts_sec = time.perf_counter() - t0

    t0 = time.perf_counter()
    with con:
        for start in range(0, len(changed), batch_size):
            batch = changed[start : start + batch_size]
            vecs = embedder.encode([embed_text(r) for r in batch])
            rows = [
                (r["id"], r["requisition_id"], vecs[i].astype("float32").tobytes())
                for i, r in enumerate(batch)
            ]
            con.executemany(
                "INSERT OR REPLACE INTO job_embeddings "
                "(id, requisition_id, vec) VALUES (?, ?, ?)",
                rows,
            )
            if have_vec:
                con.executemany(
                    "INSERT INTO jobs_vec (rowid, embedding) VALUES (?, ?)",
                    [(r[0], r[2]) for r in rows],
                )
        con.executemany(
            "INSERT OR REPLACE INTO rec_meta (key, value) VALUES (?, ?)",
            [
                ("schema_version", SCHEMA_VERSION),
                ("model", embedder.name),
                ("dim", str(embedder.dim)),
                ("vec0", "1" if have_vec else "0"),
                ("built_at", time.strftime("%Y-%m-%dT%H:%M:%S%z")),
            ],
        )
    embed_sec = time.perf_counter() - t0
    total = con.execute("SELECT COUNT(*) FROM rec_rows").fetchone()[0]
    con.execute("ANALYZE")  # keep the query planner off the wrong indexes
    con.close()

    stats = {
        "rows": total,
        "new": n_new,
        "changed": n_changed,
        "removed": len(removed),
        "model": embedder.name,
        "dim": embedder.dim,
        "vec0": have_vec,
        "full": full or not meta,
        "model_load_sec": load_sec,
        "diff_sec": diff_sec,
        "fts_sec": fts_sec,
        "embed_sec": embed_sec,
    }
    log(
        f"recindex: {total} rows indexed ({n_new} new, {n_changed} "
        f"changed, {len(removed)} removed) — model {embedder.name} "
        f"[{embedder.dim}d], vec0={'yes' if have_vec else 'no'}"
    )
    log(
        f"  model load {load_sec:.2f}s, diff {diff_sec:.2f}s, "
        f"fts {fts_sec:.2f}s, embed+vec {embed_sec:.2f}s"
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument(
        "--model",
        default=embedders.DEFAULT_MODEL,
        help="potion (default) | minilm | hash | any model2vec-loadable HF id",
    )
    parser.add_argument(
        "--full", action="store_true", help="Drop and rebuild all rec tables."
    )
    parser.add_argument("--batch-size", type=int, default=EMBED_BATCH)
    args = parser.parse_args()
    build_index(args.db, model=args.model, full=args.full, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
