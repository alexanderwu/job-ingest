#!/usr/bin/env python3
"""
Query layer for the job recommendation engine (phases 2–3 of PLAN.md).

Four query types over the indexes recindex.py builds in jobs.sqlite, all
pure functions over a read-only connection so the CLI, the FastAPI app
(api.py), and notebooks share one code path:

    filter_jobs      SQL WHERE over the flat columns (B-tree indexes)
    search_keywords  FTS5 MATCH with weighted bm25() ranking
    similar_jobs     stored vector -> k-NN (sqlite-vec, numpy fallback)
    match_resume     markdown resume -> embedding k-NN + extracted-keyword
                     FTS, fused with Reciprocal Rank Fusion

Filters compose with every query type as a pre-filter on both legs.
Vector search uses the sqlite-vec vec0 table when the extension loads,
else brute-force cosine over the job_embeddings BLOBs via numpy (PLAN
alternative #4) — same results either way at this corpus size.

CLI:
    uv run recommend.py filter --category Engineering --min-comp 150000
    uv run recommend.py search "staff platform engineer kubernetes"
    uv run recommend.py similar REQ-123
    uv run recommend.py resume my_resume.md --workplace Remote
    (add --json for machine-readable output)
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embedders  # noqa: E402
from recindex import DEFAULT_DB, load_vec_extension  # noqa: E402

# bm25 weights for jobs_fts columns (title, core_job_title, description,
# technical_tools, company_name): titles dominate, description is bulk.
BM25_WEIGHTS = (4.0, 3.0, 1.0, 2.0, 2.0)
RRF_K = 60  # standard Reciprocal Rank Fusion constant

# Columns returned for every result row.
RESULT_COLUMNS = (
    "requisition_id",
    "title",
    "core_job_title",
    "company_name",
    "job_category",
    "seniority_level",
    "role_type",
    "workplace_type",
    "formatted_workplace_location",
    "min_industry_and_role_yoe",
    "yearly_min_compensation",
    "yearly_max_compensation",
    "listed_compensation_currency",
    "is_expired",
    "collapse_key",
    "apply_url",
    "estimated_publish_date",
)


# --------------------------------------------------------------------------
# Connection / metadata
# --------------------------------------------------------------------------


def get_connection(db_path: Path | str = DEFAULT_DB) -> sqlite3.Connection:
    """Read-only connection with sqlite-vec loaded when available."""
    con = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    load_vec_extension(con)
    return con


def get_meta(con: sqlite3.Connection) -> dict:
    try:
        return dict(con.execute("SELECT key, value FROM rec_meta"))
    except sqlite3.OperationalError:
        raise SystemExit("rec tables missing — run `uv run recindex.py` first")


_embedder_cache: dict[str, object] = {}


def _query_embedder(con: sqlite3.Connection):
    """The embedder the index was built with (cached per model name)."""
    name = get_meta(con)["model"]
    if name not in _embedder_cache:
        _embedder_cache[name] = embedders.get_embedder(name)
    return _embedder_cache[name]


# --------------------------------------------------------------------------
# Filters
# --------------------------------------------------------------------------


@dataclass
class Filters:
    """Structured filters; composes with every query type. List fields
    are OR-ed within the field, AND-ed across fields."""

    categories: list[str] = field(default_factory=list)
    seniority: list[str] = field(default_factory=list)
    workplace_types: list[str] = field(default_factory=list)
    role_types: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)  # workplace_countries
    tools: list[str] = field(default_factory=list)  # technical_tools
    location: str | None = None  # substring of formatted_workplace_location
    company: str | None = None  # substring of company_name
    min_comp: float | None = None  # yearly; job's best-known comp >= this
    max_yoe: float | None = None  # keep jobs asking <= this (or unstated)
    include_expired: bool = False

    def where(self, alias: str = "j") -> tuple[str, list]:
        """-> ('<cond> AND <cond> ...', params); '1' when unconstrained."""
        conds: list[str] = []
        params: list = []

        def in_clause(col: str, values: list[str]) -> None:
            if values:
                qs = ", ".join("?" * len(values))
                # NOCASE comparison served by the NOCASE B-tree indexes
                # recindex creates (lower() would defeat them)
                conds.append(f"{alias}.{col} COLLATE NOCASE IN ({qs})")
                params.extend(values)

        in_clause("job_category", self.categories)
        in_clause("seniority_level", self.seniority)
        in_clause("workplace_type", self.workplace_types)
        in_clause("role_type", self.role_types)
        if self.countries:
            ors, ps = [], []
            for c in self.countries:
                ors.append(
                    f"EXISTS (SELECT 1 FROM json_each("
                    f"{alias}.workplace_countries) WHERE lower(value) = ?)"
                )
                ps.append(c.lower())
            conds.append("(" + " OR ".join(ors) + ")")
            params.extend(ps)
        if self.tools:
            for t in self.tools:  # AND across tools: must have all
                conds.append(
                    f"EXISTS (SELECT 1 FROM json_each("
                    f"{alias}.technical_tools) WHERE lower(value) = ?)"
                )
                params.append(t.lower())
        if self.location:
            conds.append(f"{alias}.formatted_workplace_location LIKE ? COLLATE NOCASE")
            params.append(f"%{self.location}%")
        if self.company:
            conds.append(f"{alias}.company_name LIKE ? COLLATE NOCASE")
            params.append(f"%{self.company}%")
        if self.min_comp is not None:
            conds.append(
                f"COALESCE({alias}.yearly_max_compensation, "
                f"{alias}.yearly_min_compensation) >= ?"
            )
            params.append(self.min_comp)
        if self.max_yoe is not None:
            conds.append(f"COALESCE({alias}.min_industry_and_role_yoe, 0) <= ?")
            params.append(self.max_yoe)
        if not self.include_expired:
            conds.append(f"{alias}.is_expired = 0")
        return (" AND ".join(conds) or "1", params)


def _db_token(con: sqlite3.Connection) -> tuple:
    """Cheap cache-invalidation token: changes whenever another
    connection (the ingest / recindex) commits to this database."""
    return (
        con.execute("PRAGMA database_list").fetchone()[2],
        con.execute("PRAGMA data_version").fetchone()[0],
        *con.execute("SELECT COUNT(*), COALESCE(MAX(id), 0) FROM rec_rows").fetchone(),
    )


_candidate_cache: dict[tuple, list[int] | None] = {}


def _candidate_ids(con: sqlite3.Connection, flt: Filters | None) -> list[int] | None:
    """rec_rows.id set passing the filters (the vector leg's pre-filter);
    None = unconstrained. Cached per filter until the DB changes."""
    if flt is None:
        flt = Filters()
    cond, params = flt.where("j")
    if cond == "1":
        return None
    key = (*_db_token(con), cond, tuple(params))
    if key not in _candidate_cache:
        if len(_candidate_cache) > 64:
            _candidate_cache.clear()
        _candidate_cache[key] = [
            r[0]
            for r in con.execute(
                f"SELECT r.id FROM rec_rows r "
                f"JOIN jobs j ON j.requisition_id = r.requisition_id "
                f"WHERE {cond}",
                params,
            )
        ]
    return _candidate_cache[key]


# --------------------------------------------------------------------------
# Hydration
# --------------------------------------------------------------------------


def _hydrate(con: sqlite3.Connection, rids: list[str]) -> list[dict]:
    """requisition_ids -> job dicts, preserving input order."""
    if not rids:
        return []
    qs = ", ".join("?" * len(rids))
    rows = {
        r["requisition_id"]: dict(r)
        for r in con.execute(
            f"SELECT {', '.join(RESULT_COLUMNS)} FROM jobs "
            f"WHERE requisition_id IN ({qs})",
            rids,
        )
    }
    return [rows[rid] for rid in rids if rid in rows]


# --------------------------------------------------------------------------
# 1) Structured filtering
# --------------------------------------------------------------------------


def filter_jobs(
    con: sqlite3.Connection, flt: Filters | None = None, limit: int = 20
) -> list[dict]:
    """Newest-first listing of jobs passing the filters."""
    flt = flt or Filters()
    cond, params = flt.where("j")
    rows = con.execute(
        f"SELECT {', '.join(RESULT_COLUMNS)} FROM jobs j WHERE {cond} "
        f"ORDER BY j.estimated_publish_date IS NULL, "
        f"j.estimated_publish_date DESC LIMIT ?",
        params + [limit],
    )
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------
# 2) Keyword search (FTS5 / bm25)
# --------------------------------------------------------------------------

_token_re = re.compile(r"[\w+#.\-]+", re.UNICODE)


def _fts_query(tokens: list[str], operator: str = "AND") -> str:
    quoted = ['"' + t.replace('"', "") + '"' for t in tokens if t]
    return f" {operator} ".join(quoted)


def _fts_search(
    con: sqlite3.Connection, match: str, flt: Filters | None, k: int
) -> list[tuple[str, float]]:
    """-> [(requisition_id, bm25)] best-first; [] on empty/invalid query.

    Filters are applied after the join rather than as an id pre-filter:
    every match is ranked, so results are identical, but FTS5 never has
    to probe a big `rowid IN (...)` list (which is quadratic-ish and
    took seconds at 16k rows)."""
    if not match:
        return []
    cond, params = (flt or Filters()).where("j")
    sql = (
        f"SELECT r.requisition_id, "
        f"bm25(jobs_fts, {', '.join(map(str, BM25_WEIGHTS))}) AS s "
        f"FROM jobs_fts "
        f"JOIN rec_rows r ON r.id = jobs_fts.rowid "
        f"JOIN jobs j ON j.requisition_id = r.requisition_id "
        f"WHERE jobs_fts MATCH ? AND {cond} ORDER BY s LIMIT ?"
    )
    try:
        return [(r[0], r[1]) for r in con.execute(sql, [match, *params, k])]
    except sqlite3.OperationalError:  # unparsable user query
        return []


def search_keywords(
    con: sqlite3.Connection, query: str, flt: Filters | None = None, k: int = 20
) -> list[dict]:
    """FTS5 search; all terms required, relaxed to OR when nothing hits."""
    tokens = _token_re.findall(query)
    hits = _fts_search(con, _fts_query(tokens, "AND"), flt, k)
    if not hits and len(tokens) > 1:
        hits = _fts_search(con, _fts_query(tokens, "OR"), flt, k)
    results = _hydrate(con, [rid for rid, _ in hits])
    for row, (_, score) in zip(results, hits):
        row["score"] = -score  # bm25() is negative-better; flip for display
    return results


# --------------------------------------------------------------------------
# 3) Vector k-NN (sqlite-vec, numpy fallback)
# --------------------------------------------------------------------------

_matrix_cache: dict[str, tuple[tuple, list[int], np.ndarray]] = {}


def _load_matrix(con: sqlite3.Connection) -> tuple[list[int], np.ndarray]:
    """All embeddings as one [n, dim] matrix (cached until the DB changes)."""
    key = con.execute("PRAGMA database_list").fetchone()[2]
    token = _db_token(con)
    cached = _matrix_cache.get(key)
    if cached is not None and cached[0] == token:
        return cached[1], cached[2]
    dim = int(get_meta(con)["dim"])
    ids, blobs = [], []
    for rowid, blob in con.execute("SELECT id, vec FROM job_embeddings"):
        ids.append(rowid)
        blobs.append(blob)
    mat = (
        np.frombuffer(b"".join(blobs), dtype=np.float32).reshape(len(ids), dim)
        if ids
        else np.zeros((0, dim), dtype=np.float32)
    )
    _matrix_cache[key] = (token, ids, mat)
    return ids, mat


def _knn(
    con: sqlite3.Connection, vec: np.ndarray, ids: list[int] | None, k: int
) -> list[tuple[str, float]]:
    """-> [(requisition_id, cosine_similarity)] best-first."""
    if ids is not None and not ids:
        return []
    blob = np.asarray(vec, dtype=np.float32).tobytes()
    have_vec0 = get_meta(con).get("vec0") == "1"
    if have_vec0:
        sql = "SELECT rowid, distance FROM jobs_vec WHERE embedding MATCH ?"
        if ids is not None:
            sql += f" AND rowid IN ({', '.join(map(str, ids))})"
        sql += " AND k = ?"
        try:
            hits = con.execute(sql, (blob, k)).fetchall()
            pairs = [(rowid, 1.0 - dist) for rowid, dist in hits]
            return _ids_to_rids(con, pairs)
        except sqlite3.OperationalError:
            pass  # extension didn't load in this process: numpy fallback
    all_ids, mat = _load_matrix(con)
    if not all_ids:
        return []
    sims = mat @ np.asarray(vec, dtype=np.float32)
    order = np.argsort(-sims)
    keep = set(ids) if ids is not None else None
    pairs = []
    for i in order:
        if keep is None or all_ids[i] in keep:
            pairs.append((all_ids[i], float(sims[i])))
            if len(pairs) == k:
                break
    return _ids_to_rids(con, pairs)


def _ids_to_rids(
    con: sqlite3.Connection, pairs: list[tuple[int, float]]
) -> list[tuple[str, float]]:
    if not pairs:
        return []
    qs = ", ".join(str(i) for i, _ in pairs)
    m = dict(con.execute(f"SELECT id, requisition_id FROM rec_rows WHERE id IN ({qs})"))
    return [(m[i], s) for i, s in pairs if i in m]


def similar_jobs(
    con: sqlite3.Connection,
    requisition_id: str,
    flt: Filters | None = None,
    k: int = 20,
    collapse_dupes: bool = True,
) -> list[dict]:
    """Jobs nearest to job X's stored embedding, X itself excluded."""
    row = con.execute(
        "SELECT e.vec, j.collapse_key FROM job_embeddings e "
        "JOIN jobs j ON j.requisition_id = e.requisition_id "
        "WHERE e.requisition_id = ?",
        (requisition_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"unknown or unindexed requisition_id {requisition_id!r}")
    vec = np.frombuffer(row[0], dtype=np.float32)
    own_collapse = row[1]
    ids = _candidate_ids(con, flt)
    # over-fetch: self + collapse-key dupes get dropped below
    hits = _knn(con, vec, ids, k * 3 + 8)
    results = _hydrate(con, [rid for rid, _ in hits])
    scores = dict(hits)
    out, seen_collapse = [], {own_collapse}
    for r in results:
        if r["requisition_id"] == requisition_id:
            continue
        if collapse_dupes:
            if r["collapse_key"] in seen_collapse:
                continue
            seen_collapse.add(r["collapse_key"])
        r["score"] = scores[r["requisition_id"]]
        out.append(r)
        if len(out) == k:
            break
    return out


# --------------------------------------------------------------------------
# 4) Resume matching (hybrid: embedding + keywords, RRF fusion)
# --------------------------------------------------------------------------

_MD_PATTERNS = [
    (re.compile(r"```.*?```", re.S), " "),  # fenced code
    (re.compile(r"`([^`]*)`"), r"\1"),  # inline code
    (re.compile(r"!\[[^\]]*\]\([^)]*\)"), " "),  # images
    (re.compile(r"\[([^\]]*)\]\([^)]*\)"), r"\1"),  # links -> text
    (re.compile(r"^#{1,6}\s*", re.M), ""),  # heading markers
    (re.compile(r"^\s*[-*+]\s+", re.M), ""),  # bullets
    (re.compile(r"[*_]{1,3}([^*_]+)[*_]{1,3}"), r"\1"),  # emphasis
    (re.compile(r"\|"), " "),  # table pipes
]


def strip_markdown(md: str) -> str:
    """Markdown -> plain text; line breaks kept as sentence boundaries."""
    text = md
    for pat, repl in _MD_PATTERNS:
        text = pat.sub(repl, text)
    return re.sub(r"[ \t]+", " ", text).strip()


_STOPWORDS = frozenset(
    "a an and are as at be but by for from has have i in is it my of on or "
    "our that the this to was we with you your years experience work team "
    "worked working using used skills responsibilities including".split()
)

_vocab_cache: dict[tuple, frozenset] = {}


def _corpus_vocab(con: sqlite3.Connection) -> frozenset:
    """Lowercased phrases worth matching on: every technical tool plus
    core-title unigrams/bigrams, from the corpus itself."""
    key = _db_token(con)
    if key in _vocab_cache:
        return _vocab_cache[key]
    vocab: set[str] = set()
    for (tool,) in con.execute(
        "SELECT DISTINCT lower(value) FROM jobs, json_each(jobs.technical_tools)"
    ):
        vocab.add(tool)
    for (title,) in con.execute(
        "SELECT DISTINCT lower(core_job_title) FROM jobs "
        "WHERE core_job_title IS NOT NULL"
    ):
        toks = _token_re.findall(title)
        vocab.update(toks)
        vocab.update(f"{a} {b}" for a, b in zip(toks, toks[1:]))
    vocab -= _STOPWORDS
    out = frozenset(vocab)
    _vocab_cache[key] = out
    return out


def extract_keywords(
    con: sqlite3.Connection, text: str, max_terms: int = 24
) -> list[str]:
    """Resume terms that exist in the corpus vocabulary (tools + title
    n-grams), longest phrases first; falls back to frequent resume tokens
    so the FTS leg never goes in empty."""
    vocab = _corpus_vocab(con)
    toks = [t.lower() for t in _token_re.findall(text)]
    counts: dict[str, int] = {}
    for phrase in toks + [f"{a} {b}" for a, b in zip(toks, toks[1:])]:
        if phrase in vocab:
            counts[phrase] = counts.get(phrase, 0) + 1
    ranked = sorted(counts, key=lambda p: (-len(p.split()), -counts[p], p))
    keywords = ranked[:max_terms]
    if len(keywords) < 5:  # sparse overlap: pad with frequent resume tokens
        freq: dict[str, int] = {}
        for t in toks:
            if t not in _STOPWORDS and len(t) > 2 and not t.isdigit():
                freq[t] = freq.get(t, 0) + 1
        for t in sorted(freq, key=lambda t: -freq[t]):
            if t not in keywords:
                keywords.append(t)
            if len(keywords) >= 10:
                break
    return keywords


def rrf(ranked_lists: list[list[str]], k0: int = RRF_K) -> dict[str, float]:
    """Reciprocal Rank Fusion: id -> sum over lists of 1/(k0 + rank)."""
    scores: dict[str, float] = {}
    for lst in ranked_lists:
        for rank, rid in enumerate(lst, start=1):
            scores[rid] = scores.get(rid, 0.0) + 1.0 / (k0 + rank)
    return scores


def match_resume(
    con: sqlite3.Connection,
    markdown: str,
    flt: Filters | None = None,
    k: int = 20,
) -> tuple[list[dict], list[str]]:
    """-> (ranked job dicts, extracted keywords). Hybrid retrieval:
    one embedding for the whole resume -> k-NN, extracted keywords ->
    FTS, RRF to fuse; filters pre-filter both legs."""
    text = strip_markdown(markdown)
    if not text:
        return [], []
    depth = max(k * 3, 30)

    embedder = _query_embedder(con)
    vec = embedder.encode([text])[0]
    vec_hits = _knn(con, vec, _candidate_ids(con, flt), depth)

    keywords = extract_keywords(con, text)
    fts_hits = _fts_search(con, _fts_query(keywords, "OR"), flt, depth)

    vec_rank = [rid for rid, _ in vec_hits]
    fts_rank = [rid for rid, _ in fts_hits]
    fused = rrf([vec_rank, fts_rank])
    top = sorted(fused, key=fused.__getitem__, reverse=True)[:k]

    results = _hydrate(con, top)
    vec_pos = {rid: i + 1 for i, rid in enumerate(vec_rank)}
    fts_pos = {rid: i + 1 for i, rid in enumerate(fts_rank)}
    for r in results:
        rid = r["requisition_id"]
        r["score"] = fused[rid]
        r["signals"] = {
            "vector_rank": vec_pos.get(rid),
            "keyword_rank": fts_pos.get(rid),
        }
    return results, keywords


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _add_filter_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--category", action="append", default=[])
    p.add_argument("--seniority", action="append", default=[])
    p.add_argument(
        "--workplace",
        action="append",
        default=[],
        help="workplace_type, e.g. Remote / On-site / Hybrid",
    )
    p.add_argument("--role-type", action="append", default=[])
    p.add_argument("--country", action="append", default=[])
    p.add_argument(
        "--tool",
        action="append",
        default=[],
        help="require this technical tool (repeatable, AND)",
    )
    p.add_argument("--location", help="substring of the formatted location")
    p.add_argument("--company", help="substring of the company name")
    p.add_argument("--min-comp", type=float, help="minimum yearly compensation")
    p.add_argument(
        "--max-yoe", type=float, help="max years-of-experience the job may require"
    )
    p.add_argument("--include-expired", action="store_true")
    p.add_argument("-k", "--limit", type=int, default=20)
    p.add_argument("--json", action="store_true", help="print results as a JSON array")


def _filters(args: argparse.Namespace) -> Filters:
    return Filters(
        categories=args.category,
        seniority=args.seniority,
        workplace_types=args.workplace,
        role_types=args.role_type,
        countries=args.country,
        tools=args.tool,
        location=args.location,
        company=args.company,
        min_comp=args.min_comp,
        max_yoe=args.max_yoe,
        include_expired=args.include_expired,
    )


def _print_results(rows: list[dict], as_json: bool) -> None:
    if as_json:
        print(json.dumps(rows, indent=2))
        return
    if not rows:
        print("no results")
        return
    for i, r in enumerate(rows, 1):
        comp = ""
        if r["yearly_min_compensation"] or r["yearly_max_compensation"]:
            lo, hi = (r["yearly_min_compensation"], r["yearly_max_compensation"])
            comp = f"  ${lo or hi:,.0f}–${hi or lo:,.0f}"
        score = f"  [{r['score']:.4g}]" if "score" in r else ""
        print(f"{i:>3}. {r['title']} — {r['company_name'] or '?'}{score}")
        print(
            f"     {r['seniority_level'] or '-'} | "
            f"{r['workplace_type'] or '-'} | "
            f"{r['formatted_workplace_location'] or '-'}{comp}  "
            f"({r['requisition_id']})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_filter = sub.add_parser("filter", help="structured filtering only")
    p_search = sub.add_parser("search", help="keyword search (FTS5/bm25)")
    p_search.add_argument("query")
    p_similar = sub.add_parser("similar", help="jobs similar to job X")
    p_similar.add_argument("requisition_id")
    p_similar.add_argument(
        "--keep-dupes",
        action="store_true",
        help="don't collapse same-collapse_key posts",
    )
    p_resume = sub.add_parser("resume", help="match a markdown resume")
    p_resume.add_argument("resume_md", type=Path)
    for p in (p_filter, p_search, p_similar, p_resume):
        _add_filter_args(p)

    args = parser.parse_args()
    con = get_connection(args.db)
    flt = _filters(args)

    if args.cmd == "filter":
        rows = filter_jobs(con, flt, limit=args.limit)
    elif args.cmd == "search":
        rows = search_keywords(con, args.query, flt, k=args.limit)
    elif args.cmd == "similar":
        try:
            rows = similar_jobs(
                con,
                args.requisition_id,
                flt,
                k=args.limit,
                collapse_dupes=not args.keep_dupes,
            )
        except KeyError as e:
            raise SystemExit(str(e))
    else:
        rows, keywords = match_resume(
            con, args.resume_md.read_text(encoding="utf-8"), flt, k=args.limit
        )
        if not args.json:
            print(f"keywords: {', '.join(keywords) or '(none)'}\n")
    _print_results(rows, args.json)


if __name__ == "__main__":
    main()
