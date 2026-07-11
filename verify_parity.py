#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pydantic>=2", "pyarrow>=17"]
# ///
"""
Verify that the Rust fastingest output (Arrow IPC) matches the original
Python path (gzip -> json -> Pydantic job_schema -> flatten) value-for-value.

Byte-exact equality is NOT expected for JSON-text and datetime columns
(whitespace, fractional-second padding, "+00:00" vs "Z"); those are compared
at the value level. Everything else must match exactly.

Usage:
    uv run verify_parity.py --json-dir cache/json --arrow db/jobs.arrow --n 500
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_schema import JobPage  # noqa: E402

COLUMNS = [
    "id", "source", "board_token", "apply_url", "requisition_id",
    "collapse_key", "is_expired", "title", "job_title_raw", "description",
    "core_job_title", "job_category", "seniority_level", "role_type",
    "workplace_type", "formatted_workplace_location", "workplace_countries",
    "min_industry_and_role_yoe", "yearly_min_compensation",
    "yearly_max_compensation", "listed_compensation_currency",
    "technical_tools", "estimated_publish_date", "company_name",
    "company_website", "enriched_status", "nb_employees", "year_founded",
    "latitude", "longitude", "job_information_json",
    "v5_processed_job_data_json", "enriched_company_data_json",
]

# JSON-array/object text columns: compare parsed values (and key order).
JSON_COLUMNS = {
    "workplace_countries", "technical_tools", "job_information_json",
    "v5_processed_job_data_json", "enriched_company_data_json",
}
# Datetime-valued keys inside the JSON blobs, normalized before comparison.
BLOB_DATETIME_KEYS = {"estimated_publish_date", "enriched_at"}


def flatten(page: JobPage) -> tuple:
    """The original flatten() from ingest_and_benchmark.py, kept verbatim."""
    job = page.pageProps.job
    ji = job.job_information
    v5 = job.v5_processed_job_data
    ec = job.enriched_company_data

    lat = job.geoloc[0].lat if job.geoloc else None
    lon = job.geoloc[0].lon if job.geoloc else None

    ji_blob = json.dumps(ji.model_dump(exclude={"description"}))

    return (
        job.id,
        job.source,
        str(job.board_token),
        job.apply_url,
        job.requisition_id,
        job.collapse_key,
        job.is_expired,
        ji.title,
        ji.job_title_raw,
        ji.description,
        v5.core_job_title,
        v5.job_category,
        v5.seniority_level,
        v5.role_type,
        v5.workplace_type,
        v5.formatted_workplace_location,
        json.dumps(v5.workplace_countries),
        v5.min_industry_and_role_yoe,
        v5.yearly_min_compensation,
        v5.yearly_max_compensation,
        v5.listed_compensation_currency,
        json.dumps(v5.technical_tools),
        v5.estimated_publish_date.isoformat() if v5.estimated_publish_date else None,
        v5.company_name,
        v5.company_website,
        ec.status if ec else None,
        ec.nb_employees if ec else None,
        ec.year_founded if ec else None,
        lat,
        lon,
        ji_blob,
        v5.model_dump_json(by_alias=True),
        ec.model_dump_json() if ec else None,
    )


def _normalize_json_value(v, key: str | None = None):
    """Recursively normalize a parsed JSON value: datetime-valued keys are
    parsed to aware datetimes so "...Z" vs "...+00:00" compare equal."""
    if isinstance(v, dict):
        return {k: _normalize_json_value(x, k) for k, x in v.items()}
    if isinstance(v, list):
        return [_normalize_json_value(x) for x in v]
    if key in BLOB_DATETIME_KEYS and isinstance(v, str):
        return datetime.fromisoformat(v)
    return v


def compare_value(col: str, py_val, rs_val) -> str | None:
    """Return an error description, or None if equal under the column's rule."""
    if col in JSON_COLUMNS:
        if py_val is None or rs_val is None:
            return None if py_val == rs_val else f"null mismatch: {py_val!r} vs {rs_val!r}"
        py_parsed, rs_parsed = json.loads(py_val), json.loads(rs_val)
        if isinstance(py_parsed, dict):
            if list(py_parsed.keys()) != list(rs_parsed.keys()):
                only_py = [k for k in py_parsed if k not in rs_parsed]
                only_rs = [k for k in rs_parsed if k not in py_parsed]
                return (f"key order/set mismatch (only-python={only_py}, "
                        f"only-rust={only_rs})")
        if _normalize_json_value(py_parsed) != _normalize_json_value(rs_parsed):
            return f"value mismatch:\n  python: {py_val[:400]}\n  rust:   {rs_val[:400]}"
        return None
    if col == "estimated_publish_date":
        if (py_val is None) != (rs_val is None):
            return f"null mismatch: {py_val!r} vs {rs_val!r}"
        if py_val is not None and datetime.fromisoformat(py_val) != datetime.fromisoformat(rs_val):
            return f"datetime mismatch: {py_val!r} vs {rs_val!r}"
        return None
    if py_val != rs_val:
        return f"exact mismatch: {py_val!r} vs {rs_val!r}"
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-dir", type=Path, default=Path("cache/json"))
    parser.add_argument("--arrow", type=Path, default=Path("db/jobs.arrow"))
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    with pa.ipc.open_file(args.arrow) as reader:
        table = reader.read_all()
    cols = {name: table.column(name).to_pylist() for name in COLUMNS}
    index = {rid: i for i, rid in enumerate(cols["requisition_id"])}
    print(f"Arrow table: {table.num_rows} rows")

    files = sorted(args.json_dir.glob("*.json.gz"))
    rng = random.Random(args.seed)
    sample = rng.sample(files, min(args.n, len(files)))

    checked = 0
    skipped = 0
    mismatch_counts: dict[str, int] = {}
    first_diffs: list[str] = []
    for path in sample:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            raw = json.load(fh)
        try:
            page = JobPage.model_validate(raw)
        except Exception:
            skipped += 1  # invalid under Pydantic; Rust error-count check covers these
            continue
        py_row = flatten(page)
        rid = py_row[COLUMNS.index("requisition_id")]
        if rid not in index:
            mismatch_counts["<missing row>"] = mismatch_counts.get("<missing row>", 0) + 1
            first_diffs.append(f"{path.name}: requisition_id {rid!r} not in Arrow table")
            continue
        i = index[rid]
        for col, py_val in zip(COLUMNS, py_row):
            err = compare_value(col, py_val, cols[col][i])
            if err:
                mismatch_counts[col] = mismatch_counts.get(col, 0) + 1
                if len(first_diffs) < 10:
                    first_diffs.append(f"{path.name} [{col}]: {err}")
        checked += 1

    print(f"Checked {checked} files ({skipped} skipped as Pydantic-invalid)")
    if mismatch_counts:
        print("\nMISMATCHES per column:")
        for col, n in sorted(mismatch_counts.items(), key=lambda kv: -kv[1]):
            print(f"  {col}: {n}")
        print("\nFirst diffs:")
        for d in first_diffs:
            print(f"  {d}")
        sys.exit(1)
    print("All values match. PARITY OK")


if __name__ == "__main__":
    main()
