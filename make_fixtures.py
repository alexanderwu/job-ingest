#!/usr/bin/env python3
"""
Synthetic job-listing fixtures for tests, benchmarks, and the eval
harness. The real corpus (data/raw/json/) is not in the repo, so this
module generates schema-conformant stand-ins: every page it emits round-
trips through job_schema.decode_page + flatten, i.e. the exact validation
path of the msgspec ingest engine.

Jobs are drawn from ~10 archetypes (backend, frontend, data science,
nursing, accounting, ...) with distinct titles/tools/description
vocabulary, and the requisition_id encodes the archetype
("SYN-backend-00042"), which gives the eval harness free ground-truth
labels: for a backend resume, the relevant jobs are the backend ones.
Generation is fully deterministic for a given seed.

CLI (writes .json.gz files the ingest can consume):
    uv run make_fixtures.py --out data/raw/json --n 500 [--seed 42]
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_schema import COLUMNS, decode_page, flatten  # noqa: E402

# name -> (titles, category, tools, description sentences, comp lo-hi)
ARCHETYPES: dict[str, dict] = {
    "backend": dict(
        titles=[
            "Backend Engineer",
            "Software Engineer, Backend",
            "Platform Engineer",
            "API Engineer",
        ],
        category="Software Engineering",
        tools=[
            "Python",
            "PostgreSQL",
            "Docker",
            "Kubernetes",
            "Redis",
            "AWS",
            "gRPC",
            "Terraform",
        ],
        desc=[
            "Design and operate distributed backend services and REST APIs.",
            "Own PostgreSQL schema design, query tuning and migrations.",
            "Deploy microservices to Kubernetes with CI/CD pipelines.",
        ],
        comp=(120_000, 210_000),
    ),
    "frontend": dict(
        titles=["Frontend Engineer", "UI Engineer", "Software Engineer, Web"],
        category="Software Engineering",
        tools=["TypeScript", "React", "Next.js", "CSS", "GraphQL", "Jest"],
        desc=[
            "Build accessible, responsive web interfaces in React and TypeScript.",
            "Collaborate with design on component systems and design tokens.",
            "Profile and optimize bundle size and rendering performance.",
        ],
        comp=(110_000, 190_000),
    ),
    "datasci": dict(
        titles=[
            "Data Scientist",
            "Senior Data Scientist",
            "Machine Learning Scientist",
        ],
        category="Data Science",
        tools=["Python", "pandas", "scikit-learn", "SQL", "Airflow", "Snowflake"],
        desc=[
            "Build statistical models and experiments to drive product decisions.",
            "Design A/B tests and communicate findings to stakeholders.",
            "Productionize models in partnership with engineering.",
        ],
        comp=(125_000, 200_000),
    ),
    "mleng": dict(
        titles=["Machine Learning Engineer", "ML Platform Engineer"],
        category="Data Science",
        tools=["Python", "PyTorch", "Kubernetes", "MLflow", "Spark", "AWS"],
        desc=[
            "Train, evaluate and deploy deep learning models at scale.",
            "Build feature pipelines and model-serving infrastructure.",
            "Optimize GPU training throughput and inference latency.",
        ],
        comp=(140_000, 240_000),
    ),
    "devops": dict(
        titles=[
            "Site Reliability Engineer",
            "DevOps Engineer",
            "Infrastructure Engineer",
        ],
        category="Software Engineering",
        tools=["Kubernetes", "Terraform", "AWS", "Prometheus", "Go", "Linux"],
        desc=[
            "Run production infrastructure with strong SLOs and on-call rotation.",
            "Automate provisioning with Terraform and GitOps.",
            "Improve observability with metrics, tracing and alerting.",
        ],
        comp=(130_000, 220_000),
    ),
    "nurse": dict(
        titles=["Registered Nurse", "RN, Medical-Surgical", "Registered Nurse - ICU"],
        category="Healthcare",
        tools=["Epic", "EMR", "IV therapy"],
        desc=[
            "Provide direct patient care on a 12-hour shift schedule.",
            "Active RN license required; BLS and ACLS certification preferred.",
            "Document assessments and medication administration in Epic.",
        ],
        comp=(70_000, 110_000),
    ),
    "accountant": dict(
        titles=["Staff Accountant", "Senior Accountant", "Accounting Manager"],
        category="Finance",
        tools=["Excel", "QuickBooks", "NetSuite", "GAAP"],
        desc=[
            "Own month-end close, reconciliations and journal entries.",
            "Prepare financial statements in accordance with GAAP.",
            "CPA license preferred; support annual audits.",
        ],
        comp=(65_000, 120_000),
    ),
    "product": dict(
        titles=["Product Manager", "Senior Product Manager"],
        category="Product",
        tools=["SQL", "Jira", "Figma", "Amplitude"],
        desc=[
            "Define roadmap and requirements for a cross-functional product team.",
            "Talk to customers, size opportunities and prioritize ruthlessly.",
            "Ship iteratively and measure outcomes with product analytics.",
        ],
        comp=(130_000, 210_000),
    ),
    "security": dict(
        titles=["Security Analyst", "Security Engineer", "SOC Analyst"],
        category="Security",
        tools=["Splunk", "SIEM", "Python", "AWS", "Wireshark"],
        desc=[
            "Monitor, triage and respond to security incidents in the SOC.",
            "Active TS/SCI clearance required for this position.",
            "Harden cloud infrastructure and run tabletop exercises.",
        ],
        comp=(95_000, 170_000),
    ),
    "warehouse": dict(
        titles=["Warehouse Associate", "Forklift Operator", "Fulfillment Associate"],
        category="Operations",
        tools=["Forklift", "RF scanner"],
        desc=[
            "Pick, pack and ship customer orders accurately and safely.",
            "Operate powered industrial equipment; lift up to 50 lbs.",
            "Weekend and holiday availability required during peak season.",
        ],
        comp=(35_000, 52_000),
    ),
}

_SENIORITY = ["Entry level", "Mid level", "Senior", "Staff"]
_WORKPLACE = ["Remote", "On-site", "Hybrid"]
_LOCATIONS = [
    ("San Francisco, CA, USA", "United States", 37.77, -122.42),
    ("New York, NY, USA", "United States", 40.71, -74.01),
    ("Austin, TX, USA", "United States", 30.27, -97.74),
    ("Seattle, WA, USA", "United States", 47.61, -122.33),
    ("Toronto, ON, Canada", "Canada", 43.65, -79.38),
    ("London, UK", "United Kingdom", 51.51, -0.13),
]
_COMPANIES = [
    "Acme Analytics",
    "Borealis Health",
    "Cobalt Systems",
    "Driftwood Labs",
    "Everline",
    "Foxglove Robotics",
    "Granite Peak Software",
    "Helios Grid",
]


def archetype_of(requisition_id: str) -> str:
    """'SYN-backend-00042' -> 'backend' (ground truth for the eval)."""
    return requisition_id.split("-")[1]


def make_page(arch: str, i: int, rng: random.Random) -> dict:
    """One schema-conformant pageProps payload for archetype `arch`."""
    spec = ARCHETYPES[arch]
    rid = f"SYN-{arch}-{i:05d}"
    title = rng.choice(spec["titles"])
    seniority = rng.choice(_SENIORITY)
    loc, country, lat, lon = rng.choice(_LOCATIONS)
    company = rng.choice(_COMPANIES)
    tools = rng.sample(spec["tools"], k=min(len(spec["tools"]), 4))
    lo, hi = spec["comp"]
    comp_lo = float(rng.randrange(lo, hi, 5000))
    comp_hi = comp_lo + rng.randrange(10_000, 40_000, 5000)
    desc_sentences = (
        [f"{company} is hiring a {seniority.lower()} {title.lower()} in {loc}."]
        + rng.sample(spec["desc"], k=len(spec["desc"]))
        + [
            f"Day to day you will work with {', '.join(tools)}.",
            "We offer competitive compensation and benefits.",
        ]
    )
    # ~1 in 7 same-archetype jobs share a collapse_key (repost dupes);
    # ~1 in 10 is expired.
    collapse = f"SYN-{arch}-{i - 1:05d}" if i % 7 == 0 and i > 0 else rid
    return {
        "pageProps": {
            "job": {
                "id": rid.lower(),
                "board_token": rng.choice(["acme", "jobs", 12345]),
                "source": "synthetic",
                "apply_url": f"https://example.com/apply/{rid}",
                "source_and_board_token": "synthetic:jobs",
                "requisition_id": rid,
                "collapse_key": collapse,
                "is_expired": i % 10 == 9,
                "objectID": rid,
                "job_information": {
                    "title": f"{seniority} {title}"
                    if seniority != "Entry level"
                    else title,
                    "job_title_raw": title,
                    "description": " ".join(desc_sentences),
                    "viewedByUsers": None,
                },
                "v5_processed_job_data": {
                    "core_job_title": title,
                    "technical_tools": tools,
                    "job_category": spec["category"],
                    "seniority_level": seniority,
                    "role_type": "Individual Contributor",
                    "workplace_type": rng.choice(_WORKPLACE),
                    "formatted_workplace_location": loc,
                    "workplace_countries": [country],
                    "min_industry_and_role_yoe": float(_SENIORITY.index(seniority) * 3),
                    "yearly_min_compensation": comp_lo,
                    "yearly_max_compensation": comp_hi,
                    "listed_compensation_currency": "USD",
                    "is_compensation_transparent": True,
                    "estimated_publish_date": f"2026-{rng.randrange(1, 7):02d}-"
                    f"{rng.randrange(1, 29):02d}T12:00:00Z",
                    "company_name": company,
                    "company_website": "https://example.com",
                },
                "_geoloc": [{"lat": lat, "lon": lon}],
            }
        },
        "__N_SSG": True,
    }


def pages(n: int, seed: int = 42) -> list[dict]:
    """n pages, archetypes round-robin, deterministic for a seed."""
    rng = random.Random(seed)
    names = list(ARCHETYPES)
    return [make_page(names[i % len(names)], i, rng) for i in range(n)]


def rows(n: int, seed: int = 42) -> list[tuple]:
    """n flat 33-column rows, via the real decode+flatten path."""
    return [flatten(decode_page(json.dumps(p))) for p in pages(n, seed)]


def write_files(out_dir: Path, n: int, seed: int = 42) -> list[Path]:
    """Write n .json.gz fixture files for the ingest to consume."""
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for page in pages(n, seed):
        rid = page["pageProps"]["job"]["requisition_id"]
        path = out_dir / f"{rid}.json.gz"
        # mtime is in the payload's hash path, so make writes idempotent
        data = gzip.compress(json.dumps(page).encode(), mtime=0)
        if not path.exists() or path.read_bytes() != data:
            path.write_bytes(data)
        paths.append(path)
    return paths


def populate_sqlite(db_path: Path, n: int, seed: int = 42) -> None:
    """Build a jobs.sqlite directly (no files) — for large synthetic
    benchmarks where writing/ingesting 100k .json.gz files is pointless."""
    from ingest_and_benchmark import SQLITE_DDL

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.executescript(SQLITE_DDL)
    with con:
        con.executemany(
            f"INSERT OR REPLACE INTO jobs VALUES ({','.join('?' * len(COLUMNS))})",
            rows(n, seed),
        )
    con.close()


# Per-archetype sample resumes (markdown) for the eval harness and tests.
RESUMES: dict[str, str] = {
    "backend": """\
# Jordan Lee — Backend Engineer

## Experience
- **Senior Backend Engineer**, 6 years. Built REST and gRPC APIs in
  **Python**, tuned **PostgreSQL** queries, ran services on
  **Kubernetes** with Docker and Terraform on AWS.
- Designed caching with Redis; on-call for distributed systems.

## Skills
Python, PostgreSQL, Kubernetes, Docker, Redis, AWS, Terraform, gRPC
""",
    "datasci": """\
# Priya Sharma — Data Scientist

## Experience
- **Data Scientist**, 4 years. Statistical modeling and A/B testing in
  **Python** with pandas and scikit-learn; pipelines in Airflow on
  Snowflake.
- Partnered with product managers to design experiments.

## Skills
Python, pandas, scikit-learn, SQL, Airflow, Snowflake, statistics
""",
    "nurse": """\
# Maria Gonzalez, RN

## Experience
- **Registered Nurse**, Medical-Surgical unit, 5 years. Direct patient
  care, medication administration, charting in **Epic** EMR.
- Active RN license, BLS and ACLS certified. IV therapy.

## Skills
Patient care, Epic, EMR, IV therapy, 12-hour shifts
""",
    "accountant": """\
# Sam Park, CPA

## Experience
- **Senior Accountant**, 7 years. Month-end close, reconciliations,
  journal entries, GAAP financial statements in **NetSuite** and
  QuickBooks; advanced **Excel**.
- CPA license; supported Big Four audits.

## Skills
GAAP, Excel, NetSuite, QuickBooks, month-end close, CPA
""",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="directory for the .json.gz fixture files",
    )
    parser.add_argument("--n", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    paths = write_files(args.out, args.n, args.seed)
    print(f"wrote {len(paths)} fixture files to {args.out}")


if __name__ == "__main__":
    main()
