#!/usr/bin/env python3
"""
HiringCafe job scraper (Job Scout).

Scrapes job listings, including full description text, from hiringcafe.com via its
server-side-rendered Next.js JSON data routes (no HTML parsing, no browser).

How it works (verified 2026-07-15):
  1. GET https://hiringcafe.com/          -> extract Next.js buildId from __NEXT_DATA__
  2. GET /_next/data/{buildId}/index.json?searchState={...}&page=N
        -> paginated search results ("ssrHits"), ~40+ jobs/page, no descriptions
  3. GET /_next/data/{buildId}/job/x-{requisition_id}.json
        -> 308 redirect payload with the canonical job slug
  4. GET /_next/data/{buildId}/job/{canonical-slug}.json
        -> full job object incl. job_information.description (HTML)

Usage:
  python scrape_hiringcafe.py --query "software engineer" --max-jobs 50
  python scrape_hiringcafe.py --url "https://hiringcafe.com/?searchState=..." --max-jobs 100
  python scrape_hiringcafe.py --query "data analyst" --no-descriptions   # fast, cards only

Tip: for filters (salary, remote, seniority...), set them in the hiringcafe.com UI,
copy the URL from your address bar, and pass it via --url.

Example URL:
  https://hiringcafe.com/?searchState=%7B%22searchQuery%22%3A%22software%20engineer%22%7D

Requires: pip install requests typer
Be polite: keep --delay >= 0.5s. Unofficial API; their robots.txt discourages
bulk crawling, so scrape only what you need.
"""

import csv
import json
import re
import sys
import time
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests
import typer

BASE = "https://hiringcafe.com"
USER_AGENT = (
    "JobScout/1.0 (personal job search script; contact: alexander.wu7@gmail.com)"
)

CSV_COLUMNS = [
    "id",
    "requisition_id",
    "title",
    "company",
    "location",
    "workplace_type",
    "commitment",
    "yearly_min_compensation",
    "yearly_max_compensation",
    "compensation_currency",
    "compensation_frequency",
    "date_posted",
    "source_ats",
    "board_token",
    "technical_tools",
    "requirements_summary",
    "hiringcafe_url",
    "apply_url",
    "description_text",
]


class StaleBuildId(Exception):
    pass


# ---------------------------------------------------------------- HTML -> text
class _TextExtractor(HTMLParser):
    BLOCK_TAGS = frozenset(
        {
            "p",
            "div",
            "br",
            "li",
            "ul",
            "ol",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "tr",
            "table",
            "section",
            "article",
        }
    )

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")
        if tag == "li":
            self.parts.append("- ")

    def handle_endtag(self, tag: str) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def html_to_text(html: str) -> str:
    if not html:
        return ""
    p = _TextExtractor()
    p.feed(html)
    text = "".join(p.parts)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


# ------------------------------------------------------------------- HTTP layer
class Client:
    def __init__(self, delay: float):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/html;q=0.9",
            }
        )
        self._last_request = 0.0

    def get(self, url: str, retries: int = 3) -> requests.Response:
        for attempt in range(retries):
            wait = self.delay - (time.time() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.time()
            try:
                resp = self.session.get(url, timeout=30)
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                print(f"  ! {type(e).__name__}, retrying...", file=sys.stderr)
                time.sleep(2**attempt)
                continue
            if resp.status_code == 404:
                raise StaleBuildId(url)
            if resp.status_code in (429, 403, 500, 502, 503):
                if attempt == retries - 1:
                    resp.raise_for_status()
                backoff = 5 * (2**attempt)
                print(
                    f"  ! HTTP {resp.status_code}, backing off {backoff}s...",
                    file=sys.stderr,
                )
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError("unreachable")


# --------------------------------------------------------------- API functions
def get_build_id(client: Client) -> str:
    html = client.get(BASE + "/").text
    m = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
    if not m:
        sys.exit(
            "ERROR: could not find Next.js buildId on the homepage. "
            "The site structure may have changed."
        )
    return m.group(1)


def search_page(
    client: Client, build_id: str, search_state: dict[str, Any], page: int
) -> dict[str, Any]:
    ss = quote(json.dumps(search_state, separators=(",", ":")))
    url = f"{BASE}/_next/data/{build_id}/index.json?searchState={ss}&page={page}"
    data = client.get(url).json()
    props: dict[str, Any] = data.get("pageProps", {})
    return props


def fetch_job_detail(
    client: Client, build_id: str, requisition_id: str
) -> tuple[dict[str, Any] | None, str]:
    """Returns (job_dict_or_None, canonical_hiringcafe_url_or_empty)."""
    url = f"{BASE}/_next/data/{build_id}/job/x-{quote(requisition_id)}.json"
    props = client.get(url).json().get("pageProps", {})
    canonical_url = ""
    if "__N_REDIRECT" in props:  # canonical slug redirect
        path = props["__N_REDIRECT"]  # e.g. /job/title-company-city-<id>
        canonical_url = BASE + path
        slug = path.split("/job/", 1)[-1]
        url = f"{BASE}/_next/data/{build_id}/job/{quote(slug)}.json"
        props = client.get(url).json().get("pageProps", {})
    return props.get("job"), canonical_url


# ------------------------------------------------------------------ flattening
def _join(val: Any) -> str:
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    return "" if val is None else str(val)


def flatten(
    hit: dict[str, Any], detail: dict[str, Any] | None, canonical_url: str
) -> dict[str, Any]:
    v5 = (
        (detail or hit).get("v5_processed_job_data")
        or hit.get("v5_processed_job_data")
        or {}
    )
    ji = (detail or hit).get("job_information") or {}
    return {
        "id": hit.get("id", ""),
        "requisition_id": hit.get("requisition_id", ""),
        "title": ji.get("title") or v5.get("core_job_title", ""),
        "company": v5.get("company_name", ""),
        "location": v5.get("formatted_workplace_location", ""),
        "workplace_type": v5.get("workplace_type", ""),
        "commitment": _join(v5.get("commitment")),
        "yearly_min_compensation": v5.get("yearly_min_compensation", ""),
        "yearly_max_compensation": v5.get("yearly_max_compensation", ""),
        "compensation_currency": v5.get("listed_compensation_currency", ""),
        "compensation_frequency": v5.get("listed_compensation_frequency", ""),
        "date_posted": v5.get("estimated_publish_date", ""),
        "source_ats": hit.get("source", ""),
        "board_token": hit.get("board_token", ""),
        "technical_tools": _join(v5.get("technical_tools")),
        "requirements_summary": v5.get("requirements_summary", ""),
        "hiringcafe_url": canonical_url,
        "apply_url": (detail or {}).get("apply_url") or hit.get("hc_apply_url", ""),
        "description_text": html_to_text(ji.get("description", "")),
    }


# ------------------------------------------------------------------------ main
def parse_search_state(
    query: str | None, url: str | None, search_state: str | None
) -> dict[str, Any]:
    if url:
        qs = parse_qs(urlparse(url).query)
        if "searchState" in qs:
            from_url: dict[str, Any] = json.loads(qs["searchState"][0])
            return from_url
        print(
            "WARNING: no searchState in --url; scraping default feed.", file=sys.stderr
        )
        return {}
    if search_state:
        from_arg: dict[str, Any] = json.loads(search_state)
        return from_arg
    if query:
        return {"searchQuery": query}
    return {}


app = typer.Typer(
    add_completion=False,
    help="Scrape job listings from hiringcafe.com",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.command()
def main(
    query: str | None = typer.Option(
        None, "--query", help='keyword search, e.g. "software engineer"'
    ),
    url: str | None = typer.Option(
        None,
        "--url",
        help="a hiringcafe.com URL with searchState (set filters in the UI, copy the URL)",
    ),
    search_state: str | None = typer.Option(
        None, "--search-state", help="raw searchState JSON string"
    ),
    max_jobs: int = typer.Option(40, "--max-jobs"),
    max_pages: int = typer.Option(25, "--max-pages"),
    delay: float = typer.Option(
        1.0, "--delay", help="seconds between requests (default 1.0)"
    ),
    out: str = typer.Option("hiringcafe_jobs.csv", "--out"),
    jsonl: str | None = typer.Option(
        None, "--jsonl", help="also dump raw job JSON to this file"
    ),
    no_descriptions: bool = typer.Option(
        False,
        "--no-descriptions",
        help="skip per-job detail fetches (much faster, no description text)",
    ),
) -> None:
    if delay < 0.5:
        print("Refusing delay < 0.5s — be polite to their servers.", file=sys.stderr)
        delay = 0.5

    search_state_dict = parse_search_state(query, url, search_state)
    client = Client(delay)

    print("Bootstrapping buildId...")
    build_id = get_build_id(client)
    print(f"  buildId = {build_id}")

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    raw_dump = open(jsonl, "w", encoding="utf-8") if jsonl else None
    page = 0
    try:
        while len(rows) < max_jobs and page < max_pages:
            print(f"Search page {page}...")
            try:
                props = search_page(client, build_id, search_state_dict, page)
            except StaleBuildId:
                print("  buildId went stale (site redeployed); re-bootstrapping...")
                build_id = get_build_id(client)
                props = search_page(client, build_id, search_state_dict, page)

            hits = props.get("ssrHits") or []
            if page == 0:
                print(f"  {props.get('ssrTotalCount', '?')} total jobs match")
            new_hits = [
                h
                for h in hits
                if not h.get("is_hc_pinned")
                and h.get("requisition_id")
                and h.get("id") not in seen
            ]
            print(f"  {len(hits)} hits, {len(new_hits)} new")

            for hit in new_hits:
                if len(rows) >= max_jobs:
                    break
                seen.add(hit.get("id"))
                detail, canonical_url = None, ""
                if not no_descriptions:
                    try:
                        detail, canonical_url = fetch_job_detail(
                            client, build_id, hit["requisition_id"]
                        )
                    except StaleBuildId:
                        build_id = get_build_id(client)
                        detail, canonical_url = fetch_job_detail(
                            client, build_id, hit["requisition_id"]
                        )
                    except Exception as e:
                        print(
                            f"  ! detail fetch failed for {hit.get('id')}: {e}",
                            file=sys.stderr,
                        )
                row = flatten(hit, detail, canonical_url)
                rows.append(row)
                if raw_dump:
                    raw_dump.write(
                        json.dumps({"hit": hit, "detail": detail}, ensure_ascii=False)
                        + "\n"
                    )
                print(
                    f"  [{len(rows)}/{max_jobs}] {row['title']} @ {row['company']}"
                    f" (desc: {len(row['description_text'])} chars)"
                )

            if props.get("ssrIsLastPage") or not new_hits:
                print("  last page reached.")
                break
            page += 1
    except KeyboardInterrupt:
        print("\nInterrupted — writing what we have...")
    finally:
        if raw_dump:
            raw_dump.close()

    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    with_desc = sum(1 for r in rows if len(r["description_text"]) > 100)
    print(f"\nDone: {len(rows)} jobs -> {out} ({with_desc} with full descriptions)")


if __name__ == "__main__":
    app()
