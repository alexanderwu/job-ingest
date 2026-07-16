#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests"]
# ///
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

Example URL: https://hiringcafe.com/?searchState=%7B%22searchQuery%22%3A%22software%20engineer%22%7D

Requires: pip install requests
Be polite: keep --delay >= 0.5s. Unofficial API; their robots.txt discourages
bulk crawling, so scrape only what you need.
"""

import argparse
import csv
import json
import re
import sys
import time
from html.parser import HTMLParser
from urllib.parse import quote, urlparse, parse_qs

import requests

BASE = "https://hiringcafe.com"
USER_AGENT = (
    "JobScout/1.0 (personal job search script; contact: alexander.wu7@gmail.com)"
)

CSV_COLUMNS = [
    "id", "requisition_id", "title", "company", "location", "workplace_type",
    "commitment", "yearly_min_compensation", "yearly_max_compensation",
    "compensation_currency", "compensation_frequency", "date_posted",
    "source_ats", "board_token", "technical_tools", "requirements_summary",
    "hiringcafe_url", "apply_url", "description_text",
]


class StaleBuildId(Exception):
    pass


# ---------------------------------------------------------------- HTML -> text
class _TextExtractor(HTMLParser):
    BLOCK_TAGS = {"p", "div", "br", "li", "ul", "ol", "h1", "h2", "h3", "h4",
                  "h5", "h6", "tr", "table", "section", "article"}

    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")
        if tag == "li":
            self.parts.append("- ")

    def handle_endtag(self, tag):
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
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
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/html;q=0.9",
        })
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
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 404:
                raise StaleBuildId(url)
            if resp.status_code in (429, 403, 500, 502, 503):
                if attempt == retries - 1:
                    resp.raise_for_status()
                backoff = 5 * (2 ** attempt)
                print(f"  ! HTTP {resp.status_code}, backing off {backoff}s...",
                      file=sys.stderr)
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
        sys.exit("ERROR: could not find Next.js buildId on the homepage. "
                 "The site structure may have changed.")
    return m.group(1)


def search_page(client: Client, build_id: str, search_state: dict, page: int) -> dict:
    ss = quote(json.dumps(search_state, separators=(",", ":")))
    url = f"{BASE}/_next/data/{build_id}/index.json?searchState={ss}&page={page}"
    data = client.get(url).json()
    return data.get("pageProps", {})


def fetch_job_detail(client: Client, build_id: str, requisition_id: str):
    """Returns (job_dict_or_None, canonical_hiringcafe_url_or_empty)."""
    url = f"{BASE}/_next/data/{build_id}/job/x-{quote(requisition_id)}.json"
    props = client.get(url).json().get("pageProps", {})
    canonical_url = ""
    if "__N_REDIRECT" in props:  # canonical slug redirect
        path = props["__N_REDIRECT"]              # e.g. /job/title-company-city-<id>
        canonical_url = BASE + path
        slug = path.split("/job/", 1)[-1]
        url = f"{BASE}/_next/data/{build_id}/job/{quote(slug)}.json"
        props = client.get(url).json().get("pageProps", {})
    return props.get("job"), canonical_url


# ------------------------------------------------------------------ flattening
def _join(val):
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    return val if val is not None else ""


def flatten(hit: dict, detail: dict | None, canonical_url: str) -> dict:
    v5 = (detail or hit).get("v5_processed_job_data") or hit.get("v5_processed_job_data") or {}
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
def parse_search_state(args) -> dict:
    if args.url:
        qs = parse_qs(urlparse(args.url).query)
        if "searchState" in qs:
            return json.loads(qs["searchState"][0])
        print("WARNING: no searchState in --url; scraping default feed.", file=sys.stderr)
        return {}
    if args.search_state:
        return json.loads(args.search_state)
    if args.query:
        return {"searchQuery": args.query}
    return {}


def main():
    ap = argparse.ArgumentParser(description="Scrape job listings from hiringcafe.com")
    ap.add_argument("--query", help='keyword search, e.g. "software engineer"')
    ap.add_argument("--url", help="a hiringcafe.com URL with searchState (set filters in the UI, copy the URL)")
    ap.add_argument("--search-state", help="raw searchState JSON string")
    ap.add_argument("--max-jobs", type=int, default=40)
    ap.add_argument("--max-pages", type=int, default=25)
    ap.add_argument("--delay", type=float, default=1.0, help="seconds between requests (default 1.0)")
    ap.add_argument("--out", default="hiringcafe_jobs.csv")
    ap.add_argument("--jsonl", help="also dump raw job JSON to this file")
    ap.add_argument("--no-descriptions", action="store_true",
                    help="skip per-job detail fetches (much faster, no description text)")
    args = ap.parse_args()

    if args.delay < 0.5:
        print("Refusing delay < 0.5s — be polite to their servers.", file=sys.stderr)
        args.delay = 0.5

    search_state = parse_search_state(args)
    client = Client(args.delay)

    print("Bootstrapping buildId...")
    build_id = get_build_id(client)
    print(f"  buildId = {build_id}")

    rows, seen = [], set()
    raw_dump = open(args.jsonl, "w", encoding="utf-8") if args.jsonl else None
    page = 0
    try:
        while len(rows) < args.max_jobs and page < args.max_pages:
            print(f"Search page {page}...")
            try:
                props = search_page(client, build_id, search_state, page)
            except StaleBuildId:
                print("  buildId went stale (site redeployed); re-bootstrapping...")
                build_id = get_build_id(client)
                props = search_page(client, build_id, search_state, page)

            hits = props.get("ssrHits") or []
            if page == 0:
                print(f"  {props.get('ssrTotalCount', '?')} total jobs match")
            new_hits = [h for h in hits
                        if not h.get("is_hc_pinned")
                        and h.get("requisition_id")
                        and h.get("id") not in seen]
            print(f"  {len(hits)} hits, {len(new_hits)} new")

            for hit in new_hits:
                if len(rows) >= args.max_jobs:
                    break
                seen.add(hit.get("id"))
                detail, canonical_url = None, ""
                if not args.no_descriptions:
                    try:
                        detail, canonical_url = fetch_job_detail(
                            client, build_id, hit["requisition_id"])
                    except StaleBuildId:
                        build_id = get_build_id(client)
                        detail, canonical_url = fetch_job_detail(
                            client, build_id, hit["requisition_id"])
                    except Exception as e:
                        print(f"  ! detail fetch failed for {hit.get('id')}: {e}",
                              file=sys.stderr)
                row = flatten(hit, detail, canonical_url)
                rows.append(row)
                if raw_dump:
                    raw_dump.write(json.dumps(
                        {"hit": hit, "detail": detail}, ensure_ascii=False) + "\n")
                print(f"  [{len(rows)}/{args.max_jobs}] {row['title']} @ {row['company']}"
                      f" (desc: {len(row['description_text'])} chars)")

            if props.get("ssrIsLastPage") or not new_hits:
                print("  last page reached.")
                break
            page += 1
    except KeyboardInterrupt:
        print("\nInterrupted — writing what we have...")
    finally:
        if raw_dump:
            raw_dump.close()

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    with_desc = sum(1 for r in rows if len(r["description_text"]) > 100)
    print(f"\nDone: {len(rows)} jobs -> {args.out}"
          f" ({with_desc} with full descriptions)")


if __name__ == "__main__":
    main()
