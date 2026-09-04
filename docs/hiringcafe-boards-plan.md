# HiringCafe Boards support plan

## Goal

Extend `scrape_hiringcafe.py` so a HiringCafe Board URL such as
`https://hiringcafe.com/b/healthcare-9ierbt6f` is a first-class scrape source.
Ship the four Boards in `docs/saved_hiringcafe_searches.md` as CLI and Textual
presets, and treat a date- and Board-specific directory of compressed JSON as
both the archive and the **cache** for a run:

```text
data/interim/2026/09/04/healthcare-9ierbt6f/
  page0.json.gz
  page1.json.gz
  ...
```

Along the way, replace the `requests` transport with `curl_cffi` so the scraper
survives TLS fingerprinting.

This is additive to behaviour. Existing search-state URLs, queries, raw
search-state JSON, search presets, JSONL output, detail fetching, cancellation,
and optional ingest-compatible per-job output in `--raw-dir` must keep working.

## What the existing code and the archived samples tell us

Verified 2026-09-04 against `job-search/data/interim/2026/04/20/*/page0.json.gz`
and `job-search/job_search/scrape.py`.

- Search result pages use
  `/_next/data/{build_id}/index.json?searchState=...&page=N` and expose
  `pageProps.ssrHits`, `ssrTotalCount`, and `ssrIsLastPage`.
- Board pages use `/_next/data/{build_id}/b/{board_slug}.json?page=N`. The
  archived envelope is `{"pageProps": {...}, "__N_SSP": true}` — note `__N_SSP`,
  *not* the `__N_SSG` that `write_raw_page` emits for detail pages.
- `pageProps` for a Board carries exactly:
  `archived`, `board`, `hits`, `page`, `totalCount`, `companyCount`, `pageSize`,
  `isLastPage`, `ssrError`. A sample page 0 had **106 hits with
  `pageSize` 40** against `totalCount` 3493, so neither `pageSize` nor the
  search page size can be used to predict or validate a Board page's length.
- **Board hits do not carry `is_hc_pinned`** (it is absent/None on every sampled
  hit). Board pinning lives on `pageProps.board.pinned_job_ids`, and those jobs
  are pinned *by the Board's owner* — they are the curated ones, so they must
  not be filtered out. The existing `not h.get("is_hc_pinned")` filter is
  therefore a harmless no-op for Boards, and should be documented as a
  search-only concept rather than described as "carried over".
- Board hits do carry `id` and `requisition_id`, plus the same
  `job_information` / `v5_processed_job_data` blocks a search hit has, so
  dedup, `job_summary`, `_merge_identity`, and the detail-fetch path all work
  unchanged.
- `pageProps.board` also carries `name`, `tagline`, the Board's own
  `search_state` (as a JSON string), `owner_uid`, `owner_display_name`, and
  view/subscriber counts. Two consequences: a Board's real filters are
  knowable after page 0 (useful for the dashboard summary), and the archive
  contains the Board owner's identity. `data/` is gitignored, so this stays
  local; it is the same accepted "store verbatim" tradeoff `write_raw_page`
  already documents.
- The reference `load_board` in `job-search` **already treats an existing
  `pageN.json.gz` as a cache** (`if P_jobs_json.exists() and not overwrite`),
  and drives paging off the cached envelope's `isLastPage`. That corpus also
  contains pages captured through Selenium, whose top level is
  `{"props": {"pageProps": ...}}` — the reference reads
  `jobs_dict.get('props', jobs_dict)['pageProps']` to cope. Our reader should
  do the same so an interim tree copied over from `job-search` is usable.
- The reference reached for `botasaurus_requests` (a TLS-impersonating client),
  rotating user agents, webshare proxies, and a Selenium fallback. Blocking is
  a real, previously encountered failure mode on this host, not a hypothetical.
- `ScrapeConfig` currently carries only a decoded `search_state`; source
  selection must be generalized before a Board can reach the core generator
  cleanly.
- `--raw-dir` has a different contract: it stores one validated, ingest-ready
  detail page per job. Board page archives must not overload that option or
  directory.
- Three documented Board names collide with existing search preset keys. The
  current preset registry and Textual `Select` require unique values.

## Proposed user-facing contract

### Inputs and preset names

- Keep the four existing search preset keys unchanged for compatibility:
  `DS_SF_Remote`, `DA_SF_Remote`, `DS_Healthcare`, and `DA_Healthcare`.
- Add four unambiguous Board preset keys:
  `Board_DS_SF_Remote`, `Board_DA_SF_Remote`, `Board_DA_Healthcare`, and
  `Board_Healthcare`. Their labels in Textual should start with `Board -`; the
  existing labels should start with `Search -`.
- Continue using `--preset KEY` rather than introduce a second preset flag.
- Make `--url` recognize both supported URL shapes:
  `https://hiringcafe.com/b/{board_slug}` selects a Board, while a URL with a
  `searchState` query selects a search. Reject malformed Board paths, foreign
  hosts, unexpected extra path segments, and ambiguous combinations with the
  other input modes.
- A query, raw `--search-state`, or no input continues to select the search
  endpoint. Existing default-feed behavior remains unchanged.

Example commands after the change:

```text
job-scrape --preset Board_Healthcare --max-pages 10 --max-jobs 500
job-scrape --preset Board_Healthcare --refresh          # ignore today's cache
job-scrape --url https://hiringcafe.com/b/healthcare-9ierbt6f
job-scrape --preset DS_SF_Remote --raw-dir data/raw/_json --skip-existing-raw
```

### Board page archives, as a cache

- Add a Board archive root setting, defaulting to `data/interim`, exposed as
  `--interim-dir` in the CLI and dashboard launcher.
- For a Board source, resolve the run directory **once** at scrape startup as
  `<interim-dir>/<local YYYY>/<MM>/<DD>/<board_slug>`. One resolved date keeps
  a scrape that crosses midnight from splitting itself, and keeps cache lookups
  and writes agreeing on where "today" is.
- Before requesting page N, if `page{N}.json.gz` exists under that directory and
  `--refresh` was not given, **read it instead of making a request**. A cache
  hit participates fully in the run: its hits are traversed, dedup applies, and
  its `isLastPage` decides whether paging continues.
- A cache hit charges no politeness delay (`Client._last_request` is untouched),
  so an entirely cached Board re-run makes **zero** page requests.
- An unreadable cache entry — bad gzip, bad JSON, a non-object body, a missing
  or non-object `pageProps` — yields one `warning` event and is treated as a
  miss: refetch and overwrite. A truncated or hand-copied file must never brick
  a run.
- The cache reader accepts both `{"pageProps": ...}` and
  `{"props": {"pageProps": ...}}`, so an interim tree copied from the
  `job-search` corpus is usable. The writer always emits the flat `_next/data`
  envelope, verbatim.
- On a miss, write the fetched response to `page{N}.json.gz` immediately,
  retaining the complete top-level JSON envelope. Use deterministic gzip
  metadata (`mtime=0`) and a same-directory temp file followed by `os.replace`,
  matching the safety properties of `write_raw_page`. `os.replace` makes a
  reader's view atomic, so a cache hit never sees a half-written page.
- `--refresh` forces a network fetch for every page and replaces same-numbered
  files atomically. With or without it, **never delete higher-numbered files**:
  a deliberately bounded or cancelled run must not destroy an earlier, more
  complete snapshot.
- The date directory *is* the TTL. There is no cross-day lookback: yesterday's
  Board is a different snapshot, not a cache hit.
- Accepted consequence: one day's directory can mix a cached page 0 with pages
  fetched hours later. Dedup is by job `id` within a run, so this is safe; it
  only means page 0's `totalCount` may disagree with later pages. Use
  `--refresh` when a single coherent snapshot matters.
- Archive the fetched page before iterating its hits, so an interrupt during
  detail fetching still leaves the Board page that was already received — and
  makes the resumed run a cache hit.
- Board archiving/caching is automatic for Board sources and independent of
  `--raw-dir`, `--jsonl`, and `--no-descriptions`. `--raw-dir` may still be
  enabled to additionally save per-job ingest pages.
- Search sources do not write or read paged interim archives in this change.

### Skipping jobs already staged in `--raw-dir`

Board pages are roughly one request per hundred jobs; the real cost of a re-run
is the one-or-two detail requests per job. Caching pages alone saves ~1% of a
re-run, so pair it with:

- `--skip-existing-raw` (requires `--raw-dir`): before the detail fetch, compute
  `raw_filename(hit["requisition_id"])` and skip the job entirely if that file
  already exists in the raw dir.
- Reject `--skip-existing-raw` without `--raw-dir`, and (transitively) with
  `--no-descriptions`, in the same eager-validation block that already rejects
  `--raw-dir` plus `--no-descriptions`.
- A skipped job does **not** count against `max_jobs`: the budget should be
  spent on jobs that are not already staged.
- Report skips as one per-page count (`  12 already staged, skipped`), not one
  event per job, so the log stays readable.
- **Do not let the raw-skip filter feed the "no new hits" stop.** The existing
  loop breaks on `not new_hits`; if every hit on a page is already staged, the
  post-skip list is empty and the run would stop early — exactly defeating the
  flag. Keep the defensive stop keyed on the dedup-filtered list and apply the
  raw-skip filter separately.
- Documented tradeoff: a skipped job keeps its previously stored description and
  `is_expired`, and its file mtime is untouched, so incremental ingest does not
  re-trigger for it. Drop the flag to refresh the corpus.

## Design and implementation

### 0. Replace the transport with `curl_cffi`

Land this **before** the Boards work, as its own commit: if the current
transport is being fingerprint-blocked, nothing else in this plan can be
verified live.

- Add `curl_cffi` to `[project] dependencies` and **remove `requests`**.
  `Client` is the only consumer in `src/`, and the tests stub `Client` rather
  than `requests`, so one transport and one exception hierarchy is enough.
  Pin whatever `uv add curl_cffi` resolves, and check at implementation time
  which exception module that version exposes (`curl_cffi.requests.exceptions`
  mirrors `requests`' hierarchy in recent releases; older ones only offer
  `RequestsError`). Map whatever is there onto the existing `ScrapeError`
  behaviour — the retry/backoff semantics in `Client.get` must not change.
- Build the session as `curl_requests.Session(impersonate=...)` so the TLS/JA3
  handshake and HTTP/2 settings match a real Chrome.
- **Honest by default, impersonation configurable.** Keep the existing
  `USER_AGENT` (which carries a contact address) as the default and let the run
  override it:
  - `--impersonate PROFILE` (default `chrome`, the floating alias that tracks
    the newest profile the installed `curl_cffi` supports; `none` disables
    impersonation). Prefer the alias over a pinned `chrome124`-style profile,
    which rots.
  - `--user-agent TEXT` (default the JobScout string). Passing an empty string
    means "use whatever the impersonation profile sends", i.e. full Chrome
    impersonation.
  - Both surface on `Client.__init__` as keyword arguments so the dashboard and
    the tests can set them without going through Typer.
  - Note in the help and the module docstring that the default combination is a
    Chrome handshake with an honest, attributable UA. Most WAFs gate on JA3/JA4
    before reading the UA, so this usually passes; it is also a detectable
    mismatch, which is why the full-impersonation escape hatch exists.
- Send the headers a real Next.js client navigation sends, but only for data
  routes — decide inside `Client.get` on a `/_next/data/` path prefix, so no
  call site changes and `get_build_id`'s HTML fetch is unaffected:
  `Accept: */*`, `x-nextjs-data: 1`, and a same-origin `Referer`. This is at
  least as likely to matter as the TLS layer.
- Change the 403 handling. Today 403 shares the 429/5xx backoff ladder and
  burns ~15s over three attempts. A fingerprint rejection is deterministic, so
  fail fast (at most one retry) with a message naming the active impersonation
  profile and pointing at `--impersonate`/`--user-agent`. Keep 429 and 5xx on
  the existing ladder. If a 503 arrives with a `cf-mitigated` header, say so.
- Define a small `HttpResponse` `Protocol` (`status_code: int`, `text: str`,
  `url: str`, `json() -> Any`, `raise_for_status() -> None`) and annotate
  `Client.get` and `_json_body` against it. That keeps strict mypy clean
  regardless of whether the resolved `curl_cffi` ships `py.typed`, and keeps
  test stubs free of any transport import. Add a
  `[[tool.mypy.overrides]] ignore_missing_imports` entry only if it turns out to
  be needed.
- Update the module docstring's `Requires: pip install requests typer` line and
  the `requests.get` reference in the comment at `dashboard.py:46`.
- Escalation path, if impersonation alone is insufficient: rotating UAs and
  proxies, as in `job-search`. Out of scope here; note it, do not build it.

### 1. Model scrape sources explicitly

In `src/job_ingest/scrape_hiringcafe.py`, introduce immutable source values,
for example `SearchSource(search_state)` and `BoardSource(slug)`, and make
`ScrapeConfig.source` their union. This replaces the assumption that every run
has `ScrapeConfig.search_state`.

Replace `resolve_search_state` at the CLI/dashboard boundary with a resolver
that returns a source. Keep `parse_search_state` as the focused decoder for
search inputs. Validate Board slugs with a conservative lowercase
letters/digits/hyphen pattern and extract them from URLs with `urlparse` rather
than string splitting.

Generalize the saved registry to include a source kind and exact URL while
retaining helpers that keep call sites readable. The packaged registry remains
the runtime source of truth; the Markdown file remains human-readable
provenance and should not be read at runtime.

### 2. Add the Board transport, cache reader, and page writer

Add a `board_page(client, build_id, slug, page)` transport beside
`search_page`. It should request:

```text
https://hiringcafe.com/_next/data/{build_id}/b/{slug}.json?page={page}
```

Return both the validated full response object for archiving and its
`pageProps` object for traversal. Treat a missing/non-object `pageProps`, a
non-list `hits`, invalid paging metadata, a truthy `ssrError`, or a `page` field
that does not echo the requested page as `ScrapeError`; do not silently turn
protocol drift into an empty successful scrape. A truthy `archived` should
produce a warning, not an error — an archived Board can still return hits.

Add two helpers with a single shared notion of the page path:

- `board_page_path(interim_dir, run_date, slug, page) -> Path`.
- `read_board_page(path) -> dict | None` — gzip+JSON read, tolerant of the
  `{"props": {...}}` wrapper, returning `None` (never raising) on any
  unreadable or unrecognisable content so the caller can treat it as a miss.
- `write_board_page(payload, path)` using the same atomic gzip pattern as
  `write_raw_page`. Wrap serialization, directory, temp file, and replacement
  failures in `ScrapeError`, and ensure failed writes leave no `.part` files.

Because a fully cached run may need no network at all, **bootstrap the build ID
lazily** — on the first request that actually needs it — rather than eagerly at
the top of `_scrape`. Keep emitting the `build_id` events when it does happen.

### 3. Share traversal while preserving endpoint differences

Refactor `_scrape` so source-specific page loading normalizes only the fields
needed by the common job loop:

| Meaning | Search page | Board page |
| --- | --- | --- |
| Hits | `ssrHits` | `hits` |
| Total | `ssrTotalCount` | `totalCount` |
| Last page | `ssrIsLastPage` | `isLastPage` |
| Pinned | `hit.is_hc_pinned`, filtered out | absent; `board.pinned_job_ids`, kept |
| Cache | none | read `pageN.json.gz` before requesting |
| Archive | none | full response to `pageN.json.gz` |

Keep the current duplicate filtering, `max_jobs`, `max_pages`, detail requests,
raw per-job writes, warning events, and done event semantics. Retry the same
Board page after refreshing a stale build ID, just as search pages do. Stop on
the endpoint's last-page flag; retain the defensive empty-new-hits stop so a
changed or repeating endpoint cannot loop — subject to the raw-skip caveat in
the contract above.

Give page events source-aware messages, and say where the page came from:

```text
Board healthcare-9ierbt6f page 0 (cached)...
Board healthcare-9ierbt6f page 1 (fetched -> .../2026/09/04/healthcare-9ierbt6f/page1.json.gz)...
```

After page 0 of a Board, log `board.name` and `board.tagline` if present — that
works from the cache offline too, and tells the user which Board they are
actually reading. The progress bar can remain job-based because both sources
honor `max_jobs`.

### 4. Wire the CLI

Update Typer help and the module docstring to describe Board URLs, the four
Board preset keys, and the cache. Add:

- `--interim-dir PATH` (default `data/interim`).
- `--refresh` (ignore today's cached Board pages and refetch).
- `--skip-existing-raw` (requires `--raw-dir`).
- `--impersonate PROFILE` and `--user-agent TEXT` from step 0.

Resolve the source once, log only the stable preset key/summary rather than the
raw saved URL, and print the resolved Board archive directory before network
work begins.

Keep the current mutual-exclusion contract across `--preset`, `--query`,
`--url`, and `--search-state`. Keep the minimum-delay guard and error/partial
result behavior unchanged.

### 5. Wire the Textual app

In `src/job_ingest/dashboard.py`:

- Populate the existing selector with all eight packaged presets plus Custom,
  using distinct Search/Board labels and unique values.
- Make preset summaries source-aware. Show `SHARED_FILTERS` only for search
  presets; for a Board explain that filters are owned by the saved Board and
  show the Board slug and its cache/archive behavior.
- Let Custom URL accept either a Board or search URL. Query and raw state remain
  search-only inputs.
- Build `ScrapeConfig` from the resolved source and pass the dashboard's
  interim root into it.
- Add `--interim-dir` to `job-dash`, store it on `Dashboard`, and include the
  resolved archive root in the paths panel.
- Add two checkboxes beside the existing `#write-raw` one — "Refetch cached
  pages" (`--refresh`) and "Skip jobs already staged" (enabled only when
  `#write-raw` is on). The dashboard is the surface where re-runs actually
  happen, so this is where the cache controls earn their place. Both default
  off, which reproduces today's behaviour apart from the page cache itself.
- When starting a Board run, log its stable preset/slug and resolved dated
  archive path. Preserve the current one-operation-at-a-time and cooperative
  cancellation behavior.

No control is needed to turn Board archiving on: selecting a Board makes page
archiving and caching automatic.

### 6. Tests

Extend `tests/test_scrape_hiringcafe.py` with network-free coverage for:

- Board URL parsing, slug validation, host/path rejection, search URL parsing,
  default search behavior, and mutual exclusion.
- The exact eight stable preset keys and source kinds.
- A section-aware drift guard for both `# Boards` and `# Searches` in
  `docs/saved_hiringcafe_searches.md`. The current whole-file heading regex in
  `test_the_packaged_registry_matches_the_documented_source` is insufficient:
  three names repeat across the two sections and collapse when converted to a
  dict.
- A Board stub response using the real envelope — `{"pageProps": {...},
  "__N_SSP": true}` with `hits`, `board`, `totalCount`, `page`, `pageSize`,
  `isLastPage`, `archived`, `ssrError` — and hits *without* `is_hc_pinned`.
  Assert endpoint URLs, page count, event sequence, job limits, deduplication,
  stale-build retry, and termination.
- Protocol drift: truthy `ssrError`, a `page` that does not echo the request,
  non-list `hits`, and missing `pageProps` each raise `ScrapeError`.
- Atomic gzip round-trip of the complete Board response; deterministic content,
  page numbering, dated path construction, same-page replacement, cleanup on
  failure, and no page deletion on a bounded/cancelled rerun.
- Cache behaviour: a hit makes no request and still yields the page's jobs; a
  hit drives `isLastPage` paging; a corrupt/truncated file warns and refetches
  rather than raising; the `{"props": {"pageProps": ...}}` wrapper is accepted;
  `--refresh` refetches and overwrites; a fully cached run issues **zero**
  requests (assert against a stub client that fails on any call).
- `--skip-existing-raw`: an existing `{requisition_id}.json.gz` skips the detail
  fetch, skipped jobs do not consume `max_jobs`, a page whose hits are *all*
  already staged does not stop the run early, and the flag is rejected without
  `--raw-dir` or with `--no-descriptions`.
- Board page persistence when descriptions are disabled and when cancellation
  occurs after the page fetch.
- Lazy build-id bootstrap: a cached, description-free run never calls
  `get_build_id`.
- Continued search traversal and ingest-compatible `--raw-dir` behavior as
  regression tests.
- Transport: the honest UA is the default; `--user-agent ""` defers to the
  impersonation profile; `x-nextjs-data` is sent for `/_next/data/` URLs and not
  for the homepage; a 403 fails fast with a message naming the profile, while
  429/5xx still walk the backoff ladder. Drive these through a stubbed session,
  not a live request.

Extend `tests/test_dashboard.py` to assert:

- The selector contains eight presets plus Custom with unique values.
- Search and Board summaries/shared-filter visibility change correctly.
- Choosing a Board sends a `BoardSource` and interim root to the worker;
  choosing an existing search still sends the exact decoded state.
- The refresh and skip-existing checkboxes reach `ScrapeConfig`, and
  skip-existing is disabled while `#write-raw` is off.
- A custom Board URL works and input conflicts still fail before a worker is
  launched.

Use temporary directories and injected/fixed run dates in tests; never write to
the repository's real `data/interim` or make a live HiringCafe request.

### 7. Documentation

Update `README.md` with both pipelines and make their independence explicit:

```text
Board endpoint -> data/interim/YYYY/MM/DD/<board>/pageN.json.gz   (also the cache)
Job detail     -> --raw-dir/<requisition_id>.json.gz -> fastingest
```

Document Board URL examples, all preset keys, `--interim-dir`, `--refresh`,
`--skip-existing-raw`, the same-day cache semantics and their staleness
tradeoffs, the new `--impersonate`/`--user-agent` flags and what the default
combination is, and the fact that Board page archives are not direct input to
the current ingest sidecar.

## Implementation order

1. Swap the transport to `curl_cffi` with configurable impersonation, adjust
   403 handling, and land its tests. Separate commit — everything below depends
   on being able to reach the site at all.
2. Add source types, saved Board registry entries, URL resolution, and focused
   unit tests.
3. Add the Board transport, dated path builder, cache reader, atomic page
   writer, and tests.
4. Refactor the generator onto the source abstraction, make the build-id
   bootstrap lazy, and test both routes.
5. Add `--skip-existing-raw` and its tests.
6. Update CLI options/help and add CLI-level resolution tests.
7. Update the Textual selector, summaries, checkboxes, paths, config wiring, and
   headless tests.
8. Update README/module documentation and run the full repository gate.

Each step should leave existing search behavior green; the transport/writer can
land before the generator starts using it.

## Verification

- Run `just check` (Ruff formatting/lint, strict mypy, pytest, Rust formatting,
  clippy, and Rust tests).
- Confirm the transport swap on its own: one bounded live search run against an
  existing preset, before any Board code exists.
- Run one bounded live Board smoke test into a temporary interim root:
  `job-scrape --preset Board_Healthcare --max-pages 1 --max-jobs 2
  --interim-dir <temp>`.
- Decompress `page0.json.gz` and verify the full `__N_SSP` envelope, the `board`
  block, and Board metadata are present; confirm only two job-detail events
  occurred even though the archived page can contain more than a hundred hits.
- **Run the same command a second time** and confirm the page is served from the
  cache (the event says so, no page request is made), then run it with
  `--refresh` and confirm the file is refetched and replaced.
- Re-run with `--raw-dir <temp>/raw --skip-existing-raw` twice and confirm the
  second run skips the already-staged jobs and spends its `max_jobs` budget on
  new ones.
- Run one existing search preset smoke test to confirm its request still uses
  `index.json?searchState=...` and does not create an interim Board directory.
- Launch `job-dash --interim-dir <temp>`, select a Board preset, confirm the
  archive path is visible, run/cancel a bounded scrape, and verify the app stays
  responsive and the fetched page remains readable and reusable as cache.

## Deliberately out of scope

- Importing Board archive pages into `fastingest`; its current contract is one
  detail page per file.
- A second cache tier for job detail pages under the dated directory.
  `--skip-existing-raw` covers the re-run cost at a fraction of the complexity;
  revisit if descriptions need to be re-read offline.
- Cross-day cache lookback or any TTL other than the date directory itself.
- Archiving ordinary search result pages.
- Running all presets as a batch or deduplicating across separate preset runs.
- Historical timestamped subdirectories within a date. The proposed layout
  follows the existing `job-search` corpus and treats each Board/day directory
  as the latest per-page snapshot.
- Rotating user agents and proxy pools. Named as the escalation path if Chrome
  impersonation is not enough; not built here.
- Selenium fallback. The dynamic Next.js build ID and JSON route remain the
  supported transport.
