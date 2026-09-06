# HiringCafe rate limiting: findings and fix

Investigated: 2026-09-04

## Conclusion

hiringcafe.com enforces a hard, request-volume-based rate limit on top of its
Cloudflare fingerprint defenses, independent of them. A fixed request cadence
plus per-request retry backoff (the behavior before this change) is not enough
to survive it on a run of any real size: sustained 429s caused permanent,
silent data loss (empty job descriptions) once the default retry budget was
exhausted. Adaptive pacing that widens the delay between requests in response
to 429/5xx, plus a higher retry ceiling on job-detail fetches specifically,
eliminated the data loss and roughly a third'd the total 429 count on a
same-sized live run.

Jitter (randomizing request spacing) was considered first and ruled out: it
addresses fingerprint/pattern-based bot detection, not volume-based quotas,
and the live evidence pointed at the latter.

## Investigation

### The 403 red herring: Cloudflare fingerprint detection

An initial bounded test used plain `curl` (no TLS impersonation) against the
homepage: 35 requests, several `403 Forbidden` responses with `Cf-Mitigated:
challenge` and a `Just a moment...` interstitial body, scattered
non-deterministically through the burst (positions 3, 4, 6, 10, 11, 20 of 35 —
not a clean threshold). That pattern is consistent with TLS/JA3 fingerprint
scoring, not a request-count limiter, and is exactly what `scrape_hiringcafe.py`'s
`curl_cffi` Chrome impersonation is already built to defeat. No `429`,
`Retry-After`, or `X-RateLimit-*` ever appeared in this test.

This did not reproduce the actual problem. It only ruled out one hypothesis
(fingerprint detection) as the dominant cause of 429s seen in real scraper
usage, and initially suggested jitter as a plausible next step for exactly that
kind of pattern-based defense.

### The real signal: a live scrape at realistic volume

A `job-scrape --preset Board_DS_SF_Remote --max-jobs 150 --max-pages 5` run
(using the project's own `curl_cffi` Chrome impersonation, not raw `curl`)
told a different story:

- The first **28 job-detail requests succeeded cleanly** at the default fixed
  1.0s pace.
- From request **29 onward, effectively every request hit 429**, and the run
  never recovered a fully clean steady state through job 136 (the rest of the
  bounded run).
- 222 total `HTTP 429` responses logged.
- **7 jobs permanently lost their description** (`desc: 0 chars`) after
  exhausting the then-default 3 retries per detail fetch — silent data loss,
  since the scrape continues past a failed detail fetch by design.
- Recovery was real but partial: two jobs mid-run (in the saturated stretch)
  succeeded with zero backoff, so the block was leaky rather than absolute.

A clean, repeatable trip point at a fixed request count, with severity
tracking cumulative volume rather than timing pattern, points at a
request-count/window-based quota — not a fingerprint or pattern-based defense.
Jitter changes *when* within a window a request lands, not *how many* land in
it, so it would not have addressed this.

No `Retry-After` or other rate-limit header was ever observed on a 429 in this
investigation, so there was nothing for the client to read and honor — the fix
had to be pacing behavior, not header compliance.

## Fix

Implemented in `src/job_ingest/scrape_hiringcafe.py`:

1. **Adaptive pacing** (`Client._delay_multiplier`): the inter-request delay
   used for pacing (`Client.get`'s `wait` calculation) is `self.delay *
   self._delay_multiplier`, not `self.delay` alone. On every 429/5xx, the
   multiplier grows 2x, capped at 10x; on every clean response, it decays 0.8x
   back toward 1.0x. This means the *steady-state* pace itself slows down
   under sustained rate limiting, on top of (not instead of) the existing
   per-request exponential backoff (`5 * 2**attempt`) on retries within one
   call.
2. **Higher retry ceiling for job-detail fetches**: `DETAIL_FETCH_RETRIES = 6`
   (up from the default 3), applied to both `client.get()` calls in
   `fetch_job_detail`. A page/build-id fetch failing after its retries aborts
   the whole run loudly; a detail fetch failing only drops one job's
   description silently mid-run, so it gets more headroom before conceding.

## Verification

Two live runs against the same `Board_DS_SF_Remote` preset, same shape
(`--max-jobs 150 --max-pages 5-8 --raw-dir data/raw/json
--skip-existing-raw`), before and after the fix:

| | Before | After |
| --- | --- | --- |
| Total `HTTP 429` responses | 222 | 64 |
| Permanent detail-fetch failures (lost description) | 7 | 0 |
| Jobs completed with full description | 129 / 136 | 136 / 136 |

The adaptive multiplier engaged 63 times in the "after" run, settling the pace
around 7-9s between requests (from a 1.0s floor) as load accumulated, and most
individual 429s cleared after a single `backing off 5s` retry instead of the
2-3 rounds seen before. All 178 existing tests pass; `ruff` and `mypy` are
clean.

## Deliberately out of scope

- No test coverage was added for the adaptive-pacing behavior itself
  (`_delay_multiplier` growth/decay). The existing `test_transport_backoff`
  parametrized test continues to pass unchanged because it only asserts on
  `_sleep` calls, which the multiplier does not add to. Left untested per
  discussion; revisit if the multiplier's tuning constants need to change.
- Reading and honoring a `Retry-After` header was not implemented, since none
  was ever observed on a live 429 from this host.
- No attempt was made to determine the exact request-volume quota (requests
  per minute/hour, or per-IP vs per-session) — the adaptive multiplier reacts
  to observed 429s rather than modeling the underlying limiter.
