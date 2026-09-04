# Golden test fixtures

Miniature corpus for `tests/golden.rs` and `tests/test_ingest_and_benchmark.py`.
Nine `*.json.gz` documents — seven valid, two named `invalid_*` that must fail
validation — each paired with a `*.golden.json` snapshot of its flattened row.

## Provenance

Five are real documents from `data/raw/json/`, chosen to cover schema shapes
that are otherwise untested, with the Firebase user-activity UIDs in
`job_information` replaced by synthetic `fixtureuid…` values. **No real user
identifiers are committed here.**

| fixture | covers |
|---|---|
| `aliases_no_geoloc` | `401k_matching` and `bi-weekly_*_compensation` aliases; absent `_geoloc` |
| `board_token_int` | `board_token` as a JSON number (the `str \| int` union) |
| `original_source_id_int` | `original_source_id` as a JSON number |
| `no_enrichment` | `enriched_company_data` absent entirely |
| `enriched_with_geoloc` | enrichment + geo point + populated user arrays |

Four are hand-edited from `enriched_with_geoloc`, for shapes the corpus does
not contain:

| fixture | covers |
|---|---|
| `ssp_marker` | `__N_SSP` instead of `__N_SSG` |
| `explicit_null_lists` | list fields as explicit JSON `null` (must normalize to `[]`) |
| `invalid_no_marker` | neither marker present — must be rejected |
| `invalid_missing_required_field` | `source_and_board_token` removed — must be rejected |

Each derived fixture was given its own `requisition_id`, `id`, `collapse_key`,
and `objectID`. They must stay distinct: `requisition_id` is the primary key in
both databases, so fixtures sharing one would silently collapse under
`INSERT OR REPLACE` and make the end-to-end row counts wrong.

## Regenerating the snapshots

After an intentional schema change:

```
just bless          # or: BLESS=1 cargo test --test golden
```

Then read the `git diff` carefully. A change in blob key order is exactly the
regression these snapshots exist to catch — the JSON blob columns are stored as
text, so reordering a struct field in `schema.rs` silently rewrites every row in
the database with no error anywhere.

The `.json.gz` files themselves are written with `mtime=0` so regenerating them
from the same input is byte-reproducible.
