//! Golden tests for the schema + flatten pipeline.
//!
//! `schema.rs` is the canonical description of the input JSON, and nothing
//! else checks it — so these fixtures pin the parts that fail *silently*:
//! blob key order (a reordered struct field changes
//! `job_information_json` / `v5_processed_job_data_json` with no error
//! anywhere), the source field aliases, and the null-list normalization.
//!
//! Fixtures in `tests/fixtures/` are real corpus documents with the
//! Firebase user-activity UIDs replaced by synthetic `fixtureuid…` values,
//! plus a few hand-edited files for shapes the corpus doesn't contain.
//!
//! To re-bless the snapshots after an intentional schema change:
//!     BLESS=1 cargo test --test golden
//! then read the resulting `git diff` carefully — a change to blob key order
//! is exactly the bug this file exists to catch.

use std::path::{Path, PathBuf};

use fastingest::schema::JobPage;
use fastingest::{decode_gzipped, flatten::flatten};

/// `Result::expect_err` requires `T: Debug`, and deriving Debug across the
/// whole schema just for tests isn't worth it.
fn expect_err(result: Result<JobPage, String>, msg: &str) -> String {
    match result {
        Ok(_) => panic!("{msg}"),
        Err(e) => e,
    }
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read_fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(format!("{name}.json.gz"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// Top-level keys of a JSON object in *document* order.
///
/// `serde_json::Value` stores objects in a BTreeMap, so round-tripping
/// through it sorts the keys and destroys exactly the property under test.
fn top_level_keys(json: &str) -> Vec<&str> {
    let b = json.as_bytes();
    let mut keys = Vec::new();
    let (mut depth, mut in_string, mut escaped, mut start) = (0usize, false, false, 0usize);
    for i in 0..b.len() {
        let c = b[i];
        if in_string {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_string = false;
                // A string closing at depth 1 is a key iff ':' follows it.
                if depth == 1 {
                    let rest = json[i + 1..].trim_start();
                    if rest.starts_with(':') {
                        keys.push(&json[start + 1..i]);
                    }
                }
            }
        } else {
            match c {
                b'"' => {
                    in_string = true;
                    start = i;
                }
                b'{' | b'[' => depth += 1,
                b'}' | b']' => depth -= 1,
                _ => {}
            }
        }
    }
    keys
}

/// Parse + flatten a fixture and compare its row against the checked-in
/// snapshot (or rewrite the snapshot when BLESS=1).
fn assert_golden(name: &str) {
    let page = decode_gzipped(&read_fixture(name))
        .unwrap_or_else(|e| panic!("{name} should parse, but: {e}"));
    let row = flatten(page);
    let actual = serde_json::to_string_pretty(&row).expect("FlatRow serializes");

    let snapshot = fixture_dir().join(format!("{name}.golden.json"));
    if std::env::var_os("BLESS").is_some() {
        std::fs::write(&snapshot, &actual)
            .unwrap_or_else(|e| panic!("writing {}: {e}", snapshot.display()));
        return;
    }
    let expected = std::fs::read_to_string(&snapshot).unwrap_or_else(|e| {
        panic!(
            "reading {}: {e}\nRun `BLESS=1 cargo test --test golden` to create it.",
            snapshot.display()
        )
    });
    // Normalize line endings: the snapshots are checked out with CRLF on
    // Windows depending on core.autocrlf.
    assert_eq!(
        expected.replace("\r\n", "\n"),
        actual.replace("\r\n", "\n"),
        "flattened row for {name} changed; re-bless only if intended",
    );
}

#[test]
fn aliases_no_geoloc() {
    // `401k_matching` and `bi-weekly_*_compensation` aliases; no _geoloc, so
    // latitude/longitude must be null.
    assert_golden("aliases_no_geoloc");
}

#[test]
fn board_token_int() {
    // board_token arrives as a JSON number here; the flat column is TEXT.
    assert_golden("board_token_int");
}

#[test]
fn original_source_id_int() {
    assert_golden("original_source_id_int");
}

#[test]
fn no_enrichment() {
    // enriched_company_data absent -> enriched_* columns and the blob null.
    assert_golden("no_enrichment");
}

#[test]
fn enriched_with_geoloc() {
    assert_golden("enriched_with_geoloc");
}

#[test]
fn ssp_marker_is_accepted() {
    // Newer exports carry __N_SSP instead of __N_SSG; both are valid.
    assert_golden("ssp_marker");
}

#[test]
fn explicit_null_lists_become_empty() {
    // Several list fields arrive as explicit JSON null; they must normalize to
    // [] so the JSON-array text columns are never the string "null".
    assert_golden("explicit_null_lists");

    let row = flatten(decode_gzipped(&read_fixture("explicit_null_lists")).expect("parses"));
    assert_eq!(row.technical_tools, "[]");
    assert_eq!(row.workplace_countries, "[]");
    assert!(
        row.job_information_json.contains(r#""viewedByUsers":[]"#),
        "explicit null should serialize as []: {}",
        row.job_information_json,
    );
}

#[test]
fn description_is_excluded_from_the_job_information_blob() {
    // `description` has its own column; duplicating it into the blob would
    // roughly double the largest column in the database.
    let row = flatten(decode_gzipped(&read_fixture("enriched_with_geoloc")).expect("parses"));
    assert!(
        !row.description.is_empty(),
        "fixture should have a description"
    );
    assert!(
        !top_level_keys(&row.job_information_json).contains(&"description"),
        "description must not appear in job_information_json",
    );
}

#[test]
fn blob_key_order_is_stable() {
    // The blobs are stored as text, so key order is part of the output and a
    // reordered struct field would silently change every stored row.
    let row = flatten(decode_gzipped(&read_fixture("enriched_with_geoloc")).expect("parses"));
    assert_eq!(
        top_level_keys(&row.job_information_json),
        [
            "title",
            "job_title_raw",
            "viewedByUsers",
            "savedFromUsers",
            "hiddenFromUsers",
            "appliedFromUsers",
        ],
    );
    // Spot-check the head and tail of the ~90-field v5 blob, including the
    // two renamed fields.
    let v5 = top_level_keys(&row.v5_processed_job_data_json);
    assert_eq!(v5.first(), Some(&"core_job_title"));
    assert!(v5.contains(&"401k_matching"), "alias lost: {v5:?}");
    assert!(
        v5.contains(&"bi-weekly_min_compensation"),
        "alias lost: {v5:?}"
    );
}

#[test]
fn missing_static_generation_marker_is_rejected() {
    let err = expect_err(
        decode_gzipped(&read_fixture("invalid_no_marker")),
        "a document with neither __N_SSG nor __N_SSP must fail",
    );
    assert!(
        err.starts_with("parse/validate:"),
        "unexpected error: {err}"
    );
}

#[test]
fn missing_required_field_is_rejected() {
    // source_and_board_token is never read by flatten(), but dropping it from
    // the schema would silently widen what counts as a valid document.
    let err = expect_err(
        decode_gzipped(&read_fixture("invalid_missing_required_field")),
        "a document missing source_and_board_token must fail",
    );
    assert!(
        err.contains("source_and_board_token"),
        "unexpected error: {err}",
    );
}

#[test]
fn corrupt_gzip_is_reported_not_panicked() {
    let err = expect_err(decode_gzipped(b"not actually gzip"), "must fail");
    assert!(err.starts_with("gunzip:"), "unexpected error: {err}");
}
