"""Tests for the Python wrapper around the fastingest sidecar.

The end-to-end tests reuse the Rust crate's golden fixtures
(`fastingest/tests/fixtures/`) as a miniature corpus, so they cover the part
the Rust tests can't: the subprocess contract, the Arrow handoff, and
whether DUCKDB_DDL still agrees with the sidecar's Arrow schema.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from job_ingest.ingest_and_benchmark import (
    DUCKDB_DDL,
    find_crate_dir,
    load_duckdb,
    run_fastingest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "fastingest" / "tests" / "fixtures"
# Fixtures named invalid_* are expected to fail validation.
VALID_FIXTURES = len(list(FIXTURES.glob("*.json.gz"))) - len(
    list(FIXTURES.glob("invalid_*.json.gz"))
)

needs_sidecar = pytest.mark.skipif(
    shutil.which("cargo") is None,
    reason="needs a Rust toolchain to build the fastingest sidecar",
)


class TestFindCrateDir:
    def test_finds_the_crate_from_the_source_layout(self) -> None:
        assert find_crate_dir() == REPO_ROOT / "fastingest"

    def test_env_var_takes_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FASTINGEST_DIR", "/somewhere/else")
        assert find_crate_dir() == Path("/somewhere/else")

    def test_returns_none_when_the_crate_is_absent(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """An installed copy with no crate alongside it must not guess a path."""
        monkeypatch.delenv("FASTINGEST_DIR", raising=False)
        stray = tmp_path / "site-packages" / "job_ingest" / "ingest_and_benchmark.py"
        stray.parent.mkdir(parents=True)
        stray.write_text("")
        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.__file__", str(stray), raising=True
        )
        assert find_crate_dir() is None


@needs_sidecar
class TestEndToEnd:
    def test_full_run_over_the_fixture_corpus(self, tmp_path: Path) -> None:
        table, stats = run_fastingest(
            FIXTURES, tmp_path, limit=None, full=True, parquet=False
        )

        assert stats["full"] is True
        assert stats["ok"] == VALID_FIXTURES
        assert stats["errors"] == 2, stats["error_samples"]
        assert table.num_rows == VALID_FIXTURES
        # The handoff file is transient and must not survive the call.
        assert not (tmp_path / "jobs.arrow").exists()
        assert (tmp_path / "jobs.sqlite").exists()
        assert (tmp_path / "ingest_manifest.json").exists()

    def test_duckdb_ddl_matches_the_sidecar_arrow_schema(self, tmp_path: Path) -> None:
        """load_duckdb inserts by name, so a column the DDL lacks fails loudly
        here rather than in a full ingest run."""
        table, _ = run_fastingest(
            FIXTURES, tmp_path, limit=None, full=True, parquet=False
        )
        insert_sec, total_rows = load_duckdb(table, tmp_path / "jobs.duckdb", full=True)

        assert total_rows == VALID_FIXTURES
        assert insert_sec >= 0
        ddl_columns = [
            line.strip().split()[0]
            for line in DUCKDB_DDL.splitlines()
            if line.startswith("    ")
        ]
        assert ddl_columns == list(table.column_names)

    def test_second_run_skips_unchanged_files(self, tmp_path: Path) -> None:
        run_fastingest(FIXTURES, tmp_path, limit=None, full=True, parquet=False)
        table, stats = run_fastingest(
            FIXTURES, tmp_path, limit=None, full=False, parquet=False
        )

        assert stats["full"] is False
        assert stats["skipped"] == VALID_FIXTURES
        # The two invalid files never enter the manifest, so they retry.
        assert stats["parsed"] == 2
        assert stats["errors"] == 2
        assert table.num_rows == 0

    def test_sidecar_failure_reports_stderr(self, tmp_path: Path) -> None:
        """A fatal sidecar error must surface its message, not a bare
        CalledProcessError with stderr swallowed by capture_output."""
        with pytest.raises(SystemExit) as excinfo:
            run_fastingest(
                tmp_path / "does-not-exist",
                tmp_path,
                limit=None,
                full=True,
                parquet=False,
            )
        assert "cannot read" in str(excinfo.value)


@needs_sidecar
def test_cli_rejects_unknown_flags() -> None:
    """The hand-rolled parser silently treated unknown flags as positional
    arguments; clap must reject them."""
    from job_ingest.ingest_and_benchmark import ensure_rust_binary

    proc = subprocess.run(
        [str(ensure_rust_binary()), str(FIXTURES), os.devnull, "--bogus"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "--bogus" in proc.stderr
