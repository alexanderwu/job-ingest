"""Tests for the Python wrapper around the fastingest sidecar.

The end-to-end tests reuse the Rust crate's golden fixtures
(`fastingest/tests/fixtures/`) as a miniature corpus, so they cover the part
the Rust tests can't: the subprocess contract, the Arrow handoff, and
whether DUCKDB_DDL still agrees with the sidecar's Arrow schema.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from job_ingest.ingest_and_benchmark import (
    INCOMPLETE_MARKER,
    IngestError,
    ddl_columns,
    find_crate_dir,
    load_duckdb,
    run_fastingest,
    run_ingest,
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
        assert list(ddl_columns()) == list(table.column_names)

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
        with pytest.raises(IngestError) as excinfo:
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


@needs_sidecar
class TestRunIngest:
    """The library core: typed results and streamed progress, no stdout."""

    def test_returns_a_populated_result_and_streams_progress(
        self, tmp_path: Path
    ) -> None:
        events: list[str] = []
        result = run_ingest(FIXTURES, tmp_path, full=True, on_event=events.append)

        assert result.full is True
        assert result.ok == VALID_FIXTURES
        assert result.errors == 2
        assert result.duckdb_total_rows == VALID_FIXTURES
        assert result.sqlite_total_rows == VALID_FIXTURES
        assert result.files == VALID_FIXTURES + 2
        assert len(result.error_samples) == 2
        assert result.parquet_sec is None
        assert result.parquet_bytes is None
        assert result.sqlite_bytes > 0
        assert result.duckdb_bytes > 0
        # The lines that only make sense mid-run go out through on_event; the
        # summary block is derived from the result afterwards.
        assert any("Loading into DuckDB" in line for line in events)
        assert any("full rebuild" in line for line in events)
        assert not any("BENCHMARK SUMMARY" in line for line in events)

    def test_a_successful_run_leaves_no_recovery_marker(self, tmp_path: Path) -> None:
        run_ingest(FIXTURES, tmp_path, full=True)
        assert not (tmp_path / INCOMPLETE_MARKER).exists()


class TestSidecarFailuresAreIngestErrors:
    """No Rust toolchain: the sidecar call is stubbed at subprocess level."""

    @staticmethod
    def _fake_proc(stdout: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=0, stdout=stdout, stderr=""
        )

    def _stub_binary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.ensure_rust_binary",
            lambda *_a, **_k: Path("fastingest"),
        )

    def test_a_malformed_stats_line_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        self._stub_binary(monkeypatch)
        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.subprocess.run",
            lambda *_a, **_k: self._fake_proc("not json at all"),
        )
        with pytest.raises(IngestError, match="not valid JSON"):
            run_fastingest(tmp_path, tmp_path, limit=None, full=True, parquet=False)

    def test_an_empty_stdout_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        self._stub_binary(monkeypatch)
        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.subprocess.run",
            lambda *_a, **_k: self._fake_proc(""),
        )
        with pytest.raises(IngestError, match="no stats line"):
            run_fastingest(tmp_path, tmp_path, limit=None, full=True, parquet=False)

    def test_a_missing_arrow_handoff_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        self._stub_binary(monkeypatch)
        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.subprocess.run",
            lambda *_a, **_k: self._fake_proc(json.dumps({"ok": 0})),
        )
        with pytest.raises(IngestError, match="Arrow handoff"):
            run_fastingest(tmp_path, tmp_path, limit=None, full=True, parquet=False)


class TestIncompleteMarker:
    """Crash consistency across the sidecar/DuckDB commit boundary.

    The sidecar commits SQLite and ingest_manifest.json before Python commits
    DuckDB. Without the marker, a failure in between leaves a manifest that
    tells the next incremental run to skip a delta DuckDB never received.
    """

    STATS = {
        "ok": 0,
        "files": 0,
        "skipped": 0,
        "parsed": 0,
        "errors": 0,
        "sqlite_insert_sec": 0.0,
        "sqlite_total_rows": 0,
        "error_samples": [],
    }

    def _seed_a_complete_previous_run(self, out_dir: Path) -> None:
        """Everything present, so `full` is False unless something forces it."""
        (out_dir / "jobs.sqlite").write_text("")
        (out_dir / "jobs.duckdb").write_text("")
        (out_dir / "ingest_manifest.json").write_text("{}")

    def test_a_duckdb_failure_forces_the_next_run_full(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        import pyarrow as pa

        self._seed_a_complete_previous_run(tmp_path)
        full_flags: list[bool] = []

        def fake_run_fastingest(
            _json_dir: Path,
            _out_dir: Path,
            _limit: int | None,
            full: bool,
            parquet: bool = False,
            on_event: object = None,
        ) -> tuple[pa.Table, dict[str, object]]:
            full_flags.append(full)
            return pa.table({"requisition_id": []}), dict(self.STATS)

        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.run_fastingest", fake_run_fastingest
        )
        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.load_duckdb",
            lambda *_a, **_k: (_ for _ in ()).throw(IngestError("duckdb is locked")),
        )

        with pytest.raises(IngestError, match="locked"):
            run_ingest(tmp_path, tmp_path)

        assert full_flags == [False]
        assert (tmp_path / INCOMPLETE_MARKER).exists(), (
            "the marker must survive the failure, or the next incremental run "
            "silently skips the delta DuckDB never saw"
        )

        monkeypatch.setattr(
            "job_ingest.ingest_and_benchmark.load_duckdb", lambda *_a, **_k: (0.0, 0)
        )
        result = run_ingest(tmp_path, tmp_path)

        assert full_flags == [False, True]
        assert result.full is True
        assert not (tmp_path / INCOMPLETE_MARKER).exists()
