# WSL compatibility review

Reviewed: 2026-09-04

## Conclusion

The current Python/Rust pipeline works under Ubuntu WSL 2 when it uses Linux
Python and Rust artifacts. A checkout stored in the WSL filesystem should work
normally after installing the documented prerequisites.

The existing checkout at `/mnt/c/Users/Alex/Dev/job-ingest` is not ready to be
used interchangeably from Windows and WSL without additional isolation. Its
`.venv` is a Windows virtual environment, the Rust binary lookup assumes the
default Cargo target directory, and the default raw-data symlink is specific to
this machine. The dashboard plan should address these items before claiming
WSL support.

The Textual dashboard itself is not implemented yet, so only the existing
pipeline and the plan's portability assumptions could be validated.

## Validation performed

The review used the installed Ubuntu WSL 2 distribution:

```text
Linux 6.6.114.1-microsoft-standard-WSL2 x86_64
Python 3.13.14 (isolated uv-managed test environment)
rustc 1.94.0
cargo 1.94.0
just 1.21.0
```

To preserve the Windows development environment, all Linux Python, uv cache,
and Cargo build artifacts were placed under `/tmp` rather than in the shared
checkout.

Results:

- `uv.lock` resolved and synced successfully for Linux and Python 3.13.
- Ruff format checking passed.
- Ruff linting passed.
- Strict mypy passed.
- All eight Python tests passed under Linux. Seven integration tests ran with
  the isolated Linux sidecar selected; the crate-discovery test was rerun
  separately without that test-harness override and passed.
- All twelve Rust tests passed under Linux.
- A native Linux release build of `fastingest` succeeded.
- `data/raw/json` resolves successfully from WSL on this machine.

No live HiringCafe scrape was performed, and no Windows or WSL corpus database
was modified.

## Findings

### 1. Windows and WSL cannot share the current `.venv`

Severity: high for a shared checkout; not applicable to separate checkouts.

The existing `.venv/pyvenv.cfg` points to a Windows CPython installation and
contains `.venv/Scripts/python.exe`, not `.venv/bin/python`. Consequently, a
plain WSL `uv` command in this checkout fails while inspecting
`.venv/bin/python3`.

Every Python recipe in the current [`justfile`](../justfile) invokes `uv run`
without selecting a platform-specific environment. Therefore `just check`,
`just ingest`, `just scrape`, and the planned `just dash` do not work directly
from WSL in this shared checkout.

Preferred support modes:

1. Keep a separate clone under the WSL filesystem, where the normal `.venv`
   contains Linux executables.
2. If Windows and WSL must use the same `/mnt/c` checkout, set
   `UV_PROJECT_ENVIRONMENT=.venv-wsl` for WSL commands and add `.venv-wsl/` to
   `.gitignore`.

uv documents `UV_PROJECT_ENVIRONMENT` as the supported way to override the
default `.venv` path: [uv project environment configuration](https://docs.astral.sh/uv/concepts/projects/config/).

### 2. `ensure_rust_binary()` ignores `CARGO_TARGET_DIR`

Severity: medium.

[`ensure_rust_binary()`](../src/job_ingest/ingest_and_benchmark.py) selects the
binary using the hardcoded path:

```python
crate_dir / "target" / "release" / BIN_NAME
```

It then invokes `cargo build --release` and checks that same hardcoded path.
This works in an ordinary Windows-only or WSL-only checkout. It does not support
putting WSL artifacts in a separate Cargo target directory, which is desirable
when both operating systems use one checkout. If `CARGO_TARGET_DIR` is set,
Cargo writes the Linux binary there but the wrapper reports that the expected
binary is missing.

Before documenting shared-checkout support, update the wrapper to honor Cargo's
effective target directory or add a validated `FASTINGEST_BIN` override. Add
tests for both an absolute custom target directory and the normal default.

### 3. The raw-data symlink is machine-specific

Severity: medium for setup; it works on the reviewed machine.

From WSL, the current link resolves as:

```text
data/raw/json -> /mnt/c/Users/Alex/Dev/job-search/data/cache/json
```

The target exists and is readable, so the established ingest default works on
this machine. However, `data/` is gitignored, and the link has an absolute
machine-specific target. A fresh clone—especially one under `~/...` in the WSL
filesystem—will not acquire a usable corpus link automatically.

The README and dashboard plan should tell WSL users to either recreate
`data/raw/json` with an appropriate Linux-visible target or pass an explicit
`--json-dir`. The dashboard should continue to display the resolved scrape and
ingest paths and surface a missing or broken link as a normal empty/error state.

### 4. Do not access one DuckDB file concurrently from Windows and WSL

Severity: medium.

The dashboard plan's `threading.Lock` protects only operations inside one
dashboard process. It cannot coordinate a Windows process and a WSL process
that open the same database through `C:\...` and `/mnt/c/...` respectively.

DuckDB supports either one read-write process or multiple read-only processes
and advises extra caution for database files in shared directories accessed
through different operating systems or filesystems:
[DuckDB concurrency documentation](https://duckdb.org/docs/current/connect/concurrency).

Update the plan's warning from “close any stray `duckdb.exe`” to cover any
Windows or WSL DuckDB, dashboard, ingest, or Python process using that file.
Document that the same `jobs.duckdb` must not be operated on concurrently from
both environments.

### 5. `/mnt/c` is functional but slower for this workload

Severity: low for correctness, potentially material for performance.

This project scans tens of thousands of small files and performs Rust builds
and database writes. Those operations are sensitive to filesystem latency.
Microsoft recommends storing projects in the WSL filesystem when Linux tools
drive the workload, rather than building under `/mnt/c`:
[Microsoft WSL filesystem guidance](https://learn.microsoft.com/en-us/windows/wsl/filesystems).

Recommend a WSL-native checkout such as `~/dev/job-ingest` for regular WSL use.
An explicit `--json-dir /mnt/c/...` may still point at the existing corpus if
duplicating the raw data is undesirable.

### 6. The planned filename handling is safely conservative on WSL

Severity: informational.

The dashboard plan requires raw filenames to account for Windows-invalid
characters, reserved names, case folding, trailing dots/spaces, Unicode
normalization, and length limits. Linux permits more of these names, but using
the stricter portable convention is appropriate when data may move between
Windows and WSL. The same-directory exclusive temporary file plus
`os.replace()` design is also portable to WSL.

Tests should retain Windows collision cases even when the suite runs on Linux,
rather than deriving expected behavior from the host filesystem.

## Required changes to the dashboard plan

Before treating WSL as a supported environment, incorporate these requirements
into [`textual-dashboard-plan.md`](textual-dashboard-plan.md):

- Add WSL setup documentation to Step 1 and Step 7, including Python 3.13,
  `uv`, `just`, Rust/Cargo, and a C/C++ build toolchain needed by native Rust
  dependencies.
- Recommend a WSL-native checkout. Document
  `UV_PROJECT_ENVIRONMENT=.venv-wsl` for users intentionally sharing a
  `/mnt/c` checkout with Windows.
- Make Rust binary discovery compatible with an isolated Cargo target directory
  or an explicit binary override.
- Document how to configure the machine-specific raw corpus path from WSL.
- Expand the DuckDB lock warning to cover cross-environment access.
- Add a Linux/WSL CI or acceptance lane that runs the full quality gate,
  including Textual's headless `App.run_test()` tests once implemented.
- Add a WSL manual smoke test for `just dash`, ingest, browsing, one saved-search
  scrape, path display, and the missing-data state.
- State that Windows and WSL must use separate virtual environments and Rust
  build artifacts when sharing a checkout.

## Suggested WSL verification matrix

| Scenario | Expected result |
| --- | --- |
| WSL-native checkout, normal `.venv` and Cargo `target/` | Full automated gate passes |
| Shared `/mnt/c` checkout with `.venv-wsl` and isolated Cargo target | Full automated gate passes without changing Windows artifacts |
| Missing or broken `data/raw/json` link | Dashboard mounts and explains that the corpus is unavailable |
| Explicit WSL-visible `--json-dir` and `--out-dir` | Scrape/ingest pipeline completes |
| Windows process already holds `jobs.duckdb` | WSL dashboard reports `StatsUnavailable` without crashing |
| WSL process already holds `jobs.duckdb` | Windows dashboard reports `StatsUnavailable` without crashing |
| Textual headless tests under Linux | Same widget and worker assertions pass as on Windows |

## Current support assessment

- Existing Rust implementation: verified on WSL.
- Existing Python implementation: verified on WSL with isolated environment and
  binary selection.
- Dependency lockfile: verified to resolve on Linux/Python 3.13.
- Existing raw-data link: verified on this machine only.
- Shared-checkout developer workflow: requires the changes above.
- Planned Textual dashboard: likely portable by design, but unverified until it
  is implemented and tested under WSL.
