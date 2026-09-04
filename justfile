# Job Ingest task runner. Run `just` (or `just --list`) to see all recipes.

# Show available recipes when run with no arguments.
default:
    @just --list

# Scrape hiringcafe.com
scrape *args:
    uv run src/job_ingest/scrape_hiringcafe.py {{ args }}

# Ingest
ingest *args:
    uv run src/job_ingest/ingest_and_benchmark.py {{ args }}

# Run the Textual dashboard
dash *args:
    uv run src/job_ingest/dashboard.py {{ args }}

# Create/update the virtualenv with the pinned Python and all dependencies.
install:
    uv sync

# Run the test suite (Python wrapper + Rust sidecar).
test *args:
    uv run pytest {{ args }}
    cd fastingest && cargo test

# Python tests only.
test-py *args:
    uv run pytest {{ args }}

# Rust tests only.
test-rs *args:
    cd fastingest && cargo test {{ args }}

# Re-record the golden snapshots after an intentional schema change.
# Review the resulting diff carefully before committing.
bless:
    cd fastingest && BLESS=1 cargo test --test golden

# Format code in place.
fmt:
    uv run ruff format .

# Lint, auto-fixing what can be fixed safely.
lint:
    uv run ruff check --fix .

# Static type check (strict).
typecheck:
    uv run mypy

# Static check the Rust sidecar.
clippy:
    cd fastingest && cargo clippy --all-targets -- -D warnings

# Run the full CI gate locally: format check, lint, typecheck, tests.
check:
    uv run ruff format --check .
    uv run ruff check .
    uv run mypy
    uv run pytest
    cd fastingest && cargo fmt --check
    cd fastingest && cargo clippy --all-targets -- -D warnings
    cd fastingest && cargo test
