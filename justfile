# Job Ingest task runner. Run `just` (or `just --list`) to see all recipes.

# Show available recipes when run with no arguments.
default:
    @just --list

# Create/update the virtualenv with the pinned Python and all dependencies.
install:
    uv sync

# Run the test suite.
test *args:
    uv run pytest {{ args }}

# Format code in place.
fmt:
    uv run ruff format .

# Lint, auto-fixing what can be fixed safely.
lint:
    uv run ruff check --fix .

# Run the full CI gate locally: format check, lint, typecheck, tests.
check:
    uv run ruff format --check .
    uv run ruff check .
    uv run pytest
