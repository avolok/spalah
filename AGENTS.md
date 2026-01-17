# AGENTS.md — Guidance for coding agents

Purpose
- This file informs agentic coding tools how to build, lint, test, and follow code style in this repository.
- Scope: the entire repository rooted at the project root (package `spalah`).

File status
- This `AGENTS.md` file is currently untracked (created by an agent). Do not auto-commit changes to this file without human approval.
- Always run `pre-commit run --all-files` and `ruff check --fix .` (or equivalent) before creating commits that include changes to this file.

Quick environment / prerequisites
- Python: supported range is `>=3.11,<4.0` (see `pyproject.toml`). See `pyproject.toml:1`.
- This project uses `uv` (astral) for repeatable virtualenv + dependency management. The CI uses `uv` in `.github/workflows/spalah_ci.yaml:25-33`.
- Recommended local flow:
  - Install `uv` (https://astral.sh/uv) or follow `Makefile:create_env` target.
  - Create venv: `uv venv -p python3.11` or run `make create_env` (see `Makefile:7-15`).
  - Sync dev dependencies: `uv sync --group dev` (matches CI).

Build / install commands
- Install package editable (dev):
  - `uv pip install -e .` (after `uv venv`)
  - Or without uv: `python -m pip install -e .` after activating a python3.11 venv.
- Build for release (used by semantic-release):
  - `python -m pip install -e '.[build]'` then `uv build` (see `pyproject.toml:49-54`).

Lint / format / pre-commit
- Primary tools:
  - `ruff` (configured in `pyproject.toml` with `line-length = 100` and `fix = true`) — see `pyproject.toml:72-75`.
  - `pre-commit` is configured in `.pre-commit-config.yaml` (runs `ruff-check` and `ruff-format`). See `.pre-commit-config.yaml:19-28`.
- Run linters locally:
  - `uv run ruff check .` or simply `ruff check .` (if ruff is installed in your active environment).
  - Auto-fix where appropriate: `uv run ruff check --fix .` or `ruff check --fix .`.
  - Format (ruff format): `uv run ruff format .`.
- Run pre-commit hooks against all files:
  - `pre-commit run --all-files` (recommended before committing).

Testing
- Dependencies include `pytest`, `pytest-cov`, `pytest-sugar`. CI runs tests with coverage.
- Run full test suite (as CI):
  - `uv run pytest --cache-clear --cov=spalah tests/` (CI: `.github/workflows/spalah_ci.yaml:39-40`).
  - Or, with an active venv and installed deps: `pytest --cache-clear --cov=spalah tests/`.
- Run a single test file:
  - `pytest tests/test_dataframe_slice_dataframe.py -q`.
- Run a single test function (recommended format):
  - `pytest tests/test_dataframe_slice_dataframe.py::test_slice_dataframe -q`.
  - Or with coverage: `pytest --cov=spalah tests/test_dataframe_slice_dataframe.py::test_slice_dataframe`.
- Run tests matching an expression:
  - `pytest -k "expression"` (matches test names/ids).
- Tips for Spark tests
  - The test suite uses a `SparkSession` fixture in `tests/conftest.py` (session scope) to avoid repeated session start/stop. Do not create a new SparkSession per test.
  - If you change test fixtures, update `tests/conftest.py` accordingly to minimize expensive setup.

CI notes
- GitHub Actions workflow `.github/workflows/spalah_ci.yaml` does:
  - Install `uv`, set up Python 3.11, `uv sync --group dev`, run `pre-commit` then `uv run pytest --cache-clear --cov=spalah tests/`.
- Keep CI commands and dependency groups in sync with `pyproject.toml` `dev` dependency-group.

Code Style Guidelines (apply to all edits)
- Base style
  - Follow PEP8 where reasonable. This project uses `ruff` for linting and auto-formatting. Default line length = 100.
  - Use `ruff` (and `pre-commit`) as the ground truth — run them locally and fix issues before pushing.

- Imports
  - Order: standard library -> third-party -> local package. Separate groups by a single blank line.
  - Prefer absolute imports for intra-package references (e.g. `from spalah.dataframe import slice_dataframe`).
  - Avoid star imports (`from module import *`).
  - Keep import statements sorted alphabetically within groups where possible.

- Formatting
  - Line length: 100 characters (see `pyproject.toml:72`).
  - Indentation: 4 spaces per level.
  - Use trailing comma for multi-line literals/parameters where it improves diffs.
  - Use `ruff format` / `ruff --fix` to keep formatting consistent.

- Types and annotations
  - The project targets Python >=3.11 — prefer built-in generics: use `list[str]`, `dict[str, Any]`, etc.
  - Add type hints to all public functions and class methods. Return types should be explicit for public API.
  - Use `typing` (e.g. `Any`, `Iterable`, `Optional`) when appropriate. Keep signatures readable.

- Naming conventions
  - Functions and variables: `snake_case` (e.g. `slice_dataframe`).
  - Classes: `PascalCase` (e.g. `SchemaComparer`, `DeltaTableConfig`).
  - Constants: `UPPER_SNAKE_CASE` when module-level constants are present.
  - Test files: `tests/test_*.py`. Test functions: start with `test_` and be descriptive.

- Docstrings and comments
  - Public functions and classes should have docstrings describing purpose, args, return type, and raised exceptions.
  - Keep docstrings concise and useful; prefer Google or NumPy style (either is acceptable). Inline comments should explain "why", not "what".

- Error handling
  - Prefer raising specific exception types (`ValueError`, `TypeError`, `KeyError`, or custom exceptions) instead of `Exception`.
  - Tests sometimes assert exception messages — avoid changing error messages that are asserted in tests.
  - When validating parameters, use early checks with clear messages (e.g. `raise TypeError("...")` or `raise ValueError("...")`).

- Logging
  - Use the package logging helper: `from spalah.shared.logging import get_logger` and `logger = get_logger(__name__)`.
  - Avoid printing to stdout/stderr in library code; prefer logging at appropriate levels.

- API/Backward compatibility
  - This library is a user-facing package. Maintain backward compatibility for public functions/classes where possible.
  - When making breaking changes, update `CHANGELOG.md` and follow semantic-release flow configured in `pyproject.toml`.

- Tests and fixtures
  - Use fixtures in `tests/conftest.py` to provide reusable Spark datasets. Add new fixtures there as needed.
  - Tests should not rely on global external state — keep fixtures deterministic and isolated.
  - When adding new Spark-based tests, prefer `scope='session'` fixtures for expensive SparkSession creation unless a test intentionally needs a fresh session.

Repository-specific notes and gotchas
- Keep error messages stable: many tests assert on message fragments (see `tests/test_dataframe_slice_dataframe.py`), so be conservative when editing messages.
- The project uses `delta-spark` in fixtures (`tests/conftest.py:10-20`) via the `spark.jars.packages` config. Tests assume these packages are available when running the test suite.
- CI uses `uv` to ensure dependency reproducibility. If you cannot (or prefer not to) install `uv` locally, make sure your venv includes the `dev` dependencies from `pyproject.toml`.

Cursor / Copilot rules
- I checked for Cursor rules in repository paths `.cursor/rules/` and `.cursorrules` and for Copilot rules in `.github/copilot-instructions.md`. None were found at the time of writing (no matches).
- If such rules are added later, they should be copied into this file (or referenced) so agentic tools can follow them.

Contributing and commit messages
- The project uses `python-semantic-release` settings in `pyproject.toml` — commit messages should follow conventional commit style so automatic releases can parse the version bump.
- Keep commit messages focused and use allowed tags such as: `feat`, `fix`, `perf`, `build`, `chore`, `ci`, `docs`, `style`, `refactor`, `test` (see `pyproject.toml:101-105`).

Quick command reference (copy/paste)
- Create env (via Makefile): `make create_env` (this installs `uv`, creates `.venv`, installs doc deps).
- Create venv & sync dev deps (explicit):
  - `curl -LsSf https://astral.sh/uv/install.sh | sh` (only if you need uv)
  - `uv venv -p python3.11`
  - `uv sync --group dev`
- Install editable package locally: `uv pip install -e .` or `python -m pip install -e .`
- Run all tests (CI-style): `uv run pytest --cache-clear --cov=spalah tests/`
- Run one file: `pytest tests/test_dataframe_slice_dataframe.py -q`
- Run one test: `pytest tests/test_dataframe_slice_dataframe.py::test_slice_dataframe -q`
- Run ruff auto-fix: `uv run ruff check --fix .` or `ruff check --fix .`
- Run pre-commit locally: `pre-commit run --all-files`

Where to look
- `pyproject.toml` — project metadata, `ruff` config, semantic-release: `pyproject.toml:1`.
- `.pre-commit-config.yaml` — configured pre-commit hooks including `ruff`: `.pre-commit-config.yaml:1`.
- `Makefile` — helper targets: `Makefile:1`.
- `.github/workflows/spalah_ci.yaml` — CI test and lint pipeline: `.github/workflows/spalah_ci.yaml:1`.
- Tests and fixtures: `tests/conftest.py:1` and `tests/test_dataframe_slice_dataframe.py:1`.

If you want me to also:
- Run linters and tests locally (requires permission to run commands), or
- Add a small script `scripts/run_test.sh` that wraps a typical run (make, uv, pytest),
say which one and I will proceed.
