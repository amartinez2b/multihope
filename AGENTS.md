# AGENTS GUIDE - Multihope
This file documents how autonomous agents should work inside the multihope pipeline repository.
Use these instructions alongside repository-specific context such as `CLAUDE.md` and `README.md`.
Assume the working directory is `/Users/agus/local/multihope` unless explicitly changed.

## 1. Repository Orientation
- Primary technology stack: Python 3.9+, PySpark, Databricks Labs DQX (Python).
- Repo layout mirrors the Medallion architecture: RAW → BRONZE → SILVER → GOLD.
- Source code lives under `src/` with utilities in `src/utils/` and layer scripts grouped per medallion stage.
- Tests mirror the same structure under `tests/` and use PySpark with pytest fixtures.
- Configuration files housed in `config/`, runtime data in `data/` (gitignored), notebooks in `notebooks/`.
- Do not open `.env`; copy from `.env.example` when needed and keep secrets out of version control.

## 2. Environment Setup
- Create a virtual environment: `python -m venv .venv`.
- Activate on macOS/Linux: `source .venv/bin/activate`; on Windows use `.\.venv\\Scripts\\activate`.
- Install dependencies: `pip install -r requirements.txt`.
- If PySpark complains about Java, ensure JDK 8/11/17/21 is installed and `JAVA_HOME` is set.
- Copy credentials template: `cp .env.example .env`; fill `DB_USER` and `DB_PASSWORD` manually.
- Never commit `.env`, credential files, or generated parquet outputs under `data/`.

## 3. Build, Lint, and Test Commands
- Run full test suite: `pytest tests/ -v`.
- Run a single test file: `pytest tests/test_bronze_to_silver.py -v` (replace with the desired path).
- Run a single test case: `pytest tests/test_bronze_to_silver.py::test_drop_duplicates -v`.
- PySpark tests spin up local sessions; expect higher startup time on first run.
- No dedicated lint command exists yet; follow style rules below and consider `ruff` or `flake8` locally if desired.
- Build artifacts are the parquet datasets produced by the pipeline scripts; they are not checked into git.

## 4. Pipeline Execution Entrypoints
- RAW → BRONZE: `python -m src.raw_to_bronze.customers_ingestion` (reads MySQL, writes parquet).
- BRONZE → SILVER: `python -m src.bronze_to_silver.customers_transform` (applies DQX validations).
- SILVER → GOLD: `python -m src.silver_to_gold.customers_aggregation` (aggregates by `estado`).
- Notebooks in `notebooks/` replicate these steps; execute sequentially `00_precheck` through `03_silver_to_gold`.
- Scripts expect JDBC connectivity to `www.bigdataybi.com:3306/fake`; tests use in-memory data and need no DB.
- Ensure output directories exist under `data/`; scripts create them automatically via Spark writers.

## 5. Data Quality Expectations
- Error-level checks send rows to `data/quarantine/customers`: `customer_id`, `nombre`, and `email` must be non-null and non-empty.
- Warning-level checks flag rows but keep them in silver: `identificacion` non-empty and `estado` non-null.
- Silver records gain `_ingested_at` (timestamp) and `_source_layer` (string) metadata columns.
- Downstream aggregations assume deduplicated customers and lowercase emails.
- Preserve schema evolution by appending new columns rather than mutating existing names whenever feasible.
- Update tests in `tests/` to cover any new validation or transformation rule.

## 6. Python Style and Formatting
- Follow PEP 8 spacing with max line length 100 characters; wrap Spark chains with parentheses as in existing scripts.
- Prefer `pathlib.Path` over string paths; cast to `str()` when interfacing with Spark or OS APIs.
- Import order: standard library, third-party, then local modules, each separated by a blank line.
- Use explicit relative paths via `Path(__file__).parents[...]`; avoid hardcoding absolute paths.
- Keep module-level constants upper snake case (e.g., `BRONZE_OUTPUT`).
- Provide module docstrings when scripts can run as entrypoints, describing pipeline stage and behavior.
- Apply function annotations for return types and parameters when clarity improves (see `ingest_customers` returning `int`).
- For PySpark transformations, chain methods line by line to emphasize the data flow.
- Avoid wildcard imports; alias `pyspark.sql.functions` as `F` and `types` as `T` when needed.
- Use list/dict comprehensions judiciously; prefer readability over micro-optimizations.
- Rely on `logging` for operational messages instead of `print`; configure format once per module.
- Use `logging.getLogger(__name__)` and standard log levels (`INFO`, `WARN`, `ERROR`).
- Treat Spark actions (`count`, `collect`) carefully; minimize repeated actions to avoid performance penalties.
- Handle resource cleanup: call `spark.stop()` when you create sessions outside PySpark-managed contexts.
- For CLI-friendly scripts, guard execution with `if __name__ == "__main__":` and keep business logic in functions.
- Capture configuration in YAML or environment variables, not magic numbers scattered in code.
- Use f-strings for interpolation; keep braces minimal and avoid concatenating literal strings without `f` prefixes.
- Prefer explicit column lists in DataFrame writers and readers; document assumptions in docstrings.
- When adding dependencies, update `requirements.txt` and document usage in `README.md` if user-facing.
- Avoid mutating global state beyond constants; prefer passing parameters or returning new DataFrames.
- Document non-obvious transformations with inline comments only when necessary.
- Keep tests deterministic; avoid reliance on wall-clock timestamps unless using `freeze_time` or equivalent.
- When adding notebooks, ensure they run headless; strip large outputs before committing.

## 7. Naming Conventions
- Functions and methods: `snake_case` (e.g., `ingest_customers`).
- Classes (rare): `CapWords`.
- Constants: `UPPER_SNAKE_CASE` defined near module top.
- PySpark columns: use lowercase with underscores; maintain existing column names unless schema change documented.
- Test functions: prefix with `test_` and describe behavior (e.g., `test_drop_duplicates`).
- Fixture names describe scope or data, e.g., `spark`, `bronze_data`.
- Loggers stored in `logger` variable per module.
- Temporary variables in comprehensions should remain short but descriptive (`row`, `emails`).

## 8. Error Handling and Validation
- Surface configuration issues early; raise `ValueError` for invalid config data before hitting Spark.
- Wrap external resource access (MySQL, filesystem) with informative log messages and avoid bare `except` clauses.
- Let PySpark raise for schema mismatches; catch only when you can add actionable context.
- Use guard clauses to return early; avoid deep nesting inside transformations.
- For data quality failures, prefer sending rows to quarantine rather than dropping silently.
- When adding new error rules, persist reasons as separate columns or DQX metadata for traceability.
- Keep tests around expected exceptions using `pytest.raises` with explicit error messages.
- Validate environment variables before use; log missing credentials with `logger.error` and exit gracefully.

## 9. Imports and Module Structure
- Use absolute imports rooted at `src`, e.g., `from src.utils.spark_session import create_spark_session`.
- Maintain `sys.path` adjustments at the top of executable scripts to support local runs.
- Group related helper functions in `src/utils/`; avoid circular imports by keeping layers loosely coupled.
- Keep layer scripts focused on orchestration; move shared transformations into reusable helpers where possible.
- Limit side effects at import time; defer expensive Spark operations to function bodies.
- Ensure new modules include in `__all__` only when curated API is necessary (currently unused).

## 10. Testing Strategy
- Tests rely on local Spark sessions; scope fixtures appropriately (`session` for Spark builder).
- Seed deterministic data using Python lists of tuples and explicit schemas.
- Access DataFrame columns via `F.col` to maintain consistent aliasing.
- Prefer `.collect()` only when asserting small datasets; otherwise use set equality via `.orderBy().collect()` if order matters.
- Keep pipeline tests focused on behavior—deduplication, null filtering, metadata columns, aggregation accuracy.
- When introducing new transformations, add targeted tests rather than expanding existing ones indefinitely.
- Run relevant single-file tests during development to minimize feedback loop.
- Use `pytest -k <pattern>` for selective execution when editing multiple related tests.
- Document any flaky behavior and stabilize before merging.
- Avoid reliance on external MySQL in tests; use fixtures to stub data.

## 11. Data and Configuration Hygiene
- Treat `config/database.yml` and `config/spark_config.yml` as authoritative; update together with code changes.
- Keep schema documentation in `catalog/comercial/`; update both `.md` and `.json` when altering tables.
- Generated data lives under `data/`; clean up after experiments to prevent accidental reuse.
- Respect `.gitignore`; add new transient directories before generating files.
- Ensure new environment variables are mirrored in `.env.example` with placeholders.
- Mention credential requirements in `README.md` when workflows change.
- Use Databricks Labs DQX patterns when adding validations for consistency.
- When introducing new layers or tables, extend tests, catalog, and pipeline scripts in tandem.

## 12. Collaboration and Git Workflow
- Keep commits focused and well-described; follow existing message tone (imperative, present tense).
- Do not modify git configuration beyond per-repo user identity.
- Staging should avoid secrets; double-check before committing.
- When working in branches, ensure parity with `main` before submitting PRs.
- Use `gh` CLI for PR interactions if automation is required (no direct API calls).
- Observe repository hooks; fix issues rather than bypassing with `--no-verify`.
- Reference relevant tests in commit messages when fixing failures.
- Document major changes in `README.md` or new docs to support future agents.

## 13. Agent Operational Checklist
- Read `CLAUDE.md` for pipeline overview and environment requirements.
- Confirm no Cursor or Copilot instruction files exist (checked: none under `.cursor/` or `.github/`).
- Respect `.env` privacy; never attempt to read it.
- Use `Read`, `ApplyPatch`, `Glob`, and other specialized tools per CLI instructions; avoid raw `cat`.
- Favor small, targeted changes; summarize diffs referencing file paths (e.g., `src/utils/config_loader.py`).
- Run relevant tests after code modifications; report results or reasons if skipped.
- Avoid destructive git commands (no `git reset --hard`, no force pushes to `main`).
- Default to ASCII in new files unless existing encoding dictates otherwise.
- Provide actionable follow-ups in responses when natural next steps exist (tests, commits, reviews).
- When uncertain, consult existing scripts and tests to infer repository conventions before asking questions.

## 14. External Knowledge Base
- Treat the official Databricks Labs DQX documentation as canonical for engine behavior and API usage: https://github.com/databrickslabs/dqx
- Reference that repo when clarifying rule semantics, engine constraints, telemetry requirements, or new check functions before implementing local changes.

## 15. Future Enhancements Placeholder
- If adding linters or formatters, document invocation commands here and update style guidance.
- Encourage contributing agents to share context in AGENTS.md when conventions evolve.
- Maintain approximately 150 lines to keep this guide scannable for automated tooling.
- Re-run checks for new instruction files (Cursor, Copilot) whenever repository updates bring them in.
- Keep this document synchronized with `README.md`, `CLAUDE.md`, and any new governance docs.
- Treat AGENTS.md as source of truth for automation etiquette; update responsibly.
- End of AGENTS.md.
