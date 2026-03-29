# AGENTS.md

## Overview

`lazy_fred` is a Python CLI/library for pulling economic time series from the FRED API.
It is a flat, single-module package managed by Poetry.

## Current user-facing commands

| Action | Command |
|---|---|
| Install deps | `poetry install --with dev` |
| Lint | `poetry run python -m ruff check .` |
| Run tests | `poetry run pytest -q` |
| Run doctor | `poetry run lazy-fred doctor` |
| Quick starter pull | `poetry run lazy-fred quick` |
| Standard pull | `poetry run lazy-fred standard` |
| Full pull | `poetry run lazy-fred full` |
| One-shot full + giant master CSV | `poetry run lazy-fred master` |
| Favorites | `poetry run lazy-fred favorites macro` |
| Notebook UI | `import lazy_fred as lf; lf.launch_notebook_ui("<API_KEY>")` |
| Analysis dashboard | `poetry install --extras dashboard` then `poetry run lazy-fred-dashboard` (from directory with CSVs) |
| Panel API | `from panel import load_master_long, build_aligned_panel` (pandas alignment, no Streamlit) |

## Important notes

- Live FRED tests in `test_lazy_fred.py` need `API_KEY` (or `FRED_API_KEY`).
- CLI supports start-date selection for pulls and uses exponential backoff retries.
- Existing CSV outputs are auto-backed up to `backups/<timestamp>/` before overwrite.
- PyPI publishing uses GitHub Actions (`publish-pypi.yml`) with Trusted Publishing OIDC.

