# AGENTS.md

## Cursor Cloud specific instructions

### Overview

`lazy_fred` is a Python library/CLI for automated collection of economic time series from the FRED API. It's a single-module Python package managed by **Poetry**.

### Key commands

| Action | Command |
|---|---|
| Install deps | `poetry install` |
| Lint | `poetry run ruff check .` |
| Run tests | `API_KEY=<your_key> poetry run pytest` |
| Run script | `API_KEY=<key> poetry run python3 lazy_fred.py` |
| Use as library | `import lazy_fred as lf; lf.run_fred_data_collection("<API_KEY>")` |

### Important notes

- **FRED API key required**: Tests (`test_lazy_fred.py`) make real API calls to FRED. Set `API_KEY` as an environment variable. Without it, 2 of 3 tests will fail.
- **Interactive prompts**: `run_fred_data_collection()` uses `input()` for menu-driven interaction. When running from a non-interactive context, this will block. The main script is not suitable for automated/background execution without modification.
- **Module structure**: The package is a flat single-file module (`lazy_fred.py` + `__init__.py` at root). When Poetry-installed, `import lazy_fred` loads `lazy_fred.py` directly (not a package directory). The test file uses `from .lazy_fred import ...` (relative import) which works under pytest but not standalone.
- **Poetry path**: Poetry installs to `~/.local/bin`. Ensure `PATH` includes `$HOME/.local/bin`.
- **Pre-existing lint issues**: `ruff check .` reports 4 warnings in `test_lazy_fred.py` (unused imports, `None` comparison). These are in the existing codebase.
