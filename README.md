![lazy_fred_social](https://github.com/Jmetrics86/lazy_fred/assets/19334741/c7ae2ec3-2ef6-4ca7-b126-78622537d5e0)

![example workflow](https://github.com/Jmetrics86/lazy_fred//actions/workflows/python-app.yml/badge.svg) ![PyPI - Version](https://img.shields.io/pypi/v/lazy_fred)

# lazy_fred: Effortless FRED Data Collection

`lazy_fred` is a Python library designed to simplify collecting economic data from the Federal Reserve Economic Data (FRED) API. It searches across categories, filters series by popularity/frequency, and exports results to CSV files.

## Features
- Automated search across FRED categories
- Filtered selection by frequency (daily/weekly/monthly) and popularity
- Retry and logging for robust data collection
- CSV exports for the filtered series + daily/weekly/monthly observations

## Installation

### From PyPI (recommended)
```bash
python -m pip install --upgrade lazy_fred
```

### From source (Poetry)
```bash
poetry install
```

## Configure your FRED API key

`lazy_fred` expects a `FRED API_KEY`. You can provide it either by:

1. Creating a local `.env` file in your working directory:
   ```bash
   API_KEY=your_fred_api_key_here
   ```
2. Or exporting it as an environment variable:
   ```bash
   set API_KEY=your_fred_api_key_here
   ```

If `API_KEY` is missing, the CLI will prompt you for it and write it to `.env`.

## Quick start (CLI)

After installation, run:

```bash
lazy-fred
```

The CLI is interactive and will prompt you to:
- add/remove/clear categories
- run the data collection

Note: because it uses `input()`, it is intended for interactive terminals.

## Output files

When you choose `run`, the project will generate these CSV files in your current directory:
- `filtered_series.csv`
- `daily_data.csv`
- `weekly_data.csv`
- `monthly_data.csv`

## Programmatic usage

```python
import lazy_fred as lf

lf.run_fred_data_collection("insert_api_key_here")
```

This will also start the interactive menu (it blocks on `input()`).

## Development
- Lint: `poetry run ruff check .`
- Tests (requires a real key): `API_KEY=<your_key> poetry run pytest`

## Disclaimer

This library is not affiliated with or endorsed by the Federal Reserve Bank of St. Louis or the FRED project.

## Acknowledgments

This project utilizes the `fredapi` and `fred` libraries for interacting with the FRED API.

## Contributions

Contributions are welcome. Feel free to open issues or submit pull requests.

## License

MIT

