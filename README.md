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

After installation, run one of these:

```bash
lazy-fred
# or
lazy_fred
```

Then type:
- `run-all` to pull all default categories, or
- `run` to use your current selected categories.

## Quick start (Colab/Jupyter UI)

Use this if you want buttons instead of terminal prompts:

```python
!pip install -U lazy_fred ipywidgets
import lazy_fred as lf
lf.launch_notebook_ui("YOUR_FRED_API_KEY")
```

In the UI:
- select categories and click **Run collection**, or
- click **Run all defaults**.

## Beginner guide (step-by-step)

If you just want this to work, follow these exact steps.

### Step 1: Install Python
- Install Python 3.10+ from https://www.python.org/downloads/
- During install on Windows, check "Add Python to PATH"

### Step 2: Install `lazy_fred`

Open a terminal and run:

```bash
python -m pip install --upgrade pip
python -m pip install --upgrade lazy_fred
```

### Step 3: Get a free FRED API key
- Create/sign in to FRED: https://fred.stlouisfed.org/
- Generate your API key: https://fred.stlouisfed.org/docs/api/api_key.html
- Copy the key

### Step 4: Set your API key

Pick one option:

Option A (recommended): create a `.env` file in the folder where you will run commands:

```bash
API_KEY=paste_your_real_key_here
```

Option B: set it in your terminal session.

Windows (PowerShell):
```powershell
$env:API_KEY="paste_your_real_key_here"
```

macOS/Linux (bash/zsh):
```bash
export API_KEY="paste_your_real_key_here"
```

### Step 5: Run the app

```bash
lazy-fred
# or
lazy_fred
```

You will see this menu:
- `a` = add a search category
- `r` = remove a search category
- `c` = clear categories
- `run` = start data collection
- `q` = quit

For first run: type `run` and press Enter.

### Step 6: Find your output files

After it finishes, these files will be created in your current folder:
- `filtered_series.csv`
- `daily_data.csv`
- `weekly_data.csv`
- `monthly_data.csv`

### Common beginner issues
- **`lazy-fred` / `lazy_fred` command not found**: run `python -m pip install --upgrade lazy_fred` again, then open a new terminal.
- **API key error**: confirm key is valid and named exactly `API_KEY`.
- **No output yet**: first run can take time because it queries many series.
- **Running in notebooks/background tasks**: this tool is interactive (`input()`), so use a normal terminal.

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

### Favorite quick-run commands (most popular themes)

Use these to pull popular groups quickly without menu prompts:

```python
import lazy_fred as lf

api_key = "YOUR_FRED_API_KEY"
lf.run_favorites(api_key, "macro")    # GDP, inflation, unemployment, rates
```

Other profiles:

```python
lf.run_favorites(api_key, "rates")    # rates + FX + monetary
lf.run_favorites(api_key, "labor")    # employment + openings + income
lf.run_favorites(api_key, "markets")  # financial + banking + housing + retail
```

If you want custom categories:

```python
lf.run_fred_data_collection(
    api_key,
    categories=["gdp", "unemployment", "retail trade"],
    interactive=False
)
```

### Terminal power-user shortcuts

Inside the TUI menu:
- `rs` = reset categories to defaults
- `run-all` = reset to defaults and run everything immediately

## Development
- Lint: `poetry run ruff check .`
- Tests (requires a real key): `API_KEY=<your_key> poetry run pytest`

## Release process

### 1) Bump version
Update `version` in `pyproject.toml` (for example, `0.1.66` -> `0.1.67`), then commit and push.

### 2) Publish via GitHub Trusted Publishing (recommended)
This repository includes `.github/workflows/publish-pypi.yml`.

It publishes to PyPI automatically when you:
- publish a GitHub Release, or
- push a version tag matching `v*` (example: `v0.1.67`)

Example:
```bash
git tag v0.1.67
git push origin v0.1.67
```

Requirements:
- PyPI project `lazy_fred` must have this GitHub repository configured as a Trusted Publisher.

### 3) Poetry fallback publish (manual)
If you need a manual fallback path, run the same GitHub workflow from the Actions tab with:
- `publish_method = poetry`

This method uses:
- `poetry publish --build --skip-existing`
- repository secret `PYPI_API_TOKEN`

### 4) Verify release
After publish, confirm the new version appears at:
- https://pypi.org/project/lazy_fred/

## Disclaimer

This library is not affiliated with or endorsed by the Federal Reserve Bank of St. Louis or the FRED project.

## Acknowledgments

This project utilizes the `fredapi` and `fred` libraries for interacting with the FRED API.

## Contributions

Contributions are welcome. Feel free to open issues or submit pull requests.

## License

MIT

