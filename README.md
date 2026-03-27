# lazy_fred

Simple FRED data pulls for first-time Python users.

## Quickstart

```bash
python -m pip install --upgrade lazy_fred
lazy-fred doctor
lazy-fred quick
```

If prompted, paste your FRED API key.

## Command reference

| Command | What it does | Best for |
|---|---|---|
| `lazy-fred doctor` | Checks Python version, API key, write access, and FRED connectivity | First run/debug |
| `lazy-fred quick` | Pulls a small popular starter set | Fast validation |
| `lazy-fred standard` | Pulls a medium default set | Normal usage |
| `lazy-fred full` | Pulls all default categories | Full dataset |
| `lazy-fred favorites macro` | Pulls macro favorites | Macro analysis |
| `lazy-fred favorites rates` | Pulls rates/FX favorites | Rates work |
| `lazy-fred favorites labor` | Pulls labor favorites | Jobs/labor work |
| `lazy-fred favorites markets` | Pulls markets favorites | Market signals |

## What you get

Output CSV files:
- `filtered_series.csv`
- `daily_data.csv`
- `monthly_data.csv`
- `weekly_data.csv`

If files already exist, they are auto-backed up to `backups/<timestamp>/`.

## Time and retry behavior

- Terminal shows estimate, elapsed time, and ETA while running.
- API calls use exponential backoff retries for rate limits/transient errors.
- You can choose a start date in:
  - terminal prompt before `run`/`run-all`
  - notebook date picker in Colab UI

## Colab UI

```python
!pip install -U lazy_fred ipywidgets
import lazy_fred as lf
from google.colab import userdata

lf.launch_notebook_ui(userdata.get("fred_api_key"))
```

If `launch_notebook_ui` is missing in Colab:

```python
!pip install -U "git+https://github.com/Jmetrics86/lazy_fred.git"
```

## Troubleshooting

- **Command not found**
  - Reinstall and open a new terminal:
  - `python -m pip install --upgrade lazy_fred`
- **API key errors**
  - Confirm key is valid and active on FRED.
  - Run `lazy-fred doctor`.
- **Colab function missing**
  - Install latest from GitHub (command above) and restart runtime.
- **Run seems slow**
  - This is expected for large pulls due to FRED quotas.
  - Use `quick` or a favorites profile first.

## License

MIT

