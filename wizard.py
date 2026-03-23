"""
lazy_fred Wizard — a friendly step-by-step guide to downloading FRED data.

Run directly:      python wizard.py
Via Poetry:        poetry run lazy-fred-wizard
As a module:       python -m wizard
"""

import os
import sys
import time
import datetime
import logging

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv, set_key

logger = logging.getLogger(__name__)

# ── Popular series organised by theme ──────────────────────────────────────────
POPULAR_SERIES = {
    "GDP & Economic Growth": [
        ("GDP", "Gross Domestic Product (quarterly)"),
        ("GDPC1", "Real Gross Domestic Product (quarterly)"),
        ("A191RL1Q225SBEA", "Real GDP Growth Rate (quarterly)"),
    ],
    "Interest Rates": [
        ("DFF", "Federal Funds Effective Rate (daily)"),
        ("DGS10", "10-Year Treasury Rate (daily)"),
        ("DGS2", "2-Year Treasury Rate (daily)"),
        ("DGS30", "30-Year Treasury Rate (daily)"),
        ("T10Y2Y", "10Y minus 2Y Treasury Spread (daily)"),
        ("T10Y3M", "10Y minus 3M Treasury Spread (daily)"),
    ],
    "Mortgage Rates": [
        ("MORTGAGE30US", "30-Year Fixed Mortgage Rate (weekly)"),
        ("MORTGAGE15US", "15-Year Fixed Mortgage Rate (weekly)"),
    ],
    "Employment & Labor": [
        ("UNRATE", "Unemployment Rate (monthly)"),
        ("PAYEMS", "Total Nonfarm Payrolls (monthly)"),
        ("ICSA", "Initial Jobless Claims (weekly)"),
        ("JTSJOL", "Job Openings: Total (monthly)"),
    ],
    "Inflation & Prices": [
        ("CPIAUCSL", "Consumer Price Index — All Urban (monthly)"),
        ("CPILFESL", "Core CPI (ex Food & Energy) (monthly)"),
        ("PCEPI", "PCE Price Index (monthly)"),
        ("PCEPILFE", "Core PCE Price Index (monthly)"),
    ],
    "Money Supply": [
        ("M2SL", "M2 Money Stock (monthly)"),
        ("BOGMBASE", "Monetary Base (monthly)"),
    ],
    "Housing": [
        ("HOUST", "Housing Starts (monthly)"),
        ("CSUSHPISA", "Case-Shiller Home Price Index (monthly)"),
        ("MSPUS", "Median Sales Price of Houses Sold (quarterly)"),
    ],
    "Exchange Rates": [
        ("DEXUSEU", "USD / Euro Exchange Rate (daily)"),
        ("DEXJPUS", "Japanese Yen / USD (daily)"),
        ("DEXUSUK", "USD / British Pound (daily)"),
    ],
    "Market Indicators": [
        ("SP500", "S&P 500 Index (daily)"),
        ("VIXCLS", "CBOE Volatility Index — VIX (daily)"),
        ("BAMLH0A0HYM2", "High Yield Bond Spread (daily)"),
    ],
}

FREQUENCY_LABELS = {"D": "Daily", "W": "Weekly", "M": "Monthly",
                    "Q": "Quarterly", "A": "Annual", "BW": "Biweekly"}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _divider():
    print("-" * 60)

def _heading(text):
    print()
    _divider()
    print(f"  {text}")
    _divider()

def _prompt(message, default=None):
    """Prompt user; return stripped input or *default*."""
    suffix = f" [{default}]" if default else ""
    value = input(f"{message}{suffix}: ").strip()
    return value if value else (default or "")

def _prompt_choice(message, valid, default=None):
    """Keep asking until the user enters one of *valid* choices."""
    while True:
        answer = _prompt(message, default).lower()
        if answer in valid:
            return answer
        print(f"  Please enter one of: {', '.join(valid)}")


# ── Step 1 — API key ──────────────────────────────────────────────────────────

def step_api_key():
    """Return a validated FRED API key."""
    _heading("Step 1 of 4 — FRED API Key")
    load_dotenv()
    existing = os.getenv("API_KEY")

    if existing:
        masked = existing[:6] + "..." + existing[-4:]
        use = _prompt_choice(
            f"Found API key ({masked}). Use it? (y/n)", {"y", "n"}, "y")
        if use == "y":
            return existing

    print()
    print("  You need a free FRED API key.")
    print("  Get one at: https://fred.stlouisfed.org/docs/api/fred/")
    print()

    while True:
        key = _prompt("Enter your FRED API key")
        if not key:
            print("  Key cannot be empty.")
            continue
        print("  Validating key …", end=" ", flush=True)
        try:
            Fred(api_key=key).search(
                "gdp", order_by="popularity", sort_order="desc", limit=1)
            print("OK!")
            set_key(".env", "API_KEY", key)
            return key
        except Exception:
            print("INVALID — please check and try again.")


# ── Step 2 — Choose series ─────────────────────────────────────────────────────

def step_choose_series(fred_client):
    """Return a list of FRED series-ID strings."""
    _heading("Step 2 of 4 — Choose Data Series")
    print()
    print("  How would you like to pick series?")
    print()
    print("    [1] Browse popular series (recommended for beginners)")
    print("    [2] Enter series IDs manually (comma-separated)")
    print("    [3] Search FRED by keyword")
    print()

    mode = _prompt_choice("Your choice (1/2/3)", {"1", "2", "3"}, "1")

    if mode == "1":
        return _browse_popular()
    elif mode == "2":
        return _manual_entry(fred_client)
    else:
        return _keyword_search(fred_client)


def _browse_popular():
    """Let the user tick categories, then confirm individual series."""
    categories = list(POPULAR_SERIES.keys())

    print()
    print("  Available categories:")
    print()
    for i, cat in enumerate(categories, 1):
        count = len(POPULAR_SERIES[cat])
        print(f"    [{i:>2}] {cat}  ({count} series)")
    print(f"    [{'A':>2}] Select ALL categories")
    print()

    raw = _prompt(
        "Enter category numbers separated by commas, or A for all", "A")

    if raw.upper() == "A":
        chosen_cats = categories
    else:
        indices = []
        for tok in raw.split(","):
            tok = tok.strip()
            if tok.isdigit() and 1 <= int(tok) <= len(categories):
                indices.append(int(tok) - 1)
        if not indices:
            print("  No valid selection — defaulting to ALL.")
            chosen_cats = categories
        else:
            chosen_cats = [categories[i] for i in indices]

    series_pool = []
    for cat in chosen_cats:
        series_pool.extend(POPULAR_SERIES[cat])

    print()
    print(f"  Selected {len(series_pool)} series across "
          f"{len(chosen_cats)} categories:")
    print()
    for i, (sid, desc) in enumerate(series_pool, 1):
        print(f"    {i:>3}. {sid:<25s} {desc}")

    print()
    keep = _prompt_choice(
        "Keep all of these? (y) or pick specific numbers? (n)",
        {"y", "n"}, "y")

    if keep == "y":
        return [sid for sid, _ in series_pool]

    raw = _prompt("Enter the row numbers to keep (comma-separated)")
    picked = []
    for tok in raw.split(","):
        tok = tok.strip()
        if tok.isdigit() and 1 <= int(tok) <= len(series_pool):
            picked.append(series_pool[int(tok) - 1][0])
    if not picked:
        print("  No valid selection — keeping all.")
        return [sid for sid, _ in series_pool]
    return picked


def _manual_entry(fred_client):
    """Accept comma-separated series IDs, validate them."""
    print()
    print("  Enter FRED series IDs separated by commas.")
    print("  Example: GDP, UNRATE, DGS10, SP500")
    print()

    while True:
        raw = _prompt("Series IDs")
        ids = [tok.strip().upper() for tok in raw.split(",") if tok.strip()]
        if ids:
            break
        print("  Please enter at least one series ID.")

    valid = _validate_series(fred_client, ids)
    if not valid:
        print("  None of the entered IDs were valid on FRED.")
        print("  Falling back to popular series.")
        return _browse_popular()
    return valid


def _keyword_search(fred_client):
    """Search FRED by keyword and let the user pick from results."""
    import fred as fred_lib

    print()
    keyword = _prompt("Enter a search keyword (e.g. 'inflation')")
    print(f"  Searching FRED for '{keyword}' …", flush=True)

    load_dotenv()
    fred_lib.key(os.getenv("API_KEY"))

    try:
        raw_results = fred_lib.search(keyword)
        series_list = raw_results.get("seriess", [])
    except Exception as exc:
        print(f"  Search failed: {exc}")
        print("  Falling back to popular series.")
        return _browse_popular()

    if not series_list:
        print("  No results found. Falling back to popular series.")
        return _browse_popular()

    series_list.sort(key=lambda s: int(s.get("popularity", 0)), reverse=True)
    series_list = series_list[:20]

    print()
    print(f"  Top results for '{keyword}':")
    print()
    for i, s in enumerate(series_list, 1):
        sid = s.get("id", "?")
        freq = s.get("frequency_short", "?")
        pop = s.get("popularity", 0)
        title = s.get("title", "")[:50]
        print(f"    [{i:>2}] {sid:<20s} freq={freq}  pop={str(pop):<3}  {title}")
    print()

    raw = _prompt(
        "Enter row numbers to download (comma-separated), or A for all", "A")

    all_ids = [s["id"] for s in series_list]
    if raw.upper() == "A":
        return all_ids

    picked = []
    for tok in raw.split(","):
        tok = tok.strip()
        if tok.isdigit() and 1 <= int(tok) <= len(all_ids):
            picked.append(all_ids[int(tok) - 1])
    return picked if picked else all_ids


def _validate_series(fred_client, ids):
    """Return the subset of *ids* that actually exist on FRED."""
    valid = []
    for sid in ids:
        try:
            fred_client.get_series_info(sid)
            valid.append(sid)
            print(f"    {sid} … OK")
        except Exception:
            print(f"    {sid} … not found, skipping")
        time.sleep(0.15)
    return valid


# ── Step 3 — Lookback period ──────────────────────────────────────────────────

def step_lookback():
    """Return a start-date string (YYYY-MM-DD) or None for all history."""
    _heading("Step 3 of 4 — Lookback Period")
    print()
    print("  How far back should the data go?")
    print()
    print("    [1]  1 year")
    print("    [2]  5 years")
    print("    [3] 10 years")
    print("    [4] 20 years")
    print("    [5] All available history")
    print("    [6] Custom start date")
    print()

    choice = _prompt_choice(
        "Your choice (1-6)", {"1", "2", "3", "4", "5", "6"}, "3")

    today = datetime.date.today()
    offsets = {"1": 1, "2": 5, "3": 10, "4": 20}

    if choice in offsets:
        start = today.replace(year=today.year - offsets[choice])
        print(f"  Data will start from {start.isoformat()}")
        return start.isoformat()

    if choice == "5":
        print("  Fetching all available history.")
        return None

    while True:
        raw = _prompt("Enter start date (YYYY-MM-DD)")
        try:
            datetime.datetime.strptime(raw, "%Y-%m-%d")
            print(f"  Data will start from {raw}")
            return raw
        except ValueError:
            print("  Invalid date format. Please use YYYY-MM-DD.")


# ── Step 4 — Fetch & export ───────────────────────────────────────────────────

def step_fetch_and_export(fred_client, series_ids, start_date):
    """Fetch data, split by frequency, write CSVs. Returns summary dict."""
    _heading("Step 4 of 4 — Downloading Data")

    buckets = {"D": [], "W": [], "M": [], "Q": [], "A": [], "BW": [], "other": []}
    series_meta = {}

    print()
    print(f"  Fetching metadata for {len(series_ids)} series …")
    for sid in series_ids:
        try:
            info = fred_client.get_series_info(sid)
            freq = info.get("frequency_short", "other")
            if freq not in buckets:
                freq = "other"
            buckets[freq].append(sid)
            series_meta[sid] = {
                "title": info.get("title", ""),
                "frequency": freq,
            }
            time.sleep(0.12)
        except Exception:
            print(f"    Could not get info for {sid}, skipping.")

    non_empty = {k: v for k, v in buckets.items() if v}
    print()
    print("  Frequency breakdown:")
    for freq, ids in sorted(non_empty.items()):
        label = FREQUENCY_LABELS.get(freq, freq)
        print(f"    {label:>12}: {len(ids)} series")
    print()

    output_files = {}
    total = sum(len(v) for v in non_empty.values())
    done = 0

    for freq, ids in sorted(non_empty.items()):
        if not ids:
            continue
        label = FREQUENCY_LABELS.get(freq, freq).lower()
        frames = []

        for sid in ids:
            done += 1
            pct = int(done / total * 100)
            title = series_meta.get(sid, {}).get("title", "")[:40]
            print(f"  [{pct:>3}%] Fetching {sid} — {title} …",
                  end="", flush=True)
            data = _fetch_one_series(fred_client, sid, start_date)
            if data is not None and not data.empty:
                frames.append(data)
                print(f"  {len(data)} obs")
            else:
                print("  no data")

        if frames:
            combined = pd.concat(frames, axis=0).reset_index()
            combined = combined.rename(columns={"index": "date", 0: "value"})
            fname = f"{label}_data.csv"
            combined.to_csv(fname, index=False)
            output_files[freq] = {
                "file": fname,
                "series_count": len(ids),
                "row_count": len(combined),
            }

    return output_files


MAX_RETRIES = 5
INITIAL_WAIT = 0.3

def _fetch_one_series(fred_client, series_id, start_date):
    """Fetch a single series with retries and optional start date."""
    kwargs = {}
    if start_date:
        kwargs["observation_start"] = start_date

    for attempt in range(MAX_RETRIES):
        try:
            data = pd.DataFrame(fred_client.get_series(series_id, **kwargs))
            data["series"] = series_id
            time.sleep(0.25)
            return data
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Too Many Requests" in msg:
                wait = INITIAL_WAIT * 2 ** attempt
                print(f" rate-limited, retry in {wait:.1f}s …", end="",
                      flush=True)
                time.sleep(wait)
            else:
                logger.error("Error fetching %s: %s", series_id, exc)
                return None
    logger.error("Max retries for %s", series_id)
    return None


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(output_files):
    _heading("Done!")
    print()
    if not output_files:
        print("  No data was downloaded.  Check your series IDs and API key.")
        return

    print("  Files created:")
    print()
    for freq in sorted(output_files):
        info = output_files[freq]
        label = FREQUENCY_LABELS.get(freq, freq)
        print(f"    {info['file']:<25s}  "
              f"{info['series_count']:>3} series, "
              f"{info['row_count']:>7,} rows  ({label})")

    print()
    print("  Tip: open these CSV files in Excel, Google Sheets,")
    print("  or load them with pandas:  pd.read_csv('daily_data.csv')")
    print()


# ── Main wizard ───────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("   Welcome to the lazy_fred Data Wizard!")
    print("   Easily download economic data from FRED.")
    print("=" * 60)

    api_key = step_api_key()
    fred_client = Fred(api_key=api_key)

    series_ids = step_choose_series(fred_client)
    if not series_ids:
        print("\n  No series selected. Exiting.")
        sys.exit(0)

    print()
    print(f"  You selected {len(series_ids)} series:")
    for sid in series_ids:
        print(f"    - {sid}")

    start_date = step_lookback()

    _heading("Confirm")
    print()
    print(f"  Series:   {len(series_ids)}")
    print(f"  Lookback: {'all history' if not start_date else 'from ' + start_date}")
    print()
    go = _prompt_choice("Ready to download? (y/n)", {"y", "n"}, "y")
    if go != "y":
        print("  Cancelled.")
        sys.exit(0)

    output_files = step_fetch_and_export(fred_client, series_ids, start_date)
    print_summary(output_files)


if __name__ == "__main__":
    main()
