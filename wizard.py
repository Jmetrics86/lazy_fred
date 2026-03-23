"""
lazy_fred Wizard — a friendly step-by-step guide to downloading FRED data.

Run directly:      python wizard.py
Via Poetry:        poetry run lazy-fred-wizard
As a module:       python -m wizard

Requires: rich, InquirerPy (installed automatically via poetry install)
"""

import os
import sys
import time
import datetime
import logging
import math
import csv
import threading

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv, set_key

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text
from rich import box

from InquirerPy import inquirer
from InquirerPy.separator import Separator

logger = logging.getLogger(__name__)
console = Console()


# ── Rate limiter ──────────────────────────────────────────────────────────────
# FRED allows 2 requests/second. We enforce 1.8/sec to leave headroom.

class _RateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, max_per_second: float = 1.8):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last = time.monotonic()


_rate = _RateLimiter()


def _fred_call(fn, *args, retries: int = 4, **kwargs):
    """Call a FRED API function with rate-limiting and retry on 429/5xx."""
    for attempt in range(retries):
        _rate.wait()
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            msg = str(exc)
            is_transient = any(code in msg for code in
                               ("429", "500", "502", "503",
                                "Too Many Requests", "Internal Server Error",
                                "Service Unavailable", "Bad Gateway"))
            if is_transient and attempt < retries - 1:
                wait = 1.0 * (2 ** attempt)
                time.sleep(wait)
                continue
            raise


# ── Error log ─────────────────────────────────────────────────────────────────

class ErrorLog:
    """Collect errors during a wizard run and write to CSV."""

    def __init__(self):
        self._rows: list[dict] = []

    def add(self, phase: str, series_id: str, error_type: str, message: str):
        self._rows.append({
            "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
            "phase": phase,
            "series_id": series_id,
            "error_type": error_type,
            "message": str(message)[:300],
        })

    @property
    def count(self) -> int:
        return len(self._rows)

    def write_csv(self, path: str = "error_log.csv"):
        if not self._rows:
            return None
        fieldnames = ["timestamp", "phase", "series_id",
                      "error_type", "message"]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._rows)
        return path

    def summary_table(self) -> dict[str, int]:
        """Return counts by error_type."""
        counts: dict[str, int] = {}
        for r in self._rows:
            counts[r["error_type"]] = counts.get(r["error_type"], 0) + 1
        return counts


errors = ErrorLog()

# ── Timing constants ───────────────────────────────────────────────────────────
# FRED allows 2 requests/second. Each series needs ~2 calls (metadata + data).
# With built-in sleeps and network latency we observe ~1s per series.
SECS_PER_SERIES = 1.0
SECS_PER_SEARCH_CATEGORY = 1.5  # search call + 0.5s sleep
METADATA_OVERHEAD_PER_SERIES = 0.4


def _fmt_duration(seconds: float) -> str:
    """Human-friendly duration string."""
    if seconds < 60:
        return f"~{max(int(seconds), 1)} sec"
    minutes = seconds / 60
    if minutes < 2:
        return "~1 min"
    if minutes < 60:
        return f"~{math.ceil(minutes)} min"
    hours = minutes / 60
    return f"~{hours:.1f} hr"


def _estimate_download(n_series: int) -> str:
    return _fmt_duration(n_series * SECS_PER_SERIES
                         + n_series * METADATA_OVERHEAD_PER_SERIES)


# ── Popular series organised by theme ──────────────────────────────────────────

POPULAR_SERIES = {
    "GDP & Economic Growth": [
        ("GDP", "Gross Domestic Product", "Q"),
        ("GDPC1", "Real Gross Domestic Product", "Q"),
        ("A191RL1Q225SBEA", "Real GDP Growth Rate", "Q"),
    ],
    "Interest Rates": [
        ("DFF", "Federal Funds Effective Rate", "D"),
        ("DGS10", "10-Year Treasury Rate", "D"),
        ("DGS2", "2-Year Treasury Rate", "D"),
        ("DGS30", "30-Year Treasury Rate", "D"),
        ("T10Y2Y", "10Y minus 2Y Treasury Spread", "D"),
        ("T10Y3M", "10Y minus 3M Treasury Spread", "D"),
    ],
    "Mortgage Rates": [
        ("MORTGAGE30US", "30-Year Fixed Mortgage Rate", "W"),
        ("MORTGAGE15US", "15-Year Fixed Mortgage Rate", "W"),
    ],
    "Employment & Labor": [
        ("UNRATE", "Unemployment Rate", "M"),
        ("PAYEMS", "Total Nonfarm Payrolls", "M"),
        ("ICSA", "Initial Jobless Claims", "W"),
        ("JTSJOL", "Job Openings: Total", "M"),
    ],
    "Inflation & Prices": [
        ("CPIAUCSL", "Consumer Price Index — All Urban", "M"),
        ("CPILFESL", "Core CPI (ex Food & Energy)", "M"),
        ("PCEPI", "PCE Price Index", "M"),
        ("PCEPILFE", "Core PCE Price Index", "M"),
    ],
    "Money Supply": [
        ("M2SL", "M2 Money Stock", "M"),
        ("BOGMBASE", "Monetary Base", "M"),
    ],
    "Housing": [
        ("HOUST", "Housing Starts", "M"),
        ("CSUSHPISA", "Case-Shiller Home Price Index", "M"),
        ("MSPUS", "Median Sales Price of Houses Sold", "Q"),
    ],
    "Exchange Rates": [
        ("DEXUSEU", "USD / Euro Exchange Rate", "D"),
        ("DEXJPUS", "Japanese Yen / USD", "D"),
        ("DEXUSUK", "USD / British Pound", "D"),
    ],
    "Market Indicators": [
        ("SP500", "S&P 500 Index", "D"),
        ("VIXCLS", "CBOE Volatility Index — VIX", "D"),
        ("BAMLH0A0HYM2", "High Yield Bond Spread", "D"),
    ],
}

TOTAL_POPULAR = sum(len(v) for v in POPULAR_SERIES.values())

# All 30 categories the original lazy_fred searches
KITCHEN_SINK_CATEGORIES = [
    "interest rates", "exchange rates", "monetary data",
    "financial indicator", "banking industry", "gdp", "banking",
    "business lending", "foreign exchange intervention",
    "current population", "employment", "education", "income",
    "job opening", "labor turnover", "productivity index",
    "cost index", "minimum wage", "tax rate", "retail trade",
    "services", "technology", "housing", "expenditures",
    "business survey", "wholesale trade", "transportation",
    "automotive", "house price indexes", "cryptocurrency",
]

FREQUENCY_LABELS = {
    "D": "Daily", "W": "Weekly", "M": "Monthly",
    "Q": "Quarterly", "A": "Annual", "BW": "Biweekly",
}

FREQ_STYLE = {
    "D": "green", "W": "cyan", "M": "yellow",
    "Q": "magenta", "A": "red", "BW": "blue",
}


# ── Welcome ───────────────────────────────────────────────────────────────────

def show_welcome():
    title = Text("lazy_fred Data Wizard", style="bold bright_white")
    subtitle = Text(
        "Easily download economic data from the Federal Reserve (FRED)\n"
        "in just a few steps — no coding required.",
        style="dim",
    )
    content = Text.assemble(title, "\n\n", subtitle)
    console.print()
    console.print(Panel(
        content,
        border_style="bright_blue",
        padding=(1, 4),
        title="[bold bright_cyan]Welcome[/]",
        title_align="left",
    ))

    console.print()
    console.print(Panel(
        "[bold]FRED API Rate Limit:[/]  2 requests / second  (120 / minute)\n\n"
        "Download times depend on the number of series selected.\n"
        "Time estimates are shown next to each option so you know\n"
        "what to expect before you start.",
        title="[dim]About Timing[/dim]",
        border_style="dim",
        padding=(0, 2),
    ))
    console.print()


# ── Step 1 — API key ──────────────────────────────────────────────────────────

def step_api_key() -> str:
    console.print(Panel(
        "[bold]Step 1 of 4[/]  —  FRED API Key",
        border_style="bright_blue", box=box.ROUNDED,
    ))

    load_dotenv()
    existing = os.getenv("API_KEY")

    if existing:
        masked = existing[:6] + "•" * 20 + existing[-4:]
        console.print(f"  Found existing key: [dim]{masked}[/dim]")
        use_existing = inquirer.confirm(
            message="Use this API key?", default=True
        ).execute()
        if use_existing:
            return existing

    console.print()
    console.print("  [dim]You need a free FRED API key.[/dim]")
    console.print("  [link=https://fred.stlouisfed.org/docs/api/fred/]"
                  "https://fred.stlouisfed.org/docs/api/fred/[/link]")
    console.print()

    while True:
        key = inquirer.secret(
            message="Paste your FRED API key:",
            validate=lambda val: len(val) >= 10,
            invalid_message="Key looks too short — please try again.",
            transformer=lambda val: (val[:6] + "•" * 10 + val[-4:]
                                     if len(val) > 10 else val),
        ).execute()

        with console.status("[bold cyan]Validating key…[/]"):
            try:
                Fred(api_key=key).search(
                    "gdp", order_by="popularity", sort_order="desc", limit=1)
            except Exception:
                console.print("  [red]Invalid key — please check and "
                              "try again.[/red]")
                continue

        console.print("  [green]Key is valid![/green]")
        set_key(".env", "API_KEY", key)
        return key


# ── Step 2 — Choose series ─────────────────────────────────────────────────────

def step_choose_series(fred_client: Fred) -> list[str]:
    console.print()
    console.print(Panel(
        "[bold]Step 2 of 4[/]  —  Choose Data Series",
        border_style="bright_blue", box=box.ROUNDED,
    ))

    popular_est = _estimate_download(TOTAL_POPULAR)
    kitchen_est_search = _fmt_duration(
        len(KITCHEN_SINK_CATEGORIES) * SECS_PER_SEARCH_CATEGORY)

    mode = inquirer.select(
        message="How would you like to pick series?",
        choices=[
            {"name": (f"Browse popular series  "
                      f"({TOTAL_POPULAR} series, {popular_est})"),
             "value": "popular"},
            {"name": "Enter series IDs manually  (comma-separated)",
             "value": "manual"},
            {"name": "Search FRED by keyword",
             "value": "search"},
            Separator(),
            {"name": (f"Kitchen Sink — discover & download everything  "
                      f"({len(KITCHEN_SINK_CATEGORIES)} categories, "
                      f"search {kitchen_est_search} + download varies)"),
             "value": "kitchen_sink"},
        ],
        default="popular",
    ).execute()

    if mode == "popular":
        return _browse_popular()
    elif mode == "manual":
        return _manual_entry(fred_client)
    elif mode == "search":
        return _keyword_search(fred_client)
    else:
        return _kitchen_sink(fred_client)


def _browse_popular() -> list[str]:
    categories = list(POPULAR_SERIES.keys())

    selected_cats = inquirer.checkbox(
        message="Select categories (Space to toggle, Enter to confirm):",
        choices=[
            {"name": (f"{cat}  ({len(POPULAR_SERIES[cat])} series, "
                      f"{_estimate_download(len(POPULAR_SERIES[cat]))})"),
             "value": cat, "enabled": True}
            for cat in categories
        ],
        validate=lambda result: len(result) > 0,
        invalid_message="Select at least one category.",
    ).execute()

    pool = []
    for cat in selected_cats:
        for sid, desc, freq in POPULAR_SERIES[cat]:
            pool.append((sid, desc, freq, cat))

    _display_series_table(pool)
    est = _estimate_download(len(pool))
    console.print(f"  [dim]Estimated download time for {len(pool)} "
                  f"series: [bold]{est}[/bold][/dim]")
    console.print()

    selected = inquirer.checkbox(
        message="Select individual series (Space to toggle, Enter to confirm):",
        choices=[
            {"name": (f"{sid:<22s}  [{FREQUENCY_LABELS.get(freq, freq):>9}]"
                      f"  {desc}"),
             "value": sid, "enabled": True}
            for sid, desc, freq, _ in pool
        ],
        validate=lambda result: len(result) > 0,
        invalid_message="Select at least one series.",
    ).execute()

    return selected


def _manual_entry(fred_client: Fred) -> list[str]:
    console.print()
    console.print("  [dim]Enter FRED series IDs separated by commas.[/dim]")
    console.print("  [dim]Example: GDP, UNRATE, DGS10, SP500[/dim]")
    console.print()

    while True:
        raw = inquirer.text(
            message="Series IDs:",
            validate=lambda val: len(val.strip()) > 0,
            invalid_message="Please enter at least one ID.",
        ).execute()

        ids = [tok.strip().upper() for tok in raw.split(",") if tok.strip()]
        if not ids:
            continue

        est = _estimate_download(len(ids))
        console.print(f"  [dim]Estimated time for {len(ids)} series "
                      f"(if all valid): [bold]{est}[/bold][/dim]")

        valid = _validate_series(fred_client, ids)
        if valid:
            return valid

        console.print("  [yellow]None of those IDs were valid. "
                      "Try again.[/yellow]")


def _keyword_search(fred_client: Fred) -> list[str]:
    import fred as fred_lib

    keyword = inquirer.text(
        message="Search keyword (e.g. 'inflation', 'housing'):",
        validate=lambda val: len(val.strip()) > 0,
    ).execute()

    load_dotenv()
    fred_lib.key(os.getenv("API_KEY"))

    with console.status(f"[bold cyan]Searching FRED for '{keyword}'…[/]"):
        try:
            raw_results = fred_lib.search(keyword)
            series_list = raw_results.get("seriess", [])
        except Exception as exc:
            console.print(f"  [red]Search failed: {exc}[/red]")
            console.print("  [yellow]Falling back to popular series.[/yellow]")
            return _browse_popular()

    if not series_list:
        console.print("  [yellow]No results. Falling back to popular "
                      "series.[/yellow]")
        return _browse_popular()

    series_list.sort(
        key=lambda s: int(s.get("popularity", 0)), reverse=True)
    series_list = series_list[:25]

    table = Table(
        title=f"Search results for '{keyword}'",
        box=box.SIMPLE_HEAVY, border_style="bright_blue",
    )
    table.add_column("#", style="dim", width=3)
    table.add_column("Series ID", style="bold")
    table.add_column("Freq", justify="center")
    table.add_column("Pop", justify="right")
    table.add_column("Title")

    for i, s in enumerate(series_list, 1):
        freq = s.get("frequency_short", "?")
        style = FREQ_STYLE.get(freq, "white")
        table.add_row(
            str(i),
            s.get("id", "?"),
            f"[{style}]{freq}[/]",
            str(s.get("popularity", "")),
            s.get("title", "")[:55],
        )
    console.print(table)

    est = _estimate_download(len(series_list))
    console.print(f"  [dim]Estimated time if all {len(series_list)} selected:"
                  f" [bold]{est}[/bold][/dim]")
    console.print()

    selected = inquirer.checkbox(
        message="Select series to download "
                "(Space to toggle, Enter to confirm):",
        choices=[
            {"name": f"{s['id']:<22s}  {s.get('title', '')[:50]}",
             "value": s["id"],
             "enabled": i < 10}
            for i, s in enumerate(series_list)
        ],
        validate=lambda result: len(result) > 0,
        invalid_message="Select at least one series.",
    ).execute()

    return selected


# ── Kitchen Sink ──────────────────────────────────────────────────────────────

def _kitchen_sink(fred_client: Fred) -> list[str]:
    """Search all 30 FRED categories, discover every popular series."""
    import fred as fred_lib

    console.print()
    console.print(Panel(
        "[bold bright_yellow]Kitchen Sink Mode[/]\n\n"
        f"This will search across [bold]{len(KITCHEN_SINK_CATEGORIES)}"
        f"[/bold] economic categories on FRED,\n"
        "discover all series with popularity >= 50, and let you\n"
        "download everything in one go.\n\n"
        "[dim]The FRED API allows 2 requests/second, so discovery\n"
        f"alone takes {_fmt_duration(len(KITCHEN_SINK_CATEGORIES) * SECS_PER_SEARCH_CATEGORY)}. "
        "Download time depends on how many\n"
        "series are found — typically 100–300 series.[/dim]",
        border_style="bright_yellow",
        padding=(1, 2),
    ))

    min_popularity = inquirer.select(
        message="Minimum popularity threshold (higher = fewer series, faster):",
        choices=[
            {"name": "80+   (most popular only, fewest series, fastest)",
             "value": 80},
            {"name": "60+   (popular series, moderate count)",
             "value": 60},
            {"name": "50+   (default, matches original lazy_fred)",
             "value": 50},
            {"name": "25+   (includes niche series, large download)",
             "value": 25},
            {"name": "0+    (everything — largest possible download)",
             "value": 0},
        ],
        default=50,
    ).execute()

    console.print()
    load_dotenv()
    fred_lib.key(os.getenv("API_KEY"))

    all_series: dict[str, dict] = {}
    n_cats = len(KITCHEN_SINK_CATEGORIES)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("{task.completed}/{task.total} categories"),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            "[bold cyan]Searching FRED categories…[/]", total=n_cats)

        for cat in KITCHEN_SINK_CATEGORIES:
            progress.update(task, description=f"[cyan]Searching '{cat}'[/]")
            try:
                _rate.wait()
                results = fred_lib.search(cat)
                for s in results.get("seriess", []):
                    sid = s.get("id")
                    pop = int(s.get("popularity", 0))
                    freq = s.get("frequency_short", "?")
                    obs_start = s.get("observation_start", "")
                    if (sid
                            and pop >= min_popularity
                            and freq in ("D", "W", "M", "Q", "A", "BW")
                            and _is_date_after_1900(obs_start)):
                        if sid not in all_series or pop > all_series[sid]["pop"]:
                            all_series[sid] = {
                                "title": s.get("title", ""),
                                "freq": freq,
                                "pop": pop,
                            }
            except Exception as exc:
                errors.add("search", cat, type(exc).__name__, str(exc))
            progress.advance(task)

    freq_counts: dict[str, int] = {}
    for info in all_series.values():
        f = info["freq"]
        freq_counts[f] = freq_counts.get(f, 0) + 1

    n_found = len(all_series)
    est = _estimate_download(n_found)

    console.print()
    table = Table(
        title=f"[bold]Discovered {n_found:,} series "
              f"(popularity >= {min_popularity})[/]",
        box=box.ROUNDED, border_style="bright_yellow",
    )
    table.add_column("Frequency", style="bold")
    table.add_column("Series", justify="right")
    table.add_column("Est. Download", justify="right", style="dim")

    for freq in sorted(freq_counts):
        label = FREQUENCY_LABELS.get(freq, freq)
        style = FREQ_STYLE.get(freq, "white")
        cnt = freq_counts[freq]
        table.add_row(
            f"[{style}]{label}[/]",
            str(cnt),
            _estimate_download(cnt),
        )
    table.add_section()
    table.add_row("[bold]Total[/]", f"[bold]{n_found:,}[/]",
                  f"[bold]{est}[/]")
    console.print(table)

    console.print()
    console.print(f"  [dim]FRED API limit: 2 req/sec → full download "
                  f"will take [bold]{est}[/bold][/dim]")
    console.print()

    if n_found == 0:
        console.print("  [yellow]No series found. Try lowering the "
                      "popularity threshold.[/yellow]")
        return []

    freq_filter = inquirer.checkbox(
        message="Which frequencies to include? "
                "(Space to toggle, Enter to confirm):",
        choices=[
            {"name": (f"{FREQUENCY_LABELS.get(f, f):>12}  —  "
                      f"{freq_counts[f]:>4} series  "
                      f"({_estimate_download(freq_counts[f])})"),
             "value": f, "enabled": f in ("D", "W", "M")}
            for f in sorted(freq_counts)
        ],
        validate=lambda result: len(result) > 0,
        invalid_message="Select at least one frequency.",
    ).execute()

    selected = [sid for sid, info in all_series.items()
                if info["freq"] in freq_filter]
    selected.sort(key=lambda s: (-all_series[s]["pop"], s))

    final_est = _estimate_download(len(selected))
    console.print(f"\n  [bold]{len(selected):,}[/] series selected "
                  f"— estimated download: [bold]{final_est}[/]")

    return selected


def _is_date_after_1900(date_str: str) -> bool:
    try:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        return dt >= datetime.date(1900, 1, 1)
    except (ValueError, TypeError):
        return False


def _validate_series(fred_client: Fred, ids: list[str]) -> list[str]:
    valid = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Validating series…", total=len(ids))
        for sid in ids:
            try:
                _fred_call(fred_client.get_series_info, sid)
                valid.append(sid)
                progress.update(task, description=f"[green]✓ {sid}[/]")
            except Exception:
                progress.update(
                    task, description=f"[red]✗ {sid} — not found[/]")
            progress.advance(task)

    console.print(f"  [bold]{len(valid)}[/] of {len(ids)} series validated.")
    return valid


def _display_series_table(pool):
    table = Table(
        title="Available Series",
        box=box.SIMPLE_HEAVY, border_style="bright_blue",
    )
    table.add_column("Series ID", style="bold")
    table.add_column("Freq", justify="center", width=10)
    table.add_column("Category", style="dim")
    table.add_column("Description")

    for sid, desc, freq, cat in pool:
        style = FREQ_STYLE.get(freq, "white")
        table.add_row(
            sid,
            f"[{style}]{FREQUENCY_LABELS.get(freq, freq)}[/]",
            cat,
            desc,
        )
    console.print(table)


# ── Step 3 — Lookback period ──────────────────────────────────────────────────

def step_lookback(n_series: int) -> str | None:
    console.print()
    console.print(Panel(
        "[bold]Step 3 of 4[/]  —  Lookback Period",
        border_style="bright_blue", box=box.ROUNDED,
    ))

    console.print("  [dim]Lookback length does not significantly affect "
                  "download time —[/dim]")
    console.print("  [dim]each series is one API call regardless of date "
                  "range.[/dim]")
    console.print()

    today = datetime.date.today()

    choice = inquirer.select(
        message="How far back should the data go?",
        choices=[
            {"name": "1 year",                "value": 1},
            {"name": "5 years",               "value": 5},
            {"name": "10 years",              "value": 10},
            {"name": "20 years",              "value": 20},
            {"name": "All available history",  "value": None},
            Separator(),
            {"name": "Custom start date…",     "value": "custom"},
        ],
        default=10,
    ).execute()

    if choice is None:
        console.print("  Fetching [bold]all available history[/bold].")
        return None

    if choice == "custom":
        while True:
            raw = inquirer.text(
                message="Start date (YYYY-MM-DD):",
                validate=lambda v: _is_valid_date(v),
                invalid_message="Use YYYY-MM-DD format.",
            ).execute()
            console.print(f"  Data will start from [bold]{raw}[/bold].")
            return raw

    start = today.replace(year=today.year - choice)
    console.print(f"  Data will start from [bold]{start.isoformat()}[/bold].")
    return start.isoformat()


def _is_valid_date(s):
    try:
        datetime.datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# ── Step 4 — Fetch & export ───────────────────────────────────────────────────

def step_fetch_and_export(fred_client: Fred, series_ids: list[str],
                          start_date: str | None) -> dict:
    console.print()
    console.print(Panel(
        "[bold]Step 4 of 4[/]  —  Downloading Data",
        border_style="bright_blue", box=box.ROUNDED,
    ))

    est = _estimate_download(len(series_ids))
    console.print(f"  [dim]Estimated time: [bold]{est}[/bold]  "
                  f"({len(series_ids)} series × ~{SECS_PER_SERIES:.0f}s "
                  f"each due to FRED API rate limit)[/dim]")
    console.print()

    t_start = time.time()

    buckets: dict[str, list[str]] = {}
    series_meta: dict[str, dict] = {}

    n_ids = len(series_ids)
    meta_skipped = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("{task.completed}/{task.total}"),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        meta_task = progress.add_task(
            "[cyan]Fetching metadata…[/]", total=n_ids)
        for sid in series_ids:
            progress.update(
                meta_task, description=f"[cyan]Metadata: {sid}[/]")
            try:
                info = _fred_call(fred_client.get_series_info, sid)
                freq = info.get("frequency_short", "other")
                buckets.setdefault(freq, []).append(sid)
                series_meta[sid] = {
                    "title": info.get("title", ""),
                    "frequency": freq,
                }
            except Exception as exc:
                meta_skipped += 1
                errors.add("metadata", sid,
                           type(exc).__name__, str(exc))
            progress.advance(meta_task)

    if meta_skipped:
        console.print(f"  [yellow]Skipped {meta_skipped} series "
                      f"(metadata errors — see error_log.csv)[/yellow]")

    _print_frequency_breakdown(buckets)

    output_files = {}
    total = sum(len(v) for v in buckets.values())

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TextColumn("{task.completed}/{task.total} series"),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("/"),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        main_task = progress.add_task(
            "[bold bright_cyan]Downloading…[/]", total=total)

        for freq in sorted(buckets):
            ids = buckets[freq]
            label = FREQUENCY_LABELS.get(freq, freq).lower()
            frames = []

            for sid in ids:
                title = series_meta.get(sid, {}).get("title", "")[:40]
                err_label = (f"  [dim red]({errors.count} errors)[/]"
                             if errors.count else "")
                progress.update(
                    main_task,
                    description=(f"[cyan]{sid}[/] — {title}{err_label}"),
                )
                data = _fetch_one_series(fred_client, sid, start_date)
                if data is not None and not data.empty:
                    frames.append(data)
                progress.advance(main_task)

            if frames:
                combined = pd.concat(frames, axis=0).reset_index()
                combined = combined.rename(
                    columns={"index": "date", 0: "value"})
                fname = f"{label}_data.csv"
                combined.to_csv(fname, index=False)
                output_files[freq] = {
                    "file": fname,
                    "series_count": len(ids),
                    "row_count": len(combined),
                }

    elapsed = time.time() - t_start
    return output_files, elapsed


def _print_frequency_breakdown(buckets: dict):
    table = Table(box=box.SIMPLE, border_style="dim")
    table.add_column("Frequency", style="bold")
    table.add_column("Series", justify="right")
    table.add_column("Est. Time", justify="right", style="dim")

    for freq in sorted(buckets):
        label = FREQUENCY_LABELS.get(freq, freq)
        style = FREQ_STYLE.get(freq, "white")
        cnt = len(buckets[freq])
        table.add_row(
            f"[{style}]{label}[/]",
            str(cnt),
            _estimate_download(cnt),
        )
    console.print(table)


MAX_RETRIES = 5

# Pandas nanosecond timestamps only cover ~1677-2262.
# Series older than this boundary need special handling.
_PANDAS_MIN_DATE = "1700-01-01"


def _fetch_one_series(fred_client: Fred, series_id: str,
                      start_date: str | None) -> pd.DataFrame | None:
    kwargs = {}
    if start_date:
        kwargs["observation_start"] = start_date

    try:
        raw = _fred_call(fred_client.get_series, series_id,
                         retries=MAX_RETRIES, **kwargs)
    except Exception as exc:
        errors.add("download", series_id, type(exc).__name__, str(exc))
        return None

    if raw is None or (hasattr(raw, "empty") and raw.empty):
        return None

    try:
        data = pd.DataFrame(raw)
        data["series"] = series_id
        return data
    except (pd.errors.OutOfBoundsDatetime, OverflowError):
        pass

    try:
        data = pd.DataFrame({"value": raw.values, "series": series_id},
                            index=raw.index.astype(str))
        data.index.name = raw.index.name
        errors.add("download", series_id, "DatetimeOverflow",
                   "Dates converted to strings (pre-1677 data)")
        return data
    except Exception as exc:
        errors.add("download", series_id, type(exc).__name__, str(exc))
        return None


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(output_files: dict, elapsed: float):
    console.print()
    if not output_files:
        console.print(Panel(
            "[bold red]No data downloaded.[/]\n"
            "Check your series IDs and API key.",
            border_style="red",
        ))
        return

    table = Table(
        title="[bold bright_green]Download Complete[/]",
        box=box.ROUNDED, border_style="bright_green",
    )
    table.add_column("File", style="bold")
    table.add_column("Frequency", justify="center")
    table.add_column("Series", justify="right")
    table.add_column("Rows", justify="right", style="cyan")

    total_rows = 0
    total_series = 0
    for freq in sorted(output_files):
        info = output_files[freq]
        label = FREQUENCY_LABELS.get(freq, freq)
        style = FREQ_STYLE.get(freq, "white")
        total_rows += info["row_count"]
        total_series += info["series_count"]
        table.add_row(
            info["file"],
            f"[{style}]{label}[/]",
            str(info["series_count"]),
            f"{info['row_count']:,}",
        )

    table.add_section()
    table.add_row(
        "[bold]Total[/]", "",
        f"[bold]{total_series}[/]",
        f"[bold]{total_rows:,}[/]",
    )
    console.print(table)

    elapsed_str = _fmt_duration(elapsed)
    rate = total_series / elapsed if elapsed > 0 else 0
    console.print(f"\n  [dim]Completed in [bold]{elapsed_str}[/bold] "
                  f"({elapsed:.0f}s) — "
                  f"{rate:.1f} series/sec[/dim]")

    log_path = errors.write_csv()
    if log_path:
        err_summary = errors.summary_table()
        err_table = Table(
            title=f"[bold yellow]Errors ({errors.count} total)[/]",
            box=box.ROUNDED, border_style="yellow",
        )
        err_table.add_column("Error Type", style="bold")
        err_table.add_column("Count", justify="right")
        for etype, cnt in sorted(err_summary.items()):
            err_table.add_row(etype, str(cnt))
        console.print()
        console.print(err_table)
        console.print(f"\n  [yellow]Full error details saved to "
                      f"[bold]{log_path}[/bold][/yellow]")

    console.print()
    console.print(Panel(
        "[dim]Open these CSV files in Excel, Google Sheets, or Python:[/]\n\n"
        "  [bold cyan]import pandas as pd[/]\n"
        "  [bold cyan]df = pd.read_csv('daily_data.csv')[/]",
        title="[bold]Next Steps[/]",
        border_style="dim",
        padding=(1, 2),
    ))
    console.print()


# ── Confirmation step ─────────────────────────────────────────────────────────

def show_confirmation(series_ids: list[str], start_date: str | None) -> bool:
    console.print()
    console.print(Panel(
        "[bold]Confirm Your Selections[/]",
        border_style="bright_yellow", box=box.ROUNDED,
    ))

    est = _estimate_download(len(series_ids))

    table = Table(box=box.SIMPLE, show_header=False, border_style="dim")
    table.add_column("Key", style="bold")
    table.add_column("Value")
    table.add_row("Series", f"{len(series_ids)} selected")
    table.add_row(
        "Lookback",
        "All history" if not start_date else f"From {start_date}",
    )
    table.add_row("Est. Time", f"[bold]{est}[/]")
    console.print(table)

    if len(series_ids) <= 50:
        console.print()
        console.print("  [dim]Series:[/dim]")
        for sid in series_ids:
            console.print(f"    [cyan]•[/] {sid}")
    else:
        console.print()
        console.print(f"  [dim]Showing first 20 of {len(series_ids)}:[/dim]")
        for sid in series_ids[:20]:
            console.print(f"    [cyan]•[/] {sid}")
        console.print(f"    [dim]… and {len(series_ids) - 20} more[/dim]")

    console.print()

    return inquirer.confirm(
        message="Start downloading?", default=True
    ).execute()


# ── Main wizard ───────────────────────────────────────────────────────────────

def main():
    show_welcome()

    api_key = step_api_key()
    fred_client = Fred(api_key=api_key)

    series_ids = step_choose_series(fred_client)
    if not series_ids:
        console.print("[yellow]No series selected. Exiting.[/yellow]")
        sys.exit(0)

    start_date = step_lookback(len(series_ids))

    if not show_confirmation(series_ids, start_date):
        console.print("[yellow]Cancelled.[/yellow]")
        sys.exit(0)

    output_files, elapsed = step_fetch_and_export(
        fred_client, series_ids, start_date)
    print_summary(output_files, elapsed)


if __name__ == "__main__":
    main()
