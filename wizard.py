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

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv, set_key

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.text import Text
from rich import box

from InquirerPy import inquirer
from InquirerPy.separator import Separator

logger = logging.getLogger(__name__)
console = Console()

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
            transformer=lambda val: val[:6] + "•" * 10 + val[-4:] if len(val) > 10 else val,
        ).execute()

        with console.status("[bold cyan]Validating key…[/]"):
            try:
                Fred(api_key=key).search(
                    "gdp", order_by="popularity", sort_order="desc", limit=1)
            except Exception:
                console.print("  [red]Invalid key — please check and try again.[/red]")
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

    mode = inquirer.select(
        message="How would you like to pick series?",
        choices=[
            {"name": "Browse popular series  (recommended)", "value": "popular"},
            {"name": "Enter series IDs manually  (comma-separated)", "value": "manual"},
            {"name": "Search FRED by keyword", "value": "search"},
        ],
        default="popular",
    ).execute()

    if mode == "popular":
        return _browse_popular()
    elif mode == "manual":
        return _manual_entry(fred_client)
    else:
        return _keyword_search(fred_client)


def _browse_popular() -> list[str]:
    categories = list(POPULAR_SERIES.keys())

    selected_cats = inquirer.checkbox(
        message="Select categories (Space to toggle, Enter to confirm):",
        choices=[
            {"name": f"{cat}  ({len(POPULAR_SERIES[cat])} series)",
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

    selected = inquirer.checkbox(
        message="Select individual series (Space to toggle, Enter to confirm):",
        choices=[
            {"name": f"{sid:<22s}  [{FREQUENCY_LABELS.get(freq, freq):>9}]  {desc}",
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

        valid = _validate_series(fred_client, ids)
        if valid:
            return valid

        console.print("  [yellow]None of those IDs were valid. Try again.[/yellow]")


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
        console.print("  [yellow]No results. Falling back to popular series.[/yellow]")
        return _browse_popular()

    series_list.sort(key=lambda s: int(s.get("popularity", 0)), reverse=True)
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

    selected = inquirer.checkbox(
        message="Select series to download (Space to toggle, Enter to confirm):",
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
                fred_client.get_series_info(sid)
                valid.append(sid)
                progress.update(task, description=f"[green]✓ {sid}[/]")
            except Exception:
                progress.update(task, description=f"[red]✗ {sid} — not found[/]")
            progress.advance(task)
            time.sleep(0.15)

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

def step_lookback() -> str | None:
    console.print()
    console.print(Panel(
        "[bold]Step 3 of 4[/]  —  Lookback Period",
        border_style="bright_blue", box=box.ROUNDED,
    ))

    today = datetime.date.today()

    choice = inquirer.select(
        message="How far back should the data go?",
        choices=[
            {"name": "1 year",           "value": 1},
            {"name": "5 years",          "value": 5},
            {"name": "10 years",         "value": 10},
            {"name": "20 years",         "value": 20},
            {"name": "All available history", "value": None},
            Separator(),
            {"name": "Custom start date…", "value": "custom"},
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

    buckets: dict[str, list[str]] = {}
    series_meta: dict[str, dict] = {}

    with console.status("[bold cyan]Fetching series metadata…[/]"):
        for sid in series_ids:
            try:
                info = fred_client.get_series_info(sid)
                freq = info.get("frequency_short", "other")
                buckets.setdefault(freq, []).append(sid)
                series_meta[sid] = {
                    "title": info.get("title", ""),
                    "frequency": freq,
                }
                time.sleep(0.12)
            except Exception:
                console.print(f"  [red]Could not get info for {sid}[/red]")

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
                progress.update(
                    main_task,
                    description=f"[cyan]{sid}[/] — {title}",
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

    return output_files


def _print_frequency_breakdown(buckets: dict):
    table = Table(box=box.SIMPLE, border_style="dim")
    table.add_column("Frequency", style="bold")
    table.add_column("Series", justify="right")

    for freq in sorted(buckets):
        label = FREQUENCY_LABELS.get(freq, freq)
        style = FREQ_STYLE.get(freq, "white")
        table.add_row(f"[{style}]{label}[/]", str(len(buckets[freq])))
    console.print(table)


MAX_RETRIES = 5
INITIAL_WAIT = 0.3


def _fetch_one_series(fred_client: Fred, series_id: str,
                      start_date: str | None) -> pd.DataFrame | None:
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
                time.sleep(wait)
            else:
                logger.error("Error fetching %s: %s", series_id, exc)
                return None
    logger.error("Max retries for %s", series_id)
    return None


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(output_files: dict):
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
    for freq in sorted(output_files):
        info = output_files[freq]
        label = FREQUENCY_LABELS.get(freq, freq)
        style = FREQ_STYLE.get(freq, "white")
        total_rows += info["row_count"]
        table.add_row(
            info["file"],
            f"[{style}]{label}[/]",
            str(info["series_count"]),
            f"{info['row_count']:,}",
        )

    table.add_section()
    table.add_row("[bold]Total[/]", "", "", f"[bold]{total_rows:,}[/]")
    console.print(table)

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

    table = Table(box=box.SIMPLE, show_header=False, border_style="dim")
    table.add_column("Key", style="bold")
    table.add_column("Value")
    table.add_row("Series", f"{len(series_ids)} selected")
    table.add_row(
        "Lookback",
        "All history" if not start_date else f"From {start_date}",
    )
    console.print(table)

    console.print()
    console.print("  [dim]Series:[/dim]")
    for sid in series_ids:
        console.print(f"    [cyan]•[/] {sid}")
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

    start_date = step_lookback()

    if not show_confirmation(series_ids, start_date):
        console.print("[yellow]Cancelled.[/yellow]")
        sys.exit(0)

    output_files = step_fetch_and_export(fred_client, series_ids, start_date)
    print_summary(output_files)


if __name__ == "__main__":
    main()
