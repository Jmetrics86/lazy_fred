import logging
import pandas as pd
import time
from fredapi import Fred
import fred
import datetime
import os
import sys
import shutil
from dotenv import load_dotenv, set_key
import json
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

logger = logging.getLogger(__name__)
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
console = Console()

sleep = 0.5
searchlimit = 1000 # 1000 is max
AVG_SEARCH_SECONDS_PER_CATEGORY = 1.0
AVG_PULL_SECONDS_PER_SERIES = 1.2
DEFAULT_MAX_RETRIES = 6
DEFAULT_INITIAL_BACKOFF_SECONDS = 1.0
FREQUENCY_LABELS = {
    "D": "daily",
    "W": "weekly",
    "M": "monthly",
    "Q": "quarterly",
    "A": "annual",
}
#search_categories = ['Interest Rates', 'Exchange Rates'] #this one is for quick testing
DEFAULT_SEARCH_CATEGORIES = ['interest rates', 'exchange rates', 'monetary data', 'financial indicator', 'banking industry','gdp' , 'banking', 'business lending', 'foreign exchange intervention', 'current population', 'employment', 'education', 'income', 'job opening', 'labor turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency']
search_categories = DEFAULT_SEARCH_CATEGORIES.copy()
FAVORITE_PROFILES = {
    "macro": ["gdp", "inflation", "unemployment", "interest rates"],
    "rates": ["interest rates", "exchange rates", "monetary data"],
    "labor": ["employment", "job opening", "labor turnover", "income"],
    "markets": ["financial indicator", "banking", "housing", "retail trade"],
}
STARTER_MODES = {
    "quick": FAVORITE_PROFILES["macro"],
    "standard": DEFAULT_SEARCH_CATEGORIES[:12],
    "full": DEFAULT_SEARCH_CATEGORIES,
}


def render_categories_table(categories=None):
    categories = categories if categories is not None else search_categories
    table = Table(title="Current Search Categories")
    table.add_column("#", style="cyan", justify="right")
    table.add_column("Category", style="green")
    for idx, category in enumerate(categories, start=1):
        table.add_row(str(idx), category)
    return table


def render_menu():
    return Panel(
        "[bold]Choose an action:[/bold]\n"
        "[cyan]a[/cyan] = add category\n"
        "[cyan]r[/cyan] = remove category (by number or name)\n"
        "[cyan]c[/cyan] = clear all categories\n"
        "[cyan]rs[/cyan] = reset to default categories\n"
        "[cyan]run[/cyan] = start data collection\n"
        "[cyan]run-all[/cyan] = reset + run all default categories\n"
        "[cyan]q[/cyan] = quit",
        title="lazy_fred menu",
        border_style="blue",
    )


def format_duration(seconds):
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def is_retryable_exception(exc):
    text = str(exc).lower()
    retry_markers = [
        "429",
        "too many requests",
        "rate limit",
        "503",
        "502",
        "504",
        "timeout",
        "temporarily unavailable",
    ]
    return any(marker in text for marker in retry_markers)


def backoff_sleep(attempt, initial_wait=DEFAULT_INITIAL_BACKOFF_SECONDS):
    wait_time = initial_wait * (2 ** attempt)
    console.print(f"[yellow]Retrying in {wait_time:.1f}s...[/yellow]")
    time.sleep(wait_time)


def parse_start_date(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise ValueError("Start date must be YYYY-MM-DD")


def prompt_start_date():
    raw = Prompt.ask(
        "Start date filter (YYYY-MM-DD, leave blank for full history)",
        default="",
    ).strip()
    if not raw:
        return None
    return parse_start_date(raw)


def resolve_categories(user_categories):
    """Resolve user-provided category text against default categories."""
    resolved = []
    default_lookup = {c.lower(): c for c in DEFAULT_SEARCH_CATEGORIES}

    for raw in user_categories:
        query = str(raw).strip().lower()
        if not query:
            continue

        if query in default_lookup:
            resolved.append(default_lookup[query])
            continue

        matches = [c for c in DEFAULT_SEARCH_CATEGORIES if query in c.lower()]
        if len(matches) == 1:
            console.print(f"[cyan]Mapped '{raw}' -> '{matches[0]}'[/cyan]")
            resolved.append(matches[0])
        elif len(matches) > 1:
            console.print(f"[yellow]'{raw}' matched multiple categories; using '{matches[0]}'.[/yellow]")
            resolved.append(matches[0])
        else:
            console.print(f"[yellow]No default match for '{raw}'. Using it as-is.[/yellow]")
            resolved.append(str(raw).strip())

    deduped = list(dict.fromkeys(resolved))
    return deduped


def execute_collection(api_key, categories_to_use, observation_start=None):
    backup_existing_outputs()
    collector = CollectCategories(api_key)
    console.print(Panel("Collecting search results from FRED...", border_style="cyan"))
    search_results = collector.get_fred_search(categories_to_use)
    collector.export_master(search_results)

    console.print(Panel("Pulling daily data...", border_style="cyan"))
    daily_exporter = daily_export(Fred(api_key=api_key))
    daily_exporter.dailyfilter()
    daily_exporter.daily_series_collector(observation_start=observation_start)

    console.print(Panel("Pulling monthly data...", border_style="cyan"))
    monthly_exporter = monthly_export(Fred(api_key=api_key))
    monthly_exporter.monthlyfilter()
    monthly_exporter.monthly_series_collector(observation_start=observation_start)

    console.print(Panel("Pulling weekly data...", border_style="cyan"))
    weekly_exporter = weekly_export(Fred(api_key=api_key))
    weekly_exporter.weeklyfilter()
    weekly_exporter.weekly_series_collector(observation_start=observation_start)

    output_table = Table(title="Run complete - output files")
    output_table.add_column("File", style="green")
    output_table.add_row("filtered_series.csv")
    output_table.add_row("daily_data.csv")
    output_table.add_row("monthly_data.csv")
    output_table.add_row("weekly_data.csv")
    console.print(output_table)


def build_series_metadata_map(path="filtered_series.csv"):
    """Return metadata by series ID for richer progress output."""
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}

    if "id" not in df.columns:
        return {}

    meta = {}
    for _, row in df.iterrows():
        series_id = str(row.get("id", "")).strip()
        if not series_id:
            continue
        title = str(row.get("title", "")).strip()
        frequency_short = str(row.get("frequency_short", "")).strip()
        units_short = str(row.get("units_short", "")).strip()
        seasonal_adjustment_short = str(row.get("seasonal_adjustment_short", "")).strip()
        popularity = row.get("popularity")
        try:
            popularity = int(popularity)
        except Exception:
            popularity = None
        meta[series_id] = {
            "title": title,
            "popularity": popularity,
            "frequency_short": frequency_short,
            "units_short": units_short,
            "seasonal_adjustment_short": seasonal_adjustment_short,
        }
    return meta


def build_series_insight(meta):
    """Generate a short plain-language insight for a series."""
    title = (meta.get("title") or "").lower()
    units = meta.get("units_short") or ""
    freq = meta.get("frequency_short") or ""
    freq_label = FREQUENCY_LABELS.get(freq, "periodic")

    if "unemployment" in title:
        meaning = "tracks labor market slack"
    elif "gdp" in title:
        meaning = "tracks overall economic growth"
    elif "consumer price" in title or "cpi" in title or "inflation" in title:
        meaning = "tracks inflation pressure"
    elif "federal funds" in title or "treasury" in title or "interest" in title:
        meaning = "tracks borrowing cost trends"
    elif "housing" in title or "home price" in title or "mortgage" in title:
        meaning = "tracks housing market conditions"
    elif "retail" in title:
        meaning = "tracks consumer demand"
    elif "payroll" in title or "employment" in title:
        meaning = "tracks job market activity"
    elif "exchange rate" in title or "usd" in title:
        meaning = "tracks currency market movement"
    else:
        meaning = "tracks an economic trend"

    if units:
        return f"{meaning}; reported {freq_label} in {units}"
    return f"{meaning}; reported {freq_label}"


def backup_existing_outputs():
    """Backup output files before overwriting them."""
    output_files = [
        "filtered_series.csv",
        "daily_data.csv",
        "monthly_data.csv",
        "weekly_data.csv",
    ]
    existing = [f for f in output_files if os.path.exists(f)]
    if not existing:
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = os.path.join("backups", timestamp)
    os.makedirs(backup_dir, exist_ok=True)
    for filename in existing:
        shutil.copy2(filename, os.path.join(backup_dir, filename))
    console.print(
        f"[yellow]Backed up existing output files to:[/yellow] {os.path.abspath(backup_dir)}"
    )


def run_doctor():
    """Basic environment checks for first-time users."""
    load_dotenv()
    table = Table(title="lazy_fred doctor")
    table.add_column("Check", style="cyan")
    table.add_column("Status")
    table.add_column("Details", style="dim")

    py_ok = sys.version_info >= (3, 10)
    table.add_row(
        "Python version",
        "[green]OK[/green]" if py_ok else "[red]FAIL[/red]",
        f"{sys.version.split()[0]} (needs >= 3.10)",
    )

    key = os.getenv("API_KEY") or os.getenv("FRED_API_KEY")
    has_key = bool((key or "").strip())
    table.add_row(
        "FRED API key",
        "[green]OK[/green]" if has_key else "[yellow]MISSING[/yellow]",
        "Set API_KEY or FRED_API_KEY",
    )

    write_ok = os.access(os.getcwd(), os.W_OK)
    table.add_row(
        "Output directory writable",
        "[green]OK[/green]" if write_ok else "[red]FAIL[/red]",
        os.getcwd(),
    )

    if has_key:
        try:
            Fred(api_key=key).search("gdp", limit=1)
            table.add_row("FRED API connectivity", "[green]OK[/green]", "Live call succeeded")
        except Exception as exc:
            table.add_row("FRED API connectivity", "[red]FAIL[/red]", str(exc)[:120])
    else:
        table.add_row("FRED API connectivity", "[yellow]SKIPPED[/yellow]", "No API key set")

    console.print(table)
    console.print(
        "\nTry one of: [cyan]lazy-fred quick[/cyan], [cyan]lazy-fred standard[/cyan], [cyan]lazy-fred full[/cyan]"
    )


def add_search_category(category):
    """Adds a new category to the search_categories list."""
    if category not in search_categories:
        search_categories.append(category)
        logger.info(f"Added category '{category}' to search list.")
    else:
        logger.warning(f"Category '{category}' already exists in search list.")

def remove_search_category(category):
    """Removes a category from the search_categories list."""
    if category in search_categories:
        search_categories.remove(category)
        logger.info(f"Removed category '{category}' from search list.")
    else:
        logger.warning(f"Category '{category}' not found in search list.")

def clear_search_categories():
    """Clears all categories from the search_categories list."""
    search_categories.clear()
    logger.info("Cleared all search categories.")


def reset_search_categories():
    """Resets categories to default values."""
    search_categories.clear()
    search_categories.extend(DEFAULT_SEARCH_CATEGORIES)
    logger.info("Reset search categories to defaults.")

class AccessFred:
    def set_api_key_in_environment(self):
        try:
            api_key = os.environ["API_KEY"]
            print("API_KEY exists and has the value:", api_key)
        except KeyError:
            api_key = input("API_KEY not found in .env. Please enter your API key: ")
            set_key(".env", "API_KEY", api_key)
            
    def get_and_validate_api_key(self):
        """Retrieves API key, stores in .env if valid, and handles errors."""
        load_dotenv() 
        api_key = os.getenv("API_KEY")
        fredapi = Fred(api_key=api_key)

        while not api_key:  
            api_key = input("API_KEY not found in .env. Please enter your API key: ")

            try:
                fredapi.search('category', order_by='popularity', sort_order='desc', limit=searchlimit)
                logger.info("API key is valid!")
                set_key(".env", "API_KEY", api_key) 
                return api_key
            except Exception:
                logger.error("Invalid API key. Please try again.")

class CollectCategories:
    def __init__(self, api_key):
        self.api_key = api_key
        self.fredapi = Fred(api_key=api_key)

    def get_fredapi_search_results(self, categories, searchlimit=1000):
        df_list = []
        total_categories = len(categories)
        for index, category in enumerate(categories, start=1):
            search_results = self.fredapi.search(category, order_by='popularity', sort_order='desc', limit=searchlimit)
            df_list.append(pd.DataFrame(search_results))
            time.sleep(sleep)
            print(f"Processing {category} ({index}/{total_categories})", flush=True)  # Updated f-string
        master_df = pd.concat(df_list).drop_duplicates()
        master_df['popularity'] = master_df['popularity'].astype(int)
        return master_df
    
    @staticmethod
    def save_dict_to_json(data_dict, filename="data.json"):
        with open(filename, "w") as file:
            json.dump(data_dict, file, indent=4)  

    @staticmethod
    def load_dict_from_json(filename="data.json"):
        with open(filename, "r") as file:
            return json.load(file)
    
    def get_fred_search(self, categories):
        if not self.api_key:
            raise ValueError("API_KEY is required.")
        fred.key(self.api_key)
        search_dict = []
        total_categories = len(categories)
        est_seconds = total_categories * AVG_SEARCH_SECONDS_PER_CATEGORY
        console.print(
            Panel(
                f"Search phase estimate: ~{format_duration(est_seconds)} "
                f"for {total_categories} categories",
                border_style="cyan",
            )
        )
        start = time.time()
        for index, category in enumerate(categories, start=1):
            search_results = None
            for attempt in range(DEFAULT_MAX_RETRIES):
                try:
                    search_results = fred.search(category)
                    break
                except Exception as exc:
                    if is_retryable_exception(exc) and attempt < DEFAULT_MAX_RETRIES - 1:
                        backoff_sleep(attempt)
                        continue
                    logger.error(f"Search failed for category '{category}': {exc}")
                    break
            if search_results is None:
                continue
            search_dict.append(search_results)
            time.sleep(sleep)
            elapsed = time.time() - start
            avg_so_far = elapsed / index
            eta = max((total_categories - index) * avg_so_far, 0)
            console.print(
                f"[dim]Processing {category} ({index}/{total_categories}) | "
                f"elapsed={format_duration(elapsed)} | eta={format_duration(eta)}[/dim]",
                soft_wrap=True,
            )
        CollectCategories.save_dict_to_json(search_dict)
        return search_dict

    @staticmethod
    def is_valid_date_after_1900(date_str):
        try:
            date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            return date >= datetime.date(1900, 1, 1)
        except ValueError:
            return False
    



    def export_master(self, search_results):
        filtered_series = []

        for results_dict in search_results:
            for series in results_dict['seriess']:
                observation_start = series.get('observation_start')  # Use .get to safely handle missing keys
                if observation_start and self.is_valid_date_after_1900(observation_start):
                    filtered_series.append(series)

        pd.DataFrame(filtered_series).to_csv('filtered_series.csv', index=False)


class daily_export:
    def __init__(self, fred):
        self.fred = fred

    def dailyfilter(self):
        master_df = pd.read_csv('filtered_series.csv')
        filtered_df = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'D')]
        daily_list = filtered_df['id'].tolist()
        return daily_list

    def daily_series_collector(self, observation_start=None):
        fred = Fred(api_key=os.getenv("API_KEY")) 

        merged_data = pd.DataFrame()
        series_meta = build_series_metadata_map()
        series_list = daily_export.dailyfilter(self)
        total_series = len(series_list)
        est_seconds = total_series * AVG_PULL_SECONDS_PER_SERIES
        console.print(
            Panel(
                f"Daily pull estimate: ~{format_duration(est_seconds)} "
                f"for {total_series} series",
                border_style="cyan",
            )
        )
        phase_start = time.time()
        for index, series_id in enumerate(series_list, start=1):
            for attempt in range(DEFAULT_MAX_RETRIES):
                try:
                    kwargs = {}
                    if observation_start:
                        kwargs["observation_start"] = observation_start
                    data = pd.DataFrame(fred.get_series(series_id, **kwargs))
                    data['series'] = series_id
                    merged_data = pd.concat([merged_data, data], axis=0)
                    meta = series_meta.get(series_id, {})
                    title = meta.get("title") or "Unknown series"
                    popularity = meta.get("popularity")
                    pop_text = f"popularity={popularity}" if popularity is not None else "popularity=n/a"
                    insight = build_series_insight(meta)
                    freq_label = FREQUENCY_LABELS.get(meta.get("frequency_short"), "periodic")
                    elapsed = time.time() - phase_start
                    avg_so_far = elapsed / index
                    eta = max((total_series - index) * avg_so_far, 0)
                    console.print(
                        f"[green]{series_id}[/green] ({index}/{total_series}) | "
                        f"{title} | {freq_label} | {pop_text} | "
                        f"[dim]{insight}[/dim] | "
                        f"elapsed={format_duration(elapsed)} | eta={format_duration(eta)}"
                    )
                    time.sleep(sleep)
                    break  # Break out of the retry loop if successful

                except Exception as e:
                    if is_retryable_exception(e) and attempt < DEFAULT_MAX_RETRIES - 1:
                        backoff_sleep(attempt)
                        continue
                    logger.error(f"Error fetching series {series_id}: {e}")
                    break
            

        merged_data = merged_data.reset_index()
        merged_data = merged_data.rename(columns={'index': 'date', 0: 'value'})
        merged_data.to_csv('daily_data.csv')
        total_elapsed = time.time() - phase_start
        avg_per_series = (total_elapsed / total_series) if total_series else 0
        console.print(
            f"[bold green]Daily series generated[/bold green] | "
            f"total={format_duration(total_elapsed)} | "
            f"avg/series={avg_per_series:.2f}s"
        )


class monthly_export:
    def __init__(self, fred):
        self.fred = fred

    # prompt: using the master_df create a list of series ids filtered down to only series with frequency of monthly and popularity above 50.
    def monthlyfilter(self):
        master_df = pd.read_csv('filtered_series.csv')
        monthly_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'M')]
        monthly_list = monthly_list['id'].tolist()
        return monthly_list



    def monthly_series_collector(self, observation_start=None):
        monthly_merged_data = pd.DataFrame()
        series_meta = build_series_metadata_map()
        series_list = monthly_export.monthlyfilter(self)
        total_series = len(series_list)
        est_seconds = total_series * AVG_PULL_SECONDS_PER_SERIES
        console.print(
            Panel(
                f"Monthly pull estimate: ~{format_duration(est_seconds)} "
                f"for {total_series} series",
                border_style="cyan",
            )
        )
        phase_start = time.time()
        fred = Fred(api_key=os.getenv("API_KEY")) 


        for index, series_id in enumerate(series_list, start=1):
            for attempt in range(DEFAULT_MAX_RETRIES):
                try:
                    kwargs = {}
                    if observation_start:
                        kwargs["observation_start"] = observation_start
                    data = pd.DataFrame(fred.get_series(series_id, **kwargs))
                    data['series'] = series_id
                    monthly_merged_data = pd.concat([monthly_merged_data, data], axis=0)  # Update merged data
                    meta = series_meta.get(series_id, {})
                    title = meta.get("title") or "Unknown series"
                    popularity = meta.get("popularity")
                    pop_text = f"popularity={popularity}" if popularity is not None else "popularity=n/a"
                    insight = build_series_insight(meta)
                    freq_label = FREQUENCY_LABELS.get(meta.get("frequency_short"), "periodic")
                    elapsed = time.time() - phase_start
                    avg_so_far = elapsed / index
                    eta = max((total_series - index) * avg_so_far, 0)
                    console.print(
                        f"[green]{series_id}[/green] ({index}/{total_series}) | "
                        f"{title} | {freq_label} | {pop_text} | "
                        f"[dim]{insight}[/dim] | "
                        f"elapsed={format_duration(elapsed)} | eta={format_duration(eta)}"
                    )
                    time.sleep(sleep)
                    break  # Break out of the retry loop if successful

                except Exception as e:
                    if is_retryable_exception(e) and attempt < DEFAULT_MAX_RETRIES - 1:
                        backoff_sleep(attempt)
                        continue
                    logger.error(f"Error fetching series {series_id}: {e}")
                    break


        monthly_merged_data = monthly_merged_data.reset_index()
        monthly_merged_data = monthly_merged_data.rename(columns={'index': 'date', 0: 'value'})
        monthly_merged_data.to_csv('monthly_data.csv')
        total_elapsed = time.time() - phase_start
        avg_per_series = (total_elapsed / total_series) if total_series else 0
        console.print(
            f"[bold green]Monthly series completed[/bold green] | "
            f"total={format_duration(total_elapsed)} | "
            f"avg/series={avg_per_series:.2f}s"
        )




class weekly_export:

    def __init__(self, fred):
        self.fred = fred

    # prompt: using the master_df create a list of series ids filtered down to only series with frequency of weekly and popularity above 50.
    def weeklyfilter(self):
        master_df = pd.read_csv('filtered_series.csv')
        weekly_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'W')]
        weekly_list = weekly_list['id'].tolist()
        return weekly_list

        
    def weekly_series_collector(self, observation_start=None):
        weekly_merged_data = pd.DataFrame()
        series_meta = build_series_metadata_map()
        series_list = weekly_export.weeklyfilter(self)
        total_series = len(series_list)
        est_seconds = total_series * AVG_PULL_SECONDS_PER_SERIES
        console.print(
            Panel(
                f"Weekly pull estimate: ~{format_duration(est_seconds)} "
                f"for {total_series} series",
                border_style="cyan",
            )
        )
        phase_start = time.time()
        fred = Fred(api_key=os.getenv("API_KEY")) 


        for index, series_id in enumerate(series_list, start=1):
            for attempt in range(DEFAULT_MAX_RETRIES):
                try:
                    kwargs = {}
                    if observation_start:
                        kwargs["observation_start"] = observation_start
                    data = pd.DataFrame(fred.get_series(series_id, **kwargs))
                    data['series'] = series_id
                    weekly_merged_data = pd.concat([weekly_merged_data, data], axis=0)  # Update merged data
                    meta = series_meta.get(series_id, {})
                    title = meta.get("title") or "Unknown series"
                    popularity = meta.get("popularity")
                    pop_text = f"popularity={popularity}" if popularity is not None else "popularity=n/a"
                    insight = build_series_insight(meta)
                    freq_label = FREQUENCY_LABELS.get(meta.get("frequency_short"), "periodic")
                    elapsed = time.time() - phase_start
                    avg_so_far = elapsed / index
                    eta = max((total_series - index) * avg_so_far, 0)
                    console.print(
                        f"[green]{series_id}[/green] ({index}/{total_series}) | "
                        f"{title} | {freq_label} | {pop_text} | "
                        f"[dim]{insight}[/dim] | "
                        f"elapsed={format_duration(elapsed)} | eta={format_duration(eta)}"
                    )
                    time.sleep(sleep)
                    break  # Break out of the retry loop if successful

                except Exception as e:
                    if is_retryable_exception(e) and attempt < DEFAULT_MAX_RETRIES - 1:
                        backoff_sleep(attempt)
                        continue
                    logger.error(f"Error fetching series {series_id}: {e}")
                    break

        weekly_merged_data = weekly_merged_data.reset_index()
        weekly_merged_data = weekly_merged_data.rename(columns={'index': 'date', 0: 'value'})
        weekly_merged_data.to_csv('weekly_data.csv')
        total_elapsed = time.time() - phase_start
        avg_per_series = (total_elapsed / total_series) if total_series else 0
        console.print(
            f"[bold green]Weekly series completed[/bold green] | "
            f"total={format_duration(total_elapsed)} | "
            f"avg/series={avg_per_series:.2f}s"
        )


def run_fred_data_collection(api_key, categories=None, interactive=True, observation_start=None):
    console.print(Panel("Welcome to [bold]lazy_fred[/bold]\nLet's collect FRED data.", title="Starting collection process"))
    load_dotenv()
    # Resolve API key from function arg, then environment, then prompt.
    if not api_key:
        api_key = os.getenv("API_KEY")
    if not api_key:
        api_key = Prompt.ask("API_KEY not found in environment. Please enter your API key").strip()
    if not api_key:
        logger.error("No API key provided. Exiting.")
        return

    os.environ["API_KEY"] = api_key
    set_key(".env", "API_KEY", api_key)

    try:
        fred_client = Fred(api_key=api_key)
        fred_client.search('category', order_by='popularity', sort_order='desc', limit=searchlimit)
        logger.info("API key is valid!")
        console.print("[green]API key validated.[/green]")
    except Exception:
        logger.error("Invalid API key provided. Please check and try again.")
        console.print("[red]Invalid API key provided. Please check and try again.[/red]")
        return  # Exit if the API key is invalid
    
    if categories:
        categories_to_use = resolve_categories(categories)
        if not categories_to_use:
            console.print("[red]No valid categories provided.[/red]")
            return
        if observation_start:
            observation_start = parse_start_date(observation_start)
        console.print("[green]Running non-interactive collection with selected categories.[/green]")
        console.print(render_categories_table(categories_to_use))
        execute_collection(api_key, categories_to_use, observation_start=observation_start)
        return

    if not interactive:
        console.print("[red]interactive=False requires categories=[...][/red]")
        return

    while True:
        console.print(render_categories_table())
        console.print(render_menu())
        action = Prompt.ask("Your choice").strip().lower()
        if action == "add":
            action = "a"
        elif action == "remove":
            action = "r"
        elif action == "clear":
            action = "c"
        elif action in ("reset", "rs"):
            action = "rs"
        elif action in ("runall", "run-all", "all"):
            action = "run-all"
        elif action == "quit":
            action = "q"

        os.environ["API_KEY"] = api_key
        set_key(".env", "API_KEY", api_key)

        if action == 'a':
            category = Prompt.ask("Enter category to add").strip()
            if not category:
                console.print("[yellow]No category entered.[/yellow]")
                continue
            add_search_category(category)
            console.print(f"[green]Added:[/green] {category}")
        elif action == 'r':
            category_input = Prompt.ask("Enter category number or exact name to remove").strip()
            if not category_input:
                console.print("[yellow]No category entered.[/yellow]")
                continue
            if category_input.isdigit():
                idx = int(category_input) - 1
                if 0 <= idx < len(search_categories):
                    category = search_categories[idx]
                    remove_search_category(category)
                    console.print(f"[green]Removed:[/green] {category}")
                else:
                    console.print("[yellow]Invalid category number.[/yellow]")
            else:
                remove_search_category(category_input)
                console.print(f"[green]Removed (if present):[/green] {category_input}")
        elif action == 'c':
            clear_search_categories()
            console.print("[yellow]All categories cleared.[/yellow]")
        elif action == "rs":
            reset_search_categories()
            console.print("[green]Reset to default categories.[/green]")
        elif action == "run-all":
            reset_search_categories()
            observation_start = prompt_start_date()
            console.print(f"[green]Running all default categories ({len(search_categories)}).[/green]")
            execute_collection(api_key, search_categories, observation_start=observation_start)
            break
        elif action == 'run':
            if not search_categories:
                console.print("[red]Cannot run with no categories. Add at least one first.[/red]")
                continue
            observation_start = prompt_start_date()
            execute_collection(api_key, search_categories, observation_start=observation_start)
            break  # Exit the loop after running
        elif action == 'q':
            console.print("[yellow]Exiting lazy_fred.[/yellow]")
            break  # Exit the loop
        else:
            console.print("[yellow]Invalid input. Please choose a valid action.[/yellow]")

def main():
    args = [a.strip().lower() for a in sys.argv[1:]]
    if not args:
        run_fred_data_collection(os.getenv("API_KEY"))
        return

    cmd = args[0]
    if cmd == "doctor":
        run_doctor()
    elif cmd in STARTER_MODES:
        run_starter_mode(os.getenv("API_KEY"), cmd)
    elif cmd == "favorites":
        profile = args[1] if len(args) > 1 else "macro"
        run_favorites(os.getenv("API_KEY"), profile)
    else:
        console.print(
            "[yellow]Unknown command.[/yellow] "
            "Use: doctor | quick | standard | full | favorites <profile>"
        )


def launch_notebook_ui(api_key=None):
    """
    Launch an interactive widget UI for Jupyter/Colab users.

    Example:
        import lazy_fred as lf
        lf.launch_notebook_ui("YOUR_API_KEY")
    """
    try:
        import ipywidgets as widgets
        from IPython.display import display, clear_output
    except Exception:
        raise ImportError(
            "Notebook UI requires ipywidgets. Install with: pip install ipywidgets"
        )

    if not api_key:
        api_key = os.getenv("API_KEY")

    api_key_input = widgets.Password(
        value=api_key or "",
        description="API Key:",
        layout=widgets.Layout(width="600px"),
    )
    categories_select = widgets.SelectMultiple(
        options=DEFAULT_SEARCH_CATEGORIES,
        value=("interest rates", "retail trade", "housing"),
        description="Categories:",
        rows=12,
        layout=widgets.Layout(width="600px"),
    )
    start_date_picker = widgets.DatePicker(
        description="Start date:",
        disabled=False,
    )
    run_button = widgets.Button(
        description="Run collection",
        button_style="success",
        icon="play",
    )
    run_all_button = widgets.Button(
        description="Run all defaults",
        button_style="info",
        icon="database",
    )
    output = widgets.Output()

    def _run_with_selected(_):
        with output:
            clear_output(wait=True)
            chosen_key = api_key_input.value.strip()
            if not chosen_key:
                print("Please enter API key.")
                return
            chosen_categories = list(categories_select.value)
            if not chosen_categories:
                print("Please select at least one category.")
                return
            start_date = None
            if start_date_picker.value:
                start_date = start_date_picker.value.isoformat()
            run_fred_data_collection(
                chosen_key,
                categories=chosen_categories,
                interactive=False,
                observation_start=start_date,
            )

    def _run_all_defaults(_):
        with output:
            clear_output(wait=True)
            chosen_key = api_key_input.value.strip()
            if not chosen_key:
                print("Please enter API key.")
                return
            start_date = None
            if start_date_picker.value:
                start_date = start_date_picker.value.isoformat()
            run_fred_data_collection(
                chosen_key,
                categories=DEFAULT_SEARCH_CATEGORIES,
                interactive=False,
                observation_start=start_date,
            )

    run_button.on_click(_run_with_selected)
    run_all_button.on_click(_run_all_defaults)

    display(
        widgets.VBox(
            [
                widgets.HTML("<h3>lazy_fred Notebook UI</h3>"),
                widgets.HTML(
                    "<p>Select categories and run collection. "
                    "Output CSV files are written to the current working directory.</p>"
                ),
                widgets.HTML(
                    "<p><b>Series insight:</b> during pull, each row shows ID, title, frequency, "
                    "popularity, and a plain-language meaning (for example: inflation pressure, "
                    "labor market slack, borrowing cost trends).</p>"
                ),
                api_key_input,
                start_date_picker,
                categories_select,
                widgets.HBox([run_button, run_all_button]),
                output,
            ]
        )
    )


def run_favorites(api_key=None, profile="macro"):
    """
    Run a beginner-friendly, non-interactive collection for popular themes.

    Profiles: macro, rates, labor, markets
    """
    key = (profile or "").strip().lower()
    if key not in FAVORITE_PROFILES:
        valid = ", ".join(sorted(FAVORITE_PROFILES.keys()))
        raise ValueError(f"Unknown profile '{profile}'. Use one of: {valid}")
    return run_fred_data_collection(
        api_key,
        categories=FAVORITE_PROFILES[key],
        interactive=False,
    )


def run_starter_mode(api_key=None, mode="quick"):
    """Run quick/standard/full starter modes."""
    key = (mode or "").strip().lower()
    if key not in STARTER_MODES:
        valid = ", ".join(sorted(STARTER_MODES.keys()))
        raise ValueError(f"Unknown mode '{mode}'. Use one of: {valid}")
    return run_fred_data_collection(
        api_key,
        categories=STARTER_MODES[key],
        interactive=False,
    )

if __name__ == "__main__":
    main()
