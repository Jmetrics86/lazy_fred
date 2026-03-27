import logging
import pandas as pd
import time
from fredapi import Fred
import fred
import datetime
import os
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
#search_categories = ['Interest Rates', 'Exchange Rates'] #this one is for quick testing
DEFAULT_SEARCH_CATEGORIES = ['interest rates', 'exchange rates', 'monetary data', 'financial indicator', 'banking industry','gdp' , 'banking', 'business lending', 'foreign exchange intervention', 'current population', 'employment', 'education', 'income', 'job opening', 'labor turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency']
search_categories = DEFAULT_SEARCH_CATEGORIES.copy()


def render_categories_table():
    table = Table(title="Current Search Categories")
    table.add_column("#", style="cyan", justify="right")
    table.add_column("Category", style="green")
    for idx, category in enumerate(search_categories, start=1):
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


def resolve_categories(user_categories):
    """Resolve user-provided category text against default categories."""
    resolved = []
    default_lookup = {c.lower(): c for c in search_categories}

    for raw in user_categories:
        query = str(raw).strip().lower()
        if not query:
            continue

        if query in default_lookup:
            resolved.append(default_lookup[query])
            continue

        matches = [c for c in search_categories if query in c.lower()]
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


def execute_collection(api_key, categories_to_use):
    collector = CollectCategories(api_key)
    console.print(Panel("Collecting search results from FRED...", border_style="cyan"))
    search_results = collector.get_fred_search(categories_to_use)
    collector.export_master(search_results)

    console.print(Panel("Pulling daily data...", border_style="cyan"))
    daily_exporter = daily_export(Fred(api_key=api_key))
    daily_exporter.dailyfilter()
    daily_exporter.daily_series_collector()

    console.print(Panel("Pulling monthly data...", border_style="cyan"))
    monthly_exporter = monthly_export(Fred(api_key=api_key))
    monthly_exporter.monthlyfilter()
    monthly_exporter.monthly_series_collector()

    console.print(Panel("Pulling weekly data...", border_style="cyan"))
    weekly_exporter = weekly_export(Fred(api_key=api_key))
    weekly_exporter.weeklyfilter()
    weekly_exporter.weekly_series_collector()

    output_table = Table(title="Run complete - output files")
    output_table.add_column("File", style="green")
    output_table.add_row("filtered_series.csv")
    output_table.add_row("daily_data.csv")
    output_table.add_row("monthly_data.csv")
    output_table.add_row("weekly_data.csv")
    console.print(output_table)


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
        for index, category in categories:
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
        for index, category in enumerate(categories, start=1):
            search_results = fred.search(category)
            search_dict.append(search_results)
            time.sleep(sleep)
            console.print(f"[dim]Processing {category} ({index}/{total_categories})[/dim]", soft_wrap=True)
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

    def daily_series_collector(self):
        fred = Fred(api_key=os.getenv("API_KEY")) 

        merged_data = pd.DataFrame()
        max_retries = 5  # Maximum number of retry attempts
        retry_count = 0  # Current retry count
        initial_wait_time = 0.2  # Initial wait time in seconds

        for series_id in daily_export.dailyfilter(self):
            while retry_count < max_retries:
                try:
                    data = pd.DataFrame(fred.get_series(series_id))
                    data['series'] = series_id
                    merged_data = pd.concat([merged_data, data], axis=0)
                    print(series_id)
                    time.sleep(sleep)
                    break  # Break out of the retry loop if successful

                except Exception as e:
                    if e.args[0][0] == 429:
                        wait_time = initial_wait_time * 2 ** retry_count
                        print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        retry_count += 1
                    else:
                        logger.error(f"Error fetching series {series_id}: {e}")
                        break
            else:
                logger.error(f"Max retries reached for series {series_id}. Skipping.")
                retry_count = 0 # Reset retry count for the next series
            

        merged_data = merged_data.reset_index()
        merged_data = merged_data.rename(columns={'index': 'date', 0: 'value'})
        merged_data.to_csv('daily_data.csv')
        print("daily series generated")


class monthly_export:
    def __init__(self, fred):
        self.fred = fred

    # prompt: using the master_df create a list of series ids filtered down to only series with frequency of monthly and popularity above 50.
    def monthlyfilter(self):
        master_df = pd.read_csv('filtered_series.csv')
        monthly_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'M')]
        monthly_list = monthly_list['id'].tolist()
        return monthly_list



    def monthly_series_collector(self):
        monthly_merged_data = pd.DataFrame()
        max_retries = 5  # Maximum number of retry attempts
        retry_count = 0  # Current retry count
        initial_wait_time = 0.2  # Initial wait time in seconds
        fred = Fred(api_key=os.getenv("API_KEY")) 


        for series_id in monthly_export.monthlyfilter(self):
            while retry_count < max_retries:
                try:
                    data = pd.DataFrame(fred.get_series(series_id))
                    data['series'] = series_id
                    monthly_merged_data = pd.concat([monthly_merged_data, data], axis=0)  # Update merged data
                    print(series_id)
                    time.sleep(sleep)
                    retry_count = 0 # Reset retry count for the next series
                    break  # Break out of the retry loop if successful

                except Exception as e:
                    if e.args[0][0] == 429:
                        wait_time = initial_wait_time * 2 ** retry_count
                        print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        retry_count += 1
                    else:
                        logger.error(f"Error fetching series {series_id}: {e}")
                        break
            else:
                logger.error(f"Max retries reached for series {series_id}. Skipping.")


        monthly_merged_data = monthly_merged_data.reset_index()
        monthly_merged_data = monthly_merged_data.rename(columns={'index': 'date', 0: 'value'})
        monthly_merged_data.to_csv('monthly_data.csv')
        print("monthly series completed!")




class weekly_export:

    def __init__(self, fred):
        self.fred = fred

    # prompt: using the master_df create a list of series ids filtered down to only series with frequency of weekly and popularity above 50.
    def weeklyfilter(self):
        master_df = pd.read_csv('filtered_series.csv')
        weekly_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'W')]
        weekly_list = weekly_list['id'].tolist()
        return weekly_list

        
    def weekly_series_collector(self):
        weekly_merged_data = pd.DataFrame()
        max_retries = 5  # Maximum number of retry attempts
        retry_count = 0  # Current retry count
        initial_wait_time = 0.2  # Initial wait time in seconds
        fred = Fred(api_key=os.getenv("API_KEY")) 


        for series_id in weekly_export.weeklyfilter(self):
            while retry_count < max_retries:
                try:
                    data = pd.DataFrame(fred.get_series(series_id))
                    data['series'] = series_id
                    weekly_merged_data = pd.concat([weekly_merged_data, data], axis=0)  # Update merged data
                    print(series_id)
                    time.sleep(sleep)
                    retry_count = 0 # Reset retry count for the next series
                    break  # Break out of the retry loop if successful

                except Exception as e:
                    if e.args[0][0] == 429:
                        wait_time = initial_wait_time * 2 ** retry_count
                        print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        retry_count += 1
                    else:
                        logger.error(f"Error fetching series {series_id}: {e}")
                        break
            else: # The else statement is executed if the while loop completes normally, meaning the max number of retries was reached
                logger.error(f"Max retries reached for series {series_id}. Skipping.")

        weekly_merged_data = weekly_merged_data.reset_index()
        weekly_merged_data = weekly_merged_data.rename(columns={'index': 'date', 0: 'value'})
        weekly_merged_data.to_csv('weekly_data.csv')
        print("weekly series completed!")


def run_fred_data_collection(api_key, categories=None, interactive=True):
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
        console.print("[green]Running non-interactive collection with selected categories.[/green]")
        console.print(render_categories_table())
        execute_collection(api_key, categories_to_use)
        return

    if not interactive:
        console.print("[red]interactive=False requires categories=[...][/red]")
        return

    collector = CollectCategories(api_key)
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
            console.print(f"[green]Running all default categories ({len(search_categories)}).[/green]")
            execute_collection(api_key, search_categories)
            break
        elif action == 'run':
            if not search_categories:
                console.print("[red]Cannot run with no categories. Add at least one first.[/red]")
                continue
            execute_collection(api_key, search_categories)
            break  # Exit the loop after running
        elif action == 'q':
            console.print("[yellow]Exiting lazy_fred.[/yellow]")
            break  # Exit the loop
        else:
            console.print("[yellow]Invalid input. Please choose a valid action.[/yellow]")

def main():
    run_fred_data_collection(os.getenv("API_KEY"))


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
            run_fred_data_collection(
                chosen_key,
                categories=chosen_categories,
                interactive=False,
            )

    def _run_all_defaults(_):
        with output:
            clear_output(wait=True)
            chosen_key = api_key_input.value.strip()
            if not chosen_key:
                print("Please enter API key.")
                return
            run_fred_data_collection(
                chosen_key,
                categories=DEFAULT_SEARCH_CATEGORIES,
                interactive=False,
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
                api_key_input,
                categories_select,
                widgets.HBox([run_button, run_all_button]),
                output,
            ]
        )
    )

if __name__ == "__main__":
    main()
