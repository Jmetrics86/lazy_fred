import logging
import pandas as pd
import time
from fredapi import Fred
import fred
import datetime
import os
from dotenv import load_dotenv, set_key
import json

logger = logging.getLogger(__name__)
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

sleep = 0.5
searchlimit = 1000 # 1000 is max
#search_categories = ['Interest Rates', 'Exchange Rates'] #this one is for quick testing
search_categories = ['interest rates', 'exchange rates', 'monetary data', 'financial indicator', 'banking industry','gdp' , 'banking', 'business lending', 'foreign exchange intervention', 'current population', 'employment', 'education', 'income', 'job opening', 'labor turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency']


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
        load_dotenv()
        fred.key(os.getenv("API_KEY"))
        search_dict = []
        total_categories = len(categories)
        for index, category in enumerate(categories, start=1):
            search_results = fred.search(category)
            search_dict.append(search_results)
            time.sleep(sleep)
            print(f"Processing {category} ({index}/{total_categories})", flush=True)  # Updated f-string
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


def run_fred_data_collection(api_key):
    print("""starting collection process
    """)
    load_dotenv()
    set_key(".env", "API_KEY", api_key)

    try:
        fred = Fred(api_key=api_key)
        fred.search('category', order_by='popularity', sort_order='desc', limit=searchlimit)
        logger.info("API key is valid!")
        
    except Exception:
        logger.error("Invalid API key provided. Please check and try again.")
        return  # Exit if the API key is invalid
    
    collector = CollectCategories(api_key)
    while True:
        action = input(
            "Do you want to add (a), remove (r), clear (c) categories, or run (run) the data collection? (q to quit): "
        ).lower()

        set_key(".env", "API_KEY", api_key)

        if action == 'a':
            category = input("Enter category to add: ")
            add_search_category(category)
            print(search_categories)
        elif action == 'r':
            category = input("Enter category to remove: ")
            remove_search_category(category)
            print(search_categories)
        elif action == 'c':
            clear_search_categories()
            print(search_categories)
        elif action == 'run':
            print("""Collecting Search Results from the master list!
             """)
            search_results = collector.get_fred_search(search_categories)
            collector.export_master(search_results)
            print("""Pulling data at a daily level
             """)
            daily_exporter = daily_export(Fred(api_key=api_key))
            daily_exporter.dailyfilter()
            daily_exporter.daily_series_collector()
            print("""Pulling data at a monthly level
             """)
            monthly_exporter = monthly_export(Fred(api_key=api_key))
            monthly_exporter.monthlyfilter()
            monthly_exporter.monthly_series_collector()
            print("""Pulling data at a weekly level
             """)
            weekly_exporter = weekly_export(Fred(api_key=api_key))
            weekly_exporter.weeklyfilter()
            weekly_exporter.weekly_series_collector()

            print("complete!")
            break  # Exit the loop after running
        elif action == 'q':
            break  # Exit the loop
        else:
            print("Invalid input. Please choose a valid action.")

def main():
    run_fred_data_collection(os.getenv("API_KEY"))

if __name__ == "__main__":
    main()
