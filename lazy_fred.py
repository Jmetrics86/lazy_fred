import logging
import pandas as pd
import time
from fredapi import Fred
import os
from dotenv import load_dotenv, set_key


logger = logging.getLogger(__name__)
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

#set some global varibles here
sleep = 0.2
searchlimit = 10 #1000 is max
search_categories = ['Interest Rates','Exchange Rates']
#search_categories = ['interest rates', 'exchange rates', 'monetary data', 'financial indicator', 'banking industry', 'business lending', 'foreign exchange intervention', 'current population', 'employment', 'education', 'income', 'job opening', 'labor turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency']

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


        
           
    # Global Variable for FRED instance and the sleep to avoid tripping timeout
    def get_and_validate_api_key(self):
        """Retrieves API key, stores in .env if valid, and handles errors."""
        load_dotenv()  # Load environment variables from .env file
        api_key = os.getenv("API_KEY")
        fred = Fred(api_key=os.getenv("API_KEY")) 

        while not api_key:  # Keep asking until a valid key is provided
            api_key = input("API_KEY not found in .env. Please enter your API key: ")

            try:
                #fred = Fred(api_key)
                fred.search('category', order_by='popularity', sort_order='desc', limit=searchlimit)
                logger.info("API key is valid!")

                # Store valid API key in .env
                set_key(".env", "API_KEY", api_key) 
                
                return api_key
            except Exception:
                logger.error("Invalid API key. Please try again.")






# prompt: use fredapi to cycle searching through various topics on the list to create a large dataframe of all of the results, after using a for loop to create the master dataframe, remove duplicates.

class collect_categories:        

    def get_fred_search_results(self):

        fred = Fred(api_key=os.getenv("API_KEY")) 

        max_retries = 5  # Maximum number of retry attempts

        df_list = []
        for category in search_categories:
            retries = 0
            while retries < max_retries:
                try:
                    search_results = fred.search(category, order_by='popularity', sort_order='desc', limit=searchlimit)
                    df_list.append(pd.DataFrame(search_results))
                    time.sleep(sleep)  # Rate limiting
                    break  # Exit the retry loop if successful
                except Exception as e:  # Catch any exception
                    logger.error(f"Error retrieving data for {category}: {e}. Retrying... ({retries}/{max_retries})")  # Log the error for debugging
                    retries += 1
                    time.sleep(sleep**retries)  # Increasing wait time on each retry
            else:
                print(f"Failed to retrieve data for {category} after {max_retries} attempts.")
                
        
        master_df = pd.concat(df_list)
        master_df = master_df.drop_duplicates()
        master_df.loc[:, 'popularity'] = master_df['popularity'].astype(int)
        return master_df

    def export_master(self):
        master_df = self.get_fred_search_results()
        master_df.to_csv("lazy_fred_Search.csv")

#prompt: using the master_df create a list of series ids filtered down to only series with frequency of daily and popularity above 50.
class daily_export:
    def __init__(self, fred):
        self.fred = fred

    def dailyfilter(self):
        master_df = pd.read_csv('lazy_fred_Search.csv')
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
        master_df = pd.read_csv('lazy_fred_Search.csv')
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
        master_df = pd.read_csv('lazy_fred_Search.csv')
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





#Collecting Execution Code into main method
def main():


    print("checking access!")
    AccessFred1 = AccessFred()
    AccessFred1.set_api_key_in_environment()
    AccessFred1.get_and_validate_api_key()
    fred = Fred(api_key=os.getenv("API_KEY")) 
    print("collecting categories!")
    #Aggregating categorical data and exporting
    GrabCategories1 = collect_categories()
    GrabCategories1.get_fred_search_results()
    GrabCategories1.export_master()

    print("collecting daily data!")
    #Exporting Daily Data
    daily_export1 = daily_export(fred)
    daily_export1.dailyfilter()
    daily_export1.daily_series_collector()

    print("collecting monthly data!")
    #exporting Monthly Data
    monthly_export1 = monthly_export(fred)
    monthly_export1.monthlyfilter()
    monthly_export1.monthly_series_collector()
    print("collecting weekly data!")
    #exporting Weekly Data
    weekly_export1 = weekly_export(fred)
    weekly_export1.weeklyfilter()
    weekly_export1.weekly_series_collector()
    print("complete!")

def run_fred_data_collection(api_key):
    """
    This function orchestrates the entire FRED data collection process,
    handling API key validation, search result collection, and data export.
    It also allows interactive management of search categories.
    """

    # Validate and Store API Key
    try:
        fred = Fred(api_key=api_key)
        fred.search('category', order_by='popularity', sort_order='desc', limit=searchlimit)
        logger.info("API key is valid!")
        set_key(".env", "API_KEY", api_key)
    except Exception:
        logger.error("Invalid API key provided. Please check and try again.")
        return  # Exit if the API key is invalid

    while True:
        action = input(
            "Do you want to add (a), remove (r), clear (c) categories, or run (run) the data collection? (q to quit): "
        ).lower()

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
            # Collect and Export Data
            try:
                print("Creating instance!")
                grab_categories = collect_categories()
                print(f"""
                
                collecting search results! based on the current list...

                {search_categories}

                """)
                grab_categories.get_fred_search_results()
                print("export series master list!")
                grab_categories.export_master()

                print("collecting daily data!")
                daily_exporter = daily_export(fred)
                daily_exporter.dailyfilter()
                daily_exporter.daily_series_collector()

                print("collecting monthly data!")
                monthly_exporter = monthly_export(fred)
                monthly_exporter.monthlyfilter()
                monthly_exporter.monthly_series_collector()

                print("collecting weekly data!")
                weekly_exporter = weekly_export(fred)
                weekly_exporter.weeklyfilter()
                weekly_exporter.weekly_series_collector()

                print("complete!")

            except Exception as e:
                logger.error(f"An error occurred during data collection: {e}")
            break  # Exit the loop after running
        elif action == 'q':
            break  # Exit the loop
        else:
            print("Invalid input. Please choose a valid action.")


if __name__ == "__main__":
    main()








