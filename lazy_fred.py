import logging
import pandas as pd
import time
from fredapi import Fred
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

#set some global varibles here
fred = Fred(api_key=os.getenv("API_KEY")) 
sleep = 0.1
searchlimit = 1000 #1000 is max

class AccessFred:
    # Global Variable for FRED instance and the sleep to avoid tripping timeout
        
    def get_and_validate_api_key(self):
        """Retrieves API key from environment, validates, and handles errors."""
        load_dotenv(override=True)  # Always load .env to get potential updates
        api_key = os.getenv("API_KEY")

        if not api_key:
            api_key = input("API_KEY not found in .env. Please enter your API key: ")

        try:
            #fred = Fred(api_key)
            fred.search('category', order_by='popularity', sort_order='desc', limit=searchlimit)
            logger.info("API key is valid!")
            return api_key
        except Exception :
            logger.error("Invalid API key. Please try again.")
            # Clear invalid key from .env
            os.remove(".env")
            return AccessFred.get_and_validate_api_key()  # Recursively retry




# prompt: use fredapi to cycle searching through various topics on the list to create a large dataframe of all of the results, after using a for loop to create the master dataframe, remove duplicates.

class collect_categories:

        


    def get_fred_search_results(fred):
        """
        Retrieves search results from FRED API for a list of categories,
        combines them, removes duplicates, and returns the processed DataFrame.
        """
        fred = Fred(api_key=os.getenv("API_KEY")) 
        #search_categories = ['Interest Rates', 'Exchange Rates', 'Monetary Data', 'Financial Indicator', 'Banking Industry', 'Business Lending', 'Foreign Exchange Intervention', 'Current Population', 'employment', 'education' , 'income' , 'Job Opening', 'Labor Turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency' ]
        #search_categories = ['Interest Rates','Exchange Rates']
        search_categories = ['interest rates', 'exchange rates', 'monetary data', 'financial indicator', 'banking industry', 'business lending', 'foreign exchange intervention', 'current population', 'employment', 'education', 'income', 'job opening', 'labor turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency']

        df_list = []
        for category in search_categories:
            print(category)
            search_results = fred.search(category, order_by='popularity', sort_order='desc', limit=searchlimit)
            df_list.append(pd.DataFrame(search_results))
        
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

    # # Loop through each series and merge it into the DataFrame
    # def daily_series_collector(self):
    #     # Create an empty DataFrame to store the merged data
    #     merged_data = pd.DataFrame()
        
    #     for series_id in daily_export.dailyfilter(self):
    #         data = pd.DataFrame(fred.get_series(series_id))
    #         data['series'] = series_id
    #         #merged_data = pd.concat([merged_data, data], axis=0)
    #         pd.concat([merged_data, data], axis=0)
    #         print(series_id)
    #         time.sleep(sleep)

    #         # Print the merged DataFrame
    #     merged_data = merged_data.reset_index()
    #     merged_data = merged_data.rename(columns={'index': 'date', 0: 'value'})
    #     merged_data.to_csv('daily_data.csv')
    #     print("daily series generated")



    def daily_series_collector(self):
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

#     # Loop through each series and merge it into the DataFrame
#     def monthly_series_collector(self):
        
#         # Create an empty DataFrame to store the merged data
#         monthly_merged_data = pd.DataFrame()
#         data = pd.DataFrame()

#         for series_id in monthly_export.monthlyfilter(self):
#             data = pd.DataFrame(fred.get_series(series_id))
#             data['series'] = series_id
#             pd.concat([monthly_merged_data, data], axis=0)
#             print(series_id)
#             time.sleep(sleep)

#         # Print the merged DataFrame
#         monthly_merged_data = monthly_merged_data.reset_index()
#         monthly_merged_data = monthly_merged_data.rename(columns={'index': 'date', 0: 'value'})
#         monthly_merged_data.to_csv('monthly_data.csv')
#         print("monthly series completed!")


    def monthly_series_collector(self):
        monthly_merged_data = pd.DataFrame()
        max_retries = 5  # Maximum number of retry attempts
        retry_count = 0  # Current retry count
        initial_wait_time = 0.2  # Initial wait time in seconds

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

    # # Loop through each series and merge it into the DataFrame
    # def weekly_series_collector(self):

    #     # Create an empty DataFrame to store the merged data
    #     weekly_merged_data = pd.DataFrame()
    #     data = pd.DataFrame()


    #     for series_id in weekly_export.weeklyfilter(self):
    #         data = pd.DataFrame(fred.get_series(series_id))
    #         data['series'] = series_id
    #         pd.concat([weekly_merged_data, data], axis=0)
    #         print(series_id)
    #         time.sleep(sleep)

    #     # Print the merged DataFrame
    #     weekly_merged_data = weekly_merged_data.reset_index()
    #     weekly_merged_data = weekly_merged_data.rename(columns={'index': 'date', 0: 'value'})
    #     weekly_merged_data.to_csv('weekly_data.csv')
    #     print("weekly series completed!")
        
    def weekly_series_collector(self):
        weekly_merged_data = pd.DataFrame()
        max_retries = 5  # Maximum number of retry attempts
        retry_count = 0  # Current retry count
        initial_wait_time = 0.2  # Initial wait time in seconds

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
    AccessFred1.get_and_validate_api_key()
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


if __name__ == "__main__":
    main()








