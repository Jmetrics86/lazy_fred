import logging
import pandas as pd
import time
from fredapi import Fred
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def get_and_validate_api_key():
    """Retrieves API key from environment, validates, and handles errors."""
    load_dotenv(override=True)  # Always load .env to get potential updates
    api_key = os.getenv("API_KEY")

    if not api_key:
        api_key = input("API_KEY not found in .env. Please enter your API key: ")

    try:
        fred = Fred(api_key)
        fred.search('category', order_by='popularity', sort_order='desc', limit=10)
        logger.info("API key is valid!")
        return api_key
    except Exception :
        logger.error("Invalid API key. Please try again.")
        # Clear invalid key from .env
        os.remove(".env")
        return get_and_validate_api_key()  # Recursively retry

# Main script logic
if __name__ == "__main__":
    valid_api_key = get_and_validate_api_key()

    # Now, confidently use valid_api_key for FRED API calls
    fred = Fred(api_key=valid_api_key)
    # ... your FRED API calls here ... 

    # Save valid API key to .env if it wasn't there originally
    if not os.getenv("API_KEY"):  # Double-check in case .env was deleted
        with open(".env", "w") as f:
            f.write(f"API_KEY={valid_api_key}\n")
        logger.info("Valid API key saved to .env for future use.")


# prompt: use fredapi to cycle searching through various topics on the list to create a large dataframe of all of the results, after using a for loop to create the master dataframe, remove duplicates.



search_categories = ['Interest Rates', 'Exchange Rates', 'Monetary Data', 'Financial Indicator', 'Banking Industry', 'Business Lending', 'Foreign Exchange Intervention', 'Current Population', 'employment', 'education' , 'income' , 'Job Opening', 'Labor Turnover', 'productivity index', 'cost index', 'minimum wage', 'tax rate', 'retail trade', 'services', 'technology', 'housing', 'expenditures', 'business survey', 'wholesale trade', 'transportation', 'automotive', 'house price indexes', 'cryptocurrency' ]

df_list = []
for category in search_categories:
  search_results = fred.search(category, order_by='popularity', sort_order='desc', limit=1000)
  df = pd.DataFrame(search_results)
  df_list.append(df)
  print(category)
master_df = pd.concat(df_list)
master_df = master_df.drop_duplicates()
master_df.loc[:, 'popularity'] = master_df['popularity'].astype(int)
master_df.to_csv('lazy_fred_Search.csv')


# prompt: using the master_df create a list of series ids filtered down to only series with frequency of daily and popularity above 50.

daily_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'D')]
daily_list = daily_list['id'].tolist()
print(daily_list)


# prompt: using the master_df create a list of series ids filtered down to only series with frequency of monthly and popularity above 50.

monthly_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'M')]
monthly_list = monthly_list['id'].tolist()
print(monthly_list)

# prompt: using the master_df create a list of series ids filtered down to only series with frequency of weekly and popularity above 50.

weekly_list = master_df[(master_df['popularity'] >= 50) & (master_df['frequency_short'] == 'W')]
weekly_list = weekly_list['id'].tolist()
print(weekly_list)



# Create an empty DataFrame to store the merged data
merged_data = pd.DataFrame()
data = pd.DataFrame()

# Loop through each series and merge it into the DataFrame
for series_id in daily_list:
    data = pd.DataFrame(fred.get_series(series_id))
    data['series'] = series_id
    merged_data = pd.concat([merged_data, data], axis=0)
    print(series_id)


# Print the merged DataFrame
merged_data = merged_data.reset_index()
merged_data = merged_data.rename(columns={'index': 'date', 0: 'value'})


print(merged_data)
print(merged_data.info())
merged_data.to_csv('daily_data.csv')


# Create an empty DataFrame to store the merged data
monthly_merged_data = pd.DataFrame()
data = pd.DataFrame()

# Loop through each series and merge it into the DataFrame
for series_id in monthly_list:
    data = pd.DataFrame(fred.get_series(series_id))
    data['series'] = series_id
    monthly_merged_data = pd.concat([monthly_merged_data, data], axis=0)
    print(series_id)
    time.sleep(0.5)


# Print the merged DataFrame
monthly_merged_data = monthly_merged_data.reset_index()
monthly_merged_data = monthly_merged_data.rename(columns={'index': 'date', 0: 'value'})


print(monthly_merged_data)
print(monthly_merged_data.info())
monthly_merged_data.to_csv('monthly_data.csv')


# Create an empty DataFrame to store the merged data
monthly_merged_data = pd.DataFrame()
data = pd.DataFrame()

# Loop through each series and merge it into the DataFrame
for series_id in weekly_list:
    data = pd.DataFrame(fred.get_series(series_id))
    data['series'] = series_id
    monthly_merged_data = pd.concat([monthly_merged_data, data], axis=0)
    print(series_id)
    time.sleep(0.5)


# Print the merged DataFrame
monthly_merged_data = monthly_merged_data.reset_index()
monthly_merged_data = monthly_merged_data.rename(columns={'index': 'date', 0: 'value'})


print(monthly_merged_data)
print(monthly_merged_data.info())
monthly_merged_data.to_csv('weekly_data.csv')
