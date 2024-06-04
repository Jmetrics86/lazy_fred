import pandas as pd

daily_df = pd.read_csv('daily_data.csv')
weekly_df = pd.read_csv('weekly_data.csv')
monthly_df = pd.read_csv('monthly_data.csv')

# Ensure date columns are in datetime format
for df in [daily_df, weekly_df, monthly_df]:
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

# Remove duplicates in weekly and monthly DataFrames BEFORE resampling
weekly_df = weekly_df[~weekly_df.index.duplicated(keep='first')]
monthly_df = monthly_df[~monthly_df.index.duplicated(keep='first')]

# Resample weekly and monthly to daily
weekly_df = weekly_df.resample('D').ffill()
monthly_df = monthly_df.resample('D').ffill()

# Combine and Prioritize
merged_df = pd.concat([daily_df, weekly_df, monthly_df]).groupby(level=0).first().ffill()
merged_df = merged_df.reset_index()
print(merged_df)

df.to_csv('merged.csv')
