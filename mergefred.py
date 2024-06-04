import pandas as pd


daily_df = pd.read_csv('daily_data.csv')
weekly_df = pd.read_csv('weekly_data.csv')
monthly_df = pd.read_csv('monthly_data.csv')

# Ensure date columns are in datetime format
for df in [daily_df, weekly_df, monthly_df]:
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    print('setting formats')


# Split weekly data into daily rows
weekly_df['date'] = weekly_df.index.to_period('W').start_time

print('weekly formats')

weekly_df['date'] = weekly_df['date'].apply(lambda x: pd.date_range(x, periods=7, freq='D'))

print('weekly formats - split 7 days')

weekly_df = weekly_df.explode('date').reset_index(drop=True).set_index('date')

print('weekly formats - split 7 days - explode')


#this needs WORK, monthly data exploes take FOREVER!

def split_monthly_data(row):
    start_date = row.name.to_period('M').start_time
    end_date = row.name.to_period('M').end_time
    print('start and end dates')

    return pd.date_range(start_date, end_date, freq='D')

print('monthly formats')

monthly_df['date'] = monthly_df.apply(split_monthly_data, axis=1)
monthly_df = monthly_df.explode('date').reset_index(drop=True).set_index('date')

print('merge together')


# Combine, Prioritize, and Forward Fill
merged_df = pd.concat([daily_df, weekly_df, monthly_df]).groupby(level=0).first().ffill()
merged_df = merged_df.reset_index()

print(merged_df)
merged_df.to_csv('merged_daily.csv') 