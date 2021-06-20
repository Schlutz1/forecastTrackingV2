# suite of function to get actuals metrics

from dataHandler import DatabaseHandler
from datetime import datetime

import pandas as pd
import sys

# pull df
databaseHandler = DatabaseHandler()
df = databaseHandler.getTable("perisher")


# clean up date formats
df['forecast_date'] = df['forecast_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
df['extracted_date'] = df['extracted_date'].apply(lambda x: datetime.strptime(x.split(" ")[0], '%Y-%m-%d'))


# get number of days forecast is from actual e.g. tomorrow's forecast is 1 day away, hence = 1
def get_days_from_actual(row):
    delta = row['forecast_date'] - row['extracted_date']
    return delta.days


df['days_from_actual'] = df.apply(
    get_days_from_actual
    , axis = 1
)

# get difference from actual
# actual is forecast where days_from_actual == 0 e.g. forecast date is extraction date
df_actuals = df[
    df['forecast_date'] == df['extracted_date']    
][['forecast_date', 'likely_snow_numeric']]
df_actuals.rename(columns = {
    'likely_snow_numeric': 'actual_snow_numeric'
}, inplace = True)

df_w_actuals = df.merge(
    df_actuals
    , on = 'forecast_date'
    , how = 'left'
)

# larger the number, more a service has over forecasted
# negative numbers indicate underforecasting
def get_difference_from_actual(row):
    if pd.notnull(row['actual_snow_numeric']):
        return(float(row['likely_snow_numeric']) - float(row['actual_snow_numeric']))

    else:
        return None

df_w_actuals['difference_from_actual'] = df_w_actuals.apply(
    get_difference_from_actual
    , axis = 1
)

print(df_w_actuals[['forecast_date', 'likely_snow_numeric', 'actual_snow_numeric', 'difference_from_actual']])

