# Pipeline for extraction & transformation of forecast data from Perisher

# imports
from forecastHandler import PerisherHandler, ThredboHandler
from bs4 import BeautifulSoup

import pandas as pd
import sqlite3
import os

# paths
db = 'catch.db'
cache_path = os.path.join('.', 'cache')
thredbo_forecast_cache_file = 'thredbo_forecast_cache.html'
perisher_forecast_cache_file = 'perisher_forecast_cache.html'


# globals
_id = 'pipeline.py'
load_from_cache = True # use this for testing
conn = sqlite3.connect(os.path.join(cache_path, db)) # use this to cache local results


### Thredbo forecast interactions
thredboHandler = ThredboHandler()

# get current forecast data, optionally load from cache
if load_from_cache:
    print(_id, ": Loading forecast from cache")
    with open(os.path.join(cache_path, thredbo_forecast_cache_file), 'r') as f:
        contents = f.read()
        forecast_soup = BeautifulSoup(contents, 'html.parser')

else :
    print(_id, ": Loading forecast from site, writing to cache")
    forecast_soup = thredboHandler.getForecast()

    with open(os.path.join(cache_path, thredbo_forecast_cache_file), "w", encoding="utf-8") as f:
        f.write(str(forecast_soup))

### Perisher forecast interactions
# perisherHandler = PerisherHandler()

# # get current forecast data, optionally load from cache
# if load_from_cache:
#     print(_id, ": Loading forecast from cache")
#     with open(os.path.join(cache_path, perisher_forecast_cache_file), 'r') as f:
#         contents = f.read()
#         forecast_soup = BeautifulSoup(contents, 'html.parser')

# else :
#     print(_id, ": Loading forecast from site, writing to cache")
#     forecast_soup = perisherHandler.getForecast()

#     with open(os.path.join(cache_path, perisher_forecast_cache_file), "w") as f:
#         f.write(str(forecast_soup))

# # parse forecast soup
# df_parsed_forecast = perisherHandler.parseForecast(forecast_soup)

# # clean forecast & append meta-data
# df_cleaned_forecast = perisherHandler.cleanForecast(df_parsed_forecast)
# df_cleaned_forecast.to_sql(
#     name = 'perisher',
#     con = conn, 
#     if_exists = 'append'
# )