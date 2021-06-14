# Pipeline for extraction & transformation of forecast data from Perisher

# imports
from forecastHandler import PerisherHandler, ThredboHandler
from dataHandler import DatabaseHandler
from bs4 import BeautifulSoup

import pandas as pd
import sqlite3
import sys
import os


# globals
_id = 'pipeline.py'
load_from_cache = False # use this for testing
databaseHandler = DatabaseHandler()
databaseHandler.exportForecastData()
sys.exit()

### Thredbo forecast interactions
thredboHandler = ThredboHandler()

# get current forecast data, optionally load from cache
forecast_soup = thredboHandler.getForecast(load_from_cache)
df_parsed_forecast = thredboHandler.parseForecast(forecast_soup)
df_cleaned_forecast = thredboHandler.cleanForecast(df_parsed_forecast)

# store locally
databaseHandler.writeForecastData(
    df_cleaned_forecast,
    'thredbo'
)


### Perisher forecast interactions
perisherHandler = PerisherHandler()
forecast_soup = perisherHandler.getForecast(load_from_cache)

# parse forecast soup
df_parsed_forecast = perisherHandler.parseForecast(forecast_soup)

# clean forecast & append meta-data
df_cleaned_forecast = perisherHandler.cleanForecast(df_parsed_forecast)

# store locally
databaseHandler.writeForecastData(
    df_cleaned_forecast,
    'perisher'
)

