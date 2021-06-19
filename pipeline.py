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
load_from_cache = True # use this for testing
databaseHandler = DatabaseHandler()


pipeline_config = {
    "thredbo": {
        "handler": ThredboHandler(),
        "table": "thredbo"
    },
    "perisher": {
        "handler": PerisherHandler(),
        "table": "perisher"
    }
}


# for endpoint, config in pipeline_config.items():

#     # define handler
#     handler = config['handler']

### Thredbo forecast interactions
thredboHandler = ThredboHandler()

# get current forecast data, optionally load from cache
forecast_soup = thredboHandler.getForecast(load_from_cache)
df_parsed_forecast = thredboHandler.parseForecast(forecast_soup)
df_cleaned_forecast = thredboHandler.cleanForecast(df_parsed_forecast)

# # store locally
databaseHandler.appendForecastData(
    df_cleaned_forecast,
    'thredbo'
)


### Perisher forecast interactions
perisherHandler = PerisherHandler()

forecast_soup = perisherHandler.getForecast(load_from_cache)
df_parsed_forecast = perisherHandler.parseForecast(forecast_soup)
df_cleaned_forecast = perisherHandler.cleanForecast(df_parsed_forecast)

# store locally
databaseHandler.appendForecastData(
    df_cleaned_forecast,
    'perisher'
)


### Generate final export
databaseHandler.exportForecastData()
