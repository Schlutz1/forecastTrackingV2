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


# define endpoints hit in pipeline
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

# iterate over endpoints
for endpoint, config in pipeline_config.items():

    # define handler
    handler = config['handler']

    # scrape soup
    forecast_soup = handler.getForecast(load_from_cache)
    
    # parse soup into dataframe
    df_parsed_forecast = handler.parseForecast(forecast_soup)
    
    # clean dataframe
    df_cleaned_forecast = handler.cleanForecast(df_parsed_forecast)

    # store locally
    databaseHandler.appendForecastData(
        df_cleaned_forecast,
        config['table']
    )



# ### Thredbo forecast interactions
# thredboHandler = ThredboHandler()

# # get current forecast data, optionally load from cache
# forecast_soup = thredboHandler.getForecast(load_from_cache)
# df_parsed_forecast = thredboHandler.parseForecast(forecast_soup)
# df_cleaned_forecast = thredboHandler.cleanForecast(df_parsed_forecast)

# # # store locally
# databaseHandler.appendForecastData(
#     df_cleaned_forecast,
#     'thredbo'
# )


# ### Perisher forecast interactions
# perisherHandler = PerisherHandler()

# forecast_soup = perisherHandler.getForecast(load_from_cache)
# df_parsed_forecast = perisherHandler.parseForecast(forecast_soup)
# df_cleaned_forecast = perisherHandler.cleanForecast(df_parsed_forecast)

# # store locally
# databaseHandler.appendForecastData(
#     df_cleaned_forecast,
#     'perisher'
# )


### Generate final export
databaseHandler.exportForecastData()
