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


# define endpoints in pipeline
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


### Generate final export
databaseHandler.exportForecastData()
