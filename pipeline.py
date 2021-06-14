# Pipeline for extraction & transformation of forecast data from Perisher

# imports
from forecastHandler import PerisherHandler, ThredboHandler
from bs4 import BeautifulSoup

import pandas as pd
import sqlite3
import os


# globals
_id = 'pipeline.py'
load_from_cache = True # use this for testing


### Thredbo forecast interactions
thredboHandler = ThredboHandler()

# get current forecast data, optionally load from cache
forecast_soup = thredboHandler.getForecast(load_from_cache)
print(forecast_soup.find("title").get_text())



### Perisher forecast interactions
perisherHandler = PerisherHandler()
forecast_soup = perisherHandler.getForecast(load_from_cache)
print(forecast_soup.find("span", class_="report-heading--text").get_text())

# # parse forecast soup
# df_parsed_forecast = perisherHandler.parseForecast(forecast_soup)

# # clean forecast & append meta-data
# df_cleaned_forecast = perisherHandler.cleanForecast(df_parsed_forecast)
# df_cleaned_forecast.to_sql(
#     name = 'perisher',
#     con = conn, 
#     if_exists = 'append'
# )