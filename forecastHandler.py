# Handles all forecast data interactions

# libs
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

import requests as r
import pandas as pd

import os

# paths
cache_path = os.path.join('.', 'cache')


class BaseForecastHandler():
    # abstract base class for other endpoint specific forecast handlers

    def _callEndpoint(self):
        # makes actual get request

        resp = r.get(self.endpoint)
        if resp.status_code != 200:
            print("Error occured on get")
            return None
        
        else:
            return BeautifulSoup(resp.text, features = 'html.parser')


    def getForecast(self, load_from_cache):
        # makes call to some forecast endpoint
        # optionally loads data from cache, use for testing

        if load_from_cache:
            print(self._id, ": Loading forecast from cache")
            with open(os.path.join(cache_path, self.forecast_cache_file), 'r', encoding="utf-8") as f:
                contents = f.read()
                forecast_soup = BeautifulSoup(contents, 'html.parser')

        else :
            print(self._id, ": Loading forecast from site, writing to cache")
            forecast_soup = self._callEndpoint()

            with open(os.path.join(cache_path, self.forecast_cache_file), "w", encoding="utf-8") as f:
                f.write(str(forecast_soup))

        return forecast_soup

class ThredboHandler(BaseForecastHandler):
    # class to handle all thredbo related function calls

    def __init__(self):

        self._id = "ThredboHandler"
        self. forecast_cache_file = 'thredbo_forecast_cache.html'
        self.endpoint = 'https://www.thredbo.com.au/weather/weather-report/'
        self.headers = ["date", "max_temp", "min_temp", "snow_at_1800m", "snow_at_1400m", "snow_at_1000m", "weather"]

    def parseForecast(self, forecast_soup) -> pd.DataFrame():
        
        parsed = []
        for block_counter, forecast_block in enumerate(forecast_soup.find_all("div", class_ = "forecast-block")):

            # extract data from soup
            date = forecast_block.find("div", class_="date").get_text()
            weather = forecast_block.find("p").get_text()

            temps = forecast_block.find("div", class_="data temperature")
            temp_list = [temp.get_text(strip = True) for temp in temps.find_all("div")]

            min_temp = None
            if len(temp_list) > 1:
                min_temp = temp_list[1]

            # update dict with extracted data
            row_dict = {
                'date' : date,
                'max_temp' : temp_list[0],
                'min_temp' : min_temp,
                'weather' : weather
            }

            snows = forecast_block.find("div", class_="data snow-chance snow-heights-3")
            
            if pd.notnull(snows):
                snow_list = [
                    snow.get_text(strip = True) for snow in snows.find_all("div")
                ]
                row_dict.update({
                    'snow_at_1800m' : snow_list[0],
                    'snow_at_1400m' : snow_list[1],
                    'snow_at_1000m' : snow_list[2]
                })

            parsed.append(row_dict)
        
        df_parsed_forecast = pd.DataFrame(parsed)
        return(df_parsed_forecast)


    def cleanForecast(self, df):
        # cleans parsed forecast data, and appends some standard meta-data

        # append meta-data
        df['endpoint'] = self._id
        df['forecast_type'] = '7-day'
        df['extracted_date'] = datetime.now()

        return df

class PerisherHandler(BaseForecastHandler) :
    # class to handle all perisher related function calls

    def __init__(self):

        self._id = "PerisherHandler"
        self.forecast_cache_file = 'perisher_forecast_cache.html'
        self.endpoint = 'https://www.perisher.com.au/reports-cams/reports/weather-forecast'
        self.headers = ["date", "weather", "prob_of_precip", "likely_snow", "snow_level", "wind", "visibility"]


    def parseForecast(self, forecast_soup) -> pd.DataFrame():        
        # parses blob into dataframe

        table = forecast_soup.find("div", class_="table-scroll") # <div class="table-scroll">

        parsed = []
        for row_counter, row in enumerate(table.find_all("tr")) :
            
            if row_counter > 0:
                row_dict = {}
                for col_counter, col in enumerate(row.find_all("td")):
                    row_dict[self.headers[col_counter]] = col.get_text(strip = True)

                parsed.append(row_dict)

        df_parsed_forecast = pd.DataFrame(parsed)
        return(df_parsed_forecast)


    def _getNormalizedLikelySnow(self, likely_snow):
        # normalizes likely amount of snow to numeric value

        if pd.isna(likely_snow):
            return 0

        likely_snow = likely_snow.replace("cm", "")

        if likely_snow == 'Nil':
            return 0

        if "<" in likely_snow:
            return likely_snow.split("<")[1]

        if "-" in likely_snow:
            _min = likely_snow.split("-")[0]
            _max = likely_snow.split("-")[1]
            return (int(_max) + int(_min))/2

    def cleanForecast(self, df):
        # intended to accept a parsed perisher forecast, cleans columns and appends meta data on pipeline run

        # convert likely snow to numeric
        df['likely_snow_numeric'] = df['likely_snow'].apply(lambda x: self._getNormalizedLikelySnow(x))

        # # convert date to datetime object
        datetime.strptime('FriJun 20', '%a%b %d')
        df['forecast_date'] = df['date'].apply(lambda x: datetime.strptime(x + ' 2021', '%a%b %d %Y'))
        
        # # append meta-data
        df['endpoint'] = self._id
        df['forecast_type'] = '14-day'
        df['extracted_date'] = datetime.now()

        return df
    