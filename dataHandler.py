# Handles all db interactions

import sqlite3
import os

class cacheHandler():
    # use to this handle interactions with local html caches
    # primarily used for testing, encapsulte here to make pretty
    # TODO: use this class?

    def __init__(self):
        return None

class databaseHandler(self):
    # class to handle all interactions with DB
    # TODO: understand best method on AWS to do this

    # self.conn = sqlite3.connect(os.path.join)

    def writeForecastData(self, forecast_data):
        # writes scrapped forecast data to database
        return None

    def getForecastData(self, min_date = None, max_date = None):
        # returns forecast data, optionally with a selected range
    