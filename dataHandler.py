# Handles all db interactions

# libraries
import sqlite3
import os

# paths
cache_path = os.path.join('.', 'cache')
db = 'catch.db'


class DatabaseHandler():
    # class to handle all interactions with DB
    # TODO: understand best method on AWS to do this

    def __init__(self):
        self.conn = sqlite3.connect(os.path.join(cache_path, db))

    def writeForecastData(self, df, table):
        # writes scrapped forecast data to database
        
        df.to_sql(
            name = table,
            con = self.conn,
            if_exists = 'append'
        )

    def getForecastData(self, min_date = None, max_date = None):
        # returns forecast data, optionally with a selected range
        
        return None