# Handles all db interactions

# libraries
import pandas as pd
import sqlite3
import os

# paths
export_path = os.path.join(".", "export")
cache_path = os.path.join(".", "cache")

export_file = "data_export.csv"
db = 'catch.db'


class DatabaseHandler():
    # class to handle all interactions with DB
    # TODO: understand best method on AWS to do this

    def __init__(self):
        self._id = "databaseHandler"
        self.conn = sqlite3.connect(os.path.join(cache_path, db))

    def appendToTable(self, df, table):
        # appends scrapped forecast data to database
        
        df.to_sql(
            name = table,
            con = self.conn,
            if_exists = 'append'
        )

    def overwriteTable(self, df, table, overwrite_bool = False):
        # overwrites table in db
        # :param df: dataframe to write
        # :param table: table to write to
        # :param overwrite_bool: boolean, set to True to confirm overwrite
        
        if (overwrite_bool):
            print(f"{self._id}: Warning, overwrote {table} table")

            df.to_sql(
                name = table,
                con = self.conn,
                if_exists = 'replace'
            )
        
        else:
            print(f"{self._id}: Warning, did not overwrite {table} table")

    def getTable(self, table, min_date = None, max_date = None):
        # returns forecast data, optionally with a selected range
        # :param table: table to select from
        # :param (optional) min_date: minimum date to select from
        # :param (optional) max_date: maximum date to select from
            # if no timeframe specified, defaults to all data

        df_table = pd.read_sql(
            f'''
            SELECT *
            FROM {table}
            ''',
            self.conn
        )

        return df_table

    def exportForecastData(self):
        # updates files in export, for backing up on github / where-ever
        print(f"{self._id}: Exporting forecast from cache")

        tables = pd.read_sql(
            '''
            SELECT name FROM sqlite_master WHERE type="table";
            ''',
            self.conn
        )['name']

        df_export = pd.DataFrame()
        for table in tables:
            df_table = pd.read_sql(
                f'''
                SELECT *
                FROM {table}
                ''',
                self.conn
            )

            df_export = df_export.append(df_table)
        
        df_export.to_csv(
            os.path.join(export_path, export_file),
            index = False
        )