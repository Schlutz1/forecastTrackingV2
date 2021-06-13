# Handles all interactions required to source and manage data


class perisherHandler(self) :
    # class to handle all perisher related function calls

    def getCurrentForecastData(self):
        # makes call to current perisher forecast data

        return None
    
class databaseHandler(self):
    # class to handle all interactions with DB
    # TODO: understand best method on AWS to do this

    def writeForecastData(self, forecast_data):
        # writes scrapped forecast data to database
        return None

    def getForecastData(self, min_date = None, max_date = None):
        # returns forecast data, optionally with a selected range
    