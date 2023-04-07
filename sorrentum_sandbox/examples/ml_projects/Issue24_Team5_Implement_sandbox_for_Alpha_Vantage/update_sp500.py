"""
Download Alpha Vantage data and save it into the DB.
"""

import time
from list_of_tickers import SP500
from models.ticker import Ticker
from models.time_series import TimeInterval, DataType
from api.mongo_db import Mongo

counter = 0

for symbol in SP500:
    ticker = Ticker(symbol, get_name=False)
    ticker.get_data(data_type=DataType.INTRADAY,
                    time_interval=TimeInterval.ONE)
    Mongo.save_data(ticker)
    counter += 1

    if counter >= 5:
        counter = 0
        time.sleep(61)
