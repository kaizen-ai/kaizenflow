#!/usr/bin/env python
"""
Download Alpha Vantage data and save it into the DB.

"""
from models.ticker import Ticker
from models.time_series import TimeInterval, DataType
from api.mongo_db import Mongo

# Get latest data from API
ticker = Ticker("AXP", get_name=True)
ticker.get_data(data_type=DataType.WEEKLY)

# Save updated data if needed
# Mongo will verify if the information is duplicate, or new,
# and will update with the latest values
Mongo.save_data(ticker)