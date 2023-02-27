from models.ticker import Ticker
from models.time_series import TimeInterval, DataType

# First initilize the class stock you want.
# Setting get_name to True burns an extra API call.
Apple = Ticker("AAPL", get_name=True)

# Then request the data you want using the DataType ENUM. Time Interval is only used for intraday.
Apple.get_data(data_type=DataType.INTRADAY, time_interval=TimeInterval.THIRTY)

print(Apple)