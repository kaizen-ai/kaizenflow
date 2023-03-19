from typing import List
from api.alpha_vantage import AlphaVantage
from models.time_series import TimeSeriesData, TimeInterval, DataType


class Ticker:
    def __init__(
        self,
        ticker: str,
        get_name: bool = False,
        time_series_data: List[TimeSeriesData] = None,
        **kwargs
    ) -> None:
        self.ticker = ticker

        self.name = ticker
        if get_name:
            self.name = AlphaVantage.get_name_for(ticker)

        self.time_series_data = time_series_data

        self.last_updated = None
        self.last_open = None
        self.last_close = None

        if self.time_series_data:
            last = self.time_series_data[0]

            self.last_updated = last.date
            self.last_open = last.open
            self.last_close = last.close

    def __repr__(self) -> str:
        return f"""
        Name: {self.name} | {self.ticker}
        Last Updated: {self.last_updated}
        Last Open: {self.last_open}
        Last Close: {self.last_close}
        Datapoints: {len(self.time_series_data) if self.time_series_data else "No Data"}
        """

    def get_data(self, data_type: DataType, time_interval: TimeInterval = None):
        """
        Requests and loads the specified data type using Alpha Vantage.

        Parameters:
        data_type: DataType - Data type requested (eg. DataType.DAILY)
        """
        fn = AlphaVantage.get_method(data_type.value)
        self.time_series_data = fn(self.ticker, interval=time_interval.value)
        self.update()

    def update(self):
        """Updates object attributes based on latest information if available."""
        if self.time_series_data:
            last = self.time_series_data[0]

            self.last_updated = last.date
            self.last_open = last.open
            self.last_close = last.close

    def to_json(self) -> dict:
        """Converts object to JSON as long as it has time_series_data"""
        if self.time_series_data:
            json = self.__dict__
            json['time_series_data'] = [
                timeseries_data.__dict__ for timeseries_data in self.time_series_data]
            return json
