from dataclasses import dataclass
from datetime import date
from typing import List


@dataclass
class TimeSeriesData:
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: int


class Ticker:
    def __init__(self,
                 name: str,
                 ticker: str,
                 last_updated: date,
                 last_open: float,
                 last_close: float,
                 time_series_data: List[TimeSeriesData]
                 ) -> None:
        self.name = name
        self.ticker = ticker
        self.last_updated = last_updated
        self.last_open = last_open
        self.last_close = last_close
        self.time_series_data = time_series_data
