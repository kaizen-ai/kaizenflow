import os
import requests
from enum import Enum


class TimeInterval(Enum):
    one = "1min"
    five = "5mins"
    fifthteen = "15min"
    thirty = "30min"
    hour = "60min"


class AlphaVantage:
    API_KEY = os.environ.get("ALPHA_VANTAGE")

    @classmethod
    def get_intraday_for(cls, ticker: str, interval: TimeInterval = TimeInterval.five):
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&outputsize=full&apikey={cls.API_KEY}"
        request = requests.get(url)
        return request.json()

    @classmethod
    def get_daily_for(cls, ticker: str):
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize=full&apikey={cls.API_KEY}"
        request = requests.get(url)
        return request.json()

    @classmethod
    def get_weekly_for(cls, ticker: str):
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={ticker}&outputsize=full&apikey={cls.API_KEY}"
        request = requests.get(url)
        return request.json()

    @classmethod
    def get_monthly_for(cls, ticker: str):
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={ticker}&outputsize=full&apikey={cls.API_KEY}"
        request = requests.get(url)
        return request.json()

    @classmethod
    def get_sentiment_for(cls, ticker: str):
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={cls.API_KEY}"
        request = requests.get(url)
        return request.json()
