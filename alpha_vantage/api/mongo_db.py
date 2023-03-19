import os
from typing import List

from models.ticker import Ticker
from models.time_series import TimeSeriesData
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class Mongo:
    MONGO_USER = os.environ.get("MONGO_USER")
    MONGO_PWD = os.environ.get("MONGO_PWD")

    mongo_str = f"mongodb+srv://{MONGO_USER}:{MONGO_PWD}@cluster0.jtf4vwu.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(mongo_str, server_api=ServerApi('1'))

    # If line below errors try this: https://stackoverflow.com/questions/59411362/ssl-certificate-verify-failed-certificate-verify-failed-unable-to-get-local-i
    db = client['DATA605']
    collection = db['market']

    @classmethod
    def download(cls) -> List[Ticker]:
        """Downloads the entire database stored in Mongo and returns it as a list of tickers"""
        data = cls.collection.find()
        if data:
            tickers = []
            for ticker_data in data:
                ticker_data['time_series_data'] = [TimeSeriesData(
                    **point) for point in ticker_data['time_series_data']]
                tickers.append(Ticker(**ticker_data))
            return tickers

    @classmethod
    def get_ticker(cls, ticker: str) -> Ticker:
        """
        Returns individual data for a single specified ticker

        Parameters:
        ticker: str - Ticker for company lookup.
        """
        data = cls.collection.find_one({"_id": ticker})
        if data:
            data['time_series_data'] = [TimeSeriesData(
                **point) for point in data['time_series_data']]
            return Ticker(**data)

    @ classmethod
    def save_data(cls, data: Ticker):
        """
        Saves a ticker class to mongoDB

        Parameters:
        data: Ticker - Class that needs to be saved.
        """
        json = data.to_json()
        if json:
            time_series = json['time_series_data']
            del json['time_series_data']
            cls.collection.find_one_and_update(
                {"_id": data.ticker}, {"$set": json}, upsert=True)
            cls.collection.find_one_and_update(
                {"_id": data.ticker}, {'$push': {'time_series_data': {'$each': time_series}}}, upsert=True)
