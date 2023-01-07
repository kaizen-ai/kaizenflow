"""
Implementation of save part of the ETL and QA pipeline.

Import as:

import sorrentum_sandbox.examples.reddit.db as siseredb
"""
import abc

import pymongo

import sorrentum_sandbox.download as sinsadow
import sorrentum_sandbox.save as sinsasav


class BaseMongoSaver(sinsasav.DataSaver):
    """
    Abstract class for saving data to MongoDB.
    """

    def __init__(self, mongo_client: pymongo.MongoClient, db_name: str):
        self.mongo_client = mongo_client
        self.db_name = db_name

    @abc.abstractmethod
    def save(self, data: sinsadow.RawData) -> None:
        """
        Save data to a MongoDB.

        :param data: data to persist
        """


class RedditMongoSaver(BaseMongoSaver):
    """
    Store data from the Reddit to MongoDB.
    """

    def __init__(self, *args, collection_name: str, **kwargs):
        self.collection_name = collection_name
        super().__init__(*args, db_name="reddit", **kwargs)

    def save(self, data: sinsadow.RawData) -> None:
        db = self.mongo_client
        db[self.db_name][self.collection_name].insert_many(data.get_data())
