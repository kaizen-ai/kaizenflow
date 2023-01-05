#!/usr/bin/env python
"""
Example implementation of abstract classes for ETL and QA pipeline.

Download Reddit data

Use as:
# Download Reddit data:
> example_extract.py \
    --start_timestamp '2022-10-20 10:00:00+00:00' \
    --end_timestamp '2022-10-21 15:30:00+00:00'
"""
import abc
import argparse
import dataclasses
import datetime
import logging
import os
from typing import Any, List, Tuple, Optional

import pandas as pd
import praw
import pymongo

import helpers.hdbg as hdbg
import surrentum_infra_sandbox.download as sinsadow
import surrentum_infra_sandbox.save as sinsasav

_LOG = logging.getLogger(__name__)
REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_SECRET = os.environ["REDDIT_SECRET"]
MONGO_HOST = os.environ["MONGO_HOST"]


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


@dataclasses.dataclass
class RedditPostFeatures:
    subreddit: str
    created: datetime.datetime
    symbols: List[str]
    post_length: int
    title: str
    content: str
    number_of_upvotes: int
    number_of_comments: int
    top_comment: str

    def dict(self) -> dict:
        return {k: str(v) for k, v in dataclasses.asdict(self).items()}


class RedditDownloader(sinsadow.DataDownloader):
    """
    Download reddit data using praw lib.
    """

    def __init__(self):
        self.reddit_client = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_SECRET,
            user_agent=REDDIT_SECRET,
        )

    @staticmethod
    def get_the_top_most_comment_body(post: praw.models.Submission) -> Any:
        """
        Get the top most comment body from a praw post.

        :param post: post for searching
        :return: body of the top most comment
        """
        try:
            body = post.comments[0].body
        except IndexError:
            _LOG.warn("Error fetching top comment for the post: %s", post.title)
            body = ""
        return body

    @staticmethod
    def get_symbols_from_content(
        content: str,
        symbols: Optional[Tuple[str, ...]] = None
    ) -> List[str]:
        """
        Search in content and return founded symbols.

        :param content: text for analyzing
        :param symbols: predefined list of symbols
        :return: founded symbols
        """
        if symbols is None:
            symbols = ("BTC", "ETH", "USDT", "USDC", "BNB")
        output = []
        lowercase_content = content.lower()
        for symbol in symbols:
            if symbol.lower() in lowercase_content:
                output += [symbol]
        return output

    def download(
        self,
        *,
        start_timestamp: Optional[pd.Timestamp] = pd.Timestamp.min,
        end_timestamp: Optional[pd.Timestamp] = pd.Timestamp.max,
        numbers_post_to_fetch: int = 5,
        subreddits: Optional[Tuple[str, ...]] = None
    ) -> sinsadow.RawData:
        """
        Download posts in the hot category in the predefined subreddits.

        :param start_timestamp: start datetime for searching
        :param end_timestamp: end datetime for searching
        :param numbers_post_to_fetch: maximum number posts to fetch
        :param subreddits: tuple of subreddits to fetch
        :return: downloaded data in raw format
        """
        if subreddits is None:
            subreddits = ("Cryptocurrency", "CryptoMarkets")
        output = []
        for subreddit in subreddits:
            # TODO(Vlad): This iterator is pretty slow: ~30s for the two
            #  subreddits and 10 posts for every subreddit.
            #  Have to be speed up for production usage.
            hot_posts = self.reddit_client.subreddit(subreddit).hot(
                limit=numbers_post_to_fetch
            )
            for post in hot_posts:
                post_timestamp = pd.Timestamp(
                    post.created_utc,
                    unit="s",
                    tzinfo=datetime.timezone.utc
                )
                if not start_timestamp <= post_timestamp <= end_timestamp:
                    continue
                if post.num_comments > 0:
                    top_comment = self.get_the_top_most_comment_body(post)
                else:
                    top_comment = ""
                output += [
                    RedditPostFeatures(
                        subreddit=subreddit,
                        created=post_timestamp,
                        symbols=self.get_symbols_from_content(post.selftext),
                        post_length=len(post.selftext),
                        title=post.title,
                        content=post.selftext,
                        number_of_upvotes=post.ups,
                        number_of_comments=post.num_comments,
                        top_comment=top_comment,
                    ).dict()
                ]
        return sinsadow.RawData(output)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    downloader = RedditDownloader()
    raw_data = downloader.download(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp)
    if len(raw_data.get_data()) > 0:
        mongo_saver = RedditMongoSaver(
            mongo_client=pymongo.MongoClient(
                host=MONGO_HOST,
                port=27017,
                username="mongo",
                password="mongo"
            ),
            collection_name="posts"
        )
        mongo_saver.save(raw_data)
    else:
        _LOG.info(
            "Empty output for datetime range: %s -  %s",
            args.start_timestamp,
            args.end_timestamp
        )


def add_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options for exchange download.
    """
    parser.add_argument(
        "--start_timestamp",
        required=True,
        action="store",
        type=str,
        help="Beginning of the loaded period, e.g. 2022-02-09 10:00:00+00:00",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the loaded period, e.g. 2022-02-10 10:00:00+00:00",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    hdbg.init_logger(use_exec_path=True)
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = add_download_args(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
