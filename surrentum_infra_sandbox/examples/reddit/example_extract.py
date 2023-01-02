"""
Example implementation of abstract classes for ETL and QA pipeline.

Download Reddit data
"""
import dataclasses
import datetime
import logging
import os
import pandas as pd
from typing import List, Tuple

import praw

import surrentum_infra_sandbox.download as sinsadow

_LOG = logging.getLogger(__name__)
NUMBERS_POST_TO_FETCH = 5
REDDIT_USER_AGENT = "ck_extractor"
REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_SECRET = os.environ["REDDIT_SECRET"]
SUBREDDITS = ["Cryptocurrency", "CryptoMarkets"]
SYMBOLS = ("BTC", "ETH", "USDT", "USDC", "BNB")


@dataclasses.dataclass
class RedditPostFeatures:
    created: datetime.datetime
    symbols: List[str]
    post_length: int
    title: str
    content: str
    number_of_upvotes: int
    number_of_comments: int
    top_comment: str


class RedditDownloader(sinsadow.DataDownloader):
    """
    Class for downloading reddit data using praw lib
    """
    def __init__(self):
        self.reddit_client = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_SECRET,
            user_agent=REDDIT_SECRET
        )

    @staticmethod
    def get_the_top_most_comment_body(post: praw.models.Submission) -> str:
        """
        Get the top most comment body from a praw post.

        :param post: Post for searching
        :return: Body of the top most comment
        """
        try:
            body = post.comments[0].body
        except IndexError:
            body = ""
        return body

    @staticmethod
    def get_symbols_from_content(
            content: str,
            symbols: Tuple[str] = SYMBOLS
    ) -> List[str]:
        """
        Search in content and return founded symbols.

        :param content: Text for analyzing
        :param symbols: Predefined list of symbols
        :return: Founded symbols
        """
        output = []
        lowercase_content = content.lower()
        for symbol in symbols:
            if symbol.lower() in lowercase_content:
                output += [symbol]
        return output

    def download(
        self,
        *,
        start_timestamp: pd.Timestamp = None,
        end_timestamp: pd.Timestamp = None
    ) -> sinsadow.RawData:
        """
        Download posts in the hot category in the predefined subreddits

        :param start_timestamp: start datetime for searching
        :param end_timestamp: end datetime for searching
        :return: downloaded data in raw format
        """
        output = []
        start_timestamp = start_timestamp or pd.Timestamp.min
        end_timestamp = end_timestamp or pd.Timestamp.max
        for subreddit in SUBREDDITS:
            # TODO(*): This iterator is pretty slow: ~30s for the two subreddits
            #   and 10 posts for every subreddit. Have to be speed up for
            #   production usage.
            hot_posts = self.reddit_client.subreddit(
                subreddit).hot(limit=NUMBERS_POST_TO_FETCH)
            for post in hot_posts:
                post_timestamp = pd.Timestamp(post.created_utc, unit="s")
                if not start_timestamp <= post_timestamp <= end_timestamp:
                    continue
                output += [
                    RedditPostFeatures(
                        created=post_timestamp,
                        symbols=self.get_symbols_from_content(post.selftext),
                        post_length=len(post.selftext),
                        title=post.title,
                        content=post.selftext,
                        number_of_upvotes=post.ups,
                        number_of_comments=post.num_comments,
                        top_comment=self.get_the_top_most_comment_body(post)
                    )
                ]
        return sinsadow.RawData(output)


if __name__ == '__main__':
    downloader = RedditDownloader()
    downloader.download()
