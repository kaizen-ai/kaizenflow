"""
Example implementation of abstract classes for ETL and QA pipeline.

Download Reddit data
"""
import dataclasses
import datetime
import logging
import os
from typing import List, Tuple

import praw

import surrentum_infra_sandbox.download as sinsadow

_LOG = logging.getLogger(__name__)
NUMBERS_POST_TO_FETCH = 100
REDDIT_USER_AGENT = "ck_extractor"
REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_SECRET = os.environ["REDDIT_SECRET"]
SUBREDDITS = ["Cryptocurrency", "CryptoMarkets"]
SYMBOLS = (
    "BTC", "ETH", "USDT", "USDC", "BNB", "XRP", "BUSD", "DOGE", "ADA", "MATIC",
    "DAI", "WTRX", "TRX", "DOT", "LTC", "SHIB", "STETH", "UNI7", "SOL", "AVAX",
    "LEO", "HEX", "WBTC", "LINK", "ATOM", "XMR", "TON11419", "ETC",
    "BCH", "XLM"
)


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
        lowercased_content = content.lower()
        for symbol in symbols:
            if symbol.lower() in lowercased_content:
                output += [symbol]
        return output

    def download(
        self, *, start_timestamp=None, end_timestamp=None
    ) -> sinsadow.RawData:
        """
        Download posts in the hot category in the predefined subreddits

        :param start_timestamp:
        :param end_timestamp:
        :return: downloaded data in raw format
        """
        output = []
        for subreddit in SUBREDDITS:
            hot_posts = self.reddit_client.subreddit(
                subreddit).hot(limit=NUMBERS_POST_TO_FETCH)
            for post in hot_posts:
                output += [
                    RedditPostFeatures(
                        created=datetime.datetime.fromtimestamp(post.created_utc),
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
