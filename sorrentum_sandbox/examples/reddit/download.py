"""
Extract part of the ETL and QA pipeline.

Import as:

import sorrentum_sandbox.examples.reddit.download as srseredo
"""
import dataclasses
import datetime
import logging
import os
from typing import List, Optional, Tuple, Any

import pandas as pd
import praw

import sorrentum_sandbox.download as sinsadow

_LOG = logging.getLogger(__name__)
REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_SECRET = os.environ["REDDIT_SECRET"]


@dataclasses.dataclass
class PostFeatures:
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


class PostsDownloader(sinsadow.DataDownloader):
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
            _LOG.warning(
                "Error fetching top comment for the post: %s", post.title
            )
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
            hot_posts = self.reddit_client.subreddit(subreddit).new(
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
                    PostFeatures(
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
