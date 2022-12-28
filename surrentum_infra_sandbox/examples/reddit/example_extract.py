"""
Example implementation of abstract classes for ETL and QA pipeline.

Download Reddit data
"""
import logging
import os

import praw

import surrentum_infra_sandbox.download as sinsadow

_LOG = logging.getLogger(__name__)
REDDIT_USER_AGENT = "ck_extractor"
REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_SECRET = os.environ["REDDIT_SECRET"]


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

    def download(
        self, *, start_timestamp=None, end_timestamp=None
    ) -> sinsadow.RawData:
        # get 10 hot posts from the MachineLearning subreddit
        hot_posts = self.reddit_client.subreddit('MachineLearning').hot(limit=10)
        for post in hot_posts:
            print(post.title)
        return sinsadow.RawData(hot_posts)


if __name__ == '__main__':
    downloader = RedditDownloader()
    downloader.download()
