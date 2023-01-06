#!/usr/bin/env python
"""
Download reddit data and save it into the DB.

Use as:
> download_to_db.py \
    --start_timestamp '2022-10-20 10:00:00+00:00' \
    --end_timestamp '2022-10-21 15:30:00+00:00'
"""
import argparse
import os
import logging

import pandas as pd
import pymongo

import helpers.hdbg as hdbg
import sorrentum_sandbox.examples.reddit.db as siseredb
import sorrentum_sandbox.examples.reddit.download as srseredo

_LOG = logging.getLogger(__name__)
MONGO_HOST = os.environ["MONGO_HOST"]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    downloader = srseredo.PostsDownloader()
    raw_data = downloader.download(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp)
    if len(raw_data.get_data()) > 0:
        mongo_saver = siseredb.RedditMongoSaver(
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
