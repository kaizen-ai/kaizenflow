#!/usr/bin/env python
"""
Download reddit data and save it into the DB.

Use as:
> download_to_db.py --api_key 'change to your own api key' --collection_name cmc_data
"""
import argparse
import logging

import pandas as pd
import pymongo

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import sorrentum_sandbox.projects.Issue22_Team3_Implement_sandbox_for_Coinmarketcap.db as ssan_cmc_db
import sorrentum_sandbox.projects.Issue22_Team3_Implement_sandbox_for_Coinmarketcap.download as ssan_cmc_download

_LOG = logging.getLogger(__name__)

def add_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--start",
        action="store",
        required=True,
        type=int,
        help="Path to the target directory to store CSV data into",
    )
    parser.add_argument(
        "--limit",
        action="store",
        required=True,
        type=int,
        help="Path to the target directory to store CSV data into",
    )
    parser.add_argument(
        "--collection_name",
        action="store",
        required=True,
        type=str,
        help="Path to the target directory to store CSV data into",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    hdbg.init_logger(use_exec_path=True)
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = add_download_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    downloader = ssan_cmc_download.CMCRestApiDownloader()
    raw_data = downloader.download(args.start, args.limit)
    if len(raw_data.get_data()) > 0:
        mongo_saver = ssan_cmc_db.MongoDataSaver(
            mongo_client=pymongo.MongoClient(
                host="host.docker.internal",
                port=27017,
                username="mongo",
                password="mongo",
            ),
            db_name="CoinMarketCap",
        )
        mongo_saver.save(data=raw_data, collection_name=args.collection_name)
    else:
        _LOG.info(
            "Empty output"
        )

if __name__ == "__main__":
    _main(_parse())
