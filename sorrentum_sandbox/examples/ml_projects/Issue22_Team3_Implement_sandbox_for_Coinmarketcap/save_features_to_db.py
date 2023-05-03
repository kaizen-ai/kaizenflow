#!/usr/bin/env python
"""
Load and validate data within a specified time period from MongoDB collection,
extract features and load back to the DB.

# Load Reddit data:
> load_validate_transform.py \
    --source_collection 'posts' \
    --target_collection 'processed_posts'
"""
import argparse
import logging

import pandas as pd
import pymongo

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import sorrentum_sandbox.common.download as ssandown
import sorrentum_sandbox.common.validate as sinsaval

import sorrentum_sandbox.examples.ml_projects.Issue22_Team3_Implement_sandbox_for_Coinmarketcap as coinmarketcap

coinmarketcap_load = coinmarketcap.load_data_from_db
coinmarketcap_db = coinmarketcap.db
coinmarketcap_features = coinmarketcap.compute_features


_LOG = logging.getLogger(__name__)

# #############################################################################
# Script.
# #############################################################################


def _add_load_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--source_collection",
        action="store",
        required=False,
        type=str,
        default="posts",
        help="DB collection to load data from",
    )
    parser.add_argument(
        "--target_collection",
        action="store",
        required=False,
        default="processed_posts",
        type=str,
        help="DB collection to save transformed data into",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = _add_load_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(use_exec_path=True)


    # 1) Build client.
    mongodb_client = pymongo.MongoClient(
        host="host.docker.internal", 
        port=27017, 
        username="mongo", 
        password="mongo"
    )
    coinmarketcap_mongo_client = coinmarketcap_db.MongoClient(mongodb_client, "CoinMarketCap")

    # 2) Load data.
    cmc_data = coinmarketcap_mongo_client.load(collection_name=args.source_collection)

    if len(cmc_data) == 0:
        _LOG.info("Loaded dataset is empty.")
        return
    _LOG.info("Loaded data: \n %s", cmc_data.head())

    # 2) Transform data.
    features = coinmarketcap_features.extract_features(data)

    # 5) Save back to db.
    _LOG.info("Extracted features: \n %s", features.head())
    db_saver = coinmarketcap_db.MongoDataSaver(
        mongo_client=mongodb_client, db_name="CoinMarketCap"
    )
    db_saver.save(
        data=ssandown.RawData(features), collection_name=args.target_collection
    )
    _LOG.info("Features saved to MongoDB.")


if __name__ == "__main__":
    _main(_parse())