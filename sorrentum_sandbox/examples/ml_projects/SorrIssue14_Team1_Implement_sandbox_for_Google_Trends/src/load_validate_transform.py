#!/usr/bin/env python
"""
Load and validate data within a specified time period from a PostgreSQL table,
resample, and load back the result into PostgreSQL.

# Load, validate and transform OHLCV data for binance:
> load_validate_transform.py \
    --start_timestamp '2022-10-20 12:00:00+00:00' \
    --end_timestamp '2022-10-21 12:00:00+00:00' \
    --source_table 'binance_ohlcv_spot_downloaded_1min' \
    --target_table 'binance_ohlcv_spot_resampled_5min'
"""

import logging
import common.validate as sinsaval
import src.db as sisebidb
import src.validate as sisebiva
from utilities import custom_logger

# _LOG = logging.getLogger(__name__)
# yaml_log_path = "/var/lib/app/data/"
# docker_log_path = "/root/logs/"
# _LOG = custom_logger.logger(yaml_log_path+"load_validate_transform.py.log")
# #############################################################################
# Script.
# #############################################################################


if __name__ == "__main__":
    topic = "iphone"
    # topic = topic.lower()

    #_LOG.info("-------------------------------------------")
    #_LOG.info("Fetching a connection to postgres db...")
    db_conn = sisebidb.get_db_connection()
    #_LOG.info("Connection fetched")

    #_LOG.info("Creating a Postgres client with the connection...")
    db_client = sisebidb.PostgresClient(db_conn)
    #_LOG.info("Client created")

    #_LOG.info("Fetching records with the topic: "+topic+" ...")
    data = db_client.load(dataset_signature=topic)
    #_LOG.info("Dataframe fetched")

    #_LOG.info("Renaming columns...")
    data.rename(columns={'topic': 'Topic', 'date_stamp': 'Time', 'frequency':'Frequency'}, inplace=True)
    print(data)
    #_LOG.info(f"Loaded data: \n {data.head()}")

    #_LOG.info("Initialising Normalization checker...")
    denormalized_dataset_check = sisebiva.DenormalizedDatasetCheck()
    #_LOG.info("Checker created")

    #_LOG.info("Adding checker to SingleDatasetValidator class...")
    dataset_validator = sinsaval.SingleDatasetValidator([denormalized_dataset_check])
    #_LOG.info("Check added")

    #_LOG.info("Running Validations...")
    dataset_validator.run_all_checks([data])
