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

_LOG = logging.getLogger(__name__)

# #############################################################################
# Script.
# #############################################################################


if __name__ == "__main__":
    # 1) Load data.
    db_conn = sisebidb.get_db_connection()
    db_client = sisebidb.PostgresClient(db_conn)
    data = db_client.load(dataset_signature="summer")
    data.rename(columns={'topic': 'Topic', 'date_stamp': 'Time', 'frequency':'Frequency'}, inplace=True)
    _LOG.info(f"Loaded data: \n {data.head()}")
    # 2) QA
    denormalized_dataset_check = sisebiva.DenormalizedDatasetCheck()

    dataset_validator = sinsaval.SingleDatasetValidator([denormalized_dataset_check])
    dataset_validator.run_all_checks([data])
    print(data)