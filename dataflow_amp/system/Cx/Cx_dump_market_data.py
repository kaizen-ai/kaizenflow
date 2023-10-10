#!/usr/bin/env python
# TODO(Grisha): the script might become more general purpose, e.g. dump any data from db.
"""
The script saves market data from the DB to a file.
```
> dataflow_amp/system/Cx/Cx_dump_market_data.py \
    --dst_dir '/shared_data/prod_reconciliation' \
    --start_timestamp_as_str 20221010_060500 \
    --end_timestamp_as_str 20221010_080000 \
```
"""
import argparse
import logging
import os
from typing import List

import pandas as pd

import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import im_v2.common.universe as ivcu
import oms

_LOG = logging.getLogger(__name__)


# TODO(Grisha): pass universe version and factor out the code that returns
# universe as asset_ids.
def _get_universe() -> List[int]:
    """
    Get a specified universe of assets.
    """
    vendor = "CCXT"
    mode = "trade"
    version = "v7.1"
    as_full_symbol = True
    full_symbols = ivcu.get_vendor_universe(
        vendor,
        mode,
        version=version,
        as_full_symbol=as_full_symbol,
    )
    # TODO(Grisha): select top20.
    asset_ids = [
        ivcu.string_to_numerical_id(full_symbol) for full_symbol in full_symbols
    ]
    _LOG.info("len(asset_ids)=%s", len(asset_ids))
    return asset_ids


def dump_market_data_from_db(
    dst_dir: str,
    start_timestamp_as_str: str,
    end_timestamp_as_str: str,
) -> None:
    """
    Save market data from the DB to a file.
    """
    start_timestamp = oms.timestamp_as_str_to_timestamp(start_timestamp_as_str)
    end_timestamp = oms.timestamp_as_str_to_timestamp(end_timestamp_as_str)
    # We need to use exactly the same data that the prod system ran against
    # in production.
    asset_ids = _get_universe()
    market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
    # Save data.
    file_name = "test_data.csv.gz"
    file_path = os.path.join(dst_dir, file_name)
    # Dump data for the last 7 days.
    history_start_timestamp = start_timestamp - pd.Timedelta("7D")
    # TODO(Grisha): a bit weird that we should pass `_start_time_col_name` twice, i.e.
    # when we initialize `MarketData` and in `get_data_for_interval()`.
    timestamp_col_name = market_data._start_time_col_name
    data = market_data.get_data_for_interval(
        history_start_timestamp,
        end_timestamp,
        timestamp_col_name,
        asset_ids,
        right_close=True,
    )
    # TODO(Grisha): extend `save_market_data()` so that it accepts a starting point.
    data.to_csv(file_path, compression="gzip", index=True)
    _LOG.info("Saving in '%s' done", file_path)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        type=str,
        help="Dir to save market data in.",
    )
    parser.add_argument(
        "--start_timestamp_as_str",
        action="store",
        required=True,
        type=str,
        help="String representation of the earliest date timestamp to load data for.",
    )
    parser.add_argument(
        "--end_timestamp_as_str",
        action="store",
        required=True,
        type=str,
        help="String representation of the latest date timestamp to load data for.",
    )
    parser = hparser.add_verbosity_arg(parser)
    # TODO(gp): For some reason, not even this makes mypy happy.
    # cast(argparse.ArgumentParser, parser)
    return parser  # type: ignore


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dump_market_data_from_db(
        args.dst_dir,
        args.start_timestamp_as_str,
        args.end_timestamp_as_str,
    )


if __name__ == "__main__":
    _main(_parse())
