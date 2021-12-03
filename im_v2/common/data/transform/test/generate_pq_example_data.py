#!/usr/bin/env python
"""
Generate daily PQ files.

# Example:
> python im_v2/common/data/transform/test/generate_pq_example_data.py \
    --start_date 2021-11-23 \
    --end_date 2021-11-25 \
    --assets A,B,C \
    --dst_dir im_v2/common/data/transform/test_data_by_date

Import as:

import im_v2.common.data.transform.generate_pq_example_data as imvcdtgped
"""

import argparse
import logging
from typing import List

import pandas as pd

import helpers.dbg as hdbg
import helpers.hparquet as hparque
import helpers.parser as hparser
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


def _get_daily_df(
    start_date: str, end_date: str, assets: List[str], freq: str
) -> pd.DataFrame:
    """
    Create data for the interval [start_date, end_date].

    :param start_date: start of date range including start_date
    :param end_date: end of date range excluding end_date
    :param assets: list of desired assets
    :param freq: frequency of steps between start and end date
    :return: daily dataframe as presented below
    ```
                idx asset  val1  val2
    2000-01-01    0     A    00    00
    2000-01-02    0     A    01    01
    2000-01-03    0     A    02    02
    ```
    """
    df_idx = pd.date_range(start_date, end_date, freq=freq)
    _LOG.debug("df_idx=[%s, %s]", min(df_idx), max(df_idx))
    _LOG.debug("len(df_idx)=%s", len(df_idx))
    # For each asset generate random data.
    df = []
    for idx, asset in enumerate(assets):
        df_tmp = pd.DataFrame(
            {
                "idx": idx,
                "asset": asset,
                "val1": list(range(len(df_idx))),
                "val2": list(range(len(df_idx))),
            },
            index=df_idx,
        )
        # Drop last midnight.
        # TODO(Nikola): end_date - pd.DateOffset(days=1)
        df_tmp.drop(df_tmp.tail(1).index, inplace=True)
        _LOG.debug(hprint.df_to_short_str("df_tmp", df_tmp))
        df.append(df_tmp)
    # Create a single df for all the assets.
    df = pd.concat(df)
    _LOG.debug(hprint.df_to_short_str("df", df))
    return df


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--start_date",
        action="store",
        type=str,
        required=True,
        help="From when is data going to be created, including start date.",
    )
    parser.add_argument(
        "--end_date",
        action="store",
        type=str,
        required=True,
        help="Until when is data going to be created, excluding end date.",
    )
    parser.add_argument(
        "--assets",
        action="store",
        type=str,
        required=True,
        help="Comma separated string of assets.",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Location that will be used to store generated data.",
    )
    parser.add_argument(
        "--freq",
        action="store",
        type=str,
        help="Frequency of data generation. Defaults to one hour.",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Generation timespan.
    start_date = args.start_date
    end_date = args.end_date
    # TODO(Nikola): Custom exceptions ?
    if start_date > end_date:
        raise ValueError("Start date can not be greater than end date!")
    assets = args.assets
    assets = assets.split(",")
    dst_dir = args.dst_dir
    freq = args.freq if args.freq else "1H"
    dummy_df = _get_daily_df(start_date, end_date, assets, freq)
    hparque.save_daily_df_as_pq(dummy_df, dst_dir)


if __name__ == "__main__":
    _main(_parse())
