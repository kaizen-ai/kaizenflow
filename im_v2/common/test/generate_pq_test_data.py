#!/usr/bin/env python
"""
Generate Parquet files for testing.

# Example:
> im_v2/common/test/generate_pq_test_data.py \
    --start_date 2021-11-23 \
    --end_date 2021-11-25 \
    --freq 1T \
    --partition_mode by_year_month \
    --data_type verbose2 \
    --assets 10689,10690 \
    --dst_dir im_v2/common/test/tiled.bar_data
"""

import argparse
import logging
from typing import Callable, List, Union

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparser as hparser
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


class ParquetDataFrameGenerator:
    DATA_TYPES = ("basic", "verbose1", "verbose2")
    ASSET_COLUMN_NAME_MAP = {
        "basic": "asset",
        "verbose1": "ticker",
        "verbose2": "asset_id",
    }

    def __init__(
        self,
        start_date: str,
        end_date: str,
        data_type: str,
        assets: List[Union[str, int]],
        freq: str,
    ) -> None:
        """
        Constructor.

        :param start_date: start of date range including start_date
        :param end_date: end of date range excluding end_date
        :param data_type: type of data that is generated
        :param assets: list of desired assets that can be names or ids
        :param freq: frequency of steps between start and end date
        """
        self._start_date = start_date
        self._end_date = end_date
        self._data_type = data_type
        self._assets = assets
        self._freq = freq
        # TODO(Nikola): Use `inclusive` instead `closed` after 1.4.0
        self._dataframe_index = pd.date_range(
            self._start_date, self._end_date, freq=self._freq, closed="left"
        )
        self._DATA_TYPE_FUNCTION_MAP = {
            "basic": self._get_daily_basic_dataframe,
            "verbose1": self._get_verbose1_dataframe,
            "verbose2": self._get_verbose2_dataframe,
        }

    @property
    def asset_column_name(self) -> str:
        return self.ASSET_COLUMN_NAME_MAP[self._data_type]

    @property
    def data_type_function(self) -> Callable:
        return self._DATA_TYPE_FUNCTION_MAP[self._data_type]

    def generate(self) -> pd.DataFrame:
        if self._data_type not in self.DATA_TYPES:
            raise ValueError(f"Unsupported data type `{self._data_type}`!")
        return self.data_type_function()

    def _get_core_dataframes(self) -> List[pd.DataFrame]:
        """
        Create core dataframes that are updated according to the data type.

        :return: list of core dataframes as presented below with asset column name
            as `asset` with string values
        ```
                     asset
        2000-01-01       A
        2000-01-02       A
        2000-01-03       A
        ```
        """
        # For each asset generate core data.
        df = []
        for asset in self._assets:
            asset_df = pd.DataFrame(
                {self.asset_column_name: asset},
                index=self._dataframe_index,
            )
            _LOG.debug(
                hpandas.df_to_str(asset_df, print_shape_info=True, tag="asset_df")
            )
            df.append(asset_df)
        return df

    def _get_daily_basic_dataframe(self) -> pd.DataFrame:
        """
        Update core dataframes with additional columns.

        :return: updated core dataframe as presented below
        ```
                    idx asset  val1  val2
        2000-01-01    0     A    00    00
        2000-01-02    0     A    01    01
        2000-01-03    0     A    02    02
        ```
        """
        asset_dataframes = self._get_core_dataframes()
        for idx, asset_dataframe in enumerate(asset_dataframes):
            # Before asset.
            asset_dataframe.insert(loc=0, column="idx", value=idx)
            # After asset.
            asset_dataframe.insert(
                loc=2,
                column="val1",
                value=list(range(len(self._dataframe_index))),
            )
            asset_dataframe.insert(
                loc=3,
                column="val2",
                value=list(range(len(self._dataframe_index))),
            )
        return self._wrap_all_assets_df(asset_dataframes)

    def _get_verbose1_dataframe(self) -> pd.DataFrame:
        """
        Update core dataframes with additional columns.

        :return: update core dataframe as presented below
        ```
        vendor_date  interval  start_time    end_time ticker currency  open    id
         2021-11-24        60  1637762400  1637762460      A      USD   100     1
         2021-11-24        60  1637762400  1637762460      A      USD   200     2
        ```
        """
        interval = self._dataframe_index[1] - self._dataframe_index[0]
        interval = interval.seconds
        asset_dataframes = self._get_core_dataframes()
        for id_, asset_dataframe in enumerate(asset_dataframes):
            start_time = (
                asset_dataframe.index - pd.Timestamp("1970-01-01")
            ) // pd.Timedelta("1s")
            end_time = start_time + interval
            # Before ticker.
            asset_dataframe.insert(
                loc=0,
                column="vendor_date",
                value=asset_dataframe.index.date.astype(str),
            )
            asset_dataframe.insert(loc=1, column="interval", value=interval)
            asset_dataframe.insert(loc=2, column="start_time", value=start_time)
            asset_dataframe.insert(loc=3, column="end_time", value=end_time)
            # After ticker.
            asset_dataframe.insert(loc=5, column="currency", value="USD")
            asset_dataframe.insert(
                loc=6,
                column="open",
                value=list(range(len(self._dataframe_index))),
            )
            asset_dataframe.insert(loc=7, column="id", value=id_)
        return self._wrap_all_assets_df(asset_dataframes)

    def _get_verbose2_dataframe(self) -> pd.DataFrame:
        """
        Update core dataframes with additional columns.

        :return: updated core dataframe as presented below
        ```
                    asset_id   close
        2000-01-01     10689     100
        2000-01-02     10689     200
        2000-01-03     10689     300
        ```
        """
        asset_dataframes = self._get_core_dataframes()
        for asset_dataframe in asset_dataframes:
            # After asset_id.
            asset_dataframe.insert(
                loc=1,
                column="close",
                value=list(range(len(self._dataframe_index))),
            )
        return self._wrap_all_assets_df(asset_dataframes)

    @staticmethod
    def _wrap_all_assets_df(df: List[pd.DataFrame]) -> pd.DataFrame:
        # Create a single df for all the assets.
        df = pd.concat(df)
        _LOG.debug(hpandas.df_to_str(df, print_shape_info=True, tag="df"))
        return df


def _run(args: argparse.Namespace) -> None:
    # Generate timespan.
    start_date = args.start_date
    end_date = args.end_date
    hdbg.dassert_lt(start_date, end_date)
    timespan = pd.date_range(start_date, end_date)
    hdbg.dassert_lt(2, len(timespan))
    # Obtain remaining args.
    freq = args.freq
    data_type = args.data_type
    partition_mode = args.partition_mode
    assets = args.assets
    assets = assets.split(",")
    dst_dir = args.dst_dir
    # Run dataframe generation.
    pdg = ParquetDataFrameGenerator(start_date, end_date, data_type, assets, freq)
    parquet_df = pdg.generate()
    # Add partition columns to the dataframe.
    df, partition_cols = imvcdttrut.add_date_partition_cols(
        parquet_df, partition_mode
    )
    # Partition and write dataset.
    if args.reset_index:
        df = df.reset_index(drop=True)
    imvcdttrut.partition_dataset(df, partition_cols, dst_dir)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Location that will be used to store generated data",
    )
    parser.add_argument(
        "--start_date",
        action="store",
        type=str,
        required=True,
        help="From when is data going to be created, including start date",
    )
    parser.add_argument(
        "--end_date",
        action="store",
        type=str,
        required=True,
        help="Until when is data going to be created, excluding end date",
    )
    parser.add_argument(
        "--freq",
        action="store",
        type=str,
        default="1H",
        help="Frequency of data generation",
    )
    parser.add_argument(
        "--assets",
        action="store",
        type=str,
        required=True,
        help="Comma separated string of assets that can be either names or ids",
    )
    parser.add_argument(
        "--data_type",
        action="store",
        type=str,
        default="basic",
        help="Type of data that is generated",
    )
    parser.add_argument(
        "--partition_mode",
        action="store",
        type=str,
        default="by_date",
        help="Partition Parquet dataframe by time",
    )
    parser.add_argument(
        "--no_partition",
        action="store_true",
        help="Whether to partition the resulting parquet",
    )
    parser.add_argument(
        "--reset_index",
        action="store_true",
        help="Resets dataframe index to default value",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    """
    Standard main part of the script that is parsing provided arguments.
    """
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
