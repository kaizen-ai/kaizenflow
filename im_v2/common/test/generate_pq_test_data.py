#!/usr/bin/env python
"""
Generate Parquet files for testing.

# Use example:
> im_v2/common/test/generate_pq_test_data.py \
    --start_date 2021-11-23 \
    --end_date 2021-11-25 \
    --freq 1T \
    --partition_mode by_year_month \
    --output_type verbose_close \
    --assets 10689,10690 \
    --dst_dir im_v2/common/test/tiled.bar_data

# In command sample above, data is created for certain interval with minutely frequency
# that is further partitioned by Parquet dataset for year and month.
```
.../tiled.bar_data/asset_id=10689/year=2021/month=11
.../tiled.bar_data/asset_id=10689/year=2021/month=11/data.parquet
.../tiled.bar_data/asset_id=10690/year=2021/month=11
.../tiled.bar_data/asset_id=10690/year=2021/month=11/data.parquet
```
"""

import argparse
import logging
from typing import Callable, List, Union

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hparser as hparser

_LOG = logging.getLogger(__name__)


class ParquetDataFrameGenerator:
    # Allowed types.
    OUTPUT_TYPES = ("basic", "verbose_open", "cm_task_1103")
    # Depending on output type, asset column varies. This mapping is always
    # resolving to expected asset column name.
    ASSET_COLUMN_NAME_MAP = {
        "basic": "asset",
        "verbose_open": "ticker",
        "cm_task_1103": "asset_id",
    }

    def __init__(
        self,
        start_date: str,
        end_date: str,
        output_type: str,
        assets: List[Union[str, int]],
        freq: str,
    ) -> None:
        """
        Constructor.

        :param start_date: start of date range including start_date
        :param end_date: end of date range excluding end_date
        :param output_type: type of data that is generated
        :param assets: list of desired assets that can be names or ids
        :param freq: frequency of steps between start and end date
        """
        self._start_date = start_date
        self._end_date = end_date
        self._output_type = output_type
        self._assets = assets
        self._freq = freq
        # TODO(Nikola): Use `inclusive` instead `closed` after 1.4.0
        self._dataframe_index = pd.date_range(
            self._start_date,
            self._end_date,
            freq=self._freq,
            closed="left",
            tz="UTC",
        )
        self._OUTPUT_TYPE_FUNCTION_MAP = {
            "basic": self._get_daily_basic_dataframe,
            "verbose_open": self._get_verbose_open_dataframe,
            "cm_task_1103": self._get_cm_task_1103_dataframe,
        }

    @property
    def asset_column_name(self) -> str:
        """
        Return proper asset column name from map depending on output type.
        """
        return self.ASSET_COLUMN_NAME_MAP[self._output_type]

    @property
    def output_type_function(self) -> Callable:
        """
        Return proper function for data generation depending on output type.
        """
        return self._OUTPUT_TYPE_FUNCTION_MAP[self._output_type]

    def generate(self) -> pd.DataFrame:
        """
        Generate specific dataframe based on inputs provided in instance
        creation.
        """
        if self._output_type not in self.OUTPUT_TYPES:
            raise ValueError(f"Unsupported data type `{self._output_type}`!")
        return self.output_type_function()

    def _get_core_dataframes(self) -> List[pd.DataFrame]:
        """
        Create core dataframes that are updated according to the output type.

        :return: list of core dataframes as presented below with asset column name
            as `asset` with string values
        ```
                     asset
        2000-01-01       A
        2000-01-02       A
        2000-01-03       A
        ```
        """
        # Generate core dataframe for each asset.
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
            # Positioned left from `asset` column.
            asset_dataframe.insert(loc=0, column="idx", value=idx)
            # Positioned right from `asset` column.
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

    def _get_verbose_open_dataframe(self) -> pd.DataFrame:
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
            # Positioned left from `ticker` column.
            asset_dataframe.insert(
                loc=0,
                column="vendor_date",
                value=asset_dataframe.index.date.astype(str),
            )
            asset_dataframe.insert(loc=1, column="interval", value=interval)
            asset_dataframe.insert(loc=2, column="start_time", value=start_time)
            asset_dataframe.insert(loc=3, column="end_time", value=end_time)
            # Positioned right from `ticker` column.
            asset_dataframe.insert(loc=5, column="currency", value="USD")
            asset_dataframe.insert(
                loc=6,
                column="open",
                value=list(range(len(self._dataframe_index))),
            )
            asset_dataframe.insert(loc=7, column="id", value=id_)
        return self._wrap_all_assets_df(asset_dataframes)

    def _get_cm_task_1103_dataframe(self) -> pd.DataFrame:
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
            # Positioned right from `asset_id` column.
            asset_dataframe.insert(
                loc=1,
                column="close",
                value=list(range(len(self._dataframe_index))),
            )
        return self._wrap_all_assets_df(asset_dataframes)

    @staticmethod
    def _wrap_all_assets_df(df: List[pd.DataFrame]) -> pd.DataFrame:
        # Create a single dataframe for all the assets.
        df = pd.concat(df)
        _LOG.debug(hpandas.df_to_str(df, print_shape_info=True, tag="df"))
        return df


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
        help="From when is data going to be created, value included",
    )
    parser.add_argument(
        "--end_date",
        action="store",
        type=str,
        required=True,
        help="Until when is data going to be created, value excluded",
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
        "--output_type",
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
        "--reset_index",
        action="store_true",
        help="Resets dataframe index to default sequential integer values",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _run(parser: argparse.ArgumentParser) -> None:
    # Parse args.
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Generate timespan.
    start_date = args.start_date
    end_date = args.end_date
    hdbg.dassert_lt(start_date, end_date)
    timespan = pd.date_range(start_date, end_date)
    hdbg.dassert_lt(2, len(timespan))
    # Obtain remaining args.
    freq = args.freq
    output_type = args.output_type
    partition_mode = args.partition_mode
    assets = args.assets
    assets = assets.split(",")
    dst_dir = args.dst_dir
    # Run dataframe generation.
    pdg = ParquetDataFrameGenerator(
        start_date, end_date, output_type, assets, freq
    )
    parquet_df = pdg.generate()
    # Add partition columns to the dataframe.
    df, partition_cols = hparque.add_date_partition_columns(
        parquet_df, partition_mode
    )
    # Partition and write dataset.
    if args.reset_index:
        df = df.reset_index(drop=True)
    hparque.to_partitioned_parquet(df, partition_cols, dst_dir)


if __name__ == "__main__":
    _run(_parse())