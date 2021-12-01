"""
Import as:

import im_v2.common.data.transform.generate_pq_example_data as imvcdtgped
"""

import logging
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.printing as hprint
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)

# TODO(gp): If this is testing code it should go in test_convert_pq_by_...


# TODO(gp): Why so much redundancy with _save_pq_by_asset?
def _save_daily_df_as_pq(df: pd.DataFrame, dst_dir: str) -> None:
    """
    Create and save a daily parquet structure as below:

    ```
    dst_dir/
        by_date/
            date=20211230/
                data.parquet
            date=20211231/
                data.parquet
            date=20221231/
                data.parquet
    ```
    """
    date_column_name = "date"
    with htimer.TimedScope(logging.DEBUG, "Create partition idxs"):
        df[date_column_name] = df.index.strftime("%Y%m%d")
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(df)
        partition_cols = [date_column_name]
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_cols,
            partition_filename_cb=lambda x: "data.parquet",
        )


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


# TODO(gp): Very thin. Is it needed?
def generate_pq_daily_data(
    start_date: str,
    end_date: str,
    assets: List[str],
    *,
    freq: str = "1H",
    dst_dir: str,
) -> None:
    dummy_df = _get_daily_df(start_date, end_date, assets, freq)
    _save_daily_df_as_pq(dummy_df, dst_dir)
