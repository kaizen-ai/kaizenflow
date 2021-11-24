"""
Import as:

import im_v2.common.data.transform.generate_pq_example_data as imvcdtgped
"""

import logging
import random

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.printing as hprint
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)


def _get_dummy_df(
    start_date: str, end_date: str, assets: str, freq: str
) -> pd.DataFrame:
    """
    Create Pandas random data, like:

    ```
                idx asset  val1  val2
    2000-01-01    0     A    99    30
    2000-01-02    0     A    54    46
    2000-01-03    0     A    85    86
    ```
    """
    df_idx = pd.date_range(start_date, end_date, freq=freq)
    _LOG.debug("df_idx=[%s, %s]", min(df_idx), max(df_idx))
    _LOG.debug("len(df_idx)=%s", len(df_idx))
    random.seed(1000)
    # For each asset generate random data.
    df = []
    for idx, asset in enumerate(assets.split(",")):
        df_tmp = pd.DataFrame(
            {
                "idx": idx,
                "asset": asset,
                "asset_for_check": asset,
                "val1": [random.randint(0, 100) for _ in range(len(df_idx))],
                "val2": [random.randint(0, 100) for _ in range(len(df_idx))],
            },
            index=df_idx,
        )
        # drop last midnight
        df_tmp.drop(df_tmp.tail(1).index, inplace=True)
        _LOG.debug(hprint.df_to_short_str("df_tmp", df_tmp))
        df.append(df_tmp)
    # Create a single df for all the assets.
    df = pd.concat(df)
    _LOG.debug(hprint.df_to_short_str("df", df))
    return df


def _save_dummy_df_daily_data_as_pq(df: pd.DataFrame, dst_dir: str) -> None:
    """
    Mimics current daily parquet structure in production.
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


def generate(
    start_date: str,
    end_date: str,
    assets: str = "A,B,C",
    freq: str = "1H",
    *,
    dst_dir: str,
) -> None:
    dummy_df = _get_dummy_df(start_date, end_date, assets, freq)
    _save_dummy_df_daily_data_as_pq(dummy_df, dst_dir)
