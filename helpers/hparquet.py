"""
Import as:

import helpers.hparquet as hparque
"""

import logging
import os
from typing import Any, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.dbg as hdbg
import helpers.introspection as hintros
import helpers.io_ as hio
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)


def to_parquet(
    df: pd.DataFrame,
    file_name: str,
    *,
    log_level: int = logging.DEBUG,
) -> None:
    """
    Save a dataframe as Parquet.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_isinstance(file_name, str)
    hdbg.dassert_file_extension(file_name, ["pq", "parquet"])
    #
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintros.format_size(mem))
    # Save data.
    with htimer.TimedScope(logging.DEBUG, "To parquet '%s'" % file_name) as ts:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_name)
    # Report stats.
    file_size = hintros.format_size(os.path.getsize(file_name))
    _LOG.log(
        log_level,
        "Saved '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )


# TODO(gp): What's the difference with read_pq? Maybe we use pandas there,
# while here we use PQ directly with Dataset.
def from_parquet(
    file_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Any]] = None,
    *,
    log_level: int = logging.DEBUG,
) -> pd.DataFrame:
    """
    Load a dataframe from a Parquet file.
    """
    hdbg.dassert_isinstance(file_name, str)
    hdbg.dassert_file_extension(file_name, ["pq", "parquet"])
    # Load data.
    with htimer.TimedScope(logging.DEBUG, "From parquet '%s'" % file_name) as ts:
        filesystem = None
        dataset = pq.ParquetDataset(
            file_name,
            filesystem=filesystem,
            filters=filters,
            use_legacy_dataset=False,
        )
        # To read also the index we need to use `read_pandas()`, instead of `read_table()`.
        # See https://arrow.apache.org/docs/python/parquet.html#reading-and-writing-single-files.
        table = dataset.read_pandas(columns=columns)
        df = table.to_pandas()
    # Report stats.
    file_size = hintros.format_size(os.path.getsize(file_name))
    _LOG.log(
        log_level,
        "Loaded '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )
    # Report stats about the df.
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintros.format_size(mem))
    return df


# TODO(Nikola): Add tests for new functions below.
#   Adapt to different formats, if needed.
#   Currently only ccxt_ohlcv.
def save_daily_df_as_pq(df: pd.DataFrame, dst_dir: str) -> None:
    """
    Create and save a daily parquet structure as below:

    ```
    dst_dir/
        date=20211230/
            data.parquet
        date=20211231/
            data.parquet
        date=20221231/
            data.parquet
    ```
    """
    date_col_name = "date"
    with htimer.TimedScope(logging.DEBUG, "Create partition dates"):
        with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
            df[date_col_name] = df.index.strftime("%Y%m%d")
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(df)
        partition_cols = [date_col_name]
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_cols,
            partition_filename_cb=lambda x: "data.parquet",
        )


def save_pq_by_asset(
    asset_col_name: str, parquet_df_by_date: pd.DataFrame, dst_dir: str
) -> None:
    """
    Save a dataframe in a Parquet format in `dst_dir` partitioned by year and
    asset.

    ```
    dst_dir/
        year=2021/
            month=11/
                day=06/
                    asset=A/
                        data.parquet
                    asset=B/
                        data.parquet
        year=2022/
            month=11/
                day=07/
                    asset=A/
                        data.parquet
                    asset=B/
                        data.parquet
    ```
    """
    date_col_names = ["year", "month", "day"]
    with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
        for name in date_col_names:
            parquet_df_by_date[name] = getattr(parquet_df_by_date.index, name)
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(parquet_df_by_date)
        partition_cols = [*date_col_names, asset_col_name]
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_cols,
            partition_filename_cb=lambda x: "data.parquet",
        )
