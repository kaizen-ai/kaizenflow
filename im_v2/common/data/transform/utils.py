"""
A module for handling common transform operations. These include:

- Reindexing dataframes on datetime
- Determining the partition of parquet datasets

Import as:

import im_v2.common.data.transform.utils as imvcdtrut
"""

import logging
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.timer as htimer


def partition_dataset(
    df: pd.DataFrame, partition_cols: List[str], dst_dir: str
) -> None:
    """
    Partition given dataframe indexed on datetime and save as Parquet dataset.

    In case of date partition, file layout format looks like:
    ```
    dst_dir/
        date=20211230/
            data.parquet
        date=20211231/
            data.parquet
        date=20220101/
            data.parquet
    ```

    In case of specific choice, e.g. `month`, file layout format looks like:
    ```
    dst_dir/
        year=2021/
            month=12/
                asset=A/
                    data.parquet
                asset=B/
                    data.parquet
        year=2022/
            month=01/
                asset=A/
                    data.parquet
                asset=B/
                    data.parquet
    ```

    :param df: dataframe with datetime index
    :param partition_cols: partition columns, e.g. ['asset']
    :param dst_dir: location of partitioned dataset
    """
    hdbg.dassert_is_subset(partition_cols, df.columns)
    with htimer.TimedScope(logging.DEBUG, "Saving data"):
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_cols,
            partition_filename_cb=lambda x: "data.parquet",
        )


def reindex_on_datetime(
    df: pd.DataFrame, datetime_col_name: str, unit: str = "ms"
) -> pd.DataFrame:
    """
    Set datetime index to the dataframe.

    :param df: dataframe without datetime index
    :param datetime_col_name: name of the column containing time info
    :param unit: the unit of unix epoch
    :return: dataframe with datetime index
    """
    msg = f"`{datetime_col_name}` is not valid column name!"
    hdbg.dassert_in(datetime_col_name, df.columns, msg)
    msg = "Datetime index already exists!"
    hdbg.dassert_ne(df.index.inferred_type, "datetime64", msg)
    datetime_col = df[datetime_col_name]
    # Convert original datetime column into Timestamp.
    datetime_idx = convert_timestamp_column(datetime_col, unit=unit)
    reindexed_df = df.set_index(datetime_idx)
    return reindexed_df


def convert_timestamp_column(
    datetime_col: pd.Series, unit: str = "ms"
) -> pd.Series:
    """
    Convert datetime as string or int into a timestamp.

    :param datetime_col: series containing datetime as str or int
    :param unit: the unit of unix epoch
    :return: series containing datetime as `pd.Timestamp`
    """
    if pd.api.types.is_integer_dtype(datetime_col):
        # Convert unix epoch into timestamp.
        kwargs = {"unit": unit}
        converted_datetime_col = datetime_col.apply(
            hdateti.convert_unix_epoch_to_timestamp, **kwargs
        )
    elif pd.api.types.is_string_dtype(datetime_col):
        # Convert string into timestamp.
        converted_datetime_col = hdateti.to_generalized_datetime(datetime_col)
    else:
        raise ValueError(
            "Incorrect data format. Datetime column should be of integer or string dtype."
        )
    return converted_datetime_col


def add_date_partition_cols(
    df: pd.DataFrame, partition_mode: str = "no_partition"
) -> None:
    """
    Add partition columns like year, month, day from datetime index.

    "no_partition" means partitioning by entire date, e.g. "20211201".

    :param df: original dataframe
    :param partition_mode: date unit to partition, e.g. 'year'
    """
    # Check if partition mode is valid.
    date_col_names = ["year", "month", "day"]
    msg = f"Invalid partition mode `{partition_mode}`!"
    hdbg.dassert_in(partition_mode, [*date_col_names, "no_partition"], msg)
    with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
        # Check if there is partition mode.
        if partition_mode != "no_partition":
            # Add date columns chosen by partition mode.
            for name in date_col_names:
                df[name] = getattr(df.index, name)
                if name == partition_mode:
                    # Check if reached the lowest granularity level.
                    break
        else:
            # Use generic date string if there is no partitioned mode.
            df["date"] = df.index.strftime("%Y%m%d")
