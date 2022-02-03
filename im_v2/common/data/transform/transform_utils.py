"""
Implement common transform operations.

Import as:

import im_v2.common.data.transform.transform_utils as imvcdttrut
"""

import logging
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.htimer as htimer

_LOG = logging.getLogger(__name__)


def convert_timestamp_column(
    datetime_col_name: pd.Series,
    unit: str = "ms",
) -> pd.Series:
    """
    Convert datetime as string or int into a timestamp.

    :param datetime_col_name: series containing datetime as str or int
    :param unit: the unit of unix epoch
    :return: series containing datetime as `pd.Timestamp`
    """
    if pd.api.types.is_integer_dtype(datetime_col_name):
        # Convert unix epoch into timestamp.
        kwargs = {"unit": unit}
        converted_datetime_col = datetime_col_name.apply(
            hdateti.convert_unix_epoch_to_timestamp, **kwargs
        )
    elif pd.api.types.is_string_dtype(datetime_col_name):
        # Convert string into timestamp.
        converted_datetime_col = hdateti.to_generalized_datetime(
            datetime_col_name
        )
    else:
        raise ValueError(
            "Incorrect data format. Datetime column should be of int or str dtype"
        )
    return converted_datetime_col


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
    hdbg.dassert_in(datetime_col_name, df.columns)
    hdbg.dassert_ne(
        df.index.inferred_type, "datetime64", "Datetime index already exists"
    )
    with htimer.TimedScope(logging.DEBUG, "# reindex_on_datetime"):
        datetime_col_name = df[datetime_col_name]
        # Convert original datetime column into `pd.Timestamp`.
        datetime_idx = convert_timestamp_column(datetime_col_name, unit=unit)
        df = df.set_index(datetime_idx)
    return df


# TODO(gp): -> add_date_partition_columns
def add_date_partition_cols(
    df: pd.DataFrame, partition_mode: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Add partition columns like year, month, day from datetime index.

    :param df: dataframe indexed by timestamp
    :param partition_mode:
        - "by_date": extract the date from the index
            - E.g., an index like `2022-01-10 14:00:00+00:00` is transform to a
              column `20220110`
        - "by_year_month_day": split the index in year, month, day columns
        - "by_year_month": split by year and month
        - "by_year_week": split by year and week of the year
        - "by_year": split by year
    :return:
        - df with additional partitioning columns
        - list of partitioning columns
    """
    with htimer.TimedScope(logging.DEBUG, "# add_date_partition_cols"):
        if partition_mode == "by_date":
            df["date"] = df.index.strftime("%Y%m%d")
            partition_columns = ["date"]
        else:
            if partition_mode == "by_year_month_day":
                partition_columns = ["year", "month", "date"]
            elif partition_mode == "by_year_month":
                partition_columns = ["year", "month"]
            elif partition_mode == "by_year_week":
                partition_columns = ["year", "weekofyear"]
            elif partition_mode == "by_year":
                partition_columns = ["year"]
            elif partition_mode == "by_month":
                partition_columns = ["month"]
            else:
                raise ValueError(f"Invalid partition_mode='{partition_mode}'")
            # Add date columns chosen by partition mode.
            for column_name in partition_columns:
                # Extract data corresponding to `column_name` (e.g.,
                # `df.index.year`).
                df[column_name] = getattr(df.index, column_name)
    return df, partition_columns


def partition_dataset(
    df: pd.DataFrame, partition_columns: List[str], dst_dir: str
) -> None:
    """
    Save the given dataframe as Parquet file partitioned along the given columns.

    :param df: dataframe
    :param partition_columns: partitioning columns
    :param dst_dir: location of partitioned dataset

    E.g., in case of partition using `date`, the file layout looks like:
    ```
    dst_dir/
        date=20211230/
            data.parquet
        date=20211231/
            data.parquet
        date=20220101/
            data.parquet
    ```

    In case of multiple columns like `year`, `month`, `asset`, the file layout
    looks like:
    ```
    dst_dir/
        year=2021/
            month=12/
                asset=A/
                    data.parquet
                asset=B/
                    data.parquet
        ...
        year=2022/
            month=01/
                asset=A/
                    data.parquet
                asset=B/
                    data.parquet
    ```

    """
    with htimer.TimedScope(logging.DEBUG, "# partition_dataset"):
        # Read.
        table = pa.Table.from_pandas(df)
        # Write using partition.
        # TODO(gp): @Nikola add this logic to hparquet.to_parquet as a possible
        #  option.
        _LOG.debug(hprint.to_str("partition_columns dst_dir"))
        hdbg.dassert_is_subset(partition_columns, df.columns)
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_columns,
            partition_filename_cb=lambda x: "data.parquet",
        )
