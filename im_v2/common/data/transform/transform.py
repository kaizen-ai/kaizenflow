"""
Import as:

import im_v2.common.data.transform.transform as imvcdtrtr
"""

import logging
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.timer as htimer


class ImTransform:
    """
    A class for handling common transform operations. These include:

    - Reindexing dataframes on datetime
    - Determining the partition of parquet datasets
    """

    @staticmethod
    def partition_dataset(
        df: pd.DataFrame, partition_cols: List[str], dst_dir: str
    ) -> None:
        """
        Partition given dataframe indexed on datetime and save as Parquet
        dataset.

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

        :param df: dataframe with datetime index
        :param partition_cols: partition columns, e.g. ['asset']
        :param dst_dir: location of partitioned dataset
        """
        hdbg.dassert_is_subset(partition_cols, df.columns)
        with htimer.TimedScope(logging.DEBUG, "Save data"):
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                dst_dir,
                partition_cols=partition_cols,
                partition_filename_cb=lambda x: "data.parquet",
            )

    def reindex_on_datetime(
        self, df: pd.DataFrame, datetime_col_name: str, unit: str = "ms"
    ) -> pd.DataFrame:
        """
        Set datetime index to the dataframe.

        :param df: dataframe without datetime index
        :param datetime_col_name: name of the column containing time info
        :param unit: the unit of unix epoch
        :return: dataframe with datetime index
        """
        msg = "Datetime index already exists!"
        hdbg.dassert_type_is(
            df.index, pd.core.indexes.datetimes.DatetimeIndex, msg
        )
        datetime_col = df[datetime_col_name]
        datetime_idx = self.convert_timestamp_column(datetime_col, unit=unit)
        reindexed_df = df.set_index(datetime_idx)
        return reindexed_df

    @staticmethod
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

    @staticmethod
    def add_date_partition_cols(
        df: pd.DataFrame, partition_mode: str = "no_partition"
    ) -> None:
        """
        Add partition columns like year, month, day from datetime index.
        "no_partition" means partitioning by entire date, e.g. "20211201".

        :param df: original dataframe
        :param partition_mode: date unit to partition, e.g. 'year'
        """
        date_col_names = ["year", "month", "day"]
        msg = f"Invalid partition mode `{partition_mode}`!"
        hdbg.dassert_in(partition_mode, [*date_col_names, "no_partition"], msg)
        with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
            if partition_mode != "no_partition":
                for name in date_col_names:
                    df[name] = getattr(df.index, name)
                    if name == partition_mode:
                        break
            else:
                df["date"] = df.index.strftime("%Y%m%d")
