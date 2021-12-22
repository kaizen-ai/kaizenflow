from typing import List, Optional

import pandas as pd

import helpers.datetime_ as hdateti


class ImTransform:
    """
    A class for handling common transform operations.

    These include:
    - Reindexing dataframes on datetime
    - Determining the partition of parquet datasets
    """

    # TODO(Danya): Merge functions from `hparquet.py`
    def save_partitioned_pq(
        self,
        df: pd.DataFrame,
        partition_col_names: List[str],
        dst_dir: str,
    ) -> None:
        """
        Partition given DataFrame indexed on datetime and save as parquet
        dataset.

        :param df: DataFrame with datetime index
        :param partition_col_names: partition columns, e.g. ['asset']
        :param dst_dir: location of partitioned dataset
        """
        raise NotImplementedError

    def reindex_on_datetime(
        self, df: pd.DataFrame, datetime_col_name: str
    ) -> pd.DataFrame:
        """
        Set datetime index to the dataframe.
        """
        # TODO(Danya): Assert that there is no datetime index already.
        datetime_col = df[datetime_col_name]
        datetime_idx = self.convert_timestamp_column(datetime_col)
        reindexed_df = df.set_index(datetime_idx)
        return reindexed_df

    @staticmethod
    def convert_timestamp_column(datetime_col: pd.Series) -> pd.Series:
        """
        Convert datetime as string or int into a timestamp.

        :param datetime_col: Series containing datetime as str or int
        :return: Series containing datetime as `pd.Timestamp`
        """
        # Convert unix epoch into Timestamp.
        if pd.api.types.is_integer_dtype(datetime_col):
            converted_datetime_col = datetime_col.apply(
                hdateti.convert_unix_epoch_to_timestamp
            )
        # Convert string into timestamp.
        elif pd.api.types.is_string_dtype(datetime_col):
            converted_datetime_col = hdateti.to_generalized_datetime(datetime_col)
        else:
            raise ValueError(
                "Incorrect data format. Datetime column should be of integer or string dtype."
            )
        return converted_datetime_col

    def get_date_partition_cols(self, partition_mode: str) -> pd.DataFrame:
        """
        Add partition columns like year, month, day from datetime index.

        :param partition_mode: date unit to partition, e.g. 'year'.
        :return: DataFrame with date partition cols added.
        """
        # TODO(Danya): possible values are 'year', 'month', 'day' and None.
        #  None means partitioning by entire date, e.g. "20211201".
        raise NotImplementedError
