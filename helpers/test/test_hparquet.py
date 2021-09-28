import datetime
import logging
import os
import random

import pandas as pd

import helpers.hparquet as hparquet
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestParquet1(hut.TestCase):

    def test1(self) -> None:
        # Prepare data.
        date = datetime.date(2020, 1, 1)
        df = self._get_df(date)
        _LOG.debug("df=\n%s", df.head(3))
        # Save data.
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "df.pq")
        hparquet.to_parquet(df, file_name, log_level=logging.INFO)
        # Read data back.
        df2 = hparquet.from_parquet(file_name, log_level=logging.INFO)
        _LOG.debug("df2=\n%s", df2.head(3))
        # Check.
        self.assert_equal(str(df), str(df2))
        self.assertTrue(df.equals(df2))

    def test_columns1(self) -> None:
        # Prepare data.
        date = datetime.date(2020, 1, 1)
        df = self._get_df(date)
        _LOG.debug("df=\n%s", df.head(3))
        # Save data.
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "df.pq")
        hparquet.to_parquet(df, file_name, log_level=logging.INFO)
        # Read data back.
        columns = ["val1"]
        df2 = hparquet.from_parquet(
            file_name, columns=columns, log_level=logging.INFO
        )
        _LOG.debug("df2=\n%s", df2.head(3))
        df = df[columns]
        # Check.
        self.assert_equal(str(df), str(df2))
        self.assertTrue(df.equals(df2))

    @staticmethod
    def _get_df(date: datetime.date, seed: int = 42) -> pd.DataFrame:
        """
        Create pandas random data, like:

        idx instr  val1  val2 2000-01-01    0     A    99
        30 2000-01-02    0     A    54    46 2000-01-03    0     A    85
        86
        """
        instruments = "A B C D E".split()
        date = pd.Timestamp(date, tz="America/New_York")
        start_date = date.replace(hour=9, minute=30)
        end_date = date.replace(hour=16, minute=0)
        df_idx = pd.date_range(start_date, end_date, freq="5T")
        _LOG.debug("df_idx=[%s, %s]", min(df_idx), max(df_idx))
        _LOG.debug("len(df_idx)=%s", len(df_idx))
        random.seed(seed)
        # For each instruments generate random data.
        df = []
        for idx, inst in enumerate(instruments):
            df_tmp = pd.DataFrame(
                {
                    "idx": idx,
                    "instr": inst,
                    "val1": [random.randint(0, 100) for _ in range(len(df_idx))],
                    "val2": [random.randint(0, 100) for _ in range(len(df_idx))],
                },
                index=df_idx,
            )
            df.append(df_tmp)
        # Create a single df for all the instruments.
        df = pd.concat(df)
        return df
