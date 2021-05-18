import logging
import os

import pandas as pd

import core.dataflow as dtf
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestDiskDataSource(hut.TestCase):
    def test_datetime_index_csv1(self) -> None:
        """
        Test CSV file using timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        file_path = self._save_df(df, ".csv")
        timestamp_col = None
        rdfd = dtf.DiskDataSource("read_data", file_path, timestamp_col)
        loaded_df = rdfd.fit()["df_out"]
        self.check_string(loaded_df.to_string())

    def test_datetime_col_csv1(self) -> None:
        """
        Test CSV file using timestamps in a column.
        """
        df = TestDiskDataSource._generate_df()
        df = df.reset_index()
        file_path = self._save_df(df, ".csv")
        timestamp_col = "timestamp"
        rdfd = dtf.DiskDataSource("read_data", file_path, timestamp_col)
        loaded_df = rdfd.fit()["df_out"]
        self.check_string(loaded_df.to_string())

    def test_datetime_index_parquet1(self) -> None:
        """
        Test Parquet file using timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        file_path = self._save_df(df, ".pq")
        timestamp_col = None
        rdfd = dtf.DiskDataSource("read_data", file_path, timestamp_col)
        loaded_df = rdfd.fit()["df_out"]
        self.check_string(loaded_df.to_string())

    def test_datetime_col_parquet1(self) -> None:
        """
        Test Parquet file using timestamps in a column.
        """
        df = TestDiskDataSource._generate_df()
        df = df.reset_index()
        file_path = self._save_df(df, ".pq")
        timestamp_col = "timestamp"
        rdfd = dtf.DiskDataSource("read_data", file_path, timestamp_col)
        loaded_df = rdfd.fit()["df_out"]
        self.check_string(loaded_df.to_string())

    def test_filter_dates1(self) -> None:
        """
        Test date filtering with both boundaries specified for CSV file using
        timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        file_path = self._save_df(df, ".csv")
        timestamp_col = None
        rdfd = dtf.DiskDataSource(
            "read_data",
            file_path,
            timestamp_col,
            start_date="2010-01-02",
            end_date="2010-01-05",
        )
        loaded_df = rdfd.fit()["df_out"]
        self.check_string(loaded_df.to_string())

    def test_filter_dates_open_boundary1(self) -> None:
        """
        Test date filtering with one boundary specified for CSV file using
        timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        file_path = self._save_df(df, ".csv")
        timestamp_col = None
        rdfd = dtf.DiskDataSource(
            "read_data",
            file_path,
            timestamp_col,
            start_date="2010-01-02",
        )
        loaded_df = rdfd.fit()["df_out"]
        self.check_string(loaded_df.to_string())

    @staticmethod
    def _generate_df(num_periods: int = 10) -> pd.DataFrame:
        idx = pd.date_range("2010-01-01", periods=num_periods, name="timestamp")
        df = pd.DataFrame(range(num_periods), index=idx, columns=["0"])
        return df

    def _save_df(self, df: pd.DataFrame, ext: str) -> str:
        scratch_space = self.get_scratch_space()
        file_path = os.path.join(scratch_space, f"df{ext}")
        if ext == ".csv":
            df.to_csv(file_path)
        elif ext == ".pq":
            df.to_parquet(file_path)
        else:
            raise ValueError("Invalid extension='%s'" % ext)
        return file_path


class TestArmaGenerator(hut.TestCase):
    def test1(self) -> None:
        node = dtf.ArmaGenerator(
            nid="source",
            frequency="30T",
            start_date="2010-01-04 09:00",
            end_date="2010-01-04 17:00",
            ar_coeffs=[0],
            ma_coeffs=[0],
            scale=0.1,
            burnin=0,
            seed=0,
        )
        df = node.fit()["df_out"]
        str = hut.convert_df_to_string(df, index=True, decimals=2)
        self.check_string(str)


class TestMultivariateNormalGenerator(hut.TestCase):
    def test1(self) -> None:
        node = dtf.MultivariateNormalGenerator(
            nid="source",
            frequency="30T",
            start_date="2010-01-04 09:00",
            end_date="2010-01-04 17:00",
            dim=4,
            target_volatility=10,
            seed=1,
        )
        df = node.fit()["df_out"]
        str = hut.convert_df_to_string(df, index=True, decimals=2)
        self.check_string(str)
