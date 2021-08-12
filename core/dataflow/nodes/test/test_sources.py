import logging
import os
import time
from typing import Any, Optional

import pandas as pd

import core.dataflow as dtf
import core.dataflow.test.test_real_time as cdtfttrt
import helpers.unit_test as hut  # pylint: disable=no-name-in-module

_LOG = logging.getLogger(__name__)


# #############################################################################


class TestDiskDataSource(hut.TestCase):
    def test_datetime_index_csv1(self) -> None:
        """
        Test CSV file using timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        #
        ext = ".csv"
        timestamp_col = None
        self._helper(df, ext, timestamp_col)

    def test_datetime_col_csv1(self) -> None:
        """
        Test CSV file using timestamps in a column.
        """
        df = TestDiskDataSource._generate_df()
        df = df.reset_index()
        #
        ext = ".csv"
        timestamp_col = "timestamp"
        self._helper(df, ext, timestamp_col)

    def test_datetime_index_parquet1(self) -> None:
        """
        Test Parquet file using timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        #
        ext = ".pq"
        timestamp_col = None
        self._helper(df, ext, timestamp_col)

    def test_datetime_col_parquet1(self) -> None:
        """
        Test Parquet file using timestamps in a column.
        """
        df = TestDiskDataSource._generate_df()
        df = df.reset_index()
        #
        ext = ".pq"
        timestamp_col = "timestamp"
        self._helper(df, ext, timestamp_col)

    def test_filter_dates1(self) -> None:
        """
        Test date filtering with both boundaries specified for CSV file using
        timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        #
        ext = ".csv"
        timestamp_col = None
        dds_kwargs = {"start_date": "2010-01-02", "end_date": "2010-01-05"}
        self._helper(df, ext, timestamp_col, **dds_kwargs)

    def test_filter_dates_open_boundary1(self) -> None:
        """
        Test date filtering with one boundary specified for CSV file using
        timestamps in the index.
        """
        df = TestDiskDataSource._generate_df()
        #
        ext = ".csv"
        timestamp_col = None
        dds_kwargs = {"start_date": "2010-01-02"}
        self._helper(df, ext, timestamp_col, **dds_kwargs)

    @staticmethod
    def _generate_df(num_periods: int = 10) -> pd.DataFrame:
        """
        Generate a df with a format like:
        ```
                    0
        timestamp
        2010-01-02  1
        2010-01-03  2
        2010-01-04  3
        2010-01-05  4
        ```
        """
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

    def _helper(
        self,
        df: pd.DataFrame,
        ext: str,
        timestamp_col: Optional[str],
        **dds_kwargs: Any,
    ) -> None:
        """
        Instantiate a `DiskDataSource` with the passed parameter, run it and
        check.
        """
        # Save the data with the proper extension.
        file_path = self._save_df(df, ext)
        # Instantiate node.
        dds = dtf.DiskDataSource(
            "read_data",
            file_path=file_path,
            timestamp_col=timestamp_col,
            **dds_kwargs,
        )
        # Run node.
        loaded_df = dds.fit()["df_out"]
        # Check output.
        act_result = loaded_df.to_string()
        self.check_string(act_result)


# #############################################################################


class TestArmaGenerator(hut.TestCase):
    def test1(self) -> None:
        node = dtf.ArmaGenerator(  # pylint: disable=no-member
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
        act = hut.convert_df_to_string(df, index=True, decimals=2)
        self.check_string(act)


# #############################################################################


class TestMultivariateNormalGenerator(hut.TestCase):
    def test1(self) -> None:
        node = dtf.MultivariateNormalGenerator(  # pylint: disable=no-member
            nid="source",
            frequency="30T",
            start_date="2010-01-04 09:00",
            end_date="2010-01-04 17:00",
            dim=4,
            target_volatility=10,
            seed=1,
        )
        df = node.fit()["df_out"]
        act = hut.convert_df_to_string(df, index=True, decimals=2)
        self.check_string(act)


# #############################################################################


class TestRealTimeDataSource1(hut.TestCase):
    def test_simulated_real_time1(self) -> None:
        """
        Setting the current time to a specific datetime, the node generates
        values up and including that datetime.
        """
        # Build the node.
        nid = "rtds"
        delay_in_secs = 0.0
        # No external clock, but set the clock through an explicit call to
        # `set_current_time()`.
        data_builder, data_builder_kwargs = cdtfttrt.get_test_data_builder1()
        rtds = dtf.SimulatedRealTimeDataSource(
            nid,
            delay_in_secs=delay_in_secs,
            data_builder=data_builder,
            data_builder_kwargs=data_builder_kwargs,
        )
        # Execute.
        current_time = pd.Timestamp("2010-01-04 09:35:00")
        rtds.set_current_time(current_time)
        self._helper(rtds)

    def test_true_real_time1(self) -> None:
        # Build the node.
        nid = "rtds"
        delay_in_secs = 0.0
        # Return always the same time.
        get_current_time = lambda: pd.Timestamp("2010-01-04 09:35:00")
        data_builder, data_builder_kwargs = cdtfttrt.get_test_data_builder2()
        rtds = dtf.TrueRealTimeDataSource(  # pylint: disable=no-member
            nid,
            delay_in_secs=delay_in_secs,
            external_clock=get_current_time,
            data_builder=data_builder,
            data_builder_kwargs=data_builder_kwargs,
        )
        # Execute.
        self._helper(rtds)

    def test_replayed_real_time1(self) -> None:
        # Build the node.
        nid = "rtds"
        delay_in_secs = 0.0
        # Use a replayed real-time starting at the same time as the data.
        rrt = dtf.ReplayedTime(
            pd.Timestamp("2010-01-04 09:30:00"),
            # 1.1 is a fudge factor to make sure we get in the next minute.
            speed_up_factor=60 * 1.1,
        )
        get_current_time = rrt.get_replayed_current_time
        #
        data_builder, data_builder_kwargs = cdtfttrt.get_test_data_builder1()
        rtds = dtf.ReplayedRealTimeDataSource(  # pylint: disable=no-member
            nid,
            delay_in_secs=delay_in_secs,
            external_clock=get_current_time,
            data_builder=data_builder,
            data_builder_kwargs=data_builder_kwargs,
        )
        # Execute right away.
        dict_ = rtds.fit()
        # Check.
        df = dict_["df_out"]
        act = list(map(str, df.index.tolist()))
        exp = [
            "2010-01-04 09:30:00",
        ]
        self.assert_equal(str(act), str(exp))
        # Execute after a replayed second that corresponds to a minute.
        time.sleep(1)
        dict_ = rtds.fit()
        # Check.
        df = dict_["df_out"]
        act = list(map(str, df.index.tolist()))
        exp = [
            "2010-01-04 09:30:00",
            "2010-01-04 09:31:00",
        ]
        self.assert_equal(str(act), str(exp))

    def _helper(
        self, rtds: dtf.ReplayedRealTimeDataSource
    ) -> None:
        # Execute.
        dict_ = rtds.fit()
        # Check.
        df = dict_["df_out"]
        act = list(map(str, df.index.tolist()))
        exp = [
            "2010-01-04 09:30:00",
            "2010-01-04 09:31:00",
            "2010-01-04 09:32:00",
            "2010-01-04 09:33:00",
            "2010-01-04 09:34:00",
            "2010-01-04 09:35:00",
        ]
        self.assert_equal(str(act), str(exp))

    # TODO(gp): Add a test with delay_in_secs
