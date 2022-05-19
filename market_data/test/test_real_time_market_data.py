import logging

import pandas as pd

import helpers.hpandas as hpandas
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.db.db_utils as imvcddbut
import market_data.real_time_market_data as mdrtmada

_LOG = logging.getLogger(__name__)


class TestRealTimeMarketData2(
    imvcddbut.TestImDbHelper,
):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 1000

    def setup_test_market_data(
        self, im_client: icdc.SqlRealTimeImClient
    ) -> mdrtmada.RealTimeMarketData2:
        """
        Setup RealTimeMarketData2 interface.
        """
        asset_id_col = "asset_id"
        asset_ids = [1464553467]
        start_time_col_name = "start_timestamp"
        end_time_col_name = "end_timestamp"
        columns = None
        get_wall_clock_time = lambda: pd.Timestamp(
            "2022-04-22", tz="America/New_York"
        )
        market_data = mdrtmada.RealTimeMarketData2(
            im_client,
            asset_id_col,
            asset_ids,
            start_time_col_name,
            end_time_col_name,
            columns,
            get_wall_clock_time,
        )
        return market_data

    def test_get_data_for_last_period1(self) -> None:
        """
        Test get_data_for_last_period() all conditional periods.
        """
        # Create test table.
        im_client = icdc.get_example2_realtime_client(
            self.connection, resample_1min=True
        )
        # Set up market data client.
        market_data = self.setup_test_market_data(im_client)
        # Set up processing parameters.
        timedelta = pd.Timedelta("1D")
        ts_column_name = "timestamp"
        actual = market_data.get_data_for_last_period(
            timedelta,
            ts_column_name,
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""# df=
                        index=[2022-04-22 10:30:00-04:00, 2022-04-22 12:30:00-04:00]
                        columns=asset_id,close,full_symbol,high,low,open,start_timestamp,volume
                        shape=(121, 8)
                        asset_id close full_symbol high low open start_timestamp volume
                        end_timestamp
                        2022-04-22 10:30:00-04:00 1464553467 60.0 binance::ETH_USDT 40.0 50.0 30.0 2022-04-22 10:29:00-04:00 70.0
                        2022-04-22 10:31:00-04:00 <NA> NaN binance::ETH_USDT NaN NaN NaN NaT NaN
                        2022-04-22 10:32:00-04:00 <NA> NaN binance::ETH_USDT NaN NaN NaN NaT NaN
                        ...
                        2022-04-22 12:28:00-04:00 <NA> NaN binance::ETH_USDT NaN NaN NaN NaT NaN
                        2022-04-22 12:29:00-04:00 <NA> NaN binance::ETH_USDT NaN NaN NaN NaT NaN
                        2022-04-22 12:30:00-04:00 1464553467 65.0 binance::ETH_USDT 45.0 55.0 35.0 2022-04-22 12:29:00-04:00 75.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "example2_marketdata")

    def test_get_data_for_interval1(self) -> None:
        """
        - Ask data for [9:30, 9:45]
        """
        # Create test table.
        im_client = icdc.get_example2_realtime_client(
            self.connection, resample_1min=True
        )
        # Set up market data client.
        market_data = self.setup_test_market_data(im_client)
        # Specify processing parameters.
        start_ts = pd.Timestamp("2022-04-22 09:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 09:45:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = market_data.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""# df=
                        index=[2022-04-22 10:30:00-04:00, 2022-04-22 10:30:00-04:00]
                        columns=asset_id,close,full_symbol,high,low,open,start_timestamp,volume
                        shape=(1, 8)
                        asset_id close full_symbol high low open start_timestamp volume
                        end_timestamp
                        2022-04-22 10:30:00-04:00 1464553467 60.0 binance::ETH_USDT 40.0 50.0 30.0 2022-04-22 10:29:00-04:00 70.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "example2_marketdata")

    def test_get_data_for_interval2(self) -> None:
        """
        - Ask data for [10:30, 12:00]
        """
        # Create test table.
        im_client = icdc.get_example2_realtime_client(
            self.connection, resample_1min=True
        )
        # Set up market data client.
        market_data = self.setup_test_market_data(im_client)
        # Set up processing parameters.
        start_ts = pd.Timestamp("2022-04-22 10:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 12:00:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = market_data.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""# df=
                        index=[2022-04-22 12:30:00-04:00, 2022-04-22 12:30:00-04:00]
                        columns=asset_id,close,full_symbol,high,low,open,start_timestamp,volume
                        shape=(1, 8)
                        asset_id close full_symbol high low open start_timestamp volume
                        end_timestamp
                        2022-04-22 12:30:00-04:00 1464553467 65.0 binance::ETH_USDT 45.0 55.0 35.0 2022-04-22 12:29:00-04:00 75.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "example2_marketdata")

    def test_get_data_at_timestamp1(self) -> None:
        """
        - Ask data for 9:35
        - The returned data is for 9:35
        """
        # Create test table.
        im_client = icdc.get_example2_realtime_client(
            self.connection, resample_1min=True
        )
        # Set up market data client.
        market_data = self.setup_test_market_data(im_client)
        # Set up processing parameters.
        ts = pd.Timestamp("2022-04-22 09:30:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = market_data.get_data_at_timestamp(ts, ts_col_name, asset_ids)
        # pylint: disable=line-too-long
        expected_df_as_str = r"""# df=
                                index=[2022-04-22 10:30:00-04:00, 2022-04-22 10:30:00-04:00]
                                columns=asset_id,close,full_symbol,high,low,open,start_timestamp,volume
                                shape=(1, 8)
                                asset_id close full_symbol high low open start_timestamp volume
                                end_timestamp
                                2022-04-22 10:30:00-04:00 1464553467 60.0 binance::ETH_USDT 40.0 50.0 30.0 2022-04-22 10:29:00-04:00 70.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "example2_marketdata")

    def test_get_twap_price1(self) -> None:
        """
        Test `get_twap_price()` for specified parameters.
        """
        # Create test table.
        im_client = icdc.get_example2_realtime_client(
            self.connection, resample_1min=True
        )
        # Set up market data client.
        market_data = self.setup_test_market_data(im_client)
        # Set up processing parameters.
        start_ts = pd.Timestamp("2022-04-22 10:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 15:00:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        column = "close"
        # Get
        actual = market_data.get_twap_price(
            start_ts, end_ts, ts_col_name, asset_ids, column
        )
        expected_length = None
        expected_unique_values = None
        expected_df_as_str = r"""            close
        asset_id
        1464553467   65.0"""
        # pylint: enable=line-too-long
        # Check the output series.
        self.check_srs_output(
            actual, expected_length, expected_unique_values, expected_df_as_str
        )
        # Delete the table.
        hsql.remove_table(self.connection, "example2_marketdata")

    def _check_dataframe(
        self,
        actual_df: pd.DataFrame,
        expected_df_as_str: str,
    ) -> None:
        """
        Check test results for Pandas dataframe format.

        :param actual_df: the result dataframe
        :param expected_df_as_str: expected result in string format
        """
        # Check.
        actual_df = actual_df[sorted(actual_df.columns)]
        actual_df_as_str = hpandas.df_to_str(
            actual_df, print_shape_info=True, tag="df"
        )
        _LOG.info("-> %s", actual_df_as_str)
        self.assert_equal(
            actual_df_as_str,
            expected_df_as_str,
            dedent=True,
            fuzzy_match=True,
        )
