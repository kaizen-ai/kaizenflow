import logging
from typing import Optional

import pandas as pd

import helpers.hpandas as hpandas
import helpers.hsql as hsql
import im_v2.common.db.db_utils as imvcddbut
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.talos.db.utils as imvtadbut
import market_data.real_time_market_data as mdrtmada

_LOG = logging.getLogger(__name__)


class TestRealTimeMarketData2(
    imvcddbut.TestImDbHelper,
):
    def setup_talos_market_data(
        self,
        resample_1min: Optional[bool] = True,
    ) -> mdrtmada.RealTimeMarketData2:
        """
        Setup RealTimeMarketData2 interface.
        """
        table_name = "talos_ohlcv"
        mode = "market_data"
        sql_talos_client = imvtdctacl.RealTimeSqlTalosClient(
            resample_1min, self.connection, table_name, mode
        )
        asset_id_col = "asset_id"
        asset_ids = [1464553467]
        start_time_col_name = "start_timestamp"
        end_time_col_name = "end_timestamp"
        columns = None
        get_wall_clock_time = lambda: pd.Timestamp("2022-04-22", tz="America/New_York")
        market_data = mdrtmada.RealTimeMarketData2(
            sql_talos_client,
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
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        # Set up market data client.
        market_data = self.setup_talos_market_data()
        # Set up processing parameters.
        timedelta = pd.Timedelta("1D")
        actual = market_data.get_data_for_last_period(
            timedelta
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""df="""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_get_data_for_interval1(self) -> None:
        """
        - Ask data for [9:30, 9:45]
        """
        # Create test table.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        # Set up market data client.
        market_data = self.setup_talos_market_data()
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
        columns=asset_id,close,high,low,open,start_timestamp,volume
        shape=(1, 7)
                                    asset_id  close  high   low  open           start_timestamp  volume
        end_timestamp
        2022-04-22 10:30:00-04:00  1464553467   60.0  40.0  50.0  30.0 2022-04-22 10:29:00-04:00    70.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_get_data_for_interval2(self) -> None:
        """
        - Ask data for [10:30, 12:00]
        """
        # Create test table.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        # Set up market data client.
        market_data = self.setup_talos_market_data()
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
        columns=asset_id,close,high,low,open,start_timestamp,volume
        shape=(1, 7)
                                    asset_id  close  high   low  open           start_timestamp  volume
        end_timestamp
        2022-04-22 12:30:00-04:00  1464553467   65.0  45.0  55.0  35.0 2022-04-22 12:29:00-04:00    75.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_get_data_at_timestamp1(self) -> None:
        """
        - Ask data for 9:35
        - The returned data is for 9:35
        """
        # Create test table.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        # Set up market data client.
        market_data = self.setup_talos_market_data()
        # Set up processing parameters.
        ts = pd.Timestamp("2022-04-22 09:30:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = market_data.get_data_at_timestamp(
            ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""# df=
        index=[2022-04-22 10:30:00-04:00, 2022-04-22 10:30:00-04:00]
        columns=asset_id,close,high,low,open,start_timestamp,volume
        shape=(1, 7)
                                    asset_id  close  high   low  open           start_timestamp  volume
        end_timestamp
        2022-04-22 10:30:00-04:00  1464553467   60.0  40.0  50.0  30.0 2022-04-22 10:29:00-04:00    70.0"""
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_get_twap_price1(self) -> None:
        """
        Test `get_twap_price()` for specified parameters.
        """
        # Create test table.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        # Set up market data client.
        market_data = self.setup_talos_market_data()
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
        hsql.remove_table(self.connection, "talos_ohlcv")

    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        """
        Create a test Talos OHLCV dataframe.
        """
        test_data = pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "ticks",
                "currency_pair",
                "exchange_id",
                "end_download_timestamp",
                "knowledge_timestamp",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [0, 1650637800000, 30, 40, 50, 60, 70, 80, "ETH_USDT", "binance", pd.Timestamp("2022-04-22"),
                 pd.Timestamp("2022-04-22")],
                [1, 1650638400000, 31, 41, 51, 61, 71, 72, "BTC_USDT", "binance", pd.Timestamp("2022-04-22"),
                 pd.Timestamp("2022-04-22")],
                [2, 1650639600000, 32, 42, 52, 62, 72, 73, "ETH_USDT", "binance", pd.Timestamp("2022-04-22"),
                 pd.Timestamp("2022-04-22")],
                [3, 1650641400000, 34, 44, 54, 64, 74, 74, "BTC_USDT", "binance", pd.Timestamp("2022-04-22"),
                 pd.Timestamp("2022-04-22")],
                [4, 1650645000000, 35, 45, 55, 65, 75, 75, "ETH_USDT", "binance", pd.Timestamp("2022-04-22"),
                 pd.Timestamp("2022-04-22")],
                [5, 1650647400000, 36, 46, 56, 66, 76, 76, "BTC_USDT", "binance", pd.Timestamp("2022-04-22"),
                 pd.Timestamp("2022-04-22")]
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        return test_data

    def _create_test_table(self) -> None:
        """
        Create a test Talos OHLCV table in DB.
        """
        query = imvtadbut.get_talos_ohlcv_create_table_query()
        self.connection.cursor().execute(query)

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