import logging

import pandas as pd

import helpers.hpandas as hpandas
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.db.db_utils as imvcddbut
import market_data.market_data_example as mdmadaex

_LOG = logging.getLogger(__name__)


class TestRealTimeMarketData2(
    imvcddbut.TestImDbHelper,
):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def setUp(self) -> None:
        super().setUp()
        # Create test table.
        universe_version = "infer_from_data"
        im_client = icdc.get_mock_realtime_client(
            universe_version, self.connection, resample_1min=True
        )
        # Set up market data client.
        self.market_data = mdmadaex.get_RealtimeMarketData2_example1(im_client)

    def tearDown(self) -> None:
        # Delete the table.
        hsql.remove_table(self.connection, "mock2_marketdata")
        super().tearDown()

    def test_get_data_for_last_period1(self) -> None:
        """
        Test get_data_for_last_period() all conditional periods.
        """
        # Set up processing parameters.
        timedelta = pd.Timedelta("1D")
        ts_col_name = "timestamp"
        actual = self.market_data.get_data_for_last_period(
            timedelta,
            ts_col_name=ts_col_name,
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
        # df=
        index=[2022-04-22 10:30:00-04:00, 2022-04-22 12:30:00-04:00]
        columns=asset_id,close,end_download_timestamp,full_symbol,high,id,knowledge_timestamp,low,open,start_timestamp,ticks,volume
        shape=(121, 12)
        asset_id close end_download_timestamp full_symbol high id knowledge_timestamp low open start_timestamp ticks volume
        end_timestamp
        2022-04-22 10:30:00-04:00 1464553467 60.0 2022-04-22 00:00:00+00:00 binance::ETH_USDT 40.0 0.0 2022-04-22 00:00:00+00:00 50.0 30.0 2022-04-22 10:29:00-04:00 80.0 70.0
        2022-04-22 10:31:00-04:00 1464553467 NaN NaT binance::ETH_USDT NaN NaN NaT NaN NaN 2022-04-22 10:30:00-04:00 NaN NaN
        2022-04-22 10:32:00-04:00 1464553467 NaN NaT binance::ETH_USDT NaN NaN NaT NaN NaN 2022-04-22 10:31:00-04:00 NaN NaN
        ...
        2022-04-22 12:28:00-04:00 1464553467 NaN NaT binance::ETH_USDT NaN NaN NaT NaN NaN 2022-04-22 12:27:00-04:00 NaN NaN
        2022-04-22 12:29:00-04:00 1464553467 NaN NaT binance::ETH_USDT NaN NaN NaT NaN NaN 2022-04-22 12:28:00-04:00 NaN NaN
        2022-04-22 12:30:00-04:00 1464553467 65.0 2022-04-22 00:00:00+00:00 binance::ETH_USDT 45.0 4.0 2022-04-22 00:00:00+00:00 55.0 35.0 2022-04-22 12:29:00-04:00 75.0 75.0
        """
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)

    def test_get_data_for_interval1(self) -> None:
        """
        - Ask data for [9:30, 9:45]
        """
        # Specify processing parameters.
        start_ts = pd.Timestamp("2022-04-22 09:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 09:45:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = self.market_data.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
        # df=
        index=[2022-04-22 10:30:00-04:00, 2022-04-22 10:30:00-04:00]
        columns=asset_id,close,end_download_timestamp,full_symbol,high,id,knowledge_timestamp,low,open,start_timestamp,ticks,volume
        shape=(1, 12)
        asset_id close end_download_timestamp full_symbol high id knowledge_timestamp low open start_timestamp ticks volume
        end_timestamp
        2022-04-22 10:30:00-04:00 1464553467 60.0 2022-04-22 00:00:00+00:00 binance::ETH_USDT 40.0 0 2022-04-22 00:00:00+00:00 50.0 30.0 2022-04-22 10:29:00-04:00 80.0 70.0
        """
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)

    def test_get_data_for_interval2(self) -> None:
        """
        - Ask data for [10:30, 12:00]
        """
        # Set up processing parameters.
        start_ts = pd.Timestamp("2022-04-22 10:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 12:00:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = self.market_data.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
        # df=
        index=[2022-04-22 12:30:00-04:00, 2022-04-22 12:30:00-04:00]
        columns=asset_id,close,end_download_timestamp,full_symbol,high,id,knowledge_timestamp,low,open,start_timestamp,ticks,volume
        shape=(1, 12)
        asset_id close end_download_timestamp full_symbol high id knowledge_timestamp low open start_timestamp ticks volume
        end_timestamp
        2022-04-22 12:30:00-04:00 1464553467 65.0 2022-04-22 00:00:00+00:00 binance::ETH_USDT 45.0 4 2022-04-22 00:00:00+00:00 55.0 35.0 2022-04-22 12:29:00-04:00 75.0 75.0
        """
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)

    def test_get_data_at_timestamp1(self) -> None:
        """
        - Ask data for 9:35
        - The returned data is for 9:35
        """
        # Set up processing parameters.
        ts = pd.Timestamp("2022-04-22 09:30:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        actual = self.market_data.get_data_at_timestamp(
            ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
       # df=
        index=[2022-04-22 10:30:00-04:00, 2022-04-22 10:30:00-04:00]
        columns=asset_id,close,end_download_timestamp,full_symbol,high,id,knowledge_timestamp,low,open,start_timestamp,ticks,volume
        shape=(1, 12)
        asset_id close end_download_timestamp full_symbol high id knowledge_timestamp low open start_timestamp ticks volume
        end_timestamp
        2022-04-22 10:30:00-04:00 1464553467 60.0 2022-04-22 00:00:00+00:00 binance::ETH_USDT 40.0 0 2022-04-22 00:00:00+00:00 50.0 30.0 2022-04-22 10:29:00-04:00 80.0 70.0
        """
        # pylint: enable=line-too-long
        self._check_dataframe(actual, expected_df_as_str)

    def test_get_twap_price1(self) -> None:
        """
        Test `get_twap_price()` for specified parameters.
        """
        # Set up processing parameters.
        start_ts = pd.Timestamp("2022-04-22 10:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 15:00:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        column = "close"
        # Get
        actual = self.market_data.get_twap_price(
            start_ts, end_ts, ts_col_name, asset_ids, column
        )
        expected_length = None
        expected_column_names = None
        expected_column_unique_values = None
        expected_df_as_str = r"""
        # df=
        index=[2022-04-22 15:00:00-05:00, 2022-04-22 15:00:00-05:00]
        columns=asset_id,start_timestamp,close
        shape=(1, 3)
                                     asset_id           start_timestamp  close
        end_timestamp
        2022-04-22 15:00:00-05:00  1464553467 2022-04-22 10:30:00-05:00   65.0
        """
        # pylint: enable=line-too-long
        # Check the output series.
        self.check_df_output(
            actual,
            expected_length=expected_length,
            expected_column_names=expected_column_names,
            expected_column_unique_values=expected_column_unique_values,
            expected_signature=expected_df_as_str,
        )

    def _check_dataframe(
        self,
        actual_df: pd.DataFrame,
        expected_df_as_str: str,
    ) -> None:
        """
        Check test results for Pandas dataframe format.

        We call it `_check_dataframe` and not `check_dataframe` to avoid overriding
        the method of the base class `TestCase`.

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
