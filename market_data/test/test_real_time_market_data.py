import logging
from typing import Any, Callable, Optional, List

import pandas as pd

import helpers.hsql as hsql
import helpers.hunit_test as hunitest
import helpers.hpandas as hpandas

import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.common.db.db_utils as imvcddbut
import market_data.real_time_market_data as mdrtmda
import market_data.market_data_example as mdmadaex
import market_data.test.market_data_test_case as mdtmdtca

import im_v2.talos.db.utils as imvtadbut


_LOG = logging.getLogger(__name__)


# TODO(gp): Factor out this test in a ReplayedMarketData_TestCase
def _check_get_data(
    self_: Any,
    client: imvtdctacl.RealTimeSqlTalosClient,
    func: Callable,
    expected_df_as_str: str,
) -> mdrtmda.RealTimeMarketData2:
    """

    """
    asset_id_col = "asset_id"
    asset_ids = [1464553467]
    start_time_col_name = "start_timestamp"
    end_time_col_name = "end_timestamp"
    columns = None
    get_wall_clock_time = lambda x: pd.Timestamp("2022-04-22")
    market_data = mdrtmda.RealTimeMarketData2(client,
                                asset_id_col,
                                asset_ids,
                                start_time_col_name,
                                end_time_col_name,
                                columns,
                                get_wall_clock_time)  
    # Execute function under test.
    actual_df = func(market_data)
    # Check.
    actual_df = actual_df[sorted(actual_df.columns)]
    actual_df_as_str = hpandas.df_to_str(
        actual_df, print_shape_info=True, tag="df"
    )
    _LOG.info("-> %s", actual_df_as_str)
    self_.assert_equal(
        actual_df_as_str,
        expected_df_as_str,
        dedent=True,
        fuzzy_match=True,
    )
    return market_data

class TestRealTimeMarketData2(imvcddbut.TestImDbHelper, 
    ):

    def setup_talos_sql_client(
        self,
        resample_1min: Optional[bool] = True,
    ) -> imvtdctacl.RealTimeSqlTalosClient:
        """
        Setup RealTimeMarketData2 interface.
        """
        table_name = "talos_ohlcv"
        mode = "market_data"
        sql_talos_client = imvtdctacl.RealTimeSqlTalosClient(
            resample_1min, self.connection, table_name, mode
        )
        return sql_talos_client


    def test_get_data_for_interval1(self) -> None:
        """
        - Start replaying time 5 minutes after the beginning of the day, i.e., the
          current time is 9:35.
        - Ask data for [9:30, 9:45]
        - The returned data is [9:30, 9:35].
        """
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        client = self.setup_talos_sql_client()
        
        start_ts = pd.Timestamp("2022-04-21 02:30:00-05:00")
        end_ts = pd.Timestamp("2022-04-22 15:45:00-05:00")
        ts_col_name = "timestamp"
        asset_ids = None
        func = lambda market_data: market_data.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""# df=
index=[2022-04-21 09:43:49-04:00, 2022-04-21 09:43:49-04:00]
columns=asset_id,close,high,low,open,start_timestamp,volume
shape=(1, 7)
                             asset_id  close  high   low  open           start_timestamp  volume
end_timestamp
2022-04-21 09:43:49-04:00  1464553467   65.0  45.0  55.0  35.0 2022-04-21 09:42:49-04:00    75.0"""
        # pylint: enable=line-too-long
        _check_get_data(self, client, func, expected_df_as_str)
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
                [0, 1650455029000, 30, 40, 50, 60, 70, 80, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [1, 1650469429000, 31, 41, 51, 61, 71, 72, "BTC_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [2, 1650483829000, 32, 42, 52, 62, 72, 73, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [3, 1650530629000, 34, 44, 54, 64, 74, 74, "BTC_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [4, 1650548629000, 35, 45, 55, 65, 75, 75, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [5, 1650613429000, 36, 46, 56, 66, 76, 76, "BTC_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")]
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

