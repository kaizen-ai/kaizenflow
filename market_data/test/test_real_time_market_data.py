import logging
from typing import Optional, List

import pandas as pd
import helpers.hdatetime as hdateti
import helpers.hsql as hsql
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.common.db.db_utils as imvcddbut
import market_data.real_time_market_data as mdrtmda
import im_v2.talos.db.utils as imvtadbut


_LOG = logging.getLogger(__name__)


class TestRealTimeMarketData2(icdctictc.ImClientTestCase, imvcddbut.TestImDbHelper):

    def setup_sql_talos_client(
        self,
        resample_1min: Optional[bool] = True,
    ) -> mdrtmda.RealTimeMarketData2:
        """
        Setup RealTimeMarketData2 interface.
        """
        self._create_test_table()
        test_data = self._get_test_data()
        table_name = "talos_ohlcv"
        mode = "market_data"
        hsql.copy_rows_with_copy_from(self.connection, test_data, table_name)
        sql_talos_client = imvtdctacl.RealTimeSqlTalosClient(
            resample_1min, self.connection, table_name, mode
        )
        return sql_talos_client

    def test_check_last_end_time(
        self,
    ) -> None:
        """
        Check output of `_get_last_end_time()` and `is_online()`.
        """
        #
        sql_talos_client = self.setup_sql_talos_client()

        get_wall_clock_time = hdateti.get_current_time(tz="UTC")
        #asset_ids = [3187272957, 1467591036]
        columns: List[str] = []
        asset_ids = None
        asset_id_col = "asset_id"
        start_time_col_name = "start_ts"
        end_time_col_name = "end_ts"
        market_data = mdrtmda.RealTimeMarketData2(sql_talos_client, get_wall_clock_time=get_wall_clock_time,
        columns=columns, asset_ids=asset_ids, asset_id_col=asset_id_col,
        start_time_col_name=start_time_col_name, end_time_col_name=end_time_col_name)
        #
        last_end_time = market_data.get_last_end_time()
        _LOG.info("-> last_end_time=%s", last_end_time)
        expected_last_end_time = ""
        self.assertEqual(last_end_time, expected_last_end_time)
        #
        is_online = market_data.is_online()
        expected_is_online = True
        _LOG.info("-> is_online=%s", is_online)
        self.assertEqual(is_online, expected_is_online)
        

    def test_get_data1(self) -> None:
        pass

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
                [0, 1648138860000, 30, 40, 50, 60, 70, 80, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [1, 1648138860000, 31, 41, 51, 61, 71, 72, "BTC_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [2, 1648138920000, 32, 42, 52, 62, 72, 73, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [3, 1648138920000, 34, 44, 54, 64, 74, 74, "BTC_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [4, 1648138980000, 35, 45, 55, 65, 75, 75, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
                 pd.Timestamp("2022-03-26")],
                [5, 1648138980000, 36, 46, 56, 66, 76, 76, "BTC_USDT", "binance", pd.Timestamp("2022-03-26"),
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

