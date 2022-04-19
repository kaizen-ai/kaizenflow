import logging
from typing import Optional, List

import pandas as pd

import helpers.hsql as hsql

import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.common.db.db_utils as imvcddbut
import market_data.real_time_market_data as mdrtmda
import market_data.market_data_example as mdmadaex
import market_data.test.market_data_test_case as mdtmdtca

import im_v2.talos.db.utils as imvtadbut


_LOG = logging.getLogger(__name__)


class TestRealTimeMarketData2(imvcddbut.TestImDbHelper, mdtmdtca.MarketData_get_data_TestCase):
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

    def check_last_end_time(
        self,
    ) -> None:
        """
        Check output of `_get_last_end_time()` and `is_online()`.
        """
        #
        sql_talos_client = self.setup_sql_talos_client()
        market_data, _ =  mdmadaex.get_RealTimeMarketData2(sql_talos_client)
        last_end_time = market_data.get_last_end_time()

        _LOG.info("-> last_end_time=%s", last_end_time)
        expected_last_end_time = ""
        self.assertEqual(last_end_time, expected_last_end_time)
        #
        is_online = market_data.is_online()
        expected_is_online = True
        _LOG.info("-> is_online=%s", is_online)
        self.assertEqual(is_online, expected_is_online)
        
    def test_is_online1(self) -> None:
        # Prepare inputs.
        sql_talos_client = self.setup_sql_talos_client()
        market_data, _ = mdmadaex.get_RealTimeMarketData2(sql_talos_client)
        # Run.
        actual = market_data.is_online()
        self.assertTrue(actual)

    def test_get_data_for_last_period1(self) -> None:
        # Prepare inputs.
        asset_ids = [1467591036]
        columns: List[str] = []
        sql_talos_client = self.setup_sql_talos_client()
        market_data, _ = mdmadaex.get_RealTimeMarketData2(
            sql_talos_client, asset_ids=asset_ids, columns=columns
        )
        timedelta = pd.Timedelta("1D")
        # Run.
        self._test_get_data_for_last_period(market_data, timedelta)

    def test_get_data_at_timestamp1(self) -> None:
        # Prepare inputs.
        asset_ids = [3303714233, 1467591036]
        columns: List[str] = []
        market_data = mdmadaex.get_RealTimeMarketData2(
            asset_ids, columns
        )
        ts = pd.Timestamp("2000-01-01T09:35:00-05:00")
        #
        expected_length = 2
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        exp_df_as_str = r"""
        # df=
        index=[2000-01-01 09:35:00-05:00, 2000-01-01 09:35:00-05:00]
        columns=asset_id,full_symbol,open,high,low,close,volume,feature1,start_ts
        shape=(2, 9)
                                     asset_id        full_symbol  open  high  low  close  volume  feature1                  start_ts
        end_ts
        2000-01-01 09:35:00-05:00  1467591036  binance::BTC_USDT   100   101   99  101.0       4       1.0 2000-01-01 09:34:00-05:00
        2000-01-01 09:35:00-05:00  3303714233  binance::ADA_USDT   100   101   99  101.0       4       1.0 2000-01-01 09:34:00-05:00
        """
        # pylint: enable=line-too-long
        # Run.
        self._test_get_data_at_timestamp1(
            market_data,
            ts,
            asset_ids,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            exp_df_as_str,
        )


    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        """
        Create a test Talos OHLCV dataframe.
        """
        columns = [
            "asset_id",
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "feature1",
            "start_ts",
        ]
        index = [pd.Timestamp("2000-01-01T09:36:00-05:00"),  pd.Timestamp("2000-01-01T09:36:00-05:00")]
        data = [[1467591036,  "binance::BTC_USDT", 100, 101, 99, 100.0, 5, -1.0, pd.Timestamp("2000-01-01T09:35:00-05:00")],
                [3303714233,  "binance::ADA_USDT", 100, 101, 99, 100.0, 6, -1.0, pd.Timestamp("2000-01-01T09:36:00-05:00")]
        ]
        test_data = pd.DataFrame(data, index=index, columns=columns)
        test_data.index.name = "end_ts"
        return test_data

    def _create_test_table(self) -> None:
        """
        Create a test Talos OHLCV table in DB.
        """
        query = imvtadbut.get_talos_ohlcv_create_table_query()
        self.connection.cursor().execute(query)

