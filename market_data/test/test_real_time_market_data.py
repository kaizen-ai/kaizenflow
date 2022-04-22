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
    func: Callable,
    expected_df_as_str: str,
    sql_talos_client: mdrtmda.RealTimeMarketData2,
) -> mdrtmda.RealTimeMarketData2:
    """
    - Build `RealTimeMarketData2`
    - Execute the function `get_data*` in `func`
    - Check actual output against expected.
    """
    asset_ids = []
    columns: List[str] = []
    market_data, _ = mdmadaex.get_RealTimeMarketData2(
        sql_talos_client, asset_ids=asset_ids, columns=columns
    )
    # Execute function under test.
    actual_df = func(market_data)
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
    mdtmdtca.MarketData_get_data_TestCase):
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
        market_data: mdrtmda.RealTimeMarketData2,
        expected_last_end_time: pd.Timestamp,
        expected_is_online: bool,
    ) -> None:
        """
        Check output of `get_last_end_time()` and `is_online()`.
        """
        #
        last_end_time = market_data.get_last_end_time()
        _LOG.info("-> last_end_time=%s", last_end_time)
        self.assertEqual(last_end_time, expected_last_end_time)
        #
        is_online = market_data.is_online()
        _LOG.info("-> is_online=%s", is_online)
        self.assertEqual(is_online, expected_is_online)
        

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
                [3450, 1648138860000, 30, 40, 50, 60, 70, 80, "ETH_USDT", "binance", pd.Timestamp("2022-03-26"),
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

