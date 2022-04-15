import logging
from typing import Optional

import pandas as pd
import helpers.hasyncio as hasynci
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.common.db.db_utils as imvcddbut
import market_data.market_data_example as mdmadaex
import market_data.real_time_market_data as mdrtmda

_LOG = logging.getLogger(__name__)


class TestRealTimeMarketData2(icdctictc.ImClientTestCase, imvcddbut.TestImDbHelper):
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
        
    def setup_real_time_market_data2(
        self,
        resample_1min: Optional[bool] = True,
    ) -> imvtdctacl.RealTimeSqlTalosClient:
        """
        Setup RealTimeMarketData2 interface.
        """
        table_name = "talos_ohlcv"
        sql_talos_client = imvtdctacl.RealTimeSqlTalosClient(
            resample_1min, self.connection, table_name
        )
        real_time_market_data = mdrtmda.RealTimeMarketData2(sql_talos_client)
        return real_time_market_data

    def test_get_data1(self) -> None:
        pass
