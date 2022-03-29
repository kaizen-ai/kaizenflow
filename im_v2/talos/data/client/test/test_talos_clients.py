from typing import List, Optional

import pandas as pd

import helpers.hsql as hsql
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.common.db.db_utils as imvcddbut
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.talos.data.client.talos_clients_example as imvtdctcex
import im_v2.talos.db.utils as imvtadbut

# #############################################################################
# TestTalosParquetByTileClient1
# #############################################################################


class TestTalosParquetByTileClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    @staticmethod
    def get_expected_column_names() -> List[str]:
        """
        Return a list of expected column names.
        """
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        return expected_column_names

    def test_read_data1(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbol = "binance::ADA_USDT"
        #
        expected_length = 100
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["binance::ADA_USDT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:00:00+00:00, 2022-01-01 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(100, 6)
                                         full_symbol        open        high         low       close           volume
        timestamp
        2022-01-01 00:00:00+00:00  binance::ADA_USDT  1.30800000  1.31000000  1.30700000  1.31000000   98266.80000000
        2022-01-01 00:01:00+00:00  binance::ADA_USDT  1.31000000  1.31400000  1.30800000  1.31200000  132189.40000000
        2022-01-01 00:02:00+00:00  binance::ADA_USDT  1.31200000  1.31800000  1.31100000  1.31700000  708964.20000000
        ...
        2022-01-01 01:37:00+00:00  binance::ADA_USDT  1.33700000  1.33700000  1.33600000  1.33600000  39294.80000000
        2022-01-01 01:38:00+00:00  binance::ADA_USDT  1.33600000  1.33600000  1.33400000  1.33400000  22398.70000000
        2022-01-01 01:39:00+00:00  binance::ADA_USDT  1.33400000  1.33400000  1.33200000  1.33300000  69430.10000000
        """
        # pylint: enable=line-too-long
        self._test_read_data1(
            talos_client,
            full_symbol,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data2(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        #
        expected_length = 200
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:00:00+00:00, 2022-01-01 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(200, 6)
                                         full_symbol            open            high             low           close           volume
        timestamp
        2022-01-01 00:00:00+00:00  binance::ADA_USDT      1.30800000      1.31000000      1.30700000      1.31000000   98266.80000000
        2022-01-01 00:00:00+00:00  binance::BTC_USDT  46216.93000000  46271.08000000  46208.37000000  46250.00000000      40.57574000
        2022-01-01 00:01:00+00:00  binance::ADA_USDT      1.31000000      1.31400000      1.30800000      1.31200000  132189.40000000
        ...
        2022-01-01 01:38:00+00:00  binance::BTC_USDT  46840.94000000  46854.39000000  46784.38000000  46789.23000000     18.42650000
        2022-01-01 01:39:00+00:00  binance::ADA_USDT      1.33400000      1.33400000      1.33200000      1.33300000  69430.10000000
        2022-01-01 01:39:00+00:00  binance::BTC_USDT  46789.23000000  46811.33000000  46753.84000000  46799.90000000     12.48485000
        """
        # pylint: enable=line-too-long
        self._test_read_data2(
            talos_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data3(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2022-01-01T00:01:00-00:00")
        #
        expected_length = 198
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:01:00+00:00, 2022-01-01 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(198, 6)
                                         full_symbol            open            high             low           close           volume
        timestamp
        2022-01-01 00:01:00+00:00  binance::ADA_USDT      1.31000000      1.31400000      1.30800000      1.31200000  132189.40000000
        2022-01-01 00:01:00+00:00  binance::BTC_USDT  46250.01000000  46344.23000000  46234.39000000  46312.76000000      42.38106000
        2022-01-01 00:02:00+00:00  binance::ADA_USDT      1.31200000      1.31800000      1.31100000      1.31700000  708964.20000000
        ...
        2022-01-01 01:38:00+00:00  binance::BTC_USDT  46840.94000000  46854.39000000  46784.38000000  46789.23000000     18.42650000
        2022-01-01 01:39:00+00:00  binance::ADA_USDT      1.33400000      1.33400000      1.33200000      1.33300000  69430.10000000
        2022-01-01 01:39:00+00:00  binance::BTC_USDT  46789.23000000  46811.33000000  46753.84000000  46799.90000000     12.48485000
        """
        # pylint: enable=line-too-long
        self._test_read_data3(
            talos_client,
            full_symbols,
            start_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data4(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2022-01-01T00:05:00-00:00")
        #
        expected_length = 12
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:00:00+00:00, 2022-01-01 00:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(12, 6)
                                         full_symbol            open            high             low           close           volume
        timestamp
        2022-01-01 00:00:00+00:00  binance::ADA_USDT      1.30800000      1.31000000      1.30700000      1.31000000   98266.80000000
        2022-01-01 00:00:00+00:00  binance::BTC_USDT  46216.93000000  46271.08000000  46208.37000000  46250.00000000      40.57574000
        2022-01-01 00:01:00+00:00  binance::ADA_USDT      1.31000000      1.31400000      1.30800000      1.31200000  132189.40000000
        ...
        2022-01-01 00:04:00+00:00  binance::BTC_USDT             NaN             NaN             NaN             NaN             NaN
        2022-01-01 00:05:00+00:00  binance::ADA_USDT      1.31500000      1.31800000      1.31300000      1.31800000  75423.50000000
        2022-01-01 00:05:00+00:00  binance::BTC_USDT  46321.34000000  46443.56000000  46280.00000000  46436.03000000     35.86682000
        """
        # pylint: enable=line-too-long
        self._test_read_data4(
            talos_client,
            full_symbols,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data5(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2022-01-01T00:01:00-00:00")
        end_ts = pd.Timestamp("2022-01-01T00:05:00-00:00")
        #
        expected_length = 10
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:01:00+00:00, 2022-01-01 00:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(10, 6)
                                         full_symbol            open            high             low           close           volume
        timestamp
        2022-01-01 00:01:00+00:00  binance::ADA_USDT      1.31000000      1.31400000      1.30800000      1.31200000  132189.40000000
        2022-01-01 00:01:00+00:00  binance::BTC_USDT  46250.01000000  46344.23000000  46234.39000000  46312.76000000      42.38106000
        2022-01-01 00:02:00+00:00  binance::ADA_USDT      1.31200000      1.31800000      1.31100000      1.31700000  708964.20000000
        ...
        2022-01-01 00:04:00+00:00  binance::BTC_USDT             NaN             NaN             NaN             NaN             NaN
        2022-01-01 00:05:00+00:00  binance::ADA_USDT      1.31500000      1.31800000      1.31300000      1.31800000  75423.50000000
        2022-01-01 00:05:00+00:00  binance::BTC_USDT  46321.34000000  46443.56000000  46280.00000000  46436.03000000     35.86682000
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            talos_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data6(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(
            talos_client,
            full_symbol,
        )

    def test_read_data7(self) -> None:
        resample_1min = False
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        #
        expected_length = 196
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:00:00+00:00, 2022-01-01 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(196, 6)
                                         full_symbol            open            high             low           close           volume
        timestamp
        2022-01-01 00:00:00+00:00  binance::ADA_USDT      1.30800000      1.31000000      1.30700000      1.31000000   98266.80000000
        2022-01-01 00:00:00+00:00  binance::BTC_USDT  46216.93000000  46271.08000000  46208.37000000  46250.00000000      40.57574000
        2022-01-01 00:01:00+00:00  binance::ADA_USDT      1.31000000      1.31400000      1.30800000      1.31200000  132189.40000000
        ...
        2022-01-01 01:38:00+00:00  binance::BTC_USDT  46840.94000000  46854.39000000  46784.38000000  46789.23000000     18.42650000
        2022-01-01 01:39:00+00:00  binance::ADA_USDT      1.33400000      1.33400000      1.33200000      1.33300000  69430.10000000
        2022-01-01 01:39:00+00:00  binance::BTC_USDT  46789.23000000  46811.33000000  46753.84000000  46799.90000000     12.48485000
        """
        # pylint: enable=line-too-long
        self._test_read_data7(
            talos_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbol = "binance::ADA_USDT"
        expected_start_ts = pd.Timestamp("2022-01-01T00:00:00-00:00")
        self._test_get_start_ts_for_symbol1(
            talos_client,
            full_symbol,
            expected_start_ts,
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        resample_1min = True
        talos_client = imvtdctcex.get_TalosHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbol = "binance::ADA_USDT"
        expected_end_ts = pd.Timestamp("2022-01-01T01:39:00-00:00")
        self._test_get_end_ts_for_symbol1(
            talos_client,
            full_symbol,
            expected_end_ts,
        )


# #############################################################################
# RealTimeSqlTalosClient
# #############################################################################


class TestRealTimeSqlTalosClient1(
    icdctictc.ImClientTestCase, imvcddbut.TestImDbHelper
):
    """
    
    """

    def test_build_select_query1(self) -> None:
        """
        `start_unix_epoch` is not int type.
        """
        talos_sql_client = self.setup_talos_sql_client()
        exchange_id = ["binance"]
        currency_pair = ["AVAX_USDT"]
        start_unix_epoch = "unsupported_type"
        end_unix_epoch = 1647471180000
        with self.assertRaises(AssertionError):
            talos_sql_client._build_select_query(
                exchange_id, currency_pair, start_unix_epoch, end_unix_epoch
            )

    def test_build_select_query2(self) -> None:
        """
        `exchange_ids` is not a list of strings.
        """
        talos_sql_client = self.setup_talos_sql_client()
        exchange_id = "unsupported_type"
        currency_pair = ["AVAX_USDT"]
        start_unix_epoch = 1647470940000
        end_unix_epoch = 1647471180000
        with self.assertRaises(AssertionError):
            talos_sql_client._build_select_query(
                exchange_id, currency_pair, start_unix_epoch, end_unix_epoch
            )

    def test_build_select_query3(self) -> None:
        """
        Start unix epoch is larger than end.
        """
        talos_sql_client = self.setup_talos_sql_client()
        exchange_id = ["binance"]
        currency_pair = ["AVAX_USDT"]
        start_unix_epoch = 1647471200000
        end_unix_epoch = 1647471180000
        with self.assertRaises(AssertionError):
            talos_sql_client._build_select_query(
                exchange_id, currency_pair, start_unix_epoch, end_unix_epoch
            )

    def test_build_select_query4(self) -> None:
        """
        Test SQL query string with every param provided.
        """
        talos_sql_client = self.setup_talos_sql_client()
        exchange_id = ["binance"]
        currency_pair = ["BTC_USDT"]
        start_unix_epoch = 1647470940000
        end_unix_epoch = 1647471180000
        actual_outcome = talos_sql_client._build_select_query(
            exchange_id, currency_pair, start_unix_epoch, end_unix_epoch
        )
        expected_outcome = (
            "SELECT * FROM talos_ohlcv WHERE timestamp >= 1647470940000 AND timestamp <= "
            "1647471180000 AND exchange_id IN ('binance') AND currency_pair IN ('BTC_USDT')"
        )
        # Message in case if test case got failed.
        message = "Actual and expected SQL queries are not equal!"
        self.assertEqual(actual_outcome, expected_outcome, message)

    def test_build_select_query5(self) -> None:
        """
        Test SQL query string with `None` timestamps.
        """
        talos_sql_client = self.setup_talos_sql_client()
        exchange_id = ["binance"]
        currency_pair = ["BTC_USDT"]
        start_unix_epoch = None
        end_unix_epoch = None
        actual_outcome = talos_sql_client._build_select_query(
            exchange_id, currency_pair, start_unix_epoch, end_unix_epoch
        )
        expected_outcome = "SELECT * FROM talos_ohlcv WHERE exchange_id IN ('binance') AND currency_pair IN ('BTC_USDT')"
        # Message in case if test case got failed.
        message = "Actual and expected SQL queries are not equal!"
        self.assertEqual(actual_outcome, expected_outcome, message)

    def test_build_select_query6(self) -> None:
        """
        Test SQL query string with only timestamps provided.
        """
        talos_sql_client = self.setup_talos_sql_client()
        exchange_id = []
        currency_pair = []
        start_unix_epoch = 1647470940000
        end_unix_epoch = 1647471180000
        actual_outcome = talos_sql_client._build_select_query(
            exchange_id, currency_pair, start_unix_epoch, end_unix_epoch
        )
        expected_outcome = (
            "SELECT * FROM talos_ohlcv WHERE timestamp >= 1647470940000 AND timestamp <= 1647471180000 AND "
            "exchange_id IN () AND currency_pair IN ()"
        )
        # Message in case if test case got failed.
        message = "Actual and expected SQL queries are not equal!"
        self.assertEqual(actual_outcome, expected_outcome, message)

    def setup_talos_sql_client(
        self,
        resample_1min: Optional[bool] = True,
    ) -> imvtdctacl.RealTimeSqlTalosClient:
        """
        Initialize Talos SQL Client.
        """
        table_name = "talos_ohlcv"
        sql_talos_client = imvtdctacl.RealTimeSqlTalosClient(
            resample_1min, self.connection, table_name
        )
        return sql_talos_client

    def test_read_data1(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        #
        im_client = self.setup_talos_sql_client()
        full_symbol = "binance::ETH_USDT"
        #
        expected_length = 3
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["binance::ETH_USDT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-03-24 16:21:00+00:00, 2022-03-24 16:23:00+00:00]
        columns=open,high,low,close,volume,full_symbol
        shape=(3, 6)
                                   open  high   low  close  volume        full_symbol
        timestamp
        2022-03-24 16:21:00+00:00  30.0  40.0  50.0   60.0    70.0  binance::ETH_USDT
        2022-03-24 16:22:00+00:00  32.0  42.0  52.0   62.0    72.0  binance::ETH_USDT
        2022-03-24 16:23:00+00:00  35.0  45.0  55.0   65.0    75.0  binance::ETH_USDT
        """
        # pylint: enable=line-too-long
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_read_data2(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        #
        im_client = self.setup_talos_sql_client()
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        #
        expected_length = 6
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-03-24 16:21:00+00:00, 2022-03-24 16:23:00+00:00]
        columns=open,high,low,close,volume,full_symbol
        shape=(6, 6)
                                   open  high   low  close  volume        full_symbol
        timestamp
        2022-03-24 16:21:00+00:00  31.0  41.0  51.0   61.0    71.0  binance::BTC_USDT
        2022-03-24 16:21:00+00:00  30.0  40.0  50.0   60.0    70.0  binance::ETH_USDT
        2022-03-24 16:22:00+00:00  34.0  44.0  54.0   64.0    74.0  binance::BTC_USDT
        2022-03-24 16:22:00+00:00  32.0  42.0  52.0   62.0    72.0  binance::ETH_USDT
        2022-03-24 16:23:00+00:00  36.0  46.0  56.0   66.0    76.0  binance::BTC_USDT
        2022-03-24 16:23:00+00:00  35.0  45.0  55.0   65.0    75.0  binance::ETH_USDT
        """
        # pylint: enable=line-too-long
        self._test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_read_data3(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        #
        im_client = self.setup_talos_sql_client()
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = pd.Timestamp("2022-03-24T16:21:00-00:00")
        #
        expected_length = 6
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-03-24 16:21:00+00:00, 2022-03-24 16:23:00+00:00]
        columns=open,high,low,close,volume,full_symbol
        shape=(6, 6)
                                   open  high   low  close  volume        full_symbol
        timestamp
        2022-03-24 16:21:00+00:00  31.0  41.0  51.0   61.0    71.0  binance::BTC_USDT
        2022-03-24 16:21:00+00:00  30.0  40.0  50.0   60.0    70.0  binance::ETH_USDT
        2022-03-24 16:22:00+00:00  34.0  44.0  54.0   64.0    74.0  binance::BTC_USDT
        2022-03-24 16:22:00+00:00  32.0  42.0  52.0   62.0    72.0  binance::ETH_USDT
        2022-03-24 16:23:00+00:00  36.0  46.0  56.0   66.0    76.0  binance::BTC_USDT
        2022-03-24 16:23:00+00:00  35.0  45.0  55.0   65.0    75.0  binance::ETH_USDT
        """
        # pylint: enable=line-too-long
        self._test_read_data3(
            im_client,
            full_symbols,
            start_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_read_data4(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        #
        im_client = self.setup_talos_sql_client()
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        end_ts = pd.Timestamp("2022-03-24T16:24:00-00:00")
        #
        expected_length = 6
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-03-24 16:21:00+00:00, 2022-03-24 16:23:00+00:00]
        columns=open,high,low,close,volume,full_symbol
        shape=(6, 6)
                                   open  high   low  close  volume        full_symbol
        timestamp
        2022-03-24 16:21:00+00:00  31.0  41.0  51.0   61.0    71.0  binance::BTC_USDT
        2022-03-24 16:21:00+00:00  30.0  40.0  50.0   60.0    70.0  binance::ETH_USDT
        2022-03-24 16:22:00+00:00  34.0  44.0  54.0   64.0    74.0  binance::BTC_USDT
        2022-03-24 16:22:00+00:00  32.0  42.0  52.0   62.0    72.0  binance::ETH_USDT
        2022-03-24 16:23:00+00:00  36.0  46.0  56.0   66.0    76.0  binance::BTC_USDT
        2022-03-24 16:23:00+00:00  35.0  45.0  55.0   65.0    75.0  binance::ETH_USDT
        """
        # pylint: enable=line-too-long
        self._test_read_data4(
            im_client,
            full_symbols,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    def test_read_data5(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        #
        im_client = self.setup_talos_sql_client()
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = pd.Timestamp("2022-03-24T16:21:00-00:00")
        end_ts = pd.Timestamp("2022-03-24T16:24:00-00:00")
        #
        expected_length = 6
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-03-24 16:21:00+00:00, 2022-03-24 16:23:00+00:00]
        columns=open,high,low,close,volume,full_symbol
        shape=(6, 6)
                                   open  high   low  close  volume        full_symbol
        timestamp
        2022-03-24 16:21:00+00:00  31.0  41.0  51.0   61.0    71.0  binance::BTC_USDT
        2022-03-24 16:21:00+00:00  30.0  40.0  50.0   60.0    70.0  binance::ETH_USDT
        2022-03-24 16:22:00+00:00  34.0  44.0  54.0   64.0    74.0  binance::BTC_USDT
        2022-03-24 16:22:00+00:00  32.0  42.0  52.0   62.0    72.0  binance::ETH_USDT
        2022-03-24 16:23:00+00:00  36.0  46.0  56.0   66.0    76.0  binance::BTC_USDT
        2022-03-24 16:23:00+00:00  35.0  45.0  55.0   65.0    75.0  binance::ETH_USDT
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    # TODO(Max) - add `read_data6`, see CMTask1545: `read_data6` problem in testing Talos Client.
    def test_read_data7(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "talos_ohlcv")
        #
        im_client = self.setup_talos_sql_client(False)
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        #
        expected_length = 6
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-03-24 16:21:00+00:00, 2022-03-24 16:23:00+00:00]
        columns=open,high,low,close,volume,full_symbol
        shape=(6, 6)
                                   open  high   low  close  volume        full_symbol
        timestamp
        2022-03-24 16:21:00+00:00  31.0  41.0  51.0   61.0    71.0  binance::BTC_USDT
        2022-03-24 16:21:00+00:00  30.0  40.0  50.0   60.0    70.0  binance::ETH_USDT
        2022-03-24 16:22:00+00:00  34.0  44.0  54.0   64.0    74.0  binance::BTC_USDT
        2022-03-24 16:22:00+00:00  32.0  42.0  52.0   62.0    72.0  binance::ETH_USDT
        2022-03-24 16:23:00+00:00  36.0  46.0  56.0   66.0    76.0  binance::BTC_USDT
        2022-03-24 16:23:00+00:00  35.0  45.0  55.0   65.0    75.0  binance::ETH_USDT
        """
        # pylint: enable=line-too-long
        self._test_read_data7(
            im_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "talos_ohlcv")

    # ///////////////////////////////////////////////////////////////////////

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

    @staticmethod
    def _get_expected_column_names() -> List[str]:
        """
        Return a list of expected column names.
        """
        expected_column_names = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "full_symbol",
        ]
        return expected_column_names

    def _create_test_table(self) -> None:
        """
        Create a test Talos OHLCV table in DB.
        """
        query = imvtadbut.get_talos_ohlcv_create_table_query()
        self.connection.cursor().execute(query)
