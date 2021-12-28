import os
from typing import Any, List

import pandas as pd
import pytest

import helpers.git as hgit
import helpers.printing as hprint
import helpers.sql as hsql
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.utils as imvcodbut

_LOCAL_ROOT_DIR = os.path.join(
    hgit.get_client_root(False),
    "im_v2/ccxt/data/client/test/test_data",
)


def _check_output(
    self_: Any,
    actual_df: pd.DataFrame,
    expected_length: int,
    expected_exchange_ids: List[str],
    expected_currency_pairs: List[str],
    expected_df_as_str: str,
) -> None:
    """
    Verify that actual outcome dataframe matches the expected one.

    :param actual_df: actual outcome dataframe
    :param expected_length: expected outcome dataframe length
    :param expected_exchange_ids: list of expected exchange ids
    :param expected_currency_pairs: list of expected currency pairs
    :param expected_df_as_str: expected outcome as string
    """
    # Check output df length.
    self_.assert_equal(str(expected_length), str(actual_df.shape[0]))
    # Check unique exchange ids in the output df.
    actual_exchange_ids = sorted(
        list(actual_df["exchange_id"].dropna().unique())
    )
    self_.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
    # Check unique currency pairs in the output df.
    actual_currency_pairs = sorted(
        list(actual_df["currency_pair"].dropna().unique())
    )
    self_.assert_equal(
        str(actual_currency_pairs), str(expected_currency_pairs)
    )
    actual_df = actual_df[sorted(actual_df.columns)]
    actual_df_as_str = hprint.df_to_short_str("df", actual_df)
    print(actual_df_as_str)
    self_.assert_equal(
        actual_df_as_str,
        expected_df_as_str,
        dedent=True,
        fuzzy_match=True,
    )


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "BTC_USDT"
        ccxt_loader = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        actual = ccxt_loader._get_file_path(
            imvcdclcl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        expected = os.path.join(
            _LOCAL_ROOT_DIR, "ccxt/20210924/binance/BTC_USDT.csv.gz"
        )
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "BTC_USDT"
        ccxt_loader = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        # TODO(gp): We should throw a different exception, like
        # `UnsupportedExchange`.
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imvcdclcl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        ccxt_loader = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imvcdclcl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


# #############################################################################


class TestCcxtDbClient(imvcodbut.TestImDbHelper):
    @pytest.mark.slow("needs to pull `postgres` image")
    def test_read_data1(self) -> None:
        """
        Verify that data from DB is read correctly.
        """
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Load data with client and check if it is correct.
        ccxt_db_client = imvcdclcl.CcxtDbClient("ohlcv", self.connection)
        actual = ccxt_db_client.read_data(
            ["binance::BTC_USDT", "binance::ETH_USDT"]
        )
        # Check the output values.
        expected_length = 8
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df= 
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        df.columns=close,currency_pair,epoch,exchange_id,full_symbol,high,low,open,volume
        df.shape=(8, 9)
                                   close currency_pair         epoch exchange_id        full_symbol  high   low  open  volume
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT  1.631146e+12     binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT  1.631146e+12     binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00    NaN           NaN           NaN         NaN  binance::BTC_USDT   NaN   NaN   NaN     NaN
        ...
        2021-09-09 00:02:00+00:00   62.0      ETH_USDT  1.631146e+12     binance  binance::ETH_USDT  42.0  52.0  32.0    72.0
        2021-09-09 00:03:00+00:00    NaN           NaN           NaN         NaN  binance::ETH_USDT   NaN   NaN   NaN     NaN
        2021-09-09 00:04:00+00:00   64.0      ETH_USDT  1.631146e+12     binance  binance::ETH_USDT  44.0  54.0  34.0    74.0
        """
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_df_as_str,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    @pytest.mark.slow("needs to pull `postgres` image")
    def test_read_data2(self) -> None:
        """
        Verify that data from DB is read and filtered correctly.
        """
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Load data with client and check if it is correct.
        ccxt_db_client = imvcdclcl.CcxtDbClient("ohlcv", self.connection)
        actual = ccxt_db_client.read_data(
            ["binance::BTC_USDT"],
            start_ts=pd.Timestamp("2021-09-09T00:00:00-00:00"),
            end_ts=pd.Timestamp("2021-09-09T00:01:00-00:00"),
        )
        # Check the output values.
        expected_length = 1
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df= 
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:00:00+00:00]                                                                                                                                                                          
        df.columns=close,currency_pair,epoch,exchange_id,full_symbol,high,low,open,volume                                                                                                                                                           
        df.shape=(1, 9)                                                                                                                                                                                                                             
                                   close currency_pair          epoch exchange_id        full_symbol  high   low  open  volume                                                                                                                      
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT  1631145600000     binance  binance::BTC_USDT  40.0  50.0  30.0    70.0    
        """
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_df_as_str,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def _create_test_table(self) -> None:
        """
        Create a test CCXT OHLCV table in DB.
        """
        query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        self.connection.cursor().execute(query)

    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        """
        Create a test CCXT OHLCV dataframe.
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
                "currency_pair",
                "exchange_id",
                "created_at",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "BTC_USDT", "binance", pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "BTC_USDT", "binance", pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "ETH_USDT", "binance", pd.Timestamp("2021-09-09")],
                [4, 1631145840000, 34, 44, 54, 64, 74, "BTC_USDT", "binance", pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "ETH_USDT", "binance", pd.Timestamp("2021-09-09")],
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        return test_data


# #############################################################################


# TODO(*): Consider to factor out the class calling in a `def _get_loader()`.
class TestCcxtCsvFileSystemClientReadData(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that ".csv.gz" files from filesystem are being read correctly.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        # Check actual results.
        actual = ccxt_file_client.read_data(full_symbols)
        print(actual.head(3))
        expected_length = 199
        expected_exchange_ids = ["binance", "kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df= 
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        df.columns=close,currency_pair,epoch,exchange_id,full_symbol,high,low,open,volume
        df.shape=(8, 9)
                                   close currency_pair         epoch exchange_id        full_symbol  high   low  open  volume
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT  1.631146e+12     binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT  1.631146e+12     binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00    NaN           NaN           NaN         NaN  binance::BTC_USDT   NaN   NaN   NaN     NaN
        ...
        2021-09-09 00:02:00+00:00   62.0      ETH_USDT  1.631146e+12     binance  binance::ETH_USDT  42.0  52.0  32.0    72.0
        2021-09-09 00:03:00+00:00    NaN           NaN           NaN         NaN  binance::ETH_USDT   NaN   NaN   NaN     NaN
        2021-09-09 00:04:00+00:00   64.0      ETH_USDT  1.631146e+12     binance  binance::ETH_USDT  44.0  54.0  34.0    74.0
        """
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_df_as_str,
        )

    def test2(self) -> None:
        """
        Test that files from filesystem are being filtered correctly.
        """
        ccxt_loader = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        actual = ccxt_loader.read_data(
            ["binance::BTC_USDT"],
            start_ts=pd.Timestamp("2018-08-17T00:01:00"),
            end_ts=pd.Timestamp("2018-08-17T00:05:00"),
        )
        # Check the output values.
        expected_length = 4
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def test3(self) -> None:
        """
        Test that files are being read correctly without normalization.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        # Check output.
        actual = ccxt_file_client.read_data(
            full_symbols,
            normalize=False,
        )
        expected_length = 174
        expected_exchange_ids = ["binance", "kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def test4(self) -> None:
        """
        Test that ".csv" files from filesystem are being read correctly.
        """
        ccxt_loader = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR, use_gzip=False
        )
        actual = ccxt_loader.read_data(["binance::BTC_USDT"])
        # Check the output values.
        expected_length = 100
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def test5(self) -> None:
        """
        Test that all files are being read correctly with dict output mode.
        """
        # Set input list of full symbols.
        full_symbols = ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        # Check actual results.
        actual_dict = ccxt_file_client.read_data(full_symbols, mode="dict")
        actual_dict_keys = sorted(list(actual_dict.keys()))
        actual_df1 = actual_dict[actual_dict_keys[0]]
        actual_df2 = actual_dict[actual_dict_keys[1]]
        self.assert_equal(str(actual_dict_keys), str(full_symbols))
        _check_output(
            self,
            actual=actual_df1,
            expected_length=100,
            expected_exchange_ids=["binance"],
            expected_currency_pairs=["BTC_USDT"],
            check_string=False,
        )
        _check_output(
            self,
            actual=actual_df2,
            expected_length=99,
            expected_exchange_ids=["kucoin"],
            expected_currency_pairs=["ETH_USDT"],
            check_string=False,
        )
        # Create combined actual string and check it.
        actual_string_df1 = hunitest.convert_df_to_json_string(actual_df1)
        actual_string_df2 = hunitest.convert_df_to_json_string(actual_df2)
        actual_string = "\n".join(
            [
                actual_dict_keys[0],
                actual_string_df1,
                "\n",
                actual_dict_keys[1],
                actual_string_df2,
            ]
        )
        self.check_string(actual_string)

    def test6(self) -> None:
        """
        Test unsupported full symbol.
        """
        ccxt_loader = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data(["unsupported_exchange::unsupported_currency"])

    def test7(self) -> None:
        """
        Test unsupported data type.
        """
        with self.assertRaises(AssertionError):
            imvcdclcl.CcxtCsvFileSystemClient(
                data_type="unsupported_data_type", root_dir=_LOCAL_ROOT_DIR
            )


# #############################################################################


class TestCcxtParquetFileSystemClientReadData(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that Parquet files from filesystem are being read correctly.
        """
        ccxt_loader = imvcdclcl.CcxtParquetFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        actual = ccxt_loader.read_data(["binance::BTC_USDT"])
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test that Parquet files from filesystem are being filtered correctly.
        """
        ccxt_loader = imvcdclcl.CcxtParquetFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        actual = ccxt_loader.read_data(
            ["binance::BTC_USDT"],
            start_ts=pd.Timestamp("2018-08-17T00:01:00"),
            end_ts=pd.Timestamp("2018-08-17T00:05:00"),
        )
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)


# #############################################################################


class TestGetTimestamp(hunitest.TestCase):
    def test_get_start_ts(self) -> None:
        """
        Test that the earliest timestamp available is computed correctly.
        """
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        start_ts = ccxt_file_client.get_start_ts_available("binance::BTC_USDT")
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self.assertEqual(start_ts, expected_start_ts)

    def test_get_end_ts(self) -> None:
        """
        Test that the latest timestamp available is computed correctly.
        """
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        end_ts = ccxt_file_client.get_end_ts_available("binance::BTC_USDT")
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        # TODO(Grisha): use `assertGreater` when start downloading more data.
        self.assertEqual(end_ts, expected_end_ts)


# #############################################################################


class TestGetUniverse(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that CCXT universe is computed correctly.
        """
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        universe = ccxt_file_client.get_universe()
        # Check the length of the universe.
        self.assertEqual(len(universe), 38)
        # Check the first elements of the universe.
        first_elements = universe[:3]
        first_elements_expected = [
            "binance::ADA_USDT",
            "binance::AVAX_USDT",
            "binance::BNB_USDT",
        ]
        self.assertEqual(first_elements, first_elements_expected)
        # Check the last elements of the universe.
        last_elements = universe[-3:]
        last_elements_expected = [
            "kucoin::LINK_USDT",
            "kucoin::SOL_USDT",
            "kucoin::XRP_USDT",
        ]
        self.assertEqual(last_elements, last_elements_expected)
