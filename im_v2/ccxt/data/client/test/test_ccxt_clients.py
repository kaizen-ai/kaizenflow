import os
from typing import Any, List

import pandas as pd

import helpers.hprint as hprint
import helpers.hsql as hsql
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut


def _check_output(
    self_: Any,
    actual_df: pd.DataFrame,
    expected_length: int,
    expected_exchange_ids: List[str],
    expected_currency_pairs: List[str],
    expected_signature: str,
) -> None:
    """
    Verify that actual outcome dataframe matches the expected one.

    :param actual_df: actual outcome dataframe
    :param expected_length: expected outcome dataframe length
    :param expected_exchange_ids: list of expected exchange ids
    :param expected_currency_pairs: list of expected currency pairs
    :param expected_signature: expected outcome as string
    """
    # Build signature.
    act = []
    #
    actual_df = actual_df[sorted(actual_df.columns)]
    act.append(hprint.df_to_short_str("df", actual_df))
    #
    actual_exchange_ids = sorted(list(actual_df["exchange_id"].dropna().unique()))
    act.append("exchange_ids=%s" % ",".join(actual_exchange_ids))
    #
    actual_currency_pairs = sorted(
        list(actual_df["currency_pair"].dropna().unique())
    )
    act.append("currency_pairs=%s" % ",".join(actual_currency_pairs))
    actual_signature = "\n".join(act)
    # Check.
    self_.assert_equal(
        actual_signature,
        expected_signature,
        dedent=True,
        fuzzy_match=True,
    )
    # Check output df length.
    self_.assert_equal(str(expected_length), str(actual_df.shape[0]))
    # Check unique exchange ids in the output df.
    self_.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
    # Check unique currency pairs in the output df.
    self_.assert_equal(str(actual_currency_pairs), str(expected_currency_pairs))


# #############################################################################
# TestCcxtDbClient
# #############################################################################


class TestCcxtDbClient(imvcddbut.TestImDbHelper):
    def test_read_data1(self) -> None:
        """
        Verify that data from DB is read correctly.
        """
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Load data with client and check if it is correct.
        ccxt_db_client = imvcdccccl.CcxtDbClient("ohlcv", self.connection)
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = None
        end_ts = None
        actual = ccxt_db_client.read_data(full_symbols, start_ts, end_ts)
        # Check the output values.
        expected_length = 8
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(8, 8)
                                   close currency_pair  exchange_id    full_symbol  high   low  open  volume
        timestamp
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT  binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT  binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00   62.0      ETH_USDT  binance  binance::ETH_USDT  42.0  52.0  32.0    72.0
        ...
        2021-09-09 00:03:00+00:00    NaN           NaN      NaN                NaN   NaN   NaN   NaN     NaN
        2021-09-09 00:04:00+00:00   64.0      BTC_USDT  binance  binance::BTC_USDT  44.0  54.0  34.0    74.0
        2021-09-09 00:04:00+00:00   64.0      ETH_USDT  binance  binance::ETH_USDT  44.0  54.0  34.0    74.0
        exchange_ids=binance
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )
        # Delete the table.
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_read_data2(self) -> None:
        """
        Verify that data from DB is read and filtered correctly.
        """
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Load data with client and check if it is correct.
        ccxt_db_client = imvcdccccl.CcxtDbClient("ohlcv", self.connection)
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2021-09-09T00:00:00-00:00")
        end_ts = pd.Timestamp("2021-09-09T00:01:00-00:00")
        actual = ccxt_db_client.read_data(full_symbols, start_ts, end_ts)
        # Check the output values.
        expected_length = 1
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_signature = """
        # df=
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:00:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(1, 8)
                                   close currency_pair  exchange_id    full_symbol  high   low  open  volume
        timestamp
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT  binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        exchange_ids=binance
        currency_pairs=BTC_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
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
# TestCcxtCsvFileSystemClient
# #############################################################################


class TestCcxtCsvFileSystemClient(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test correctness of reading:

        - OHLCV data
        - for 1 currencies
        - from a ".csv" file on the local filesystem
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        # Run.
        full_symbols = ["binance::BTC_USDT"]
        start_ts = None
        end_ts = None
        actual = ccxt_client.read_data(full_symbols, start_ts, end_ts)
        # Check the output values.
        expected_length = 100
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        df.index in [2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(100, 8)
                                     close currency_pair  exchange_id    full_symbol     high      low     open     volume
        timestamp
        2018-08-17 00:00:00+00:00  6311.64      BTC_USDT  binance  binance::BTC_USDT  6319.04  6310.32  6316.00   9.967395
        2018-08-17 00:01:00+00:00  6302.81      BTC_USDT  binance  binance::BTC_USDT  6311.77  6302.81  6311.64  16.781206
        2018-08-17 00:02:00+00:00  6297.26      BTC_USDT  binance  binance::BTC_USDT  6306.00  6292.79  6302.81  55.373226
        ...
        2018-08-17 01:37:00+00:00  6343.14      BTC_USDT  binance  binance::BTC_USDT  6347.00  6343.00  6346.96  10.787817
        2018-08-17 01:38:00+00:00  6339.25      BTC_USDT  binance  binance::BTC_USDT  6345.98  6335.04  6345.98  38.197244
        2018-08-17 01:39:00+00:00  6342.95      BTC_USDT  binance  binance::BTC_USDT  6348.91  6339.00  6339.25  16.394692
        exchange_ids=binance
        currency_pairs=BTC_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test2(self) -> None:
        """
        Test correctness of reading:

        - OHLCV data
        - for 2 currencies
        - from a ".csv.gz" on the local filesystem
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        # Run.
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = None
        end_ts = None
        actual = ccxt_client.read_data(full_symbols, start_ts, end_ts)
        # Check.
        expected_length = 199
        expected_exchange_ids = ["binance", "kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        df.index in [2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(199, 8)
                                         close currency_pair  exchange_id    full_symbol         high          low         open     volume
        timestamp
        2018-08-17 00:00:00+00:00  6311.640000      BTC_USDT  binance  binance::BTC_USDT  6319.040000  6310.320000  6316.000000   9.967395
        2018-08-17 00:01:00+00:00  6302.810000      BTC_USDT  binance  binance::BTC_USDT  6311.770000  6302.810000  6311.640000  16.781206
        2018-08-17 00:01:00+00:00   286.712987      ETH_USDT   kucoin   kucoin::ETH_USDT   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 01:38:00+00:00   293.007409      ETH_USDT   kucoin   kucoin::ETH_USDT   293.007409   292.158945   292.158945   0.001164
        2018-08-17 01:39:00+00:00  6342.950000      BTC_USDT  binance  binance::BTC_USDT  6348.910000  6339.000000  6339.250000  16.394692
        2018-08-17 01:39:00+00:00   292.158946      ETH_USDT   kucoin   kucoin::ETH_USDT   292.158946   292.158945   292.158945   0.235161
        exchange_ids=binance,kucoin
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test3(self) -> None:
        """
        Test that files from filesystem are being filtered correctly.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        # Run.
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00-00:00")
        actual = ccxt_client.read_data(full_symbols, start_ts, end_ts)
        # Check the output values.
        expected_length = 4
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(4, 8)
                                     close currency_pair  exchange_id    full_symbol     high      low     open     volume
        timestamp
        2018-08-17 00:01:00+00:00  6302.81      BTC_USDT  binance  binance::BTC_USDT  6311.77  6302.81  6311.64  16.781206
        2018-08-17 00:02:00+00:00  6297.26      BTC_USDT  binance  binance::BTC_USDT  6306.00  6292.79  6302.81  55.373226
        2018-08-17 00:03:00+00:00  6294.52      BTC_USDT  binance  binance::BTC_USDT  6299.97  6286.93  6299.97  34.611797
        ...
        2018-08-17 00:02:00+00:00  6297.26      BTC_USDT  binance  binance::BTC_USDT  6306.00  6292.79  6302.81  55.373226
        2018-08-17 00:03:00+00:00  6294.52      BTC_USDT  binance  binance::BTC_USDT  6299.97  6286.93  6299.97  34.611797
        2018-08-17 00:04:00+00:00  6296.10      BTC_USDT  binance  binance::BTC_USDT  6299.98  6290.00  6294.52  22.088586
        exchange_ids=binance
        currency_pairs=BTC_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_invalid_full_symbol1(self) -> None:
        """
        Test unsupported full symbol.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        #
        full_symbols = ["unsupported_exchange::unsupported_currency"]
        start_ts = None
        end_ts = None
        with self.assertRaises(AssertionError):
            ccxt_client.read_data(full_symbols, start_ts, end_ts)

    def test_invalid_data_type1(self) -> None:
        """
        Test unsupported data type.
        """
        data_type = "unsupported_data_type"
        root_dir = imvcdcccex.get_test_data_dir()
        with self.assertRaises(AssertionError):
            imvcdccccl.CcxtCsvFileSystemClient(data_type, root_dir)


# #############################################################################


class TestGetTimestamp(hunitest.TestCase):
    def test_get_start_ts(self) -> None:
        """
        Test that the earliest timestamp available is computed correctly.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        #
        start_ts = ccxt_client.get_start_ts_for_symbol("binance::BTC_USDT")
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self.assertEqual(start_ts, expected_start_ts)

    def test_get_end_ts(self) -> None:
        """
        Test that the latest timestamp available is computed correctly.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        #
        end_ts = ccxt_client.get_end_ts_for_symbol("binance::BTC_USDT")
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        # TODO(Grisha): use `assertGreater` when start downloading more data.
        self.assertEqual(end_ts, expected_end_ts)


# #############################################################################


class TestGetUniverse(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that CCXT universe is computed correctly.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        #
        universe = ccxt_client.get_universe()
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


# #############################################################################


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        # Run.
        exchange_id = "binance"
        currency_pair = "BTC_USDT"
        actual = ccxt_client._get_file_path(
            imvcdccccl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        root_dir = imvcdcccex.get_test_data_dir()
        expected = os.path.join(root_dir, "ccxt/20210924/binance/BTC_USDT.csv.gz")
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        # Run.
        # TODO(gp): We should throw a different exception, like
        #  `UnsupportedExchange`.
        # TODO(gp): Same change also for CDD test_loader.py
        exchange_id = "unsupported exchange"
        currency_pair = "BTC_USDT"
        with self.assertRaises(AssertionError):
            ccxt_client._get_file_path(
                imvcdccccl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
        # TODO(gp): Same change also for CDD test_loader.py
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        with self.assertRaises(AssertionError):
            ccxt_client._get_file_path(
                imvcdccccl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


# #############################################################################
# TestCcxtParquetFileSystemClient
# #############################################################################


class TestCcxtParquetFileSystemClient(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that Parquet files from filesystem are being read correctly.
        """
        ccxt_client = imvcdcccex.get_CcxtParquetFileSytemClient_example1()
        #
        full_symbols = ["binance::BTC_USDT"]
        start_ts = None
        end_ts = None
        actual = ccxt_client.read_data(full_symbols, start_ts, end_ts)
        # Check the output values.
        expected_length = 100
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        df.index in [2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(100, 8)
                                     close currency_pair  exchange_id    full_symbol     high      low     open     volume
        timestamp
        2018-08-17 00:00:00+00:00  6311.64      BTC_USDT  binance  binance::BTC_USDT  6319.04  6310.32  6316.00   9.967395
        2018-08-17 00:01:00+00:00  6302.81      BTC_USDT  binance  binance::BTC_USDT  6311.77  6302.81  6311.64  16.781206
        2018-08-17 00:02:00+00:00  6297.26      BTC_USDT  binance  binance::BTC_USDT  6306.00  6292.79  6302.81  55.373226
        ...
        2018-08-17 01:37:00+00:00  6343.14      BTC_USDT  binance  binance::BTC_USDT  6347.00  6343.00  6346.96  10.787817
        2018-08-17 01:38:00+00:00  6339.25      BTC_USDT  binance  binance::BTC_USDT  6345.98  6335.04  6345.98  38.197244
        2018-08-17 01:39:00+00:00  6342.95      BTC_USDT  binance  binance::BTC_USDT  6348.91  6339.00  6339.25  16.394692
        exchange_ids=binance
        currency_pairs=BTC_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test2(self) -> None:
        """
        Test that Parquet files from filesystem are being filtered correctly.
        """
        ccxt_client = imvcdcccex.get_CcxtParquetFileSytemClient_example1()
        #
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00-00:00")
        actual = ccxt_client.read_data(full_symbols, start_ts, end_ts)
        # Check the output values.
        expected_length = 4
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_signature = """
        # df=
        df.index in [2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(4, 8)
                                     close currency_pair  exchange_id    full_symbol     high      low     open     volume
        timestamp
        2018-08-17 00:01:00+00:00  6302.81      BTC_USDT  binance  binance::BTC_USDT  6311.77  6302.81  6311.64  16.781206
        2018-08-17 00:02:00+00:00  6297.26      BTC_USDT  binance  binance::BTC_USDT  6306.00  6292.79  6302.81  55.373226
        2018-08-17 00:03:00+00:00  6294.52      BTC_USDT  binance  binance::BTC_USDT  6299.97  6286.93  6299.97  34.611797
        ...
        2018-08-17 00:02:00+00:00  6297.26      BTC_USDT  binance  binance::BTC_USDT  6306.00  6292.79  6302.81  55.373226
        2018-08-17 00:03:00+00:00  6294.52      BTC_USDT  binance  binance::BTC_USDT  6299.97  6286.93  6299.97  34.611797
        2018-08-17 00:04:00+00:00  6296.10      BTC_USDT  binance  binance::BTC_USDT  6299.98  6290.00  6294.52  22.088586
        exchange_ids=binance
        currency_pairs=BTC_USDT"""
        # pylint: enable=line-too-long
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )
