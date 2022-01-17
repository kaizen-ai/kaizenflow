import abc
from typing import Any, List

import pandas as pd

import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.client.ccxt_clients_example as ivcdcccex
import im_v2.common.data.client.clients as imvcdclcl


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
# ImClientTestCase
# #############################################################################


class ImClientTestCase(abc.ABC):
    @abc.abstractmethod
    def test_read_data1(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbol: str,
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
        expected_signature: str,
    ) -> None:
        """
        Test:
        - reading data for one symbol
        - start_ts = end_ts = None
        """
        full_symbols = [full_symbol]
        start_ts = None
        end_ts = None
        actual = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    @abc.abstractmethod
    def test_read_data2(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbols: List[str],
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
        expected_signature: str,
    ) -> None:
        """
        Test:
        - reading data for two symbols
        - start_ts = end_ts = None
        """
        start_ts = None
        end_ts = None
        actual = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    @abc.abstractmethod
    def test_read_data3(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbols: List[str],
        start_ts: pd.Timestamp,
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
        expected_signature: str,
    ) -> None:
        """
        Test:
        - reading data for two symbols
        - specified start_ts
        - end_ts = None
        """
        end_ts = None
        actual = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    @abc.abstractmethod
    def test_read_data4(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbols: List[str],
        end_ts: pd.Timestamp,
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
        expected_signature: str,
    ) -> None:
        """
        Test:
        - reading data for two symbols
        - start_ts = None
        - specified end_ts
        """
        start_ts = None
        actual = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    @abc.abstractmethod
    def test_read_data5(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbols: List[str],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
        expected_signature: str,
    ) -> None:
        """
        Test:
        - reading data for two symbols
        - specified start_ts and end_ts
        """
        actual = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(
            self,
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    @abc.abstractmethod
    def test_get_start_ts_for_symbol1(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbol: str,
        expected_start_ts: pd.Timestamp,
    ) -> None:
        """
        Test that the earliest timestamp available is computed correctly.
        """
        actual_start_ts = im_client.get_start_ts_for_symbol(full_symbol)
        self.assertEqual(actual_start_ts, expected_start_ts)

    @abc.abstractmethod
    def test_get_end_ts_for_symbol1(
        self,
        im_client: imvcdclcl.ImClient,
        full_symbol: str,
        expected_end_ts: pd.Timestamp,
    ) -> None:
        """
        Test that the latest timestamp available is computed correctly.
        """
        actual_end_ts = im_client.get_end_ts_for_symbol(full_symbol)
        self.assertEqual(actual_end_ts, expected_end_ts)


class TestCcxtCsvPqByAssetClient1(ImClientTestCase, hunitest.TestCase):
    def test_read_data1(self) -> None:
        """
        Test on a ".pq" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtParquetByAssetClient_example1()
        full_symbol = "binance::BTC_USDT"
        #
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
        super().test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data2(self) -> None:
        """
        Test on a ".csv.gz" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
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
        super().test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data3(self) -> None:
        """
        Test on a ".csv.gz" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:02:00-00:00")
        #
        expected_length = 196
        expected_exchange_ids = ["binance", "kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2018-08-17 00:02:00+00:00, 2018-08-17 01:39:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(196, 8)
                                         close currency_pair exchange_id        full_symbol         high          low         open     volume
        timestamp
        2018-08-17 00:02:00+00:00  6297.260000      BTC_USDT     binance  binance::BTC_USDT  6306.000000  6292.790000  6302.810000  55.373226
        2018-08-17 00:02:00+00:00   285.400197      ETH_USDT      kucoin   kucoin::ETH_USDT   286.405988   285.400193   286.405988   0.162255
        2018-08-17 00:03:00+00:00  6294.520000      BTC_USDT     binance  binance::BTC_USDT  6299.970000  6286.930000  6299.970000  34.611797
        ...
        2018-08-17 01:38:00+00:00   293.007409      ETH_USDT      kucoin   kucoin::ETH_USDT   293.007409   292.158945   292.158945   0.001164
        2018-08-17 01:39:00+00:00  6342.950000      BTC_USDT     binance  binance::BTC_USDT  6348.910000  6339.000000  6339.250000  16.394692
        2018-08-17 01:39:00+00:00   292.158946      ETH_USDT      kucoin   kucoin::ETH_USDT   292.158946   292.158945   292.158945   0.235161
        exchange_ids=binance,kucoin
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        super().test_read_data3(
            im_client,
            full_symbols,
            start_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data4(self) -> None:
        """
        Test on a ".csv.gz" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2018-08-17T00:05:00-00:00")
        #
        expected_length = 9
        expected_exchange_ids = ["binance", "kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2018-08-17 00:00:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(9, 8)
                                         close currency_pair exchange_id        full_symbol         high          low         open     volume
        timestamp
        2018-08-17 00:00:00+00:00  6311.640000      BTC_USDT     binance  binance::BTC_USDT  6319.040000  6310.320000  6316.000000   9.967395
        2018-08-17 00:01:00+00:00  6302.810000      BTC_USDT     binance  binance::BTC_USDT  6311.770000  6302.810000  6311.640000  16.781206
        2018-08-17 00:01:00+00:00   286.712987      ETH_USDT      kucoin   kucoin::ETH_USDT   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 00:03:00+00:00   285.400193      ETH_USDT      kucoin   kucoin::ETH_USDT   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  6296.100000      BTC_USDT     binance  binance::BTC_USDT  6299.980000  6290.000000  6294.520000  22.088586
        2018-08-17 00:04:00+00:00   285.884638      ETH_USDT      kucoin   kucoin::ETH_USDT   285.884638   285.400193   285.400193   0.074655
        exchange_ids=binance,kucoin
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        super().test_read_data4(
            im_client,
            full_symbols,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data5(self) -> None:
        """
        Test on a ".csv.gz" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00-00:00")
        #
        expected_length = 8
        expected_exchange_ids = ["binance", "kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(8, 8)
                                         close currency_pair exchange_id        full_symbol         high          low         open     volume
        timestamp
        2018-08-17 00:01:00+00:00  6302.810000      BTC_USDT     binance  binance::BTC_USDT  6311.770000  6302.810000  6311.640000  16.781206
        2018-08-17 00:01:00+00:00   286.712987      ETH_USDT      kucoin   kucoin::ETH_USDT   286.712987   286.712987   286.712987   0.017500
        2018-08-17 00:02:00+00:00  6297.260000      BTC_USDT     binance  binance::BTC_USDT  6306.000000  6292.790000  6302.810000  55.373226
        ...
        2018-08-17 00:03:00+00:00   285.400193      ETH_USDT      kucoin   kucoin::ETH_USDT   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  6296.100000      BTC_USDT     binance  binance::BTC_USDT  6299.980000  6290.000000  6294.520000  22.088586
        2018-08-17 00:04:00+00:00   285.884638      ETH_USDT      kucoin   kucoin::ETH_USDT   285.884638   285.400193   285.400193   0.074655
        exchange_ids=binance,kucoin
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        super().test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_get_start_ts_for_symbol1(self) -> None:
        """
        Test on a ".csv" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        super().test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        """
        Test on a ".csv" file on the local filesystem.
        """
        im_client = ivcdcccex.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        super().test_get_end_ts_for_symbol1(
            im_client, full_symbol, expected_end_ts
        )
