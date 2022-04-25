import os
from typing import List

import pandas as pd
import pytest

import helpers.hparquet as hparque
import helpers.hsql as hsql
import helpers.hsystem as hsystem
import im_v2.ccxt.data.client as icdcl
import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.common.db.db_utils as imvcddbut
import im_v2.common.universe as ivcu


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


# #############################################################################
# TestCcxtCsvClient1
# #############################################################################


class TestCcxtCsvClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data1(self) -> None:
        im_client = imvcdcccex.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 100
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["binance::BTC_USDT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(100, 6)
                                         full_symbol     open     high      low    close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.00  6319.04  6310.32  6311.64   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.64  6311.77  6302.81  6302.81  16.781206
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.81  6306.00  6292.79  6297.26  55.373226
        ...
        2018-08-17 01:37:00+00:00  binance::BTC_USDT  6346.96  6347.00  6343.00  6343.14  10.787817
        2018-08-17 01:38:00+00:00  binance::BTC_USDT  6345.98  6345.98  6335.04  6339.25  38.197244
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.25  6348.91  6339.00  6342.95  16.394692
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

    def test_read_data2(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 199
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(199, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 01:38:00+00:00   kucoin::ETH_USDT   292.158945   293.007409   292.158945   293.007409   0.001164
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000  6339.000000  6342.950000  16.394692
        2018-08-17 01:39:00+00:00   kucoin::ETH_USDT   292.158945   292.158946   292.158945   292.158946   0.235161
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

    def test_read_data3(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:02:00-00:00")
        #
        expected_length = 196
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:02:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(196, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226
        2018-08-17 00:02:00+00:00   kucoin::ETH_USDT   286.405988   286.405988   285.400193   285.400197   0.162255
        2018-08-17 00:03:00+00:00  binance::BTC_USDT  6299.970000  6299.970000  6286.930000  6294.520000  34.611797
        ...
        2018-08-17 01:38:00+00:00   kucoin::ETH_USDT   292.158945   293.007409   292.158945   293.007409   0.001164
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000  6339.000000  6342.950000  16.394692
        2018-08-17 01:39:00+00:00   kucoin::ETH_USDT   292.158945   292.158946   292.158945   292.158946   0.235161
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

    def test_read_data4(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 9
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(9, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655
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

    def test_read_data5(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 8
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(8, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655
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

    def test_read_data6(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    def test_read_data7(self) -> None:
        resample_1min = False
        im_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 174
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(174, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 01:38:00+00:00   kucoin::ETH_USDT   292.158945   293.007409   292.158945   293.007409   0.001164
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000  6339.000000  6342.950000  16.394692
        2018-08-17 01:39:00+00:00   kucoin::ETH_USDT   292.158945   292.158946   292.158945   292.158946   0.235161
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

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        im_client = imvcdcccex.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        im_client = imvcdcccex.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        im_client = imvcdcccex.get_CcxtCsvClient_example2()
        expected_length = 38
        expected_first_elements = [
            "binance::ADA_USDT",
            "binance::AVAX_USDT",
            "binance::BNB_USDT",
        ]
        expected_last_elements = [
            "kucoin::LINK_USDT",
            "kucoin::SOL_USDT",
            "kucoin::XRP_USDT",
        ]
        self._test_get_universe1(
            im_client,
            expected_length,
            expected_first_elements,
            expected_last_elements,
        )


# #############################################################################
# TestCcxtPqByAssetClient1
# #############################################################################


class TestCcxtPqByAssetClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 100
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["binance::BTC_USDT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(100, 6)
                                         full_symbol     open     high      low    close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.00  6319.04  6310.32  6311.64   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.64  6311.77  6302.81  6302.81  16.781206
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.81  6306.00  6292.79  6297.26  55.373226
        ...
        2018-08-17 01:37:00+00:00  binance::BTC_USDT  6346.96  6347.00  6343.00  6343.14  10.787817
        2018-08-17 01:38:00+00:00  binance::BTC_USDT  6345.98  6345.98  6335.04  6339.25  38.197244
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.25  6348.91  6339.00  6342.95  16.394692
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

    def test_read_data2(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 199
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(199, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 01:38:00+00:00   kucoin::ETH_USDT   292.158945   293.007409   292.158945   293.007409   0.001164
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000  6339.000000  6342.950000  16.394692
        2018-08-17 01:39:00+00:00   kucoin::ETH_USDT   292.158945   292.158946   292.158945   292.158946   0.235161
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

    def test_read_data3(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:02:00-00:00")
        #
        expected_length = 196
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:02:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(196, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226
        2018-08-17 00:02:00+00:00   kucoin::ETH_USDT   286.405988   286.405988   285.400193   285.400197   0.162255
        2018-08-17 00:03:00+00:00  binance::BTC_USDT  6299.970000  6299.970000  6286.930000  6294.520000  34.611797
        ...
        2018-08-17 01:38:00+00:00   kucoin::ETH_USDT   292.158945   293.007409   292.158945   293.007409   0.001164
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000  6339.000000  6342.950000  16.394692
        2018-08-17 01:39:00+00:00   kucoin::ETH_USDT   292.158945   292.158946   292.158945   292.158946   0.235161
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

    def test_read_data4(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 9
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(9, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655
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

    def test_read_data5(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 8
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(8, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655
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

    def test_read_data6(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    def test_read_data7(self) -> None:
        resample_1min = False
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 174
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 01:39:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(174, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 01:38:00+00:00   kucoin::ETH_USDT   292.158945   293.007409   292.158945   293.007409   0.001164
        2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000  6339.000000  6342.950000  16.394692
        2018-08-17 01:39:00+00:00   kucoin::ETH_USDT   292.158945   292.158946   292.158945   292.158946   0.235161
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

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtParquetByAssetClient_example1(
            resample_1min
        )
        expected_length = 38
        expected_first_elements = [
            "binance::ADA_USDT",
            "binance::AVAX_USDT",
            "binance::BNB_USDT",
        ]
        expected_last_elements = [
            "kucoin::LINK_USDT",
            "kucoin::SOL_USDT",
            "kucoin::XRP_USDT",
        ]
        self._test_get_universe1(
            im_client,
            expected_length,
            expected_first_elements,
            expected_last_elements,
        )


# #############################################################################
# TestCcxtDbClient1
# #############################################################################


class TestCcxtCddDbClient1(icdctictc.ImClientTestCase, imvcddbut.TestImDbHelper):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data1(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 5
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["binance::BTC_USDT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(5, 6)
                                         full_symbol  open  high   low  close  volume
        timestamp
        2021-09-09 00:00:00+00:00  binance::BTC_USDT  30.0  40.0  50.0   60.0    70.0
        2021-09-09 00:01:00+00:00  binance::BTC_USDT  31.0  41.0  51.0   61.0    71.0
        2021-09-09 00:02:00+00:00  binance::BTC_USDT   NaN   NaN   NaN    NaN     NaN
        2021-09-09 00:03:00+00:00  binance::BTC_USDT   NaN   NaN   NaN    NaN     NaN
        2021-09-09 00:04:00+00:00  binance::BTC_USDT  34.0  44.0  54.0   64.0    74.0
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
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_read_data2(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        #
        expected_length = 8
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(8, 6)
                                         full_symbol  open  high   low  close  volume
        timestamp
        2021-09-09 00:00:00+00:00  binance::BTC_USDT  30.0  40.0  50.0   60.0    70.0
        2021-09-09 00:01:00+00:00  binance::BTC_USDT  31.0  41.0  51.0   61.0    71.0
        2021-09-09 00:02:00+00:00  binance::BTC_USDT   NaN   NaN   NaN    NaN     NaN
        ...
        2021-09-09 00:03:00+00:00  binance::ETH_USDT   NaN   NaN   NaN    NaN     NaN
        2021-09-09 00:04:00+00:00  binance::BTC_USDT  34.0  44.0  54.0   64.0    74.0
        2021-09-09 00:04:00+00:00  binance::ETH_USDT  34.0  44.0  54.0   64.0    74.0
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
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_read_data3(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = pd.Timestamp("2021-09-09T00:02:00-00:00")
        #
        expected_length = 4
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2021-09-09 00:02:00+00:00, 2021-09-09 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(4, 6)
                                         full_symbol  open  high   low  close  volume
        timestamp
        2021-09-09 00:02:00+00:00  binance::ETH_USDT  32.0  42.0  52.0   62.0    72.0
        2021-09-09 00:03:00+00:00  binance::ETH_USDT   NaN   NaN   NaN    NaN     NaN
        2021-09-09 00:04:00+00:00  binance::BTC_USDT  34.0  44.0  54.0   64.0    74.0
        2021-09-09 00:04:00+00:00  binance::ETH_USDT  34.0  44.0  54.0   64.0    74.0
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
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_read_data4(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        end_ts = pd.Timestamp("2021-09-09T00:02:00-00:00")
        #
        expected_length = 3
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2021-09-09 00:00:00+00:00, 2021-09-09 00:02:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(3, 6)
                                         full_symbol  open  high   low  close  volume
        timestamp
        2021-09-09 00:00:00+00:00  binance::BTC_USDT  30.0  40.0  50.0   60.0    70.0
        2021-09-09 00:01:00+00:00  binance::BTC_USDT  31.0  41.0  51.0   61.0    71.0
        2021-09-09 00:02:00+00:00  binance::ETH_USDT  32.0  42.0  52.0   62.0    72.0
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
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_read_data5(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = pd.Timestamp("2021-09-09T00:01:00-00:00")
        end_ts = pd.Timestamp("2021-09-09T00:02:00-00:00")
        #
        expected_length = 2
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2021-09-09 00:01:00+00:00, 2021-09-09 00:02:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(2, 6)
                                         full_symbol  open  high   low  close  volume
        timestamp
        2021-09-09 00:01:00+00:00  binance::BTC_USDT  31.0  41.0  51.0   61.0    71.0
        2021-09-09 00:02:00+00:00  binance::ETH_USDT  32.0  42.0  52.0   62.0    72.0
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
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_read_data6(self) -> None:
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    def test_read_data7(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = False
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        #
        expected_length = 5
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(5, 6)
                                         full_symbol  open  high   low  close  volume
        timestamp
        2021-09-09 00:00:00+00:00  binance::BTC_USDT  30.0  40.0  50.0   60.0    70.0
        2021-09-09 00:01:00+00:00  binance::BTC_USDT  31.0  41.0  51.0   61.0    71.0
        2021-09-09 00:02:00+00:00  binance::ETH_USDT  32.0  42.0  52.0   62.0    72.0
        2021-09-09 00:04:00+00:00  binance::BTC_USDT  34.0  44.0  54.0   64.0    74.0
        2021-09-09 00:04:00+00:00  binance::ETH_USDT  34.0  44.0  54.0   64.0    74.0
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
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    # ///////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2021-09-09 00:00:00", utc=True)
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )
        # Delete the table.
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    def test_get_end_ts_for_symbol1(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2021-09-09 00:04:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)
        # Delete the table.
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    # ///////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        resample_1min = True
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, resample_1min, self.connection
            )
        )
        expected_length = 38
        expected_first_elements = [
            "binance::ADA_USDT",
            "binance::AVAX_USDT",
            "binance::BNB_USDT",
        ]
        expected_last_elements = [
            "kucoin::LINK_USDT",
            "kucoin::SOL_USDT",
            "kucoin::XRP_USDT",
        ]
        self._test_get_universe1(
            im_client,
            expected_length,
            expected_first_elements,
            expected_last_elements,
        )

    # ///////////////////////////////////////////////////////////////////////

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
                "end_download_timestamp",
                "knowledge_timestamp",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "BTC_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "BTC_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "ETH_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [4, 1631145840000, 34, 44, 54, 64, 74, "BTC_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "ETH_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        return test_data

    def _create_test_table(self) -> None:
        """
        Create a test CCXT OHLCV table in DB.
        """
        query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        self.connection.cursor().execute(query)


# #############################################################################
# TestCcxtHistoricalPqByTileClient1
# #############################################################################


@pytest.mark.skipif(
    hsystem.is_inside_ci(),
    reason="Extend AWS authentication system CmTask #1666.",
)
class TestCcxtHistoricalPqByTileClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 2881
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["binance::BTC_USDT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-19 00:00:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(2881, 6)
                                         full_symbol     open     high      low    close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.00  6319.04  6310.32  6311.64   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.64  6311.77  6302.81  6302.81  16.781206
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.81  6306.00  6292.79  6297.26  55.373226
        ...
        2018-08-18 23:58:00+00:00  binance::BTC_USDT  6385.48  6390.00  6385.48  6387.01  37.459319
        2018-08-18 23:59:00+00:00  binance::BTC_USDT  6390.00  6390.00  6386.82  6387.96  10.584910
        2018-08-19 00:00:00+00:00  binance::BTC_USDT  6387.96  6387.97  6375.64  6377.25  39.426236
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

    def test_read_data2(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 5761
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-19 00:00:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(5761, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-18 23:59:00+00:00   kucoin::ETH_USDT          NaN      NaN          NaN      NaN        NaN
        2018-08-19 00:00:00+00:00  binance::BTC_USDT  6387.960000  6387.97  6375.640000  6377.25  39.426236
        2018-08-19 00:00:00+00:00   kucoin::ETH_USDT   293.870469   294.00   293.870469   294.00   0.704782
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

    def test_read_data3(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-18T00:23:00-00:00")
        #
        expected_length = 2836
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-18 00:23:00+00:00, 2018-08-19 00:00:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(2836, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-18 00:23:00+00:00  binance::BTC_USDT  6570.830000  6573.800000  6567.980000  6573.800000  43.493238
        2018-08-18 00:23:00+00:00   kucoin::ETH_USDT   316.138881   316.138881   316.021676   316.021676   0.800971
        2018-08-18 00:24:00+00:00  binance::BTC_USDT  6573.560000  6575.000000  6564.470000  6567.010000  58.972297
        ...
        2018-08-18 23:59:00+00:00   kucoin::ETH_USDT          NaN      NaN          NaN      NaN        NaN
        2018-08-19 00:00:00+00:00  binance::BTC_USDT  6387.960000  6387.97  6375.640000  6377.25  39.426236
        2018-08-19 00:00:00+00:00   kucoin::ETH_USDT   293.870469   294.00   293.870469   294.00   0.704782
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

    def test_read_data4(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 9
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-17 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(9, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655
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

    def test_read_data5(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 8
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(8, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655
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

    def test_read_data6(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    def test_read_data7(self) -> None:
        resample_1min = False
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 4791
        expected_column_names = get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2018-08-17 00:00:00+00:00, 2018-08-19 00:00:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(4791, 6)
                                         full_symbol         open         high          low        close     volume
        timestamp
        2018-08-17 00:00:00+00:00  binance::BTC_USDT  6316.000000  6319.040000  6310.320000  6311.640000   9.967395
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500
        ...
        2018-08-18 23:59:00+00:00  binance::BTC_USDT  6390.000000  6390.00  6386.820000  6387.96  10.584910
        2018-08-19 00:00:00+00:00  binance::BTC_USDT  6387.960000  6387.97  6375.640000  6377.25  39.426236
        2018-08-19 00:00:00+00:00   kucoin::ETH_USDT   293.870469   294.00   293.870469   294.00   0.704782
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

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2018-08-19 00:00:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        resample_1min = True
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example2(
            resample_1min
        )
        expected_length = 38
        expected_first_elements = [
            "binance::ADA_USDT",
            "binance::AVAX_USDT",
            "binance::BNB_USDT",
        ]
        expected_last_elements = [
            "kucoin::LINK_USDT",
            "kucoin::SOL_USDT",
            "kucoin::XRP_USDT",
        ]
        self._test_get_universe1(
            im_client,
            expected_length,
            expected_first_elements,
            expected_last_elements,
        )

    # ////////////////////////////////////////////////////////////////////////

    @pytest.mark.skip("Enable when unit test data needs to be generated.")
    def test_write_test_data_to_s3(self) -> None:
        """
        Write unit test data to S3.
        """
        data = self._get_unit_test_data()
        partition_columns = ["currency_pair", "year", "month"]
        # TODO(Grisha): Do not hard-wire the path, use `helpers/hs3.py`.
        dst_dir = "s3://cryptokaizen-data/unit_test/historical/ccxt/latest"
        aws_profile = "ck"
        exchange_id_col_name = "exchange_id"
        for exchange_id, df_exchange_id in data.groupby(exchange_id_col_name):
            exchange_dir = os.path.join(dst_dir, exchange_id)
            df_exchange_id = df_exchange_id.drop(columns="exchange_id")
            hparque.to_partitioned_parquet(
                df_exchange_id,
                partition_columns,
                exchange_dir,
                aws_profile=aws_profile,
            )

    def _get_unit_test_data(self) -> pd.DataFrame:
        """
        Get small part of historical data from S3 for unit testing.

        Implemented transformations:
        - Add necessary columns for partitioning
        - Remove unnecessary column
        - Cut the data so that is light-weight enough for testing
        - Create gaps in data to test resampling

        return: data to be loaded to S3
        """
        resample_1min = False
        im_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example1(
            resample_1min
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        end_ts = pd.to_datetime("2018-08-19 00:00:00", utc=True)
        data = im_client.read_data(full_symbols, start_ts, end_ts)
        # Add missing columns.
        data["exchange_id"], data["currency_pair"] = ivcu.parse_full_symbol(
            data["full_symbol"]
        )
        data["year"] = data.index.year
        data["month"] = data.index.month
        # Remove unnecessary column.
        data = data.drop(columns="full_symbol")
        # Artificially create gaps in data in order test resampling.
        data = pd.concat([data[:100], data[115:]])
        return data
