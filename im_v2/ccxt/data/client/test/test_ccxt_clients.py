from typing import List

import pandas as pd

import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.ccxt.data.client.test.ccxt_clients_example as ivcdctcce
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.common.db.db_utils as imvcddbut

# #############################################################################
# TestCcxtCsvClient1
# #############################################################################


class TestCcxtCsvClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data1(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 100
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 199
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:02:00-00:00")
        #
        expected_length = 196
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 9
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 8
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example2()
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example2()
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

    # TODO(gp): @all if this is common, move it outside the classes.
    @staticmethod
    def _get_expected_column_names() -> List[str]:
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
# TestCcxtPqByAssetClient1
# #############################################################################


class TestCcxtPqByAssetClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data1(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 100
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        #
        expected_length = 199
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:02:00-00:00")
        #
        expected_length = 196
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 9
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        start_ts = pd.Timestamp("2018-08-17T00:01:00-00:00")
        end_ts = pd.Timestamp("2018-08-17T00:04:00-00:00")
        #
        expected_length = 8
        expected_column_names = self._get_expected_column_names()
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
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbol = "binance::BTC_USDT"
        expected_start_ts = pd.to_datetime("2018-08-17 00:00:00", utc=True)
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_ts
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2018-08-17 01:39:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)

    # ////////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
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

    @staticmethod
    def _get_expected_column_names() -> List[str]:
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 5
        expected_column_names = self._get_expected_column_names()
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        #
        expected_length = 8
        expected_column_names = self._get_expected_column_names()
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = pd.Timestamp("2021-09-09T00:02:00-00:00")
        #
        expected_length = 4
        expected_column_names = self._get_expected_column_names()
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        end_ts = pd.Timestamp("2021-09-09T00:02:00-00:00")
        #
        expected_length = 3
        expected_column_names = self._get_expected_column_names()
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbols = ["binance::BTC_USDT", "binance::ETH_USDT"]
        start_ts = pd.Timestamp("2021-09-09T00:01:00-00:00")
        end_ts = pd.Timestamp("2021-09-09T00:02:00-00:00")
        #
        expected_length = 2
        expected_column_names = self._get_expected_column_names()
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbol = "unsupported_exchange::unsupported_currency"
        self._test_read_data6(im_client, full_symbol)

    # ///////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        #
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
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
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
            )
        )
        full_symbol = "binance::BTC_USDT"
        expected_end_ts = pd.to_datetime("2021-09-09 00:04:00", utc=True)
        self._test_get_end_ts_for_symbol1(im_client, full_symbol, expected_end_ts)
        # Delete the table.
        hsql.remove_table(self.connection, "ccxt_ohlcv")

    # ///////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        vendor = "CCXT"
        im_client = (
            icdcl.CcxtCddDbClient(  # pylint: disable=no-value-for-parameter
                vendor, self.connection
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

    @staticmethod
    def _get_expected_column_names() -> List[str]:
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
