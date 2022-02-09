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
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data2(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
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
        self._test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data3(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
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
        self._test_read_data3(
            im_client,
            full_symbols,
            start_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data4(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
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
        self._test_read_data4(
            im_client,
            full_symbols,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data5(self) -> None:
        im_client = ivcdctcce.get_CcxtCsvClient_example1()
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
        self._test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data2(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
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
        self._test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data3(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
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
        self._test_read_data3(
            im_client,
            full_symbols,
            start_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data4(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
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
        self._test_read_data4(
            im_client,
            full_symbols,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
            expected_signature,
        )

    def test_read_data5(self) -> None:
        im_client = ivcdctcce.get_CcxtParquetByAssetClient_example1()
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
        self._test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(5, 8)
                                   close currency_pair exchange_id        full_symbol  high   low  open  volume
        timestamp
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT     binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT     binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00    NaN           NaN         NaN  binance::BTC_USDT   NaN   NaN   NaN     NaN
        2021-09-09 00:03:00+00:00    NaN           NaN         NaN  binance::BTC_USDT   NaN   NaN   NaN     NaN
        2021-09-09 00:04:00+00:00   64.0      BTC_USDT     binance  binance::BTC_USDT  44.0  54.0  34.0    74.0
        exchange_ids=binance
        currency_pairs=BTC_USDT"""
        # pylint: enable=line-too-long
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT     binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT     binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00    NaN           NaN         NaN  binance::BTC_USDT   NaN   NaN   NaN     NaN
        ...
        2021-09-09 00:03:00+00:00    NaN           NaN         NaN  binance::ETH_USDT   NaN   NaN   NaN     NaN
        2021-09-09 00:04:00+00:00   64.0      BTC_USDT     binance  binance::BTC_USDT  44.0  54.0  34.0    74.0
        2021-09-09 00:04:00+00:00   64.0      ETH_USDT     binance  binance::ETH_USDT  44.0  54.0  34.0    74.0
        exchange_ids=binance
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        self._test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2021-09-09 00:02:00+00:00, 2021-09-09 00:04:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(4, 8)
                                   close currency_pair exchange_id        full_symbol  high   low  open  volume
        timestamp
        2021-09-09 00:02:00+00:00   62.0      ETH_USDT     binance  binance::ETH_USDT  42.0  52.0  32.0    72.0
        2021-09-09 00:03:00+00:00    NaN           NaN         NaN  binance::ETH_USDT   NaN   NaN   NaN     NaN
        2021-09-09 00:04:00+00:00   64.0      BTC_USDT     binance  binance::BTC_USDT  44.0  54.0  34.0    74.0
        2021-09-09 00:04:00+00:00   64.0      ETH_USDT     binance  binance::ETH_USDT  44.0  54.0  34.0    74.0
        exchange_ids=binance
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        self._test_read_data3(
            im_client,
            full_symbols,
            start_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
        end_ts = pd.Timestamp("2021-09-09T00:04:00-00:00")
        #
        expected_length = 3
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2021-09-09 00:00:00+00:00, 2021-09-09 00:02:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(3, 8)
                                   close currency_pair exchange_id        full_symbol  high   low  open  volume
        timestamp
        2021-09-09 00:00:00+00:00   60.0      BTC_USDT     binance  binance::BTC_USDT  40.0  50.0  30.0    70.0
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT     binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00   62.0      ETH_USDT     binance  binance::ETH_USDT  42.0  52.0  32.0    72.0
        exchange_ids=binance
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        self._test_read_data4(
            im_client,
            full_symbols,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
        end_ts = pd.Timestamp("2021-09-09T00:04:00-00:00")
        #
        expected_length = 2
        expected_exchange_ids = ["binance"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        # pylint: disable=line-too-long
        expected_signature = r"""# df=
        df.index in [2021-09-09 00:01:00+00:00, 2021-09-09 00:02:00+00:00]
        df.columns=close,currency_pair,exchange_id,full_symbol,high,low,open,volume
        df.shape=(2, 8)
                                   close currency_pair exchange_id        full_symbol  high   low  open  volume
        timestamp
        2021-09-09 00:01:00+00:00   61.0      BTC_USDT     binance  binance::BTC_USDT  41.0  51.0  31.0    71.0
        2021-09-09 00:02:00+00:00   62.0      ETH_USDT     binance  binance::ETH_USDT  42.0  52.0  32.0    72.0
        exchange_ids=binance
        currency_pairs=BTC_USDT,ETH_USDT"""
        # pylint: enable=line-too-long
        self._test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
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
                "knowledge_timestamp"
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
