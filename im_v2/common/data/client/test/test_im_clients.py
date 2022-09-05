import pandas as pd
import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client as icdc
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.crypto_chassis.data.client as iccdc

# TODO(Grisha): factor out `ImClient` calls in a helper function.
@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
@pytest.mark.slow("Slow via GH, fast on server.")
class TestHistoricalPqByTileClients1(icdctictc.ImClientTestCase):
    """
    The purpose is to demonstrate possible output formats.
    """

    def test_CcxtHistoricalPqByTileClient1(self) -> None:
        """
        - dataset = ohlcv
        - contract_type = futures
        """
        # Initialize the client.
        universe_version = "v4"
        resample_1min = False
        dataset = "ohlcv"
        contract_type = "futures"
        data_snapshot = "20220620"
        im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "knowledge_timestamp",
        ]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume,knowledge_timestamp
        shape=(12, 7)
                                         full_symbol        open        high         low       close      volume              knowledge_timestamp
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.7727      0.7740      0.7727      0.7735  399952.000 2022-06-24 00:18:14.550505+00:00
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37959.2000  37987.8000  37959.1000  37973.9000     146.115 2022-06-24 05:47:16.075108+00:00
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.7736      0.7736      0.7727      0.7734  188093.000 2022-06-24 00:18:14.550505+00:00
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37933.300  37936.5000  37920.0000  37921.4000     58.987 2022-06-24 05:47:16.075108+00:00
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.772      0.7726      0.7717      0.7722  83315.000 2022-06-24 00:18:14.550505+00:00
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37921.400  37938.5000  37918.4000  37931.3000     48.736 2022-06-24 05:47:16.075108+00:00
        """
        # Check.
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

    def test_CcxtHistoricalPqByTileClient2(self) -> None:
        """
        - dataset = ohlcv
        - contract_type = spot
        """
        # Initialize the client.
        universe_version = "v4"
        resample_1min = False
        dataset = "ohlcv"
        contract_type = "spot"
        data_snapshot = "20220530"
        im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "knowledge_timestamp",
        ]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume,knowledge_timestamp
        shape=(12, 7)
                                         full_symbol        open        high         low       close       volume knowledge_timestamp
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.7731      0.7745      0.7731      0.7741  40082.50000          2022-05-10
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37969.9900  37998.2500  37969.9900  37987.2000     14.82533          2022-05-10
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.7740      0.7741      0.7732      0.7739  14064.20000          2022-05-10
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37951.8100  37951.8100  37938.4900  37939.3600     10.20479          2022-05-10
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.7726      0.7731      0.7724      0.7727  42522.60000          2022-05-10
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37939.3600  37956.1000  37936.0400  37950.7500     15.30911          2022-05-10
        """
        # Check.
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

    def test_CryptoChassisHistoricalPqByTileClient1(self) -> None:
        """
        - dataset = ohlcv
        - contract_type = futures
        """
        # Initialize the client.
        universe_version = "v3"
        resample_1min = True
        dataset = "ohlcv"
        contract_type = "futures"
        data_snapshot = "20220620"
        im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_column_names = [
            "close",
            "full_symbol",
            "high",
            "knowledge_timestamp",
            "low",
            "number_of_trades",
            "open",
            "twap",
            "volume",
            "vwap",
        ]
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume,vwap,number_of_trades,twap,knowledge_timestamp
        shape=(12, 10)
                                         full_symbol        open        high         low       close      volume          vwap  number_of_trades          twap              knowledge_timestamp
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.7727      0.7740      0.7727      0.7735  399952.000      0.773474               152      0.773473 2022-06-20 09:48:13.737310+00:00
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37959.2000  37987.8000  37959.1000  37973.9000     146.115  37974.469000               694  37974.598000 2022-06-20 09:48:46.910826+00:00
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.7736      0.7736      0.7727      0.7734  188093.000      0.773096                79      0.773120 2022-06-20 09:48:13.737310+00:00
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37933.300  37936.5000  37920.0000  37921.4000     58.987  37925.266000               406  37926.456000 2022-06-20 09:48:46.910826+00:00
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.772      0.7726      0.7717      0.7722  83315.000      0.772204                71      0.772199 2022-06-20 09:48:13.737310+00:00
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37921.400  37938.5000  37918.4000  37931.3000     48.736  37925.609000               404  37925.788000 2022-06-20 09:48:46.910826+00:00
        """
        # Check.
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

    def test_CryptoChassisHistoricalPqByTileClient2(self) -> None:
        """
        - dataset = ohlcv
        - contract_type = spot
        """
        # Initialize the client.
        universe_version = "v3"
        resample_1min = True
        dataset = "ohlcv"
        contract_type = "spot"
        data_snapshot = "20220530"
        im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_column_names = [
            "close",
            "full_symbol",
            "high",
            "knowledge_timestamp",
            "low",
            "number_of_trades",
            "open",
            "twap",
            "volume",
            "vwap",
        ]
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume,vwap,number_of_trades,twap,knowledge_timestamp
        shape=(12, 10)
                                         full_symbol        open        high         low       close       volume          vwap  number_of_trades          twap              knowledge_timestamp
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.7731      0.7745      0.7731      0.7741  40082.50000      0.773899                59      0.773944 2022-05-18 10:05:55.699321+00:00
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37969.9900  37998.2500  37969.9900  37987.2000     14.82533  37989.163000               565  37985.203200 2022-05-18 10:06:14.692939+00:00
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.7740      0.7741      0.7732      0.7739  14064.20000      0.773764                39      0.773687 2022-05-18 10:05:55.699321+00:00
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37951.8100  37951.8100  37938.4900  37939.3600     10.20479  37946.983900               405  37944.26480 2022-05-18 10:06:14.692939+00:00
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.7726      0.7731      0.7724      0.7727  42522.60000      0.772769                40      0.77277 2022-05-18 10:05:55.699321+00:00
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37939.3600  37956.1000  37936.0400  37950.7500     15.30911  37943.278000               557  37942.54040 2022-05-18 10:06:14.692939+00:00
        """
        # Check.
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

    def test_CryptoChassisHistoricalPqByTileClient3(self) -> None:
        """
        - dataset = bid_ask
        - contract_type = futures
        """
        # Initialize the client.
        universe_version = "v3"
        resample_1min = True
        dataset = "bid_ask"
        contract_type = "futures"
        data_snapshot = "20220620"
        im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_column_names = [
            "full_symbol",
            "bid_price",
            "bid_size",
            "ask_price",
            "ask_size",
        ]
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,bid_price,bid_size,ask_price,ask_size
        shape=(12, 5)
                                         full_symbol     bid_price     bid_size     ask_price     ask_size
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.773418  1577685.000      0.773433   741874.000
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37973.770035      228.977  37974.633461      175.528
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.773116   599395.000      0.773162  1076322.000
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37925.827424     165.176  37926.802980     192.241
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.772086  852299.000      0.772323  817526.000
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37925.604266     254.863  37925.031683     183.618
        """
        # Check.
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

    def test_CryptoChassisHistoricalPqByTileClient4(self) -> None:
        """
        - dataset = bid_ask
        - contract_type = spot
        """
        # Initialize the client.
        universe_version = "v3"
        resample_1min = True
        dataset = "bid_ask"
        contract_type = "spot"
        data_snapshot = "20220530"
        im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_column_names = [
            "full_symbol",
            "bid_price",
            "bid_size",
            "ask_price",
            "ask_size",
            "knowledge_timestamp",
        ]
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,bid_price,bid_size,ask_price,ask_size,knowledge_timestamp
        shape=(12, 6)
                                         full_symbol   bid_price     bid_size   ask_price   ask_size              knowledge_timestamp
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.7731  22296.50000      0.7732  404.60000 2022-05-24 15:04:20.110341+00:00
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37969.9900      4.10126  37970.0000    0.16837 2022-05-24 15:58:40.729661+00:00
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.7740  11602.90000      0.7741  455.40000 2022-05-24 15:04:20.110341+00:00
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37951.8000   5.38906  37951.8100      2.46727 2022-05-24 15:58:40.729661+00:00
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.7726  51.70000      0.7727  20467.60000 2022-05-24 15:04:20.110341+00:00
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37939.3600   5.72901  37939.3700      4.35547 2022-05-24 15:58:40.729661+00:00
        """
        # Check.
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


class TestDataFrameImClients1(icdctictc.ImClientTestCase):
    """
    The purpose is to demonstrate possible output formats.
    """

    def test_read_data5(self) -> None:
        # Initialize client.
        im_client = icdc.get_DataFrameImClient_example1()
        # Set expected values.
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        start_timestamp = pd.Timestamp("2000-01-01 14:34:00+00:00")
        end_timestamp = pd.Timestamp("2000-01-01 14:38:00+00:00")
        expected_length = 10
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "feature1",
        ]
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        expected_signature = r"""# df=
        index=[2000-01-01 14:34:00+00:00, 2000-01-01 14:38:00+00:00]
        columns=full_symbol,open,high,low,close,volume,feature1
        shape=(10, 7)
                                         full_symbol  open  high  low  close  volume  feature1
        timestamp
        2000-01-01 14:34:00+00:00  binance::ADA_USDT   100   101   99  101.0       3       1.0
        2000-01-01 14:34:00+00:00  binance::BTC_USDT   100   101   99  101.0       3       1.0
        2000-01-01 14:35:00+00:00  binance::ADA_USDT   100   101   99  101.0       4       1.0
        ...
        2000-01-01 14:37:00+00:00  binance::BTC_USDT   100   101   99  100.0       6      -1.0
        2000-01-01 14:38:00+00:00  binance::ADA_USDT   100   101   99  100.0       7      -1.0
        2000-01-01 14:38:00+00:00  binance::BTC_USDT   100   101   99  100.0       7      -1.0
        """
        # Run test.
        self._test_read_data5(
            im_client,
            full_symbols,
            start_timestamp,
            end_timestamp,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )


class TestHistoricalPqByTileClients2(hunitest.TestCase):
    """
    Check daily updated data timestamps are the latest.
    """

    @pytest.mark.slow("Slow via GH, fast on server.")
    def test1(self) -> None:
        """
        Compare daily updating data min and max timestamps are in the yesterday-today
        time range.
        
        E.g., "2022.08.28-2022.08.29".
        """
        # Initialize client in order to get data for the previous day.
        im_client = icdcl.get_CcxtHistoricalPqByTileClient_example3()
        full_symbols = ["binance::APE_USDT", "binance::BTC_USDT"]
        # Get the current day to calculate start date. Use `pd.Timestamp` instead of
        # `datetime.datetime` since dataset timestamps are pandas type.
        today = pd.Timestamp.today(tz="UTC")
        start_ts = today - pd.Timedelta(days=1)
        end_ts = today
        columns = None
        data = im_client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            filter_data_mode="assert",
        )
        # Check data timestamps with only year, month, day, beacause time for both values
        # won't be equal. So compare min and max timestamps.
        data_min_timestamp = data.index.min().strftime('%Y.%m.%d')
        data_max_timestamp = data.index.max().strftime('%Y.%m.%d') 
        actual = f"{data_min_timestamp}-{data_min_timestamp}"
        #
        expected_min_timestamp = start_ts.strftime('%Y.%m.%d')
        expected_max_timestamp = end_ts.strftime('%Y.%m.%d')
        expected = (
            f"{expected_min_timestamp}-{expected_max_timestamp}"
        )
        self.assert_equal(actual, expected)
