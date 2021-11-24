import os
from typing import List

import pandas as pd
import pytest

import helpers.git as hgit
import helpers.s3 as hs3
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im.ccxt.db.utils as imccdbuti
import im_v2.ccxt.data.client.loader as imcdacllo
import im_v2.common.universe.universe as imvcounun

_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "ETH/USDT"
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader._get_file_path(
            imcdacllo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        # TODO(gp): CmampTask413: Use get_bucket()
        expected = (
            "s3://alphamatic-data/data/ccxt/20210924/binance/ETH_USDT.csv.gz"
        )
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        # TODO(gp): We should throw a different exception, like
        # `UnsupportedExchange`.
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imcdacllo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imcdacllo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


class TestCcxtDbClient(hunitest.TestCase):
    def setUp(self) -> None:
        """
        Initialize the test container.
        """
        super().setUp()
        self.docker_compose_file_path = os.path.join(
            hgit.get_amp_abs_path(), "im_v2/devops/compose/docker-compose.yml"
        )
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} "
            "up -d im_postgres_local"
        )
        hsysinte.system(cmd, suppress_output=False)
        # Set DB credentials.
        self.host = "localhost"
        self.dbname = "im_postgres_db_local"
        self.port = 5432
        self.password = "alsdkqoen"
        self.user = "aljsdalsd"

    def tearDown(self) -> None:
        """
        Bring down the test container.
        """
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} down -v"
        )
        hsysinte.system(cmd, suppress_output=False)

        super().tearDown()

    @pytest.mark.slow()
    def test_waitdb(self) -> None:
        """
        Smoke test.
        """
        hsql.wait_db_connection(self.host, self.dbname, self.port)

    def test_data_insertion(self) -> None:
        """
        Verify that testing dataframe insertion is correct.
        """
        self._create_test_table()
        test_data = self._get_test_data()
        # Try uploading test data.
        self.connection = hsql.get_connection(
            self.host,
            self.dbname,
            self.port,
            self.user,
            self.password,
            autocommit=True,
        )
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Load data.
        df = hsql.execute_query_to_df(self.connection, "SELECT * FROM ccxt_ohlcv")
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    def test_read_data1(self) -> None:
        """
        Verify that data from DB is read correctly.
        """
        self._create_test_table()
        test_data = self._get_test_data()
        # Try uploading test data.
        self.connection = hsql.get_connection(
            self.host,
            self.dbname,
            self.port,
            self.user,
            self.password,
            autocommit=True,
        )
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Load data with client and check if it is correct.
        ccxt_db_client = imcdacllo.CcxtDbClient("ohlcv", self.connection)
        df = ccxt_db_client.read_data("binance::BTC/USDT")
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    def _create_test_table(self) -> None:
        """
        Create a test CCXT OHLCV table.
        """
        query = imccdbuti.get_ccxt_ohlcv_create_table_query()
        hsql.wait_db_connection(self.host, self.dbname, self.port)
        connection = hsql.get_connection(
            self.host,
            self.dbname,
            self.port,
            self.user,
            self.password,
            autocommit=True,
        )
        connection.cursor().execute(query)

    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        test_data = pd.DataFrame(
            columns=[
                "id", "timestamp", "open", "high", "low", "close", "volume", "currency_pair", "exchange_id", "created_at"
            ],
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "BTC/USDT", "binance", pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "BTC/USDT", "binance", pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "ETH/USDT", "binance", pd.Timestamp("2021-09-09")],
                [4, 1631145780000, 33, 43, 53, 63, 73, "BTC/USDT", "kucoin", pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "BTC/USDT", "binance", pd.Timestamp("2021-09-09")],
                [6, 1631145900000, 34, 44, 54, 64, 74, "BTC/USDT", "kucoin", pd.Timestamp("2021-09-09")],
                [7, 1631145960000, 34, 44, 54, 64, 74, "ETH/USDT", "binance", pd.Timestamp("2021-09-09")],
            ]
        )
        return test_data


class TestCcxtLoaderFromFileReadUniverseData(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that all files from universe version are being read correctly.
        """
        # Initialize loader and get actual result.
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_universe_data(
            universe="small", data_type="OHLCV"
        )
        actual_json = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_json)

    @pytest.mark.slow("18 seconds.")
    def test2(self) -> None:
        """
        Test that data for provided list of tuples is being read correctly.
        """
        # Set input universe.
        input_universe = [
            imvcounun.ExchangeCurrencyTuple("kucoin", "BTC/USDT"),
            imvcounun.ExchangeCurrencyTuple("kucoin", "ETH/USDT"),
        ]
        # Initialize loader and get actual result.
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_universe_data(
            universe=input_universe, data_type="OHLCV"
        )
        actual_json = hunitest.convert_df_to_json_string(actual)
        # Check output.
        self.check_string(actual_json)

    def test3(self) -> None:
        """
        Test that all files from small test universe are being read correctly.
        """
        # Initialize loader and get actual result.
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_universe_data(
            universe="small", data_type="OHLCV"
        )
        # Check output.
        expected_length = 190046
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL/USDT", "XRP/USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def _check_output(
        self,
        actual: pd.DataFrame,
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
    ) -> None:
        """
        Verify that actual outcome dataframe matches the expected one.

        :param actual: actual outcome dataframe
        :param expected_length: expected outcome dataframe length
        :param expected_exchange_ids: list of expected exchange ids
        :param expected_currency_pairs: list of expected currency pairs
        """
        # Check output df length.
        self.assert_equal(str(expected_length), str(actual.shape[0]))
        # Check unique exchange ids in the output df.
        actual_exchange_ids = sorted(list(actual["exchange_id"].unique()))
        self.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
        # Check unique currency pairs in the output df.
        actual_currency_pairs = sorted(list(actual["currency_pair"].unique()))
        self.assert_equal(
            str(actual_currency_pairs), str(expected_currency_pairs)
        )
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)


# TODO(*): Consider to factor out the class calling in a `def _get_loader()`.
class TestCcxtLoaderFromFileReadData(hunitest.TestCase):
    @pytest.mark.slow("12 seconds.")
    def test1(self) -> None:
        """
        Test that files on S3 are being read correctly.
        """
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_data("binance", "BTC/USDT", "OHLCV")
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("unsupported_exchange_id", "BTC/USDT", "OHLCV")

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("binance", "unsupported_currency_pair", "OHLCV")

    def test4(self) -> None:
        """
        Test unsupported data type.
        """
        ccxt_loader = imcdacllo.CcxtLoaderFromFile(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("binance", "BTC/USDT", "unsupported_data_type")
