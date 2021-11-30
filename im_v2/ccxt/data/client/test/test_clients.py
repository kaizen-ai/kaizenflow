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
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client.clients as ivcdclcl

_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "ETH_USDT"
        ccxt_loader = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader._get_file_path(
            imvcdclcl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        s3_bucket_path = hs3.get_path()
        expected = os.path.join(
            s3_bucket_path, "data/ccxt/20210924/binance/ETH_USDT.csv.gz"
        )
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA_USDT"
        ccxt_loader = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
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
        ccxt_loader = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imvcdclcl._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


@pytest.mark.skipif(hgit.is_lime(), reason="lime doesn't have dind support")
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
        self.user = "aljsdalsd"
        self.password = "alsdkqoen"
        # Wait for DB connection.
        hsql.wait_db_connection(
            self.host, self.dbname, self.port, self.user, self.password
        )
        # Get DB connection.
        self.connection = hsql.get_connection(
            self.host,
            self.dbname,
            self.port,
            self.user,
            self.password,
            autocommit=True,
        )

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

    @pytest.mark.slow("8 seconds.")
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
        df = ccxt_db_client.read_data("binance::BTC_USDT")
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    @pytest.mark.slow("8 seconds.")
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
        df = ccxt_db_client.read_data(
            "binance::BTC_USDT",
            start_ts=pd.Timestamp("2021-09-08T20:01:00-04:00"),
            end_ts=pd.Timestamp("2021-09-08T20:04:00-04:00"),
        )
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    def _create_test_table(self) -> None:
        """
        Create a test CCXT OHLCV table in DB.
        """
        query = imccdbuti.get_ccxt_ohlcv_create_table_query()
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
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "BTC_USDT", "binance", pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "BTC_USDT", "binance", pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "ETH_USDT", "binance", pd.Timestamp("2021-09-09")],
                [4, 1631145780000, 33, 43, 53, 63, 73, "BTC_USDT", "kucoin", pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "BTC_USDT", "binance", pd.Timestamp("2021-09-09")],
                [6, 1631145900000, 34, 44, 54, 64, 74, "BTC_USDT", "kucoin", pd.Timestamp("2021-09-09")],
                [7, 1631145960000, 34, 44, 54, 64, 74, "ETH_USDT", "binance", pd.Timestamp("2021-09-09")],
            ]
            # fmt: on
        )
        return test_data


# TODO(*): Consider to factor out the class calling in a `def _get_loader()`.
class TestCcxtLoaderFromFileReadData(hunitest.TestCase):
    @pytest.mark.slow("14 seconds.")
    def test1(self) -> None:
        """
        Test that files on S3 are being read correctly.
        """
        ccxt_loader = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_data("binance::BTC_USDT")
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test unsupported full symbol.
        """
        ccxt_loader = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("unsupported_exchange::unsupported_currency")

    def test3(self) -> None:
        """
        Test unsupported data type.
        """
        with self.assertRaises(AssertionError):
            imvcdclcl.CcxtFileSystemClient(
                data_type="unsupported_data_type",
                root_dir=_AM_S3_ROOT_DIR,
                aws_profile="am",
            )


@pytest.mark.skipif(hgit.is_lime(), reason="lime doesn't have dind support")
class TestMultipleSymbolsCcxtFileSystemClient(hunitest.TestCase):
    @pytest.mark.slow("12 seconds.")
    def test1(self) -> None:
        """
        Test that data for provided list of full symbols is being read
        correctly.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::XRP_USDT", "gateio::SOL_USDT"]
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        multiple_symbols_client = ivcdclcl.MultipleSymbolsClient(
            class_=ccxt_file_client, mode="concat"
        )
        # Check actual results.
        actual = multiple_symbols_client.read_data(full_symbols=full_symbols)
        expected_length = 1593983
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL_USDT", "XRP_USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def test2(self) -> None:
        """
        Test that all files are being read and filtered correctly.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::SOL_USDT", "gateio::XRP_USDT"]
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imvcdclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        multiple_symbols_client = ivcdclcl.MultipleSymbolsClient(
            class_=ccxt_file_client, mode="concat"
        )
        # Check output.
        actual = multiple_symbols_client.read_data(
            full_symbols=full_symbols,
            start_ts=pd.Timestamp("2021-09-01T00:00:00-04:00"),
            end_ts=pd.Timestamp("2021-09-02T00:00:00-04:00"),
        )
        expected_length = 2880
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL_USDT", "XRP_USDT"]
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
        actual_exchange_ids = sorted(
            list(actual["exchange_id"].dropna().unique())
        )
        self.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
        # Check unique currency pairs in the output df.
        actual_currency_pairs = sorted(
            list(actual["currency_pair"].dropna().unique())
        )
        self.assert_equal(
            str(actual_currency_pairs), str(expected_currency_pairs)
        )
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)


# TODO(Dan): CmTask607.
@pytest.mark.skipif(hgit.is_lime(), reason="lime doesn't have dind support")
class TestMultipleSymbolsCcxtDbClient(hunitest.TestCase):
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
        # Wait for DB connection.
        hsql.wait_db_connection(
            self.host, self.dbname, self.port, self.user, self.password
        )
        # Get DB connection.
        self.connection = hsql.get_connection(
            self.host,
            self.dbname,
            self.port,
            self.user,
            self.password,
            autocommit=True,
        )

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

    @pytest.mark.slow("8 seconds.")
    def test1(self) -> None:
        """
        Test that data for provided list of full symbols is being read
        correctly.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::SOL_USDT", "gateio::XRP_USDT"]
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Initialize CCXT DB client and pass it to multiple symbols client.
        ccxt_db_client = imvcdclcl.CcxtDbClient("ohlcv", self.connection)
        multiple_symbols_client = ivcdclcl.MultipleSymbolsClient(
            class_=ccxt_db_client, mode="concat"
        )
        # Check actual results.
        actual = multiple_symbols_client.read_data(full_symbols=full_symbols)
        expected_length = 8
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL_USDT", "XRP_USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    @pytest.mark.slow("10 seconds.")
    def test2(self) -> None:
        """
        Test that all files are being read and filtered correctly.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::SOL_USDT", "gateio::XRP_USDT"]
        # Load test data.
        self._create_test_table()
        test_data = self._get_test_data()
        hsql.copy_rows_with_copy_from(self.connection, test_data, "ccxt_ohlcv")
        # Initialize CCXT DB client and pass it to multiple symbols client.
        ccxt_db_client = imvcdclcl.CcxtDbClient("ohlcv", self.connection)
        multiple_symbols_client = ivcdclcl.MultipleSymbolsClient(
            class_=ccxt_db_client, mode="concat"
        )
        # Check output.
        actual = multiple_symbols_client.read_data(
            full_symbols=full_symbols,
            start_ts=pd.Timestamp("2021-09-09T00:01:00"),
            end_ts=pd.Timestamp("2021-09-09T00:04:00"),
        )
        expected_length = 3
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL_USDT", "XRP_USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def _create_test_table(self) -> None:
        """
        Create a test CCXT OHLCV table in DB.
        """
        query = imccdbuti.get_ccxt_ohlcv_create_table_query()
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
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "XRP_USDT", "gateio", pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "XRP_USDT", "gateio", pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "SOL_USDT", "kucoin", pd.Timestamp("2021-09-09")],
                [4, 1631145780000, 33, 43, 53, 63, 73, "SOL_USDT", "kucoin", pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "XRP_USDT", "gateio", pd.Timestamp("2021-09-09")],
                [6, 1631145900000, 34, 44, 54, 64, 74, "XRP_USDT", "gateio", pd.Timestamp("2021-09-09")],
                [7, 1631145960000, 34, 44, 54, 64, 74, "ETH_USDT", "binance", pd.Timestamp("2021-09-09")],
            ]
            # fmt: on
        )
        return test_data

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
        actual_exchange_ids = sorted(
            list(actual["exchange_id"].dropna().unique())
        )
        self.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
        # Check unique currency pairs in the output df.
        actual_currency_pairs = sorted(
            list(actual["currency_pair"].dropna().unique())
        )
        self.assert_equal(
            str(actual_currency_pairs), str(expected_currency_pairs)
        )
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual.reset_index())
        self.check_string(actual_string)
