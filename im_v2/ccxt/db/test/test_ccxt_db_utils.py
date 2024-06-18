import logging
import unittest.mock as umock

import ccxt
import pytest

import helpers.henv as henv
import helpers.hsql as hsql
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not henv.execute_repo_config_code("has_dind_support()")
    and not henv.execute_repo_config_code("use_docker_sibling_containers()"),
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Need docker children / sibling support and/or Run only if CK S3 is available",
)
@pytest.mark.slow("22 seconds.")
class TestPopulateExchangeCurrencyTables(imvcddbut.TestImDbHelper):
    # Mock calls to external providers.
    ccxt_patch = umock.patch.object(imvccdbut, "ccxt", spec=ccxt)

    # This will be run before and after each test.
    @pytest.fixture(autouse=True)
    def setup_teardown_test(self):
        # Run before each test.
        self.set_up_test()
        yield
        # Run after each test.
        self.tear_down_test()

    def set_up_test(self) -> None:
        # Activate mock for `ccxt`.
        self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
        # Set up the mock exchanges
        self.ccxt_mock.exchanges = ["binance"]
        # Mock the binance ccxt to return load market keys.
        self.ccxt_mock.binance.return_value.load_markets.return_value.keys.return_value = [
            "MOCKBTC/MOCKUSD"
        ]

    def tear_down_test(self) -> None:
        self.ccxt_patch.stop()

    def test1(self) -> None:
        """
        Check that the exchange_currency and currency_pair tables are
        populated.
        """
        # Run test.
        imvccdbut.populate_exchange_currency_tables(self.connection)
        #
        # Check that the exchange_currency table is populated.
        #
        # Check the content of the table.
        query = f"SELECT * FROM exchange_currency"
        actual = hsql.execute_query_to_df(self.connection, query)
        # Define expected values.
        expected_length = 1
        expected_column_value = None
        expected_signature = r"""
        # df=
        index=[0, 0]
        columns=exchange_id,exchange_name
        shape=(1, 2)
        exchange_id exchange_name
        0            0       binance
        """
        # Check signature.
        self.check_df_output(
            actual,
            expected_length,
            expected_column_value,
            expected_column_value,
            expected_signature,
        )
        #
        # Check that the currency_pair table is populated.
        #
        # Check the content of the table.
        query = f"SELECT * FROM currency_pair"
        actual = hsql.execute_query_to_df(self.connection, query)
        # Define expected values.
        expected_length = 1
        expected_column_value = None
        expected_signature = r"""
        # df=
        index=[0, 0]
        columns=currency_pair_id,currency_pair
        shape=(1, 2)
        currency_pair_id    currency_pair
        0                 0  MOCKBTC/MOCKUSD
        """
        # Check signature.
        self.check_df_output(
            actual,
            expected_length,
            expected_column_value,
            expected_column_value,
            expected_signature,
        )
