import helpers.unit_test as hunitest

import im_v2.ccxt.data.client.clients as imcdaclcl
import im_v2.common.data.client.abstract_data_loader as imvcdcadlo

_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestDassertIsFullSymbolValid(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test correct format.
        """
        full_symbol = "binance::BTC_USDT"
        imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test2(self) -> None:
        """
        Test incorrect format: `/` symbol.
        """
        full_symbol = "binance::BTC/USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test3(self) -> None:
        """
        Test incorrect format: whitespace symbol.
        """
        full_symbol = "bi nance::BTC_USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test4(self) -> None:
        """
        Test incorrect format: digit.
        """
        full_symbol = "bi1nance::BTC2USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test5(self) -> None:
        """
        Test incorrect format: empty string.
        """
        full_symbol = ""
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test6(self) -> None:
        """
        Test incorrect format: not string.
        """
        full_symbol = 123
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test7(self) -> None:
        """
        Test incorrect format: not separated by `::`.
        """
        full_symbol = "binance;;BTC_USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)


class TestParseFullSymbol(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test split full symbol into exchange, symbol.
        """
        full_symbol = "ftx::ADA_USDT"
        exchange, symbol = imvcdcadlo.parse_full_symbol(full_symbol)
        self.assert_equal(exchange, "ftx")
        self.assert_equal(symbol, "ADA_USDT")

    def test2(self) -> None:
        """
        Test split full symbol into exchange, symbol.
        """
        full_symbol = "kucoin::XPR_USDT"
        exchange, symbol = imvcdcadlo.parse_full_symbol(full_symbol)
        self.assert_equal(exchange, "kucoin")
        self.assert_equal(symbol, "XPR_USDT")


class TestConstructFullSymbol(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test construct full symbol from exchange, symbol.
        """
        exchange = "bitfinex"
        symbol = "SOL_USDT"
        full_symbol = imvcdcadlo.construct_full_symbol(exchange, symbol)
        self.assert_equal(full_symbol, "bitfinex::SOL_USDT")

    def test2(self) -> None:
        """
        Test construct full symbol from exchange, symbol.
        """
        exchange = "exchange"
        symbol = "symbol"
        full_symbol = imvcdcadlo.construct_full_symbol(exchange, symbol)
        self.assert_equal(full_symbol, "exchange::symbol")


class TestMultipleSymbolsClientReadData(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that all files from universe version are being read correctly.
        """
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imcdaclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        multiple_symbols_client = imvcdcadlo.MultipleSymbolsClient(
            class_=ccxt_file_client, mode="concat"
        )
        # Check actual results.
        actual = multiple_symbols_client.read_data(full_symbols="small")
        expected_length = 190046
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL_USDT", "XRP_USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    @pytest.mark.slow("18 seconds.")
    def test2(self) -> None:
        """
        Test that data for provided list of full symbols is being read correctly.
        """
        # Set input list of full symbols.
        full_symbols = ["kucoin::BTC_USDT", "kucoin::ETC_USDT"]
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imcdaclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        multiple_symbols_client = imvcdcadlo.MultipleSymbolsClient(
            class_=ccxt_file_client, mode="concat"
        )
        # Check actual results.
        actual = multiple_symbols_client.read_data(full_symbols=full_symbols)
        expected_length = 190046
        expected_exchange_ids = ["kucoin"]
        expected_currency_pairs = ["BTC_USDT", "ETH_USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def test3(self) -> None:
        """
        Test that all files are being read and filtered correctly.
        """
        # Initialize CCXT file client and pass it to multiple symbols client.
        ccxt_file_client = imcdaclcl.CcxtFileSystemClient(
            data_type="ohlcv", root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        multiple_symbols_client = imvcdcadlo.MultipleSymbolsClient(
            class_=ccxt_file_client, mode="concat"
        )
        # Check output.
        actual = multiple_symbols_client.read_data(
            full_symbols="small",
            start_ts=pd.Timestamp("2021-09-01T00:00:00-04:00"),
            end_ts=pd.Timestamp("2021-09-02T00:00:00-04:00"),
        )
        expected_length = 190046
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
