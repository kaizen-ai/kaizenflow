import helpers.unit_test as hunitest

import im_v2.common.data.client.clients as imvcdcadlo


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
