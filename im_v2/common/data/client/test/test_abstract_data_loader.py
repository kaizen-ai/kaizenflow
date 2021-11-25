import helpers.unit_test as hunitest

import im_v2.common.data.client.abstract_data_loader as imvcdcadlo


class TestDassertIsFullSymbolValid(hunitest.TestCase):
    def test1(self) -> None:
        full_symbol = "binance::BTC_USDT"
        imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test2(self) -> None:
        full_symbol = "binance::BTC/USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test3(self) -> None:
        full_symbol = "bi nance::BTC_USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test4(self) -> None:
        full_symbol = "bi1nance::BTC2USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test5(self) -> None:
        full_symbol = ""
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test6(self) -> None:
        full_symbol = 123
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)

    def test7(self) -> None:
        full_symbol = "binance;;BTC_USDT"
        with self.assertRaises(AssertionError):
            imvcdcadlo.dassert_is_full_symbol_valid(full_symbol)
