import logging

import helpers.hunit_test as hunitest
import im_v2.ccxt.utils as imv2ccuti

_LOG = logging.getLogger(__name__)


class TestUtils(hunitest.TestCase):
    def test1(self) -> None:
        currency_pair = "BTC_USDT"
        exchange = "binance"
        contract_type = "spot"
        actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
            currency_pair, exchange, contract_type
        )
        expected = "BTC/USDT"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        currency_pair = "BTC_USDT"
        exchange = "binance"
        contract_type = "futures"
        actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
            currency_pair, exchange, contract_type
        )
        expected = r"""BTC/USDT:USDT"""
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        currency_pair = "BTC_USDT"
        exchange = "kraken"
        contract_type = "spot"
        actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
            currency_pair, exchange, contract_type
        )
        expected = "BTC/USDT"
        self.assert_equal(actual, expected)

    def test4(self) -> None:
        currency_pair = "BTC_USD"
        exchange = "krakenfutures"
        contract_type = "futures"
        actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
            currency_pair, exchange, contract_type
        )
        expected = "BTC/USD:USD"
        self.assert_equal(actual, expected)

    def test5(self) -> None:
        currency_pair = "BTC_USDT"
        exchange = "okx"
        contract_type = "spot"
        actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
            currency_pair, exchange, contract_type
        )
        expected = "BTC/USDT"
        self.assert_equal(actual, expected)

    def test6(self) -> None:
        currency_pair = "BTC_USDT"
        exchange = "okx"
        contract_type = "futures"
        actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
            currency_pair, exchange, contract_type
        )
        expected = "BTC/USDT:USDT"
        self.assert_equal(actual, expected)
    
    def test7(self) -> None:
        currency_pair = "BTCUSDT"
        exchange = "okx"
        contract_type = "futures"
        with self.assertRaises(AssertionError) as fail:
            actual = imv2ccuti.convert_currency_pair_to_ccxt_format(
                currency_pair, exchange, contract_type
            )
        actual_error = str(fail.exception)
        expected = r"""
        * Failed assertion *
        '_' in 'BTCUSDT'
        The currency_pair 'BTCUSDT' does not match the format '{str1}_{str2}'.
        """
        self.assert_equal(actual_error, expected, fuzzy_match=True)
        

