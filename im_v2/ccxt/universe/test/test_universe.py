import helpers.unit_test as hunitest
import im_v2.ccxt.universe.universe as imvccunun


class TestGetUniverse(hunitest.TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        _ = imvccunun.get_trade_universe()

    def test_get_universe2(self) -> None:
        """
        Verify that incorrect universe name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imvccunun.get_trade_universe("non-existent")


class TestGetVendorUniverse(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that universe is received correctly.
        """
        universe_as_full_symbols = imvccunun.get_vendor_universe(version="small")
        self.assertEqual(len(universe_as_full_symbols), 2)
        self.assert_equal(universe_as_full_symbols[0], "gateio::XRP_USDT")
        self.assert_equal(universe_as_full_symbols[1], "kucoin::SOL_USDT")
