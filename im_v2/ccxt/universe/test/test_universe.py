import helpers.hunit_test as hunitest
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
        Test that universe as full symbols is received correctly.
        """
        universe_as_full_symbols = imvccunun.get_vendor_universe(version="small")
        self.assertEqual(len(universe_as_full_symbols), 2)
        self.assert_equal(universe_as_full_symbols[0], "gateio::XRP_USDT")
        self.assert_equal(universe_as_full_symbols[1], "kucoin::SOL_USDT")

    def test2(self) -> None:
        """
        Test that universe as numeric ids is received correctly.
        """
        universe_as_numeric_ids = imvccunun.get_vendor_universe(
            version="small", vendor="CCXT", as_ids=True
        )
        self.assertEqual(len(universe_as_numeric_ids), 2)
        self.assertEqual(universe_as_numeric_ids[0], 2002879833)
        self.assertEqual(universe_as_numeric_ids[1], 2568064341)


class TestStringToNumId(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that string id is converted to numeric correctly.
        """
        num_id = imvccunun.string_to_num_id("binance::BTC_USDT")
        self.assertEqual(num_id, 1467591036)


class TestBuildNumToStringIdMapping(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that numeric to string ids mapping is being built correctly.
        """
        mapping = imvccunun.build_num_to_string_id_mapping(
            ("gateio::XRP_USDT", "kucoin::SOL_USDT")
        )
        self.assertEqual(len(mapping), 2)
        self.assert_equal(mapping[2002879833], "gateio::XRP_USDT")
        self.assert_equal(mapping[2568064341], "kucoin::SOL_USDT")

    def test2(self) -> None:
        """
        Smoke test that latest CCXT universe has no collisions in numeric ids.

        Since the mapping is dict with numeric ids as keys, there are no
        collisions if it is just built.
        """
        latest_universe = tuple(imvccunun.get_vendor_universe())
        _ = imvccunun.build_num_to_string_id_mapping(latest_universe)
