import im_v2.common.universe.test.test_universe as imvcountt


class TestGetUniverseFilePath1(imvcountt.TestGetUniverseFilePath1_TestCase):
    def test_get_universe_file_path(self) -> None:
        """
        A smoke test to test correct file path return when correct version is
        provided.
        """
        self._test_get_universe_file_path("CCXT", "v1")
        self._test_get_universe_file_path("CCXT", "v3")

    def test_get_latest_file_version(self) -> None:
        """
        Verify that the max universe version is correctly detected and
        returned.
        """
        self._test_get_latest_file_version("CCXT")


class TestGetUniverse1(imvcountt.TestGetUniverse1_TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        self._test_get_universe1("CCXT")

    def test_get_universe_invalid_version(self) -> None:
        """
        Verify that incorrect universe version is recognized.
        """
        self._test_get_universe_invalid_version("CCXT")

    def test_get_vendor_universe_small(self) -> None:
        """
        Test that vendor universe is loaded correctly as dict using small
        universe file.
        """
        self._test_get_vendor_universe_small("CCXT", "gateio", "XRP_USDT")
        self._test_get_vendor_universe_small("CCXT", "kucoin", "SOL_USDT")

    def test_get_vendor_universe_as_full_symbol(self) -> None:
        """
        Test that universe as full symbols is received correctly from small
        universal.
        """
        self._test_get_vendor_universe_as_full_symbol(
            "CCXT", ["gateio::XRP_USDT", "kucoin::SOL_USDT"]
        )
