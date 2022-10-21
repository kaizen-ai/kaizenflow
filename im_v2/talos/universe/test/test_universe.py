import im_v2.common.universe.test.test_universe as imvcountt


class TestGetUniverseFilePath1(imvcountt.TestGetUniverseFilePath1_TestCase):
    def test_get_universe_file_path(self) -> None:
        """
        A smoke test to test correct file path return when correct version is
        provided.
        """
        self._test_get_universe_file_path("talos", "download", "v1")

    def test_get_latest_file_version(self) -> None:
        """
        Verify that the max universe version is correctly detected and
        returned.
        """
        self._test_get_latest_file_version("talos", "download")


class TestGetUniverse1(imvcountt.TestGetUniverse1_TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        self._test_get_universe1("talos")

    def test_get_universe_invalid_version(self) -> None:
        """
        Verify that incorrect universe version is recognized.
        """
        self._test_get_universe_invalid_version("talos")

    def test_get_vendor_universe_small(self) -> None:
        """
        Test that vendor universe is loaded correctly as dict using small
        universe file.
        """
        self._test_get_vendor_universe_small("talos", "binance", "ADA_USDT")
        self._test_get_vendor_universe_small("talos", "ftx", "BNB_USDT")

    def test_get_vendor_universe_as_full_symbol(self) -> None:
        """
        Test that universe as full symbols is received correctly from small
        universe.
        """
        self._test_get_vendor_universe_as_full_symbol(
            "talos", ["binance::ADA_USDT", "ftx::BNB_USDT"]
        )