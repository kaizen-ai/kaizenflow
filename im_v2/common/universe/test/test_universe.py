import os

import pytest

import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hunit_test as hunitest
import im_v2.common.universe.universe as imvcounun

# Currently available vendors.
_VENDORS = ["CCXT", "Talos"]


# TODO(Juraj): Factor out the body of the for loops in test methods
# (after #1487 is done).
class TestExtractUniverseVersion1(hunitest.TestCase):

    def test_extract_universe_version(self) -> None:
        """
        Verify function provides expected output on valid inputs.
        """
        versions = [("01", 1), ("10", 10), ("25", 25)]
        for s, n in versions:
            fn = f"/app/im_v2/ccxt/universe/universe_v{s}.json"
            self.assertEqual(imvcounun._extract_universe_version(fn), n)

    def test_extract_universe_version_incorrect_format(self) -> None:
        """
        Verify function raises AssertionError on incorrect input format.
        """
        file_names = ["incorrect", "universe_vxx.json", ""]
        expected_fail = "Can't parse file"
        for fn in file_names:
            with pytest.raises(AssertionError) as fail:
                _ = imvcounun._extract_universe_version(fn)
            self.assertIn(expected_fail, str(fail.value))


# TODO(Juraj): Factor out the body of the for loops in test methods
# (after #1487 is done).


class TestGetUniverseFilePath1(hunitest.TestCase):

    def test_get_universe_file_path(self) -> None:
        """
        A smoke test to test correct file path return when version is provided.
        """
        # These should already exist in the filesystem.
        test_vers = ["v03", "v01"]
        expected_part = "im_v2/{}/universe/universe_{}.json"
        for vendor, version in zip(_VENDORS, test_vers):
            actual = imvcounun._get_universe_file_path(vendor, version=version)
            expected = os.path.join(
                hgit.get_amp_abs_path(),
                expected_part.format(vendor.lower(), version),
            )
            self.assertEqual(actual, expected)

    def test_get_latest_file_version(self) -> None:
        """
        Verify that the max universe version is correctly detected and
        returned.
        """
        # Future proof this test when new versions are added.
        # Assuming we won't have more versions :).
        max_ver = 9999
        expected_part = "im_v2/{}/universe/universe_v{}.json"
        for vendor in _VENDORS:
            mock_universe = os.path.join(
                hgit.get_amp_abs_path(),
                expected_part.format(vendor.lower(), max_ver),
            )
            # Create tmp mock file as max version.
            with open(mock_universe, mode="w", encoding="utf-8") as _:
                pass
            actual = imvcounun._get_universe_file_path(vendor)
            # Delete tmp file.
            hio.delete_file(mock_universe)
            self.assertEqual(actual, mock_universe)


class TestGetUniverse1(hunitest.TestCase):

    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        for vendor in _VENDORS:
            _ = imvcounun._get_trade_universe(vendor)
            _ = imvcounun._get_trade_universe(vendor, version="v01")

    def test_get_universe_invalid_vendor(self) -> None:
        """
        Verify that incorrect vendor name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imvcounun._get_trade_universe("unknown")

    def test_get_universe_invalid_version(self) -> None:
        """
        Verify that incorrect universe version is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imvcounun._get_trade_universe("CCXT", version="unknown")


class TestGetVendorUniverse1(hunitest.TestCase):

    def test_get_vendor_universe(self) -> None:
        """
        Test that vendor universe is loaded correctly as dict.
        """
        self.get_vendor_universe_helper("CCXT", "gateio", "XRP_USDT")
        self.get_vendor_universe_helper("Talos", "binance", "ADA_USDT")

    def get_vendor_universe_helper(
        self, vendor: str, exchange: str, currency_pair: str
    ) -> None:
        """
        Helper function to test universe is loaded correctly as dict.
        """
        universe = imvcounun.get_vendor_universe(vendor, version="small")
        self.assertIn(exchange, universe)
        self.assertEqual([currency_pair], universe[exchange])

    def test_get_vendor_universe_as_full_symbol(self) -> None:
        """
        Test that universe as full symbols is received correctly.
        """
        universe_as_full_symbols = imvcounun.get_vendor_universe(
            "CCXT", version="small", as_full_symbol=True
        )
        self.assertEqual(len(universe_as_full_symbols), 2)
        self.assertEqual(universe_as_full_symbols[0], "gateio::XRP_USDT")
        self.assertEqual(universe_as_full_symbols[1], "kucoin::SOL_USDT")

        universe_as_full_symbols = imvcounun.get_vendor_universe(
            "Talos", version="small", as_full_symbol=True
        )
        self.assertEqual(len(universe_as_full_symbols), 2)
        self.assertEqual(universe_as_full_symbols[0], "binance::ADA_USDT")
        self.assertEqual(universe_as_full_symbols[1], "ftx::BNB_USDT")
