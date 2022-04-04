"""
Import as:

import im_v2.common.universe.test_universe as imvcounte
"""

import os
from typing import List, Tuple

import pytest

import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hunit_test as hunitest
import im_v2.common.universe.universe as imvcounun


class TestExtractUniverseVersion1(hunitest.TestCase):
    def test_extract_universe_version1(self) -> None:
        """
        Verify function provides expected output on valid input.
        """
        self._test_extract_universe_version("1.1", (1, 1))

    def test_extract_universe_version2(self) -> None:
        """
        Verify function provides expected output on valid input.
        """
        self._test_extract_universe_version("4", (4, 0))

    def test_extract_universe_version3(self) -> None:
        """
        Verify function provides expected output on valid input.
        """
        self._test_extract_universe_version("1.0", (1, 0))

    def test_extract_universe_version4(self) -> None:
        """
        Verify function provides expected output on valid input.
        """
        self._test_extract_universe_version("3.11", (3, 11))

    def test_extract_universe_version5(self) -> None:
        """
        Verify function provides expected output on valid input.
        """
        self._test_extract_universe_version("16.2", (16, 2))

    def test_extract_universe_version6(self) -> None:
        """
        Verify function provides expected output on valid input.
        """
        self._test_extract_universe_version("25.11", (25, 11))

    def test_extract_universe_version_incorrect_format1(self) -> None:
        """
        Verify function raises AssertionError on incorrect input format.
        """
        self._test_extract_universe_version_incorrect_format("incorrect")

    def test_extract_universe_version_incorrect_format2(self) -> None:
        """
        Verify function raises AssertionError on incorrect input format.
        """
        self._test_extract_universe_version_incorrect_format("universe_vxx.json")

    def test_extract_universe_version_incorrect_format3(self) -> None:
        """
        Verify function raises AssertionError on incorrect input format.
        """
        self._test_extract_universe_version_incorrect_format("universe_v.1.json")

    def test_extract_universe_version_incorrect_format4(self) -> None:
        """
        Verify function raises AssertionError on incorrect input format.
        """
        self._test_extract_universe_version_incorrect_format("universe_11.json")

    def _test_extract_universe_version(
        self, version: str, expected: Tuple[int, int]
    ) -> None:
        """
        Verify function provides expected output on valid inputs.

        :param version: version in string format to input, e.g. 1.0
        :param expected: expected output version in (major, minor) format
        """
        fn = f"/app/im_v2/ccxt/universe/universe_v{version}.json"
        self.assertEqual(imvcounun._extract_universe_version(fn), expected)

    def _test_extract_universe_version_incorrect_format(
        self, file_name: str
    ) -> None:
        """
        Helper function to verify function raises AssertionError on incorrect
        input format.

        :param file_name: incorrect file_name to test
        """
        expected_fail = "Can't parse file"
        with pytest.raises(AssertionError) as fail:
            _ = imvcounun._extract_universe_version(file_name)
        self.assertIn(expected_fail, str(fail.value))


class TestGetUniverseGeneral1(hunitest.TestCase):
    def test_get_universe_invalid_vendor(self) -> None:
        """
        Verify that incorrect vendor name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imvcounun._get_trade_universe("unknown")


class TestGetUniverseFilePath1_TestCase(hunitest.TestCase):
    def _test_get_universe_file_path(self, vendor: str, version: str) -> None:
        """
        A smoke test to test correct file path return when correct version is
        provided.

        :param vendor: vendor to apply test to, e.g. CCXT or Talos
        :param version: version to test (should be present for a given vendor)
         e.g. Talos -> v1, CCXT -> v1/v2/v3
        """
        # These should already exist in the filesystem.
        expected_part = "im_v2/{}/universe/universe_{}.json"
        actual = imvcounun._get_universe_file_path(vendor, version=version)
        expected = os.path.join(
            hgit.get_amp_abs_path(),
            expected_part.format(vendor.lower(), version),
        )
        self.assertEqual(actual, expected)

    def _test_get_latest_file_version(self, vendor: str) -> None:
        """
        Verify that the max universe version is correctly detected and
        returned.

        :param vendor: vendor to apply test to, e.g. CCXT or Talos
        """
        # Future proof this test when new versions are added.
        # Assuming we won't have more versions :).
        max_ver = 9999
        expected_part = "im_v2/{}/universe/universe_v{}.json"
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


class TestGetUniverse1_TestCase(hunitest.TestCase):
    def _test_get_universe1(self, vendor: str) -> None:
        """
        A smoke test to verify that universe loads correctly.

        :param vendor: vendor to apply test to, e.g. CCXT or Talos
        """
        _ = imvcounun._get_trade_universe(vendor)
        _ = imvcounun._get_trade_universe(vendor, version="v1")

    def _test_get_universe_invalid_version(
        self, vendor: str, *, version: str = "unknown"
    ) -> None:
        """
        Verify that incorrect universe version is recognized.

        :param vendor: vendor to apply test to, e.g. CCXT or Talos
        """
        with self.assertRaises(AssertionError):
            _ = imvcounun._get_trade_universe(vendor, version=version)

    def _test_get_vendor_universe_small(
        self, vendor: str, exchange: str, currency_pair: str
    ) -> None:
        """
        Test that vendor universe is loaded correctly as dict using small
        universe file.

        :param vendor: vendor to apply test to, e.g. CCXT or Talos
        :param exchange: exchange to load currency pairs for
        :param currency_pair: currency pair in format: SYMBOL_SYMBOL
        """
        self._get_vendor_universe_small(vendor, exchange, currency_pair)

    def _get_vendor_universe_small(
        self, vendor: str, exchange: str, currency_pair: str
    ) -> None:
        """
        Helper function to test universe is loaded correctly as dict.
        """
        universe = imvcounun.get_vendor_universe(vendor, version="small")
        self.assertIn(exchange, universe)
        self.assertEqual([currency_pair], universe[exchange])

    def _test_get_vendor_universe_as_full_symbol(
        self, vendor: str, universe_as_full_symbols: List[str]
    ) -> None:
        """
        Test that universe as full symbols is received correctly from small
        universe.

        :param vendor: vendor to apply test to, e.g. CCXT or Talos
        :param universe_as_full_symbols: list of currency pairs as
         full symbols in format exchange_id::SYMBOL_SYMBOL
        """
        actual = imvcounun.get_vendor_universe(
            vendor, version="small", as_full_symbol=True
        )
        self.assertEqual(len(universe_as_full_symbols), 2)
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual[0], universe_as_full_symbols[0])
        self.assertEqual(actual[1], universe_as_full_symbols[1])
