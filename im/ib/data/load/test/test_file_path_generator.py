"""
Import as:

import im.ib.data.load.test.test_file_path_generator as tfpgen
"""

import os

import pytest

import helpers.s3 as hs3
import helpers.unit_test as hunitest
import im.common.data.types as imcodatyp
import im.ib.data.config as imibdacon
import im.ib.data.load.ib_file_path_generator as imidlifpge


_S3_BUCKET = hs3.get_bucket()


class TestIbFilePathGenerator(hunitest.TestCase):
    """
    Test correctness of S3 IB paths.
    """

    def setUp(self) -> None:
        super().setUp()
        self._file_path_generator = imidlifpge.IbFilePathGenerator()

    @pytest.mark.skip(msg="See alphamatic/dev_tools#288")
    def test_get_latest_symbols_file1(self) -> None:
        """
        Get the latest file with the info.
        """
        latest_file = imidlifpge.IbFilePathGenerator.get_latest_symbols_file()
        self.assertRegex(latest_file, "^%s" % imibdacon.S3_PREFIX)

    def test_generate_file_path1(self) -> None:
        """
        Test path for ESZ21.
        """
        # Generate path to symbol.
        act = self._file_path_generator.generate_file_path(
            symbol="ESZ21",
            frequency=imcodatyp.Frequency.Minutely,
            asset_class=imcodatyp.AssetClass.Futures,
            contract_type=imcodatyp.ContractType.Expiry,
            exchange="GLOBEX",
            currency="USD",
            ext=imcodatyp.Extension.CSV,
        )
        # Compare with expected value.
        exp = (
            f"s3://{_S3_BUCKET}/data/ib/Futures/GLOBEX/USD/minutely/ESZ21.csv.gz"
        )
        self.assert_equal(act, exp)

    def test_generate_file_path2(self) -> None:
        """
        Test path for TSLA.
        """
        # Generate path to symbol.
        act = self._file_path_generator.generate_file_path(
            symbol="TSLA",
            frequency=imcodatyp.Frequency.Minutely,
            asset_class=imcodatyp.AssetClass.Stocks,
            contract_type=imcodatyp.ContractType.Continuous,
            exchange="NSDQ",
            currency="USD",
            ext=imcodatyp.Extension.CSV,
        )
        # Compare with expected value.
        exp = f"s3://{_S3_BUCKET}/data/ib/stocks/NSDQ/USD/minutely/TSLA.csv.gz"
        self.assert_equal(act, exp)

    def test_generate_file_path3(self) -> None:
        """
        Test path for CLH21.
        """
        # Generate path to symbol.
        act = self._file_path_generator.generate_file_path(
            symbol="CLH21",
            frequency=imcodatyp.Frequency.Daily,
            asset_class=imcodatyp.AssetClass.Futures,
            contract_type=imcodatyp.ContractType.Expiry,
            exchange="ECBOT",
            currency="EUR",
            ext=imcodatyp.Extension.CSV,
        )
        # Compare with expected value.
        exp = f"s3://{_S3_BUCKET}/data/ib/Futures/ECBOT/EUR/daily/CLH21.csv.gz"
        self.assert_equal(act, exp)
