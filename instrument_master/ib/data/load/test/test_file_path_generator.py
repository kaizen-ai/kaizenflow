import pytest

import helpers.unit_test as hut
import instrument_master.common.data.types as icdtyp
import instrument_master.ib.data.config as iidcon
import instrument_master.ib.data.load.ib_file_path_generator as iidlib


class TestIbFilePathGenerator(hut.TestCase):
    """
    Test correctness of S3 IB paths.
    """

    def setUp(self) -> None:
        super().setUp()
        self._file_path_generator = iidlib.IbFilePathGenerator()

    def test_get_latest_symbols_file1(self) -> None:
        """
        Get the latest file with the info.
        """
        latest_file = iidlib.IbFilePathGenerator.get_latest_symbols_file()
        self.assertRegex(latest_file, "^%s" % iidcon.S3_PREFIX)

    def test_generate_file_path1(self) -> None:
        """
        Test path for ESZ21.
        """
        # Generate path to symbol.
        act = self._file_path_generator.generate_file_path(
            symbol="ESZ21",
            frequency=icdtyp.Frequency.Minutely,
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Expiry,
            exchange="GLOBEX",
            currency="USD",
            ext=icdtyp.Extension.CSV,
        )
        # Compare with expected value.
        exp = "s3://alphamatic-data/data/ib/Futures/GLOBEX/USD/minutely/ESZ21.csv.gz"
        self.assert_equal(act, exp)

    def test_generate_file_path2(self) -> None:
        """
        Test path for TSLA.
        """
        # Generate path to symbol.
        act = self._file_path_generator.generate_file_path(
            symbol="TSLA",
            frequency=icdtyp.Frequency.Minutely,
            asset_class=icdtyp.AssetClass.Stocks,
            contract_type=icdtyp.ContractType.Continuous,
            exchange="NSDQ",
            currency="USD",
            ext=icdtyp.Extension.CSV,
        )
        # Compare with expected value.
        exp = "s3://alphamatic-data/data/ib/stocks/NSDQ/USD/minutely/TSLA.csv.gz"
        self.assert_equal(act, exp)

    def test_generate_file_path3(self) -> None:
        """
        Test path for CLH21.
        """
        # Generate path to symbol.
        act = self._file_path_generator.generate_file_path(
            symbol="CLH21",
            frequency=icdtyp.Frequency.Daily,
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Expiry,
            exchange="ECBOT",
            currency="EUR",
            ext=icdtyp.Extension.CSV,
        )
        # Compare with expected value.
        exp = "s3://alphamatic-data/data/ib/Futures/ECBOT/EUR/daily/CLH21.csv.gz"
        self.assert_equal(act, exp)