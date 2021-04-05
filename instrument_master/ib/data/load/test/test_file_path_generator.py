import helpers.unit_test as hut
import instrument_master.common.data.types as icdtyp
import instrument_master.ib.data.load.ib_file_path_generator as iidlib


class TestIbFilePathGenerator(hut.TestCase):
    """
    Test correctness of S3 IB paths.
    """

    def setUp(self) -> None:
        super().setUp()
        self._file_path_generator = iidlib.IbFilePathGenerator()

    def test_generate_file_path1(self) -> None:
        """
        Test path for ESZ21.
        """
        # Generate path to symbol.
        path = self._file_path_generator.generate_file_path(
            symbol="ESZ21",
            frequency=icdtyp.Frequency.Minutely,
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Expiry,
            ext=icdtyp.Extension.CSV,
        )
        # Compare with expected value.
        self.check_string(path)

    def test_generate_file_path2(self) -> None:
        """
        Test path for TSLA.
        """
        # Generate path to symbol.
        path = self._file_path_generator.generate_file_path(
            symbol="TSLA",
            frequency=icdtyp.Frequency.Minutely,
            asset_class=icdtyp.AssetClass.Stocks,
            contract_type=icdtyp.ContractType.Continuous,
            ext=icdtyp.Extension.CSV,
        )
        # Compare with expected value.
        self.check_string(path)

    def test_generate_file_path3(self) -> None:
        """
        Test path for CLH21.
        """
        # Generate path to symbol.
        path = self._file_path_generator.generate_file_path(
            symbol="CLH21",
            frequency=icdtyp.Frequency.Daily,
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Expiry,
            ext=icdtyp.Extension.CSV,
        )
        # Compare with expected value.
        self.check_string(path)
