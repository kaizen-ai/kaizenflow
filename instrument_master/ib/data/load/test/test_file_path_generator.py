import helpers.unit_test as hut
import instrument_master.common.data.types as vcdtyp
import instrument_master.ib.data.load.ib_file_path_generator as vidlfi


class TestIbFilePathGenerator(hut.TestCase):
    """
    Test correctness of S3 IB paths.
    """

    def setUp(self) -> None:
        super().setUp()
        self._file_path_generator = vidlfi.IbFilePathGenerator()

    def test_generate_file_path1(self) -> None:
        """
        Test path for ESZ21.
        """
        # Generate path to symbol.
        path = self._file_path_generator.generate_file_path(
            symbol="ESZ21",
            frequency=vcdtyp.Frequency.Minutely,
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Expiry,
            ext=vcdtyp.Extension.CSV,
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
            frequency=vcdtyp.Frequency.Minutely,
            asset_class=vcdtyp.AssetClass.Stocks,
            contract_type=vcdtyp.ContractType.Continuous,
            ext=vcdtyp.Extension.CSV,
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
            frequency=vcdtyp.Frequency.Daily,
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Expiry,
            ext=vcdtyp.Extension.CSV,
        )
        # Compare with expected value.
        self.check_string(path)
