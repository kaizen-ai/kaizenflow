import vendors_amp.ib.data.load.file_path_generator as ibpath
import helpers.unit_test as hut
import vendors_amp.common.data.types as typ


class TestIbFilePathGenerator(hut.TestCase):
    """
    Test correctness of S3 IB paths.
    """
    def setUp(self) -> None:
        super().setUp()
        self._file_path_generator = ibpath.IbFilePathGenerator()

    def test_generate_file_path1(self) -> None:
        """
        Test path for ESZ21.
        """
        # Generate path to symbol.
        path = self._file_path_generator.generate_file_path(
            symbol="ESZ21",
            frequency=typ.Frequency.Minutely,
            asset_class=typ.AssetClass.Futures,
            contract_type=typ.ContractType.Expiry,
            ext=typ.Extension.CSV,
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
            frequency=typ.Frequency.Minutely,
            asset_class=typ.AssetClass.Stocks,
            contract_type=typ.ContractType.Continuous,
            ext=typ.Extension.CSV,
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
            frequency=typ.Frequency.Daily,
            asset_class=typ.AssetClass.Futures,
            contract_type=typ.ContractType.Expiry,
            ext=typ.Extension.CSV,
        )
        # Compare with expected value.
        self.check_string(path)

