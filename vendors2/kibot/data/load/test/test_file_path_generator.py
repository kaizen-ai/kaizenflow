import helpers.unit_test as hut
import vendors2.kibot.data.config as config
import vendors2.kibot.data.load.file_path_generator as fpgen
import vendors2.kibot.data.types as types


class TestFilePathGenerator(hut.TestCase):
    def test1(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Futures,
            contract_type=types.ContractType.Continuous,
            frequency=types.Frequency.Daily,
            ext=types.Extension.CSV,
        )
        expected_file_path = "All_Futures_Continuous_Contracts_daily/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test2(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Futures,
            contract_type=types.ContractType.Continuous,
            frequency=types.Frequency.Daily,
            ext=types.Extension.Parquet,
        )
        expected_file_path = "pq/All_Futures_Continuous_Contracts_daily/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test3(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Futures,
            contract_type=types.ContractType.Expiry,
            frequency=types.Frequency.Daily,
            ext=types.Extension.Parquet,
        )
        expected_file_path = "pq/All_Futures_Contracts_daily/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test4(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Futures,
            contract_type=types.ContractType.Expiry,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.Parquet,
        )
        expected_file_path = "pq/All_Futures_Contracts_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test5(self) -> None:
        """Test generating file name for stocks without unadjusted raises
        exception."""
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Stocks,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.Parquet,
        )
        expected_file_path = "pq/all_stocks_unadjusted_1min/TEST.pq"

        with self.assertRaises(AssertionError):
            self._assert_file_path(
                args=args, expected_file_path=expected_file_path
            )

    def test6(self) -> None:
        """Test generating file name for unadjusted stocks works."""
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Stocks,
            unadjusted=True,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.Parquet,
        )
        expected_file_path = "pq/all_stocks_unadjusted_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test7(self) -> None:
        """Test generating file name for unadjusted etfs works."""
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.ETFs,
            unadjusted=True,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.Parquet,
        )
        expected_file_path = "pq/all_etfs_unadjusted_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test8(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Futures,
            contract_type=types.ContractType.Continuous,
            frequency=types.Frequency.Tick,
            ext=types.Extension.CSV,
        )
        expected_file_path = "All_Futures_Continuous_Contracts_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test9(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Forex,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.CSV,
        )
        expected_file_path = "all_forex_pairs_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test10(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Forex,
            frequency=types.Frequency.Daily,
            ext=types.Extension.CSV,
        )
        expected_file_path = "all_forex_pairs_daily/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test11(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.Stocks,
            unadjusted=False,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.CSV,
        )
        expected_file_path = "all_stocks_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)


    def test12(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.ETFs,
            unadjusted=False,
            frequency=types.Frequency.Minutely,
            ext=types.Extension.CSV,
        )
        expected_file_path = "all_etfs_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test13(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.SP500,
            unadjusted=False,
            frequency=types.Frequency.Tick,
            ext=types.Extension.CSV,
        )
        expected_file_path = "sp_500_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test14(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=types.AssetClass.SP500,
            unadjusted=True,
            frequency=types.Frequency.Tick,
            ext=types.Extension.CSV,
        )
        expected_file_path = "sp_500_unadjusted_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def _assert_file_path(self, args: dict, expected_file_path: str) -> None:
        generator = fpgen.FilePathGenerator()

        actual = generator.generate_file_path(**args)
        expected = f"{config.S3_PREFIX}/{expected_file_path}"
        self.assertEqual(actual, expected)
