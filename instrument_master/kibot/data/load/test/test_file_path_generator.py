import helpers.unit_test as hut
import instrument_master.common.data.types as vcdtyp
import instrument_master.kibot.data.config as vkdcon
import instrument_master.kibot.data.load.file_path_generator as vkdlfi


class TestFilePathGenerator(hut.TestCase):
    def test1(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Continuous,
            frequency=vcdtyp.Frequency.Daily,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "all_futures_continuous_contracts_daily/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test2(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Continuous,
            frequency=vcdtyp.Frequency.Daily,
            ext=vcdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_futures_continuous_contracts_daily/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test3(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Expiry,
            frequency=vcdtyp.Frequency.Daily,
            ext=vcdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_futures_contracts_daily/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test4(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Expiry,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_futures_contracts_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test5(self) -> None:
        """
        Test generating file name for stocks without unadjusted raises
        exception.
        """
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Stocks,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_stocks_unadjusted_1min/TEST.pq"

        with self.assertRaises(AssertionError):
            self._assert_file_path(
                args=args, expected_file_path=expected_file_path
            )

    def test6(self) -> None:
        """
        Test generating file name for unadjusted stocks works.
        """
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Stocks,
            unadjusted=True,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_stocks_unadjusted_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test7(self) -> None:
        """
        Test generating file name for unadjusted etfs works.
        """
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.ETFs,
            unadjusted=True,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_etfs_unadjusted_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test8(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Continuous,
            frequency=vcdtyp.Frequency.Tick,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "all_futures_continuous_contracts_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test9(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Forex,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "all_forex_pairs_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test10(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Forex,
            frequency=vcdtyp.Frequency.Daily,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "all_forex_pairs_daily/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test11(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.Stocks,
            unadjusted=False,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "all_stocks_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test12(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.ETFs,
            unadjusted=False,
            frequency=vcdtyp.Frequency.Minutely,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "all_etfs_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test13(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.SP500,
            unadjusted=False,
            frequency=vcdtyp.Frequency.Tick,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "sp_500_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test14(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=vcdtyp.AssetClass.SP500,
            unadjusted=True,
            frequency=vcdtyp.Frequency.Tick,
            ext=vcdtyp.Extension.CSV,
        )
        expected_file_path = "sp_500_unadjusted_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def _assert_file_path(self, args: dict, expected_file_path: str) -> None:
        generator = vkdlfi.KibotFilePathGenerator()

        actual = generator.generate_file_path(**args)
        expected = f"{vkdcon.S3_PREFIX}/{expected_file_path}"
        self.assertEqual(actual, expected)
