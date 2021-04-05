import helpers.unit_test as hut
import instrument_master.common.data.types as icdtyp
import instrument_master.kibot.data.config as ikdcon
import instrument_master.kibot.data.load.kibot_file_path_generator as ikdlki


class TestFilePathGenerator(hut.TestCase):
    def test1(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            frequency=icdtyp.Frequency.Daily,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "all_futures_continuous_contracts_daily/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test2(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            frequency=icdtyp.Frequency.Daily,
            ext=icdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_futures_continuous_contracts_daily/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test3(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Expiry,
            frequency=icdtyp.Frequency.Daily,
            ext=icdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_futures_contracts_daily/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test4(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Expiry,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.Parquet,
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
            asset_class=icdtyp.AssetClass.Stocks,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.Parquet,
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
            asset_class=icdtyp.AssetClass.Stocks,
            unadjusted=True,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_stocks_unadjusted_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test7(self) -> None:
        """
        Test generating file name for unadjusted etfs works.
        """
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.ETFs,
            unadjusted=True,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.Parquet,
        )
        expected_file_path = "pq/all_etfs_unadjusted_1min/TEST.pq"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test8(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            frequency=icdtyp.Frequency.Tick,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "all_futures_continuous_contracts_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test9(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Forex,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "all_forex_pairs_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test10(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Forex,
            frequency=icdtyp.Frequency.Daily,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "all_forex_pairs_daily/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test11(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.Stocks,
            unadjusted=False,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "all_stocks_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test12(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.ETFs,
            unadjusted=False,
            frequency=icdtyp.Frequency.Minutely,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "all_etfs_1min/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test13(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.SP500,
            unadjusted=False,
            frequency=icdtyp.Frequency.Tick,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "sp_500_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def test14(self) -> None:
        args = dict(
            symbol="TEST",
            asset_class=icdtyp.AssetClass.SP500,
            unadjusted=True,
            frequency=icdtyp.Frequency.Tick,
            ext=icdtyp.Extension.CSV,
        )
        expected_file_path = "sp_500_unadjusted_tick/TEST.csv.gz"
        self._assert_file_path(args=args, expected_file_path=expected_file_path)

    def _assert_file_path(self, args: dict, expected_file_path: str) -> None:
        generator = ikdlki.KibotFilePathGenerator()
        actual = generator.generate_file_path(**args)
        expected = f"{ikdcon.S3_PREFIX}/{expected_file_path}"
        self.assertEqual(actual, expected)
