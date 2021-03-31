import helpers.unit_test as hut
import instrument_master.common.data.types as vcdtyp
import instrument_master.kibot.data.load.dataset_name_parser as vkdlda


class TestDatasetNameParserExtractAssetClass(hut.TestCase):
    def test_all_futures(self) -> None:
        # Define input variables.
        dataset = "all_futures_continuous_contracts_1min"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_asset_class(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.AssetClass.Futures
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_all_stocks(self) -> None:
        # Define input variables.
        dataset = "all_stocks_1min"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_asset_class(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.AssetClass.Stocks
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_all_etfs(self) -> None:
        # Define input variables.
        dataset = "all_etfs_daily"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_asset_class(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.AssetClass.ETFs
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_all_forex(self) -> None:
        # Define input variables.
        dataset = "all_forex_pairs_1min"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_asset_class(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.AssetClass.Forex
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_sp500(self) -> None:
        # Define input variables.
        dataset = "sp_500_tickbidask"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_asset_class(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.AssetClass.SP500
        # Compare actual and expected output.
        self.assertEqual(act, exp)


class TestDatasetNameParserExtractFrequency(hut.TestCase):
    def test_daily(self) -> None:
        # Define input variables.
        dataset = "all_futures_continuous_contracts_daily"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_frequency(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.Frequency.Daily
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_minutely(self) -> None:
        # Define input variables.
        dataset = "all_futures_continuous_contracts_1min"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_frequency(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.Frequency.Minutely
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_tick(self) -> None:
        # Define input variables.
        dataset = "all_futures_continuous_contracts_tick"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_frequency(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.Frequency.Tick
        # Compare actual and expected output.
        self.assertEqual(act, exp)


class TestDatasetNameParserExtractContractType(hut.TestCase):
    def test_continuous(self) -> None:
        # Define input variables.
        dataset = "all_futures_continuous_contracts_1min"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_contract_type(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.ContractType.Continuous
        # Compare actual and expected output.
        self.assertEqual(act, exp)

    def test_expiry(self) -> None:
        # Define input variables.
        dataset = "all_futures_contracts_1min"
        # Call function to test.
        cls = vkdlda.DatasetNameParser()
        act = cls._extract_contract_type(dataset=dataset)
        # Define expected output.
        exp = vcdtyp.ContractType.Expiry
        # Compare actual and expected output.
        self.assertEqual(act, exp)
