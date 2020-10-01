import helpers.unit_test as hut
import vendors2.kibot.data.load as load
import vendors2.kibot.data.types as types


class TestKibotDataLoader(hut.TestCase):
    def test1(self) -> None:
        df = load.KibotDataLoader().read_data(
            symbol="XG",
            asset_class=types.AssetClass.Futures,
            frequency=types.Frequency.Daily,
            contract_type=types.ContractType.Continuous,
        )

        self.check_string(df.head(10).to_string())
