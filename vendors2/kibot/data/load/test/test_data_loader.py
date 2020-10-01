import pytest

import helpers.unit_test as hut
import vendors2.kibot.data.load as load
import vendors2.kibot.data.types as types


class TestKibotDataLoader(hut.TestCase):
    @pytest.mark.slow
    def test1(self) -> None:
        # use the private method to avoid caching
        # TODO(*): Disable caching in unit tests.
        df = load.KibotDataLoader()._read_data(
            symbol="XG",
            asset_class=types.AssetClass.Futures,
            frequency=types.Frequency.Daily,
            contract_type=types.ContractType.Continuous,
        )

        self.check_string(df.head(10).to_string())
