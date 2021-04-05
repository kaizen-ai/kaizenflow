import pytest

import helpers.unit_test as hut
import instrument_master.common.data.types as vcdtyp
import instrument_master.kibot.data.load as vkdloa


class TestKibotS3DataLoader(hut.TestCase):
    @pytest.mark.slow
    def test1(self) -> None:
        # Use the private method to avoid caching
        df = vkdloa.KibotS3DataLoader()._read_data(
            symbol="XG",
            asset_class=vcdtyp.AssetClass.Futures,
            frequency=vcdtyp.Frequency.Daily,
            contract_type=vcdtyp.ContractType.Continuous,
        )
        self.check_string(df.head(10).to_string())
