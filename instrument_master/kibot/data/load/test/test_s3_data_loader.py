import pytest

import helpers.unit_test as hut
import instrument_master.common.data.types as vcdtyp
import instrument_master.kibot.data.load as vkdloa


class TestS3KibotDataLoader(hut.TestCase):
    @pytest.mark.slow
    def test1(self) -> None:
        # use the private method to avoid caching
        # TODO(*): Disable caching in unit tests.
        df = vkdloa.S3KibotDataLoader()._read_data(
            symbol="XG",
            asset_class=vcdtyp.AssetClass.Futures,
            frequency=vcdtyp.Frequency.Daily,
            contract_type=vcdtyp.ContractType.Continuous,
        )

        self.check_string(df.head(10).to_string())
