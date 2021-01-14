import pytest

import helpers.unit_test as hut
import vendors2.kibot.data.load as vkdloa
import vendors2.kibot.data.types as vkdtyp


class TestS3KibotDataLoader(hut.TestCase):
    # @pytest.mark.slow
    @pytest.mark.skip("Disabled because of #4770")
    def test1(self) -> None:
        # use the private method to avoid caching
        # TODO(*): Disable caching in unit tests.
        df = vkdloa.S3KibotDataLoader()._read_data(
            symbol="XG",
            asset_class=vkdtyp.AssetClass.Futures,
            frequency=vkdtyp.Frequency.Daily,
            contract_type=vkdtyp.ContractType.Continuous,
        )

        self.check_string(df.head(10).to_string())
