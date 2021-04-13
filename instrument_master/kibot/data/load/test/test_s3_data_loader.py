import pandas as pd
import pytest

import helpers.unit_test as hut
import instrument_master.common.data.types as icdtyp
import instrument_master.kibot.data.load as vkdloa


class TestKibotS3DataLoader(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._s3_data_loader = vkdloa.KibotS3DataLoader()

    @pytest.mark.slow
    def test1(self) -> None:
        # Use the private method to avoid caching.
        df = self._s3_data_loader._read_data(
            symbol="XG",
            asset_class=icdtyp.AssetClass.Futures,
            frequency=icdtyp.Frequency.Daily,
            contract_type=icdtyp.ContractType.Continuous,
        )
        self.check_string(df.head(10).to_string())

    @pytest.mark.skip(reason="Not implemented yet")
    def test_read_data_with_start_end_ts(self) -> None:
        """
        Test correctness of hourly ES data loading.
        """
        # Load data.
        data = self._s3_data_loader._read_data(
            symbol="XG",
            asset_class=icdtyp.AssetClass.Futures,
            frequency=icdtyp.Frequency.Daily,
            contract_type=icdtyp.ContractType.Continuous,
            start_ts=pd.to_datetime("1990-12-28 00:00:00"),
            end_ts=pd.to_datetime("1991-01-14 00:00:00"),
        )
        data.insert(0, "date", data.index)
        # Transform dataframe to string.
        actual_string = hut.convert_df_to_string(data)
        # Compare with expected.
        self.check_string(actual_string, fuzzy_match=True)
