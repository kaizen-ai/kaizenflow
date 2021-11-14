import pandas as pd
import pytest

import helpers.unit_test as hunitest
import im.common.data.types as imcodatyp
import im.kibot.data.load.kibot_s3_data_loader as imkdlksdlo


class TestKibotS3DataLoader(hunitest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._s3_data_loader = imkdlksdlo.KibotS3DataLoader()

    @pytest.mark.slow
    def test1(self) -> None:
        df = self._s3_data_loader._read_data(
            symbol="XG",
            asset_class=imcodatyp.AssetClass.Futures,
            frequency=imcodatyp.Frequency.Daily,
            contract_type=imcodatyp.ContractType.Continuous,
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
            asset_class=imcodatyp.AssetClass.Futures,
            frequency=imcodatyp.Frequency.Daily,
            contract_type=imcodatyp.ContractType.Continuous,
            start_ts=pd.to_datetime("1990-12-28 00:00:00"),
            end_ts=pd.to_datetime("1991-01-14 00:00:00"),
        )
        data.insert(0, "date", data.index)
        # Transform dataframe to string.
        actual_string = hunitest.convert_df_to_string(data)
        # Compare with expected.
        self.check_string(actual_string, fuzzy_match=True)
