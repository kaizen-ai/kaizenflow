from typing import List, Tuple

import pandas as pd

import os
import helpers.hunit_test as hunitest
import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.hsql as hsql
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.universe.universe_utils as imvcuunut


# Helper class for generating test data.
from im_v2.common.data.client import full_symbol as imvcdcfusy


class HistoricalClientTestHelper(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """
    def _generate_test_data(
            self,
            start_date: str,
            end_date: str,
            assets: str,
            *,
            output_type: str = "verbose_close",
            partition_mode: str = "by_year_month"
    ) -> str:
        """
        Generate test data in form of partitioned Parquet files.
        """
        test_dir = self.get_scratch_space()
        tiled_bar_data_dir = os.path.join(test_dir, "tiled.bar_data")
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/test/generate_pq_test_data.py",
        )
        cmd.append(file_path)
        cmd.append(f"--start_date {start_date}")
        cmd.append(f"--end_date {end_date}")
        cmd.append(f"--assets {assets}")
        cmd.append(f"--partition_mode {partition_mode}")
        cmd.append(f"--dst_dir {tiled_bar_data_dir}")
        cmd.append(f"--output_type {output_type}")
        cmd = " ".join(cmd)
        hsystem.system(cmd)
        return test_dir


# #############################################################################
# TestHistoricalPqByAssetClient1
# #############################################################################


# full_symbols = imvccunun.get_vendor_universe()
# numerical_asset_id = [
#     imvcuunut.string_to_numerical_id(full_symbol)
#     for full_symbol in full_symbols
# ]


class HistoricalByAssetDummy(imvcdchpcl.HistoricalPqByAssetClient):
    """
    Only here to allow usage of `HistoricalPqByAssetClient`.
    """
    # TODO(Nikola): Forced to override?
    def get_universe(self) -> List[str]:
        """
        See description in the parent class.
        """
        universe = imvccunun.get_vendor_universe()
        return universe  # type: ignore[no-any-return]


class TestHistoricalPqByAssetClient1(HistoricalClientTestHelper):
    def test_read_data1(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        assets = "1467591036"
        # assets = "1467591036,1508924190"
        test_dir = self._generate_test_data(start_date, end_date, assets)
        # Init client for testing.
        asset_column_name = "asset_id"
        partition_mode = "by_year_month"
        im_client = HistoricalByAssetDummy(asset_column_name, test_dir, partition_mode)
        # Compare the expected values.
        full_symbol = "binance::BTC_USDT"
        # full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"][0]
        expected_length = 4261
        expected_column_names = r"" # TODO(Nikola): Passes with this.. something is off.
        expected_column_unique_values = {"full_symbol": [1467591036.0]}  # TODO(Nikola): Why float?
        # TODO(Nikola): Assuming NaN is because generated data is hourly.
        expected_signature = r"""# df=
        df.index in [2021-12-30 00:00:00+00:00, 2022-01-01 23:00:00+00:00]
        df.columns=full_symbol,close,year,month
        df.shape=(4261, 4)
                                    full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00  1.467591e+09    0.0  2021    12
        2021-12-30 00:01:00+00:00  1.467591e+09    NaN   NaN   NaN
        2021-12-30 00:02:00+00:00  1.467591e+09    NaN   NaN   NaN
        ...
        2022-01-01 22:58:00+00:00  1.467591e+09    NaN   NaN   NaN
        2022-01-01 22:59:00+00:00  1.467591e+09    NaN   NaN   NaN
        2022-01-01 23:00:00+00:00  1.467591e+09   71.0  2022     1"""
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
