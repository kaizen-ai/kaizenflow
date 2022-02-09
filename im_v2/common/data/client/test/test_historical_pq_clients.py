from typing import List, Tuple

import pandas as pd

import os
import helpers.hunit_test as hunitest
import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.hsql as hsql
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl

# #############################################################################
# TestHistoricalPqByAssetClient1
# #############################################################################


class TestHistoricalPqByAssetClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """
    def _generate_test_data(self, output_type) -> str:
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
        cmd.append("--start_date 2021-12-30")
        cmd.append("--end_date 2022-01-02")
        cmd.append("--assets 10689,10690")
        cmd.append("--partition_mode by_year_month")
        cmd.append(f"--dst_dir {tiled_bar_data_dir}")
        if output_type:
            cmd.append(f"--output_type {output_type}")
        cmd = " ".join(cmd)
        hsystem.system(cmd)
        return test_dir

    # def check_directory_structure_with_file_contents(self, tiled_bar_data_dir: str) -> None:
    #     """
    #     Generate directory and file structure together with file contents in
    #     form of string. String is compared with previously generated `.txt` file
    #     for any differences.
    #
    #     :param tiled_bar_data_dir:  Location of partitioned Parquet files.
    #     """
    #     include_file_content = True
    #     tiled_bar_data_signature = hunitest.get_dir_signature(
    #         tiled_bar_data_dir, include_file_content
    #     )
    #     self.check_string(tiled_bar_data_signature, purify_text=True)

    def test_read_data1(self) -> None:
        asset_column_name = "asset_id"
        output_type = "verbose_close"
        test_dir = self._generate_test_data(output_type)
        # TODO(Nikola): Dummy class to inherit abstract one.
        im_client = imvcdchpcl.HistoricalPqByAssetClient(asset_column_name, test_dir)
        full_symbol = "binance::BTC_USDT"
        #
        expected_length = 100
        expected_column_names = """
        Depends on the output type... it can be asset_id, close...
        vendor_date, interval, ticker, etc.
        """
        # TODO(Nikola): From where should asset ids be obtained?
        #   Currently only name pairs are visible in examples... no ids.
        expected_column_unique_values = {"full_symbol": ["binance::BTC_USDT"]}
        expected_signature = ""
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
