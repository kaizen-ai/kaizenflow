import os

import pandas as pd
import pytest

import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest


class TestCsvToPq(hunitest.TestCase):
    """
    Generate some CSV files and run the CSV to Parquet conversion using command
    line.
    """

    def generate_example_csv_files(self) -> None:
        """
        Create CSV files in scratch directory.
        """
        test_dir = self.get_scratch_space()
        self.csv_dir_path = os.path.join(test_dir, "csv_dir")
        hio.create_dir(self.csv_dir_path, False)
        d1 = {
            "timestamp": [1638646800000, 1638646860000, 1638646960000],
            "open": [49317.68, 49330.63, 49320.31],
            "high": [49346.95, 49400.98, 49500.75],
            "volume": [23.13681, 61.99752, 79.92761],
            "low": [49315.45, 49322.78, 49325.23],
            "close": [49330.63, 49325.23, 49328.23],
            "currency_pair": ["BTC_USDT", "ETH_USDT", "BTC_USDT"],
            "end_download_timestamp": [
                "2021-12-07 13:01:20.183463+00:00",
                "2021-12-07 13:01:20.183463+00:00",
                "2021-12-07 13:01:20.183463+00:00",
            ],
            "exchange_id": ["binance", "binance", "binance"],
        }
        d2 = {
            "timestamp": [1638656800000, 1638676860000, 1638656960000],
            "open": [49318.68, 49331.63, 49321.31],
            "high": [49446.95, 49500.98, 49600.75],
            "volume": [24.13681, 62.99752, 80.92761],
            "low": [49325.45, 49323.78, 49326.23],
            "close": [49340.63, 49335.23, 49428.23],
            "currency_pair": ["BTC_USDT", "ETH_USDT", "BTC_USDT"],
            "end_download_timestamp": [
                "2021-12-07 13:01:20.183463+00:00",
                "2021-12-07 13:01:20.183463+00:00",
                "2021-12-07 13:01:20.183463+00:00",
            ],
            "exchange_id": ["binance", "binance", "binance"],
        }
        df1 = pd.DataFrame(data=d1)
        df1.to_csv(os.path.join(self.csv_dir_path, "test1.csv"), index=False)
        df2 = pd.DataFrame(data=d2)
        df2.to_csv(os.path.join(self.csv_dir_path, "test2.csv"), index=False)

    @pytest.mark.skip(
        reason="CmTask1305: after removing circular dependencies in "
        "`hio.from_file`, this test fails reading a parquet file"
    )
    def test_csv_to_pq_script(self) -> None:
        # Generate the files.
        self.generate_example_csv_files()
        pq_dir_path = os.path.join(self.get_scratch_space(), "pq_dir")
        # Run command.
        exec_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/convert_csv_to_pq.py",
        )
        cmd = [
            exec_path,
            f"--src_dir {self.csv_dir_path}",
            f"--dst_dir {pq_dir_path}",
            "--datetime_col timestamp",
            "--asset_col currency_pair",
        ]
        cmd = " ".join(cmd)
        hsystem.system(cmd)
        # Check.
        include_file_content = True
        dir_signature = hunitest.get_dir_signature(
            pq_dir_path,
            include_file_content,
            remove_dir_name=True,
        )
        self.check_string(dir_signature, purify_text=True)