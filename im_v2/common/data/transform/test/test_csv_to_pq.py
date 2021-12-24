import os

import pandas as pd
import pytest

import helpers.io_ as hio
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.common.data.transform.csv_to_pq as imvcdtctpq

class TestCsvToPq(hunitest.TestCase):
    def test_csv_to_pq_script(self) -> None:
        """
        Test that generated parquet dataset is correct.
        """
        # Generate the files.
        self._generate_example_csv_files()
        pq_dir_path = os.path.join(self.get_scratch_space(), "pq_dir")
        # Run command.
        cmd = [
            "im_v2/common/data/transform/csv_to_pq.py",
            f"--src_dir {self.csv_dir_path}",
            f"--dst_dir {pq_dir_path}",
            "--datetime_col timestamp",
            "--asset_col currency_pair",
        ]
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        # Check output directory structure.
        directories = []
        for root, dirs, files in os.walk(pq_dir_path):
            for subdir in dirs:
                directories.append(os.path.join(root, subdir))
        actual_dirs = "\n".join(directories)
        # Check output.
        actual_df = pd.read_parquet(pq_dir_path)
        actual_df = hunitest.convert_df_to_json_string(actual_df, n_tail=None)
        actual_result = "\n".join([actual_dirs, actual_df])
        self.check_string(actual_result, purify_text=True)

    def _generate_example_csv_files(self) -> None:
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
            "created_at": [
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
            "created_at": [
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


class TestConvertTimestampColumn(hunitest.TestCase):
    def test_convert_integer(self) -> None:
        """
        Verify that integer datetime is converted correctly.
        """
        test_data = pd.Series([1638756800000, 1639656800000, 1648656800000])
        actual = imvcdtctpq.convert_timestamp_column(test_data)
        actual = str(actual)
        self.check_string(actual)

    def test_convert_string(self) -> None:
        """
        Verify that string datetime is converted correctly.
        """
        test_data = pd.Series(["2021-01-12", "2021-02-14", "2010-12-11"])
        actual = imvcdtctpq.convert_timestamp_column(test_data)
        actual = str(actual)
        self.check_string(actual)

    def test_convert_incorrect(self) -> None:
        """
        Assert that incorrect types are not converted.
        """
        test_data = pd.Series([37.9, 88.11, 14.0])
        with self.assertRaises(ValueError):
            imvcdtctpq.convert_timestamp_column(test_data)
