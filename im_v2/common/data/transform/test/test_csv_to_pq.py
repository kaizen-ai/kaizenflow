import os

import pytest

import pandas as pd

import helpers.io_ as hio
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest


class TestCsvToPq(hunitest.TestCase):
    @pytest.mark.skip("Enable when purify_text is set to True CMTask782")
    def test1(self) -> None:
        """
        Test that generated .pq file is correct.
        """
        # Generate the files.
        self._generate_example_csv_file()
        pq_dir_path = os.path.join(self.get_scratch_space(), "pq_dir")
        # Run command.
        cmd = (
            "im_v2/common/data/transform/csv_to_pq.py"
            f" --src_dir {self.csv_dir_path}"
            f" --dst_dir {pq_dir_path}"
        )
        hsysinte.system(cmd)
        # Check output.
        df = pd.read_parquet(os.path.join(pq_dir_path, "test.parquet"))
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    @pytest.mark.skip("Enable when purify_text is set to True CMTask782")
    def test2(self) -> None:
        """
        Test that --incremental option does not change the file.
        """
        # Generate the files.
        self._generate_example_csv_file()
        self._generate_example_pq_file()
        # Read file before running command.
        parquet_file_path = os.path.join(self.pq_dir_path, "test.parquet")
        df = pd.read_parquet(parquet_file_path)
        before = hunitest.convert_df_to_json_string(df, n_tail=None)
        # Run command.
        cmd = (
            "im_v2/common/data/transform/csv_to_pq.py"
            f" --src_dir {self.csv_dir_path}"
            f" --dst_dir {self.pq_dir_path}"
            " --incremental"
        )
        hsysinte.system(cmd)
        # Read file after running command.
        df = pd.read_parquet(parquet_file_path)
        after = hunitest.convert_df_to_json_string(df, n_tail=None)
        # Check that file does not change.
        self.assert_equal(before, after)

    def _generate_example_csv_file(self) -> None:
        """
        Create a CSV file in scratch directory.
        """
        test_dir = self.get_scratch_space()
        self.csv_dir_path = os.path.join(test_dir, "csv_dir")
        hio.create_dir(self.csv_dir_path, False)
        d = {
            "timestamp": [1638646800000, 1638646860000],
            "open": [49317.68, 49330.63],
            "high": [49346.95, 49400.98],
            "volume": [23.13681, 61.99752],
            "low": [49315.45, 49322.78],
            "close": [49330.63, 49325.23],
            "currency_pair": ["BTC_USDT", "BTC_USDT"],
            "created_at": [
                "2021-12-07 13:01:20.183463+00:00",
                "2021-12-07 13:01:20.183463+00:00",
            ],
            "exchange_id": ["binance", "binance"],
        }
        df = pd.DataFrame(data=d)
        df.to_csv(os.path.join(self.csv_dir_path, "test.csv"), index=False)

    def _generate_example_pq_file(self) -> None:
        """
        Create a PQ file in scratch directory.
        """
        test_dir = self.get_scratch_space()
        self.pq_dir_path = os.path.join(test_dir, "pq_dir")
        hio.create_dir(self.pq_dir_path, False)
        # Create a PQ file with a different content than `_generate_example_csv_file`
        # since we use the content to check whether the file was overwritten or not.
        d = {
            "timestamp": [1632646800000, 1638646860000],
            "open": [49666.18, 49666.63],
            "high": [49666.95, 49666.98],
            "volume": [23.12681, 61.99752],
            "low": [49666.45, 49322.78],
            "close": [49230.63, 49225.23],
            "currency_pair": ["BTC_USDT", "BTC_USDT"],
            "created_at": [
                "2021-12-07 13:01:20.183463+00:00",
                "2021-12-07 13:01:20.183463+00:00",
            ],
            "exchange_id": ["binance", "binance"],
        }
        df = pd.DataFrame(data=d)
        df.to_parquet(os.path.join(self.pq_dir_path, "test.parquet"), index=False)
