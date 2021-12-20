import os

import pytest

import helpers.git as hgit
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.common.data.transform.partition_pq_dataset as ppqiw


class TestPartitionPQDataset(hunitest.TestCase):
    def test_partition_dataset_by_date(self):
        test_dir = self.get_scratch_space()
        source_dir = os.path.join(test_dir, "source_dir")
        dst_dir = os.path.join(test_dir, "by_date")
        cmd = []
        script_path = "im_v2/common/data/transform/partition_pq_dataset.py "
        cmd.append(script_path)
        cmd.append(f"--src_dir {source_dir} ")
        cmd.append(f"--dst_dir {dst_dir} ")
        cmd.append(f"--by date ")
        cmd.append(f"--datetime_col start_date")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        include_file_content = True
        signature = hunitest.get_dir_signature(
            dst_dir, include_file_content
        )
        self.check_string(signature)

    def test_partition_dataset_by_asset(self):
        test_dir = self.get_scratch_space()
        source_dir = os.path.join(test_dir, "source_dir")
        dst_dir = os.path.join(test_dir, "by_asset")
        cmd = []
        script_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/partition_pq_dataset.py")
        cmd.append(script_path)
        cmd.append(f"--src_dir {source_dir}")
        cmd.append(f"--dst_dir {dst_dir}")
        cmd.append(f"--by asset")
        cmd.append(f"--datetime_col start_date")
        cmd.append(f"--asset_col currency")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        include_file_content = True
        signature = hunitest.get_dir_signature(
            dst_dir, include_file_content
        )
        self.check_string(signature)

    def _generate_daily_data(self) -> None:
        """
        Generate daily data for 3 days and save as unpartitioned parquet.
        """
        test_dir = self.get_scratch_space()
        source_dir = os.path.join(test_dir, "source_dir")
        # Generate some data.
        cmd = []
        script_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/test/generate_pq_example_data.py",
        )
        cmd.append(script_path)
        cmd.append("--start_date 2021-12-30")
        cmd.append("--end_date 2022-01-02")
        cmd.append("--assets A,B,C")
        cmd.append(f"--dst_dir {source_dir}")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
