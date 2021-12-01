import os
import pytest

import helpers.hparquet as hparque
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.common.data.transform.generate_pq_example_data as imvcdtgped


class TestPqByDateToByAsset1(hunitest.TestCase):
    @pytest.mark.skip(reason="Passing on EC2, failing on job run Run_fast_tests #595")
    def test_convert_pq_by_date_to_by_asset(self) -> None:
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        by_asset_dir = os.path.join(test_dir, "by_asset")

        assets = ["A", "B", "C"]
        imvcdtgped.generate_pq_daily_data(
            "2021-12-30", "2022-01-02", assets, dst_dir=by_date_dir
        )
        file_path = (
            "im_v2/common/data/transform/convert_pq_by_date_to_by_asset.py"
        )
        cmd = (
            f"python {file_path} --src_dir {by_date_dir} --dst_dir {by_asset_dir}"
        )
        hsysinte.system(cmd)

        # Check directory structure.
        include_file_content = False
        by_date_signature = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        by_asset_signature = hunitest.get_dir_signature(
            by_asset_dir, include_file_content
        )
        self.check_string(by_date_signature, tag="by_date_signature")
        self.check_string(by_asset_signature, tag="by_asset_signature")

        # Check parquet files content.
        signatures = by_date_signature.split("\n") + by_asset_signature.split(
            "\n"
        )
        for signature in signatures:
            if signature.endswith(".parquet") or signature.endswith(".pq"):
                df = hparque.from_parquet(signature)
                df_signature = hunitest.get_df_signature(df, 12)
                self.check_string(
                    df_signature, tag=signature.split("tmp.scratch/")[-1]
                )
