import logging
import os

import helpers.git as hgit
import helpers.hparquet as hparque
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.common.data.transform.generate_pq_example_data as imvcdtgped

_LOG = logging.getLogger(__name__)

# TODO(Nikola): Add a unit test for `imvcdtgped.generate_pq_daily_data` just
#  generating data and then a check_string to show how the data looks like.

# TODO(Nikola): Add one test for the command line and other tests testing directly _run
#  to get coverage.
# TODO(Nikola): We want to share code among the tests, instead of copy-paste.

class TestPqByDateToByAsset1(hunitest.TestCase):

    def test_daily_data1(self) -> None:
        """
        Generate daily data for 4 days in a by-date format and then convert
        it to by-asset.
        """
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        # Generate some data.
        assets = ["A", "B", "C"]
        imvcdtgped.generate_pq_daily_data(
            "2021-12-30", "2022-01-02", assets, dst_dir=by_date_dir
        )
        # Build command line to convert the data.
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/convert_pq_by_date_to_by_asset.py"
        )
        cmd.append(file_path)
        cmd.append(f"--src_dir {by_date_dir}")
        by_asset_dir = os.path.join(test_dir, "by_asset")
        cmd.append(f"--dst_dir {by_asset_dir}")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        # Check directory structure.
        include_file_content = False
        by_date_signature = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        # TODO(Nikola): Let's create a single txt and do a single check_string
        #  on it.
        act = []
        act.append("# by_date_signature=")
        act.append(by_date_signature)
        # Remove references to dirs.
        by_asset_signature = hunitest.get_dir_signature(
            by_asset_dir, include_file_content
        )
        act.append("# by_asset_signature=")
        act.append(by_asset_signature)
        # Check parquet files content.
        # TODO(Nikola): Let's generalize get_dir_signature to report a snippet
        #  of PQ files.
        # for signature in signatures:
        #     if signature.endswith(".parquet") or signature.endswith(".pq"):
        #         df = hparque.from_parquet(signature)
        #         num_rows = 12
        #         df_signature = hunitest.get_df_signature(df, num_rows)
        #         self.check_string(
        #             df_signature, tag=signature.split("tmp.scratch/")[-1]
        #         )

        act = "\n".join(act)
        purify_text = True
        self.check_string(act, purify_text=purify_text)


# TODO(Nikola): Add unit test for --transform reindex_on_unix_epoch
# The input looks like:
# ```
#   vendor_date  interval  start_time    end_time ticker currency  open         id
# 0  2021-11-24        60  1637762400  1637762460      A      USD   100         1
# 1  2021-11-24        60  1637762400  1637762460      A      USD   200         1
# ```
# We need another function to generate this format.
