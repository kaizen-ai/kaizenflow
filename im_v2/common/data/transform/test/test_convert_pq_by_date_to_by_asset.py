import os

import pytest

import helpers.git as hgit
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

# TODO(Nikola): Add one test for the command line and other tests testing directly _run
#  to get coverage.


class TestPqByDateToByAsset1(hunitest.TestCase):
    @pytest.mark.skip("Enable when purify_text is set to True CMTask782")
    def test_daily_data1(self):
        verbose = False
        self._test_daily_data(verbose)

    @pytest.mark.skip("Enable when purify_text is set to True CMTask782")
    def test_daily_data2(self):
        verbose = True
        self._test_daily_data(verbose)

    # TODO(Nikola): Parametrize?
    def _test_daily_data(self, verbose: bool) -> None:
        """
        Generate daily data for 3 days in a by-date format and then convert it
        to by-asset.
        """
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        # Generate some data.
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/test/generate_pq_example_data.py",
        )
        cmd.append(file_path)
        cmd.append("--start_date 2021-12-30")
        cmd.append("--end_date 2022-01-02")
        cmd.append("--assets A,B,C")
        cmd.append(f"--dst_dir {by_date_dir}")
        if verbose:
            cmd.append("--verbose")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        # Build command line to convert the data.
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/convert_pq_by_date_to_by_asset.py",
        )
        cmd.append(file_path)
        cmd.append(f"--src_dir {by_date_dir}")
        by_asset_dir = os.path.join(test_dir, "by_asset")
        cmd.append(f"--dst_dir {by_asset_dir}")
        cmd.append("--num_threads 2")
        if verbose:
            cmd.append("--transform_func reindex_on_unix_epoch")
            cmd.append("--asset_col_name ticker")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        # Check directory structure with file contents.
        include_file_content = True
        by_date_signature = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        act = []
        act.append("# by_date=")
        act.append(by_date_signature)
        by_asset_signature = hunitest.get_dir_signature(
            by_asset_dir, include_file_content
        )
        act.append("# by_asset=")
        act.append(by_asset_signature)
        act = "\n".join(act)
        self.check_string(act)
