import argparse
import copy
import os
from typing import Tuple

import pytest

import helpers.git as hgit
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.common.data.transform.convert_pq_by_date_to_by_asset as imvcdtcpbdtba


class TestPqByDateToByAsset1(hunitest.TestCase):
    test_kwargs = {
        "transform_func": "",
        "asset_col_name": "asset",
        # parallelization args
        "num_threads": "2",
        "dry_run": True,
        "no_incremental": True,
        "skip_on_error": True,
        "num_attempts": 1,
    }

    def generate_test_data(self, verbose: bool) -> Tuple[str, str]:
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
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
        return test_dir, by_date_dir

    def check_directory_structure_with_file_contents(
        self, by_date_dir: str, by_asset_dir: str
    ) -> None:
        include_file_content = True
        by_date_signature = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        actual = []
        actual.append("# by_date=")
        actual.append(by_date_signature)
        by_asset_signature = hunitest.get_dir_signature(
            by_asset_dir, include_file_content
        )
        actual.append("# by_asset=")
        actual.append(by_asset_signature)
        actual = "\n".join(actual)
        purify_text = True
        self.check_string(actual, purify_text=purify_text)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_daily_data1(self) -> None:
        verbose = False
        self._test_daily_data(verbose)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_daily_data2(self) -> None:
        verbose = True
        self._test_daily_data(verbose)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_daily_data_direct_run1(self) -> None:
        verbose = False
        self._test_daily_data_direct_run(verbose)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_daily_data_direct_run2(self) -> None:
        verbose = True
        self._test_daily_data_direct_run(verbose)

    def _test_daily_data(self, verbose: bool) -> None:
        """
        Generate daily data for 3 days in a by-date format and then convert it
        to by-asset.
        """
        test_dir, by_date_dir = self.generate_test_data(verbose)
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
        self.check_directory_structure_with_file_contents(
            by_date_dir, by_asset_dir
        )

    def _test_daily_data_direct_run(self, verbose: bool) -> None:
        """
        Tests directly _run function for coverage increase.

        Flow is same as in _test_daily_data.
        """
        test_dir, by_date_dir = self.generate_test_data(verbose)
        by_asset_dir = os.path.join(test_dir, "by_asset")
        kwargs = copy.deepcopy(self.test_kwargs)
        kwargs.update(
            {
                "src_dir": by_date_dir,
                "dst_dir": by_asset_dir,
            }
        )
        if verbose:
            kwargs.update(
                {
                    "transform_func": "reindex_on_unix_epoch",
                    "asset_col_name": "ticker",
                }
            )
        args = argparse.Namespace(**kwargs)
        imvcdtcpbdtba._run(args)
        self.check_directory_structure_with_file_contents(
            by_date_dir, by_asset_dir
        )


# TODO(Nikola): Command line tests plus error checks for direct run.
#   _save_chunk must be tested separately!
